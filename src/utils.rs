use crate::{argv::Argv, progress::global_progress, ClusterNode};
use chrono::Duration;
use fred::{
  cmd,
  prelude::*,
  types::{
    Builder,
    ClusterDiscoveryPolicy,
    RedisConfig,
    ReplicaConfig,
    ScanResult,
    Scanner,
    Server,
    ServerConfig,
    UnresponsiveConfig,
  },
};
use futures::{
  future::{select, try_join_all, Either},
  pin_mut,
  Stream,
  TryStreamExt,
};
use log::{error, log_enabled, trace, warn};
use prettytable::Table;
use regex::Regex;
use serde_json::{Map, Value};
use std::{
  collections::HashMap,
  future::Future,
  sync::atomic::{AtomicUsize, Ordering},
  time::Duration as StdDuration,
};
use tokio::{pin, task::JoinHandle, time::sleep};

#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use fred::types::TlsConnector;
#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use std::fs;

use crate::progress::min_refresh_delay;
#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use log::debug;

pub const DEFAULT_ESTIMATE_UPDATE_INTERVAL: StdDuration = StdDuration::from_millis(2500);

pub fn incr_atomic(val: &AtomicUsize, amt: usize) -> usize {
  val.fetch_add(amt, Ordering::Acquire).saturating_add(amt)
}

pub fn read_atomic(val: &AtomicUsize) -> usize {
  val.load(Ordering::Acquire)
}

pub fn set_atomic(val: &AtomicUsize, amt: usize) -> usize {
  val.swap(amt, Ordering::SeqCst)
}

#[cfg(feature = "enable-native-tls")]
fn build_tls_config(argv: &Argv) -> Result<Option<TlsConnector>, RedisError> {
  use fred::native_tls::{Certificate, Identity, TlsConnector};

  if argv.tls {
    let mut builder = TlsConnector::builder();
    if let Some(ca_cert_path) = argv.tls_ca_cert.as_ref() {
      debug!("Reading CA certificate from {}", ca_cert_path);
      let buf = fs::read(ca_cert_path)?;
      let trusted_cert = Certificate::from_pem(&buf)?;
      builder.add_root_certificate(trusted_cert);
    }

    if let Some(key_path) = argv.tls_key.as_ref() {
      if let Some(cert_path) = argv.tls_cert.as_ref() {
        debug!("Reading client key from {}, cert from {}", key_path, cert_path);
        let client_key_buf = fs::read(key_path)?;
        let client_cert_buf = fs::read(cert_path)?;

        let client_cert_chain = if let Some(ca_cert_path) = argv.tls_ca_cert.as_ref() {
          let ca_cert_buf = fs::read(ca_cert_path)?;

          let mut chain = Vec::with_capacity(ca_cert_buf.len() + client_cert_buf.len());
          chain.extend(&client_cert_buf);
          chain.extend(&ca_cert_buf);
          chain
        } else {
          client_cert_buf
        };

        let identity = Identity::from_pkcs8(&client_cert_chain, &client_key_buf)?;
        builder.identity(identity);
      }
    }

    builder.build().map(|t| Some(t.into())).map_err(|e| e.into())
  } else {
    Ok(None)
  }
}

#[cfg(feature = "enable-rustls")]
fn build_tls_config(argv: &Argv) -> Result<Option<TlsConnector>, RedisError> {
  use fred::rustls::{pki_types::PrivatePkcs8KeyDer, ClientConfig, RootCertStore};

  if argv.tls {
    let (cert_chain, root_store) = if let Some(ca_cert_path) = argv.tls_ca_cert.as_ref() {
      debug!("Reading CA certificate from {}", ca_cert_path);
      let ca_cert_buf = fs::read(ca_cert_path)?;
      let mut root_store = RootCertStore::empty();
      root_store.add(ca_cert_buf.clone().into())?;

      let cert_chain = if let Some(cert_path) = argv.tls_cert.as_ref() {
        debug!("Reading client cert from {}", cert_path);
        let cert_buf = fs::read(cert_path)?;
        Some(vec![cert_buf.into(), ca_cert_buf.into()])
      } else {
        None
      };

      (cert_chain, root_store)
    } else {
      let system_certs = fred::rustls_native_certs::load_native_certs()?;
      let mut cert_store = RootCertStore::empty();
      for system_cert in system_certs.into_iter() {
        cert_store.add(system_cert)?;
      }

      let cert_chain = if let Some(cert_path) = argv.tls_cert.as_ref() {
        debug!("Reading client cert from {}", cert_path);
        let cert_buf = fs::read(cert_path)?;
        Some(vec![cert_buf.into()])
      } else {
        None
      };
      (cert_chain, cert_store)
    };

    let config = if let Some(client_cert_chain) = cert_chain {
      let key_buf = match argv.tls_key.as_ref() {
        Some(key_path) => {
          debug!("Reading client key from {}", key_path);
          fs::read(key_path)?
        },
        None => return Err(RedisError::new(RedisErrorKind::Tls, "Missing client key path.")),
      };

      ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(client_cert_chain, PrivatePkcs8KeyDer::from(key_buf).into())?
    } else {
      ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
    };

    Ok(Some(config.into()))
  } else {
    Ok(None)
  }
}

/// Discover any other servers that should be scanned.
pub async fn discover_servers(argv: &Argv, client: &RedisClient) -> Result<Vec<Server>, RedisError> {
  let config = client.client_config();

  Ok(if client.is_clustered() {
    // use cached primary nodes or one replica per primary
    let primary_nodes = client
      .cached_cluster_state()
      .map(|state| state.unique_primary_nodes())
      .ok_or_else(|| RedisError::new(RedisErrorKind::Config, "Missing cluster state."))?;

    if argv.replicas {
      // pick one replica per primary node
      let replicas = client.replicas().nodes();
      let mut inverted = HashMap::with_capacity(primary_nodes.len());
      for (replica, primary) in replicas.into_iter() {
        let _ = inverted.insert(primary, replica);
      }

      inverted.into_values().collect()
    } else {
      primary_nodes
    }
  } else if config.server.is_sentinel() {
    client.active_connections().await?
  } else {
    config.server.hosts()
  })
}

/// Create a new `Builder` instance by replacing the destination server with a centralized server config to the
/// provided `server`.
pub fn change_builder_server(builder: &Builder, server: &Server) -> Result<Builder, RedisError> {
  let config = builder
    .get_config()
    .map(|config| {
      let mut config = config.clone();
      config.server = ServerConfig::Centralized { server: server.clone() };
      config
    })
    .ok_or_else(|| RedisError::new(RedisErrorKind::Config, "Invalid client builder."))?;

  let mut out = Builder::from_config(config);
  out.set_connection_config(builder.get_connection_config().clone());
  out.set_performance_config(builder.get_performance_config().clone());
  if let Some(policy) = builder.get_policy() {
    out.set_policy(policy.clone());
  }
  Ok(out)
}

/// Create a client builder and the initial connection to the server.
pub async fn init(argv: &Argv) -> Result<(Builder, RedisClient), RedisError> {
  let server = if let Some(ref service) = argv.sentinel_service {
    ServerConfig::Sentinel {
      hosts:        vec![Server::new(&argv.host, argv.port)],
      service_name: service.clone(),
    }
  } else if argv.cluster || argv.replicas {
    ServerConfig::Clustered {
      hosts:  vec![Server::new(&argv.host, argv.port)],
      policy: ClusterDiscoveryPolicy::ConfigEndpoint,
    }
  } else {
    ServerConfig::Centralized {
      server: Server::new(&argv.host, argv.port),
    }
  };
  let config = RedisConfig {
    server,
    #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
    tls: build_tls_config(argv)?.map(|t| t.into()),
    username: argv.username.clone(),
    password: argv.password.clone(),
    database: argv.db,
    fail_fast: true,
    ..Default::default()
  };

  let mut builder = Builder::from_config(config);
  builder.with_connection_config(|config| {
    config.unresponsive = UnresponsiveConfig {
      max_timeout: Some(StdDuration::from_millis(10_000)),
      interval:    StdDuration::from_millis(2_000),
    };

    if argv.replicas {
      config.replica = ReplicaConfig {
        lazy_connections: true,
        primary_fallback: true,
        ignore_reconnection_errors: true,
        connection_error_count: 1,
        ..Default::default()
      };
    }
  });
  if let Some(dur) = argv.reconnect {
    builder.set_policy(ReconnectPolicy::new_constant(0, dur));
  }

  let client = builder.build()?;
  client.init().await?;
  Ok((builder, client))
}

pub fn pretty_duration(dur: &Duration) -> String {
  let num_seconds = dur.num_seconds();
  let seconds = num_seconds % 60;
  let minutes = (num_seconds / 60) % 60;
  let hours = (num_seconds / 60 / 60) % 24;
  let days = num_seconds / 60 / 60 / 24;

  if days > 0 {
    format!("{days}d {hours}h {minutes}m {seconds}s")
  } else if hours > 0 {
    format!("{hours}h {minutes}m {seconds}s")
  } else if minutes > 0 {
    format!("{minutes}m {seconds}s")
  } else {
    format!("{seconds}s")
  }
}

pub async fn update_estimate(server: Server, client: RedisClient) -> Result<(), RedisError> {
  loop {
    tokio::time::sleep(DEFAULT_ESTIMATE_UPDATE_INTERVAL).await;

    trace!("Updating estimate for {}", server);
    let estimate: u64 = client.dbsize().await?;
    global_progress().update_estimate(&server, estimate);
  }
}

pub async fn check_readonly(node: &ClusterNode, client: &RedisClient) -> Result<(), RedisError> {
  if node.readonly {
    client.custom::<(), ()>(cmd!("READONLY"), vec![]).await?;
  }

  Ok(())
}

/// Returns whether the key should be skipped based on the argv `filter` and `reject` expressions.
pub fn should_skip_key_by_regexp(filter: &Option<Regex>, reject: &Option<Regex>, key: &RedisKey) -> bool {
  let matches_filter = if let Some(ref regex) = filter {
    regexp_match(regex, key)
  } else {
    true
  };
  let matches_reject = if let Some(ref regex) = reject {
    regexp_match(regex, key)
  } else {
    false
  };

  !(matches_filter && !matches_reject)
}

/// Returns whether the key matches the provided regexp.
pub fn regexp_match(regex: &Regex, key: &RedisKey) -> bool {
  let key_str = key.as_str_lossy();
  if log_enabled!(log::Level::Trace) {
    trace!(
      "Checking {} against {}: {}",
      key_str,
      regex.as_str(),
      regex.is_match(key_str.as_ref())
    );
  }
  regex.is_match(key_str.as_ref())
}

/// Returns whether the key matches the provided regexp.
pub fn regexp_capture(regex: &Option<Regex>, key: &RedisKey, delimiter: &str) -> Option<String> {
  if let Some(regex) = regex.as_ref() {
    let key_str = key.as_str_lossy();
    let out: Vec<String> = regex
      .captures(key_str.as_ref())
      .iter()
      .flat_map(|s| {
        let mut out = Vec::with_capacity(s.len() - 1);
        for i in 0 .. s.len() - 1 {
          if let Some(val) = s.get(i + 1) {
            out.push(val.as_str().to_string());
          }
        }

        out
      })
      .collect();

    let out = out.join(delimiter);
    if out.is_empty() {
      None
    } else {
      Some(out)
    }
  } else {
    None
  }
}

pub async fn scan_server<F, Fut>(
  server: Server,
  ignore: bool,
  delay: u64,
  scanner: impl Stream<Item = Result<ScanResult, RedisError>>,
  func: F,
) -> Result<(usize, usize), RedisError>
where
  Fut: Future<Output = Result<(usize, usize, usize, usize), RedisError>>,
  F: Fn(usize, usize, usize, usize, Vec<RedisKey>) -> Fut,
{
  let mut last_error = None;
  let (mut local_scanned, mut local_success, mut local_skipped, mut local_errored) = (0, 0, 0, 0);
  pin_mut!(scanner);

  loop {
    let mut page = match scanner.try_next().await {
      Ok(Some(page)) => page,
      Ok(None) => break,
      Err(e) => {
        last_error = Some(e);
        break;
      },
    };

    let keys = page.take_results().unwrap_or_default();
    let (scanned, success, skipped, errored) =
      match func(local_scanned, local_success, local_skipped, local_errored, keys).await {
        Ok(counts) => counts,
        Err(e) => {
          error!("Error in scan loop: {:?}", e);
          last_error = Some(e);
          break;
        },
      };
    local_scanned = scanned;
    local_success = success;
    local_skipped = skipped;
    local_errored = errored;

    if delay > 0 && page.has_more() {
      if delay > min_refresh_delay() / 2 {
        global_progress().update(&server, format!("Sleeping for {}ms", delay), Some(local_scanned as u64));
      }

      sleep(StdDuration::from_millis(delay)).await;
    }

    global_progress().update(
      &server,
      format!(
        "{} success, {} skipped, {} errored",
        local_success, local_skipped, local_errored
      ),
      Some(local_scanned as u64),
    );

    if let Err(e) = page.next() {
      // the more useful error shows up on the next try_next() call
      warn!("Error trying to scan next page: {:?}", e);
    }
  }

  if let Some(error) = last_error {
    global_progress().finish(&server, format!("Errored: {:?}", error));

    if ignore {
      Ok((local_success, local_scanned))
    } else {
      Err(error)
    }
  } else {
    let percent = if local_scanned == 0 {
      0.0
    } else {
      local_success as f64 / local_scanned as f64 * 100.0
    };

    global_progress().finish(
      &server,
      format!(
        "Finished scanning ({}/{} = {:.2}% success, {} skipped, {} errored).",
        local_success, local_scanned, percent, local_skipped, local_errored
      ),
    );
    Ok((local_success, local_scanned))
  }
}

pub async fn wait_with_interrupts(ops: Vec<JoinHandle<Result<(), RedisError>>>) -> Result<(), RedisError> {
  let ctrl_c_ft = tokio::signal::ctrl_c();
  let ops_ft = try_join_all(ops);
  pin!(ctrl_c_ft);
  pin!(ops_ft);

  match select(ops_ft, ctrl_c_ft).await {
    Either::Left((lhs, _)) => match lhs?.into_iter().find_map(|e| e.err()) {
      None => Ok(()),
      Some(err) => Err(err),
    },
    Either::Right((_, _)) => Ok(()),
  }
}

pub fn print_json(headers: &[&'static str], values: Vec<Vec<String>>) -> String {
  let mut out = Vec::with_capacity(values.len());
  for row in values.into_iter() {
    let mut value = Value::Object(Map::with_capacity(row.len()));
    for (idx, val) in row.into_iter().enumerate() {
      value[headers[idx]] = val.into();
    }
    out.push(value);
  }

  serde_json::to_string(&Value::Array(out)).expect("Failed to convert results to JSON")
}

pub fn print_csv(headers: &[&'static str], values: Vec<Vec<String>>) -> String {
  let mut writer = csv::Writer::from_writer(Vec::new());
  writer.write_record(headers).unwrap();

  for row in values.into_iter() {
    writer.write_record(row).unwrap();
  }
  writer.flush().unwrap();
  let buf = writer.into_inner().unwrap();
  String::from_utf8(buf).unwrap()
}

pub fn print_table(headers: &[&'static str], values: Vec<Vec<String>>) -> String {
  let mut table = Table::new();
  table.add_row(headers.into());

  for row in values.into_iter() {
    table.add_row(row.into());
  }

  let mut dst = Vec::new();
  table.print(&mut dst).expect("Failed to serialize table");
  String::from_utf8(dst).expect("Failed to convert to string")
}
