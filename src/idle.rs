use crate::{
  argv::{Argv, Commands, IdleArgv, OutputFormat},
  clear_status,
  output::Output,
  pqueue::{HashAndOrd, PrioQueue},
  progress::{self, global_progress, setup_event_logs, Counters},
  status,
  utils,
  ClusterNode,
  Command,
};
use fred::{
  prelude::*,
  types::{ClusterHash, CustomCommand},
};
use log::{debug, error};
use regex::Regex;
use std::{
  future::Future,
  hash::{DefaultHasher, Hash, Hasher},
  sync::Arc,
};

static HEADERS: &[&str] = &["Key", "Idle"];

#[derive(Clone, Debug)]
pub struct Idle {
  pub key:  RedisKey,
  pub idle: i64,
}

impl Idle {
  pub fn serialize(self) -> Vec<String> {
    vec![
      self.key.as_str_lossy().escape_default().to_string(),
      self.idle.to_string(),
    ]
  }
}

impl HashAndOrd for Idle {
  fn weight(&self) -> i64 {
    self.idle
  }

  fn int_hash(&self) -> u64 {
    // default hasher should be fine
    let mut h = DefaultHasher::new();
    self.key.as_bytes().hash(&mut h);
    h.finish()
  }

  fn merge(&mut self, _: Self) {}
}

#[derive(Clone)]
pub struct State {
  pub argv:     Arc<Argv>,
  pub cmd_argv: Arc<IdleArgv>,
  pub counters: Arc<Counters>,
  pub pqueue:   Arc<PrioQueue<Idle>>,
}

impl State {
  pub fn take(self: Box<Self>) -> (Vec<Idle>, usize) {
    let offset = self.cmd_argv.offset as usize;
    let results = Arc::try_unwrap(self.pqueue)
      .unwrap_or_else(|o| o.deep_copy())
      .into_vec();

    (results, offset)
  }
}

impl Output for State {
  fn format(&self) -> OutputFormat {
    self.cmd_argv.format.clone()
  }

  fn print_table(self: Box<Self>) -> String {
    let (results, offset) = self.take();
    let rows: Vec<_> = results.into_iter().skip(offset).map(|v| v.serialize()).collect();
    utils::print_table(HEADERS, rows)
  }

  fn print_json(self: Box<Self>) -> String {
    let (results, offset) = self.take();
    let rows: Vec<_> = results.into_iter().skip(offset).map(|v| v.serialize()).collect();
    utils::print_json(HEADERS, rows)
  }

  fn print_csv(self: Box<Self>) -> String {
    let (results, offset) = self.take();
    let rows: Vec<_> = results.into_iter().skip(offset).map(|v| v.serialize()).collect();
    utils::print_csv(HEADERS, rows)
  }
}

async fn scan_node(state: &State, server: Server, client: RedisClient) -> Result<(usize, usize), RedisError> {
  let idletime = CustomCommand::new_static("OBJECT IDLETIME", ClusterHash::FirstKey, false);
  let scanner = client.scan(&state.argv.pattern, Some(state.argv.page_size), None);
  let filter = state.argv.filter.as_ref().and_then(|s| Regex::new(s).ok());
  let reject = state.argv.reject.as_ref().and_then(|s| Regex::new(s).ok());

  utils::scan_server(
    server.clone(),
    state.argv.ignore,
    state.argv.delay,
    scanner,
    move |mut scanned, mut success, mut skipped, errored, keys| {
      let (idletime, filter, reject, client, server) = (
        idletime.clone(),
        filter.clone(),
        reject.clone(),
        client.clone(),
        server.clone(),
      );

      async move {
        state.counters.incr_scanned(keys.len());
        scanned += keys.len();

        let keys: Vec<_> = keys
          .into_iter()
          .filter(|key| {
            if utils::should_skip_key_by_regexp(&filter, &reject, key) {
              skipped += 1;
              state.counters.incr_skipped(1);
              false
            } else {
              true
            }
          })
          .collect();

        if !keys.is_empty() {
          debug!("Calling OBJECT IDLETIME on {} keys...", keys.len());
          // if this fails in this context it's a bug
          let pipeline = client.pipeline();
          for key in keys.iter() {
            pipeline.custom(idletime.clone(), vec![key.clone()]).await?;
          }

          let idle_times = match pipeline.all::<Vec<Option<i64>>>().await {
            Ok(sizes) => sizes,
            Err(e) => {
              error!("{} Error calling MEMORY USAGE: {:?}", server, e);

              if state.argv.ignore {
                return Ok((scanned, success, skipped, errored));
              } else {
                return Err(e);
              }
            },
          };
          state.counters.incr_success(keys.len());
          success += keys.len();

          for (idx, key) in keys.into_iter().enumerate() {
            if let Some(idle) = idle_times[idx] {
              state.pqueue.push_or_update(Idle { key, idle });
            }
          }
        }
        Ok((scanned, success, skipped, errored))
      }
    },
  )
  .await
}

pub struct IdleCommand;

impl Command for IdleCommand {
  fn run(
    argv: Arc<Argv>,
    _: RedisClient,
    nodes: Vec<ClusterNode>,
  ) -> impl Future<Output = Result<Option<Box<dyn Output>>, RedisError>> + Send {
    async move {
      let cmd_argv = match argv.command {
        Commands::Idle(ref inner) => Arc::new(inner.clone()),
        _ => return Err(RedisError::new(RedisErrorKind::Config, "Invalid command")),
      };

      let mut tasks = Vec::with_capacity(nodes.len());
      let counters = Counters::new();
      let max_size = cmd_argv.limit + cmd_argv.offset;
      let pqueue = Arc::new(PrioQueue::new(cmd_argv.sort.clone(), max_size as usize));
      let state = State {
        argv: argv.clone(),
        cmd_argv,
        pqueue,
        counters,
      };

      progress::watch_totals(&state.counters);
      status!("Connecting to servers...");
      for node in nodes.into_iter() {
        let state = state.clone();
        tasks.push(tokio::spawn(async move {
          let client = node.builder.build()?;
          client.init().await?;
          utils::check_readonly(&node, &client).await?;

          let estimate: u64 = client.dbsize().await?;
          global_progress().add_server(&node.server, Some(estimate), None);
          let estimate_task = tokio::spawn(utils::update_estimate(node.server.clone(), client.clone()));
          let event_task = setup_event_logs(&client);

          let result = scan_node(&state, node.server, client).await;
          estimate_task.abort();
          event_task.abort();
          result.map(|_| ())
        }));
      }
      clear_status!();

      if let Err(err) = utils::wait_with_interrupts(tasks).await {
        eprintln!("Fatal error while scanning: {:?}", err);
      }
      Ok(Some(Box::new(state) as Box<dyn Output>))
    }
  }
}
