use crate::{argv::Argv, output::Output};
use fred::prelude::*;
use std::{fmt, future::Future, sync::Arc};

#[cfg(all(feature = "enable-native-tls", feature = "enable-rustls"))]
compile_error!("Cannot use both TLS dependencies.");

/// A cluster node identifier and any information necessary to connect to it.
pub struct ClusterNode {
  pub server:   Server,
  pub builder:  Builder,
  pub readonly: bool,
}

impl fmt::Debug for ClusterNode {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ClusterNode")
      .field("server", &format!("{}", self.server))
      .field("readonly", &self.readonly)
      .finish()
  }
}

/// Initialize the client and discover any other servers to be scanned.
pub async fn init(argv: &Argv) -> Result<(RedisClient, Vec<ClusterNode>), RedisError> {
  status!("Discovering servers...");

  let (builder, client) = utils::init(argv).await?;
  let nodes = utils::discover_servers(argv, &client)
    .await?
    .into_iter()
    .map(|server| {
      let builder = utils::change_builder_server(&builder, &server)?;
      Ok(ClusterNode {
        server,
        builder,
        readonly: argv.replicas,
      })
    })
    .collect::<Result<Vec<_>, RedisError>>()?;

  Ok((client, nodes))
}

pub trait Command {
  fn run(
    argv: Arc<Argv>,
    client: RedisClient,
    nodes: Vec<ClusterNode>,
  ) -> impl Future<Output = Result<Option<Box<dyn Output>>, RedisError>> + Send;
}

pub mod argv;
pub mod expire;
pub mod sum;
pub mod idle;
pub mod memory;
pub mod output;
pub mod pqueue;
pub mod progress;
pub mod touch;
pub mod ttl;
pub mod utils;
