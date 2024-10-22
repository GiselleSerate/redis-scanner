use crate::{
  argv::Argv,
  clear_status,
  output::Output,
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
use std::{future::Future, sync::Arc};

async fn scan_node(
  argv: &Arc<Argv>,
  counters: &Arc<Counters>,
  server: Server,
  client: RedisClient,
) -> Result<(usize, usize), RedisError> {
  let touch = CustomCommand::new_static("TOUCH", ClusterHash::FirstKey, false);
  let scanner = client.scan(&argv.pattern, Some(argv.page_size), None);
  let filter = argv.filter.as_ref().and_then(|s| Regex::new(s).ok());
  let reject = argv.reject.as_ref().and_then(|s| Regex::new(s).ok());

  utils::scan_server(
    server.clone(),
    argv.ignore,
    argv.delay,
    scanner,
    move |mut scanned, mut success, mut skipped, errored, keys| {
      let (touch, filter, reject, client, server) = (
        touch.clone(),
        filter.clone(),
        reject.clone(),
        client.clone(),
        server.clone(),
      );

      async move {
        counters.incr_scanned(keys.len());
        scanned += keys.len();

        let keys: Vec<_> = keys
          .into_iter()
          .filter(|key| {
            if utils::should_skip_key_by_regexp(&filter, &reject, key) {
              skipped += 1;
              counters.incr_skipped(1);
              false
            } else {
              true
            }
          })
          .collect();

        if !keys.is_empty() {
          debug!("Calling TOUCH on {} keys...", keys.len());
          // if this fails in this context it's a bug
          let groups =
            fred::util::group_by_hash_slot(keys).expect("Failed to group scan results by hash slot. This is a bug.");

          let pipeline = client.pipeline();
          for (_, keys) in groups.into_iter() {
            pipeline.custom(touch.clone(), keys.into_iter().collect()).await?;
          }

          let count = match pipeline.all::<Vec<usize>>().await {
            Ok(res) => res.into_iter().fold(0, |a, b| a + b),
            Err(e) => {
              error!("{} Error calling TOUCH: {:?}", server, e);

              if argv.ignore {
                return Ok((scanned, success, skipped, errored));
              } else {
                return Err(e);
              }
            },
          };

          counters.incr_success(count);
          success += count;
        }

        Ok((scanned, success, skipped, errored))
      }
    },
  )
  .await
}

pub struct TouchCommand;

impl Command for TouchCommand {
  fn run(
    argv: Arc<Argv>,
    _: RedisClient,
    nodes: Vec<ClusterNode>,
  ) -> impl Future<Output = Result<Option<Box<dyn Output>>, RedisError>> + Send {
    async move {
      let mut tasks = Vec::with_capacity(nodes.len());
      let counters = Counters::new();

      progress::watch_totals(&counters);
      status!("Connecting to servers...");
      for node in nodes.into_iter() {
        let (argv, counters) = (argv.clone(), counters.clone());
        tasks.push(tokio::spawn(async move {
          let client = node.builder.build()?;
          client.init().await?;
          utils::check_readonly(&node, &client).await?;

          let estimate: u64 = client.dbsize().await?;
          global_progress().add_server(&node.server, Some(estimate), None);
          let estimate_task = tokio::spawn(utils::update_estimate(node.server.clone(), client.clone()));
          let event_task = setup_event_logs(&client);

          let result = scan_node(&argv, &counters, node.server, client).await;
          estimate_task.abort();
          event_task.abort();
          result.map(|_| ())
        }));
      }

      clear_status!();
      if let Err(err) = utils::wait_with_interrupts(tasks).await {
        eprintln!("Fatal error while scanning: {:?}", err);
      }
      Ok(None)
    }
  }
}
