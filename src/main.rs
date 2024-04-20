use clap::Parser;
use redis_scanner::{
  self,
  argv::{Argv, Commands},
  idle::IdleCommand,
  memory::MemoryCommand,
  touch::TouchCommand,
  ttl::TtlCommand,
  Command,
};
use std::{fs, sync::Arc};

#[macro_use]
extern crate log;

#[tokio::main]
async fn main() {
  pretty_env_logger::init();
  let argv = Arc::new(Argv::parse().fix());
  let file = argv.output_file();
  debug!("Argv: {:?}", argv);
  let (client, nodes) = redis_scanner::init(&argv).await.expect("Failed to initialize");
  debug!("Discovered nodes: {:?}", nodes);

  let output = match argv.command {
    Commands::Idle(_) => IdleCommand::run(argv, client, nodes).await.unwrap(),
    Commands::Ttl(_) => TtlCommand::run(argv, client, nodes).await.unwrap(),
    Commands::Touch(_) => TouchCommand::run(argv, client, nodes).await.unwrap(),
    Commands::Memory(_) => MemoryCommand::run(argv, client, nodes).await.unwrap(),
  };
  if let Some(output) = output {
    let output = output.print();
    if let Some(file) = file {
      fs::write(file, output).unwrap();
    } else {
      println!("{}", output);
    }
  }
}
