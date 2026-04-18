extern crate pretty_env_logger;
#[macro_use]
extern crate log;

mod command_parser;
mod commands;
mod common;
mod database;
mod engine;
mod network;
mod rdb;
mod resp;
mod server;

use log::info;

use crate::{common::Error, server::*};
use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    #[arg(long)]
    replicaof: Option<String>,

    #[arg(long)]
    dir: Option<String>,

    #[arg(long, default_value_t = String::from("dump.rdb"))]
    dbfilename: String,

    #[arg(long, default_value_t = String::from("no"))]
    appendonly: String,

    #[arg(long, default_value_t = String::from("appendonlydir"))]
    appenddirname: String,

    #[arg(long, default_value_t = String::from("appendonly.aof"))]
    appendfilename: String,

    #[arg(long, default_value_t = String::from("everysec"))]
    appendfsync: String,
}

impl Args {
    fn parsed_replica_of(&self) -> Option<(String, u16)> {
        self.replicaof.clone().map(|raw| {
            let parts = raw.split(' ').collect::<Vec<_>>();
            if parts.len() != 2 {
                panic!("Invalid replica of argument");
            }

            let replica_port = u16::from_str_radix(parts[1], 10).expect("port from argument");
            (parts[0].to_string(), replica_port)
        })
    }

    fn is_append_only(&self) -> bool {
        &self.appendonly == "yes"
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // unsafe { std::env::set_var("RUST_LOG", "debug") };
    pretty_env_logger::init();

    info!("Peter-Redis starting");

    let args = Args::parse();

    let server = Server::new(
        args.port,
        args.parsed_replica_of(),
        args.dir.clone().unwrap_or(
            std::env::current_dir()?
                .as_os_str()
                .to_str()
                .unwrap()
                .to_string(),
        ),
        args.dbfilename.clone(),
        args.is_append_only(),
        args.appenddirname,
        args.appendfilename,
        args.appendfsync,
    );
    server.run().await?;

    info!("Peter-Redis ending");

    Ok(())
}
