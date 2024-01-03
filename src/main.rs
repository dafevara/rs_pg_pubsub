#![allow(unused)]

use clap::{Parser, Subcommand};
use tokio::sync::Mutex;
use futures::executor::block_on;

mod db;
mod publish;
mod subscribe;
mod model;

#[derive(Subcommand)]
enum Cmd {
    Init {
        #[arg(short, long)]
        reset: bool,
    },
    Populate {
        n: i32
    },
    Publish {
        n: i32
    },
    Subscribe {
        channel: String,
        workers: i32,
    }

}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Cmd>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Cmd::Init { reset }) => {
            if let Err(err_desc) = db::create_tables().await {
                panic!("{:?}", err_desc)
            };
        },
        Some(Cmd::Populate { n } ) => {
            if let Err(err_populate) = db::populate_base_data(*n).await {
                panic!("{:?}", err_populate)
            }
        },
        Some(Cmd::Publish { n }) => {
            if let Err(err_publish) = publish::payments(*n).await {
                panic!("{:?}", err_publish)
            }
        },
        Some(Cmd::Subscribe {channel, workers}) => {
            if let Err(e) = subscribe::attach(*workers).await {
                panic!("An error occurred: {}", e);
            }
        },
        None => {}
    }
}
