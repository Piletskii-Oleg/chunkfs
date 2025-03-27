mod gcc;

use clap::{Parser, ValueEnum};
use std::io;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();

    let client = reqwest::Client::new();
    let loader = Loader { client };

    match cli.source {
        Source::Gcc => {
            println!("d");
            loader.load_gcc().await?;
        }
        Source::Linux => {}
    }

    Ok(())
}

#[derive(ValueEnum, Copy, Clone)]
enum Source {
    Gcc,
    Linux,
}

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    source: Source,
    #[arg(long)]
    path: PathBuf,
}

struct Loader {
    client: reqwest::Client,
}

impl Loader {
    async fn load_gcc(&self) -> io::Result<()> {
        gcc::load_to(&PathBuf::from(".")).await?;
        Ok(())
    }
}
