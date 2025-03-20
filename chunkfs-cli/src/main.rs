extern crate chunkfs;

use std::io;

use clap::Parser;

fn main() -> io::Result<()> {
    let cli = chunkfscli::cli::Cli::parse();

    cli.start()
}
