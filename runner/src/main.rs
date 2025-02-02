extern crate chunkfs;

use chunkfs::bench::generator::random;
use chunkfs::bench::CDCFixture;
use chunkfs::chunkers::SuperChunker;
use chunkfs::hashers::Sha256Hasher;
use rand::distributions::Uniform;
use std::collections::HashMap;
use std::io;

fn main() -> io::Result<()> {
    let mut fixture = CDCFixture::new(HashMap::default(), Sha256Hasher::default());

    let dataset = random("a", 1000000000, Uniform::new(0, 1))?;

    println!("{dataset:?}");

    let mes = fixture.measure(&dataset, SuperChunker::default())?;

    println!("{:?}", mes);

    Ok(())
}
