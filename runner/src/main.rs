extern crate chunkfs;

use std::collections::HashMap;
use std::io;

use chunkfs::bench::generator::fio;
use chunkfs::bench::CDCFixture;
use chunkfs::chunkers::RabinChunker;
use chunkfs::hashers::Sha256Hasher;

fn main() -> io::Result<()> {
    let mut fixture = CDCFixture::new(HashMap::default(), Sha256Hasher::default());

    let dataset = fio("a", 100000, 30)?;

    let mes = fixture.measure(Box::new(RabinChunker::default()), &dataset)?;

    println!("{:?}", mes);

    Ok(())
}
