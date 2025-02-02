use chunkfs::bench::generator::fio;
use chunkfs::bench::CDCFixture;
use chunkfs::chunkers::{SeqChunker, SuperChunker};
use chunkfs::hashers::Sha256Hasher;
use chunking::seq::{Config, OperationMode};
use chunking::SizeParams;
use std::collections::HashMap;
use std::io;

fn main() -> io::Result<()> {
    let mut fixture = CDCFixture::new(HashMap::default(), Sha256Hasher::default());

    let dataset = fio("a", 100000, 30)?;

    let seq = SeqChunker::new(
        OperationMode::Increasing,
        SizeParams::new(8192, 16384, 65536),
        Config::default(),
    );

    let dedup = fixture.dedup_ratio(&dataset, seq)?;
    println!("Dedup ratio: {:?}", dedup);

    let mes = fixture.measure(&dataset, SuperChunker::default())?;
    println!("One run using SuperChunker: {mes:?}");

    Ok(())
}
