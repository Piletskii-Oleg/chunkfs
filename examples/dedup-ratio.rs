use std::collections::HashMap;
use std::io;

use chunkfs::bench::{CDCFixture, Dataset};
use chunkfs::chunkers::SuperChunker;
use chunkfs::hashers::Sha256Hasher;
use chunkfs::MB;

fn main() -> io::Result<()> {
    let mut fixture = CDCFixture::new(HashMap::default(), Sha256Hasher::default());

    let mut handle = fixture.fs.create_file("file", SuperChunker::default())?;
    fixture.fs.write_to_file(&mut handle, &[3; 100 * MB])?;
    fixture.fs.close_file(handle)?;

    let new = fixture.fs.get_to_dedup_ratio("file", 3.0)?;
    fixture.fs.write_file_to_disk(&new, "../new")?;

    let dataset = Dataset::new("../new", "dataset")?;
    let measurement = fixture.measure(&dataset, SuperChunker::default())?;
    println!("measurement: {:?}", measurement);
    println!("dedup ratio: {}", fixture.fs.cdc_dedup_ratio());
    Ok(())
}