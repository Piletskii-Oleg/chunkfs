extern crate serde_json;

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufWriter;
use itertools::Itertools;
use chunkfs::bench::CDCFixture;
use chunkfs::bench::generator::fio;
use chunkfs::chunkers::SuperChunker;
use chunkfs::hashers::Sha256Hasher;

fn main() -> io::Result<()> {
    let mut fixture = CDCFixture::new(HashMap::new(), Sha256Hasher::default());

    let dataset = fio("a", 10000, 30)?;

    fixture.measure::<SuperChunker>(&dataset)?;

    let map = fixture.size_distribution(100);

    println!("{:#?}", map.iter().sorted());

    let mut writer = BufWriter::new(File::create("distribution.json")?);
    serde_json::to_writer(&mut writer, &map)?;
    println!("{}", fixture.fs.cdc_dedup_ratio());

    Ok(())
}