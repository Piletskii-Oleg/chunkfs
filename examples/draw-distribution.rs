extern crate serde_json;

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufWriter;
use itertools::Itertools;
use chunkfs::bench::{CDCFixture, Dataset};
use chunkfs::bench::generator::fio;
use chunkfs::chunkers::SuperChunker;
use chunkfs::hashers::Sha256Hasher;

fn main() -> io::Result<()> {
    let mut fixture = CDCFixture::new(HashMap::new(), Sha256Hasher::default());

    let dataset = Dataset::new("kernel.tar", "kernel")?;

    fixture.measure::<SuperChunker>(&dataset)?;

    for adjustment in [100, 500, 1000] {
        let map = fixture.size_distribution(adjustment);

        // println!("{:#?}", map.iter().sorted());

        let pairs = map.into_iter().collect::<Vec<(usize, u32)>>();
        let path = format!("distribution-{}-{}.json", dataset.name, adjustment);
        let mut writer = BufWriter::new(File::create(path)?);
        serde_json::to_writer(&mut writer, &pairs)?;
        // println!("{}", fixture.fs.cdc_dedup_ratio());
    }
    Ok(())
}