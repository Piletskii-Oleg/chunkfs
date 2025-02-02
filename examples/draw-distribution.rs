extern crate serde_json;

use chunkfs::bench::generator::fio;
use chunkfs::bench::CDCFixture;
use chunkfs::chunkers::SuperChunker;
use chunkfs::hashers::Sha256Hasher;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufWriter;

fn main() -> io::Result<()> {
    let mut fixture = CDCFixture::new(HashMap::new(), Sha256Hasher::default());

    let dataset = fio("a", 100000, 30)?;

    fixture.measure(&dataset, SuperChunker::default())?;

    for adjustment in [100, 500, 1000] {
        let map = fixture.size_distribution(adjustment);

        let pairs = map.into_iter().collect::<Vec<(usize, u32)>>();
        let path = format!("distribution-{}-{}.json", dataset.name, adjustment);
        let mut writer = BufWriter::new(File::create(path)?);
        serde_json::to_writer(&mut writer, &pairs)?;
    }
    Ok(())
}
