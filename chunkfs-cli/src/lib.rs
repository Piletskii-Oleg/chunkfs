pub mod cli;

use chunkfs::bench::{CDCFixture, Dataset};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::ChunkerRef;
use itertools::iproduct;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufWriter;
use std::time::Duration;

pub fn measure_datasets(datasets: &[Dataset], chunkers: &[ChunkerRef]) -> io::Result<()> {
    let mut fixture = CDCFixture::new(HashMap::default(), Sha256Hasher::default());

    for (dataset, chunker) in iproduct!(datasets.iter(), chunkers.iter()) {
        fixture.fs.clear_database()?;

        println!("{chunker:?}, {}", dataset.name);

        let measurement = fixture.measure(dataset, chunker.clone())?;
        measurement.write_to_csv("measurements.csv")?;

        for adjustment in [100, 500, 1000] {
            let map = fixture.size_distribution(adjustment);

            save_distribution(&dataset.name, chunker, adjustment, map)?;
        }

        std::thread::sleep(Duration::from_secs(10));
    }

    Ok(())
}

pub fn save_distribution(
    name: &str,
    chunker: &ChunkerRef,
    adjustment: usize,
    map: HashMap<usize, u32>,
) -> io::Result<()> {
    let pairs = map.into_iter().collect::<Vec<(usize, u32)>>();

    let chunker_name = format!("{:?}", chunker)
        .split(",")
        .next()
        .unwrap()
        .to_string();

    let path = format!(
        "distributions/distribution-{}-{}-{}.json",
        name, chunker_name, adjustment
    );

    let mut writer = BufWriter::new(File::create(path)?);
    serde_json::to_writer(&mut writer, &pairs)?;
    Ok(())
}
