use chunkfs::bench::generator::fio;
use chunkfs::bench::{avg_measurement, CDCFixture};
use chunkfs::chunkers::SuperChunker;
use chunkfs::hashers::Sha256Hasher;
use std::collections::HashMap;
use std::io;

fn main() -> io::Result<()> {
    let mut fixture = CDCFixture::new(HashMap::default(), Sha256Hasher::default());

    let dataset = fio("a", 100000, 30)?;

    //let measurements = fixture.measure_multi(FSChunker::default().into(), &dataset, 100)?;
    //let avg = avg_measurement(measurements);
    //println!("Avg measurements for FSChunker: {:?}", avg);

    let measurements = fixture.measure_multi(SuperChunker::default().into(), &dataset, 100)?;
    let avg = avg_measurement(measurements);
    println!("Avg measurements for SuperChunker: {:?}", avg);

    Ok(())
}
