use chunkfs::bench::generator::fio;
use chunkfs::bench::CDCFixture;
use chunkfs::chunkers::{LeapChunker, RabinChunker, SuperChunker};
use chunkfs::hashers::Sha256Hasher;
use std::collections::HashMap;
use std::io;

fn main() -> io::Result<()> {
    let mut fixture = CDCFixture::new(HashMap::default(), Sha256Hasher::default());

    let dataset = fio("a", 10000, 30)?;

    //let measurements = fixture.measure_multi(FSChunker::default().into(), &dataset, 100)?;
    //let avg = avg_measurement(measurements);
    //println!("Avg measurements for FSChunker: {:?}", avg);

    // let measurements = fixture.measure_multi::<SuperChunker>(&dataset, 100)?;
    // let avg = avg_measurement(measurements);
    // println!("Avg measurements for SuperChunker: {:?}", avg);

    let dedup = fixture.dedup_ratio::<SuperChunker>(&dataset)?;
    println!("Dedup ratio: {:?}", dedup);
    let dedup = fixture.dedup_ratio::<LeapChunker>(&dataset)?;
    println!("Dedup ratio: {:?}", dedup);
    let dedup = fixture.dedup_ratio::<RabinChunker>(&dataset)?;
    println!("Dedup ratio: {:?}", dedup);
    let dedup = fixture.dedup_ratio::<SuperChunker>(&dataset)?;
    println!("Dedup ratio: {:?}", dedup);

    Ok(())
}
