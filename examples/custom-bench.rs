use chunkfs::bench::generator::fio;
use chunkfs::bench::{CDCFixture, Dataset};
use chunkfs::chunkers::SeqChunker;
use chunkfs::hashers::Sha256Hasher;
use chunking::seq::{Config, OperationMode};
use chunking::SizeParams;
use std::collections::HashMap;
use std::io;

fn main() -> io::Result<()> {
    let mut fixture = CDCFixture::new(HashMap::default(), Sha256Hasher::default());

    let dataset = fio("a", 100000, 30)?;

    //let measurements = fixture.measure_multi(FSChunker::default().into(), &dataset, 100)?;
    //let avg = avg_measurement(measurements);
    //println!("Avg measurements for FSChunker: {:?}", avg);

    // let measurements = fixture.measure_multi::<SuperChunker>(&dataset, 100)?;
    // let avg = avg_measurement(measurements);
    // println!("Avg measurements for SuperChunker: {:?}", avg);

    let kernel = Dataset::new("kernel.tar", "kernel")?;

    let seq = SeqChunker::new(OperationMode::Increasing, SizeParams::new(8192, 16384, 65536), Config::default());
    let dedup = fixture.dedup_ratio(&kernel, seq.into())?;
    println!("Dedup ratio: {:?}", dedup);

    let mes = fixture.measure::<SeqChunker>(&kernel)?;
    println!("{mes:?}");

    Ok(())
}
