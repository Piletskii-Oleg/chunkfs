extern crate chunkfs;

use std::collections::HashMap;
use std::io;

use chunkfs::chunkers::RabinChunker;
use chunkfs::{bench, create_cdc_filesystem};
use chunkfs::hashers::SimpleHasher;

fn main() -> io::Result<()> {
    let base = HashMap::default();
    let mut fs = create_cdc_filesystem(base, SimpleHasher);

    let mut file = fs.create_file("file", RabinChunker::default())?;
    let data = bench::generator::fio(100000, 100)?;

    fs.write_from_stream(&mut file, data)?;
    let measurements = fs.close_file(file)?;

    println!("{:?}", measurements);
    println!("{}", fs.cdc_dedup_ratio());

    Ok(())
}
