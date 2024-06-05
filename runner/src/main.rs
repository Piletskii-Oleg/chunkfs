extern crate chunkfs;

use std::collections::HashMap;
use std::fmt::Debug;
use std::io;
use std::time::Instant;

use chunkfs::chunkers::{LeapChunker, RabinChunker};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::Chunker;
use chunkfs::FileSystem;
use chunkfs::Hasher;

fn main() -> io::Result<()> {
    //parametrized_write(FSChunker::new(16384), SimpleHasher)?;
    //parametrized_write(FSChunker::new(16384), Sha256Hasher::default())?;
    println!();
    //parametrized_write(LeapChunker::default(), SimpleHasher)?;
    //parametrized_write(LeapChunker::default(), Sha256Hasher::default())?;
    parametrized_write(RabinChunker::new(), Sha256Hasher::default())
}

const MB: usize = 1024 * 1024;

fn parametrized_write(
    chunker: impl Chunker + Debug,
    hasher: impl Hasher + Debug,
) -> io::Result<()> {
    println!("Current chunker: {:?}", chunker);
    println!("Current hasher: {:?}", hasher);
    let base = HashMap::default();
    let mut fs = FileSystem::new_cdc_only(base, hasher);

    let mut handle = fs.create_file("file".to_string(), chunker, true)?;

    const MB_COUNT: usize = 1024;

    let data = generate_data(MB_COUNT);
    let watch = Instant::now();
    fs.write_to_file(&mut handle, &data)?;
    let write_time = watch.elapsed();

    let measurements = fs.close_file(handle)?;
    println!(
        "Written {MB_COUNT} MB in {:.3} seconds => write speed is {:.3} MB/s",
        write_time.as_secs_f64(),
        MB_COUNT as f64 / write_time.as_secs_f64()
    );

    let speed = MB_COUNT as f64 / measurements.chunk_time().as_secs_f64();
    println!(
        "Chunked {MB_COUNT} MB in {:.3} ns => chunk speed is {:.3} MB/s",
        measurements.chunk_time().as_nanos(),
        speed
    );

    let handle = fs.open_file("file", LeapChunker::default())?;
    let watch = Instant::now();
    let read = fs.read_file_complete(&handle)?;
    let read_time = watch.elapsed().as_secs_f64();
    println!(
        "Read {MB_COUNT} MB in {:.3} seconds => chunk speed is {:.3} MB/s",
        read_time,
        MB_COUNT as f64 / read_time
    );

    assert_eq!(read.len(), data.len());
    assert_eq!(read, data);

    Ok(())
}

fn generate_data(mb_size: usize) -> Vec<u8> {
    let bytes = mb_size * MB;
    (0..bytes).map(|_| rand::random::<u8>()).collect()
}
