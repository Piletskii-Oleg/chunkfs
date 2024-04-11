extern crate chunkfs;

use std::fmt::Debug;
use std::io;
use std::time::Instant;

use chunkfs::base::HashMapBase;
use chunkfs::chunker::{Chunker, FSChunker, LeapChunker};
use chunkfs::hasher::{Hasher, Sha256Hasher, SimpleHasher};
use chunkfs::FileSystem;

fn main() -> io::Result<()> {
    parametrized_write(FSChunker::new(16384), SimpleHasher)?;
    parametrized_write(FSChunker::new(16384), Sha256Hasher::default())?;
    println!();
    parametrized_write(LeapChunker::default(), SimpleHasher)?;
    parametrized_write(LeapChunker::default(), Sha256Hasher::default())?;
    Ok(())
}

fn parametrized_write(
    chunker: impl Chunker + Debug,
    hasher: impl Hasher + Debug,
) -> io::Result<()> {
    println!("Current chunker: {:?}", chunker);
    println!("Current hasher: {:?}", hasher);
    let base = HashMapBase::default();
    let mut fs = FileSystem::new(base);

    let mut handle = fs.create_file("file".to_string(), chunker, hasher, true)?;

    const MB_COUNT: usize = 1024;
    let data = generate_data(1024);
    let watch = Instant::now();
    for i in 0..MB_COUNT {
        fs.write_to_file(&mut handle, &data[1024 * 1024 * i..1024 * 1024 * (i + 1)])?;
    }
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
    let handle = fs.open_file("file", LeapChunker::default(), SimpleHasher)?;
    let watch = Instant::now();
    let read = fs.read_file_complete(&handle)?;
    let read_time = watch.elapsed().as_secs_f64();
    println!(
        "Read {MB_COUNT} MB in {:.3} seconds => chunk speed is {:.3} MB/s",
        read_time,
        MB_COUNT as f64 / read_time
    );

    assert_eq!(read.len(), 1024 * 1024 * MB_COUNT);
    assert_eq!(read, data);

    Ok(())
}

fn generate_data(size: usize) -> Vec<u8> {
    let bytes = size * 1024 * 1024;
    (0..bytes).map(|_| rand::random::<u8>()).collect()
}
