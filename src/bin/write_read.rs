extern crate chunkfs;

use std::fmt::Debug;
use std::io;
use std::time::Instant;

use chunkfs::base::HashMapBase;
use chunkfs::chunker::{Chunker, FSChunker, LeapChunker};
use chunkfs::hasher::SimpleHasher;
use chunkfs::FileSystem;

fn main() -> io::Result<()> {
    parametrized_write(FSChunker::new(16384))?;
    println!();
    parametrized_write(LeapChunker::default())?;
    Ok(())
}

fn parametrized_write(chunker: impl Chunker + Debug) -> io::Result<()> {
    println!("Current chunker: {:?}", chunker);
    let base = HashMapBase::default();
    let mut fs = FileSystem::new(base);

    let mut file = fs.create_file("file".to_string(), chunker, SimpleHasher, true)?;

    const MB_COUNT: usize = 1024;
    let data = generate_data(1024);
    let watch = Instant::now();
    for i in 0..MB_COUNT {
        fs.write_to_file(&mut file, &data[1024 * 1024 * i..1024 * 1024 * (i + 1)])?;
    }
    let write_time = watch.elapsed();
    let measurements = fs.close_file(file)?;
    println!(
        "Written {MB_COUNT} MB in {} seconds => write speed is {:.3} MB/s",
        write_time.as_secs_f64(),
        MB_COUNT as f64 / write_time.as_secs_f64()
    );

    let speed = MB_COUNT as f64 / measurements.chunk_time().as_secs_f64();
    println!(
        "Chunked {MB_COUNT} MB in {} ns => chunk speed is {:.3} MB/s",
        measurements.chunk_time().as_nanos(),
        speed
    );
    let file = fs.open_file("file", LeapChunker::default(), SimpleHasher)?;
    let watch = Instant::now();
    let read = fs.read_file_complete(&file)?;
    let read_time = watch.elapsed().as_secs_f64();
    println!(
        "Read {MB_COUNT} MB in {} seconds => chunk speed is {:.3} MB/s",
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
