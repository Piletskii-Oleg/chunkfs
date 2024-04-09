extern crate chunkfs;

use std::io;

use chunkfs::base::HashMapBase;
use chunkfs::chunker::LeapChunker;
use chunkfs::hasher::SimpleHasher;
use chunkfs::FileSystem;

fn main() -> io::Result<()> {
    let base = HashMapBase::default();
    let mut fs = FileSystem::new(base);

    let mut file = fs.create_file(
        "file".to_string(),
        LeapChunker::default(),
        SimpleHasher,
        true,
    )?;

    const MB_COUNT: usize = 1024 * 3;
    let data = vec![10; 1024 * 1024];
    for _ in 0..MB_COUNT {
        fs.write_to_file(&mut file, &data)?;
    }
    let measurements = fs.close_file(file)?;
    println!("{:?}", measurements);

    let speed = MB_COUNT as f64 / measurements.chunk_time().as_nanos() as f64;
    println!(
        "chunked {MB_COUNT} MB: {:.3} MB/s",
        speed * 1000.0 * 1000000.0
    );
    let mut file = fs.open_file("file", LeapChunker::default(), SimpleHasher)?;
    let read = fs.read_file_complete(&mut file)?;

    assert_eq!(read.len(), 1024 * 1024 * MB_COUNT);
    assert_eq!(read, vec![10; 1024 * 1024 * MB_COUNT]);

    Ok(())
}
