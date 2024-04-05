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
    let data = vec![10; 1024 * 1024];
    fs.write_to_file(&mut file, &data)?;
    let measurements = fs.close_file(file)?;
    println!("{:?}", measurements);

    let mut file = fs.open_file("file", LeapChunker::default(), SimpleHasher)?;
    let read = fs.read_from_file(&mut file)?;

    assert_eq!(read.len(), 1024 * 1024);
    assert_eq!(read, data);

    Ok(())
}
