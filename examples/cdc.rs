use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{Read as _, Seek as _};

use chunkfs::chunkers::LeapChunker;
use chunkfs::fio::generate_with_fio;
use chunkfs::hashers::Sha256Hasher;
use chunkfs::{Chunker, FileSystem};

fn main() -> io::Result<()> {
    let base = HashMap::default();
    let mut fs = FileSystem::new_with_key(base, Sha256Hasher::default(), 0);

    let mut file = fs.create_file("file", LeapChunker::default())?;

    const SIZE: usize = 8192 * 100;
    const KB: usize = 1024;

    let mut reader = generate_with_fio(SIZE, 10)?;
    fs.write_from_stream(&mut file, &mut reader)?;
    let measurements = fs.close_file(file)?;
    println!("{:?}", measurements);

    println!("{}", fs.cdc_dedup_ratio());

    let file = fs.open_file_readonly("file")?;
    let read = fs.read_file_complete(&file)?;

    assert_eq!(read.len(), SIZE * KB);
    let mut buffer = Vec::with_capacity(SIZE * KB);
    reader.seek(io::SeekFrom::Start(0))?;
    reader.read_to_end(&mut buffer)?;

    assert_eq!(read, buffer);

    Ok(())
}
