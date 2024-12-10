use chunkfs::chunkers::SuperChunker;
use chunkfs::fio::generate_with_fio;
use chunkfs::hashers::Sha256Hasher;
use chunkfs::FileSystem;
use std::collections::HashMap;
use std::io;
use std::io::{BufReader, Read, Seek};

fn main() -> io::Result<()> {
    let mut fs = FileSystem::new_with_scrubber(
        HashMap::default(),
        HashMap::default(),
        Box::new(chunkfs::CopyScrubber),
        Sha256Hasher::default(),
    );

    const SIZE: usize = 8192 * 100;
    const KB: usize = 1024;

    let mut handle = fs.create_file("file", SuperChunker::default())?;
    let mut file = BufReader::new(generate_with_fio(SIZE, 30)?);
    fs.write_from_stream(&mut handle, &mut file)?;
    fs.close_file(handle)?;

    let res = fs.scrub()?;
    println!("{res:?}");

    let handle = fs.open_file_readonly("file")?;
    let read = fs.read_file_complete(&handle)?;

    assert_eq!(read.len(), SIZE * KB);
    let mut buffer = Vec::with_capacity(SIZE * KB);
    file.seek(io::SeekFrom::Start(0))?;
    file.read_to_end(&mut buffer)?;

    assert_eq!(read, buffer);

    Ok(())
}
