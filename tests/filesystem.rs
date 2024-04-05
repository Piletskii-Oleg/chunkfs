extern crate chunkfs;

use chunkfs::base::HashMapBase;
use chunkfs::chunker::{FSChunker, LeapChunker};
use chunkfs::hasher::SimpleHasher;
use chunkfs::{FileOpener, FileSystem};

#[test]
fn write_read_complete_test() {
    let mut fs = FileSystem::new(HashMapBase::default());

    let mut handle = fs
        .create_file(
            "file".to_string(),
            LeapChunker::default(),
            SimpleHasher,
            true,
        )
        .unwrap();
    fs.write_to_file(&mut handle, &[1; 1024 * 1024]).unwrap();
    fs.write_to_file(&mut handle, &[1; 1024 * 1024]).unwrap();

    let measurements = fs.close_file(handle).unwrap();
    println!("{:?}", measurements);

    let handle = FileOpener::new()
        .with_hasher(SimpleHasher)
        .with_chunker(LeapChunker::default())
        .open(&mut fs, "file")
        .unwrap();
    let read = fs.read_file_complete(&handle).unwrap();
    assert_eq!(read.len(), 1024 * 1024 * 2);
    assert_eq!(read, [1; 1024 * 1024 * 2]);
}

#[test]
fn write_read_blocks_test() {
    let mut fs = FileSystem::new(HashMapBase::default());

    let mut handle = fs
        .create_file("file".to_string(), FSChunker::new(4096), SimpleHasher, true)
        .unwrap();

    let ones = vec![1; 1024 * 1024];
    let twos = vec![2; 1024 * 1024];
    let threes = vec![3; 1024 * 1024];
    fs.write_to_file(&mut handle, &ones).unwrap();
    fs.write_to_file(&mut handle, &twos).unwrap();
    fs.write_to_file(&mut handle, &threes).unwrap();
    let measurements = fs.close_file(handle).unwrap();
    println!("{:?}", measurements);

    let mut handle = fs
        .open_file("file", LeapChunker::default(), SimpleHasher)
        .unwrap();
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), ones);
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), twos);
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), threes);
}

//#[test]
fn two_file_handles_to_one_file() {
    let mut fs = FileSystem::new(HashMapBase::default());
    let mut handle1 = fs
        .create_file(
            "file".to_string(),
            LeapChunker::default(),
            SimpleHasher,
            true,
        )
        .unwrap();
    let mut handle2 = fs
        .open_file("file", LeapChunker::default(), SimpleHasher)
        .unwrap();
    fs.write_to_file(&mut handle1, &[1; 1024 * 1024]).unwrap();
    assert_eq!(fs.read_from_file(&mut handle2).unwrap().len(), 1024 * 1024)
}
