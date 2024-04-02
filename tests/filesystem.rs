extern crate chunkfs;

use chunkfs::base::HashMapBase;
use chunkfs::chunker::{FSChunker, LeapChunker};
use chunkfs::hasher::SimpleHasher;
use chunkfs::FileSystem;

#[test]
fn write_read_complete_test() {
    let mut fs = FileSystem::new(HashMapBase::default());

    let mut handle = fs
        .create_file("file".to_string(), LeapChunker::default(), SimpleHasher)
        .unwrap();
    fs.write_to_file(&mut handle, &[1; 1024 * 1024]).unwrap();
    fs.write_to_file(&mut handle, &[1; 1024 * 1024]).unwrap();

    let measurements = fs.close_file(handle).unwrap();
    println!("{:?}", measurements);

    let handle = fs
        .open_file("file", LeapChunker::default(), SimpleHasher)
        .unwrap();
    let read = fs.read_file_complete(&handle).unwrap();
    assert_eq!(read.len(), 1024 * 1024 * 2);
    assert_eq!(read, [1; 1024 * 1024 * 2]);
}

#[test]
fn write_read_blocks_test() {
    let mut fs = FileSystem::new(HashMapBase::default());

    let mut handle = fs
        .create_file("file".to_string(), FSChunker::new(4096), SimpleHasher)
        .unwrap();
    fs.write_to_file(&mut handle, &[1; 1024 * 1024]).unwrap();
    fs.write_to_file(&mut handle, &[2; 1024 * 1024]).unwrap();
    fs.write_to_file(&mut handle, &[3; 1024 * 1024]).unwrap();
    let measurements = fs.close_file(handle).unwrap();
    println!("{:?}", measurements);

    let mut handle = fs
        .open_file("file", LeapChunker::default(), SimpleHasher)
        .unwrap();
    assert_eq!(
        fs.read_from_file(&mut handle).unwrap(),
        vec![1; 1024 * 1024]
    );
    assert_eq!(
        fs.read_from_file(&mut handle).unwrap(),
        vec![2; 1024 * 1024]
    );
    assert_eq!(
        fs.read_from_file(&mut handle).unwrap(),
        vec![3; 1024 * 1024]
    );
}
