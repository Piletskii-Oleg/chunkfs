extern crate chunkfs;

use std::collections::HashMap;
use std::io::ErrorKind;

use approx::assert_relative_eq;

use chunkfs::chunkers::{FSChunker, LeapChunker};
use chunkfs::hashers::SimpleHasher;
use chunkfs::{DataContainer, Database, FileSystem};

const MB: usize = 1024 * 1024;

#[test]
fn write_read_complete_test() {
    let mut fs = FileSystem::new_with_key(HashMap::default(), SimpleHasher, 0);

    let mut handle = fs
        .create_file("file", LeapChunker::default(), true)
        .unwrap();
    fs.write_to_file(&mut handle, &[1; MB]).unwrap();
    fs.write_to_file(&mut handle, &[1; MB]).unwrap();

    let measurements = fs.close_file(handle).unwrap();
    println!("{:?}", measurements);

    let handle = fs.open_file("file", LeapChunker::default()).unwrap();
    let read = fs.read_file_complete(&handle).unwrap();
    assert_eq!(read.len(), MB * 2);
    assert_eq!(read, [1; MB * 2]);
}

#[test]
fn write_read_blocks_test() {
    let mut fs = FileSystem::new_with_key(HashMap::default(), SimpleHasher, 0);

    let mut handle = fs.create_file("file", FSChunker::new(4096), true).unwrap();

    let ones = vec![1; MB];
    let twos = vec![2; MB];
    let threes = vec![3; MB];
    fs.write_to_file(&mut handle, &ones).unwrap();
    fs.write_to_file(&mut handle, &twos).unwrap();
    fs.write_to_file(&mut handle, &threes).unwrap();
    let measurements = fs.close_file(handle).unwrap();
    println!("{:?}", measurements);

    let mut handle = fs.open_file("file", LeapChunker::default()).unwrap();
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), ones);
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), twos);
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), threes);
}

#[test]
fn read_file_with_size_less_than_1mb() {
    let mut fs = FileSystem::new_with_key(HashMap::default(), SimpleHasher, 0);

    let mut handle = fs.create_file("file", FSChunker::new(4096), true).unwrap();

    let ones = vec![1; 10];
    fs.write_to_file(&mut handle, &ones).unwrap();
    let measurements = fs.close_file(handle).unwrap();
    println!("{:?}", measurements);

    let mut handle = fs.open_file("file", LeapChunker::default()).unwrap();
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), ones);
}

#[test]
fn write_read_big_file_at_once() {
    let mut fs = FileSystem::new_with_key(HashMap::default(), SimpleHasher, 0);

    let mut handle = fs.create_file("file", FSChunker::new(4096), true).unwrap();

    let data = vec![1; 3 * MB + 50];
    fs.write_to_file(&mut handle, &data).unwrap();
    fs.close_file(handle).unwrap();

    let handle = fs.open_file("file", LeapChunker::default()).unwrap();
    assert_eq!(fs.read_file_complete(&handle).unwrap().len(), data.len());
}

#[test]
fn scrub_compiles_on_cdc_map_but_returns_error() {
    let mut fs = FileSystem::new_with_key(HashMap::default(), SimpleHasher, 0);
    let result = fs.scrub();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput)
}

#[test]
fn two_file_handles_to_one_file() {
    let mut fs = FileSystem::new_with_key(HashMap::default(), SimpleHasher, 0);
    let mut handle1 = fs
        .create_file("file", LeapChunker::default(), true)
        .unwrap();
    let mut handle2 = fs.open_file("file", LeapChunker::default()).unwrap();
    fs.write_to_file(&mut handle1, &[1; MB]).unwrap();
    fs.close_file(handle1).unwrap();
    assert_eq!(fs.read_from_file(&mut handle2).unwrap().len(), MB)
}

#[test]
fn non_iterable_database_can_be_used_with_fs() {
    struct EmptyDatabase;

    impl Database<Vec<u8>, DataContainer<()>> for EmptyDatabase {
        fn insert(&mut self, _key: Vec<u8>, _value: DataContainer<()>) -> std::io::Result<()> {
            unimplemented!()
        }

        fn get(&self, _key: &Vec<u8>) -> std::io::Result<DataContainer<()>> {
            unimplemented!()
        }

        fn remove(&mut self, _key: &Vec<u8>) {
            unimplemented!()
        }

        fn contains(&self, _key: &Vec<u8>) -> bool {
            unimplemented!()
        }
    }

    let _ = FileSystem::new(EmptyDatabase, SimpleHasher);
}

#[test]
fn dedup_ratio_is_correct_for_fixed_size_chunker() {
    let mut fs = FileSystem::new_with_key(HashMap::new(), SimpleHasher, 9);

    const MB: usize = 1024 * 1024;
    const CHUNK_SIZE: usize = 4096;

    let data = vec![10; MB];

    // first write => 1 MB, 1 chunk
    let mut fh = fs
        .create_file("file", FSChunker::new(CHUNK_SIZE), true)
        .unwrap();
    fs.write_to_file(&mut fh, &data).unwrap();
    fs.close_file(fh).unwrap();
    assert_relative_eq!(fs.cdc_dedup_ratio(), MB as f64 / CHUNK_SIZE as f64);

    // second write, same data => 2 MBs, 1 chunk
    let mut fh = fs.open_file("file", FSChunker::new(CHUNK_SIZE)).unwrap();
    fs.write_to_file(&mut fh, &data).unwrap();
    fs.close_file(fh).unwrap();
    assert_relative_eq!(fs.cdc_dedup_ratio(), (2 * MB) as f64 / CHUNK_SIZE as f64);

    // third write, different data => 3 MBs, 2 chunks
    let new_data = vec![20; MB];
    let mut fh = fs.open_file("file", FSChunker::new(CHUNK_SIZE)).unwrap();
    fs.write_to_file(&mut fh, &new_data).unwrap();
    fs.close_file(fh).unwrap();

    assert_relative_eq!(
        fs.cdc_dedup_ratio(),
        (3 * MB) as f64 / (CHUNK_SIZE * 2) as f64
    );
}
