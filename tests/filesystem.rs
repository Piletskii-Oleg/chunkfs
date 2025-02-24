extern crate chunkfs;

use std::collections::HashMap;
use std::io;
use std::io::{Seek, Write};

use approx::assert_relative_eq;

use chunkfs::chunkers::{FSChunker, LeapChunker, SuperChunker};
use chunkfs::hashers::{Sha256Hasher, SimpleHasher};
use chunkfs::{create_cdc_filesystem, ChunkerRef, DataContainer, Database, WriteMeasurements};

const MB: usize = 1024 * 1024;

#[test]
fn write_read_complete_test() {
    let mut fs = create_cdc_filesystem(HashMap::default(), SimpleHasher);

    let mut handle = fs.create_file("file", LeapChunker::default()).unwrap();
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
    let mut fs = create_cdc_filesystem(HashMap::default(), SimpleHasher);

    let mut handle = fs.create_file("file", FSChunker::new(4096)).unwrap();

    let ones = vec![1; MB];
    let twos = vec![2; MB];
    let threes = vec![3; MB];
    let extra = vec![3; 50];

    fs.write_to_file(&mut handle, &ones).unwrap();
    fs.write_to_file(&mut handle, &twos).unwrap();
    fs.write_to_file(&mut handle, &threes).unwrap();
    fs.write_to_file(&mut handle, &extra).unwrap();
    fs.close_file(handle).unwrap();

    let complete = ones
        .into_iter()
        .chain(twos.into_iter())
        .chain(threes.into_iter())
        .chain(extra.into_iter())
        .collect::<Vec<_>>();

    let mut handle = fs.open_file("file", LeapChunker::default()).unwrap();
    let mut buffer = Vec::with_capacity(MB * 3 + 50);
    for _ in 0..4 {
        let buf = fs.read_from_file(&mut handle).unwrap();
        buffer.extend_from_slice(&buf);
    }
    assert_eq!(buffer.len(), MB * 3 + 50);
    assert!(complete == buffer);
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), []);
}

#[test]
fn read_file_with_size_less_than_1mb() {
    let mut fs = create_cdc_filesystem(HashMap::default(), SimpleHasher);

    let mut handle = fs.create_file("file", FSChunker::new(4096)).unwrap();

    let ones = vec![1; 10];
    fs.write_to_file(&mut handle, &ones).unwrap();
    let measurements = fs.close_file(handle).unwrap();
    println!("{:?}", measurements);

    let mut handle = fs.open_file_readonly("file").unwrap();
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), ones);
}

#[test]
fn write_read_big_file_at_once() {
    let mut fs = create_cdc_filesystem(HashMap::default(), SimpleHasher);

    let mut handle = fs.create_file("file", FSChunker::new(4096)).unwrap();

    let data = vec![1; 3 * MB + 50];
    fs.write_to_file(&mut handle, &data).unwrap();
    fs.close_file(handle).unwrap();

    let handle = fs.open_file("file", LeapChunker::default()).unwrap();
    assert_eq!(fs.read_file_complete(&handle).unwrap().len(), data.len());
}

#[test]
fn scrub_compiles_on_cdc_map_but_returns_error() {
    let mut fs = create_cdc_filesystem(HashMap::default(), SimpleHasher);
    let result = fs.scrub();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput)
}

#[test]
fn two_file_handles_to_one_file() {
    let mut fs = create_cdc_filesystem(HashMap::default(), SimpleHasher);
    let mut handle1 = fs.create_file("file", LeapChunker::default()).unwrap();
    let mut handle2 = fs.open_file("file", LeapChunker::default()).unwrap();
    fs.write_to_file(&mut handle1, &[1; MB]).unwrap();
    fs.close_file(handle1).unwrap();
    assert_eq!(fs.read_file_complete(&mut handle2).unwrap().len(), MB)
}

#[test]
fn non_iterable_database_can_be_used_with_fs() {
    struct DummyDatabase;

    impl Database<Vec<u8>, DataContainer<()>> for DummyDatabase {
        fn insert(&mut self, _key: Vec<u8>, _value: DataContainer<()>) -> std::io::Result<()> {
            unimplemented!()
        }

        fn get(&self, _key: &Vec<u8>) -> std::io::Result<DataContainer<()>> {
            unimplemented!()
        }

        fn contains(&self, _key: &Vec<u8>) -> bool {
            unimplemented!()
        }
    }

    let _ = create_cdc_filesystem(DummyDatabase, SimpleHasher);
}

#[test]
fn dedup_ratio_is_correct_for_fixed_size_chunker() {
    let mut fs = create_cdc_filesystem(HashMap::new(), SimpleHasher);

    const MB: usize = 1024 * 1024;
    const CHUNK_SIZE: usize = 4096;

    let data = vec![10; MB];

    // first write => 1 MB, 1 chunk
    let mut fh = fs.create_file("file", FSChunker::new(CHUNK_SIZE)).unwrap();
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

#[test]
fn different_chunkers_from_vec_can_be_used_with_same_filesystem() {
    let mut fs = create_cdc_filesystem(HashMap::new(), Sha256Hasher::default());
    let chunkers: Vec<ChunkerRef> = vec![
        SuperChunker::default().into(),
        LeapChunker::default().into(),
    ];

    let data = vec![0; 1024 * 1024];
    for chunker in chunkers {
        let name = format!("file-{chunker:?}");
        let mut fh = fs.create_file(&name, chunker).unwrap();
        fs.write_to_file(&mut fh, &data).unwrap();
        fs.close_file(fh).unwrap();

        let fh = fs.open_file(&name, FSChunker::default()).unwrap();
        let read = fs.read_file_complete(&fh).unwrap();

        assert_eq!(read.len(), data.len());
        //assert_eq!(read, data);
    }
}

#[test]
fn readonly_file_handle_cannot_write_can_read() {
    let mut fs = create_cdc_filesystem(HashMap::new(), SimpleHasher);
    let mut fh = fs.create_file("file", FSChunker::default()).unwrap();
    fs.write_to_file(&mut fh, &[1; MB]).unwrap();
    fs.close_file(fh).unwrap();

    // cannot write
    let mut ro_fh = fs.open_file_readonly("file").unwrap();
    let result = fs.write_to_file(&mut ro_fh, &[1; MB]);
    assert!(result.is_err());
    assert!(result.is_err_and(|e| e.kind() == io::ErrorKind::PermissionDenied));

    // can read complete
    let read = fs.read_file_complete(&ro_fh).unwrap();
    assert_eq!(read.len(), MB);
    assert_eq!(read, [1; MB]);

    let _ = fs.read_from_file(&mut ro_fh).unwrap();

    // can close
    let measurements = fs.close_file(ro_fh).unwrap();
    assert_eq!(measurements, WriteMeasurements::default())
}

#[test]
fn write_from_stream_slice() {
    let mut fs = create_cdc_filesystem(HashMap::new(), SimpleHasher);
    let mut fh = fs.create_file("file", FSChunker::default()).unwrap();
    fs.write_from_stream(&mut fh, &[1; MB * 2][..]).unwrap();
    fs.close_file(fh).unwrap();

    let ro_fh = fs.open_file_readonly("file").unwrap();
    let read = fs.read_file_complete(&ro_fh).unwrap();
    assert_eq!(read.len(), MB * 2);
    assert_eq!(fs.read_file_complete(&ro_fh).unwrap(), vec![1; MB * 2]);
}

#[test]
fn write_from_stream_buf_reader() {
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&[1; MB]).unwrap();
    file.seek(io::SeekFrom::Start(0)).unwrap();

    let mut fs = create_cdc_filesystem(HashMap::new(), SimpleHasher);
    let mut fh = fs.create_file("file", FSChunker::default()).unwrap();

    fs.write_from_stream(&mut fh, file).unwrap();
    fs.close_file(fh).unwrap();

    let ro_fh = fs.open_file_readonly("file").unwrap();
    let read = fs.read_file_complete(&ro_fh).unwrap();
    assert_eq!(read.len(), MB);
    assert_eq!(read, [1; MB]);
}
