extern crate chunkfs;

use chunkfs::base::HashMapBase;
use chunkfs::chunker::FSChunker;
use chunkfs::hasher::SimpleHasher;
use chunkfs::FileSystemBuilder;

#[test]
fn write_read_complete_test() {
    let mut fs = FileSystemBuilder::new()
        .with_chunker(FSChunker::new(4096))
        .with_hasher(SimpleHasher)
        .with_base(HashMapBase::default())
        .build()
        .unwrap();

    let mut handle = fs.create_file("file".to_string()).unwrap();
    fs.write_to_file(&mut handle, &[1; 1024 * 1024]).unwrap();
    fs.write_to_file(&mut handle, &[1; 1024 * 1024]).unwrap();

    fs.close_file(handle).unwrap();

    let handle = fs.open_file("file").unwrap();
    assert_eq!(
        fs.read_file_complete(&handle).unwrap(),
        vec![1; 1024 * 1024 * 2]
    );
}

#[test]
fn write_read_blocks_test() {
    let mut fs = FileSystemBuilder::new()
        .with_chunker(FSChunker::new(4096))
        .with_hasher(SimpleHasher)
        .with_base(HashMapBase::default())
        .build()
        .unwrap();

    let mut handle = fs.create_file("file".to_string()).unwrap();
    fs.write_to_file(&mut handle, &[1; 1024 * 1024]).unwrap();
    fs.write_to_file(&mut handle, &[2; 1024 * 1024]).unwrap();
    fs.write_to_file(&mut handle, &[3; 1024 * 1024]).unwrap();
    fs.close_file(handle).unwrap();

    let mut handle = fs.open_file("file").unwrap();
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
