use chunkfs::chunkers::SuperChunker;
use chunkfs::hashers::SimpleHasher;
use chunkfs::{FuseFS, MB};
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;

const MOUNT_POINT: &str = "./mount_point";

fn main() {
    let db = HashMap::default();
    let fuse_fs = FuseFS::new(db, SimpleHasher, SuperChunker::default());

    fs::create_dir_all(MOUNT_POINT).unwrap();

    let session = fuser::spawn_mount2(fuse_fs, MOUNT_POINT, &vec![]).unwrap();

    let file_path = format!("{}/{}", MOUNT_POINT, "file");
    // careful: writing is sequential only
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(&file_path)
        .unwrap();

    let data1 = vec![1u8; 2 * MB];
    let data2 = vec![2u8; 5 * MB];
    file.write_all(&data1).unwrap();
    file.write_all(&data2).unwrap();

    let expected: Vec<u8> = vec![1, 1, 1, 1, 2, 2, 2];
    let mut actual = vec![0u8; expected.len()];
    file.read_exact_at(&mut actual, 2 * MB as u64 - 4).unwrap();
    assert_eq!(expected, actual);

    file.seek(SeekFrom::Start(0)).unwrap();
    let mut file_data = vec![0u8; 7 * MB];
    let read_size = file.read_to_end(&mut file_data).unwrap();
    assert_eq!(read_size, 7 * MB);

    drop(session);
    fs::remove_dir_all(MOUNT_POINT).unwrap();
}
