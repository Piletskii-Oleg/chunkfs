use chunkfs::chunkers::SuperChunker;
use chunkfs::hashers::SimpleHasher;
use chunkfs::FuseFS;
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;

const MOUNT_POINT: &str = "./mount_point";

#[test]
fn write_fuse_fs() {
    let db = HashMap::default();
    let fuse_fs = FuseFS::new(db, SimpleHasher, SuperChunker::default());

    fs::create_dir_all(MOUNT_POINT).unwrap();

    let session = fuser::spawn_mount2(fuse_fs, MOUNT_POINT, &vec![]).unwrap();

    let file_path = format!("{}/{}", MOUNT_POINT, "file");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&file_path)
        .unwrap();

    let mut data1 = vec![1u8; 2000];
    let mut data2 = vec![2u8; 5000];
    file.write(&data1).unwrap();
    file.write_at(&data2, data1.len() as u64).unwrap();

    let mut file = OpenOptions::new().read(true).open(&file_path).unwrap();
    data1.append(&mut data2);
    let mut actual = Vec::new();
    file.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, data1);

    drop(session);

    fs::remove_dir_all(MOUNT_POINT).unwrap();
}
