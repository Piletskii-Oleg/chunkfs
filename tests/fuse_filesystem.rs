use chunkfs::chunkers::SuperChunker;
use chunkfs::hashers::SimpleHasher;
use chunkfs::{FuseFS, MB};
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
use uuid::Uuid;

fn generate_unique_mount_point() -> String {
    Uuid::new_v4().to_string()
}

#[test]
fn write_fuse_fs() {
    let db = HashMap::default();
    let fuse_fs = FuseFS::new(db, SimpleHasher, SuperChunker::default());
    let mount_point = generate_unique_mount_point();

    fs::create_dir_all(&mount_point).unwrap();

    let session = fuser::spawn_mount2(fuse_fs, &mount_point, &vec![]).unwrap();

    let file_path = format!("{}/{}", &mount_point, "file");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&file_path)
        .unwrap();

    let mut data1 = vec![1u8; 2000];
    let mut data2 = vec![2u8; 5000];
    file.write_all(&data1).unwrap();
    file.write_at(&data2, data1.len() as u64).unwrap();

    let mut file = OpenOptions::new().read(true).open(&file_path).unwrap();
    data1.append(&mut data2);
    let mut actual = Vec::new();
    file.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, data1);

    drop(session);
    fs::remove_dir_all(&mount_point).unwrap();
}

#[test]
fn different_data_writes() {
    let db = HashMap::default();
    let fuse_fs = FuseFS::new(db, SimpleHasher, SuperChunker::default());
    let mount_point = generate_unique_mount_point();

    fs::create_dir_all(&mount_point).unwrap();

    let session = fuser::spawn_mount2(fuse_fs, &mount_point, &vec![]).unwrap();

    let file_path = format!("{}/{}", &mount_point, "file");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&file_path)
        .unwrap();

    let mut data1 = vec![1u8; 500];
    let mut data2 = vec![2u8; 700];
    let data3 = vec![3u8; 3 * MB];
    let mut data4 = vec![4u8; 10 * MB];
    file.write_all(&data1).unwrap();
    file.write_all(&data2).unwrap();
    file.write_all(&data3).unwrap();
    file.write_all(&data4).unwrap();

    let mut file = OpenOptions::new().read(true).open(&file_path).unwrap();
    data1.append(&mut data2);
    data1.append(&mut vec![3u8; MB + 11]);
    let mut actual = vec![0u8; 500 + 700 + MB + 11];
    file.read_exact(&mut actual).unwrap();
    assert_eq!(actual, data1);
    let first_read_len = actual.len();

    let mut expected = vec![3u8; MB - 11];
    expected.append(&mut data4);
    let mut actual = vec![0u8; expected.len()];
    file.read_exact_at(&mut actual, (first_read_len + MB) as u64)
        .unwrap();
    assert_eq!(actual, expected);

    drop(session);
    fs::remove_dir_all(&mount_point).unwrap();
}
