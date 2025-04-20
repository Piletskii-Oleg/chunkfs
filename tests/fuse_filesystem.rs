use chunkfs::chunkers::SuperChunker;
use chunkfs::hashers::SimpleHasher;
use chunkfs::{FuseFS, MB};
use fuser::BackgroundSession;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions, Permissions};
use std::io::{Read, Write};
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::Path;
use uuid::Uuid;

fn generate_unique_mount_point() -> String {
    Uuid::new_v4().to_string()
}

struct FuseFixture {
    mount_point: String,
    fuse_session: Option<BackgroundSession>,
}

impl FuseFixture {
    fn default() -> Self {
        let mount_point = generate_unique_mount_point();
        let db = HashMap::default();
        let fuse_fs = FuseFS::new(db, SimpleHasher, SuperChunker::default());
        fs::create_dir_all(&mount_point).unwrap();

        let fuse_session = fuser::spawn_mount2(fuse_fs, &mount_point, &vec![]).unwrap();

        Self {
            mount_point,
            fuse_session: Some(fuse_session),
        }
    }
}

impl Drop for FuseFixture {
    fn drop(&mut self) {
        if let Some(session) = self.fuse_session.take() {
            drop(session)
        }
        fs::remove_dir(&self.mount_point).unwrap();
    }
}

fn file_size(file: &File) -> u64 {
    file.metadata().unwrap().len()
}

#[test]
fn permissions() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    File::create(&file_path).unwrap();

    let read_ok = || {
        let mut file = OpenOptions::new().read(true).open(&file_path).unwrap();
        let mut buf = vec![];
        file.read_to_end(&mut buf).unwrap();
        assert_eq!(file_size(&file), buf.len() as u64);
    };
    let read_denied = || {
        let res = OpenOptions::new().read(true).open(&file_path);
        assert!(res.is_err());
    };
    let write_ok = || {
        let file = OpenOptions::new().write(true).open(&file_path).unwrap();
        let write_len = file.write_at(&mut vec![0; 512], file_size(&file)).unwrap();
        assert_eq!(write_len, 512);
    };
    let write_denied = || {
        let res = OpenOptions::new().write(true).open(&file_path);
        assert!(res.is_err());
    };

    let perms: Vec<_> = (0o000..=0o777).map(|m| Permissions::from_mode(m)).collect();
    for perm in perms {
        fs::set_permissions(&file_path, perm.clone()).unwrap();
        if perm.mode() & 0o400 != 0 {
            read_ok();
        } else {
            read_denied();
        }

        if perm.mode() & 0o200 != 0 {
            write_ok();
        } else {
            write_denied();
        }
    }
}

#[test]
fn write_not_to_end_fails() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let dir_path = mount_point.join("directory");
    let res = fs::create_dir(&dir_path);
    assert!(res.is_err());
}

#[test]
fn create_dir_fails() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    let mut file = File::create(&file_path).unwrap();

    file.write_all(b"Hello, Chunkfs!").unwrap();
    file.write_all(&vec![0; MB]).unwrap();

    let res1 = file.write_at(&vec![1, 2, 3], 10);
    let res2 = file.write_at(&vec![1, 2, 3], file_size(&file) + 1);
    assert!(res1.is_err());
    assert!(res2.is_err());
}

#[test]
fn filehandles_mods() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    File::create(&file_path).unwrap();

    let mut file = OpenOptions::new().write(true).open(&file_path).unwrap();
    let res = file.read(&mut vec![0; 512]);
    assert!(res.is_err());

    let file = OpenOptions::new().read(true).open(&file_path).unwrap();
    let res = file.write_at(&mut vec![0; 512], file_size(&file));
    assert!(res.is_err());

    let res = OpenOptions::new().open(&file_path);
    assert!(res.is_err());
}

#[test]
fn write_fuse_fs() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    let mut file = File::create(&file_path).unwrap();

    let mut data1 = vec![1u8; 2000];
    let mut data2 = vec![2u8; 5000];
    file.write_all(&data1).unwrap();
    file.write_at(&data2, data1.len() as u64).unwrap();

    let mut file = OpenOptions::new().read(true).open(&file_path).unwrap();
    data1.append(&mut data2);
    let mut actual = Vec::new();
    file.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, data1);
}

#[test]
fn different_data_writes() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    let mut file = File::create(&file_path).unwrap();

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
}
