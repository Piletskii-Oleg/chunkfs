use cdc_chunkers::SizeParams;
use chunkfs::chunkers::{LeapChunker, SuperChunker};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::{ChunkerRef, FuseFS, MB};
use filetime::FileTime;
use fuser::BackgroundSession;
use fuser::MountOption::AutoUnmount;
use libc::O_DIRECT;
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::fs::{File, OpenOptions, Permissions};
use std::io::{Read, Write};
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use uuid::Uuid;

fn generate_unique_mount_point() -> String {
    Uuid::new_v4().to_string()
}

struct FuseFixture {
    mount_point: PathBuf,
    fuse_session: Option<BackgroundSession>,
}

impl FuseFixture {
    fn default() -> Self {
        Self::with_chunker(SuperChunker::default())
    }

    fn with_chunker<C>(chunker: C) -> Self
    where
        C: Into<ChunkerRef>,
    {
        let mount_dir = Path::new("mount_dir");
        let mount_point = mount_dir.join(generate_unique_mount_point());
        let db = HashMap::default();
        let fuse_fs = FuseFS::new(db, Sha256Hasher::default(), chunker);
        fs::create_dir_all(&mount_point).unwrap();

        let fuse_session = fuser::spawn_mount2(fuse_fs, &mount_point, &[AutoUnmount]).unwrap();

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

fn to_unix_secs(time: &SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn get_metadata_times(file: &File) -> (u64, u64, u64) {
    let metadata = file.metadata().unwrap();
    (
        metadata.atime() as u64,
        metadata.mtime() as u64,
        metadata.ctime() as u64,
    )
}

#[test]
fn metadata_times() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);
    let file_path = mount_point.join("file");

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(O_DIRECT)
        .open(&file_path)
        .unwrap();
    let (atime_init, mtime_init, ctime_init) = get_metadata_times(&file);

    std::thread::sleep(std::time::Duration::from_secs(1));

    file.write_all(&vec![0; 512]).unwrap();
    let (atime1, mtime1, ctime1) = get_metadata_times(&file);
    assert!(mtime1 > mtime_init);
    assert!(ctime1 > ctime_init);
    assert_eq!(atime1, atime_init);

    std::thread::sleep(std::time::Duration::from_secs(1));

    file.read_at(&mut vec![0; 512], 0).unwrap();
    let (atime2, mtime2, ctime2) = get_metadata_times(&file);
    assert!(atime2 > atime1);
    assert_eq!(mtime2, mtime1);
    assert!(ctime2 > ctime1);
}

#[test]
fn manual_setattr() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);
    let file_path = mount_point.join("file");

    let before_creation = SystemTime::now();
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .custom_flags(O_DIRECT)
        .open(&file_path)
        .unwrap();

    let (atime1, mtime1, ctime1) = get_metadata_times(&file);
    assert_eq!(atime1, mtime1);
    assert_eq!(mtime1, ctime1);
    let before_creation_in_unix_secs = to_unix_secs(&before_creation);
    assert!(ctime1 >= before_creation_in_unix_secs);

    std::thread::sleep(std::time::Duration::from_secs(1));

    let now = SystemTime::now();
    let now_minus10s = now - std::time::Duration::from_secs(10);
    let now_minus100s = now - std::time::Duration::from_secs(100);

    let new_atime = FileTime::from_system_time(now_minus10s);
    let new_mtime = FileTime::from_system_time(now_minus100s);

    filetime::set_file_atime(&file_path, new_atime).unwrap();
    filetime::set_file_mtime(&file_path, new_mtime).unwrap();

    let (atime2, mtime2, ctime2) = get_metadata_times(&file);
    assert_eq!(atime2, to_unix_secs(&now_minus10s));
    assert_eq!(mtime2, to_unix_secs(&now_minus100s));
    assert!(ctime2 > ctime1);
}

#[test]
fn readdir() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    File::create(mount_point.join("file1")).unwrap();
    File::create(mount_point.join("file2")).unwrap();

    let mut files = vec![];
    for entry in fs::read_dir(mount_point).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        assert!(path.is_file());
        files.push(path.file_name().unwrap().to_owned());
    }
    assert!(files.contains(&OsString::from("file1")));
    assert!(files.contains(&OsString::from("file2")));
    assert_eq!(files.len(), 2)
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
        assert_eq!(
            res.unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
    };
    let write_ok = || {
        let file = OpenOptions::new().write(true).open(&file_path).unwrap();
        let write_len = file.write_at(&vec![0; 512], file_size(&file)).unwrap();
        assert_eq!(write_len, 512);
    };
    let write_denied = || {
        let res = OpenOptions::new().write(true).open(&file_path);
        assert_eq!(
            res.unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
    };

    let perms: Vec<_> = (0o000..=0o777).map(Permissions::from_mode).collect();
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
fn create_dir_fails() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let dir_path = mount_point.join("directory");
    let res = fs::create_dir(&dir_path);
    assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::ENOSYS));
}

#[test]
fn write_not_to_end_fails() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    let mut file = File::create(&file_path).unwrap();

    file.write_all(b"Hello, Chunkfs!").unwrap();
    file.write_all(&vec![0; MB]).unwrap();

    let res1 = file.write_at(&[1, 2, 3], 10);
    let res2 = file.write_at(&[1, 2, 3], file_size(&file) + 1);
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
    let res = file.write_at(&vec![0; 512], file_size(&file));
    assert!(res.is_err());

    let res = OpenOptions::new().open(&file_path);
    assert!(res.is_err());
}

#[test]
fn write_fuse_fs() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .custom_flags(O_DIRECT)
        .open(&file_path)
        .unwrap();

    let mut data1 = vec![1u8; 2000];
    let mut data2 = vec![2u8; 5000];
    file.write_all(&data1).unwrap();
    file.write_at(&data2, data1.len() as u64).unwrap();

    let mut file = OpenOptions::new()
        .custom_flags(O_DIRECT)
        .read(true)
        .open(&file_path)
        .unwrap();
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
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .custom_flags(O_DIRECT)
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

    let mut file = OpenOptions::new()
        .custom_flags(O_DIRECT)
        .read(true)
        .open(&file_path)
        .unwrap();
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

#[test]
fn read_dropped_cache() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .custom_flags(O_DIRECT)
        .write(true)
        .read(true)
        .open(&file_path)
        .unwrap();
    file.write_all(&[0; 10 * MB]).unwrap();
    file.flush().unwrap();
    file.write_all(&[1; 3 * MB]).unwrap();

    let mut actual = vec![10; 14 * MB];
    assert_eq!(file.read_at(&mut actual, 0).unwrap(), 13 * MB);
    let expected = [vec![0; 10 * MB], vec![1; 3 * MB], vec![10; MB]].concat();
    assert_eq!(
        actual, expected,
        "read entire file with dropped and dirty cache is correct"
    );

    actual = vec![10; 7 * MB];
    assert_eq!(file.read_at(&mut actual, 0).unwrap(), 7 * MB);
    assert_eq!(
        actual,
        [0; 7 * MB],
        "read dropped cache from start to end - epsilon is correct"
    );

    actual = vec![10; 12 * MB];
    assert_eq!(file.read_at(&mut actual, 0).unwrap(), 12 * MB);
    let expected = [vec![0; 10 * MB], vec![1; 2 * MB]].concat();
    assert_eq!(
        actual, expected,
        "read dropped cache from start to end + epsilon is correct"
    );

    actual = vec![10; 7 * MB];
    assert_eq!(file.read_at(&mut actual, MB as u64).unwrap(), 7 * MB);
    assert_eq!(
        actual,
        [0; 7 * MB],
        "read dropped cache from start + epsilon to end - epsilon is correct"
    );

    actual = vec![10; 10 * MB];
    assert_eq!(file.read_at(&mut actual, 7 * MB as u64).unwrap(), 6 * MB);
    let expected = [vec![0; 3 * MB], vec![1; 3 * MB], vec![10; 4 * MB]].concat();
    assert_eq!(
        actual, expected,
        "read dropped cache from start + epsilon to end + epsilon is correct"
    );
}

#[test]
fn read_cache() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .custom_flags(O_DIRECT)
        .read(true)
        .open(&file_path)
        .unwrap();
    file.write_all(&[0; 10 * MB]).unwrap();
    file.flush().unwrap();
    file.write_all(&[1; 3 * MB]).unwrap();

    let mut actual = vec![10; 15 * MB];
    assert_eq!(file.read_at(&mut actual, 0).unwrap(), 13 * MB);
    let expected = [vec![0; 10 * MB], vec![1; 3 * MB], vec![10; 2 * MB]].concat();
    assert_eq!(
        actual, expected,
        "read entire file with dropped and dirty cache is correct"
    );

    actual = vec![10; 10 * MB];
    assert_eq!(file.read_at(&mut actual, 5 * MB as u64).unwrap(), 8 * MB);
    let expected = [vec![0; 5 * MB], vec![1; 3 * MB], vec![10; 2 * MB]].concat();
    assert_eq!(
        actual, expected,
        "read cache from start - epsilon to end + epsilon is correct"
    );

    actual = vec![10; 2 * MB];
    assert_eq!(file.read_at(&mut actual, 10 * MB as u64).unwrap(), 2 * MB);
    assert_eq!(
        actual,
        [1; 2 * MB],
        "read cache from start to end - epsilon is correct"
    );

    actual = vec![10; 5 * MB];
    assert_eq!(file.read_at(&mut actual, 11 * MB as u64).unwrap(), 2 * MB);
    let expected = [vec![1; 2 * MB], vec![10; 3 * MB]].concat();
    assert_eq!(
        actual, expected,
        "read cache from start + epsilon to end + epsilon is correct"
    );

    actual = vec![10; MB];
    assert_eq!(file.read_at(&mut actual, 11 * MB as u64).unwrap(), MB);
    assert_eq!(
        actual, [1; MB],
        "read cache from start + epsilon to end - epsilon is correct"
    );
}

#[test]
fn concurrent_file_handles() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    File::create(&file_path).unwrap();

    let handle1 = OpenOptions::new()
        .append(true)
        .custom_flags(O_DIRECT)
        .open(&file_path)
        .unwrap();
    let handle2 = OpenOptions::new()
        .append(true)
        .custom_flags(O_DIRECT)
        .open(&file_path)
        .unwrap();
    let handle3 = OpenOptions::new()
        .append(true)
        .custom_flags(O_DIRECT)
        .open(&file_path)
        .unwrap();
    for _ in 0..12 {
        handle1
            .write_all_at(&vec![1; MB], file_size(&handle1))
            .unwrap();
        handle2
            .write_all_at(&vec![2; MB], file_size(&handle1))
            .unwrap();
        handle3
            .write_all_at(&vec![3; MB], file_size(&handle1))
            .unwrap();
    }
    drop(handle2);
    drop(handle3);
    drop(handle1);

    let expected = [[1; MB], [2; MB], [3; MB]].concat().repeat(12);

    let mut file = File::open(&file_path).unwrap();
    let mut actual = vec![0; 12 * 3 * MB];
    assert_eq!(file.read(&mut actual).unwrap(), 12 * 3 * MB);
    assert_eq!(actual, expected);
    assert_eq!(file.metadata().unwrap().len(), 12 * 3 * MB as u64);
}

#[test]
fn offset_change_does_not_affect_cache_drop() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .custom_flags(O_DIRECT)
        .write(true)
        .read(true)
        .open(&file_path)
        .unwrap();
    file.write_all(&[0; 3 * MB]).unwrap();
    assert_eq!(MB, file.read_at(&mut [0; MB], 2 * MB as u64).unwrap());
    drop(file);

    let mut file = OpenOptions::new()
        .custom_flags(O_DIRECT)
        .read(true)
        .append(true)
        .open(&file_path)
        .unwrap();
    file.write_all(&[0; 3 * MB]).unwrap();
    file.flush().unwrap();

    let mut actual = vec![10; 6 * MB];
    assert_eq!(file.read_at(&mut actual, 0).unwrap(), 6 * MB);
    assert_eq!(actual, [0; 6 * MB]);
    assert_eq!(file.metadata().unwrap().len(), 6 * MB as u64);
}

#[test]
fn single_chunk_read() {
    let fuse_fixture = FuseFixture::with_chunker(LeapChunker::new(SizeParams {
        min: 1000,
        avg: 1000,
        max: 1000,
    }));
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .custom_flags(O_DIRECT)
        .write(true)
        .read(true)
        .open(&file_path)
        .unwrap();

    file.write_all(&[0; 300]).unwrap();

    let mut actual = vec![10; 100];
    assert_eq!(100, file.read_at(&mut actual, 100).unwrap());
    assert_eq!(
        actual, [0; 100],
        "read from start + epsilon to end - epsilon of a single chunk is correct"
    );

    actual = vec![10; 200];
    assert_eq!(200, file.read_at(&mut actual, 100).unwrap());
    assert_eq!(
        actual, [0; 200],
        "read from start + epsilon to end of a single chunk is correct"
    );

    actual = vec![10; 1000];
    assert_eq!(200, file.read_at(&mut actual, 100).unwrap());
    assert_eq!(
        actual,
        [vec![0; 200], vec![10; 800]].concat(),
        "read from start + epsilon to end + epsilon of a single chunk is correct"
    );

    actual = vec![10; 1000];
    assert_eq!(0, file.read_at(&mut actual, 1000).unwrap());
    assert_eq!(
        actual, [10; 1000],
        "read from end to end + epsilon of a single chunk is correct"
    );
}

#[test]
fn read_first_chunk_piece() {
    let fuse_fixture = FuseFixture::with_chunker(LeapChunker::new(SizeParams {
        min: 1000,
        avg: 1000,
        max: 1000,
    }));
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .custom_flags(O_DIRECT)
        .write(true)
        .read(true)
        .open(&file_path)
        .unwrap();

    file.write_all(&[0; 1000]).unwrap();
    file.write_all(&[1; 4500]).unwrap();

    let mut actual = vec![10; 40];
    assert_eq!(40, file.read_at(&mut actual, 0).unwrap());
    assert_eq!(
        actual, [0; 40],
        "read from start to end - epsilon of first chunk is correct"
    );

    actual = vec![10; 30];
    assert_eq!(30, file.read_at(&mut actual, 40).unwrap());
    assert_eq!(
        actual, [0; 30],
        "read from start + epsilon to end - epsilon of first chunk is correct"
    );

    actual = vec![10; 960];
    assert_eq!(960, file.read_at(&mut actual, 40).unwrap());
    assert_eq!(
        actual, [0; 960],
        "read from start + epsilon to end of first chunk is correct"
    );

    actual = vec![10; 60];
    assert_eq!(60, file.read_at(&mut actual, 970).unwrap());
    assert_eq!(
        actual,
        [[0; 30], [1; 30]].concat(),
        "read from start + epsilon to end + epsilon of first chunk is correct"
    );

    actual = vec![10; 0];
    assert_eq!(0, file.read_at(&mut actual, 0).unwrap());
    assert_eq!(
        actual, [0; 0],
        "read zero bytes from start of first chunk if correct"
    );

    actual = vec![10; 0];
    assert_eq!(0, file.read_at(&mut actual, 1000).unwrap());
    assert_eq!(
        actual, [0; 0],
        "read zero bytes from end of first chunk if correct"
    );
}

#[test]
fn read_middle_chunk_piece() {
    let fuse_fixture = FuseFixture::with_chunker(LeapChunker::new(SizeParams {
        min: 1000,
        avg: 1000,
        max: 1000,
    }));
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .custom_flags(O_DIRECT)
        .write(true)
        .read(true)
        .open(&file_path)
        .unwrap();

    file.write_all(&[0; 5500]).unwrap();

    let mut actual = vec![10; 40];
    assert_eq!(40, file.read_at(&mut actual, 1040).unwrap());
    assert_eq!(
        actual, [0; 40],
        "read from start + epsilon to end - epsilon of middle chunk is correct"
    );

    actual = vec![10; 0];
    assert_eq!(0, file.read_at(&mut actual, 1040).unwrap());
    assert_eq!(
        actual, [0; 0],
        "read zero bytes from start + epsilon of middle chunk is correct"
    );
}

#[test]
fn read_last_chunk_piece() {
    let fuse_fixture = FuseFixture::with_chunker(LeapChunker::new(SizeParams {
        min: 1000,
        avg: 1000,
        max: 1000,
    }));
    let mount_point = Path::new(&fuse_fixture.mount_point);

    let file_path = mount_point.join("file");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .custom_flags(O_DIRECT)
        .write(true)
        .read(true)
        .open(&file_path)
        .unwrap();

    file.write_all(&[0; 5000]).unwrap();
    file.write_all(&[1; 500]).unwrap();

    let mut actual = vec![10; 40];
    assert_eq!(40, file.read_at(&mut actual, 5000).unwrap());
    assert_eq!(
        actual, [1; 40],
        "read from start to end - epsilon of last chunk is correct"
    );

    actual = vec![10; 30];
    assert_eq!(30, file.read_at(&mut actual, 5010).unwrap());
    assert_eq!(
        actual, [1; 30],
        "read from start + epsilon to end - epsilon of last chunk is correct"
    );

    actual = vec![10; 470];
    assert_eq!(470, file.read_at(&mut actual, 5030).unwrap());
    assert_eq!(
        actual, [1; 470],
        "read from start + epsilon to end of last chunk is correct"
    );

    actual = vec![10; 500];
    assert_eq!(470, file.read_at(&mut actual, 5030).unwrap());
    assert_eq!(
        actual,
        [vec![1; 470], vec![10; 30]].concat(),
        "read from start + epsilon to end + epsilon of last chunk is correct"
    );

    actual = vec![10; 530];
    assert_eq!(500, file.read_at(&mut actual, 5000).unwrap());
    assert_eq!(
        actual,
        [vec![1; 500], vec![10; 30]].concat(),
        "read from start to end + epsilon of last chunk is correct"
    );

    actual = vec![10; 0];
    assert_eq!(0, file.read_at(&mut actual, 5000).unwrap());
    assert_eq!(
        actual, [0; 0],
        "read zero bytes from start of last chunk if correct"
    );

    actual = vec![10; 0];
    assert_eq!(0, file.read_at(&mut actual, 5500).unwrap());
    assert_eq!(
        actual, [0; 0],
        "read zero bytes from end of last chunk if correct"
    );

    actual = vec![10; 40];
    assert_eq!(0, file.read_at(&mut actual, 5600).unwrap());
    assert_eq!(
        actual, [10; 40],
        "read n bytes from end + epsilon of last chunk if correct"
    );
}

#[test]
fn lookup_permission() {
    let fuse_fixture = FuseFixture::default();
    let mount_point = Path::new(&fuse_fixture.mount_point);
    File::create(mount_point.join("file1")).unwrap();
    File::create(mount_point.join("file2")).unwrap();

    let lookup_ok = || {
        let mut files = vec![];

        for entry in fs::read_dir(mount_point).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            assert!(path.is_file());
            let _ = entry.metadata().unwrap();
            files.push(path.file_name().unwrap().to_str().unwrap().to_string());
        }
        assert!(files.contains(&"file1".to_string()));
        assert!(files.contains(&"file2".to_string()));
        assert_eq!(files.len(), 2);
    };
    let lookup_denied = || {
        for entry in fs::read_dir(mount_point).unwrap() {
            let metadata = entry.unwrap().metadata();
            assert_eq!(
                metadata.unwrap_err().kind(),
                std::io::ErrorKind::PermissionDenied
            );
        }
    };

    let perms: Vec<_> = (0o000..=0o777).map(Permissions::from_mode).collect();
    for perm in perms {
        // reading mount directory is handled by upper filesystem
        if perm.mode() & 0o400 == 0 {
            continue;
        }

        fs::set_permissions(mount_point, perm.clone()).unwrap();
        if perm.mode() & 0o100 != 0 {
            lookup_ok();
        } else {
            lookup_denied();
        }
    }
}
