use cdc_chunkers::SizeParams;
use chunkfs::chunkers::{LeapChunker, RabinChunker, SuperChunker, UltraChunker};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::{ChunkerRef, FuseFS, MB};
use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput};
use fuser::MountOption::AutoUnmount;
use libc::O_DIRECT;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

const SAMPLE_SIZE: usize = 30;

#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
enum Algorithms {
    Rabin,
    Leap,
    Super,
    Ultra,
}

#[allow(dead_code)]
fn chunkers() -> Vec<Algorithms> {
    vec![
        Algorithms::Rabin,
        Algorithms::Leap,
        Algorithms::Super,
        Algorithms::Ultra,
    ]
}

#[allow(dead_code)]
fn get_chunker(algorithm: Algorithms, params: SizeParams) -> ChunkerRef {
    match algorithm {
        Algorithms::Rabin => RabinChunker::new(params).into(),
        Algorithms::Leap => LeapChunker::new(params).into(),
        Algorithms::Super => UltraChunker::new(params).into(),
        Algorithms::Ultra => SuperChunker::new(params).into(),
    }
}

#[allow(dead_code)]
fn get_default_sizes(algorithm: Algorithms) -> SizeParams {
    match algorithm {
        Algorithms::Rabin => SizeParams::rabin_default(),
        Algorithms::Leap => SizeParams::leap_default(),
        Algorithms::Super => SizeParams::super_default(),
        Algorithms::Ultra => SizeParams::ultra_default(),
    }
}

struct Dataset {
    filename: String,
    size: u64,
}

pub fn bench(c: &mut Criterion) {
    let dataset1_len = File::open("archX4.tar").unwrap().metadata().unwrap().len();
    let dataset1 = Dataset {
        filename: "archX4.tar".to_string(),
        size: dataset1_len,
    };
    let datasets = vec![dataset1];

    for dataset in datasets {
        let mut group = c.benchmark_group("FuseChunkers");
        group.sample_size(SAMPLE_SIZE);
        group.throughput(Throughput::Bytes(dataset.size));

        for chunker in chunkers() {
            let params = get_default_sizes(chunker);
            bench_write(&dataset, &mut group, chunker, params);
        }

        for chunker in chunkers() {
            let params = get_default_sizes(chunker);
            bench_read(&dataset, &mut group, chunker, params);
        }
    }
}

fn bench_write(
    dataset: &Dataset,
    group: &mut BenchmarkGroup<WallTime>,
    algorithm: Algorithms,
    params: SizeParams,
) {
    let bench_name = dataset.filename.clone();
    let parameter = format!("write_fuse-{:?}-{}", algorithm, params);
    group.bench_function(BenchmarkId::new(bench_name, parameter), |b| {
        b.iter_batched(
            || {
                let mount_point = Path::new("mount_dir/mount_point");
                let db = HashMap::default();
                let chunker = get_chunker(algorithm, params);
                let fuse_fs = FuseFS::new(db, Sha256Hasher::default(), chunker);

                fs::create_dir_all(mount_point).unwrap();
                let session = fuser::spawn_mount2(fuse_fs, mount_point, &[AutoUnmount]).unwrap();

                let fuse_path = mount_point.join("file");
                let fuse_file = OpenOptions::new()
                    .write(true)
                    .read(true)
                    .create(true)
                    .custom_flags(O_DIRECT)
                    .truncate(true)
                    .open(&fuse_path)
                    .unwrap();

                let source = File::open(&dataset.filename).unwrap();

                (session, source, fuse_file)
            },
            |(_session, mut source, mut fuse_file)| {
                let mut buf = vec![0; 50 * MB];
                loop {
                    let bytes_read = source.read(&mut buf).unwrap();
                    if bytes_read == 0 {
                        break;
                    }
                    fuse_file.write_all(&buf[..bytes_read]).unwrap();
                }
                drop(fuse_file);
            },
            BatchSize::PerIteration,
        )
    });
}

fn bench_read(
    dataset: &Dataset,
    group: &mut BenchmarkGroup<WallTime>,
    algorithm: Algorithms,
    params: SizeParams,
) {
    let mount_point = Path::new("mount_dir/mount_point");
    let db = HashMap::default();
    let chunker = get_chunker(algorithm, params);
    let fuse_fs = FuseFS::new(db, Sha256Hasher::default(), chunker);

    fs::create_dir_all(mount_point).unwrap();
    let _session = fuser::spawn_mount2(fuse_fs, mount_point, &[AutoUnmount]).unwrap();

    let fuse_path = mount_point.join("file");
    let mut fuse_file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .truncate(true)
        .open(&fuse_path)
        .unwrap();

    let mut source = File::open(&dataset.filename).unwrap();

    let mut buf = vec![0; 50 * MB];
    loop {
        let bytes_read = source.read(&mut buf).unwrap();
        if bytes_read == 0 {
            break;
        }
        fuse_file.write_all(&buf[..bytes_read]).unwrap();
    }
    fuse_file.flush().unwrap();
    drop(fuse_file);

    let bench_name = dataset.filename.clone();
    let parameter = format!("read_fuse-{:?}-{}", algorithm, params);
    group.bench_function(BenchmarkId::new(bench_name, parameter), |b| {
        b.iter_batched(
            || {
                OpenOptions::new()
                    .read(true)
                    .custom_flags(O_DIRECT)
                    .open(&fuse_path)
                    .unwrap()
            },
            |mut fuse_file| {
                let mut buf = vec![0; 50 * MB];
                loop {
                    let bytes_read = fuse_file.read(&mut buf).unwrap();
                    if bytes_read == 0 {
                        break;
                    }
                }
            },
            BatchSize::PerIteration,
        )
    });
}

pub fn benches() {
    let mut criterion: Criterion<_> = Criterion::default().configure_from_args();
    bench(&mut criterion);
}

fn main() {
    benches();

    Criterion::default().configure_from_args().final_summary();
}
