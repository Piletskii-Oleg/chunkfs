use chunkfs::chunkers::{LeapChunker, RabinChunker, SuperChunker, UltraChunker};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::FileSystem;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput, BenchmarkGroup};
use std::collections::HashMap;
use std::fs;
use criterion::measurement::WallTime;

struct Dataset<'a> {
    path: &'a str,
    name: &'a str,
    size: usize,
}

impl<'a> Dataset<'a> {
    fn new(path: &'a str, name: &'a str) -> Self {
        let size = {
            let data = fs::read(path).unwrap();
            data.len()
        };
        Dataset { path, name, size }
    }
}

const MB_COUNT: usize = 1024;

pub fn criterion_benchmark(c: &mut Criterion) {
    let datasets = vec![
        //Dataset::new("create.zip", "create"),
        //Dataset::new("mail.tar", "mail"),
        Dataset::new("linux.tar", "linux"),
        Dataset::new("ubuntu.iso", "ubuntu")
    ];

    for dataset in datasets {
        let mut group = c.benchmark_group("Chunkers");
        group.sample_size(60);
        group.throughput(Throughput::Bytes(dataset.size as u64));

        let data = fs::read(dataset.path).unwrap();

        //bench_write(&dataset, &mut group, &data);

        bench_read_ultra(&dataset, &mut group, &data);
        bench_read_leap(&dataset, &mut group, &data);
        bench_read_rabin(&dataset, &mut group, &data);
        bench_read_super(&dataset, &mut group, &data);
    }
}

fn bench_write(dataset: &Dataset, group: &mut BenchmarkGroup<WallTime>, data: &Vec<u8>) {
    group.bench_function(
        BenchmarkId::new("write", format!("{}", dataset.name)),
        |b| {
            b.iter_batched(
                || {
                    let base = HashMap::default();
                    let mut fs = FileSystem::new_cdc_only(base, Sha256Hasher::default());

                    let chunker = UltraChunker::default();
                    let mut handle = fs.create_file("file".to_string(), chunker, true).unwrap();

                    (fs, handle)
                },
                |(mut fs, mut handle)| {
                    fs.write_to_file(&mut handle, &data).unwrap();
                    fs.close_file(handle).unwrap();
                },
                BatchSize::PerIteration,
            )
        },
    );
}

fn bench_read_ultra(dataset: &Dataset, group: &mut BenchmarkGroup<WallTime>, data: &Vec<u8>) {
    group.bench_function(
        BenchmarkId::new("read-ultra", format!("{}", dataset.name)),
        |b| {
            b.iter_batched(
                || {
                    let base = HashMap::default();
                    let mut fs = FileSystem::new_cdc_only(base, Sha256Hasher::default());

                    let chunker = UltraChunker::default();
                    let mut handle = fs.create_file("file".to_string(), chunker, true).unwrap();
                    fs.write_to_file(&mut handle, &data).unwrap();
                    fs.close_file(handle).unwrap();

                    let chunker = UltraChunker::default();
                    let handle = fs.open_file("file", chunker).unwrap();

                    (fs, handle)
                },
                |(mut fs, mut handle)| {
                    fs.read_file_complete(&mut handle).unwrap();
                },
                BatchSize::PerIteration,
            )
        },
    );
}

fn bench_read_rabin(dataset: &Dataset, group: &mut BenchmarkGroup<WallTime>, data: &Vec<u8>) {
    group.bench_function(
        BenchmarkId::new("read-rabin", format!("{}", dataset.name)),
        |b| {
            b.iter_batched(
                || {
                    let base = HashMap::default();
                    let mut fs = FileSystem::new_cdc_only(base, Sha256Hasher::default());

                    let chunker = RabinChunker::new();
                    let mut handle = fs.create_file("file".to_string(), chunker, true).unwrap();
                    fs.write_to_file(&mut handle, &data).unwrap();
                    fs.close_file(handle).unwrap();

                    let chunker = UltraChunker::default();
                    let handle = fs.open_file("file", chunker).unwrap();

                    (fs, handle)
                },
                |(mut fs, mut handle)| {
                    fs.read_file_complete(&mut handle).unwrap();
                },
                BatchSize::PerIteration,
            )
        },
    );
}

fn bench_read_super(dataset: &Dataset, group: &mut BenchmarkGroup<WallTime>, data: &Vec<u8>) {
    group.bench_function(
        BenchmarkId::new("read-super", format!("{}", dataset.name)),
        |b| {
            b.iter_batched(
                || {
                    let base = HashMap::default();
                    let mut fs = FileSystem::new_cdc_only(base, Sha256Hasher::default());

                    let chunker = SuperChunker::new();
                    let mut handle = fs.create_file("file".to_string(), chunker, true).unwrap();
                    fs.write_to_file(&mut handle, &data).unwrap();
                    fs.close_file(handle).unwrap();

                    let chunker = UltraChunker::default();
                    let handle = fs.open_file("file", chunker).unwrap();

                    (fs, handle)
                },
                |(mut fs, mut handle)| {
                    fs.read_file_complete(&mut handle).unwrap();
                },
                BatchSize::PerIteration,
            )
        },
    );
}

fn bench_read_leap(dataset: &Dataset, group: &mut BenchmarkGroup<WallTime>, data: &Vec<u8>) {
    group.bench_function(
        BenchmarkId::new("read-leap", format!("{}", dataset.name)),
        |b| {
            b.iter_batched(
                || {
                    let base = HashMap::default();
                    let mut fs = FileSystem::new_cdc_only(base, Sha256Hasher::default());

                    let chunker = LeapChunker::default();
                    let mut handle = fs.create_file("file".to_string(), chunker, true).unwrap();
                    fs.write_to_file(&mut handle, &data).unwrap();
                    fs.close_file(handle).unwrap();

                    let chunker = UltraChunker::default();
                    let handle = fs.open_file("file", chunker).unwrap();

                    (fs, handle)
                },
                |(mut fs, mut handle)| {
                    fs.read_file_complete(&mut handle).unwrap();
                },
                BatchSize::PerIteration,
            )
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
