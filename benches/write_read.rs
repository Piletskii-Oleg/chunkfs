use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput};

use chunkfs::bench::Dataset;
use chunkfs::chunkers::{LeapChunker, RabinChunker, SuperChunker, UltraChunker};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::{create_cdc_filesystem, ChunkerRef};

const SAMPLE_SIZE: usize = 60;

#[derive(Copy, Clone, Debug)]
enum Algorithms {
    Rabin,
    Leap,
    Super,
    Ultra,
}

fn chunkers() -> Vec<Algorithms> {
    vec![
        Algorithms::Rabin,
        Algorithms::Leap,
        Algorithms::Super,
        Algorithms::Ultra,
    ]
}

fn get_chunker(algorithm: Algorithms) -> ChunkerRef {
    match algorithm {
        Algorithms::Rabin => RabinChunker::default().into(),
        Algorithms::Leap => LeapChunker::default().into(),
        Algorithms::Super => UltraChunker::default().into(),
        Algorithms::Ultra => SuperChunker::default().into(),
    }
}

pub fn bench(c: &mut Criterion) {
    let datasets = vec![Dataset::new("kernel.tar", "kernel").unwrap()];

    for dataset in datasets {
        let mut group = c.benchmark_group("Chunkers");
        group.sample_size(SAMPLE_SIZE);
        group.throughput(Throughput::Bytes(dataset.size as u64));

        for chunker in chunkers() {
            bench_write(&dataset, &mut group, chunker);
        }

        for chunker in chunkers() {
            bench_read(&dataset, &mut group, chunker);
        }
    }
}

fn bench_write(dataset: &Dataset, group: &mut BenchmarkGroup<WallTime>, algorithm: Algorithms) {
    let bench_name = &dataset.name;
    let parameter = format!("write-{:?}", algorithm);
    group.bench_function(BenchmarkId::new(bench_name, parameter), |b| {
        b.iter_batched(
            || {
                let data = BufReader::new(File::open(&dataset.path).unwrap());

                let mut fs = create_cdc_filesystem(HashMap::default(), Sha256Hasher::default());

                let chunker = get_chunker(algorithm);
                let handle = fs.create_file("file", chunker).unwrap();

                (fs, handle, data)
            },
            |(mut fs, mut handle, data)| {
                fs.write_from_stream(&mut handle, data).unwrap();
                fs.close_file(handle).unwrap();
            },
            BatchSize::PerIteration,
        )
    });
}

fn bench_read(dataset: &Dataset, group: &mut BenchmarkGroup<WallTime>, algorithm: Algorithms) {
    let bench_name = &dataset.name;
    let parameter = format!("read-{:?}", algorithm);
    group.bench_function(BenchmarkId::new(bench_name, parameter), |b| {
        b.iter_batched(
            || {
                let data = BufReader::new(File::open(&dataset.path).unwrap());

                let base = HashMap::default();
                let mut fs = create_cdc_filesystem(base, Sha256Hasher::default());

                let chunker = get_chunker(algorithm);
                let mut handle = fs.create_file("file", chunker).unwrap();
                fs.write_from_stream(&mut handle, data).unwrap();
                fs.close_file(handle).unwrap();

                let handle = fs.open_file_readonly("file").unwrap();

                (fs, handle)
            },
            |(fs, handle)| {
                fs.read_file_complete(&handle).unwrap();
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
