use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

use chunking::SizeParams;
use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput};
use itertools::iproduct;

use chunkfs::bench::Dataset;
use chunkfs::chunkers::{LeapChunker, RabinChunker, SuperChunker, UltraChunker};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::{create_cdc_filesystem, Chunker};

const SAMPLE_SIZE: usize = 60;

struct SizeParameters {
    min: Vec<usize>,
    avg: Vec<usize>,
    max: Vec<usize>,
}

impl SizeParameters {
    /// Calculates cartesian product of min, avg and max sizes.
    fn variants(&self) -> Vec<SizeParams> {
        iproduct!(self.min.iter(), self.avg.iter(), self.max.iter())
            .filter(|(min, avg, max)| min <= avg && avg <= max && min <= max)
            .map(|(&min, &avg, &max)| SizeParams { min, avg, max })
            .collect()
    }
}

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

fn get_chunker(algorithm: Algorithms, params: SizeParams) -> Box<dyn Chunker> {
    match algorithm {
        Algorithms::Rabin => Box::new(RabinChunker::new(params)),
        Algorithms::Leap => Box::new(LeapChunker::new(params)),
        Algorithms::Super => Box::new(UltraChunker::new(params)),
        Algorithms::Ultra => Box::new(SuperChunker::new(params)),
    }
}

pub fn bench(c: &mut Criterion) {
    let datasets = vec![Dataset::new("kernel.tar", "kernel").unwrap()];

    let size_params = SizeParameters {
        min: vec![1024, 2048, 4096],
        avg: vec![4096, 20000],
        max: vec![40000, 60000, 10000],
    };

    for dataset in datasets {
        let mut group = c.benchmark_group("ChunkerSizes");
        group.sample_size(SAMPLE_SIZE);
        group.throughput(Throughput::Bytes(dataset.size as u64));

        for params in size_params.variants() {
            bench_write(&dataset, &mut group, Algorithms::Super, params)
        }
    }
}

fn bench_write(
    dataset: &Dataset,
    group: &mut BenchmarkGroup<WallTime>,
    algorithm: Algorithms,
    params: SizeParams,
) {
    let bench_name = &dataset.name;
    let parameter = format!("write-{:?}-{}", algorithm, params);
    group.bench_function(BenchmarkId::new(bench_name, parameter), |b| {
        b.iter_batched(
            || {
                let data = BufReader::new(File::open(&dataset.path).unwrap());

                let mut fs = create_cdc_filesystem(HashMap::default(), Sha256Hasher::default());

                let chunker = get_chunker(algorithm, params);
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

pub fn benches() {
    let mut criterion: Criterion<_> = Criterion::default().configure_from_args();
    bench(&mut criterion);
}

fn main() {
    benches();

    Criterion::default().configure_from_args().final_summary();
}
