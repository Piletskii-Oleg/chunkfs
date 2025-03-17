extern crate chunkfs;

use std::collections::HashMap;
use std::io;

use chunkfs::bench::{CDCFixture, Dataset};
use chunkfs::chunkers::seq::{Config, OperationMode};
use chunkfs::chunkers::*;
use chunkfs::hashers::{Sha256Hasher, SimpleHasher};
use chunkfs::{ChunkHash, ChunkerRef, DataContainer, Hasher, IterableDatabase, KB};

use clap::{Parser, Subcommand, ValueEnum};

use crate::Commands::{DedupRatio, Measure};

fn main() -> io::Result<()> {
    let cli = Cli::parse();

    match cli.hasher {
        CliHasher::Sha256 => do_stuff(cli, Sha256Hasher::default().into()),
        CliHasher::Simple => do_stuff(cli, SimpleHasher.into()),
    }
}

fn do_stuff<Hash: ChunkHash>(cli: Cli, hasher: Box<dyn Hasher<Hash = Hash>>) -> io::Result<()> {
    match cli.database {
        CliDatabase::Hashmap => {
            let fixture = CDCFixture::new(HashMap::default(), hasher);
            execute_command(cli, fixture)
        }
    }
}

fn execute_command<B, Hash>(cli: Cli, mut fixture: CDCFixture<B, Hash>) -> io::Result<()>
where
    B: IterableDatabase<Hash, DataContainer<()>>,
    Hash: ChunkHash,
{
    let chunker = get_chunker(&cli);

    match cli.commands {
        Measure {
            dataset_path,
            dataset_name,
            count,
            cleanup,
        } => {
            let dataset = Dataset::new(&dataset_path, &dataset_name)?;
            let measurements = if cleanup {
                fixture.measure_multi(&dataset, chunker, count)?
            } else {
                fixture.measure_repeated(&dataset, chunker, count)?
            };

            for measurement in measurements {
                measurement.write_to_csv(&cli.report_path)?;
            }

            Ok(())
        }

        DedupRatio {
            dataset_path,
            dataset_name,
        } => {
            let dataset = Dataset::new(&dataset_path, &dataset_name)?;

            let measurement = fixture.measure(&dataset, chunker)?;
            measurement.write_to_csv(&cli.report_path)
        }
    }
}

#[derive(ValueEnum, Debug, Copy, Clone)]
enum SeqOperationMode {
    Increasing,
    Decreasing,
}

impl From<SeqOperationMode> for OperationMode {
    fn from(value: SeqOperationMode) -> Self {
        match value {
            SeqOperationMode::Increasing => OperationMode::Increasing,
            SeqOperationMode::Decreasing => OperationMode::Decreasing,
        }
    }
}

#[derive(ValueEnum, Debug, Copy, Clone, PartialEq)]
enum CliChunker {
    Super,
    Rabin,
    Seq,
    Ultra,
    Leap,
    FixedSize,
    Fast,
}

fn get_chunker(cli: &Cli) -> ChunkerRef {
    let params = SizeParams {
        min: cli.min * KB,
        avg: cli.avg * KB,
        max: cli.max * KB,
    };

    match cli.chunker {
        CliChunker::Super => SuperChunker::new(params).into(),
        CliChunker::Rabin => RabinChunker::new(params).into(),
        CliChunker::Seq => {
            let mode = cli.seq_mode.unwrap();
            SeqChunker::new(mode.into(), params, Config::default()).into()
        }
        CliChunker::Ultra => UltraChunker::new(params).into(),
        CliChunker::Leap => LeapChunker::new(params).into(),
        CliChunker::FixedSize => FSChunker::new(params.min).into(),
        CliChunker::Fast => FastChunker::new(params).into(),
    }
}

#[derive(ValueEnum, Debug, Copy, Clone, PartialEq)]
enum CliDatabase {
    Hashmap,
}

#[derive(ValueEnum, Debug, Copy, Clone, PartialEq)]
enum CliHasher {
    Sha256,
    Simple,
}

#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Cli {
    /// Underlying database
    #[arg(long)]
    database: CliDatabase,

    /// Hasher used for chunks
    #[arg(long)]
    hasher: CliHasher,

    /// Chunking algorithm
    #[arg(long)]
    chunker: CliChunker,

    /// Mode of operation for SeqCDC algorithm
    #[arg(long, required_if_eq("chunker", "seq"), value_name = "MODE")]
    seq_mode: Option<SeqOperationMode>,

    /// Minimum chunk size (in KB)
    #[arg(long, value_name = "MIN_CHUNK_SIZE")]
    min: usize,

    /// Average chunk size (in KB)
    #[arg(long, value_name = "AVG_CHUNK_SIZE")]
    avg: usize,

    /// Maximum chunk size (in KB)
    #[arg(long, value_name = "MAX_CHUNK_SIZE")]
    max: usize,

    /// Path where report should be saved in .csv format
    #[arg(long)]
    report_path: String,

    #[command(subcommand)]
    commands: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Conduct some amount of measurements
    Measure {
        /// Path to dataset to test on
        #[arg(long)]
        dataset_path: String,

        /// Name of the dataset
        #[arg(long)]
        dataset_name: String,

        #[arg(long)]
        /// How many measurements to conduct
        count: usize,

        #[arg(long, default_value = "true")]
        /// Whether the system has to be cleaned up after each measurement
        cleanup: bool,
    },

    /// Calculate dedup ratio
    DedupRatio {
        /// Path to dataset to test on
        #[arg(long)]
        dataset_path: String,

        /// Name of the dataset
        #[arg(long)]
        dataset_name: String,
    },
}
