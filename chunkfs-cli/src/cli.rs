use chunkfs::bench::{CDCFixture, Dataset};
use chunkfs::chunkers::seq::OperationMode;
use chunkfs::chunkers::{
    seq, FSChunker, FastChunker, LeapChunker, RabinChunker, SeqChunker, SizeParams, SuperChunker,
    UltraChunker,
};
use chunkfs::hashers::{Sha256Hasher, SimpleHasher};
use chunkfs::storages::SledStorage;
use chunkfs::{ChunkHash, ChunkerRef, DataContainer, Hasher, IterableDatabase, KB};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Deserialize;
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(ValueEnum, Deserialize, Debug, Copy, Clone)]
#[serde(rename_all(deserialize = "kebab-case"))]
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

#[derive(ValueEnum, Deserialize, Debug, Copy, Clone, PartialEq)]
#[serde(rename_all(deserialize = "kebab-case"))]
enum CliChunker {
    Super,
    Rabin,
    Seq,
    Ultra,
    Leap,
    FixedSize,
    Fast,
}

fn get_chunker(args: &CliArgs) -> ChunkerRef {
    let params = SizeParams {
        min: args.min * KB,
        avg: args.avg * KB,
        max: args.max * KB,
    };

    match args.chunker {
        CliChunker::Super => SuperChunker::new(params).into(),
        CliChunker::Rabin => RabinChunker::new(params).into(),
        CliChunker::Seq => {
            let mode = args
                .seq_mode
                .expect("SeqCDC selected but 'seq-mode' parameter is missing");
            SeqChunker::new(mode.into(), params, seq::Config::default()).into()
        }
        CliChunker::Ultra => UltraChunker::new(params).into(),
        CliChunker::Leap => LeapChunker::new(params).into(),
        CliChunker::FixedSize => FSChunker::new(params.min).into(),
        CliChunker::Fast => FastChunker::new(params).into(),
    }
}

#[derive(ValueEnum, Deserialize, Debug, Copy, Clone, PartialEq)]
#[serde(rename_all(deserialize = "kebab-case"))]
enum CliDatabase {
    Hashmap,
    Sled,
}

#[derive(ValueEnum, Deserialize, Debug, Copy, Clone, PartialEq)]
#[serde(rename_all(deserialize = "kebab-case"))]
enum CliHasher {
    Sha256,
    Simple,
}

#[derive(Args, Deserialize, Clone, Debug)]
#[serde(rename_all(deserialize = "kebab-case"))]
struct CliArgs {
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

    /// Path to folder where report should be saved
    #[arg(long)]
    report_path: PathBuf,
}

#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Cli {
    /// Path to a config (exclusive)
    #[arg(long, exclusive = true)]
    config: Option<PathBuf>,

    #[command(flatten)]
    args: Option<CliArgs>,

    #[command(subcommand)]
    commands: Commands,
}

#[derive(Subcommand, Deserialize, Debug)]
#[serde(rename_all(deserialize = "kebab-case"))]
#[serde(rename_all_fields(deserialize = "kebab-case"))]
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

        #[arg(long)]
        /// Paths to data that would fill the database
        fill_paths: Option<Vec<String>>,

        #[arg(long)]
        /// Adjustment factor for chunk distribution.
        /// Chunks smaller than this will be grouped together.
        adjustment: Option<usize>,
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

    /// Run a configuration from file
    RunConfig,
}

enum Scenario {}

#[derive(Deserialize)]
struct Config {
    args: CliArgs,
    command: Commands,
}

impl Cli {
    pub fn start(&self) -> io::Result<()> {
        if self.config.is_some() {
            self.parse_config()
        } else {
            self.parse_cli_args()
        }
    }

    fn parse_cli_args(&self) -> io::Result<()> {
        let args = self.args.as_ref().unwrap();
        let commands = &self.commands;

        if !args.report_path.is_dir() {
            let msg = "Report path is not a directory";
            return Err(io::Error::new(io::ErrorKind::InvalidInput, msg));
        }

        Self::choose_hasher(args, commands)
    }

    fn parse_config(&self) -> io::Result<()> {
        let path = self.config.as_ref().unwrap();
        let toml = std::fs::read_to_string(path)?;

        let config =
            toml::from_str::<Config>(&toml).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        if !config.args.report_path.is_dir() {
            let msg = "Report path is not a directory";
            return Err(io::Error::new(io::ErrorKind::InvalidInput, msg));
        }

        Self::choose_hasher(&config.args, &config.command)
    }

    fn choose_hasher(args: &CliArgs, commands: &Commands) -> io::Result<()> {
        match args.hasher {
            CliHasher::Sha256 => {
                Cli::choose_database(args, commands, Sha256Hasher::default().into())
            }
            CliHasher::Simple => Cli::choose_database(args, commands, SimpleHasher.into()),
        }
    }

    fn choose_database<Hash: ChunkHash + bincode::Decode<()> + bincode::Encode>(
        args: &CliArgs,
        command: &Commands,
        hasher: Box<dyn Hasher<Hash = Hash>>,
    ) -> io::Result<()> {
        match args.database {
            CliDatabase::Hashmap => {
                let fixture = CDCFixture::new(HashMap::default(), hasher);
                Cli::execute_command(args, command, fixture)
            }
            CliDatabase::Sled => {
                let db_path = format!("db-{}", Uuid::new_v4());
                let fixture = CDCFixture::new(SledStorage::new(db_path)?, hasher);
                Cli::execute_command(args, command, fixture)
            }
        }
    }

    fn execute_command<B, Hash>(
        args: &CliArgs,
        command: &Commands,
        mut fixture: CDCFixture<B, Hash>,
    ) -> io::Result<()>
    where
        B: IterableDatabase<Hash, DataContainer<()>>,
        Hash: ChunkHash + bincode::Decode<()> + bincode::Encode,
    {
        let chunker = get_chunker(args);

        match command {
            Commands::Measure {
                dataset_path,
                dataset_name,
                count,
                cleanup,
                fill_paths,
                adjustment,
            } => {
                let dataset = Dataset::new(dataset_path, dataset_name)?;

                let mut distributions = Vec::with_capacity(*count);
                let adjustment = adjustment.unwrap_or_else(|| 1000);

                let mut measurements = Vec::with_capacity(*count);

                if *cleanup {
                    for _ in 0..*count {
                        fixture.fs.clear_database()?;
                        Self::fill_with(&mut fixture, &chunker, fill_paths)?;

                        let measurement = fixture.measure(&dataset, chunker.clone())?;
                        distributions.push(fixture.size_distribution(adjustment));

                        measurements.push(measurement)
                    }
                } else {
                    Self::fill_with(&mut fixture, &chunker, fill_paths)?;

                    for _ in 0..*count {
                        let measurement = fixture.measure(&dataset, chunker.clone())?;
                        distributions.push(fixture.size_distribution(adjustment));
                        measurements.push(measurement)
                    }
                };

                let date = chrono::offset::Utc::now().to_string();
                let report_path = args.report_path.join(date);
                std::fs::create_dir_all(&report_path)?;
                let measurement_path = report_path.join("report.csv");

                for measurement in measurements {
                    measurement.write_to_csv(&measurement_path)?;
                }

                for (index, distribution) in distributions.into_iter().enumerate() {
                    let pairs = distribution.into_iter().collect::<Vec<(usize, u32)>>();

                    let file_name = format!("distribution.{index}.json");
                    let path = report_path.join(file_name);

                    let mut writer = io::BufWriter::new(std::fs::File::create(path)?);
                    serde_json::to_writer(&mut writer, &pairs)?;
                }
            }

            Commands::DedupRatio {
                dataset_path,
                dataset_name,
            } => {
                let dataset = Dataset::new(dataset_path, dataset_name)?;

                let measurement = fixture.measure(&dataset, chunker)?;
                measurement.write_to_csv(&args.report_path)?
            }

            Commands::RunConfig => println!("should choose another command"),
        };

        Ok(())
    }

    fn fill_with<B, Hash>(
        fixture: &mut CDCFixture<B, Hash>,
        chunker: &ChunkerRef,
        fill_paths: &Option<Vec<String>>,
    ) -> io::Result<()>
    where
        B: IterableDatabase<Hash, DataContainer<()>>,
        Hash: ChunkHash,
    {
        if let Some(fill_paths) = fill_paths {
            for fill_path in fill_paths {
                let mut reader = io::BufReader::new(std::fs::File::open(fill_path)?);

                fixture.fill_with(&mut reader, chunker.clone())?
            }
        }
        Ok(())
    }
}
