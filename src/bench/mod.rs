pub mod generator;

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io;
use std::io::Read;
use std::iter::Sum;
use std::ops::{Add, AddAssign};
use std::path::Path;
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::system::file_layer::FileHandle;
use crate::{
    create_cdc_filesystem, ChunkHash, ChunkerRef, DataContainer, FileSystem, Hasher,
    IterableDatabase, WriteMeasurements,
};

/// A file system fixture that allows user to do measurements and carry out benchmarks
/// for CDC algorithms.
///
/// Clears the database before each method call.
pub struct CDCFixture<B, H, Hash>
where
    B: IterableDatabase<Hash, DataContainer<()>>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
{
    pub fs: FileSystem<B, H, Hash, (), HashMap<(), Vec<u8>>>,
}

impl<B, H, Hash> CDCFixture<B, H, Hash>
where
    B: IterableDatabase<Hash, DataContainer<()>>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
{
    /// Creates a fixture, opening a database with given base and hasher.
    pub fn new(base: B, hasher: H) -> Self {
        let fs = create_cdc_filesystem(base, hasher);
        Self { fs }
    }

    /// Conducts a measurement on a given dataset using given chunker.
    pub fn measure<C>(&mut self, dataset: &Dataset, chunker: C) -> io::Result<TimeMeasurement>
    where
        C: Into<ChunkerRef>,
    {
        let chunker = chunker.into();

        let (mut file, uuid) = self.init_file(chunker)?;

        let mut dataset_file = dataset.open()?;

        let now = Instant::now();
        self.fs.write_from_stream(&mut file, &mut dataset_file)?;
        let write_time = now.elapsed();

        let WriteMeasurements {
            chunk_time,
            hash_time,
        } = self.fs.close_file(file)?;
        let read_time = self.verify(dataset, &uuid)?;

        let measurement = TimeMeasurement {
            name: dataset.name.to_string(),
            write_time,
            read_time,
            chunk_time,
            hash_time,
        };

        Ok(measurement)
    }

    /// Conducts n measurements on a given dataset using given chunker.
    ///
    /// Clears database after each successful dataset write and before the first one.
    pub fn measure_multi<C>(
        &mut self,
        dataset: &Dataset,
        chunker: C,
        n: usize,
    ) -> io::Result<Vec<TimeMeasurement>>
    where
        C: Into<ChunkerRef>,
    {
        self.fs.clear_database()?;

        let chunker = chunker.into();

        (0..n)
            .map(|_| {
                self.fs.clear_database()?;
                self.measure(dataset, chunker.clone())
            })
            .collect()
    }

    /// Conducts m measurements on a given dataset using given chunker.
    ///
    /// Does not clear database after each successful dataset write,
    /// but clears it before the first one.
    pub fn measure_repeated<C>(
        &mut self,
        dataset: &Dataset,
        chunker: C,
        m: usize,
    ) -> io::Result<Vec<TimeMeasurement>>
    where
        C: Into<ChunkerRef>,
    {
        self.fs.clear_database()?;

        let chunker = chunker.into();

        (0..m)
            .map(|_| self.measure(dataset, chunker.clone()))
            .collect()
    }

    /// Calculates deduplication ratio of the given dataset using given chunker.
    ///
    /// Clears database on call.
    pub fn dedup_ratio<C>(&mut self, dataset: &Dataset, chunker: C) -> io::Result<DedupMeasurement>
    where
        C: Into<ChunkerRef>,
    {
        self.fs.clear_database()?;

        let chunker = chunker.into();

        let (mut file, uuid) = self.init_file(chunker)?;
        let mut dataset_file = dataset.open()?;

        self.fs.write_from_stream(&mut file, &mut dataset_file)?;
        self.fs.close_file(file)?;
        self.verify(dataset, &uuid)?;

        Ok(DedupMeasurement {
            name: dataset.name.to_string(),
            dedup_ratio: self.fs.cdc_dedup_ratio(),
        })
    }

    /// Gives out a hash map containing chunk size distribution in the database.
    ///
    /// Takes `adjustment` as a parameter, which specifies minimal difference between different sized chunks,
    /// i.e. the size step in the distribution.
    ///
    /// Does not modify the database, i.e. does not clear it.
    pub fn size_distribution(&self, adjustment: usize) -> HashMap<usize, u32> {
        let mut chunk_map = HashMap::new();
        for chunk in self
            .fs
            .iterator()
            .map(|(_, container)| container.unwrap_chunk())
        {
            chunk_map
                .entry(chunk.len() / adjustment * adjustment)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }

        chunk_map
    }

    /// Verifies that the written dataset contents are valid.
    ///
    /// Returns read time for the file.
    fn verify(&self, dataset: &Dataset, uuid: &str) -> io::Result<Duration> {
        let file = self.fs.open_file_readonly(uuid)?;

        let now = Instant::now();
        let read = self.fs.read_file_complete(&file)?;
        let read_time = now.elapsed();

        if read.len() != dataset.size {
            let msg = "dataset size and size of written file are different";
            return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
        }

        let mut dataset_file = dataset.open()?;
        let mut buffer = Vec::with_capacity(dataset.size);
        dataset_file.read_to_end(&mut buffer)?;

        if read != buffer {
            let msg = "contents of dataset and written file are different";
            return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
        }

        Ok(read_time)
    }

    /// Creates a file with a random name and a given chunker, then returns it and its name.
    fn init_file(&mut self, chunker: ChunkerRef) -> io::Result<(FileHandle, String)> {
        let uuid = Uuid::new_v4().to_string();

        self.fs.create_file(&uuid, chunker).map(|file| (file, uuid))
    }
}

#[serde_with::serde_as]
#[derive(Default, serde::Serialize)]
pub struct TimeMeasurement {
    pub name: String,
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    pub write_time: Duration,
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    pub read_time: Duration,
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    pub chunk_time: Duration,
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    pub hash_time: Duration,
}

impl TimeMeasurement {
    /// Writes the measurement to a csv file specified by path.
    /// Measurement units are seconds.
    ///
    /// # Behavior
    /// * If the file does not exist, creates it and writes the measurements.
    /// * If it exists, appends the measurements.
    pub fn write_to_csv<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let mut writer = match File::options().append(true).open(&path) {
            Ok(file) => csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(file),
            Err(e) if e.kind() == io::ErrorKind::NotFound => csv::Writer::from_path(&path)?,
            Err(e) => return Err(e),
        };

        writer.serialize(self)?;
        writer.flush()?;

        Ok(())
    }
}

/// Calculates an average measurement out of a vector of measurements.
pub fn avg_measurement(measurements: Vec<TimeMeasurement>) -> TimeMeasurement {
    let n = measurements.len();
    let sum = measurements.into_iter().sum::<TimeMeasurement>();

    TimeMeasurement {
        name: sum.name,
        write_time: sum.write_time / n as u32,
        read_time: sum.read_time / n as u32,
        chunk_time: sum.chunk_time / n as u32,
        hash_time: sum.hash_time / n as u32,
    }
}

#[derive(Debug)]
pub struct DedupMeasurement {
    pub name: String,
    pub dedup_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct Dataset {
    pub path: String,
    pub name: String,
    pub size: usize,
}

impl Dataset {
    /// Creates a new instance of dataset.
    ///
    /// Will fail if the provided path does not exist,
    /// or if file metadata cannot be read.
    ///
    /// # Parameters
    /// * `path` - path of the dataset file
    /// * `name` - custom name of the dataset
    pub fn new(path: &str, name: &str) -> io::Result<Self> {
        let size = File::open(path)?.metadata()?.len() as usize;
        Ok(Dataset {
            path: path.to_string(),
            name: name.to_string(),
            size,
        })
    }

    /// Opens the dataset and returns its `File` instance.
    ///
    /// Can be used to open the underlying dataset multiple times,
    /// but it is not recommended.
    pub fn open(&self) -> io::Result<File> {
        File::open(&self.path)
    }
}

impl Add for TimeMeasurement {
    type Output = TimeMeasurement;

    fn add(self, rhs: Self) -> Self::Output {
        let mut measurement = self;
        measurement.read_time += rhs.read_time;
        measurement.write_time += rhs.write_time;
        measurement.chunk_time += rhs.chunk_time;
        measurement.hash_time += rhs.hash_time;
        measurement
    }
}

impl AddAssign for TimeMeasurement {
    fn add_assign(&mut self, rhs: Self) {
        self.read_time += rhs.read_time;
        self.write_time += rhs.write_time;
        self.chunk_time += rhs.chunk_time;
        self.hash_time += rhs.hash_time;
    }
}

impl Debug for TimeMeasurement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Dataset: {}\nRead time: {:?}\nWrite time: {:?}\nChunk time: {:?}\nHash time: {:?}",
            self.name, self.read_time, self.write_time, self.chunk_time, self.hash_time,
        )
    }
}

impl Sum for TimeMeasurement {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(TimeMeasurement::default(), |acc, next| acc + next)
    }
}
