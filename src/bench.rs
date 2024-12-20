pub mod generator;

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io;
use std::io::Read;
use std::ops::AddAssign;
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::{
    create_cdc_filesystem, ChunkHash, Chunker, DataContainer, FileSystem, Hasher, IterableDatabase,
    WriteMeasurements,
};

/// A file system fixture that allows user to do measurements and carry out benchmarks
/// for CDC algorithms.
pub struct CDCFixture<B, H, Hash>
where
    B: IterableDatabase<Hash, DataContainer<()>>,
    H: Hasher<Hash=Hash>,
    Hash: ChunkHash,
{
    fs: FileSystem<B, H, Hash, (), HashMap<(), Vec<u8>>>,
}

impl<B, H, Hash> CDCFixture<B, H, Hash>
where
    B: IterableDatabase<Hash, DataContainer<()>>,
    H: Hasher<Hash=Hash>,
    Hash: ChunkHash,
{
    /// Creates a fixture, opening a database with given base and hasher.
    pub fn new(base: B, hasher: H) -> Self {
        let fs = create_cdc_filesystem(base, hasher);
        Self { fs }
    }

    /// Conducts a measurement on a given dataset using given chunker.
    pub fn measure(
        &mut self,
        chunker: Box<dyn Chunker>,
        dataset: &Dataset,
    ) -> io::Result<Measurement> {
        let mut file = self.fs.create_file(dataset.uuid, chunker)?;

        let mut dataset_file = dataset.open()?;

        let now = Instant::now();
        self.fs.write_from_stream(&mut file, &mut dataset_file)?;
        let write_time = now.elapsed();

        let write_measurements = self.fs.close_file(file)?;

        let read_time = self.verify(dataset)?;

        let measurement = Measurement {
            uuid: dataset.uuid.to_string(),
            name: dataset.name.to_string(),
            write_time,
            read_time,
            write_measurements,
            dedup_ratio: self.fs.cdc_dedup_ratio(),
        };

        Ok(measurement)
    }

    pub fn measure_multi(&mut self, _n: usize) -> Measurement {
        todo!()
    }

    pub fn measure_repeated(&mut self, _m: usize) -> Measurement {
        todo!()
    }

    /// Verifies that the written dataset contents are valid.
    ///
    /// Returns read time for the file.
    fn verify(&self, dataset: &Dataset) -> io::Result<Duration> {
        let file = self.fs.open_file_readonly(dataset.uuid.to_string())?;

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
}

#[derive(Default)]
pub struct Measurement {
    pub uuid: String,
    pub name: String,
    pub write_time: Duration,
    pub read_time: Duration,
    pub write_measurements: WriteMeasurements,
    pub dedup_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct Dataset {
    pub path: String,
    pub name: String,
    pub size: usize,
    uuid: Uuid,
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
        let uuid = Uuid::new_v4();
        Ok(Dataset {
            path: path.to_string(),
            name: name.to_string(),
            size,
            uuid,
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

impl AddAssign for Measurement {
    fn add_assign(&mut self, rhs: Self) {
        self.read_time += rhs.read_time;
        self.write_time += rhs.write_time;
        self.write_measurements += rhs.write_measurements;
    }
}

impl Debug for Measurement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Dataset: {}\nRead time: {:?}\nWrite time: {:?}\nChunk time: {:?}\nHash time: {:?}\nDedup ratio: {:.3}",
            self.name,
            self.read_time,
            self.write_time,
            self.write_measurements.chunk_time,
            self.write_measurements.hash_time,
            self.dedup_ratio,
        )
    }
}
