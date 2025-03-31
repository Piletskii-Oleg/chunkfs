pub mod generator;
mod report;

use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::io::Read as _;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::system::file_layer::FileHandle;
use crate::{
    create_cdc_filesystem, ChunkHash, ChunkerRef, DataContainer, FileSystem, Hasher,
    IterableDatabase, WriteMeasurements, MB,
};

use report::{DedupMeasurement, MeasureResult, Throughput, TimeMeasurement};

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

/// A file system fixture that allows user to do measurements and carry out benchmarks
/// for CDC algorithms.
///
/// Clears the database before each method call.
pub struct CDCFixture<B, Hash>
where
    B: IterableDatabase<Hash, DataContainer<()>>,
    Hash: ChunkHash,
{
    pub fs: FileSystem<B, Hash, (), HashMap<(), Vec<u8>>>,
}

impl<B, Hash> CDCFixture<B, Hash>
where
    B: IterableDatabase<Hash, DataContainer<()>>,
    Hash: ChunkHash,
{
    /// Creates a fixture, opening a database with given base and hasher.
    pub fn new<H>(base: B, hasher: H) -> Self
    where
        H: Into<Box<dyn Hasher<Hash = Hash> + 'static>>,
    {
        let fs = create_cdc_filesystem(base, hasher.into());
        Self { fs }
    }

    /// Fills the underlying database with some data.
    pub fn fill_with<R>(&mut self, data: R, chunker: ChunkerRef) -> io::Result<()>
    where
        R: io::Read,
    {
        let (mut file, _) = self.init_file(chunker.clone())?;

        self.fs.write_from_stream(&mut file, data)?;

        self.fs.close_file(file).map(|_| ())
    }

    /// Conducts a measurement on a given dataset using given chunker.
    pub fn measure<C>(&mut self, dataset: &Dataset, chunker: C) -> io::Result<MeasureResult>
    where
        C: Into<ChunkerRef>,
    {
        let chunker = chunker.into();
        let chunker_name = format!("{:?}", chunker);

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
            write_time,
            read_time,
            chunk_time,
            hash_time,
        };

        let throughput = Throughput::new(dataset.size, measurement);

        let result = MeasureResult {
            date: chrono::Utc::now(),
            name: dataset.name.to_string(),
            file_name: uuid,
            chunker: chunker_name,
            measurement,
            throughput,
            dedup_ratio: self.fs.cdc_dedup_ratio(),
            full_dedup_ratio: self.fs.full_cdc_dedup_ratio(),
            avg_chunk_size: self.fs.average_chunk_size(),
            size: dataset.size,
            path: dataset.path.clone(),
        };

        Ok(result)
    }

    /// Conducts n measurements on a given dataset using given chunker.
    ///
    /// Clears database after each successful dataset write and before the first one.
    pub fn measure_multi<C>(
        &mut self,
        dataset: &Dataset,
        chunker: C,
        n: usize,
    ) -> io::Result<Vec<MeasureResult>>
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
    ) -> io::Result<Vec<MeasureResult>>
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
            .storage_iterator()
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

        drop(read);

        let mut fs_file = self.fs.open_file_readonly(uuid)?;
        let mut dataset_file = dataset.open()?;
        let mut buffer = Vec::with_capacity(MB);

        loop {
            let read = self.fs.read_from_file(&mut fs_file)?;
            if read.is_empty() {
                break;
            }

            buffer.clear();
            io::Read::take(&mut dataset_file, read.len() as u64).read_to_end(&mut buffer)?;

            if read != buffer {
                let msg = "contents of dataset and written file are different";
                return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
            }
        }

        Ok(read_time)
    }

    /// Creates a file with a random name and a given chunker, then returns it and its name.
    fn init_file(&mut self, chunker: ChunkerRef) -> io::Result<(FileHandle, String)> {
        let uuid = Uuid::new_v4().to_string();

        self.fs.create_file(&uuid, chunker).map(|file| (file, uuid))
    }
}
