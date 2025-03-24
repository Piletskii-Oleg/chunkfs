use crate::MB;
use chrono::{DateTime, Utc};
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::io;
use std::iter::Sum;
use std::ops::{Add, AddAssign};
use std::path::Path;
use std::time::Duration;

#[derive(Default, Clone)]
pub struct MeasureResult {
    pub date: DateTime<Utc>,
    pub name: String,
    pub chunker: String,
    pub size: usize,
    pub dedup_ratio: f64,
    pub full_dedup_ratio: f64,
    pub avg_chunk_size: usize,
    pub measurement: TimeMeasurement,
    pub throughput: Throughput,
    pub file_name: String,
    pub path: String,
}

impl MeasureResult {
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

        let serializable = SerializableResult::new(self);

        writer.serialize(serializable)?;
        writer.flush()?;

        Ok(())
    }
}

impl Debug for MeasureResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Dataset: {}\n{:?}\nDedup ratio: {:.3}",
            self.name, self.measurement, self.dedup_ratio
        )
    }
}

#[serde_with::serde_as]
#[derive(serde::Serialize)]
struct SerializableResult {
    #[serde_with(as = "serialize_dt")]
    pub date: DateTime<Utc>,
    pub name: String,
    pub chunker: String,
    pub size: usize,
    pub dedup_ratio: f64,
    pub full_dedup_ratio: f64,
    pub avg_chunk_size: usize,
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    pub write_time: Duration,
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    pub read_time: Duration,
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    pub chunk_time: Duration,
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    pub hash_time: Duration,
    pub chunk_throughput: f64,
    pub hash_throughput: f64,
    pub write_throughput: f64,
    pub read_throughput: f64,
    pub path: String,
}

impl SerializableResult {
    fn new(result: &MeasureResult) -> SerializableResult {
        Self {
            date: result.date,
            name: result.name.clone(),
            chunker: result.chunker.clone(),
            size: result.size,
            dedup_ratio: result.dedup_ratio,
            full_dedup_ratio: result.full_dedup_ratio,
            avg_chunk_size: result.avg_chunk_size,
            write_time: result.measurement.write_time,
            read_time: result.measurement.read_time,
            chunk_time: result.measurement.chunk_time,
            hash_time: result.measurement.hash_time,
            chunk_throughput: result.throughput.chunk,
            hash_throughput: result.throughput.hash,
            write_throughput: result.throughput.write,
            read_throughput: result.throughput.read,
            path: result.path.clone(),
        }
    }
}

#[derive(Default, Copy, Clone)]
pub struct TimeMeasurement {
    pub write_time: Duration,
    pub read_time: Duration,
    pub chunk_time: Duration,
    pub hash_time: Duration,
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
            "Read time: {:?}\nWrite time: {:?}\nChunk time: {:?}\nHash time: {:?}",
            self.read_time, self.write_time, self.chunk_time, self.hash_time,
        )
    }
}

impl Sum for TimeMeasurement {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(TimeMeasurement::default(), |acc, next| acc + next)
    }
}

#[derive(Copy, Clone, Default)]
pub struct Throughput {
    pub chunk: f64,
    pub hash: f64,
    pub write: f64,
    pub read: f64,
}

impl Throughput {
    pub fn new(size: usize, measurement: TimeMeasurement) -> Self {
        Self {
            chunk: (size / MB) as f64 / measurement.chunk_time.as_secs_f64(),
            hash: (size / MB) as f64 / measurement.hash_time.as_secs_f64(),
            write: (size / MB) as f64 / measurement.write_time.as_secs_f64(),
            read: (size / MB) as f64 / measurement.read_time.as_secs_f64(),
        }
    }
}

impl Display for Throughput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Chunk throughput: {:.3} MB/s\
        \nHash throughput: {:.3} MB/s\
        \nWrite throughput: {:.3} MB/s\
        \nRead throughput: {:.3} MB/s",
            self.chunk, self.hash, self.write, self.read
        )
    }
}

#[derive(Debug)]
pub struct DedupMeasurement {
    pub name: String,
    pub dedup_ratio: f64,
}
