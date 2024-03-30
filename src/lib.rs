use std::ops::{Add, AddAssign};
use std::time::Duration;

pub use storage::{base, chunker, hasher};
pub use system::{FileSystem, FileSystemBuilder};

mod file_layer;
mod storage;
mod system;

/// Block size, used by `read` and `write` methods in the `FileSystem`.
/// Blocks given to the user or by them must be of this size.
pub const SEG_SIZE: usize = 1024 * 1024; // 1MB

pub type VecHash = Vec<u8>;

/// Measurements that are received after writing data to a file.
/// Contain time spent for chunking and for hashing.
#[derive(Debug, PartialEq, Default, Clone)] // copy?
pub struct WriteMeasurements {
    chunk_time: Duration,
    hash_time: Duration,
}

impl WriteMeasurements {
    pub(crate) fn new(chunk_time: Duration, hash_time: Duration) -> Self {
        Self {
            chunk_time,
            hash_time,
        }
    }

    pub fn chunk_time(&self) -> Duration {
        self.chunk_time
    }

    pub fn hash_time(&self) -> Duration {
        self.hash_time
    }
}

impl Add for WriteMeasurements {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            chunk_time: self.chunk_time + rhs.chunk_time,
            hash_time: self.hash_time + rhs.hash_time,
        }
    }
}

impl AddAssign for WriteMeasurements {
    fn add_assign(&mut self, rhs: Self) {
        self.chunk_time += rhs.chunk_time;
        self.hash_time += rhs.hash_time;
    }
}
