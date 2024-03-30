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
#[derive(Debug)]
pub struct WriteMeasurements {
    chunk_time: Duration,
    hash_time: Duration,
}

impl WriteMeasurements {
    pub fn chunk_time(&self) -> Duration {
        self.chunk_time
    }

    pub fn hash_time(&self) -> Duration {
        self.hash_time
    }
}
