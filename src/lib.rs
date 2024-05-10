use std::{hash, io};
use std::ops::{Add, AddAssign};
use std::time::Duration;

pub use system::{FileOpener, FileSystem, OpenError};

#[cfg(feature = "chunkers")]
pub mod chunkers;
#[cfg(feature = "hashers")]
pub mod hashers;

pub mod base;
mod file_layer;
mod storage;
mod system;

pub trait ChunkHash: hash::Hash + Clone + Eq + PartialEq + Default {}

impl<T: hash::Hash + Clone + Eq + PartialEq + Default> ChunkHash for T {}

/// Block size, used by [`read`][crate::FileSystem::read_from_file]
/// and [`write`][crate::FileSystem::write_to_file] methods in the [`FileSystem`].
/// Blocks given to the user or by them must be of this size.
const SEG_SIZE: usize = 1024 * 1024; // 1MB

/// A chunk of the processed data. Doesn't store any data,
/// only contains offset and length of the chunk.
#[derive(Copy, Clone, Debug)]
pub struct Chunk {
    offset: usize,
    length: usize,
}

impl Chunk {
    pub fn new(offset: usize, length: usize) -> Self {
        Self { offset, length }
    }

    /// Effective range of the chunk in the data.
    pub fn range(&self) -> std::ops::Range<usize> {
        self.offset..self.offset + self.length
    }

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn offset(&self) -> usize {
        self.offset
    }
}

/// Base functionality for objects that split given data into chunks.
/// Doesn't modify the given data or do anything else.
///
/// Chunks that are found are returned by [`chunk_data`][Chunker::chunk_data] method.
/// If some contents were cut because the end of `data` and not the end of the chunk was reached,
/// it must be returned with [`rest`][Chunker::rest] instead of storing it in the [`chunk_data`][Chunker::chunk_data]'s output.
pub trait Chunker {
    /// Goes through whole `data` and finds chunks. If last chunk is not actually a chunk but a leftover,
    /// it is returned via [`rest`][Chunker::rest] method and is not contained in the vector.
    ///
    /// `empty` is an empty vector whose capacity is determined by [`estimate_chunk_count`][Chunker::estimate_chunk_count].
    /// Resulting chunks should be written right to it, and it should be returned as result.
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk>;

    /// Returns leftover data that was not enough for chunk to be found,
    /// but had to be cut because no more data is available.
    ///
    /// Empty if the whole file was successfully chunked.
    fn remainder(&self) -> &[u8];

    /// Returns an estimate amount of chunks that will be created once the algorithm runs through the whole
    /// data buffer. Used to pre-allocate the buffer with the required size so that allocation times are not counted
    /// towards total chunking time.
    fn estimate_chunk_count(&self, data: &[u8]) -> usize;
}

/// Functionality for an object that hashes the input.
pub trait Hasher {
    type Hash: ChunkHash;

    /// Takes some `data` and returns its `hash`.
    fn hash(&mut self, data: &[u8]) -> Self::Hash;
}

/// A data segment with corresponding hash.
pub struct Segment<Hash: ChunkHash> {
    pub hash: Hash,
    pub data: Vec<u8>,
}

pub enum SegmentData<FBCKey, SBCKey> {
    Chunk(Vec<u8>),
    FBChunk(Vec<FBCKey>),
    SBChunk(Vec<SBCKey>),
}

/// Serves as base functionality for storing the actual data.
pub trait Database<Hash: ChunkHash> {
    /// Saves given data to the underlying storage.
    fn save(&mut self, segments: Vec<Segment<Hash>>) -> io::Result<()>;

    /// Clones and returns the data corresponding to the given hashes, or returns Error(NotFound),
    /// if some of the hashes were not found.
    fn retrieve(&self, request: Vec<Hash>) -> io::Result<Vec<Vec<u8>>>;
}

impl<Hash: ChunkHash> Segment<Hash> {
    pub fn new(hash: Hash, data: Vec<u8>) -> Self {
        Self { hash, data }
    }
}

/// Measurements that are received after writing data to a file.
/// Contain time spent for chunking and for hashing.
#[derive(Debug, PartialEq, Default, Clone, Copy)]
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
