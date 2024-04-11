use std::marker::PhantomData;
use std::time::{Duration, Instant};
use std::{hash, io};

pub use crate::chunker::Chunker;
pub use crate::hasher::Hasher;
pub use crate::storage::base::Database;
use crate::storage::base::Segment;
use crate::{WriteMeasurements, SEG_SIZE};

pub mod base;

/// Hashed span in a [`file`][crate::file_layer::File] with a certain length.
#[derive(Debug)]
pub struct Span<Hash: hash::Hash + Clone + Eq + PartialEq + Default> {
    pub hash: Hash,
    pub length: usize,
}

/// Spans received after [Storage::write] or [Storage::flush], along with time measurements.
#[derive(Debug)]
pub struct SpansInfo<Hash: hash::Hash + Clone + Eq + PartialEq + Default> {
    pub spans: Vec<Span<Hash>>,
    pub measurements: WriteMeasurements,
}

impl<Hash: hash::Hash + Clone + Eq + PartialEq + Default> Span<Hash> {
    pub fn new(hash: Hash, length: usize) -> Self {
        Self { hash, length }
    }
}

/// Underlying storage for the actual stored data.
#[derive(Debug)]
pub struct Storage<B, Hash>
where
    B: Database<Hash>,
    Hash: hash::Hash + Clone + Eq + PartialEq + Default,
{
    base: B,
    hash_phantom: PhantomData<Hash>,
}

impl<B, Hash> Storage<B, Hash>
where
    B: Database<Hash>,
    Hash: hash::Hash + Clone + Eq + PartialEq + Default,
{
    pub fn new(base: B) -> Self {
        Self {
            base,
            hash_phantom: Default::default(),
        }
    }

    /// Writes 1 MB of data to the [`base`][crate::base::Base] storage after deduplication.
    ///
    /// Returns resulting lengths of [chunks][crate::chunker::Chunk] with corresponding hash,
    /// along with amount of time spent on chunking and hashing.
    pub fn write<C: Chunker, H: Hasher<Hash = Hash>>(
        &mut self,
        data: &[u8],
        writer: &mut StorageWriter<C, H, Hash>,
    ) -> io::Result<SpansInfo<Hash>> {
        writer.write(data, &mut self.base)
    }

    /// Flushes remaining data to the storage and returns its [`span`][Span] with hashing and chunking times.
    pub fn flush<C: Chunker, H: Hasher<Hash = Hash>>(
        &mut self,
        writer: &mut StorageWriter<C, H, Hash>,
    ) -> io::Result<SpansInfo<Hash>> {
        writer.flush(&mut self.base)
    }

    /// Retrieves the data from the storage based on hashes of the data [`segments`][Segment],
    /// or Error(NotFound) if some of the hashes were not present in the base.
    pub fn retrieve(&self, request: Vec<Hash>) -> io::Result<Vec<Vec<u8>>> {
        self.base.retrieve(request)
    }
}

/// Writer that conducts operations on [Storage].
/// Only exists during [FileSystem::write_to_file][crate::FileSystem::write_to_file].
/// Receives `buffer` from [FileHandle][crate::file_layer::FileHandle] and gives it back after a successful write.
#[derive(Debug)]
pub struct StorageWriter<'handle, C, H, Hash>
where
    C: Chunker,
    H: Hasher,
    Hash: hash::Hash + Clone + Eq + PartialEq + Default,
{
    chunker: &'handle mut C,
    hasher: &'handle mut H,
    hash_phantom: PhantomData<Hash>,
}

impl<'handle, C, H, Hash> StorageWriter<'handle, C, H, Hash>
where
    C: Chunker,
    H: Hasher<Hash = Hash>,
    Hash: hash::Hash + Clone + Eq + PartialEq + Default,
{
    pub fn new(chunker: &'handle mut C, hasher: &'handle mut H) -> Self {
        Self {
            chunker,
            hasher,
            hash_phantom: Default::default(),
        }
    }

    /// Writes 1 MB of data to the [`base`][crate::base::Base] storage after deduplication.
    ///
    /// Returns resulting lengths of [chunks][crate::chunker::Chunk] with corresponding hash,
    /// along with amount of time spent on chunking and hashing.
    pub fn write<B: Database<Hash>>(
        &mut self,
        data: &[u8],
        base: &mut B,
    ) -> io::Result<SpansInfo<Hash>> {
        debug_assert!(data.len() == SEG_SIZE); // we assume that all given data segments are 1MB long for now

        let mut buffer = self.chunker.remainder().to_vec();
        buffer.extend_from_slice(data);

        let empty = Vec::with_capacity(self.chunker.estimate_chunk_count(&buffer));

        let start = Instant::now();
        let chunks = self.chunker.chunk_data(&buffer, empty);
        let chunk_time = start.elapsed();

        let start = Instant::now();
        let hashes = chunks
            .iter()
            .map(|chunk| self.hasher.hash(&buffer[chunk.range()]))
            .collect::<Vec<_>>();
        let hash_time = start.elapsed();

        let segments = hashes
            .into_iter()
            .zip(
                chunks.iter().map(|chunk| buffer[chunk.range()].to_vec()), // cloning buffer data again
            )
            .map(|(hash, data)| Segment::new(hash, data))
            .collect::<Vec<_>>();

        // have to copy hashes? or do something else?
        let spans = segments
            .iter()
            .map(|segment| Span::new(segment.hash.clone(), segment.data.len()))
            .collect();
        base.save(segments)?;

        Ok(SpansInfo {
            spans,
            measurements: WriteMeasurements::new(chunk_time, hash_time),
        })
    }

    /// Flushes remaining data to the storage and returns its [`span`][Span] with hashing and chunking times.
    pub fn flush<B: Database<Hash>>(&mut self, base: &mut B) -> io::Result<SpansInfo<Hash>> {
        // is this necessary?
        if self.chunker.remainder().is_empty() {
            return Ok(SpansInfo {
                spans: vec![],
                measurements: Default::default(),
            });
        }

        let remainder = self.chunker.remainder().to_vec();
        let start = Instant::now();
        let hash = self.hasher.hash(&remainder);
        let hash_time = start.elapsed();

        let segment = Segment::new(hash.clone(), remainder.clone());
        base.save(vec![segment])?;

        let span = Span::new(hash, remainder.len());
        Ok(SpansInfo {
            spans: vec![span],
            measurements: WriteMeasurements::new(Duration::default(), hash_time),
        })
    }
}
