use std::io;
use std::time::{Duration, Instant};

use crate::ChunkHash;
pub use crate::Chunker;
pub use crate::Database;
pub use crate::Hasher;
use crate::Segment;
use crate::{WriteMeasurements, SEG_SIZE};

/// Hashed span in a [`file`][crate::file_layer::File] with a certain length.
#[derive(Debug)]
pub struct Span<Hash: ChunkHash> {
    pub hash: Hash,
    pub length: usize,
}

/// Spans received after [Storage::write] or [Storage::flush], along with time measurements.
#[derive(Debug)]
pub struct SpansInfo<Hash: ChunkHash> {
    pub spans: Vec<Span<Hash>>,
    pub measurements: WriteMeasurements,
}

impl<Hash: ChunkHash> Span<Hash> {
    pub fn new(hash: Hash, length: usize) -> Self {
        Self { hash, length }
    }
}

/// Underlying storage for the actual stored data.
#[derive(Debug)]
pub struct Storage<B, H, Hash>
where
    B: Database<Hash>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
{
    base: B,
    hasher: H,
}

impl<B, H, Hash> Storage<B, H, Hash>
where
    B: Database<Hash>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
{
    pub fn new(base: B, hasher: H) -> Self {
        Self { base, hasher }
    }

    /// Writes 1 MB of data to the [`base`][crate::base::Base] storage after deduplication.
    ///
    /// Returns resulting lengths of [chunks][crate::chunker::Chunk] with corresponding hash,
    /// along with amount of time spent on chunking and hashing.
    pub fn write<C: Chunker>(
        &mut self,
        data: &[u8],
        chunker: &mut C,
    ) -> io::Result<SpansInfo<Hash>> {
        let mut writer = StorageWriter::new(chunker, &mut self.hasher);
        writer.write(data, &mut self.base)
    }

    /// Flushes remaining data to the storage and returns its [`span`][Span] with hashing and chunking times.
    pub fn flush<C: Chunker>(&mut self, chunker: &mut C) -> io::Result<SpansInfo<Hash>> {
        let mut writer = StorageWriter::new(chunker, &mut self.hasher);
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
struct StorageWriter<'handle, C, H>
where
    C: Chunker,
    H: Hasher,
{
    chunker: &'handle mut C,
    hasher: &'handle mut H,
}

impl<'handle, C, H> StorageWriter<'handle, C, H>
where
    C: Chunker,
    H: Hasher,
{
    fn new(chunker: &'handle mut C, hasher: &'handle mut H) -> Self {
        Self { chunker, hasher }
    }

    /// Writes 1 MB of data to the [`base`][crate::base::Base] storage after deduplication.
    ///
    /// Returns resulting lengths of [chunks][crate::chunker::Chunk] with corresponding hash,
    /// along with amount of time spent on chunking and hashing.
    fn write<B: Database<H::Hash>>(
        &mut self,
        data: &[u8],
        base: &mut B,
    ) -> io::Result<SpansInfo<H::Hash>> {
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
    fn flush<B: Database<H::Hash>>(&mut self, base: &mut B) -> io::Result<SpansInfo<H::Hash>> {
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
