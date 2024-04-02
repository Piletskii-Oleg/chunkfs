use std::time::{Duration, Instant};

pub use crate::chunker::Chunker;
pub use crate::hasher::Hasher;
pub use crate::storage::base::Base;
use crate::storage::base::Segment;
use crate::{VecHash, WriteMeasurements, SEG_SIZE};

pub mod base;

/// Hashed span in a file with a certain length.
#[derive(Debug)]
pub struct Span {
    pub hash: VecHash,
    pub length: usize,
}

#[derive(Debug)]
pub struct SpansInfo {
    pub spans: Vec<Span>,
    pub measurements: WriteMeasurements,
}

impl Span {
    pub fn new(hash: VecHash, length: usize) -> Self {
        Self { hash, length }
    }
}

/// Underlying storage for the actual stored data
#[derive(Debug)]
pub struct Storage<B>
where
    B: Base,
{
    base: B,
}

impl<B> Storage<B>
where
    B: Base,
{
    pub fn new(base: B) -> Self {
        Self { base }
    }

    /// Writes 1 MB of data to the base storage after deduplication.
    ///
    /// Returns resulting lengths of chunks with corresponding hash,
    /// along with amount of time spent on chunking and hashing.
    pub fn write<C: Chunker, H: Hasher>(
        &mut self,
        data: &[u8],
        worker: &mut StorageWriter<C, H>,
    ) -> std::io::Result<SpansInfo> {
        worker.write(data, &mut self.base)
    }

    /// Flushes remaining data to the storage and returns its span with hashing and chunking times.
    pub fn flush<C: Chunker, H: Hasher>(
        &mut self,
        worker: &mut StorageWriter<C, H>,
    ) -> std::io::Result<SpansInfo> {
        worker.flush(&mut self.base)
    }

    /// Retrieves the data from the storage based on hashes of the data segments,
    /// or Error(NotFound) if some of the hashes were not present in the base
    pub fn retrieve(&self, request: Vec<VecHash>) -> std::io::Result<Vec<Vec<u8>>> {
        self.base.retrieve(request)
    }
}

#[derive(Debug)]
pub struct StorageWriter<C, H>
where
    C: Chunker,
    H: Hasher,
{
    chunker: C,
    hasher: H,
    buffer: Vec<u8>,
}

impl<C, H> StorageWriter<C, H>
where
    C: Chunker,
    H: Hasher,
{
    pub fn new(chunker: C, hasher: H) -> Self {
        Self {
            // create during process? and add function to return `rest`
            chunker,
            hasher,
            buffer: vec![],
        }
    }

    /// Writes 1 MB of data to the base storage after deduplication.
    ///
    /// Returns resulting lengths of chunks with corresponding hash,
    /// along with amount of time spent on chunking and hashing.
    pub fn write<'storage, B: Base>(
        &mut self,
        data: &[u8],
        base: &'storage mut B,
    ) -> std::io::Result<SpansInfo> {
        debug_assert!(data.len() == SEG_SIZE); // we assume that all given data segments are 1MB long for now

        self.buffer.extend_from_slice(data); // remove copying? we need to have `rest` stored and indexed

        let empty = Vec::with_capacity(self.chunker.estimate_chunk_count(&self.buffer));

        let start = Instant::now();
        let chunks = self.chunker.chunk_data(&self.buffer, empty);
        let chunk_time = start.elapsed();

        let start = Instant::now();
        let hashes = chunks
            .iter()
            .map(|chunk| self.hasher.hash(&self.buffer[chunk.range()]))
            .collect::<Vec<VecHash>>();
        let hash_time = start.elapsed();

        let segments = hashes
            .into_iter()
            .zip(
                chunks
                    .iter()
                    .map(|chunk| self.buffer[chunk.range()].to_vec()), // cloning buffer data again
            )
            .map(|(hash, data)| Segment::new(hash, data))
            .collect::<Vec<Segment>>();

        // have to copy hashes? or do something else?
        let spans = segments
            .iter()
            .map(|segment| Span::new(segment.hash.clone(), segment.data.len()))
            .collect();
        base.save(segments)?;

        self.buffer = self.chunker.rest().to_vec();

        Ok(SpansInfo {
            spans,
            measurements: WriteMeasurements::new(chunk_time, hash_time),
        })
    }

    /// Flushes remaining data to the storage and returns its span with hashing and chunking times.
    pub fn flush<B: Base>(&mut self, base: &mut B) -> std::io::Result<SpansInfo> {
        let start = Instant::now();
        let hash = self.hasher.hash(&self.buffer);
        let hash_time = start.elapsed();

        let segment = Segment::new(hash.clone(), self.buffer.clone());
        base.save(vec![segment])?;

        let span = Span::new(hash, self.buffer.len());
        self.buffer = vec![];
        Ok(SpansInfo {
            spans: vec![span],
            measurements: WriteMeasurements::new(Duration::default(), hash_time),
        })
    }
}
