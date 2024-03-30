use std::time::{Duration, Instant};

pub use crate::storage::base::Base;
use crate::storage::base::Segment;
pub use crate::storage::chunker::Chunker;
pub use crate::storage::hasher::Hasher;
use crate::{VecHash, SEG_SIZE};

pub mod base;
pub mod chunker;
pub mod hasher;

/// Hashed span in a file with a certain length.
#[derive(Debug)]
pub struct Span {
    pub hash: VecHash,
    pub length: usize,
}

#[derive(Debug)]
pub struct SpansInfo {
    pub spans: Vec<Span>,
    pub chunk_time: Duration,
    pub hash_time: Duration,
}

impl Span {
    pub fn new(hash: VecHash, length: usize) -> Self {
        Self { hash, length }
    }
}

/// Underlying storage for the actual stored data
#[derive(Debug)]
pub struct Storage<C, H, B>
where
    C: Chunker,
    H: Hasher,
    B: Base,
{
    chunker: C,
    hasher: H,
    base: B,
    // for one file at a time, else it would break when flush is used
    buffer: Vec<u8>,
}

impl<C, H, B> Storage<C, H, B>
where
    C: Chunker,
    H: Hasher,
    B: Base,
{
    pub fn new(chunker: C, hasher: H, base: B) -> Self {
        Self {
            // create during process? and add function to return `rest`
            chunker,
            hasher,
            base,
            buffer: vec![],
        }
    }

    /// Writes 1 MB of data to the base storage after deduplication.
    ///
    /// Returns resulting lengths of chunks with corresponding hash,
    /// along with amount of time spent on chunking and hashing.
    pub fn write(&mut self, data: &[u8]) -> std::io::Result<SpansInfo> {
        debug_assert!(data.len() == SEG_SIZE); // we assume that all given data segments are 1MB long for now

        self.buffer.extend_from_slice(data); // remove copying? we need to have `rest` stored and indexed

        let empty = Vec::with_capacity(self.chunker.estimate_chunk_count(data));

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
        self.base.save(segments)?;

        self.buffer = self.chunker.rest().to_vec();

        Ok(SpansInfo {
            spans,
            chunk_time,
            hash_time,
        })
    }

    /// Flushes remaining data to the storage and returns its span with hashing and chunking times.
    pub fn flush(&mut self) -> std::io::Result<SpansInfo> {
        let start = Instant::now();
        let hash = self.hasher.hash(&self.buffer);
        let hash_time = start.elapsed();

        let segment = Segment::new(hash.clone(), self.buffer.clone());
        self.base.save(vec![segment])?;

        let span = Span::new(hash, self.buffer.len());
        self.buffer = vec![];
        Ok(SpansInfo {
            spans: vec![span],
            chunk_time: Duration::default(),
            hash_time,
        })
    }

    /// Retrieves the data from the storage based on hashes of the data segments,
    /// or Error(NotFound) if some of the hashes were not present in the base
    pub fn retrieve(&self, request: Vec<VecHash>) -> std::io::Result<Vec<Vec<u8>>> {
        self.base.retrieve(request)
    }
}
