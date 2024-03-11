pub use crate::storage::base::Base;
use crate::storage::base::Segment;
pub use crate::storage::chunker::Chunker;
pub use crate::storage::hasher::Hasher;
use crate::{Hash, SEG_SIZE};

pub mod base;
pub mod chunker;
mod hasher;

/// Hashed span in a file
#[derive(Debug)]
pub struct Span {
    pub hash: Hash,
    pub length: usize,
}

impl Span {
    pub fn new(hash: Hash, length: usize) -> Self {
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
            chunker,
            hasher,
            base,
            buffer: vec![],
        }
    }

    /// Writes 1 MB of data to the base storage after deduplication.
    ///
    /// Returns resulting lengths of chunks with corresponding hash
    pub fn write(&mut self, data: &[u8]) -> std::io::Result<Vec<Span>> {
        // if there is no more data to be written
        if data.is_empty() {
            return Ok(vec![]);
        }

        assert_eq!(data.len(), SEG_SIZE); // we assume that all given data segments are 1MB long for now

        self.buffer.extend_from_slice(data); // remove copying? we need to have `rest` stored and indexed
        let data = &self.buffer; // this, or replace all occurrences of data with self.buffer
        let all_chunks = self.chunker.chunk_data(data);
        let (rest, chunks) = all_chunks.split_last().unwrap(); // should always be not empty? for now at least, when data.len() is 1 MB

        let hashes = chunks
            .iter()
            .map(|chunk| self.hasher.hash(&data[chunk.range()]))
            .collect::<Vec<Hash>>();

        let segments = hashes
            .into_iter()
            .zip(chunks.iter().map(|chunk| data[chunk.range()].to_vec()))
            .map(|(hash, data)| Segment::new(hash, data))
            .collect::<Vec<Segment>>();

        // have to copy hashes? or do something else?
        let spans = segments
            .iter()
            .map(|segment| Span::new(segment.hash.clone(), segment.data.len()))
            .collect();
        self.base.save(segments)?;

        self.buffer = data[rest.range()].to_vec();

        Ok(spans)
    }

    pub fn flush(&mut self) -> std::io::Result<Span> {
        let hash = self.hasher.hash(&self.buffer);

        let segment = Segment::new(hash.clone(), self.buffer.clone());
        self.base.save(vec![segment])?;

        let span = Span::new(hash, self.buffer.len());
        self.buffer = vec![];
        return Ok(span);
    }

    pub fn retrieve_chunks(&mut self, request: Vec<Hash>) -> std::io::Result<Vec<Vec<u8>>> {
        self.base.retrieve(request)
    }
}
