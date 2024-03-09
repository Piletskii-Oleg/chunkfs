use std::io::Read;

pub use crate::storage::base::Base;
pub use crate::storage::chunker::Chunker;
pub use crate::storage::hasher::Hasher;
use crate::{Hash, SEG_SIZE};

mod base;
mod chunker;
mod hasher;

#[derive(Clone)]
pub struct Chunk {
    offset: usize,
    length: usize,
}

impl Chunk {
    fn range(&self) -> std::ops::Range<usize> {
        self.offset..self.offset + self.length
    }
}

pub struct Segment {
    hash: Hash,
    data: Vec<u8>,
}

pub struct Span {
    hash: Hash,
    length: usize,
}

pub struct Storage<C, H, B>
where
    C: Iterator<Item = Chunk>,
    H: Hasher,
    B: Base,
{
    chunker: C,
    hasher: H,
    base: B,
    buffer: Vec<u8>,
}

impl<C, H, B> Storage<C, H, B>
where
    C: Iterator<Item = Chunk> + Chunker,
    H: Hasher,
    B: Base,
{
    fn write(&mut self, data: &[u8]) -> std::io::Result<Vec<Span>> {
        // if there is no more data to be written
        if data.is_empty() {
            let hash = self.hasher.hash(&self.buffer);

            let segment = Segment {
                hash: hash.clone(),
                data: self.buffer.clone(),
            };
            self.base.save(vec![segment])?;

            let span = Span {
                hash,
                length: self.buffer.len(),
            };
            return Ok(vec![span]);
        }

        assert_eq!(data.len(), SEG_SIZE); // we assume that all given data segments are 1MB long for now

        self.buffer.extend_from_slice(data); // remove copying? we need to have `rest` stored and indexed
        let data = &self.buffer; // this, or replace all occurrences of data with self.buffer
        let all_chunks = self.chunker.chunk_data(data);
        let (rest, chunks) = all_chunks.split_last().unwrap(); // should always be not empty?

        let hashes = chunks
            .iter()
            .map(|chunk| self.hasher.hash(&data[chunk.range()]))
            .collect::<Vec<Hash>>();

        let segments = hashes
            .into_iter()
            .zip(chunks.iter().map(|chunk| data[chunk.range()].to_vec()))
            .map(|(hash, data)| Segment { hash, data })
            .collect::<Vec<Segment>>();

        // have to copy hashes? or do something else?
        let spans = segments
            .iter()
            .map(|segment| Span {
                hash: segment.hash.clone(),
                length: segment.data.len(),
            })
            .collect();
        self.base.save(segments)?;

        self.buffer = data[rest.range()].to_vec();

        Ok(spans)
    }

    fn retrieve_chunks(&mut self, request: Vec<Hash>) -> Vec<Segment> {
        todo!()
    }
}
