use std::cmp::min;

/// A chunk of the processed data.
#[derive(Copy, Clone, Debug)]
pub struct Chunk {
    offset: usize,
    length: usize,
}

impl Chunk {
    pub(crate) fn new(offset: usize, length: usize) -> Self {
        Self { offset, length }
    }

    /// Effective range of the chunk in the data.
    pub(crate) fn range(&self) -> std::ops::Range<usize> {
        self.offset..self.offset + self.length
    }
}

/// Base functionality for objects that split given data into chunks.
/// Doesn't modify the given data or do anything else.
pub trait Chunker {
    // how do we measure time? should be added to the trait, probably
    fn chunk_data(&mut self, data: &[u8]) -> Vec<Chunk>;
    fn rest(&self) -> &[u8];
}

/// Chunker that utilizes Fixed Sized Chunking (FSC) algorithm,
/// splitting file into even-sized chunks.
pub struct FSChunker {
    chunk_size: usize,
    rest: Vec<u8>,
}

impl FSChunker {
    pub fn new(chunk_size: usize) -> Self {
        Self {
            chunk_size,
            rest: vec![],
        }
    }
}

impl Chunker for FSChunker {
    fn chunk_data(&mut self, data: &[u8]) -> Vec<Chunk> {
        let mut offset = 0;
        let mut chunks = Vec::with_capacity(data.len() / self.chunk_size + 1);
        while offset < data.len() {
            let chunk = Chunk::new(offset, min(self.chunk_size, data.len() - offset));
            chunks.push(chunk);
            offset += self.chunk_size;
        }
        self.rest = data[chunks.last().unwrap().range()].to_vec();
        chunks
    }

    fn rest(&self) -> &[u8] {
        &self.rest
    }
}
