use std::cmp::min;

/// A chunk of the processed data. Doesn't store any data,
/// only contains offset and length of the chunk.
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
///
/// Chunks that are found are returned by `chunk_data` method.
/// If some contents were cut because the end of `data` and not the end of the chunk was reached,
/// it must be returned with `rest` method instead of storing it in the `chunk_data`'s output.
pub trait Chunker {
    // how do we measure time? should be added to the trait, probably
    /// Goes through whole `data` and finds chunks. If last chunk is not actually a chunk but a leftover,
    /// it is returned via `rest` method and is not contained in the vector.
    fn chunk_data(&mut self, data: &[u8]) -> Vec<Chunk>;
    // &[u8] or chunk?
    /// Empty if the whole file was successfully chunked,
    /// or contains leftover data that was not enough for chunk to be found,
    /// but had to be cut because no more data is available.
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

        let last_chunk = chunks.last().unwrap();
        if last_chunk.length < self.chunk_size {
            self.rest = data[last_chunk.range()].to_vec();
        } else {
            self.rest = vec![];
        }
        chunks
    }

    fn rest(&self) -> &[u8] {
        &self.rest
    }
}
