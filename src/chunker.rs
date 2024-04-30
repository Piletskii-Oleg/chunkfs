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
