use crate::{Chunk, Chunker};
use cdc_chunkers::SizeParams;
use std::fmt::{Debug, Formatter};

pub struct FastChunker {
    sizes: SizeParams,
}

impl Chunker for FastChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let (min, avg, max) = (self.sizes.min as u32, self.sizes.avg as u32, self.sizes.max as u32);

        let chunker = fastcdc::v2020::FastCDC::new(data, min, avg, max);
        let mut chunks = empty;

        for chunk in chunker {
            chunks.push(Chunk::new(chunk.offset, chunk.length));
        }

        chunks
    }

    fn estimate_chunk_count(&self, data: &[u8]) -> usize {
        data.len() / self.sizes.min
    }
}

impl Debug for FastChunker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FastCDC (2020), sizes: {:?}", self.sizes)
    }
}