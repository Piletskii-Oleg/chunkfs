use std::fmt::{Debug, Formatter};

use cdc_chunkers::SizeParams;

use crate::{Chunk, Chunker, KB};

pub struct FastChunker {
    sizes: SizeParams,
}

impl FastChunker {
    pub fn new(sizes: SizeParams) -> Self {
        FastChunker { sizes }
    }
}

impl Default for FastChunker {
    fn default() -> Self {
        let sizes = SizeParams {
            min: 8 * KB,
            avg: 16 * KB,
            max: 64 * KB,
        };

        Self::new(sizes)
    }
}

impl Chunker for FastChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let (min, avg, max) = (
            self.sizes.min as u32,
            self.sizes.avg as u32,
            self.sizes.max as u32,
        );

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
