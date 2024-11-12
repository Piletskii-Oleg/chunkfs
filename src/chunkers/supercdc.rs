use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use chunking::SizeParams;

use crate::{Chunk, Chunker};

/// Chunker that utilizes Super CDC algorithm.
pub struct SuperChunker {
    rest: Vec<u8>,
    records: Option<HashMap<u64, usize>>,
    sizes: SizeParams,
}

impl SuperChunker {
    pub fn new(sizes: SizeParams) -> Self {
        Self {
            rest: vec![],
            records: Some(HashMap::new()),
            sizes,
        }
    }
}

impl Default for SuperChunker {
    fn default() -> Self {
        Self::new(SizeParams::super_default())
    }
}

impl Debug for SuperChunker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SuperCDC, sizes: {:?}", self.sizes)
    }
}

impl Chunker for SuperChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let mut chunker = chunking::supercdc::Chunker::with_records(
            data,
            self.records.take().unwrap(),
            self.sizes,
        );
        let mut chunks = empty;
        loop {
            match chunker.next() {
                None => break,
                Some(chunk) => chunks.push(Chunk::new(chunk.pos, chunk.len)),
            }
        }

        self.records = Some(chunker.give_records());
        self.rest = data[chunks.pop().unwrap().range()].to_vec();
        chunks
    }

    fn remainder(&self) -> &[u8] {
        &self.rest
    }

    fn estimate_chunk_count(&self, data: &[u8]) -> usize {
        data.len() / self.sizes.min
    }
}
