use std::fmt::{Debug, Formatter};

use chunking::seq::{Config, OperationMode};
use chunking::{seq, SizeParams};

use crate::{Chunk, Chunker};

pub struct SeqChunker {
    rest: Vec<u8>,
    mode: OperationMode,
    sizes: SizeParams,
    config: Config,
}

impl SeqChunker {
    pub fn new(mode: OperationMode, sizes: SizeParams, config: Config) -> Self {
        Self {
            rest: vec![],
            mode,
            sizes,
            config,
        }
    }
}

impl Default for SeqChunker {
    fn default() -> Self {
        Self::new(OperationMode::Increasing, SizeParams::seq_default(), Config::default())
    }
}

impl Debug for SeqChunker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SeqCDC, sizes: {:?}, mode: {:?}", self.sizes, self.mode)
    }
}

impl Chunker for SeqChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let mut chunks = empty;

        let chunker = seq::Chunker::new(data, self.sizes, self.mode, self.config);
        for chunk in chunker {
            chunks.push(Chunk::new(chunk.pos, chunk.len));
        }

        self.rest = data[chunks.pop().unwrap().range()].to_vec();

        chunks
    }

    fn remainder(&self) -> &[u8] {
        &self.rest
    }

    fn estimate_chunk_count(&self, data: &[u8]) -> usize {
        data.len() / self.sizes.avg
    }
}


