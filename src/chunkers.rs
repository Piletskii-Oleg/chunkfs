use std::cmp::min;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use crate::{Chunk, Chunker};

/// Chunker that utilizes Fixed Sized Chunking (FSC) algorithm,
/// splitting file into even-sized chunks.
#[derive(Debug)]
pub struct FSChunker {
    chunk_size: usize,
    rest: Vec<u8>,
}

#[derive(Default, Debug)]
pub struct LeapChunker {
    rest: Vec<u8>,
}

#[derive(Debug)]
pub struct SuperChunker {
    rest: Vec<u8>,
    records: Option<HashMap<u64, usize>>,
}

pub struct RabinChunker {
    rest: Vec<u8>,
    params: Option<chunking::rabin::ChunkerParams>,
}

#[derive(Default)]
pub struct UltraChunker {
    rest: Vec<u8>,
}

impl RabinChunker {
    pub fn new() -> Self {
        Self {
            rest: vec![],
            params: Some(chunking::rabin::ChunkerParams::new()),
        }
    }
}

impl FSChunker {
    pub fn new(chunk_size: usize) -> Self {
        Self {
            chunk_size,
            rest: vec![],
        }
    }
}

impl SuperChunker {
    pub fn new() -> Self {
        Self {
            rest: vec![],
            records: Some(HashMap::new()),
        }
    }
}

impl Chunker for FSChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let mut offset = 0;
        let mut chunks = empty;
        while offset < data.len() {
            let chunk = Chunk::new(offset, min(self.chunk_size, data.len() - offset));
            chunks.push(chunk);
            offset += self.chunk_size;
        }

        let last_chunk = chunks.pop().unwrap();
        if last_chunk.length() < self.chunk_size {
            self.rest = data[last_chunk.range()].to_vec();
        } else {
            chunks.push(last_chunk);
            self.rest = vec![];
        }
        chunks
    }

    fn remainder(&self) -> &[u8] {
        &self.rest
    }

    fn estimate_chunk_count(&self, data: &[u8]) -> usize {
        data.len() / self.chunk_size + 1
    }
}

impl Chunker for UltraChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let chunker = chunking::ultra::Chunker::new(data);
        let mut chunks = empty;
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
        data.len() / 4096
    }
}

impl Chunker for LeapChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let chunker = chunking::leap_based::Chunker::new(data);
        let mut chunks = empty;
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
        data.len() / 1024 * 8
    }
}

impl Chunker for SuperChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let mut chunker =
            chunking::supercdc::Chunker::with_records(data, self.records.take().unwrap());
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
        data.len() / 2048
    }
}

impl Chunker for RabinChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let mut chunker = chunking::rabin::Chunker::with_params(data, self.params.take().unwrap());
        let mut chunks = empty;
        loop {
            match chunker.next() {
                None => break,
                Some(chunk) => chunks.push(Chunk::new(chunk.pos, chunk.len)),
            }
        }

        self.params = Some(chunker.give_params());
        self.rest = data[chunks.pop().unwrap().range()].to_vec();
        chunks
    }

    fn remainder(&self) -> &[u8] {
        &self.rest
    }

    fn estimate_chunk_count(&self, data: &[u8]) -> usize {
        data.len() / 16384
    }
}

impl Debug for RabinChunker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RabinCDC")
    }
}

impl Debug for UltraChunker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UltraCDC")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use chunking::ultra;
    use sha3::{Sha3_256, Digest};
    use crate::Chunker;
    use crate::chunkers::{LeapChunker, RabinChunker, SuperChunker, UltraChunker};

    #[test]
    #[ignore]
    fn dedup_ratio() {
        let mut chunker = RabinChunker::new();

        let data = std::fs::read("linux.tar").unwrap();

        let chunks = chunker.chunk_data(&data, vec![]);

        let chunks_len = chunks.len();
        let chunks_map: HashMap<_, usize> = HashMap::from_iter(chunks.into_iter().map(|chunk| {
            let hash = Sha3_256::digest(&data[chunk.offset..chunk.offset + chunk.length]);
            let mut res = vec![0u8; hash.len()];
            res.copy_from_slice(&hash);
            (res, chunk.length)
        }));
        println!(
            "Chunk ratio (unique / all): {} / {} = {:.3}",
            chunks_map.len(),
            chunks_len,
            chunks_map.len() as f64 / chunks_len as f64
        );
        println!(
            "Data size ratio: {} / {} = {:.3}",
            chunks_map.iter().map(|(_, &b)| b).sum::<usize>(),
            data.len(),
            chunks_map.iter().map(|(_, &b)| b).sum::<usize>() as f64 / data.len() as f64
        );
    }
}
