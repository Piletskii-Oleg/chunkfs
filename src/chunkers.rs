pub use chunking::SizeParams;

pub use fixed_size::FSChunker;
pub use leap::LeapChunker;
pub use rabin::RabinChunker;
pub use supercdc::SuperChunker;
pub use ultra::UltraChunker;

mod fixed_size;
mod leap;
mod rabin;
mod supercdc;
mod ultra;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use sha3::{Digest, Sha3_256};

    use crate::chunkers::RabinChunker;
    use crate::Chunker;

    #[test]
    #[ignore]
    fn dedup_ratio() {
        let mut chunker = RabinChunker::default();

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
