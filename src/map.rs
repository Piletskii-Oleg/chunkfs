use std::time::Duration;

use crate::ChunkHash;

pub type CDCMap<Hash> = Box<dyn Map<Hash, Vec<u8>>>;

pub trait Map<K, V> {
    fn add(&mut self, key: K, value: V);

    fn retrieve(&self, key: K) -> Option<V>;

    fn remove(&mut self, key: K);
}

#[derive(Debug, Default, PartialEq, Eq, Copy, Clone)]
pub struct ScrubMeasurements {
    processed_data: usize,
    running_time: Duration,
    data_left: usize,
}

pub trait Scrub<Hash: ChunkHash> {
    fn scrub(&mut self, cdc_map: &CDCMap<Hash>) -> ScrubMeasurements;
}

pub struct ChunkStorage<Hash: ChunkHash> {
    cdc_map: CDCMap<Hash>,
    scrubber: Box<dyn Scrub<Hash>>,
    // fbc_map
    // sbc_map
}

#[cfg(test)]
mod tests {
    use crate::base::HashMapBase;
    use crate::ChunkHash;
    use crate::map::{CDCMap, ChunkStorage, Scrub, ScrubMeasurements};

    struct DumbScrubber;

    impl<Hash: ChunkHash> Scrub<Hash> for DumbScrubber {
        fn scrub(&mut self, _: &CDCMap<Hash>) -> ScrubMeasurements {
            ScrubMeasurements::default()
        }
    }

    fn hashmap_works_as_cdc_map() {
        let mut chunk_storage: ChunkStorage<i32> = ChunkStorage {
            cdc_map: Box::new(HashMapBase::default()),
            scrubber: Box::new(DumbScrubber),
        };

        let measurements = chunk_storage.scrubber.scrub(&chunk_storage.cdc_map);
        assert_eq!(measurements, ScrubMeasurements::default())
    }
}
