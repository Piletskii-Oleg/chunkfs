use std::collections::HashMap;
use std::io;
use std::time::Duration;

use crate::ChunkHash;

pub type CDCMap<Hash> = Box<dyn Map<Hash, Vec<u8>>>;
pub type ChunkMap<Hash> = Box<dyn Map<Hash, Vec<u8>>>;

pub trait Map<K, V> {
    fn insert(&mut self, key: K, value: V) -> io::Result<()>;

    fn get(&self, key: &K) -> Option<V>;

    fn remove(&mut self, key: &K);

    fn save(&mut self, keys: Vec<K>, values: Vec<V>) -> io::Result<()> {
        for (key, value) in keys.into_iter().zip(values) {
            self.insert(key, value)?;
        }
        Ok(())
    }

    fn retrieve(&mut self, keys: &[K]) -> Vec<Option<V>> {
        keys.iter().map(|key| self.get(key)).collect()
    }
}

#[derive(Debug, Default, PartialEq, Eq, Copy, Clone)]
pub struct ScrubMeasurements {
    processed_data: usize,
    running_time: Duration,
    data_left: usize,
}

pub trait Scrub<Hash: ChunkHash> {
    fn scrub(&mut self, cdc_map: &ChunkMap<Hash>, target_map: &ChunkMap<Hash>) -> ScrubMeasurements;
}

enum MapType {
    CDC,
    Target
}

pub struct ChunkStorage<Hash: ChunkHash> {
    cdc_map: ChunkMap<Hash>,
    scrubber: Box<dyn Scrub<Hash>>,
    target_map: ChunkMap<Hash>,
    correspondence_map: HashMap<Hash, MapType>
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::base::HashMapBase;
    use crate::ChunkHash;
    use crate::map::{ChunkMap, ChunkStorage, Scrub, ScrubMeasurements};

    struct DumbScrubber;

    impl<Hash: ChunkHash> Scrub<Hash> for DumbScrubber {
        fn scrub(&mut self, cdc: &ChunkMap<Hash>, target: &ChunkMap<Hash>) -> ScrubMeasurements {
            ScrubMeasurements::default()
        }
    }

    #[test]
    fn hashmap_works_as_cdc_map() {
        let mut chunk_storage: ChunkStorage<i32> = ChunkStorage {
            cdc_map: Box::new(HashMapBase::default()),
            scrubber: Box::new(DumbScrubber),
            target_map: Box::new(HashMapBase::default()),
            correspondence_map: HashMap::new()
        };

        let measurements = chunk_storage.scrubber.scrub(&chunk_storage.cdc_map, &chunk_storage.target_map);
        assert_eq!(measurements, ScrubMeasurements::default())
    }
}
