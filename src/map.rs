use std::collections::HashMap;
use std::io;
use std::time::Duration;

use crate::{ChunkHash};

#[derive(Clone)]
pub enum Data {
    Chunk(Vec<u8>),
    TargetChunk,
}

pub type CDCMap<Hash> = Box<dyn Map<Hash, Data, Item=Data>>;
pub type ChunkMap<Hash> = Box<dyn Map<Hash, Vec<u8>, Item=Vec<u8>>>;

pub trait Map<K, V>: Iterator<Item = V> {
    fn insert(&mut self, key: K, value: V) -> io::Result<()>;

    fn get(&self, key: &K) -> io::Result<V>;

    fn remove(&mut self, key: &K);

    fn save(&mut self, keys: Vec<K>, values: Vec<V>) -> io::Result<()> {
        for (key, value) in keys.into_iter().zip(values) {
            self.insert(key, value)?;
        }
        Ok(())
    }

    fn retrieve(&mut self, keys: &[K]) -> io::Result<Vec<V>> {
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
    fn scrub(&mut self, cdc_map: &mut CDCMap<Hash>, target_map: &mut ChunkMap<Hash>) -> ScrubMeasurements;
}

enum MapType {
    Cdc,
    Target
}

pub struct ChunkStorage<Hash: ChunkHash> {
    cdc_map: CDCMap<Hash>,
    scrubber: Box<dyn Scrub<Hash>>,
    target_map: ChunkMap<Hash>,
    correspondence_map: HashMap<Hash, MapType>
}

impl<Hash: ChunkHash> ChunkStorage<Hash> {
    pub fn new(cdc_map: CDCMap<Hash>, target_map: ChunkMap<Hash>, scrubber: Box<dyn Scrub<Hash>>) -> Self {
        Self {
            cdc_map,
            scrubber,
            target_map,
            correspondence_map: HashMap::default(),
        }
    }
}

impl Default for Data {
    fn default() -> Self {
        Data::Chunk(vec![])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::base::HashMapBase;
    use crate::ChunkHash;
    use crate::map::{CDCMap, ChunkMap, ChunkStorage, Scrub, ScrubMeasurements};

    struct DumbScrubber;

    impl<Hash: ChunkHash> Scrub<Hash> for DumbScrubber {
        fn scrub(&mut self, cdc: &mut CDCMap<Hash>, target: &mut ChunkMap<Hash>) -> ScrubMeasurements {
            for chunk in cdc {
                println!("1");
            }
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

        let measurements = chunk_storage.scrubber.scrub(&mut chunk_storage.cdc_map, &mut chunk_storage.target_map);
        assert_eq!(measurements, ScrubMeasurements::default())
    }
}
