use std::collections::HashMap;
use std::io;
use std::time::Duration;

use crate::{ChunkHash};

#[derive(Clone)]
pub enum Data {
    Chunk(Vec<u8>),
    TargetChunk,
}

pub type CDCMap<Hash> = Box<dyn Map<Hash, Data>>;
pub type ChunkMap<Hash> = Box<dyn Map<Hash, Vec<u8>>>;

pub trait Map<K, V> {
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

    fn scrub(&mut self, cdc_map: &mut dyn Iterator<Item = (&Hash, &mut Data)>, target_map: &mut ChunkMap<Hash>) -> ScrubMeasurements;
}

enum MapType {
    Cdc,
    Target
}

pub struct ChunkStorage<Hash: ChunkHash, CDC: Map<Hash, Data>> {
    cdc_map: CDC,
    scrubber: Box<dyn Scrub<Hash>>,
    target_map: ChunkMap<Hash>,
    correspondence_map: HashMap<Hash, MapType>
}

impl<Hash: ChunkHash, CDC> ChunkStorage<Hash, CDC>
where
    CDC: Map<Hash, Data>
{
    pub fn new(cdc_map: CDC, target_map: ChunkMap<Hash>, scrubber: Box<dyn Scrub<Hash>>) -> Self {
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
    use crate::map::{ChunkMap, ChunkStorage, Data, Scrub, ScrubMeasurements};

    struct DumbScrubber;

    impl<Hash: ChunkHash> Scrub<Hash> for DumbScrubber {
        fn scrub(&mut self, cdc: &mut dyn Iterator<Item = (&Hash, &mut Data)>, target: &mut ChunkMap<Hash>) -> ScrubMeasurements {
            for (hash, chunk) in cdc {

            }
            ScrubMeasurements::default()
        }
    }

    #[test]
    fn hashmap_works_as_cdc_map() {
        let mut chunk_storage: ChunkStorage<i32, _> = ChunkStorage {
            cdc_map: HashMap::default(),
            scrubber: Box::new(DumbScrubber),
            target_map: Box::new(HashMapBase::default()),
            correspondence_map: HashMap::new()
        };

        let measurements = chunk_storage.scrubber.scrub(&mut Box::new(&mut chunk_storage.cdc_map.iter_mut()), &mut chunk_storage.target_map);
        assert_eq!(measurements, ScrubMeasurements::default())
    }
}
