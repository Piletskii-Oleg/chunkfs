use std::fmt::Formatter;
use std::io;
use std::time::Duration;

use crate::{ChunkHash};

#[derive(Clone, Debug)]
struct DataStruct<K>(Data<K>);

#[derive(Clone)]
pub enum Data<K> {
    Chunk(Vec<u8>),
    TargetChunk(Vec<K>),
}

pub type ChunkMap<K> = Box<dyn Map<K, Vec<u8>>>;

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

pub trait Scrub<Hash: ChunkHash, K> {
    fn scrub(&mut self, cdc_map: &mut dyn Iterator<Item = (&Hash, &mut DataStruct<K>)>, target_map: &mut ChunkMap<K>) -> ScrubMeasurements;
}

pub struct ChunkStorage<Hash: ChunkHash, CDC: Map<Hash, DataStruct<K>>, K> {
    cdc_map: CDC,
    scrubber: Box<dyn Scrub<Hash, K>>,
    target_map: ChunkMap<K>,
}

impl<Hash: ChunkHash, CDC, K> ChunkStorage<Hash, CDC, K>
where
    CDC: Map<Hash, DataStruct<K>>
{
    pub fn new(cdc_map: CDC, target_map: ChunkMap<K>, scrubber: Box<dyn Scrub<Hash, K>>) -> Self {
        Self {
            cdc_map,
            scrubber,
            target_map,
        }
    }
}

impl<K> DataStruct<K> {
    fn make_target(&mut self, keys: Vec<K>) {
        self.0 = Data::TargetChunk(keys);
    }
}

impl<K> std::fmt::Debug for Data<K> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Data::Chunk(_) => write!(f, "Chunk"),
            Data::TargetChunk(_) => write!(f, "TargetChunk")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::base::HashMapBase;
    use crate::ChunkHash;
    use crate::map::{ChunkMap, ChunkStorage, Data, DataStruct, Scrub, ScrubMeasurements};

    struct DumbScrubber;

    impl<Hash: ChunkHash, K> Scrub<Hash, K> for DumbScrubber {
        fn scrub(&mut self, cdc: &mut dyn Iterator<Item = (&Hash, &mut DataStruct<K>)>, target: &mut ChunkMap<K>) -> ScrubMeasurements {
            for (chunk, data) in cdc {
                data.make_target(vec![]);
            }

           ScrubMeasurements::default()
        }
    }

    #[test]
    fn hashmap_works_as_cdc_map() {
        let mut map = HashMap::new();
        map.insert(1, DataStruct(Data::Chunk(vec![])));
        map.insert(2, DataStruct(Data::Chunk(vec![])));
        let mut chunk_storage: ChunkStorage<_, _, String> = ChunkStorage {
            cdc_map: map,
            scrubber: Box::new(DumbScrubber),
            target_map: Box::new(HashMapBase::default()),
        };

        let measurements = chunk_storage.scrubber.scrub(&mut Box::new(&mut chunk_storage.cdc_map.iter_mut()), &mut chunk_storage.target_map);
        assert_eq!(measurements, ScrubMeasurements::default());

        println!("{:?}", chunk_storage.cdc_map)
    }
}
