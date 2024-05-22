use crate::map::{Map, TargetMap};
use crate::storage::DataContainer;
use crate::ChunkHash;
use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;
use std::time::Duration;

#[derive(Debug, Default, PartialEq, Eq, Copy, Clone)]
pub struct ScrubMeasurements {
    processed_data: usize,
    running_time: Duration,
    data_left: usize,
}

pub trait Scrub<Hash: ChunkHash, K, CDC>
where
    CDC: Map<Hash, DataContainer<K>>,
    for<'a> &'a mut CDC: IntoIterator<Item = (&'a Hash, &'a mut DataContainer<K>)>,
{
    fn scrub<'a>(
        &mut self,
        cdc_map: <&'a mut CDC as IntoIterator>::IntoIter,
        target_map: &mut TargetMap<K>,
    ) -> ScrubMeasurements
    where
        Hash: 'a,
        K: 'a;
}

impl<Hash: ChunkHash, V: Clone> Map<Hash, V> for HashMap<Hash, V> {
    fn insert(&mut self, key: Hash, value: V) -> io::Result<()> {
        self.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &Hash) -> io::Result<V> {
        self.get(key).cloned().ok_or(ErrorKind::NotFound.into())
    }

    fn remove(&mut self, key: &Hash) {
        self.remove(key);
    }
}

pub struct DumbScrubber;

impl<Hash: ChunkHash, K, B> Scrub<Hash, K, B> for DumbScrubber
where
    B: Map<Hash, DataContainer<K>>,
    for<'a> &'a mut B: IntoIterator<Item = (&'a Hash, &'a mut DataContainer<K>)>,
{
    fn scrub<'a>(
        &mut self,
        cdc: <&'a mut B as IntoIterator>::IntoIter,
        _target: &mut TargetMap<K>,
    ) -> ScrubMeasurements
    where
        Hash: 'a,
        K: 'a,
    {
        for (_, data) in cdc {
            data.make_target(vec![]);
        }

        ScrubMeasurements::default()
    }
}
