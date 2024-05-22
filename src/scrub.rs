use crate::map::{Database};
use crate::storage::DataContainer;
use crate::ChunkHash;
use std::time::Duration;

#[derive(Debug, Default, PartialEq, Eq, Copy, Clone)]
pub struct ScrubMeasurements {
    processed_data: usize,
    running_time: Duration,
    data_left: usize,
}

pub trait Scrub<Hash: ChunkHash, K, CDC>
where
    CDC: Database<Hash, DataContainer<K>>,
    for<'a> &'a mut CDC: IntoIterator<Item = (&'a Hash, &'a mut DataContainer<K>)>,
{
    fn scrub<'a>(
        &mut self,
        cdc_map: <&'a mut CDC as IntoIterator>::IntoIter,
        target_map: &mut Box<dyn Database<K, Vec<u8>>>,
    ) -> ScrubMeasurements
    where
        Hash: 'a,
        K: 'a;
}

pub struct DumbScrubber;

impl<Hash: ChunkHash, K, B> Scrub<Hash, K, B> for DumbScrubber
where
    B: Database<Hash, DataContainer<K>>,
    for<'a> &'a mut B: IntoIterator<Item = (&'a Hash, &'a mut DataContainer<K>)>,
{
    fn scrub<'a>(
        &mut self,
        cdc: <&'a mut B as IntoIterator>::IntoIter,
        _target: &mut Box<dyn Database<K, Vec<u8>>>,
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
