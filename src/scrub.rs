use crate::map::{Database};
use crate::storage::DataContainer;
use crate::ChunkHash;
use std::time::Duration;

/// Basic functionality for implementing algorithms which process chunks provided by the [Chunker][crate::Chunker].
///
/// # Method of use
/// The `database` stores [DataContainers][DataContainer], which are either a CDC chunk, that is, a `Vec<u8>`,
/// or a collection of target keys, using which the original chunk could be restored.
///
/// The basic idea behind the scrubber is that it takes chunks from `database` via an iterator and
/// processes them, e.g., splits, or simply transfers them to the `target_map`, leaving only a collection of `Keys` in the initial [DataContainer].
///
/// After moving the data from `database` to `target_map`, we should be able to have access to it via the `database`.
/// Therefore, after moving, we should leave a `Vec<Key>` in place of the source chunk. It is done via [DataContainer::make_target] method.
/// Not using it will lead to either not getting any benefits from the algorithm, or to being unable to access the initial chunk anymore, if it was deleted.
///
/// # Arguments
/// The only method [scrub][Scrub::scrub] takes two arguments:
/// 1. A CDC [Database], which contains `Hash`-[`DataContainer`] pairs. To access the underlying data in the container,
/// [DataContainer::extract] or [DataContainer::extract_mut] should be used.
///
/// 2. A target map, which contains `Key`-`Vec<u8>` pairs, where `Key` is a generic value determined by the implementation.
/// The way data is stored is determined by the target map implementation, the only information known to the scrubber is that
/// the target map implements [Database] trait.
///
/// # Guarantees
/// It is guaranteed that after a chunk has been processed by the scrubber, the keys, stored in the [DataContainer], will be in the linear order,
/// such that it would be possible to get the initial chunk simply by iterating over the stored `Vec<Key>`, retrieving the corresponding data chunks
/// and concatenating them.
pub trait Scrub<Hash: ChunkHash, B, Key>
    where
        B: Database<Hash, DataContainer<Key>>,
        for<'a> &'a mut B: IntoIterator<Item = (&'a Hash, &'a mut DataContainer<Key>)>,
{
    /// # CDC Database
    /// We should be able to iterate over the `database` to process all chunks we had stored before.
    /// The [IntoIterator] trait should be implemented for `database`, but it should not be a big concern, because the structures that are to be implemented
    /// for the algorithm are some `target_map` database and the scrubber itself. `database` should be considered a given entity.
    ///
    /// # Arguments
    /// The method, besides `&mut self`, takes two other arguments:
    /// 1. A CDC [Database], which contains `Hash`-[`DataContainer`] pairs. The [DataContainer] stores either a CDC chunk, that is, a `Vec<u8>`,
    /// or a collection of target keys, using which the original chunk could be restored.
    ///
    /// 2. A target map, which contains `Key`-`Vec<u8>` pairs, where `Key` is a generic key determined by the map implementation.
    /// The way data is stored is determined by the target map implementation, the only information known to the scrubber is that
    /// the target map implements [Database] trait.
    ///
    /// # How to implement
    /// To iterate over the underlying chunks, [database.into_iter()][IntoIterator::into_iter] should be used.
    /// It will automatically yield pairs, which consist of `&mut Hash` and `&mut DataContainer`. To access the underlying data in the container,
    /// [DataContainer::extract] or [DataContainer::extract_mut] should be used.
    ///
    /// If the chunk is suitable for being transferred to the `target_map`, it should NOT be deleted, but instead be replaced by the `target_map`'s keys,
    /// using which the original chunk can be restored. This is accomplished by the [DataContainer::make_target] method.
    ///
    /// It should also gather information to return the [measurements][ScrubMeasurements].
    ///
    /// # Guarantees
    /// It is guaranteed that after a chunk has been processed by the scrubber, the keys, stored in the [DataContainer], will be in the linear order,
    /// such that it would be possible to get the initial chunk simply by iterating over the stored `Vec<Key>`, retrieving the corresponding data chunks
    /// and concatenating them.
    fn scrub<'a>(
        &mut self,
        database: &mut B,
        target_map: &mut Box<dyn Database<Key, Vec<u8>>>,
    ) -> ScrubMeasurements
        where
            Hash: 'a,
            Key: 'a;
}

/// Measurements made by the scrubber.
///
/// Contains information about the amount of data processed by the scrubber (in bytes),
/// time spent on scrubbing,
/// and the amount of data left untouched.
#[derive(Debug, Default, PartialEq, Eq, Copy, Clone)]
pub struct ScrubMeasurements {
    /// How much data was processed by the scrubber (in bytes).
    pub processed_data: usize,
    /// Time spent on scrubbing.
    pub running_time: Duration,
    /// The amount of data left untouched (in bytes).
    pub data_left: usize,
}

pub struct DumbScrubber;

impl<Hash: ChunkHash, B, Key> Scrub<Hash, B, Key> for DumbScrubber
where
    B: Database<Hash, DataContainer<Key>>,
    for<'a> &'a mut B: IntoIterator<Item = (&'a Hash, &'a mut DataContainer<Key>)>,
{
    fn scrub<'a>(
        &mut self,
        _database: &mut B,
        _target: &mut Box<dyn Database<Key, Vec<u8>>>,
    ) -> ScrubMeasurements
    where
        Hash: 'a,
        Key: 'a,
    {
        ScrubMeasurements::default()
    }
}
