use std::collections::HashMap;
use std::io;
use std::time::{Duration, Instant};

use crate::{ChunkHash, Data};

use super::database::{Database, IterableDatabase};
use super::storage::DataContainer;

/// Basic functionality for implementing algorithms which process chunks provided by the [Chunker][crate::Chunker]. The implementations should encapsulate
/// algorithm logic (write part) inside themselves and not delegate it to `database`. The read part of the algorithm should be encapsulated in `target_map`.
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
///    [DataContainer::extract] or [DataContainer::extract_mut] should be used.
///
/// 2. A target map, which contains `Key`-`Vec<u8>` pairs, where `Key` is a generic value determined by the implementation.
///    The way data is stored is determined by the target map implementation, the only information known to the scrubber is that
///    the target map implements [Database] trait. It should only be used for storage purposes and not contain any algorithm logic.
pub trait Scrub<Hash: ChunkHash, B, Key, T>
where
    Hash: ChunkHash,
    B: IterableDatabase<Hash, DataContainer<Key>>,
    T: Database<Key, Vec<u8>>,
{
    /// # How to implement
    /// To iterate over the underlying chunks, `database.iterator_mut()` should be used.
    /// It will automatically yield pairs, which consist of `&Hash` and `&mut DataContainer`. To access the underlying data in the container,
    /// [DataContainer::extract] or [DataContainer::extract_mut] should be used.
    ///
    /// If the chunk is suitable for being transferred to the `target_map`, it should NOT be deleted, but instead be replaced by the `target_map`'s keys,
    /// using which the original chunk can be restored. This is accomplished by the [DataContainer::make_target] method.
    ///
    /// It should also gather information to return the [measurements][ScrubMeasurements].
    ///
    /// # Arguments
    /// The method, besides `&mut self`, takes two other arguments:
    /// 1. A CDC [Database], which contains `Hash`-[`DataContainer`] pairs. The [DataContainer] stores either a CDC chunk, that is, a `Vec<u8>`,
    ///    or a collection of target keys, using which the original chunk could be restored.
    ///
    /// 2. A target map, which contains `Key`-`Vec<u8>` pairs, where `Key` is a generic key determined by the map implementation.
    ///    The way data is stored is determined by the target map implementation, the only information known to the scrubber is that
    ///    the target map implements [Database] trait.
    ///
    /// # CDC Database
    /// We should be able to iterate over the `database` to process all chunks we had stored before.
    /// The [IntoIterator] trait should be implemented for `database`, but it should not be a big concern, because the only structure that should be implemented
    /// for the algorithm is the scrubber itself. `database` should be considered a given entity, along with the `target_map`.
    fn scrub<'a>(&mut self, database: &mut B, target_map: &mut T) -> io::Result<ScrubMeasurements>
    where
        Hash: 'a,
        Key: 'a;
}

/// Measurements made by the scrubber.
///
/// Contains information about the amount of data processed by the scrubber (in bytes),
/// time spent on scrubbing,
/// the amount of data left untouched and clustering information.
#[derive(Debug, Default, PartialEq, Clone)]
pub struct ScrubMeasurements {
    /// How much data was processed by the scrubber (in bytes).
    pub processed_data: usize,
    /// Time spent on scrubbing.
    pub running_time: Duration,
    /// The amount of data left untouched (in bytes).
    pub data_left: usize,
    /// All information about clusterization:
    /// 1. Total cluster size (number of vertices).
    /// 2. Number of clusters (total number of parent vertices).
    /// 3. The number of vertices within a single cluster.
    /// 4. Distance to the parent vertex.
    /// 5. Distance between clusters (between parent vertices).
    /// 6. Deduplication coefficient for each cluster.
    pub clusterization_report: Option<ClusteringMeasurements>,
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct ClusteringMeasurements {
    /// Number of vertices (chunks).
    pub total_cluster_size: usize,
    /// Total number of parent vertices.
    pub number_of_clusters: usize,
    /// The number of vertices within a single cluster.
    /// It contains the hash values of the parent vertices as keys.
    pub number_of_vertices_in_cluster: HashMap<u32, usize>,
    /// Distance to the parent vertex.
    /// It contains the hash values of the parent vertices as keys.
    pub distance_to_vertices_in_cluster: HashMap<u32, Vec<usize>>,
    /// Distance between clusters (between parent vertices).
    /// The key is the parent in the cluster. The distance is calculated to the other parents.
    pub distance_to_other_clusters: HashMap<u32, Vec<usize>>,
    /// Deduplication coefficient for each cluster.
    pub cluster_dedup_ratio: HashMap<u32, f64>,
}

pub struct DumbScrubber;

pub struct CopyScrubber;

impl<Hash, B, T> Scrub<Hash, B, Hash, T> for CopyScrubber
where
    Hash: ChunkHash,
    B: IterableDatabase<Hash, DataContainer<Hash>>,
    T: Database<Hash, Vec<u8>>,
{
    fn scrub<'a>(&mut self, database: &mut B, target: &mut T) -> io::Result<ScrubMeasurements>
    where
        Hash: 'a,
    {
        let now = Instant::now();
        let mut processed_data = 0;

        for (hash, container) in database.iterator_mut() {
            match container.extract() {
                Data::Chunk(chunk) => {
                    target.insert(hash.clone(), chunk.clone())?;
                    processed_data += chunk.len();
                }
                Data::TargetChunk(_) => (),
            }
            container.make_target(vec![hash.clone()]);
        }

        let running_time = now.elapsed();
        Ok(ScrubMeasurements {
            processed_data,
            running_time,
            data_left: 0,
            clusterization_report: None,
        })
    }
}

impl<Hash, B, Key, T> Scrub<Hash, B, Key, T> for DumbScrubber
where
    Hash: ChunkHash,
    B: IterableDatabase<Hash, DataContainer<Key>>,
    T: Database<Key, Vec<u8>>,
{
    fn scrub<'a>(&mut self, _database: &mut B, _target: &mut T) -> io::Result<ScrubMeasurements>
    where
        Hash: 'a,
        Key: 'a,
    {
        Ok(ScrubMeasurements::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::HashMap;
    use crate::DataContainer;

    fn create_test_data() -> Vec<(Vec<u8>, Vec<u8>)> {
        vec![
            (b"chunk1".to_vec(), b"content1".to_vec()),
            (b"chunk2".to_vec(), b"content2".to_vec()),
            (b"chunk3".to_vec(), b"content3".to_vec()),
            (b"duplicate_chunk".to_vec(), b"same_content".to_vec()),
            (b"another_duplicate".to_vec(), b"same_content".to_vec()),
        ]
    }

    #[test]
    fn scrub_should_return_correct_scrub_measurements_for_copy_scrubber() {
        let test_data = create_test_data();
        let mut total_data_size = 0;

        let mut database: HashMap<Vec<u8>, DataContainer<Vec<u8>>> = HashMap::new();
        for (hash, chunk) in test_data {
            total_data_size += chunk.len();
            database.insert(hash.clone(), DataContainer::from(chunk));
        }

        let mut target_map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut scrubber = CopyScrubber;
        let scrub_report = scrubber.scrub(&mut database, &mut target_map).unwrap();

        assert_eq!(scrub_report.processed_data, total_data_size);
        assert!(scrub_report.running_time > Duration::from_secs(0));
        assert_eq!(scrub_report.data_left, 0);
        assert_eq!(scrub_report.clusterization_report, None);
    }

    #[test]
    fn scrub_should_handle_empty_database() {
        let mut scrubber = CopyScrubber;
        let mut database: HashMap<Vec<u8>, DataContainer<Vec<u8>>> = HashMap::new();
        let mut target_map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        let scrub_report = scrubber.scrub(&mut database, &mut target_map).unwrap();

        assert_eq!(scrub_report.processed_data, 0);
        assert_eq!(scrub_report.data_left, 0);
        assert_eq!(scrub_report.clusterization_report, None);
        assert!(target_map.is_empty());
    }
}
