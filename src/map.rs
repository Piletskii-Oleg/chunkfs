use std::fmt::Formatter;
use std::io;
use std::time::{Duration, Instant};

use crate::{Chunker, ChunkHash, Database, Hasher, Segment, WriteMeasurements};
use crate::storage::{Span, SpansInfo, Storage};

#[derive(Clone, Debug)]
pub struct DataContainer<K>(Data<K>);

#[derive(Clone)]
pub enum Data<K> {
    Chunk(Vec<u8>),
    TargetChunk(Vec<K>),
}

pub type TargetMap<K> = Box<dyn Map<K, Vec<u8>>>;

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

    fn retrieve(&self, keys: &[K]) -> io::Result<Vec<V>> {
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
    fn scrub(
        &mut self,
        cdc_map: &mut dyn Iterator<Item = (&Hash, &mut DataContainer<K>)>,
        target_map: &mut TargetMap<K>,
    ) -> ScrubMeasurements;
}

pub struct ChunkStorage<H, Hash, CDC, K>
where
    H: Hasher,
    Hash: ChunkHash,
    CDC: Map<H::Hash, DataContainer<K>>,
{
    cdc_map: CDC,
    scrubber: Box<dyn Scrub<Hash, K>>,
    target_map: TargetMap<K>,
    hasher: H
}

impl<H, Hash, CDC, K> ChunkStorage<H, Hash, CDC, K>
where
    H: Hasher,
    Hash: ChunkHash,
    CDC: Map<H::Hash, DataContainer<K>>,
{
    pub fn new(cdc_map: CDC, target_map: TargetMap<K>, scrubber: Box<dyn Scrub<Hash, K>>, hasher: H) -> Self {
        Self {
            cdc_map,
            scrubber,
            target_map,
            hasher
        }
    }

    /// Writes 1 MB of data to the [`base`][crate::base::Base] storage after deduplication.
    ///
    /// Returns resulting lengths of [chunks][crate::chunker::Chunk] with corresponding hash,
    /// along with amount of time spent on chunking and hashing.
    pub fn write<C: Chunker>(
        &mut self,
        data: &[u8],
        chunker: &mut C,
    ) -> io::Result<SpansInfo<H::Hash>> {
        let mut writer = StorageWriter::new(chunker, &mut self.hasher);
        writer.write(data, &mut self.cdc_map)
    }

    /// Flushes remaining data to the storage and returns its [`span`][Span] with hashing and chunking times.
    pub fn flush<C: Chunker>(&mut self, chunker: &mut C) -> io::Result<SpansInfo<H::Hash>> {
        let mut writer = StorageWriter::new(chunker, &mut self.hasher);
        writer.flush(&mut self.cdc_map)
    }

    /// Retrieves the data from the storage based on hashes of the data [`segments`][Segment],
    /// or Error(NotFound) if some of the hashes were not present in the base.
    pub fn retrieve(&self, request: &[H::Hash]) -> io::Result<Vec<Vec<u8>>> {
        let retrieved = self.cdc_map.retrieve(request)?;

        retrieved.into_iter().map(|container| match container.0 {
            Data::Chunk(chunk) => Ok(chunk),
            Data::TargetChunk(keys) => Ok(self.target_map.retrieve(&keys)?.concat())
        }).collect()
    }
}

/// Writer that conducts operations on [Storage].
/// Only exists during [FileSystem::write_to_file][crate::FileSystem::write_to_file].
/// Receives `buffer` from [FileHandle][crate::file_layer::FileHandle] and gives it back after a successful write.
#[derive(Debug)]
struct StorageWriter<'handle, C, H>
    where
        C: Chunker,
        H: Hasher,
{
    chunker: &'handle mut C,
    hasher: &'handle mut H,
}

impl<'handle, C, H> StorageWriter<'handle, C, H>
    where
        C: Chunker,
        H: Hasher,
{
    fn new(chunker: &'handle mut C, hasher: &'handle mut H) -> Self {
        Self { chunker, hasher }
    }

    /// Writes 1 MB of data to the [`base`][crate::base::Base] storage after deduplication.
    ///
    /// Returns resulting lengths of [chunks][crate::chunker::Chunk] with corresponding hash,
    /// along with amount of time spent on chunking and hashing.
    fn write<K, B: Map<H::Hash, DataContainer<K>>>(
        &mut self,
        data: &[u8],
        base: &mut B,
    ) -> io::Result<SpansInfo<H::Hash>> {
        //debug_assert!(data.len() == SEG_SIZE); // we assume that all given data segments are 1MB long for now

        let mut buffer = self.chunker.remainder().to_vec();
        buffer.extend_from_slice(data);

        let empty = Vec::with_capacity(self.chunker.estimate_chunk_count(&buffer));

        let start = Instant::now();
        let chunks = self.chunker.chunk_data(&buffer, empty);
        let chunk_time = start.elapsed();

        let start = Instant::now();
        let hashes = chunks
            .iter()
            .map(|chunk| self.hasher.hash(&buffer[chunk.range()]))
            .collect::<Vec<_>>();
        let hash_time = start.elapsed();

        let chunks = chunks.iter().map(|chunk| buffer[chunk.range()].to_vec()).collect::<Vec<_>>();

        // have to copy hashes? or do something else?
        let spans = hashes
            .iter()
            .zip(chunks.iter())
            .map(|(hash, chunk)| Span::new(hash.clone(), chunk.len()))
            .collect();

        let converted_chunks = chunks.into_iter().map(|chunk| DataContainer(Data::Chunk(chunk))).collect();
        base.save(hashes, converted_chunks)?;

        Ok(SpansInfo {
            spans,
            measurements: WriteMeasurements::new(chunk_time, hash_time),
        })
    }

    /// Flushes remaining data to the storage and returns its [`span`][Span] with hashing and chunking times.
    fn flush<K, B: Map<H::Hash, DataContainer<K>>>(&mut self, base: &mut B) -> io::Result<SpansInfo<H::Hash>> {
        // is this necessary?
        if self.chunker.remainder().is_empty() {
            return Ok(SpansInfo {
                spans: vec![],
                measurements: Default::default(),
            });
        }

        let remainder = self.chunker.remainder().to_vec();
        let remainder_length = remainder.len();
        let start = Instant::now();
        let hash = self.hasher.hash(&remainder);
        let hash_time = start.elapsed();

        base.insert(hash.clone(), DataContainer(Data::Chunk(remainder)))?;

        let span = Span::new(hash, remainder_length);
        Ok(SpansInfo {
            spans: vec![span],
            measurements: WriteMeasurements::new(Duration::default(), hash_time),
        })
    }
}

impl<K> DataContainer<K> {
    fn make_target(&mut self, keys: Vec<K>) {
        self.0 = Data::TargetChunk(keys);
    }
}

impl<K> std::fmt::Debug for Data<K> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Data::Chunk(chunk) => write!(f, "Chunk with len {}", chunk.len()),
            Data::TargetChunk(keys) => write!(f, "TargetChunk with {} keys", keys.len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::base::HashMapBase;
    use crate::map::{ChunkStorage, Data, DataContainer, Scrub, ScrubMeasurements, TargetMap};
    use crate::ChunkHash;
    use std::collections::HashMap;
    use crate::hashers::SimpleHasher;

    struct DumbScrubber;

    impl<Hash: ChunkHash, K> Scrub<Hash, K> for DumbScrubber {
        fn scrub(
            &mut self,
            cdc: &mut dyn Iterator<Item = (&Hash, &mut DataContainer<K>)>,
            _target: &mut TargetMap<K>,
        ) -> ScrubMeasurements {
            for (_, data) in cdc {
                data.make_target(vec![]);
            }

            ScrubMeasurements::default()
        }
    }

    #[test]
    fn hashmap_works_as_cdc_map() {
        let mut map = HashMap::new();
        map.insert(vec![1], DataContainer(Data::<Vec<u8>>::Chunk(vec![1])));
        map.insert(vec![2], DataContainer(Data::<Vec<u8>>::Chunk(vec![2])));
        let mut chunk_storage = ChunkStorage {
            cdc_map: map,
            scrubber: Box::new(DumbScrubber),
            target_map: Box::new(HashMapBase::default()),
            hasher: SimpleHasher
        };

        let measurements = chunk_storage.scrubber.scrub(
            &mut chunk_storage.cdc_map.iter_mut(),
            &mut chunk_storage.target_map,
        );
        assert_eq!(measurements, ScrubMeasurements::default());

        println!("{:?}", chunk_storage.cdc_map)
    }
}
