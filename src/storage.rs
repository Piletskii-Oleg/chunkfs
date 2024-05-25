use std::fmt::Formatter;
use std::io;
use std::time::{Duration, Instant};

use crate::map::{Database};
use crate::scrub::{Scrub, ScrubMeasurements};
use crate::{Chunker, ChunkHash, Hasher};
use crate::WriteMeasurements;

/// Container for storage data.
#[derive(Clone, Debug, Default)]
pub struct DataContainer<K>(Data<K>);

/// Contains either a chunk produced by [Chunker], or a vector of target keys, using which the initial chunk can be restored.
#[derive(Clone)]
pub enum Data<K> {
    Chunk(Vec<u8>),
    TargetChunk(Vec<K>),
}

/// Hashed span in a [`file`][crate::file_layer::File] with a certain length.
#[derive(Debug)]
pub struct Span<Hash: ChunkHash> {
    pub hash: Hash,
    pub length: usize,
}

/// Spans received after [Storage::write] or [Storage::flush], along with time measurements.
#[derive(Debug)]
pub struct SpansInfo<Hash: ChunkHash> {
    pub spans: Vec<Span<Hash>>,
    pub measurements: WriteMeasurements,
}

impl<Hash: ChunkHash> Span<Hash> {
    pub fn new(hash: Hash, length: usize) -> Self {
        Self { hash, length }
    }
}

/// Underlying storage for the actual stored data.
pub struct ChunkStorage<H, Hash, B, K>
where
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
    B: Database<Hash, DataContainer<K>>,
    for<'a> &'a mut B: IntoIterator<Item = (&'a Hash, &'a mut DataContainer<K>)>,
{
    database: B,
    scrubber: Box<dyn Scrub<Hash, K, B>>,
    target_map: Box<dyn Database<K, Vec<u8>>>,
    hasher: H,
}

impl<H, Hash, B, K> ChunkStorage<H, Hash, B, K>
where
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
    B: Database<H::Hash, DataContainer<K>>,
    for<'a> &'a mut B: IntoIterator<Item = (&'a Hash, &'a mut DataContainer<K>)>,
{
    pub fn new(
        database: B,
        target_map: Box<dyn Database<K, Vec<u8>>>,
        scrubber: Box<dyn Scrub<Hash, K, B>>,
        hasher: H,
    ) -> Self {
        Self {
            database,
            scrubber,
            target_map,
            hasher,
        }
    }

    pub fn scrub(&mut self) -> ScrubMeasurements {
        self.scrubber.scrub(&mut self.database, &mut self.target_map)
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
        writer.write(data, &mut self.database)
    }

    /// Flushes remaining data to the storage and returns its [`span`][Span] with hashing and chunking times.
    pub fn flush<C: Chunker>(&mut self, chunker: &mut C) -> io::Result<SpansInfo<H::Hash>> {
        let mut writer = StorageWriter::new(chunker, &mut self.hasher);
        writer.flush(&mut self.database)
    }

    /// Retrieves the data from the storage based on hashes of the data [`segments`][Segment],
    /// or Error(NotFound) if some of the hashes were not present in the base.
    pub fn retrieve(&self, request: &[H::Hash]) -> io::Result<Vec<Vec<u8>>> {
        let retrieved = self.database.get_multi(request)?;

        retrieved
            .into_iter()
            .map(|container| match container.0 {
                Data::Chunk(chunk) => Ok(chunk),
                Data::TargetChunk(keys) => Ok(self.target_map.get_multi(&keys)?.concat()),
            })
            .collect()
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
    fn write<K, B: Database<H::Hash, DataContainer<K>>>(
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

        let chunks = chunks
            .iter()
            .map(|chunk| buffer[chunk.range()].to_vec())
            .collect::<Vec<_>>();

        // have to copy hashes? or do something else?
        let spans = hashes
            .iter()
            .zip(chunks.iter())
            .map(|(hash, chunk)| Span::new(hash.clone(), chunk.len()))
            .collect();

        let converted_chunks = chunks
            .into_iter()
            .map(|chunk| DataContainer(Data::Chunk(chunk)));

        let pairs = hashes.into_iter().zip(converted_chunks).collect(); // we allocate memory for (K, V) pairs, which is not really required
        base.insert_multi(pairs)?;

        Ok(SpansInfo {
            spans,
            measurements: WriteMeasurements::new(chunk_time, hash_time),
        })
    }

    /// Flushes remaining data to the storage and returns its [`span`][Span] with hashing and chunking times.
    fn flush<K, B: Database<H::Hash, DataContainer<K>>>(
        &mut self,
        base: &mut B,
    ) -> io::Result<SpansInfo<H::Hash>> {
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
    /// Replaces stored data with the vector of target map keys, using which the chunk can be restored.
    ///
    /// # Guarantees
    /// It is guaranteed that the keys will be in the linear order,
    /// such that it would be possible to get the initial chunk simply by iterating over the stored `Vec<K>`, retrieving the corresponding data chunks
    /// and concatenating them.
    pub fn make_target(&mut self, keys: Vec<K>) {
        self.0 = Data::TargetChunk(keys);
    }

    /// Gets the reference to the data stored in the container.
    pub fn extract(&self) -> &Data<K> {
        &self.0
    }

    /// Gets the mutable reference to the data stored in the container.
    pub fn extract_mut(&mut self) -> &mut Data<K> {
        &mut self.0
    }
}

impl<K> From<Vec<u8>> for DataContainer<K> {
    fn from(value: Vec<u8>) -> Self {
        Self(Data::Chunk(value))
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

impl<K> Default for Data<K> {
    fn default() -> Self {
        Self::Chunk(vec![])
    }
}

#[cfg(test)]
mod tests {
    use crate::hashers::SimpleHasher;
    use crate::scrub::DumbScrubber;
    use crate::storage::ChunkStorage;
    use crate::storage::DataContainer;
    use crate::storage::ScrubMeasurements;
    use std::collections::HashMap;

    #[test]
    fn hashmap_works_as_cdc_map() {
        let mut map: HashMap<Vec<u8>, DataContainer<i32>> = HashMap::new();
        map.insert(vec![1], DataContainer::from(vec![]));
        map.insert(vec![2], DataContainer::from(vec![]));
        let mut chunk_storage = ChunkStorage {
            database: map,
            scrubber: Box::new(DumbScrubber),
            target_map: Box::new(HashMap::default()),
            hasher: SimpleHasher,
        };

        let measurements = chunk_storage.scrubber.scrub(
            &mut chunk_storage.database,
            &mut chunk_storage.target_map,
        );
        assert_eq!(measurements, ScrubMeasurements::default());

        println!("{:?}", chunk_storage.database)
    }
}
