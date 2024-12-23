use std::fmt::Formatter;
use std::io;
use std::time::{Duration, Instant};

use crate::{ChunkHash, Hasher};
use crate::{ChunkerRef, WriteMeasurements};

use super::database::{Database, IterableDatabase};
use super::scrub::{Scrub, ScrubMeasurements};

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
    total_length: usize,
}

impl<Hash: ChunkHash> Span<Hash> {
    pub fn new(hash: Hash, length: usize) -> Self {
        Self { hash, length }
    }
}

/// Underlying storage for the actual stored data.
pub struct ChunkStorage<H, Hash, B, K, T>
where
    H: Hasher<Hash=Hash>,
    Hash: ChunkHash,
    B: Database<Hash, DataContainer<K>>,
    T: Database<K, Vec<u8>>,
{
    database: B,
    scrubber: Option<Box<dyn Scrub<Hash, B, K, T>>>,
    target_map: T,
    hasher: H,
    size_written: usize,
}

impl<H, Hash, B, K, T> ChunkStorage<H, Hash, B, K, T>
where
    H: Hasher<Hash=Hash>,
    Hash: ChunkHash,
    B: Database<H::Hash, DataContainer<K>>,
    T: Database<K, Vec<u8>>,
{
    pub fn new(database: B, hasher: H, target_map: T) -> Self {
        Self {
            database,
            scrubber: None,
            target_map,
            hasher,
            size_written: 0,
        }
    }

    /// Writes 1 MB of data to the [`base`][crate::base::Base] storage after deduplication.
    ///
    /// Returns resulting lengths of [chunks][crate::chunker::Chunk] with corresponding hash,
    /// along with amount of time spent on chunking and hashing.
    pub fn write(
        &mut self,
        data: &[u8],
        chunker: &ChunkerRef,
    ) -> io::Result<SpansInfo<H::Hash>> {
        let mut writer = StorageWriter::new(chunker, &mut self.hasher);
        let spans_info = writer.write(data, &mut self.database)?;

        self.size_written += spans_info.total_length;

        Ok(spans_info)
    }

    /// Flushes remaining data to the storage and returns its [`span`][Span] with hashing and chunking times.
    pub fn flush(&mut self, chunker: &ChunkerRef) -> io::Result<SpansInfo<H::Hash>> {
        let mut writer = StorageWriter::new(chunker, &mut self.hasher);
        let spans_info = writer.flush(&mut self.database)?;

        self.size_written += spans_info.total_length;

        Ok(spans_info)
    }

    /// Retrieves the data from the storage based on hashes of the data [`segments`][Segment],
    /// or Error(NotFound) if some of the hashes were not present in the base.
    pub fn retrieve(&self, request: &[H::Hash]) -> io::Result<Vec<Vec<u8>>> {
        let retrieved = self.database.get_multi(request)?;

        retrieved
            .into_iter()
            .map(|container| match &container.0 {
                Data::Chunk(chunk) => Ok(chunk.clone()),
                Data::TargetChunk(keys) => Ok(self
                    .target_map
                    .get_multi(keys)?
                    .into_iter()
                    .flatten()
                    .collect()),
            })
            .collect()
    }
}

impl<H, Hash, B, K, T> ChunkStorage<H, Hash, B, K, T>
where
    H: Hasher<Hash=Hash>,
    Hash: ChunkHash,
    B: IterableDatabase<H::Hash, DataContainer<K>>,
    T: Database<K, Vec<u8>>,
{
    pub fn new_with_scrubber(
        database: B,
        target_map: T,
        scrubber: Box<dyn Scrub<Hash, B, K, T>>,
        hasher: H,
    ) -> Self {
        Self {
            database,
            scrubber: Some(scrubber),
            target_map,
            hasher,
            size_written: 0,
        }
    }

    pub fn scrub(&mut self) -> io::Result<ScrubMeasurements> {
        self.scrubber
            .as_mut()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "scrubber cannot be used with CDC filesystem",
                )
            })?
            .scrub(&mut self.database, &mut self.target_map)
    }

    /// Returns size of CDC chunks in the storage. Doesn't count for chunks processed with SBC or FBC.
    fn total_cdc_size(&self) -> usize {
        self.database
            .values()
            .fold(0, |total_size, container| match container.extract() {
                Data::Chunk(chunk) => total_size + chunk.len(),
                Data::TargetChunk(_) => total_size,
            })
    }

    /// Calculates deduplication ratio of the storage, not accounting for chunks processed with scrubber.
    pub fn cdc_dedup_ratio(&self) -> f64 {
        (self.size_written as f64) / (self.total_cdc_size() as f64)
    }
}

impl<H, Hash, B, K, T> ChunkStorage<H, Hash, B, K, T>
where
    H: Hasher<Hash=Hash>,
    Hash: ChunkHash,
    B: IterableDatabase<H::Hash, DataContainer<K>>,
    T: IterableDatabase<K, Vec<u8>>,
{
    fn total_size(&self) -> usize {
        let cdc_size = self.total_cdc_size();
        let scrubbed_size = self
            .target_map
            .values()
            .fold(0, |total_size, data| total_size + data.len());
        cdc_size + scrubbed_size
    }

    pub fn total_dedup_ratio(&self) -> f64 {
        (self.size_written as f64) / (self.total_size() as f64)
    }
}

/// Writer that conducts operations on [Storage].
/// Only exists during [FileSystem::write_to_file][crate::FileSystem::write_to_file].
/// Receives `buffer` from [FileHandle][crate::file_layer::FileHandle] and gives it back after a successful write.
struct StorageWriter<'handle, H>
where
    H: Hasher,
{
    chunker: &'handle ChunkerRef,
    hasher: &'handle mut H,
}

impl<'handle, H> StorageWriter<'handle, H>
where
    H: Hasher,
{
    fn new(chunker: &'handle ChunkerRef, hasher: &'handle mut H) -> Self {
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

        let mut buffer = self.chunker.borrow().remainder().to_vec();
        buffer.extend_from_slice(data);

        let empty = Vec::with_capacity(self.chunker.borrow().estimate_chunk_count(&buffer));

        let start = Instant::now();
        let chunks = self.chunker.borrow_mut().chunk_data(&buffer, empty);
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

        let total_length = chunks.iter().map(|chunk| chunk.len()).sum::<usize>();

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
            total_length,
        })
    }

    /// Flushes remaining data to the storage and returns its [`span`][Span] with hashing and chunking times.
    fn flush<K, B: Database<H::Hash, DataContainer<K>>>(
        &mut self,
        base: &mut B,
    ) -> io::Result<SpansInfo<H::Hash>> {
        // is this necessary?
        if self.chunker.borrow().remainder().is_empty() {
            return Ok(SpansInfo {
                spans: vec![],
                measurements: Default::default(),
                total_length: 0,
            });
        }

        let remainder = self.chunker.borrow().remainder().to_vec();
        let remainder_length = remainder.len();
        let start = Instant::now();
        let hash = self.hasher.hash(&remainder);
        let hash_time = start.elapsed();

        base.insert(hash.clone(), DataContainer(Data::Chunk(remainder)))?;

        let span = Span::new(hash, remainder_length);
        Ok(SpansInfo {
            spans: vec![span],
            measurements: WriteMeasurements::new(Duration::default(), hash_time),
            total_length: remainder_length,
        })
    }
}

impl<K> DataContainer<K> {
    /// Replaces stored data with the vector of target map keys, using which the chunk can be restored.
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
    use std::collections::HashMap;

    use super::ChunkStorage;
    use super::DataContainer;
    use super::ScrubMeasurements;
    use crate::chunkers::{FSChunker, SuperChunker};
    use crate::hashers::SimpleHasher;
    use crate::system::scrub::DumbScrubber;

    #[test]
    fn hashmap_works_as_cdc_map() {
        let mut map: HashMap<Vec<u8>, DataContainer<()>> = HashMap::new();
        map.insert(vec![1], DataContainer::from(vec![]));
        map.insert(vec![2], DataContainer::from(vec![]));
        let mut chunk_storage = ChunkStorage {
            database: map,
            scrubber: Some(Box::new(DumbScrubber)),
            target_map: HashMap::default(),
            hasher: SimpleHasher,
            size_written: 0,
        };

        let measurements = chunk_storage
            .scrubber
            .as_mut()
            .unwrap()
            .scrub(&mut chunk_storage.database, &mut chunk_storage.target_map)
            .unwrap();
        assert_eq!(measurements, ScrubMeasurements::default());

        println!("{:?}", chunk_storage.database)
    }

    #[test]
    fn total_cdc_size_is_calculated_correctly_for_fixed_size_chunker_on_simple_data() {
        let mut chunk_storage = ChunkStorage::new(
            HashMap::<Vec<u8>, DataContainer<()>>::new(),
            SimpleHasher,
            HashMap::default(),
        );

        let data = vec![10; 1024 * 1024];
        let chunker = FSChunker::new(4096).into();

        chunk_storage.write(&data, &chunker).unwrap();
        chunk_storage.flush(&chunker).unwrap();

        assert_eq!(chunk_storage.total_cdc_size(), 4096)
    }

    #[test]
    fn size_written_is_calculated_correctly() {
        let mut chunk_storage = ChunkStorage::new(
            HashMap::<Vec<u8>, DataContainer<()>>::new(),
            SimpleHasher,
            HashMap::default(),
        );

        let data = [
            vec![4; 1024 * 256],
            vec![8; 1024 * 256],
            vec![16; 1024 * 256],
            vec![32; 1024 * 256],
        ]
            .concat();

        let chunker = SuperChunker::default().into();

        chunk_storage.write(&data, &chunker).unwrap();
        chunk_storage.write(&data, &chunker).unwrap();
        chunk_storage.flush(&chunker).unwrap();

        assert_eq!(chunk_storage.size_written, 1024 * 1024 * 2);
    }
}
