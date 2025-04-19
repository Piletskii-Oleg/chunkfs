use crate::{ChunkHash, Hasher, SEG_SIZE};
use crate::{ChunkerRef, WriteMeasurements};
use bincode::{Decode, Encode};
use std::cmp::min;
use std::fmt::Formatter;
use std::io;
use std::time::{Duration, Instant};

use super::database::{Database, IterableDatabase};
use super::scrub::{Scrub, ScrubMeasurements};

/// Container for storage data.
#[derive(Clone, Debug, Default, Encode, Decode)]
pub struct DataContainer<K>(Data<K>);

/// Contains either a chunk produced by [Chunker], or a vector of target keys, using which the initial chunk can be restored.
#[derive(Clone, Encode, Decode)]
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
#[derive(Debug, Default)]
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
pub struct ChunkStorage<Hash, B, K, T>
where
    Hash: ChunkHash,
    B: Database<Hash, DataContainer<K>>,
    T: Database<K, Vec<u8>>,
{
    database: B,
    scrubber: Option<Box<dyn Scrub<Hash, B, K, T>>>,
    target_map: T,
    hasher: Box<dyn Hasher<Hash = Hash>>,
    size_written: usize,
}

impl<Hash, B, K, T> ChunkStorage<Hash, B, K, T>
where
    Hash: ChunkHash,
    B: Database<Hash, DataContainer<K>>,
    T: Database<K, Vec<u8>>,
{
    pub fn new(database: B, hasher: Box<dyn Hasher<Hash = Hash>>, target_map: T) -> Self {
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
    pub fn write(&mut self, data: &[u8], chunker: &ChunkerRef) -> io::Result<Vec<SpansInfo<Hash>>> {
        let mut writer = StorageWriter::new(chunker, &mut self.hasher);

        let mut current = 0;
        let mut all_spans = vec![];

        while current < data.len() {
            let remaining = data.len() - current;
            let to_process = min(SEG_SIZE, remaining);

            let spans = writer.write(&data[current..current + to_process], &mut self.database)?;

            current += to_process;

            all_spans.push(spans);
        }

        let last_span = writer.flush(&mut self.database)?;

        all_spans.push(last_span);
        all_spans.retain(|span| span.total_length > 0);

        self.size_written += data.len();

        Ok(all_spans)
    }

    pub fn write_from_stream<R>(
        &mut self,
        mut reader: R,
        chunker: &ChunkerRef,
    ) -> io::Result<Vec<SpansInfo<Hash>>>
    where
        R: io::Read,
    {
        let mut writer = StorageWriter::new(chunker, &mut self.hasher);

        let mut all_spans = vec![];
        let mut buffer = vec![0u8; SEG_SIZE];

        loop {
            let n = reader.read(&mut buffer)?;
            if n == 0 {
                break;
            }

            let spans = writer.write(&buffer[..n], &mut self.database)?;
            self.size_written += spans.total_length;

            all_spans.push(spans);
        }

        let last_span = writer.flush(&mut self.database)?;
        self.size_written += last_span.total_length;

        all_spans.push(last_span);
        all_spans.retain(|span| span.total_length > 0);

        Ok(all_spans)
    }

    /// Retrieves the data from the storage based on hashes of the data [`segments`][Segment],
    /// or Error(NotFound) if some of the hashes were not present in the base.
    pub fn retrieve(&self, request: &[Hash]) -> io::Result<Vec<Vec<u8>>> {
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

impl<Hash, B, K, T> ChunkStorage<Hash, B, K, T>
where
    Hash: ChunkHash,
    B: IterableDatabase<Hash, DataContainer<K>>,
    T: Database<K, Vec<u8>>,
{
    pub fn new_with_scrubber(
        database: B,
        target_map: T,
        scrubber: Box<dyn Scrub<Hash, B, K, T>>,
        hasher: Box<dyn Hasher<Hash = Hash>>,
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

    /// Returns average chunk size in the storage.
    pub fn average_chunk_size(&self) -> usize {
        let (count, size) = self
            .database
            .values()
            .fold((0, 0), |(count, size), container| {
                let chunk_size = match container.extract() {
                    Data::Chunk(chunk) => chunk.len(),
                    Data::TargetChunk(_) => 0,
                };
                (count + 1, size + chunk_size)
            });

        size / count
    }

    pub fn full_cdc_dedup_ratio(&self) -> f64 {
        let key_size = self
            .database
            .keys()
            .map(|key| self.hasher.len(key))
            .sum::<usize>();

        (self.size_written as f64) / (self.total_cdc_size() as f64 + key_size as f64)
    }

    pub fn iterator(&self) -> Box<dyn Iterator<Item = (&Hash, &DataContainer<K>)> + '_> {
        self.database.iterator()
    }

    /// Removes all stored data in the database and sets written size to 0.
    pub fn clear_database(&mut self) -> io::Result<()> {
        self.size_written = 0;
        self.database.clear()
    }
}

impl<Hash, B, K, T> ChunkStorage<Hash, B, K, T>
where
    Hash: ChunkHash,
    B: IterableDatabase<Hash, DataContainer<K>>,
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

    /// Removes all stored data in the target map and sets written size to 0.
    pub fn clear_database_full(&mut self) -> io::Result<()> {
        self.size_written = 0;
        self.database.clear()?;
        self.target_map.clear()
    }
}

/// Writer that conducts operations on [Storage].
/// Only exists during [FileSystem::write_to_file][crate::FileSystem::write_to_file].
/// Receives `buffer` from [FileHandle][crate::file_layer::FileHandle] and gives it back after a successful write.
struct StorageWriter<'handle, Hash>
where
    Hash: ChunkHash,
{
    chunker: &'handle ChunkerRef,
    hasher: &'handle mut Box<dyn Hasher<Hash = Hash>>,
    rest: Vec<u8>,
}

impl<'handle, Hash> StorageWriter<'handle, Hash>
where
    Hash: ChunkHash,
{
    fn new(
        chunker: &'handle ChunkerRef,
        hasher: &'handle mut Box<dyn Hasher<Hash = Hash>>,
    ) -> Self {
        Self {
            chunker,
            hasher,
            rest: vec![],
        }
    }

    /// Writes 1 MB of data to the [`base`][crate::base::Base] storage after deduplication.
    ///
    /// Returns resulting lengths of [chunks][crate::chunker::Chunk] with corresponding hash,
    /// along with amount of time spent on chunking and hashing.
    fn write<K, B: Database<Hash, DataContainer<K>>>(
        &mut self,
        data: &[u8],
        base: &mut B,
    ) -> io::Result<SpansInfo<Hash>> {
        //debug_assert!(data.len() == SEG_SIZE); // we assume that all given data segments are 1MB long for now

        let mut buffer = self.rest.clone();
        buffer.extend_from_slice(data);

        let empty = Vec::with_capacity(self.chunker.lock().unwrap().estimate_chunk_count(&buffer));

        let start = Instant::now();
        let mut chunks = self.chunker.lock().unwrap().chunk_data(&buffer, empty);
        let chunk_time = start.elapsed();

        if chunks.is_empty() {
            return Ok(SpansInfo::default());
        }

        self.rest = buffer[chunks.pop().unwrap().range()].to_vec();

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
        let start = Instant::now();
        base.try_insert_multi(pairs)?;
        let save_time = start.elapsed();

        Ok(SpansInfo {
            spans,
            measurements: WriteMeasurements::new(save_time, chunk_time, hash_time),
            total_length,
        })
    }

    /// Flushes remaining data to the storage and returns its [`span`][Span] with hashing and chunking times.
    fn flush<K, B: Database<Hash, DataContainer<K>>>(
        &mut self,
        base: &mut B,
    ) -> io::Result<SpansInfo<Hash>> {
        // is this necessary?
        if self.rest.is_empty() {
            return Ok(SpansInfo::default());
        }

        let remainder = self.rest.to_vec();
        let remainder_length = remainder.len();
        let start = Instant::now();
        let hash = self.hasher.hash(&remainder);
        let hash_time = start.elapsed();

        let start = Instant::now();
        base.try_insert(hash.clone(), DataContainer(Data::Chunk(remainder)))?;
        let save_time = start.elapsed();

        let span = Span::new(hash, remainder_length);
        Ok(SpansInfo {
            spans: vec![span],
            measurements: WriteMeasurements::new(save_time, Duration::default(), hash_time),
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

    /// Returns a contained chunk if it is of type `Data::Chunk`.
    ///
    /// Will panic otherwise.
    pub fn unwrap_chunk(&self) -> &Vec<u8> {
        match &self.0 {
            Data::Chunk(chunk) => chunk,
            Data::TargetChunk(_) => {
                panic!("Target chunk found in DataContainer; expected simple chunk")
            }
        }
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
            hasher: Box::new(SimpleHasher),
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
            SimpleHasher.into(),
            HashMap::default(),
        );

        let data = vec![10; 1024 * 1024];
        let chunker = FSChunker::new(4096).into();

        chunk_storage.write(&data, &chunker).unwrap();

        assert_eq!(chunk_storage.total_cdc_size(), 4096)
    }

    #[test]
    fn size_written_is_calculated_correctly() {
        let mut chunk_storage = ChunkStorage::new(
            HashMap::<Vec<u8>, DataContainer<()>>::new(),
            SimpleHasher.into(),
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

        assert_eq!(chunk_storage.size_written, 1024 * 1024 * 2);
    }
}
