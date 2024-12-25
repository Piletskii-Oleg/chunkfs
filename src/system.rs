use std::cmp::min;
use std::collections::HashMap;
use std::io;

use database::{Database, IterableDatabase};
use file_layer::{FileHandle, FileLayer};
use scrub::{Scrub, ScrubMeasurements};
use storage::{ChunkStorage, DataContainer};

use super::{ChunkHash, ChunkerRef, Hasher, WriteMeasurements, SEG_SIZE};

pub mod database;
pub mod file_layer;
pub mod scrub;
pub mod storage;

/// A file system provided by chunkfs.
///
/// To create a file system that can be used with CDC algorithms only, [`create_cdc_filesystem`] should be used.
///
/// If you want to test scrubber, [`FileSystem::new_with_scrubber`] should be used.
pub struct FileSystem<B, H, Hash, K, T>
where
    B: Database<Hash, DataContainer<K>>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
    T: Database<K, Vec<u8>>,
{
    storage: ChunkStorage<H, Hash, B, K, T>,
    file_layer: FileLayer<Hash>,
}

/// Creates a file system that can be used to compare CDC algorithms.
///
/// Resulting filesystem cannot be scrubbed using [`scrub`][FileSystem::scrub].
///
/// If database is iterable (e.g. `HashMap` or something that implements [`IterableDatabase`]),
/// CDC dedup ratio can be calculated using [`cdc_dedup_ratio`][FileSystem::cdc_dedup_ratio].
pub fn create_cdc_filesystem<B, H, Hash>(
    base: B,
    hasher: H,
) -> FileSystem<B, H, Hash, (), HashMap<(), Vec<u8>>>
where
    B: Database<Hash, DataContainer<()>>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
{
    FileSystem::new(base, hasher, HashMap::default())
}

impl<B, H, Hash, K, T> FileSystem<B, H, Hash, K, T>
where
    B: Database<Hash, DataContainer<K>>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
    T: Database<K, Vec<u8>>,
{
    /// Checks if the file with the given `name` exists.
    pub fn file_exists(&self, name: &str) -> bool {
        self.file_layer.file_exists(name)
    }

    /// Tries to open a file with the given name and returns its `FileHandle` if it exists,
    /// or `None`, if it doesn't.
    pub fn open_file<S, C>(&self, name: S, chunker: C) -> io::Result<FileHandle>
    where
        S: AsRef<str>,
        C: Into<ChunkerRef>,
    {
        self.file_layer.open(name.as_ref(), chunker.into())
    }

    pub fn open_file_readonly<S>(&self, name: S) -> io::Result<FileHandle>
    where
        S: AsRef<str>,
    {
        self.file_layer.open_readonly(name.as_ref())
    }

    /// Creates a file with the given name and returns its `FileHandle`.
    /// Returns `ErrorKind::AlreadyExists`, if the file with the same name exists in the file system.
    pub fn create_file<S, C>(&mut self, name: S, chunker: C) -> io::Result<FileHandle>
    where
        S: Into<String>,
        C: Into<ChunkerRef>,
    {
        self.file_layer.create(name, chunker.into(), true)
    }

    /// Writes given data to the file.
    ///
    /// # Errors
    /// `io::ErrorKind::PermissionDenied` - if the handle was opened in read-only mode
    pub fn write_to_file(&mut self, handle: &mut FileHandle, data: &[u8]) -> io::Result<()> {
        if !self.file_exists(handle.name()) {
            return Err(io::ErrorKind::NotFound.into());
        }

        let Some(chunker) = &mut handle.chunker else {
            let msg = "file handle is read-only";
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, msg));
        };

        let mut current = 0;
        let mut all_spans = vec![];
        while current < data.len() {
            let remaining = data.len() - current;
            let to_process = min(SEG_SIZE, remaining);

            let spans = self
                .storage
                .write(&data[current..current + to_process], chunker)?;
            all_spans.push(spans);

            current += to_process;
        }

        for spans in all_spans {
            self.file_layer.write(handle, spans);
        }

        Ok(())
    }

    /// Writes given data to the file. Takes any reader as an input, including slices.
    ///
    /// # Errors
    /// `io::ErrorKind::PermissionDenied` - if the handle was opened in read-only mode
    pub fn write_from_stream<R>(&mut self, handle: &mut FileHandle, mut reader: R) -> io::Result<()>
    where
        R: io::Read,
    {
        if !self.file_exists(handle.name()) {
            return Err(io::ErrorKind::NotFound.into());
        }

        let Some(chunker) = &mut handle.chunker else {
            let msg = "file handle is read-only";
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, msg));
        };

        let mut buffer = vec![0u8; SEG_SIZE];
        let mut all_spans = vec![];
        loop {
            let n = reader.read(&mut buffer)?;
            if n == 0 {
                break;
            }

            let spans = self.storage.write(&buffer[..n], chunker)?;
            all_spans.push(spans);
        }

        for spans in all_spans {
            self.file_layer.write(handle, spans);
        }

        Ok(())
    }

    /// Closes the file and ensures that all data that was written to it is stored.
    /// Returns [WriteMeasurements] containing chunking and hashing times.
    pub fn close_file(&mut self, mut handle: FileHandle) -> io::Result<WriteMeasurements> {
        if !self.file_exists(handle.name()) {
            return Err(io::ErrorKind::NotFound.into());
        }

        if let Some(chunker) = &mut handle.chunker {
            let span = self.storage.flush(chunker)?;
            self.file_layer.write(&mut handle, span);
        }

        Ok(handle.close())
    }

    /// Reads all contents of the file from beginning to end and returns them.
    pub fn read_file_complete(&self, handle: &FileHandle) -> io::Result<Vec<u8>> {
        let hashes = self.file_layer.read_complete(handle);
        Ok(self.storage.retrieve(&hashes)?.concat()) // it assumes that all retrieved data segments are in correct order
    }

    /// Reads 1 MB of data from a file and returns it.
    ///
    /// **Careful:** it modifies internal `FileHandle` data. After using this `write_to_file` should not be used on the same FileHandle.
    pub fn read_from_file(&mut self, handle: &mut FileHandle) -> io::Result<Vec<u8>> {
        let hashes = self.file_layer.read(handle);
        Ok(self.storage.retrieve(&hashes)?.concat())
    }

    /// Gives out a distribution of the chunks with the same hash for the given file.
    ///
    /// Output is a hash map, where the key is the used hash and the value is
    /// count of the same chunks and the length of one of them.
    pub fn chunk_count_distribution(&self, handle: &FileHandle) -> HashMap<Hash, (u32, usize)> {
        self.file_layer.chunk_count_distribution(handle)
    }

    /// Creates a file system with the given [`hasher`][Hasher], `base` and `target_map`. Unlike [`new_with_scrubber`][Self::new_with_scrubber],
    /// doesn't require a database to be iterable. Resulting filesystem cannot be scrubbed using [`scrub`][Self::scrub].
    fn new(base: B, hasher: H, target_map: T) -> Self {
        Self {
            storage: ChunkStorage::new(base, hasher, target_map),
            file_layer: Default::default(),
        }
    }
}

impl<B, H, Hash, K, T> FileSystem<B, H, Hash, K, T>
where
    B: IterableDatabase<Hash, DataContainer<K>>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
    T: Database<K, Vec<u8>>,
{
    /// Creates a file system with the given [`hasher`][Hasher], original [`database`][Database] and target map, and a [`scrubber`][Scrub].
    ///
    /// Provided `database` must implement [`IterableDatabase`].
    pub fn new_with_scrubber(
        database: B,
        target_map: T,
        scrubber: Box<dyn Scrub<Hash, B, K, T>>,
        hasher: H,
    ) -> Self {
        Self {
            storage: ChunkStorage::new_with_scrubber(database, target_map, scrubber, hasher),
            file_layer: Default::default(),
        }
    }

    /// Scrubs the data in the database. Must be used with filesystems created using [`new_with_scrubber`][Self::new_with_scrubber],
    /// otherwise it returns [`ErrorKind::InvalidInput`][io::ErrorKind::InvalidInput].
    ///
    /// For more info check [`Scrub`][Scrub] trait and its [`scrub`][Scrub::scrub] method.
    pub fn scrub(&mut self) -> io::Result<ScrubMeasurements> {
        self.storage.scrub()
    }

    /// Calculates deduplication ratio of the storage, not accounting for chunks processed with scrubber,
    /// if there had been any.
    pub fn cdc_dedup_ratio(&self) -> f64 {
        self.storage.cdc_dedup_ratio()
    }

    pub fn iterator(&self) -> Box<dyn Iterator<Item = (&Hash, &DataContainer<K>)> + '_> {
        self.storage.iterator()
    }

    /// Completely clears the chunk database, invalidating already created file handles. Doesn't touch the target map.
    ///
    /// **WARNING**: Since it invalidates all file handles, data contained in target map will not be valid too.
    /// If the target map is iterable, it can be cleared with [`clear_target_map`][Self::clear_target_map].
    pub fn clear_database(&mut self) -> io::Result<()> {
        self.file_layer.clear();
        self.storage.clear_database()
    }
}

impl<B, H, Hash, K, T> FileSystem<B, H, Hash, K, T>
where
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
    B: IterableDatabase<H::Hash, DataContainer<K>>,
    T: IterableDatabase<K, Vec<u8>>,
{
    /// Calculates total deduplication ratio of the storage,
    /// accounting for chunks both unprocessed and processed with scrubber.
    pub fn total_dedup_ratio(&self) -> f64 {
        self.storage.total_dedup_ratio()
    }

    /// Clears the contained target map. Doesn't remove the info about file handles.
    pub fn clear_target_map(&mut self) -> io::Result<()> {
        self.storage.clear_target_map()
    }
}
