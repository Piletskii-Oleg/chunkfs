use std::cmp::min;
use std::io;

use crate::file_layer::{FileHandle, FileLayer};
use crate::map::{Database, IterableDatabase};
use crate::scrub::{Scrub, ScrubMeasurements};
use crate::storage::{ChunkStorage, DataContainer};
use crate::WriteMeasurements;
use crate::{ChunkHash, SEG_SIZE};
use crate::{Chunker, Hasher};

/// A file system provided by chunkfs.
pub struct FileSystem<B, H, Hash, K>
where
    B: Database<Hash, DataContainer<K>>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
{
    storage: ChunkStorage<H, Hash, B, K>,
    file_layer: FileLayer<Hash>,
}

impl<B, H, Hash, K> FileSystem<B, H, Hash, K>
where
    B: Database<Hash, DataContainer<K>>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
{
    /// Functionally the same as [`Self::new`], but it also takes a key example as a parameter so that rust compiler knows
    /// which type it is.
    ///
    /// Any value can be passed as a `_key` as it is not used anywhere, e.g. 0.
    pub fn new_with_key(base: B, hasher: H, _key: K) -> Self {
        Self {
            storage: ChunkStorage::new(base, hasher),
            file_layer: Default::default(),
        }
    }

    /// Creates a file system with the given [`hasher`][Hasher] and [`base`][Base]. Unlike [`new_with_scrubber`][Self::new_with_scrubber],
    /// doesn't require a database to be iterable. Resulting filesystem cannot be scrubbed using [`scrub`][Self::scrub].
    ///
    /// Use [`Self::new_with_key`] if this method throws a long compile-time error message that says something about
    /// giving filesystem an explicit type, where the type for type parameter 'K' is specified.
    pub fn new(base: B, hasher: H) -> Self {
        Self {
            storage: ChunkStorage::new(base, hasher),
            file_layer: Default::default(),
        }
    }

    /// Checks if the file with the given `name` exists.
    pub fn file_exists(&self, name: &str) -> bool {
        self.file_layer.file_exists(name)
    }

    /// Tries to open a file with the given name and returns its `FileHandle` if it exists,
    /// or `None`, if it doesn't.
    pub fn open_file(
        &self,
        name: &str,
        chunker: impl Into<Box<dyn Chunker>>,
    ) -> io::Result<FileHandle> {
        self.file_layer.open(name, chunker.into())
    }

    /// Creates a file with the given name and returns its `FileHandle`.
    /// Returns `ErrorKind::AlreadyExists`, if the file with the same name exists in the file system.
    pub fn create_file(
        &mut self,
        name: impl Into<String>,
        chunker: impl Into<Box<dyn Chunker>>,
        create_new: bool,
    ) -> io::Result<FileHandle> {
        self.file_layer.create(name, chunker.into(), create_new)
    }

    /// Writes given data to the file.
    pub fn write_to_file(&mut self, handle: &mut FileHandle, data: &[u8]) -> io::Result<()> {
        let mut current = 0;
        let mut all_spans = vec![];
        while current < data.len() {
            let remaining = data.len() - current;
            let to_process = min(SEG_SIZE, remaining);

            let spans = self
                .storage
                .write(&data[current..current + to_process], &mut handle.chunker)?;
            all_spans.push(spans);

            current += to_process;
        }

        for spans in all_spans {
            self.file_layer.write(handle, spans);
        }

        Ok(())
    }

    /// Closes the file and ensures that all data that was written to it
    /// is stored. Returns [WriteMeasurements] containing chunking and hashing times.
    pub fn close_file(&mut self, mut handle: FileHandle) -> io::Result<WriteMeasurements> {
        let span = self.storage.flush(&mut handle.chunker)?;
        self.file_layer.write(&mut handle, span);

        Ok(handle.close())
    }

    /// Reads all contents of the file from beginning to end and returns them.
    pub fn read_file_complete(&self, handle: &FileHandle) -> io::Result<Vec<u8>> {
        let hashes = self.file_layer.read_complete(handle);
        Ok(self.storage.retrieve(&hashes)?.concat()) // it assumes that all retrieved data segments are in correct order
    }

    /// Reads 1 MB of data from a file and returns it.
    pub fn read_from_file(&mut self, handle: &mut FileHandle) -> io::Result<Vec<u8>> {
        let hashes = self.file_layer.read(handle);
        Ok(self.storage.retrieve(&hashes)?.concat())
    }
}

impl<B, H, Hash, K> FileSystem<B, H, Hash, K>
where
    B: IterableDatabase<Hash, DataContainer<K>>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
{
    /// Creates a file system with the given [`hasher`][Hasher], original [`base`][Base] and target map, and a [`scrubber`][Scrub].
    ///
    /// Provided `database` must implement [IntoIterator].
    pub fn new_with_scrubber(
        database: B,
        target_map: Box<dyn Database<K, Vec<u8>>>,
        scrubber: Box<dyn Scrub<Hash, B, K>>,
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

    /// Calculates deduplication ratio of the storage, not accounting for chunks processed with scrubber.
    pub fn cdc_dedup_ratio(&mut self) -> f64 {
        self.storage.cdc_dedup_ratio()
    }
}
