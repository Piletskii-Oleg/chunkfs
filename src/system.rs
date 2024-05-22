use std::cmp::min;
use std::collections::HashMap;
use std::io;

use crate::file_layer::{FileHandle, FileLayer};
use crate::map::{Map, TargetMap};
use crate::scrub::{DumbScrubber, Scrub, ScrubMeasurements};
use crate::storage::{ChunkStorage, DataContainer};
use crate::WriteMeasurements;
use crate::{ChunkHash, SEG_SIZE};
use crate::{Chunker, Hasher};

/// A file system provided by chunkfs.
pub struct FileSystem<B, H, Hash, K>
where
    B: Map<Hash, DataContainer<K>>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
    for<'a> &'a mut B: IntoIterator<Item = (&'a H::Hash, &'a mut DataContainer<K>)>,
{
    storage: ChunkStorage<H, Hash, B, K>,
    file_layer: FileLayer<Hash>,
}

impl<B, H, Hash> FileSystem<B, H, Hash, i32>
where
    B: Map<Hash, DataContainer<i32>>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
    for<'a> &'a mut B: IntoIterator<Item = (&'a H::Hash, &'a mut DataContainer<i32>)>,
{
    pub fn new_cdc_only(base: B, hasher: H) -> Self {
        Self {
            storage: ChunkStorage::new(
                base,
                Box::<HashMap<i32, Vec<u8>>>::default(),
                Box::new(DumbScrubber),
                hasher,
            ),
            file_layer: Default::default(),
        }
    }
}

impl<B, H, Hash, K> FileSystem<B, H, Hash, K>
where
    B: Map<Hash, DataContainer<K>>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
    for<'a> &'a mut B: IntoIterator<Item = (&'a H::Hash, &'a mut DataContainer<K>)>,
{
    /// Creates a file system with the given [`base`][Base].
    pub fn new(
        base: B,
        target_map: TargetMap<K>,
        scrubber: Box<dyn Scrub<Hash, K, B>>,
        hasher: H,
    ) -> Self {
        Self {
            storage: ChunkStorage::new(base, target_map, scrubber, hasher),
            file_layer: Default::default(),
        }
    }

    /// Checks if the file with the given `name` exists.
    pub fn file_exists(&self, name: &str) -> bool {
        self.file_layer.file_exists(name)
    }

    /// Tries to open a file with the given name and returns its `FileHandle` if it exists,
    /// or `None`, if it doesn't.
    pub fn open_file<C: Chunker>(&self, name: &str, chunker: C) -> io::Result<FileHandle<C>> {
        self.file_layer.open(name, chunker)
    }

    /// Creates a file with the given name and returns its `FileHandle`.
    /// Returns `ErrorKind::AlreadyExists`, if the file with the same name exists in the file system.
    pub fn create_file<C: Chunker>(
        &mut self,
        name: String,
        chunker: C,
        create_new: bool,
    ) -> io::Result<FileHandle<C>> {
        self.file_layer.create(name, chunker, create_new)
    }

    /// Writes given data to the file.
    pub fn write_to_file<C: Chunker>(
        &mut self,
        handle: &mut FileHandle<C>,
        data: &[u8],
    ) -> io::Result<()> {
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
    pub fn close_file<C: Chunker>(
        &mut self,
        mut handle: FileHandle<C>,
    ) -> io::Result<WriteMeasurements> {
        let span = self.storage.flush(&mut handle.chunker)?;
        self.file_layer.write(&mut handle, span);

        Ok(handle.close())
    }

    /// Reads all contents of the file from beginning to end and returns them.
    pub fn read_file_complete<C: Chunker>(&self, handle: &FileHandle<C>) -> io::Result<Vec<u8>> {
        let hashes = self.file_layer.read_complete(handle);
        Ok(self.storage.retrieve(&hashes)?.concat()) // it assumes that all retrieved data segments are in correct order
    }

    /// Reads 1 MB of data from a file and returns it.
    pub fn read_from_file<C: Chunker>(
        &mut self,
        handle: &mut FileHandle<C>,
    ) -> io::Result<Vec<u8>> {
        let hashes = self.file_layer.read(handle);
        Ok(self.storage.retrieve(&hashes)?.concat())
    }

    pub fn scrub(&mut self) -> ScrubMeasurements {
        self.storage.scrub()
    }
}