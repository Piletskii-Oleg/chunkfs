use std::cmp::min;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::io::ErrorKind;

use crate::file_layer::{FileHandle, FileLayer};
use crate::storage::Storage;
use crate::WriteMeasurements;
use crate::{ChunkHash, SEG_SIZE};
use crate::{Chunker, Database, Hasher};

/// A file system provided by chunkfs.
pub struct FileSystem<B, H, Hash>
where
    B: Database<Hash>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
{
    storage: Storage<B, H, Hash>,
    file_layer: FileLayer<Hash>,
}

impl<B, H, Hash> FileSystem<B, H, Hash>
where
    B: Database<Hash>,
    H: Hasher<Hash = Hash>,
    Hash: ChunkHash,
{
    /// Creates a file system with the given [`base`][Base].
    pub fn new(base: B, hasher: H) -> Self {
        Self {
            storage: Storage::new(base, hasher),
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
        Ok(self.storage.retrieve(hashes)?.concat()) // it assumes that all retrieved data segments are in correct order
    }

    /// Reads 1 MB of data from a file and returns it.
    pub fn read_from_file<C: Chunker>(
        &mut self,
        handle: &mut FileHandle<C>,
    ) -> io::Result<Vec<u8>> {
        let hashes = self.file_layer.read(handle);
        Ok(self.storage.retrieve(hashes)?.concat())
    }
}

/// Used to open a file with the given chunker and hasher, with some other options.
/// Chunker and hasher must be provided using [with_chunker][`Self::with_chunker`] and [with_hasher][`Self::with_hasher`].
pub struct FileOpener<C>
where
    C: Chunker,
{
    chunker: Option<C>,
    create_new: bool,
}

/// Error that may happen when opening a file using [FileOpener].
#[derive(Debug)]
pub enum OpenError {
    NoChunkerProvided,
    IoError(io::Error),
}

impl Display for OpenError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::NoChunkerProvided => write!(
                f,
                "No chunker was provided. A chunker is necessary to write to the file."
            ),
            OpenError::IoError(io) => io.fmt(f),
        }
    }
}

impl Error for OpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            OpenError::NoChunkerProvided => None,
            OpenError::IoError(io) => Some(io),
        }
    }
}

impl From<io::Error> for OpenError {
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
    }
}

impl From<ErrorKind> for OpenError {
    fn from(value: ErrorKind) -> Self {
        Self::IoError(value.into())
    }
}

impl<C> FileOpener<C>
where
    C: Chunker,
{
    /// Initializes [FileOpener] with empty fields.
    /// `chunker` and `hasher` must be explicitly given using [with_chunker][`Self::with_chunker`]
    /// and [with_hasher][`Self::with_hasher`].
    pub fn new() -> Self {
        Self {
            chunker: None,
            create_new: false,
        }
    }

    /// Sets a [`chunker`][Chunker] that will be used to split the written data into chunks.
    pub fn with_chunker(mut self, chunker: C) -> Self {
        self.chunker = Some(chunker);
        self
    }

    /// Sets a flag that indicates whether new file should be created, and if it exists, be overwritten.
    pub fn create_new(mut self, create_new: bool) -> Self {
        self.create_new = create_new;
        self
    }

    /// Opens a file in the given [FileSystem] and with the given name. Creates new file if the flag was set.
    /// Returns an [OpenError] if the `chunker` or `hasher` were not set.
    pub fn open<B: Database<Hash>, H: Hasher<Hash = Hash>, Hash: ChunkHash>(
        self,
        fs: &mut FileSystem<B, H, Hash>,
        name: &str,
    ) -> Result<FileHandle<C>, OpenError> {
        let chunker = self.chunker.ok_or(OpenError::NoChunkerProvided)?;

        if self.create_new {
            fs.create_file(name.to_string(), chunker, self.create_new)
                .map_err(OpenError::IoError)
        } else {
            fs.open_file(name, chunker).map_err(OpenError::IoError)
        }
    }
}

impl<C> Default for FileOpener<C>
where
    C: Chunker,
{
    fn default() -> Self {
        Self::new()
    }
}
