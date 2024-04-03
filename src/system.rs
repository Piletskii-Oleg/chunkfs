use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::io::ErrorKind;

use crate::file_layer::{FileHandle, FileLayer};
use crate::storage::{Base, Chunker, Hasher, Storage};
use crate::WriteMeasurements;

pub struct FileSystem<B>
where
    B: Base,
{
    storage: Storage<B>,
    file_layer: FileLayer,
}

impl<B> FileSystem<B>
where
    B: Base,
{
    pub fn new(base: B) -> Self {
        Self {
            storage: Storage::new(base),
            file_layer: Default::default(),
        }
    }

    pub fn file_exists(&self, name: &str) -> bool {
        self.file_layer.file_exists(name)
    }

    /// Tries to open a file with the given name and returns its `FileHandle` if it exists,
    /// or `None`, if it doesn't.
    pub fn open_file<C: Chunker, H: Hasher>(
        &self,
        name: &str,
        c: C,
        h: H,
    ) -> io::Result<FileHandle<C, H>> {
        self.file_layer.open(name, c, h)
    }

    /// Creates a file with the given name and returns its `FileHandle`.
    /// Returns `ErrorKind::AlreadyExists`, if the file with the same name exists in the file system.
    pub fn create_file<C: Chunker, H: Hasher>(
        &mut self,
        name: String,
        c: C,
        h: H,
    ) -> io::Result<FileHandle<C, H>> {
        self.file_layer.create(name, c, h)
    }

    /// Writes given data to the file. Size of the slice must be exactly 1 MB.
    pub fn write_to_file<C: Chunker, H: Hasher>(
        &mut self,
        handle: &mut FileHandle<C, H>,
        data: &[u8],
    ) -> io::Result<()> {
        let spans = self.storage.write(data, &mut handle.writer)?;
        self.file_layer.write(handle, spans);
        Ok(())
    }

    /// Closes the file and ensures that all data that was written to it
    /// is stored. Returns time spent on chunking and hashing.
    pub fn close_file<C: Chunker, H: Hasher>(
        &mut self,
        mut handle: FileHandle<C, H>,
    ) -> io::Result<WriteMeasurements> {
        let span = self.storage.flush(&mut handle.writer)?;
        self.file_layer.write(&mut handle, span);
        Ok(handle.close())
    }

    /// Reads all contents of the file from beginning to end and returns them.
    pub fn read_file_complete<C: Chunker, H: Hasher>(
        &self,
        handle: &FileHandle<C, H>,
    ) -> io::Result<Vec<u8>> {
        let hashes = self.file_layer.read_complete(handle);
        Ok(self.storage.retrieve(hashes)?.concat()) // it assumes that all retrieved data segments are in correct order
    }

    /// Reads 1 MB of data from a file and returns it.
    pub fn read_from_file<C: Chunker, H: Hasher>(
        &mut self,
        handle: &mut FileHandle<C, H>,
    ) -> io::Result<Vec<u8>> {
        let hashes = self.file_layer.read(handle);
        Ok(self.storage.retrieve(hashes)?.concat())
    }
}

pub struct FileOpener<C, H>
where
    C: Chunker,
    H: Hasher,
{
    chunker: Option<C>,
    hasher: Option<H>,
    create_new: bool,
}

#[derive(Debug)]
pub enum OpenError {
    NoChunkerProvided,
    NoHasherProvided,
    IoError(std::io::Error),
}

impl Display for OpenError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::NoChunkerProvided => write!(
                f,
                "No chunker was provided. A chunker is necessary to write to the file."
            ),
            OpenError::NoHasherProvided => write!(
                f,
                "No hasher was provided. A hasher is necessary to write to the file."
            ),
            OpenError::IoError(io) => io.fmt(f),
        }
    }
}

impl Error for OpenError {}

impl From<std::io::Error> for OpenError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}

impl From<ErrorKind> for OpenError {
    fn from(value: ErrorKind) -> Self {
        Self::IoError(value.into())
    }
}

impl<C, H> FileOpener<C, H>
where
    C: Chunker,
    H: Hasher,
{
    pub fn new() -> Self {
        Self {
            chunker: None,
            hasher: None,
            create_new: false,
        }
    }

    pub fn with_chunker(mut self, chunker: C) -> Self {
        self.chunker = Some(chunker);
        self
    }

    pub fn with_hasher(mut self, hasher: H) -> Self {
        self.hasher = Some(hasher);
        self
    }

    pub fn create_new(mut self, create_new: bool) -> Self {
        self.create_new = create_new;
        self
    }

    pub fn open<B: Base>(
        self,
        fs: &mut FileSystem<B>,
        name: &str,
    ) -> Result<FileHandle<C, H>, OpenError> {
        let chunker = self.chunker.ok_or(OpenError::NoChunkerProvided)?;
        let hasher = self.hasher.ok_or(OpenError::NoHasherProvided)?;

        if self.create_new && fs.file_exists(name) {
            return Err(ErrorKind::AlreadyExists.into());
        } else if self.create_new {
            return fs
                .create_file(name.to_string(), chunker, hasher)
                .map_err(OpenError::IoError);
        }

        fs.open_file(name, chunker, hasher)
            .map_err(OpenError::IoError)
    }
}

impl<C, H> Default for FileOpener<C, H>
where
    C: Chunker,
    H: Hasher,
{
    fn default() -> Self {
        Self::new()
    }
}
