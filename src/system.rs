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

    /// Tries to open a file with the given name and returns its `FileHandle` if it exists,
    /// or `None`, if it doesn't.
    pub fn open_file<C: Chunker, H: Hasher>(
        &self,
        name: &str,
        c: C,
        h: H,
    ) -> Option<FileHandle<C, H>> {
        self.file_layer.open(name, c, h)
    }

    /// Creates a file with the given name and returns its `FileHandle`.
    /// Returns `ErrorKind::AlreadyExists`, if the file with the same name exists in the file system.
    pub fn create_file<C: Chunker, H: Hasher>(
        &mut self,
        name: String,
        c: C,
        h: H,
    ) -> std::io::Result<FileHandle<C, H>> {
        self.file_layer.create(name, c, h)
    }

    /// Writes given data to the file. Size of the slice must be exactly 1 MB.
    pub fn write_to_file<C: Chunker, H: Hasher>(
        &mut self,
        handle: &mut FileHandle<C, H>,
        data: &[u8],
    ) -> std::io::Result<()> {
        let spans = self.storage.write(data, &mut handle.writer)?;
        self.file_layer.write(handle, spans);
        Ok(())
    }

    /// Closes the file and ensures that all data that was written to it
    /// is stored. Returns time spent on chunking and hashing.
    pub fn close_file<C: Chunker, H: Hasher>(
        &mut self,
        mut handle: FileHandle<C, H>,
    ) -> std::io::Result<WriteMeasurements> {
        let span = self.storage.flush(&mut handle.writer)?;
        self.file_layer.write(&mut handle, span);
        Ok(handle.close())
    }

    /// Reads all contents of the file from beginning to end and returns them.
    pub fn read_file_complete<C: Chunker, H: Hasher>(
        &self,
        handle: &FileHandle<C, H>,
    ) -> std::io::Result<Vec<u8>> {
        let hashes = self.file_layer.read_complete(handle);
        Ok(self.storage.retrieve(hashes)?.concat()) // it assumes that all retrieved data segments are in correct order
    }

    /// Reads 1 MB of data from a file and returns it.
    pub fn read_from_file<C: Chunker, H: Hasher>(
        &mut self,
        handle: &mut FileHandle<C, H>,
    ) -> std::io::Result<Vec<u8>> {
        let hashes = self.file_layer.read(handle);
        Ok(self.storage.retrieve(hashes)?.concat())
    }
}
