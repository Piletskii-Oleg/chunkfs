use crate::file_layer::{FileHandle, FileLayer};
use crate::storage::{Base, Chunker, Hasher, Storage};

pub struct FileSystem<C, H, B>
where
    C: Chunker,
    H: Hasher,
    B: Base,
{
    storage: Storage<C, H, B>,
    file_layer: FileLayer,
}

impl<C, H, B> FileSystem<C, H, B>
where
    C: Chunker,
    H: Hasher,
    B: Base,
{
    pub fn new(chunker: C, hasher: H, base: B) -> Self {
        Self {
            storage: Storage::new(chunker, hasher, base),
            file_layer: Default::default(),
        }
    }

    /// Tries to open a file with the given name and returns its `FileHandle` if it exists,
    /// or `None`, if it doesn't.
    pub fn open_file(&self, name: &str) -> Option<FileHandle> {
        self.file_layer.open(name)
    }

    /// Creates a file with the given name and returns its `FileHandle`.
    /// Returns `ErrorKind::AlreadyExists`, if the file with the same name exists in the file system.
    pub fn create_file(&mut self, name: String) -> std::io::Result<FileHandle> {
        self.file_layer.create(name)
    }

    /// Writes given data to the file. Size of the slice must be exactly 1 MB.
    pub fn write_to_file(&mut self, handle: &mut FileHandle, data: &[u8]) -> std::io::Result<()> {
        let spans = self.storage.write(data)?;
        self.file_layer.write(handle, spans);
        Ok(())
    }

    /// Closes the file and ensures that all data that was written to it
    /// is stored.
    pub fn close_file(&mut self, mut handle: FileHandle) -> std::io::Result<()> {
        let span = self.storage.flush()?;
        self.file_layer.write(&mut handle, vec![span]);
        Ok(())
    }

    /// Reads all contents of the file from beginning to end and returns them.
    pub fn read_file_complete(&self, handle: &FileHandle) -> std::io::Result<Vec<u8>> {
        let hashes = self.file_layer.read_complete(handle);
        Ok(self.storage.retrieve(hashes)?.concat()) // it assumes that all retrieved data segments are in correct order
    }

    /// Reads 1 MB of data from a file and returns it.
    pub fn read_from_file(&mut self, handle: &mut FileHandle) -> std::io::Result<Vec<u8>> {
        let hashes = self.file_layer.read(handle);
        Ok(self.storage.retrieve(hashes)?.concat())
    }
}

pub struct FileSystemBuilder<C, H, B>
where
    C: Chunker,
    H: Hasher,
    B: Base,
{
    chunker: Option<C>,
    hasher: Option<H>,
    base: Option<B>,
}

impl<C, H, B> FileSystemBuilder<C, H, B>
where
    C: Chunker,
    H: Hasher,
    B: Base,
{
    pub fn new() -> Self {
        FileSystemBuilder {
            chunker: None,
            hasher: None,
            base: None,
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

    pub fn with_base(mut self, base: B) -> Self {
        self.base = Some(base);
        self
    }

    pub fn build(self) -> Result<FileSystem<C, H, B>, String> {
        let chunker = self.chunker.ok_or("No chunker provided")?;
        let hasher = self.hasher.ok_or("No hasher provided")?;
        let base = self.base.ok_or("No base provided")?;
        Ok(FileSystem {
            storage: Storage::new(chunker, hasher, base),
            file_layer: Default::default(),
        })
    }
}

impl<C, H, B> Default for FileSystemBuilder<C, H, B>
where
    C: Chunker,
    H: Hasher,
    B: Base,
{
    fn default() -> Self {
        Self::new()
    }
}
