use crate::file_layer::{FileError, FileHandle, FileLayer};
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
    pub fn open_file(&mut self, name: &str) -> Option<FileHandle> {
        self.file_layer.open(name)
    }

    pub fn create_file(&mut self, name: String) -> Result<FileHandle, FileError> {
        self.file_layer.create(name)
    }

    pub fn write_to_file(&mut self, handle: &mut FileHandle, data: &[u8]) -> std::io::Result<()> {
        let spans = self.storage.write(data)?;
        handle.write(spans);
        Ok(())
    }

    pub fn close_file(&mut self, handle: FileHandle) -> std::io::Result<()> {
        handle.close();
        Ok(())
    }

    pub fn read_from_file(&mut self, handle: FileHandle) -> std::io::Result<Vec<u8>> {
        let hashes = self.file_layer.read(&handle);
        Ok(self.storage.retrieve_chunks(hashes)?.concat()) // it assumes that all retrieved data segments are in correct order
    }
}
