use crate::file_layer::{FileHandle, FileLayer};
use crate::storage::Storage;
use crate::{Base, Chunker, Hasher};

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
    pub fn open(&self, name: String) -> std::io::Result<FileHandle> {
        todo!()
    }

    pub fn create(&mut self, name: String) -> FileHandle {
        todo!()
    }

    pub fn write(&mut self, handle: FileHandle, data: &[u8]) -> std::io::Result<()> {
        todo!()
    }

    pub fn close(&mut self, handle: FileHandle) -> std::io::Result<()> {
        todo!()
    }

    pub fn read(&mut self, handle: FileHandle) -> std::io::Result<Vec<u8>> {
        todo!()
    }
}
