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
    pub fn open(&self, name: String) -> std::io::Result<FileHandle> {
        todo!()
    }

    pub fn create(&mut self, name: String) -> FileHandle {
        self.file_layer.create(name)
    }

    pub fn write(&mut self, handle: &mut FileHandle, data: &[u8]) -> std::io::Result<()> {
        let spans = self.storage.write(data)?;
        handle.write(spans);
        Ok(())
    }

    pub fn close(&mut self, handle: FileHandle) -> std::io::Result<()> {
        handle.close();
        Ok(())
    }

    pub fn read(&mut self, handle: FileHandle) -> std::io::Result<Vec<u8>> {
        todo!()
    }
}
