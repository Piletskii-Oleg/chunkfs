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

    pub fn read_from_file(&mut self, handle: &FileHandle) -> std::io::Result<Vec<u8>> {
        let hashes = self.file_layer.read(&handle);
        Ok(self.storage.retrieve_chunks(hashes)?.concat()) // it assumes that all retrieved data segments are in correct order
    }
}

#[cfg(test)]
mod tests {
    use crate::file_layer::FileLayer;
    use crate::storage::base::HashMapBase;
    use crate::storage::chunker::FSChunker;
    use crate::storage::{Hasher, Storage};
    use crate::{FileSystem, Hash};

    struct SimpleHasher;

    impl Hasher for SimpleHasher {
        fn hash(&mut self, data: &[u8]) -> Hash {
            data.to_vec()
        }
    }

    #[test]
    fn create_write_test() {
        let mut fs = FileSystem {
            storage: Storage::new(FSChunker::new(4096), SimpleHasher, HashMapBase::new()),
            file_layer: FileLayer::new(),
        };

        // first mutable borrow occurs here
        let mut handle = fs.create_file("file".to_string()).unwrap();
        // second mutable borrow occurs here - sadness
        fs.write_to_file(&mut handle, &vec![1; 1024 * 1024])
            .unwrap()

        // let handle_two = fs.open_file("file").unwrap();
    }
}
