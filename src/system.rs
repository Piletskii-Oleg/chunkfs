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

    pub fn open_file(&self, name: &str) -> Option<FileHandle> {
        self.file_layer.open(name)
    }

    // owned String or &str?
    pub fn create_file(&mut self, name: String) -> std::io::Result<FileHandle> {
        self.file_layer.create(name)
    }

    pub fn write_to_file(&mut self, handle: &FileHandle, data: &[u8]) -> std::io::Result<()> {
        let spans = self.storage.write(data)?;
        self.file_layer.write(handle, spans);
        Ok(())
    }

    pub fn close_file(&mut self, handle: FileHandle) -> std::io::Result<()> {
        let span = self.storage.flush()?;
        self.file_layer.write(&handle, vec![span]);
        Ok(())
    }

    pub fn read_from_file(&mut self, handle: &FileHandle) -> std::io::Result<Vec<u8>> {
        let hashes = self.file_layer.read(handle);
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
    fn write_read_test() {
        let mut fs = FileSystem {
            storage: Storage::new(FSChunker::new(4096), SimpleHasher, HashMapBase::default()),
            file_layer: FileLayer::default(),
        };

        let handle = fs.create_file("file".to_string()).unwrap();
        fs.write_to_file(&handle, &vec![1; 1024 * 1024]).unwrap();

        // file has to be closed in order to flush all remaining data
        // else test would not pass
        // how do we go around that? is it fine?
        fs.close_file(handle).unwrap();

        let handle = fs.open_file("file").unwrap();
        assert_eq!(fs.read_from_file(&handle).unwrap(), vec![1; 1024 * 1024]);
    }
}
