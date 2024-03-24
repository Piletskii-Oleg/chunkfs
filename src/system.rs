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

#[cfg(test)]
mod tests {
    use crate::file_layer::FileLayer;
    use crate::storage::base::HashMapBase;
    use crate::storage::chunker::FSChunker;
    use crate::storage::{Hasher, Storage};
    use crate::{FileSystem, VecHash};

    struct SimpleHasher;

    impl Hasher for SimpleHasher {
        fn hash(&mut self, data: &[u8]) -> VecHash {
            data.to_vec()
        }
    }

    #[test]
    fn write_read_complete_test() {
        let mut fs = FileSystem {
            storage: Storage::new(FSChunker::new(4096), SimpleHasher, HashMapBase::default()),
            file_layer: FileLayer::default(),
        };

        let mut handle = fs.create_file("file".to_string()).unwrap();
        fs.write_to_file(&mut handle, &[1; 1024 * 1024]).unwrap();
        fs.write_to_file(&mut handle, &[1; 1024 * 1024]).unwrap();

        fs.close_file(handle).unwrap();

        let handle = fs.open_file("file").unwrap();
        assert_eq!(
            fs.read_file_complete(&handle).unwrap(),
            vec![1; 1024 * 1024 * 2]
        );
    }

    #[test]
    fn write_read_blocks_test() {
        let mut fs = FileSystem {
            storage: Storage::new(FSChunker::new(4096), SimpleHasher, HashMapBase::default()),
            file_layer: FileLayer::default(),
        };

        let mut handle = fs.create_file("file".to_string()).unwrap();
        fs.write_to_file(&mut handle, &[1; 1024 * 1024]).unwrap();
        fs.write_to_file(&mut handle, &[2; 1024 * 1024]).unwrap();
        fs.write_to_file(&mut handle, &[3; 1024 * 1024]).unwrap();
        fs.close_file(handle).unwrap();

        let mut handle = fs.open_file("file").unwrap();
        assert_eq!(
            fs.read_from_file(&mut handle).unwrap(),
            vec![1; 1024 * 1024]
        );
        assert_eq!(
            fs.read_from_file(&mut handle).unwrap(),
            vec![2; 1024 * 1024]
        );
        assert_eq!(
            fs.read_from_file(&mut handle).unwrap(),
            vec![3; 1024 * 1024]
        );
    }
}
