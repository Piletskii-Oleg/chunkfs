use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;

use crate::chunker::Chunker;
use crate::storage::{Hasher, SpansInfo};
use crate::{VecHash, WriteMeasurements, SEG_SIZE};

/// Hashed span, starting at `offset`.
#[derive(Debug, PartialEq, Eq, Default)]
pub struct FileSpan {
    hash: VecHash,
    offset: usize,
}

/// A named file, doesn't store actual contents,
/// but rather hashes for them.
pub struct File {
    name: String,
    spans: Vec<FileSpan>,
}

/// Layer that contains all files, accessed by their names.
#[derive(Default)]
pub struct FileLayer {
    files: HashMap<String, File>,
}

/// Handle for an open file.
#[derive(Debug)]
pub struct FileHandle<C, H>
where
    C: Chunker,
    H: Hasher,
{
    // can't make file_name a reference
    // or have a reference to File,
    // or it would count as an immutable reference for FileSystem
    file_name: String,
    offset: usize,
    measurements: WriteMeasurements,
    pub chunker: C,
    pub hasher: H,
    pub write_buffer: Option<Vec<u8>>,
}

impl File {
    fn new(name: String) -> Self {
        File {
            name,
            spans: vec![],
        }
    }
}

impl<C, H> FileHandle<C, H>
where
    C: Chunker,
    H: Hasher,
{
    fn new(file: &File, chunker: C, hasher: H) -> Self {
        FileHandle {
            file_name: file.name.clone(),
            offset: 0,
            measurements: Default::default(),
            chunker,
            hasher,
            write_buffer: Some(vec![]),
        }
    }

    /// Closes handle and returns `WriteMeasurements` made while file was open.
    pub(crate) fn close(self) -> WriteMeasurements {
        self.measurements
    }

    pub fn name(&self) -> &str {
        &self.file_name
    }
}

impl FileLayer {
    /// Creates a file and returns its `FileHandle`
    pub fn create<C: Chunker, H: Hasher>(
        &mut self,
        name: String,
        c: C,
        h: H,
    ) -> io::Result<FileHandle<C, H>> {
        if self.files.contains_key(&name) {
            return Err(ErrorKind::AlreadyExists.into());
        }

        let file = File::new(name.clone());
        let written_file = self.files.entry(name).or_insert(file);
        Ok(FileHandle::new(written_file, c, h))
    }

    /// Opens a file based on its name and returns its `FileHandle`
    pub fn open<C: Chunker, H: Hasher>(
        &self,
        name: &str,
        c: C,
        h: H,
    ) -> io::Result<FileHandle<C, H>> {
        self.files
            .get(name)
            .map(|file| FileHandle::new(file, c, h))
            .ok_or(ErrorKind::NotFound.into())
    }

    /// Returns reference to a file using `FileHandle` that corresponds to it.
    fn find_file<C: Chunker, H: Hasher>(&self, handle: &FileHandle<C, H>) -> &File {
        self.files.get(&handle.file_name).unwrap()
    }

    /// Returns mutable reference to a file using `FileHandle` that corresponds to it.
    fn find_file_mut<C: Chunker, H: Hasher>(&mut self, handle: &FileHandle<C, H>) -> &mut File {
        self.files.get_mut(&handle.file_name).unwrap()
    }

    /// Reads all hashes of the file, from beginning to end.
    pub fn read_complete<C: Chunker, H: Hasher>(&self, handle: &FileHandle<C, H>) -> Vec<VecHash> {
        let file = self.find_file(handle);
        file.spans
            .iter()
            .map(|span| span.hash.clone()) // cloning hashes
            .collect()
    }

    /// Writes spans to the end of the file.
    pub fn write<C: Chunker, H: Hasher>(&mut self, handle: &mut FileHandle<C, H>, info: SpansInfo) {
        let file = self.find_file_mut(handle);
        for span in info.spans {
            file.spans.push(FileSpan {
                hash: span.hash,
                offset: handle.offset,
            });
            handle.offset += span.length;
        }

        handle.measurements += info.measurements;
    }

    /// Reads 1 MB of data from the open file and returns received hashes,
    /// starting point is based on the `FileHandle`'s offset.
    pub fn read<C: Chunker, H: Hasher>(&mut self, handle: &mut FileHandle<C, H>) -> Vec<VecHash> {
        let file = self.find_file(handle);

        let mut bytes_read = 0;
        let mut last_offset = handle.offset;
        let hashes = file
            .spans
            .iter()
            .skip_while(|span| span.offset < handle.offset) // find current span in the file
            .take_while(|span| {
                bytes_read += span.offset - last_offset;
                last_offset = span.offset;
                bytes_read < SEG_SIZE
            }) // take 1 MB of spans after current one
            .map(|span| span.hash.clone()) // take their hashes
            .collect();

        handle.offset += bytes_read;

        hashes
    }

    pub fn file_exists(&self, name: &str) -> bool {
        self.files.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use crate::chunker::FSChunker;
    use crate::file_layer::FileLayer;
    use crate::hasher::SimpleHasher;

    #[test]
    fn file_layer_create_file() {
        let mut fl = FileLayer::default();
        let name = "hello".to_string();
        fl.create(name.clone(), FSChunker::new(4096), SimpleHasher)
            .unwrap();

        assert_eq!(fl.files.get(&name).unwrap().name, "hello");
        assert_eq!(fl.files.get(&name).unwrap().spans, vec![]);
    }

    #[test]
    fn cant_create_two_files_with_same_name() {
        let mut fl = FileLayer::default();
        fl.create("hello".to_string(), FSChunker::new(4096), SimpleHasher)
            .unwrap();

        let result = fl.create("hello".to_string(), FSChunker::new(4096), SimpleHasher);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::AlreadyExists);
    }
}
