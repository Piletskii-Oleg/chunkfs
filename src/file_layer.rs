use std::collections::HashMap;
use std::io::ErrorKind;
use std::time::Duration;

use crate::storage::SpansInfo;
use crate::{VecHash, WriteMeasurements, SEG_SIZE};

/// Hashed span, starting at `offset`
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

/// Layer that contains all files
#[derive(Default)]
pub struct FileLayer {
    files: HashMap<String, File>,
}

/// Handle for an opened file
#[derive(Debug, PartialEq)]
pub struct FileHandle {
    // can't make file_name a reference
    // or have a reference to File,
    // or it would count as an immutable reference for FileSystem
    file_name: String,
    offset: usize,
    chunk_time: Duration,
    // pub or do something else?
    hash_time: Duration,
}

impl File {
    fn new(name: String) -> Self {
        File {
            name,
            spans: vec![],
        }
    }
}

impl FileHandle {
    fn new(file: &File) -> Self {
        FileHandle {
            file_name: file.name.clone(),
            offset: 0,
            chunk_time: Default::default(),
            hash_time: Default::default(),
        }
    }

    /// Closes handle and returns `WriteMeasurements` made while file was open.
    pub fn close(self) -> WriteMeasurements {
        WriteMeasurements {
            chunk_time: self.chunk_time,
            hash_time: self.hash_time,
        }
    }
}

impl FileLayer {
    /// Creates a file and returns its `FileHandle`
    pub fn create(&mut self, name: String) -> std::io::Result<FileHandle> {
        if self.files.contains_key(&name) {
            return Err(std::io::Error::from(ErrorKind::AlreadyExists));
        }

        let file = File::new(name.clone());
        let file = self.files.entry(name).or_insert(file);
        Ok(FileHandle::new(file))
    }

    /// Opens a file based on its name and returns its `FileHandle`
    pub fn open(&self, name: &str) -> Option<FileHandle> {
        self.files.get(name).map(FileHandle::new)
    }

    /// Returns reference to a file using `FileHandle` that corresponds to it.
    fn find_file(&self, handle: &FileHandle) -> &File {
        self.files.get(&handle.file_name).unwrap()
    }

    /// Returns mutable reference to a file using `FileHandle` that corresponds to it.
    fn find_file_mut(&mut self, handle: &FileHandle) -> &mut File {
        self.files.get_mut(&handle.file_name).unwrap()
    }

    /// Reads all hashes of the file, from beginning to end
    pub fn read_complete(&self, handle: &FileHandle) -> Vec<VecHash> {
        let file = self.find_file(handle);
        file.spans
            .iter()
            .map(|span| span.hash.clone()) // cloning hashes
            .collect()
    }

    /// Writes spans to the end of the file
    pub fn write(&mut self, handle: &mut FileHandle, info: SpansInfo) {
        let file = self.find_file_mut(handle);
        for span in info.spans {
            file.spans.push(FileSpan {
                hash: span.hash,
                offset: handle.offset,
            });
            handle.offset += span.length;
        }

        handle.chunk_time += info.chunk_time;
        handle.hash_time += info.hash_time;
    }

    /// Reads 1 MB of data from the open file and returns received hashes,
    /// starting point is based on the `FileHandle`'s offset.
    pub fn read(&mut self, handle: &mut FileHandle) -> Vec<VecHash> {
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
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use crate::file_layer::FileLayer;

    #[test]
    fn file_layer_create_file() {
        let mut fl = FileLayer::default();
        let name = "hello".to_string();
        fl.create(name.clone()).unwrap();

        assert_eq!(fl.files.get(&name).unwrap().name, "hello");
        assert_eq!(fl.files.get(&name).unwrap().spans, vec![]);
    }

    #[test]
    fn cant_create_two_files_with_same_name() {
        let mut fl = FileLayer::default();
        fl.create("hello".to_string()).unwrap();

        let result = fl.create("hello".to_string());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::AlreadyExists);
    }
}
