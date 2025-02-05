use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;

use crate::system::storage::SpansInfo;
use crate::{ChunkHash, ChunkerRef};
use crate::{WriteMeasurements, SEG_SIZE};

/// Hashed span, starting at `offset`.
#[derive(Debug, PartialEq, Eq, Default, Clone)]
pub struct FileSpan<Hash: ChunkHash> {
    hash: Hash,
    offset: usize,
}

/// A named file, doesn't store actual contents,
/// but rather hashes for them.
#[derive(Clone)]
pub struct File<Hash: ChunkHash> {
    name: String,
    spans: Vec<FileSpan<Hash>>,
}

/// Layer that contains all [`files`][File], accessed by their names.
#[derive(Default)]
pub struct FileLayer<Hash: ChunkHash> {
    files: HashMap<String, File<Hash>>,
}

/// Handle for an open [`file`][File].
pub struct FileHandle {
    // can't make file_name a reference
    // or have a reference to File,
    // or it would count as an immutable reference for FileSystem
    file_name: String,
    offset: usize,
    measurements: WriteMeasurements,
    // maybe not pub(crate) but something else? cannot think of anything
    pub(crate) chunker: Option<ChunkerRef>,
}

impl<Hash: ChunkHash> File<Hash> {
    fn new(name: String) -> Self {
        File {
            name,
            spans: vec![],
        }
    }
}

impl FileHandle {
    fn new<Hash: ChunkHash>(file: &File<Hash>, chunker: ChunkerRef) -> Self {
        FileHandle {
            file_name: file.name.clone(),
            offset: 0,
            measurements: Default::default(),
            chunker: Some(chunker),
        }
    }

    fn new_readonly<Hash: ChunkHash>(file: &File<Hash>) -> Self {
        FileHandle {
            file_name: file.name.clone(),
            offset: 0,
            measurements: Default::default(),
            chunker: None,
        }
    }

    /// Returns name of the file.
    pub fn name(&self) -> &str {
        &self.file_name
    }

    /// Closes handle and returns [`WriteMeasurements`] made while file was open.
    pub(crate) fn close(self) -> WriteMeasurements {
        self.measurements
    }
}

impl<Hash: ChunkHash> FileLayer<Hash> {
    /// Creates a [`file`][File] and returns its [`FileHandle`]
    pub fn create(
        &mut self,
        name: impl Into<String>,
        chunker: ChunkerRef,
        create_new: bool,
    ) -> io::Result<FileHandle> {
        let name = name.into();
        if !create_new && self.files.contains_key(&name) {
            return Err(ErrorKind::AlreadyExists.into());
        }

        let file = File::new(name.clone());
        let _ = self.files.insert(name.clone(), file);
        let written_file = self.files.get(&name).unwrap();
        Ok(FileHandle::new(written_file, chunker))
    }

    /// Opens a [`file`][File] based on its name and returns its [`FileHandle`]
    pub fn open(&self, name: &str, chunker: ChunkerRef) -> io::Result<FileHandle> {
        self.files
            .get(name)
            .map(|file| FileHandle::new(file, chunker))
            .ok_or(ErrorKind::NotFound.into())
    }

    pub fn open_readonly(&self, name: &str) -> io::Result<FileHandle> {
        self.files
            .get(name)
            .map(|file| FileHandle::new_readonly(file))
            .ok_or(ErrorKind::NotFound.into())
    }

    /// Returns reference to a file using [`FileHandle`] that corresponds to it.
    fn find_file(&self, handle: &FileHandle) -> &File<Hash> {
        self.files.get(&handle.file_name).unwrap()
    }

    /// Returns mutable reference to a file using [`FileHandle`] that corresponds to it.
    fn find_file_mut(&mut self, handle: &FileHandle) -> &mut File<Hash> {
        self.files.get_mut(&handle.file_name).unwrap()
    }

    /// Reads all hashes of the file, from beginning to end.
    pub fn read_complete(&self, handle: &FileHandle) -> Vec<Hash> {
        let file = self.find_file(handle);
        file.spans
            .iter()
            .map(|span| span.hash.clone()) // cloning hashes, takes a lot of time
            .collect()
    }

    /// Writes spans to the end of the file.
    pub fn write(&mut self, handle: &mut FileHandle, info: SpansInfo<Hash>) {
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
    pub fn read(&self, handle: &mut FileHandle) -> Vec<Hash> {
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

    /// Checks if the file with the given name exists.
    pub fn file_exists(&self, name: &str) -> bool {
        self.files.contains_key(name)
    }

    /// Deletes all file data.
    pub fn clear(&mut self) {
        self.files.clear()
    }

    /// Gives out a distribution of the chunks with the same hash for the given file.
    pub fn chunk_count_distribution(&self, handle: &FileHandle) -> HashMap<Hash, (u32, usize)> {
        let file = self.find_file(handle);

        let mut distribution = HashMap::new();

        let lengths = file
            .spans
            .iter()
            .zip(file.spans.iter().skip(1))
            .map(|(first, second)| second.offset - first.offset);

        for (span, length) in file.spans.iter().zip(lengths) {
            distribution
                .entry(span.hash.clone())
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, length));
        }
        distribution
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use crate::chunkers::FSChunker;
    use crate::system::file_layer::FileLayer;

    #[test]
    fn file_layer_create_file() {
        let mut fl: FileLayer<Vec<u8>> = FileLayer::default();
        let name = "hello";
        let chunker = FSChunker::default().into();
        fl.create(name, chunker, true).unwrap();

        assert_eq!(fl.files.get(name).unwrap().name, "hello");
        assert_eq!(fl.files.get(name).unwrap().spans, vec![]);
    }

    #[test]
    fn cant_create_two_files_with_same_name() {
        let mut fl: FileLayer<Vec<u8>> = FileLayer::default();
        fl.create("hello".to_string(), FSChunker::new(4096).into(), false)
            .unwrap();

        let result = fl.create("hello".to_string(), FSChunker::new(4096).into(), false);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind(), ErrorKind::AlreadyExists);
    }
}
