use std::collections::HashMap;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::{hash, io};

use crate::base::Base;
use crate::chunker::Chunker;
use crate::storage::{Hasher, SpansInfo};
use crate::{WriteMeasurements, SEG_SIZE};

/// Hashed span, starting at `offset`.
#[derive(Debug, PartialEq, Eq, Default)]
pub struct FileSpan<Hash: hash::Hash + Eq + PartialEq + Clone> {
    hash: Hash,
    offset: usize,
}

/// A named file, doesn't store actual contents,
/// but rather hashes for them.
pub struct File<Hash: hash::Hash + Eq + PartialEq + Clone> {
    name: String,
    spans: Vec<FileSpan<Hash>>,
}

/// Layer that contains all [`files`][File], accessed by their names.
#[derive(Default)]
pub struct FileLayer<Hash: hash::Hash + Eq + PartialEq + Clone> {
    files: HashMap<String, File<Hash>>,
}

/// Handle for an open [`file`][File].
#[derive(Debug)]
pub struct FileHandle<C, H, Hash>
where
    C: Chunker,
    H: Hasher<Hash>,
    Hash: hash::Hash + Clone + Eq + PartialEq,
{
    // can't make file_name a reference
    // or have a reference to File,
    // or it would count as an immutable reference for FileSystem
    file_name: String,
    offset: usize,
    measurements: WriteMeasurements,
    // maybe not pub(crate) but something else? cannot think of anything
    pub(crate) chunker: C,
    pub(crate) hasher: H,
    pub(crate) write_buffer: Option<Vec<u8>>,
    d: PhantomData<Hash>,
}

impl<Hash: hash::Hash + Eq + PartialEq + Clone> File<Hash> {
    fn new(name: String) -> Self {
        File {
            name,
            spans: vec![],
        }
    }
}

impl<C, H, Hash> FileHandle<C, H, Hash>
where
    C: Chunker,
    H: Hasher<Hash>,
    Hash: hash::Hash + Clone + Eq + PartialEq,
{
    fn new(file: &File<Hash>, chunker: C, hasher: H) -> Self {
        FileHandle {
            file_name: file.name.clone(),
            offset: 0,
            measurements: Default::default(),
            chunker,
            hasher,
            write_buffer: Some(vec![]),
            d: Default::default(),
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

impl<Hash: hash::Hash + Eq + PartialEq + Clone> FileLayer<Hash> {
    /// Creates a [`file`][File] and returns its [`FileHandle`]
    pub fn create<C: Chunker, H: Hasher<Hash>>(
        &mut self,
        name: String,
        c: C,
        h: H,
        create_new: bool,
    ) -> io::Result<FileHandle<C, H, Hash>> {
        if !create_new && self.files.contains_key(&name) {
            return Err(ErrorKind::AlreadyExists.into());
        }

        let file = File::new(name.clone());
        let _ = self.files.insert(name.clone(), file);
        let written_file = self.files.get(&name).unwrap();
        Ok(FileHandle::new(written_file, c, h))
    }

    /// Opens a [`file`][File] based on its name and returns its [`FileHandle`]
    pub fn open<C: Chunker, H: Hasher<Hash>>(
        &self,
        name: &str,
        c: C,
        h: H,
    ) -> io::Result<FileHandle<C, H, Hash>> {
        self.files
            .get(name)
            .map(|file| FileHandle::new(file, c, h))
            .ok_or(ErrorKind::NotFound.into())
    }

    /// Returns reference to a file using [`FileHandle`] that corresponds to it.
    fn find_file<C: Chunker, H: Hasher<Hash>>(
        &self,
        handle: &FileHandle<C, H, Hash>,
    ) -> &File<Hash> {
        self.files.get(&handle.file_name).unwrap()
    }

    /// Returns mutable reference to a file using [`FileHandle`] that corresponds to it.
    fn find_file_mut<C: Chunker, H: Hasher<Hash>>(
        &mut self,
        handle: &FileHandle<C, H, Hash>,
    ) -> &mut File<Hash> {
        self.files.get_mut(&handle.file_name).unwrap()
    }

    /// Reads all hashes of the file, from beginning to end.
    pub fn read_complete<C: Chunker, H: Hasher<Hash>>(
        &self,
        handle: &FileHandle<C, H, Hash>,
    ) -> Vec<Hash> {
        let file = self.find_file(handle);
        file.spans
            .iter()
            .map(|span| span.hash.clone()) // cloning hashes, takes a lot of time
            .collect()
    }

    /// Writes spans to the end of the file.
    pub fn write<C: Chunker, H: Hasher<Hash>>(
        &mut self,
        handle: &mut FileHandle<C, H, Hash>,
        info: SpansInfo<Hash>,
    ) {
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
    pub fn read<C: Chunker, H: Hasher<Hash>>(
        &self,
        handle: &mut FileHandle<C, H, Hash>,
    ) -> Vec<Hash> {
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
        fl.create(name.clone(), FSChunker::new(4096), SimpleHasher, true)
            .unwrap();

        assert_eq!(fl.files.get(&name).unwrap().name, "hello");
        assert_eq!(fl.files.get(&name).unwrap().spans, vec![]);
    }

    #[test]
    fn cant_create_two_files_with_same_name() {
        let mut fl = FileLayer::default();
        fl.create(
            "hello".to_string(),
            FSChunker::new(4096),
            SimpleHasher,
            false,
        )
        .unwrap();

        let result = fl.create(
            "hello".to_string(),
            FSChunker::new(4096),
            SimpleHasher,
            false,
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::AlreadyExists);
    }
}
