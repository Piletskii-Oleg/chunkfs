use crate::storage::Span;
use crate::Hash;

#[derive(Debug, PartialEq)]
pub struct FileSpan {
    hash: Hash,
    offset: usize,
}

#[derive(Debug, PartialEq)]
pub struct File {
    name: String,
    spans: Vec<FileSpan>,
}

#[derive(Debug)]
pub struct FileLayer {
    files: Vec<File>,
}

#[derive(Debug, PartialEq)]
pub struct FileHandle<'a> {
    file: &'a mut File,
    spans: Vec<FileSpan>,
    is_modified: bool,
}

impl File {
    fn new(name: String, spans: Vec<FileSpan>) -> Self {
        File { name, spans }
    }
}

impl<'a> FileHandle<'a> {
    fn from_file(file: &'a mut File) -> Self {
        FileHandle {
            file,
            spans: vec![],
            is_modified: false,
        }
    }

    pub fn write(&mut self, spans: Vec<Span>) {
        let mut offset = 0;
        for span in spans {
            // pushes spans to itself first, pushes them to file after close
            self.spans.push(FileSpan {
                hash: span.hash,
                offset,
            });
            offset += span.length;
        }
        self.is_modified = true;
    }

    pub fn close(self) {
        // full rewrite on close if any writes were done
        if !self.is_modified {
            self.file.spans = self.spans;
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum FileError {
    FileAlreadyExists,
}

impl FileLayer {
    pub fn new() -> Self {
        FileLayer { files: vec![] }
    }
    pub fn create(&mut self, name: String) -> Result<FileHandle, FileError> {
        if self.files.iter().find(|file| file.name == name).is_some() {
            return Err(FileError::FileAlreadyExists);
        }

        let file = File::new(name, vec![]);
        self.files.push(file);
        Ok(FileHandle::from_file(self.files.last_mut().unwrap()))
    }

    pub fn open(&mut self, name: &str) -> Option<FileHandle> {
        // uses iter_mut because FileHandle requires &mut File
        if let Some(file) = self.files.iter_mut().find(|file| file.name == name) {
            Some(FileHandle::from_file(file))
        } else {
            None
        }
    }

    pub fn read(&self, handle: &FileHandle) -> Vec<Hash> {
        // this is probably not what was intended
        // it simply reads all hashes continuously and clones them
        handle
            .file
            .spans
            .iter()
            .map(|span| span.hash.clone()) // cloning hashes
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::file_layer::{FileError, FileLayer};

    #[test]
    fn file_layer_create_file() {
        let mut fl = FileLayer::new();
        fl.create("hello".to_string()).unwrap();

        assert_eq!(fl.files[0].name, "hello");
        assert_eq!(fl.files[0].spans, vec![]);
    }

    #[test]
    fn cant_create_two_files_with_same_name() {
        let mut fl = FileLayer::new();
        fl.create("hello".to_string()).unwrap();
        assert_eq!(
            fl.create("hello".to_string()),
            Err(FileError::FileAlreadyExists)
        )
    }
}
