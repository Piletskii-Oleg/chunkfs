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
pub struct FileHandle {
    // can't make file_name a reference, or it would count as an immutable reference for FileSystem
    file_name: String,
    spans: Vec<FileSpan>,
    is_modified: bool,
}

impl File {
    fn new(name: String, spans: Vec<FileSpan>) -> Self {
        File { name, spans }
    }
}

impl FileHandle {
    fn from_file(file: &File) -> Self {
        FileHandle {
            file_name: file.name.clone(),
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
            // self.file.spans = self.spans;
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

    pub fn open(&self, name: &str) -> Option<FileHandle> {
        // uses iter_mut because FileHandle requires &mut File
        if let Some(file) = self.files.iter().find(|file| file.name == name) {
            Some(FileHandle::from_file(file))
        } else {
            None
        }
    }

    fn find_file(&self, handle: &FileHandle) -> &File {
        self.files
            .iter()
            .find(|file| file.name == handle.file_name)
            .unwrap()
    }

    fn find_file_mut(&mut self, handle: &FileHandle) -> &mut File {
        self.files
            .iter_mut()
            .find(|file| file.name == handle.file_name)
            .unwrap()
    }

    pub fn read(&self, handle: &FileHandle) -> Vec<Hash> {
        let file = self.find_file(handle);
        // this is probably not what was intended
        // it simply reads all hashes continuously and clones them
        file.spans
            .iter()
            .map(|span| span.hash.clone()) // cloning hashes
            .collect()
    }

    pub fn write(&mut self, handle: &FileHandle, spans: Vec<Span>) {
        let file = self.find_file_mut(handle);
        let mut offset = 0;
        for span in spans {
            file.spans.push(FileSpan {
                hash: span.hash,
                offset,
            });
            offset += span.length;
        }
    }

    pub fn close(&mut self, _handle: FileHandle) {}
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
