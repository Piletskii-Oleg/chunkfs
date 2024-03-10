use crate::storage::Span;
use crate::Hash;

pub struct FileSpan {
    hash: Hash,
    offset: usize,
}

pub struct File {
    name: String,
    spans: Vec<FileSpan>,
}

pub struct FileLayer {
    files: Vec<File>,
}

pub struct FileHandle<'a> {
    file: &'a mut File,
    spans: Vec<FileSpan>,
}

impl File {
    fn new(name: String, spans: Vec<FileSpan>) -> Self {
        File { name, spans }
    }

    fn add_span(&mut self, file_span: FileSpan) {
        self.spans.push(file_span);
    }
}

impl<'a> FileHandle<'a> {
    fn from_file(file: &'a mut File) -> Self {
        FileHandle {
            file,
            spans: vec![],
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
    }

    pub fn close(mut self) {
        // full write only
        if !self.file.spans.is_empty() {
            self.file.spans = self.spans;
        }
    }
}

impl FileLayer {
    pub fn create(&mut self, name: String) -> FileHandle {
        let file = File::new(name, vec![]);
        self.files.push(file);
        FileHandle::from_file(self.files.last_mut().unwrap())
    }
}
