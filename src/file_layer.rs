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

pub struct FileHandle {}
