use crate::Hash;

// in file layer
pub struct FileSpan {
    hash: Hash,
    offset: usize,
}

pub struct FileLayer {
    files: Vec<File>,
}

pub struct FileHandle {}

pub struct File {
    name: String,
    spans: Vec<FileSpan>,
}
