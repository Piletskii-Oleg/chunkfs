mod file_layer;
mod storage;

pub struct Chunk {
    offset: usize,
    length: usize,
}

// written to base. make into a hashmap?
pub struct Segment {
    hash: u64,     // key
    data: Vec<u8>, // or [u8]? deduplicated?
}

pub struct Span {
    hash: u64,
    length: usize,
}

// in file layer
pub struct FileSpan {
    hash: u64,
    offset: usize,
}

pub trait Chunker {
    fn next_chunk(&mut self) -> Option<Chunk>;
}

pub trait Hasher {
    fn hash(&mut self) -> u64; // type for hash?
}

pub trait Base {
    fn save(&mut self); // std::io::Result?
}
