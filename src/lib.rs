mod file_layer;
mod storage;
mod system;

pub type Hash = Vec<u8>;

pub struct Chunk {
    offset: usize,
    length: usize,
}

pub struct Segment {
    hash: Hash,
    data: Vec<u8>, // or [u8]? deduplicated?
}

pub struct Span {
    hash: Hash,
    length: usize,
}

pub trait Chunker {
    fn next_chunk(&mut self) -> Option<Chunk>;
}

pub trait Hasher {
    fn hash(&mut self, data: &[u8]) -> Hash; // type for hash?
}

pub trait Base {
    fn save(&mut self, segments: Vec<Segment>) -> std::io::Result<()>;

    fn retrieve(&mut self) -> std::io::Result<Vec<Segment>>;
}
