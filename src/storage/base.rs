use crate::storage::Segment;
use crate::Hash;

pub trait Base {
    fn save(&mut self, segments: Vec<Segment>) -> std::io::Result<()>;

    fn retrieve(&mut self, request: Vec<Hash>) -> std::io::Result<Vec<Vec<u8>>>;
}
