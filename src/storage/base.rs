use crate::storage::Segment;

pub trait Base {
    fn save(&mut self, segments: Vec<Segment>) -> std::io::Result<()>;

    fn retrieve(&mut self) -> std::io::Result<Vec<Segment>>;
}
