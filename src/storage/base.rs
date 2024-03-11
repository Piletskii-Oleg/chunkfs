use std::collections::HashMap;

use crate::Hash;

pub trait Base {
    fn save(&mut self, segments: Vec<Segment>) -> std::io::Result<()>;

    fn retrieve(&mut self, request: Vec<Hash>) -> std::io::Result<Vec<Vec<u8>>>;
}

/// A data segment with hash
pub struct Segment {
    pub hash: Hash,
    pub data: Vec<u8>,
}

impl Segment {
    pub fn new(hash: Hash, data: Vec<u8>) -> Self {
        Self { hash, data }
    }
}

pub struct HashMapBase {
    segment_map: HashMap<Hash, Vec<u8>>,
}

impl HashMapBase {
    pub fn new() -> Self {
        Self {
            segment_map: HashMap::new(),
        }
    }
}

impl Base for HashMapBase {
    fn save(&mut self, segments: Vec<Segment>) -> std::io::Result<()> {
        for segment in segments {
            self.segment_map.entry(segment.hash).or_insert(segment.data);
        }
        Ok(())
    }

    fn retrieve(&mut self, request: Vec<Hash>) -> std::io::Result<Vec<Vec<u8>>> {
        // 1. unwrapping if no data is found. what kind of error can be used here?
        // 2. cloning stored data instead of passing reference.
        // is it how it is supposed to be or should we give a reference to underlying data?
        Ok(request
            .into_iter()
            .map(|hash| self.segment_map.get(&hash).unwrap().clone())
            .collect())
    }
}
