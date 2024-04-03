use std::collections::HashMap;
use std::io::ErrorKind;

use crate::VecHash;

/// Serves as base functionality for storing the actual data
pub trait Base {
    /// Saves given data to the underlying storage.
    fn save(&mut self, segments: Vec<Segment>) -> std::io::Result<()>;

    /// Clones and returns the data corresponding to the given hashes, or returns Error(NotFound),
    /// if some of the hashes were not found.
    fn retrieve(&self, request: Vec<VecHash>) -> std::io::Result<Vec<Vec<u8>>>;
}

/// A data segment with corresponding hash.
pub struct Segment {
    pub hash: VecHash,
    pub data: Vec<u8>,
}

impl Segment {
    pub fn new(hash: VecHash, data: Vec<u8>) -> Self {
        Self { hash, data }
    }
}

/// Simple in-memory hashmap-based storage.
#[derive(Default)]
pub struct HashMapBase {
    segment_map: HashMap<VecHash, Vec<u8>>,
}

impl Base for HashMapBase {
    fn save(&mut self, segments: Vec<Segment>) -> std::io::Result<()> {
        for segment in segments {
            self.segment_map.entry(segment.hash).or_insert(segment.data);
        }
        Ok(())
    }

    fn retrieve(&self, request: Vec<VecHash>) -> std::io::Result<Vec<Vec<u8>>> {
        // 1. unwrapping if no data is found. what kind of error can be used here?
        // 2. cloning stored data instead of passing reference.
        // is it how it is supposed to be or should we give a reference to underlying data?
        request
            .into_iter()
            .map(|hash| {
                self.segment_map
                    .get(&hash)
                    .cloned()
                    .ok_or(ErrorKind::NotFound.into())
            })
            .collect()
    }
}
