use std::collections::HashMap;
use std::io::ErrorKind;
use std::{hash, io};

/// Serves as base functionality for storing the actual data.
pub trait Database<Hash: hash::Hash + Clone + Eq + PartialEq + Default> {
    /// Saves given data to the underlying storage.
    fn save(&mut self, segments: Vec<Segment<Hash>>) -> io::Result<()>;

    /// Clones and returns the data corresponding to the given hashes, or returns Error(NotFound),
    /// if some of the hashes were not found.
    fn retrieve(&self, request: Vec<Hash>) -> io::Result<Vec<Vec<u8>>>;
}

/// A data segment with corresponding hash.
pub struct Segment<Hash: hash::Hash + Clone + Eq + PartialEq + Default> {
    pub hash: Hash,
    pub data: Vec<u8>,
}

impl<Hash: hash::Hash + Clone + Eq + PartialEq + Default> Segment<Hash> {
    pub fn new(hash: Hash, data: Vec<u8>) -> Self {
        Self { hash, data }
    }
}

/// Simple in-memory hashmap-based storage.
#[derive(Default)]
pub struct HashMapBase<Hash: hash::Hash + Clone + Eq + PartialEq + Default> {
    segment_map: HashMap<Hash, Vec<u8>>, // hashmap<Hash, RefCell<Vec<u8>> for referencing
}

impl<Hash: hash::Hash + Clone + Eq + PartialEq + Default> Database<Hash> for HashMapBase<Hash> {
    fn save(&mut self, segments: Vec<Segment<Hash>>) -> io::Result<()> {
        for segment in segments {
            self.segment_map.entry(segment.hash).or_insert(segment.data);
        }
        Ok(())
    }

    // vec<result>?
    fn retrieve(&self, request: Vec<Hash>) -> io::Result<Vec<Vec<u8>>> {
        // cloning stored data instead of passing reference.
        // is it how it is supposed to be or should we give a reference to underlying data?
        request
            .into_iter()
            .map(|hash| {
                self.segment_map
                    .get(&hash)
                    .cloned() // can be done without cloning
                    .ok_or(ErrorKind::NotFound.into())
            })
            .collect()
    }
}
