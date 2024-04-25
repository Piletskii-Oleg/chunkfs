use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;

use crate::hasher::ChunkHash;

/// Serves as base functionality for storing the actual data.
pub trait Database<Hash: ChunkHash> {
    /// Saves given data to the underlying storage.
    fn save(&mut self, segments: Vec<Segment<Hash>>) -> io::Result<()>;

    /// Clones and returns the data corresponding to the given hashes, or returns Error(NotFound),
    /// if some of the hashes were not found.
    fn retrieve(&self, request: Vec<Hash>) -> io::Result<Vec<Vec<u8>>>;
}

/// A data segment with corresponding hash.
pub struct Segment<Hash: ChunkHash> {
    pub hash: Hash,
    pub data: Vec<u8>,
}

impl<Hash: ChunkHash> Segment<Hash> {
    pub fn new(hash: Hash, data: Vec<u8>) -> Self {
        Self { hash, data }
    }
}

/// Simple in-memory hashmap-based storage.
#[derive(Default)]
pub struct HashMapBase<Hash: ChunkHash> {
    segment_map: HashMap<Hash, Vec<u8>>, // hashmap<Hash, RefCell<Vec<u8>> for referencing
}

impl<Hash: ChunkHash> Database<Hash> for HashMapBase<Hash> {
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
