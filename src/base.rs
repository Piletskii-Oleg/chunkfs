use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;

use crate::{ChunkHash, Database, Segment};

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
