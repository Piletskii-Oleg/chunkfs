use std::io;
use std::path::Path;
use sled::Error;
use crate::{ChunkHash, Database};

struct SledStorage {
    db: sled::Db
}

impl SledStorage {
    fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }
}

impl<Hash: ChunkHash, V: Clone> Database<Hash, V> for SledStorage {
    fn insert(&mut self, key: Hash, value: V) -> io::Result<()> {
        self.db.insert(key, value)?;

        Ok(())
    }

    fn get(&self, key: &Hash) -> io::Result<V> {
        self.db.get(key)?
    }

    fn contains(&self, key: &Hash) -> bool {
        todo!()
    }
}