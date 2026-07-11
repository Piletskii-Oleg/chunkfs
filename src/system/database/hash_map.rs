use crate::{ChunkHash, Database, IterableDatabase};
use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;

impl<Hash: ChunkHash, V: Clone> Database<Hash, V> for HashMap<Hash, V> {
    fn insert(&mut self, key: Hash, value: V) -> io::Result<()> {
        self.entry(key).or_insert(value);
        Ok(())
    }

    fn get(&self, key: &Hash) -> io::Result<V> {
        self.get(key).ok_or(ErrorKind::NotFound.into()).cloned()
    }

    fn contains(&self, key: &Hash) -> bool {
        self.contains_key(key)
    }
}

impl<Hash: ChunkHash, V: Clone> IterableDatabase<Hash, V> for HashMap<Hash, V> {
    fn iterator(&self) -> Box<dyn Iterator<Item = (&Hash, &V)> + '_> {
        Box::new(self.iter())
    }

    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item = (&Hash, &mut V)> + '_> {
        Box::new(self.iter_mut())
    }

    fn clear(&mut self) -> io::Result<()> {
        HashMap::clear(self);
        Ok(())
    }
}
