use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;
use crate::ChunkHash;

/// Serves as base functionality for storing the actual data.
pub trait Database<K, V> {
    /// Inserts a key-value pair into the storage.
    fn insert(&mut self, key: K, value: V) -> io::Result<()>;

    /// Retrieves a value by a given key.
    ///
    /// # Errors
    /// Should return [ErrorKind::NotFound], if the key-value pair
    /// was not found in the storage.
    fn get(&self, key: &K) -> io::Result<V>;

    /// Removes a key-value pair from the storage, given the key.
    fn remove(&mut self, key: &K);

    /// Inserts multiple key-value pairs into the storage.
    fn insert_multi(&mut self, pairs: Vec<(K, V)>) -> io::Result<()> {
        for (key, value) in pairs.into_iter() {
            self.insert(key, value)?;
        }
        Ok(())
    }

    /// Retrieves a multitude of values, corresponding to the keys, in the correct order.
    fn get_multi(&self, keys: &[K]) -> io::Result<Vec<V>> {
        keys.iter().map(|key| self.get(key)).collect()
    }
}

impl<Hash: ChunkHash, V: Clone> Database<Hash, V> for HashMap<Hash, V> {
    fn insert(&mut self, key: Hash, value: V) -> io::Result<()> {
        self.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &Hash) -> io::Result<V> {
        self.get(key).cloned().ok_or(ErrorKind::NotFound.into())
    }

    fn remove(&mut self, key: &Hash) {
        self.remove(key);
    }
}
