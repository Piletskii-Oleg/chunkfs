use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;

use crate::ChunkHash;

/// Serves as base functionality for storing the actual data.
pub trait Database<K, V> {
    /// Inserts a key-value pair into the storage.
    fn insert(&mut self, key: K, value: V) -> io::Result<()>;

    /// Retrieves a value by a given key. Note that it returns a value, not a reference.
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

    /// Returns `true` if the database contains a value for the specified key.
    fn contains(&self, key: &K) -> bool;
}

pub trait IterableDatabase<K, V>: Database<K, V> {
    fn iterator(&self) -> Box<dyn Iterator<Item = (&K, &V)> + '_>;

    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item = (&K, &mut V)> + '_>;

    fn keys<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a K> + 'a>
    where
        V: 'a,
    {
        Box::new(self.iterator().map(|(k, _)| k))
    }

    fn values<'a>(&'a self) -> Box<dyn Iterator<Item = &'a V> + 'a>
    where
        K: 'a,
    {
        Box::new(self.iterator().map(|(_, v)| v))
    }

    fn values_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut V> + 'a>
    where
        K: 'a,
    {
        Box::new(self.iterator_mut().map(|(_, v)| v))
    }
}

impl<Hash: ChunkHash, V: Clone> IterableDatabase<Hash, V> for HashMap<Hash, V> {
    fn iterator(&self) -> Box<dyn Iterator<Item = (&Hash, &V)> + '_> {
        Box::new(self.iter())
    }

    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item = (&Hash, &mut V)> + '_> {
        Box::new(self.iter_mut())
    }
}

impl<Hash: ChunkHash, V: Clone> Database<Hash, V> for HashMap<Hash, V> {
    fn insert(&mut self, key: Hash, value: V) -> io::Result<()> {
        self.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &Hash) -> io::Result<V> {
        self.get(key).ok_or(ErrorKind::NotFound.into()).cloned()
    }

    fn remove(&mut self, key: &Hash) {
        self.remove(key);
    }

    fn contains(&self, key: &Hash) -> bool {
        self.contains_key(key)
    }
}
