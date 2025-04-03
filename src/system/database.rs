use crate::ChunkHash;
use std::collections::HashMap;
use std::io;

/// Serves as base functionality for storing the actual data as key-value pairs.
///
/// Supports inserting and getting values by key, checking if the key is present in the storage.
pub trait Database<K, V> {
    /// Inserts a key-value pair into the storage. If the key is already present, then nothing happens.
    fn try_insert(&mut self, key: K, value: V) -> io::Result<()>;

    /// Inserts a key-value pair into the storage. If the key is already present, then rewrites it.
    fn insert(&mut self, key: K, value: V) -> io::Result<()>;

    /// Retrieves a value by a given key. Note that it returns a value, not a reference.
    ///
    /// # Errors
    /// Should return [ErrorKind::NotFound], if the key-value pair
    /// was not found in the storage.
    fn get(&self, key: &K) -> io::Result<V>;

    /// Try inserts multiple key-value pairs into the storage.
    fn try_insert_multi(&mut self, pairs: Vec<(K, V)>) -> io::Result<()> {
        for (key, value) in pairs.into_iter() {
            self.try_insert(key, value)?;
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

/// Allows iteration over database contents.
pub trait IterableDatabase<K, V>: Database<K, V> {
    /// Returns a simple immutable iterator over values.
    fn iterator(&self) -> Box<dyn Iterator<Item = (&K, &V)> + '_>;

    /// Returns an iterator that can mutate values but not keys.
    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item = (&K, &mut V)> + '_>;

    /// Returns an immutable iterator over keys.
    fn keys<'a>(&'a self) -> Box<dyn Iterator<Item = &'a K> + 'a>
    where
        V: 'a;

    //// Returns an immutable iterator over value copies.
    fn values(&self) -> Box<dyn Iterator<Item = V> + '_>;

    /// Returns a mutable iterator over values.
    fn values_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut V> + 'a>
    where
        K: 'a,
    {
        Box::new(self.iterator_mut().map(|(_, v)| v))
    }

    /// Clears the database, removing all contained key-value pairs.
    fn clear(&mut self) -> io::Result<()>;
}

impl<Hash: ChunkHash, V: Clone> Database<Hash, V> for HashMap<Hash, V> {
    fn try_insert(&mut self, key: Hash, value: V) -> io::Result<()> {
        self.entry(key).or_insert(value);
        Ok(())
    }

    fn insert(&mut self, key: Hash, value: V) -> io::Result<()> {
        self.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &Hash) -> io::Result<V> {
        self.get(key).ok_or(io::ErrorKind::NotFound.into()).cloned()
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

    fn keys<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Hash> + 'a>
    where
        V: 'a,
    {
        Box::new(self.keys())
    }

    fn values(&self) -> Box<dyn Iterator<Item = V> + '_> {
        Box::new(self.values().map(|v| v.clone()))
    }

    fn clear(&mut self) -> io::Result<()> {
        HashMap::clear(self);
        Ok(())
    }
}
