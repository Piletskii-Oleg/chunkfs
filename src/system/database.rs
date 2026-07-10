use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;

use crate::{ChunkHash, MB};

/// Serves as base functionality for storing the actual data as key-value pairs.
///
/// Supports inserting and getting values by key, checking if the key is present in the storage.
pub trait Database<K, V> {
    /// Inserts a key-value pair into the storage.
    fn insert(&mut self, key: K, value: V) -> io::Result<()>;

    /// Retrieves a value by a given key. Note that it returns a value, not a reference.
    ///
    /// # Errors
    /// Should return [ErrorKind::NotFound], if the key-value pair
    /// was not found in the storage.
    fn get(&self, key: &K) -> io::Result<V>;

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

/// Allows iteration over database contents.
pub trait IterableDatabase<K, V>: Database<K, V> {
    /// Returns a simple immutable iterator over values.
    fn iterator(&self) -> Box<dyn Iterator<Item = (&K, &V)> + '_>;

    /// Returns an iterator that can mutate values but not keys.
    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item = (&K, &mut V)> + '_>;

    /// Returns an immutable iterator over keys.
    fn keys<'a>(&'a self) -> Box<dyn Iterator<Item = &'a K> + 'a>
    where
        V: 'a,
    {
        Box::new(self.iterator().map(|(k, _)| k))
    }

    /// Returns an immutable iterator over values.
    fn values<'a>(&'a self) -> Box<dyn Iterator<Item = &'a V> + 'a>
    where
        K: 'a,
    {
        Box::new(self.iterator().map(|(_, v)| v))
    }

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

use std::hash::Hash;

/// Container subsystem configuration.
#[derive(Clone, Debug)]
pub struct ContainerConfig {
    /// Maximum size of a container in bytes, defaults to 4 MB.
    pub max_size: usize,
}

impl Default for ContainerConfig {
    fn default() -> Self {
        Self { max_size: 4 * MB }
    }
}

/// Immutable data structure that stores several chunks.
#[derive(Clone, Debug)]
struct Container<K, V> {
    // Correspondence between keys and actual data.
    values: HashMap<K, V>,
    // Currently used space.
    size: usize,
    // Maximum available space of the container.
    max_size: usize,
}

impl<K, V> Container<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    // Creates a new container with fixed maximum available space.
    fn new(max_size: usize) -> Self {
        Self {
            values: HashMap::new(),
            max_size,
            size: 0,
        }
    }

    // Returns the currently used container space.
    fn size(&self) -> usize {
        self.size
    }

    // Inserts a new entry into the container. Note if a key is already present it is
    // overwritten. The old entry is forgotten and is overwritten with a new one. Extra space is
    // used.
    fn insert(&mut self, key: K, value: V, value_size: usize) -> io::Result<()> {
        if self.size + value_size > self.max_size {
            let msg = "The container is too full for the desired data";
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, msg));
        }

        self.values.insert(key, value);
        self.size += value_size;

        Ok(())
    }

    // Gets the latest value inserted with the desired key.
    fn get(&self, key: &K) -> io::Result<V> {
        self.values
            .get(key)
            .ok_or(ErrorKind::NotFound.into())
            .cloned()
    }
}

/// Database implementation that stores values in immutable fixed-size containers.
pub struct ContainerDatabase<K, V> {
    // Database configuration primarily configuring a single container size.
    config: ContainerConfig,
    // A list of containers that are "sealed" which can be considered read-only.
    containers: Vec<Container<K, V>>,
    // A current container that is being filled, kind of append+read-only.
    current: Container<K, V>,
    // Correspondence between keys and container numbers (container ids).
    index: HashMap<K, usize>,
}

impl<K, V> ContainerDatabase<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    // Creates a new containerized key-value database from with the desired config.
    pub fn new(config: ContainerConfig) -> Self {
        Self {
            current: Container::new(config.max_size),
            config,
            containers: Vec::new(),
            index: HashMap::new(),
        }
    }

    // Issues a new container to fit the desired size.
    fn container(&mut self, value_size: usize) -> (usize, &mut Container<K, V>) {
        if self.current.size() + value_size > self.current.max_size {
            let container =
                std::mem::replace(&mut self.current, Container::new(self.config.max_size));
            self.containers.push(container)
        }
        (self.containers.len(), &mut self.current)
    }

    /// Returns statistics about the container system.
    pub fn stats(&self) -> ContainerStats {
        let sealed_containers = self.containers.len();
        let current_size = self.current.size();
        let total_containers = sealed_containers + if self.current.size() > 0 { 1 } else { 0 };

        let total_sealed_size: usize = self.containers.iter().map(|c| c.size()).sum();
        let total_size = total_sealed_size + current_size;

        ContainerStats {
            total_containers,
            sealed_containers,
            total_size,
        }
    }

    // Performs garbage collection: removes unused containers.
    pub fn start_gc(&mut self) {
        let current_id = self.containers.len();
        let mut marked: Vec<bool> = vec![false; current_id + 1];
        self.index.iter().for_each(|(_, id)| {
            marked[*id] = true;
        });

        let mut new_ids: HashMap<usize, usize> = self
            .containers
            .iter()
            .enumerate()
            .filter(|(id, _)| marked[*id])
            .enumerate()
            .map(|(new_id, (old_id, _))| (old_id, new_id))
            .collect();
        new_ids.insert(current_id, new_ids.len());

        let mut id = 0;
        self.containers.retain(|_| {
            let r = marked[id];
            id += 1;
            r
        });

        self.index.iter_mut().for_each(|(_, v)| *v = new_ids[v]);
    }

    // Perform compaction: remove gaps between the used elements within the containers.
    pub fn compact(&mut self) {
        todo!("Compaction is not yet supported.")
    }
}

impl<K, V> Default for ContainerDatabase<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new(ContainerConfig::default())
    }
}

impl<K, V> Database<K, V> for ContainerDatabase<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone + AsRef<[u8]>,
{
    fn insert(&mut self, key: K, value: V) -> io::Result<()> {
        let value_size = value.as_ref().len();
        if value_size > self.config.max_size {
            let msg = "Desired value is too large to fit in a single container even if it is empty";
            return Err(io::Error::new(io::ErrorKind::InvalidInput, msg));
        }

        let (id, container) = self.container(value_size);
        container.insert(key.clone(), value, value_size)?;
        self.index.insert(key, id);

        Ok(())
    }

    fn get(&self, key: &K) -> io::Result<V> {
        let container_id = *self
            .index
            .get(key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "The key is not found"))?;

        let container = match self.containers.get(container_id) {
            Some(container) => container,
            None => &self.current,
        };

        container.get(key)
    }

    fn contains(&self, key: &K) -> bool {
        self.index.contains_key(key)
    }
}

impl<K, V> IterableDatabase<K, V> for ContainerDatabase<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone + AsRef<[u8]>,
{
    fn iterator(&self) -> Box<dyn Iterator<Item = (&K, &V)> + '_> {
        Box::new(
            self.containers
                .iter()
                .flat_map(|c| c.values.iter())
                .chain(self.current.values.iter()),
        )
    }

    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item = (&K, &mut V)> + '_> {
        Box::new(
            self.containers
                .iter_mut()
                .flat_map(|c| c.values.iter_mut())
                .chain(self.current.values.iter_mut()),
        )
    }

    fn clear(&mut self) -> io::Result<()> {
        self.containers.clear();
        self.current = Container::new(self.config.max_size);
        self.index.clear();
        Ok(())
    }
}

/// Statistics about the container subsystem.
#[derive(Debug)]
pub struct ContainerStats {
    pub total_containers: usize,
    pub sealed_containers: usize,
    pub total_size: usize,
}

#[cfg(test)]
mod container_database_tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut db: ContainerDatabase<Vec<u8>, Vec<u8>> = ContainerDatabase::default();

        let key = b"key1".to_vec();
        let value = b"value1".to_vec();

        db.insert(key.clone(), value.clone()).unwrap();

        assert!(db.contains(&key));
        assert_eq!(db.get(&key).unwrap(), value);
    }

    #[test]
    fn test_with_string_keys() {
        let mut db: ContainerDatabase<String, Vec<u8>> = ContainerDatabase::default();

        db.insert("key1".to_string(), b"value1".to_vec()).unwrap();
        db.insert("key2".to_string(), b"value2".to_vec()).unwrap();

        assert_eq!(db.get(&"key1".to_string()).unwrap(), b"value1".to_vec());
        assert_eq!(db.get(&"key2".to_string()).unwrap(), b"value2".to_vec());
    }

    #[test]
    fn test_multiple_containers() {
        let config = ContainerConfig { max_size: 100 };
        let mut db: ContainerDatabase<String, Vec<u8>> = ContainerDatabase::new(config);

        for i in 0..10 {
            let key = format!("key{}", i);
            let value = vec![i as u8; 60];
            db.insert(key, value).unwrap();
        }

        for i in 0..10 {
            let key = format!("key{}", i);
            assert!(db.contains(&key));
        }

        let stats = db.stats();
        assert!(stats.sealed_containers > 0);
        println!("Stats: {:?}", stats);
    }

    #[test]
    fn test_overwriting() {
        let mut db: ContainerDatabase<Vec<u8>, Vec<u8>> = ContainerDatabase::default();

        let key = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        db.insert(key.clone(), value1).unwrap();
        db.insert(key.clone(), value2.clone()).unwrap();

        assert_eq!(db.get(&key).unwrap(), value2);
    }

    #[test]
    fn test_iterator() {
        let mut db: ContainerDatabase<String, Vec<u8>> = ContainerDatabase::default();

        db.insert("key1".to_string(), b"value1".to_vec()).unwrap();
        db.insert("key2".to_string(), b"value2".to_vec()).unwrap();
        db.insert("key3".to_string(), b"value3".to_vec()).unwrap();

        let count = db.iterator().count();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_stats() {
        let config = ContainerConfig { max_size: 50 };
        let mut db: ContainerDatabase<String, Vec<u8>> = ContainerDatabase::new(config);

        db.insert("key1".to_string(), vec![0; 30]).unwrap();
        db.insert("key2".to_string(), vec![0; 30]).unwrap();
        db.insert("key3".to_string(), vec![0; 30]).unwrap();

        let stats = db.stats();
        println!("{:?}", stats);

        assert!(stats.total_containers >= 2);
        assert!(stats.total_size >= 90);
    }

    #[test]
    fn test_gc() {
        let config = ContainerConfig { max_size: 100 };
        let mut db: ContainerDatabase<String, Vec<u8>> = ContainerDatabase::new(config);

        for i in 0..100 {
            let key = format!("key{}", i % 10);
            let value = vec![i as u8; 60];
            db.insert(key, value).unwrap();
        }

        let before_gc_stats = db.stats();
        println!("Before GC stats: {:?}", before_gc_stats);

        db.start_gc();

        let after_gc_stats = db.stats();
        println!("After GC stats: {:?}", after_gc_stats);

        assert!(before_gc_stats.sealed_containers > 50);
        assert!(after_gc_stats.sealed_containers < before_gc_stats.sealed_containers);
        assert!(after_gc_stats.sealed_containers < 11);
    }
}
