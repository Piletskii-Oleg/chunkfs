use std::io;

pub type TargetMap<K> = Box<dyn Map<K, Vec<u8>>>;

pub trait Map<K, V> {
    fn insert(&mut self, key: K, value: V) -> io::Result<()>;

    fn get(&self, key: &K) -> io::Result<V>;

    fn remove(&mut self, key: &K);

    fn save(&mut self, keys: Vec<K>, values: Vec<V>) -> io::Result<()> {
        for (key, value) in keys.into_iter().zip(values) {
            self.insert(key, value)?;
        }
        Ok(())
    }

    fn retrieve(&self, keys: &[K]) -> io::Result<Vec<V>> {
        keys.iter().map(|key| self.get(key)).collect()
    }
}
