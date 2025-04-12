use std::io;
use std::path::Path;

use crate::{ChunkHash, Database};

pub struct SledStorage {
    db: sled::Db,
    config: bincode::config::Configuration,
}

impl SledStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let db = sled::open(path)?;
        let config = bincode::config::Configuration::default();
        Ok(Self { db, config })
    }
}

impl<Hash, V> Database<Hash, V> for SledStorage
where
    Hash: ChunkHash + bincode::Encode,
    V: Clone + bincode::Encode + bincode::Decode<()>,
{
    fn insert(&mut self, key: Hash, value: V) -> io::Result<()> {
        let key = bincode::encode_to_vec(key, self.config).map_err(io::Error::other)?;
        let value = bincode::encode_to_vec(value, self.config).map_err(io::Error::other)?;

        self.db.insert(key, value)?;

        Ok(())
    }

    fn get(&self, key: &Hash) -> io::Result<V> {
        let key = bincode::encode_to_vec(key, self.config).map_err(io::Error::other)?;

        match self.db.get(key)? {
            None => Err(io::Error::from(io::ErrorKind::NotFound)),
            Some(data) => {
                let (value, _) =
                    bincode::decode_from_slice(&data, self.config).map_err(io::Error::other)?;

                Ok(value)
            }
        }
    }

    fn contains(&self, key: &Hash) -> bool {
        let key = bincode::encode_to_vec(key, self.config)
            .map_err(io::Error::other)
            .unwrap();

        self.db.contains_key(key).unwrap()
    }
}
