use crate::{ChunkHash, Database, IterableDatabase, KB};
use std::io;
use std::path::{Path, PathBuf};

pub struct SledStorage {
    db: sled::Db,
    path: PathBuf,
    config: bincode::config::Configuration,
    key_buffer: Vec<u8>,
    value_buffer: Vec<u8>,
}

impl SledStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let db = sled::open(&path)?;
        let config = bincode::config::Configuration::default();
        Ok(Self {
            db,
            config,
            key_buffer: vec![0; 128 * KB],
            value_buffer: vec![0; 128 * KB],
            path: path.as_ref().to_path_buf(),
        })
    }
}

impl<Hash, V> Database<Hash, V> for SledStorage
where
    Hash: ChunkHash + bincode::Encode,
    V: Clone + bincode::Encode + bincode::Decode<()>,
{
    fn insert(&mut self, key: Hash, value: V) -> io::Result<()> {
        let key = bincode::encode_into_slice(key, &mut self.key_buffer, self.config)
            .map_err(io::Error::other)?;
        let value = bincode::encode_into_slice(value, &mut self.value_buffer, self.config)
            .map_err(io::Error::other)?;

        self.db
            .insert(&self.key_buffer[..key], &self.value_buffer[..value])?;

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

    fn insert_multi(&mut self, pairs: Vec<(Hash, V)>) -> io::Result<()> {
        let mut batch = sled::Batch::default();

        for (key, value) in pairs {
            let key = bincode::encode_into_slice(key, &mut self.key_buffer, self.config)
                .map_err(io::Error::other)?;
            let value = bincode::encode_into_slice(value, &mut self.value_buffer, self.config)
                .map_err(io::Error::other)?;

            batch.insert(&self.key_buffer[..key], &self.value_buffer[..value]);
        }

        self.db.apply_batch(batch).map_err(io::Error::other)
    }

    fn contains(&self, key: &Hash) -> bool {
        let key = bincode::encode_to_vec(key, self.config)
            .map_err(io::Error::other)
            .unwrap();

        self.db.contains_key(key).unwrap()
    }
}

impl<Hash, V> IterableDatabase<Hash, V> for SledStorage
where
    Hash: ChunkHash + bincode::Encode + bincode::Decode<()>,
    V: Clone + bincode::Encode + bincode::Decode<()>,
{
    fn iterator(&self) -> Box<dyn Iterator<Item = (Hash, V)> + '_> {
        Box::new(self.db.iter().map(|k| {
            let (k, v) = k.unwrap();
            let (key, _) = bincode::decode_from_slice(&k, self.config).unwrap();
            let (value, _) = bincode::decode_from_slice(&v, self.config).unwrap();
            (key, value)
        }))
    }

    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item = (&Hash, &mut V)> + '_> {
        panic!("Not supported")
    }

    fn clear(&mut self) -> io::Result<()> {
        std::fs::remove_dir_all(&self.path)?;

        self.db = sled::open(&self.path)?;

        Ok(())
    }
}
