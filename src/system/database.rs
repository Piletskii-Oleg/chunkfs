use crate::ChunkHash;
use bincode::{config, decode_from_slice, encode_to_vec, Decode, Encode};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, Write};
use std::marker::PhantomData;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

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

#[derive(Clone)]
struct DataInfo {
    start_block: u64,
    data_length: u64,
}

const BLKGETSIZE64: u64 = 0x80081272;
const BLKSSZGET: u64 = 0x1268;

pub struct DiskDatabase<K, V>
where
    K: ChunkHash,
    V: Clone + Encode + Decode<()>,
{
    device: File,
    database_map: HashMap<K, DataInfo>,
    total_size: u64,
    block_size: u64,
    used_blocks: u64,
    _data_type: PhantomData<V>,
}

impl<K, V> DiskDatabase<K, V>
where
    K: ChunkHash,
    V: Clone + Encode + Decode<()>,
{
    pub fn init_on_regular_file<P>(file_path: P, total_size: u64) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(file_path)?;
        file.set_len(total_size)?;

        let database_map = HashMap::new();

        Ok(Self {
            device: file,
            database_map,
            total_size,
            block_size: 512,
            used_blocks: 0,
            _data_type: PhantomData,
        })
    }

    pub fn init<P>(blkdev_path: P) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let device = OpenOptions::new()
            .read(true)
            .write(true)
            .open(blkdev_path)?;
        let _fd = device.as_raw_fd();

        let mut total_size: u64 = 0;
        let mut block_size: u64 = 0;
        if -1 == unsafe { libc::ioctl(_fd, BLKGETSIZE64, &mut total_size) } {
            return Err(io::Error::last_os_error());
        };
        if -1 == unsafe { libc::ioctl(_fd, BLKSSZGET, &mut block_size) } {
            return Err(io::Error::last_os_error());
        };
        if block_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "block size cannot be 0",
            ));
        }

        let database_map = HashMap::new();

        Ok(Self {
            device,
            database_map,
            total_size,
            block_size,
            used_blocks: 0,
            _data_type: PhantomData {},
        })
    }

    fn padding_to_multiple_block_size(&self, length: u64) -> u64 {
        if length % self.block_size == 0 {
            0
        } else {
            let blocks_number = length.div_ceil(self.block_size);
            blocks_number * self.block_size - length
        }
    }

    fn write<T: Encode>(&mut self, value: T) -> io::Result<DataInfo> {
        let mut encoded = encode_to_vec(&value, config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let data_length = encoded.len() as u64;

        if self.used_blocks * self.block_size + data_length >= self.total_size {
            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
        }

        let blocks_number = data_length.div_ceil(self.block_size);
        let padding_size = self.padding_to_multiple_block_size(data_length);
        encoded.extend(vec![0; padding_size as usize]); // padding for work with O_DIRECT flag

        self.device
            .seek(io::SeekFrom::Start(self.used_blocks * self.block_size))?;
        self.device.write_all(&encoded)?;

        let data_info = DataInfo {
            start_block: self.used_blocks,
            data_length,
        };
        self.used_blocks += blocks_number;
        Ok(data_info)
    }

    fn read<T: Decode<()>>(&self, data_info: DataInfo) -> io::Result<T> {
        let mut data = vec![0u8; data_info.data_length as usize];
        let padding_size = self.padding_to_multiple_block_size(data.len() as u64);
        data.extend(vec![0; padding_size as usize]);

        self.device
            .read_at(&mut data, data_info.start_block * self.block_size)?;
        let (data, _) = decode_from_slice(&data, config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(data)
    }
}

impl<K, V> Database<K, V> for DiskDatabase<K, V>
where
    K: ChunkHash,
    V: Clone + Encode + Decode<()>,
{
    fn try_insert(&mut self, key: K, value: V) -> io::Result<()> {
        if self.database_map.contains_key(&key) {
            return Ok(());
        }
        Self::insert(self, key, value)
    }

    fn insert(&mut self, key: K, value: V) -> io::Result<()> {
        let data_info = self.write(value)?;
        self.database_map.insert(key, data_info);
        Ok(())
    }

    fn get(&self, key: &K) -> io::Result<V> {
        let data_info = self.database_map.get(key).ok_or(io::ErrorKind::NotFound)?;
        self.read(data_info.clone())
    }

    fn contains(&self, key: &K) -> bool {
        self.database_map.contains_key(key)
    }
}

impl<K, V> IterableDatabase<K, V> for DiskDatabase<K, V>
where
    K: ChunkHash,
    V: Clone + Encode + Decode<()>,
{
    /// Returns a simple immutable iterator over values.
    fn iterator(&self) -> Box<dyn Iterator<Item = (&K, &V)> + '_> {
        unimplemented!()
    }

    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item = (&K, &mut V)> + '_> {
        unimplemented!()
    }

    fn keys<'a>(&'a self) -> Box<dyn Iterator<Item = &'a K> + 'a>
    where
        V: 'a,
    {
        Box::new(self.database_map.keys())
    }

    fn values(&self) -> Box<dyn Iterator<Item = V> + '_> {
        Box::new(self.database_map.keys().map(|k| self.get(k).unwrap()))
    }

    fn values_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut V> + 'a>
    where
        K: 'a,
    {
        unimplemented!()
    }

    fn clear(&mut self) -> io::Result<()> {
        self.database_map.clear();
        self.used_blocks = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KB;
    use chunkfs::hashers::Sha256Hasher;
    use chunkfs::Hasher;

    #[test]
    fn diskdb_write_read_clear() {
        let file_path = "pseudo_dev";
        let file_size = 1024 * 1024 * 12;

        let mut db = DiskDatabase::init_on_regular_file(file_path, file_size).unwrap();
        let v1: Vec<u8> = vec![1; 8 * KB + 30];
        let v2: Vec<u8> = vec![2; 8 * KB + 70];

        let mut hasher = Sha256Hasher::default();
        let k1 = hasher.hash(&v1);
        let k2 = hasher.hash(&v2);

        db.try_insert(k1, v1.clone()).unwrap();
        db.try_insert(k2, v2.clone()).unwrap();
        let actual1 = db.get(&k1).unwrap();
        let actual2 = db.get(&k2).unwrap();
        assert_eq!(actual1, v1);
        assert_eq!(actual2, v2);

        db.clear().unwrap();
        let empty = db.get(&k1);
        assert_eq!(empty.is_err(), true);
    }
}
