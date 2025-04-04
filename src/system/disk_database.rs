use crate::{ChunkHash, Database, IterableDatabase};
use bincode::{decode_from_slice, encode_to_vec, Decode, Encode};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, Write};
use std::marker::PhantomData;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

/// Information about the location of the data on the disk.
#[derive(Clone)]
struct DataInfo {
    start_block: u64,
    /// Serialized data length
    data_length: u64,
}

/// Constant for requesting total size of the block device via ioctl
const BLKGETSIZE64: u64 = 0x80081272;
/// Constant for requesting size of the block in the block device via ioctl
const BLKSSZGET: u64 = 0x1268;

/// Database that stores data on a block device
pub struct DiskDatabase<K, V>
where
    K: ChunkHash,
    V: Clone + Encode + Decode<()>,
{
    device: File,
    /// If Database initialized on a regular file, contains the path to it.
    file_path: Option<String>,
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
    /// Init database on a regular file.
    ///
    /// Sets the size of the file specified in the path. Considers the block size to be 512.
    ///
    /// Intended for testing so that it does not require privileges for initialization on the block device.
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
            .open(file_path.as_ref())?;
        file.set_len(total_size)?;

        let database_map = HashMap::new();

        Ok(Self {
            device: file,
            file_path: Some(file_path.as_ref().to_str().unwrap().to_owned()),
            database_map,
            total_size,
            block_size: 512,
            used_blocks: 0,
            _data_type: PhantomData,
        })
    }

    /// Init database on a block device.
    ///
    /// Takes information about the block device via ioctl.
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
            file_path: None,
            database_map,
            total_size,
            block_size,
            used_blocks: 0,
            _data_type: PhantomData {},
        })
    }

    /// Looks for the complement of a number up to a multiple of the block size.
    ///
    /// For example, the result for 1000 with a block size of 512 would be 1024.
    fn padding_to_multiple_block_size(&self, length: u64) -> u64 {
        if length % self.block_size == 0 {
            0
        } else {
            let blocks_number = length.div_ceil(self.block_size);
            blocks_number * self.block_size - length
        }
    }

    /// Serializes and writes data to disk. Returns `DataInfo` with information about the allocated data.
    fn write<T: Encode>(&mut self, value: T) -> io::Result<DataInfo> {
        let mut encoded = encode_to_vec(&value, bincode::config::standard())
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

    /// Searches for data by `DataInfo`, returns deserialized data.
    fn read<T: Decode<()>>(&self, data_info: DataInfo) -> io::Result<T> {
        let mut data = vec![0u8; data_info.data_length as usize];
        let padding_size = self.padding_to_multiple_block_size(data.len() as u64);
        data.extend(vec![0; padding_size as usize]);

        self.device
            .read_at(&mut data, data_info.start_block * self.block_size)?;
        let (data, _) = decode_from_slice(&data, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(data)
    }
}

impl<K, V> Drop for DiskDatabase<K, V>
where
    K: ChunkHash,
    V: Clone + Encode + Decode<()>,
{
    fn drop(&mut self) {
        if let Some(file_path) = self.file_path.take() {
            std::fs::remove_file(&file_path).unwrap()
        }
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
