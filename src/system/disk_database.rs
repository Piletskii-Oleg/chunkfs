use crate::system::data_block::{Alignment, DataBlock, DataInfo};
use crate::{ChunkHash, Database, IterableDatabase};
use bincode::error::EncodeError;
use bincode::{encode_to_vec, Decode, Encode};
use libc::O_DIRECT;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::marker::PhantomData;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

/// Constant for requesting the total size of the block device via ioctl
const BLKGETSIZE64: u64 = 0x80081272;
/// Constant for requesting size of the block in the block device via ioctl
const BLKSSZGET: u64 = 0x1268;

enum InitType {
    /// [`DiskDatabase`] is initialized on a block device.
    BlockDevice,
    /// [`DiskDatabase`] is initialized on a regular file. Contains a path to the file.
    RegularFile(PathBuf),
}

/// Database that stores data on a block device
pub struct DiskDatabase<K, V>
where
    K: ChunkHash,
    V: Clone + Encode + Decode<()>,
{
    /// Handle for an open block device (or regular file if initialized via `init_on_regular_file`).
    device: File,
    /// Type of the database initialization.
    init_type: InitType,
    /// A map that maps keys to the location of data on a disk.
    database_map: HashMap<K, DataInfo>,
    /// Size of the block device (or regular file).
    total_size: u64,
    /// Number of occupied blocks.
    used_size: u64,
    /// Whether the device is opened with the O_DIRECT flag.
    alignment: Alignment,
    /// Values data type. Database doesn't actually own them, so this field is necessary.
    _data_type: PhantomData<V>,
}

impl<K, V> DiskDatabase<K, V>
where
    K: ChunkHash,
    V: Clone + Encode + Decode<()>,
{
    /// Init database on a regular file.
    ///
    /// Creates a file with [`Self::create_db_file`]. Set the size of the file specified in the path.
    /// You can specify the ` o_direct ` flag for an open file in O_DIRECT mode. Consider the block size to be 512.
    /// The File is removed on a drop() call.
    ///
    /// Intended for testing so that it does not require a block device.
    pub fn init_on_regular_file<P>(file_path: P, db_size: u64, o_direct: bool) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = Self::create_db_file(&file_path, db_size, o_direct)?;

        let database_map = HashMap::new();
        let total_size = file.metadata()?.len();

        Ok(Self {
            device: file,
            init_type: InitType::RegularFile(file_path.as_ref().to_path_buf()),
            database_map,
            total_size,
            used_size: 0,
            alignment: Alignment::ByBlockSize(512),
            _data_type: PhantomData,
        })
    }

    /// Creates a regular file in the specified path with the specified size and o_direct flag, if specified.
    fn create_db_file<P>(file_path: P, db_size: u64, o_direct: bool) -> io::Result<File>
    where
        P: AsRef<Path>,
    {
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).read(true).write(true);
        if o_direct {
            options.custom_flags(O_DIRECT);
        };

        let file = options.open(file_path)?;
        file.set_len(db_size)?;
        Ok(file)
    }

    /// Init database on a block device, with an O_DIRECT flag, if specified.
    ///
    /// Takes information about the block device via ioctl.
    pub fn init<P>(blkdev_path: P, o_direct: bool) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        if o_direct {
            options.custom_flags(O_DIRECT);
        }
        let device = options.open(blkdev_path)?;
        let fd = device.as_raw_fd();

        let mut total_size: u64 = 0;
        let mut block_size: u64 = 0;
        if -1 == unsafe { libc::ioctl(fd, BLKGETSIZE64, &mut total_size) } {
            return Err(io::Error::last_os_error());
        };
        if -1 == unsafe { libc::ioctl(fd, BLKSSZGET, &mut block_size) } {
            return Err(io::Error::last_os_error());
        };
        if block_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "block size cannot be 0",
            ));
        }
        let alignment = if o_direct {
            Alignment::ByBlockSize(block_size)
        } else {
            Alignment::None
        };

        let database_map = HashMap::new();

        Ok(Self {
            device,
            init_type: InitType::BlockDevice,
            database_map,
            total_size,
            used_size: 0,
            alignment,
            _data_type: PhantomData {},
        })
    }

    /// Read into datablocks from the block device based on their offsets.
    fn fill_datablocks(&self, datablocks: Vec<&mut DataBlock>) -> io::Result<()> {
        datablocks
            .into_iter()
            .map(|datablock| {
                let offset = datablock.offset();
                self.device.read_at(datablock.data_mut(), offset)
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(())
    }

    /// Read and decode multiple data from the disk.
    fn read_multi<T: Decode<()>>(&self, data_infos: Vec<&DataInfo>) -> io::Result<Vec<T>> {
        if data_infos.is_empty() {
            return Ok(Vec::new());
        }

        let mut datablocks = DataBlock::split_to_datablocks(self.alignment.clone(), data_infos);
        self.fill_datablocks(datablocks.iter_mut().collect())?;
        DataBlock::decode_datablocks(datablocks.iter().collect())
    }

    /// Serializes and writes multiple data to the disk. Returns `Vec<DataInfo>` with information about the allocated data.
    fn write_multi<T: Encode>(&mut self, values: &[&T]) -> io::Result<Vec<DataInfo>> {
        if values.is_empty() {
            return Ok(Vec::new());
        }

        let encoded_values = values
            .iter()
            .map(|value| encode_to_vec(value, bincode::config::standard()))
            .collect::<Result<Vec<_>, EncodeError>>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let encoded_size: usize = encoded_values.iter().map(|vec| vec.len()).sum();
        if self.used_size + encoded_size as u64 >= self.total_size {
            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
        }

        let datablock =
            DataBlock::from_values(self.alignment.clone(), encoded_values, self.used_size)?;
        self.device.write_all_at(datablock.data(), self.used_size)?;
        self.used_size += datablock.data().len() as u64;

        Ok(datablock.data_infos())
    }
}

impl<K, V> Drop for DiskDatabase<K, V>
where
    K: ChunkHash,
    V: Clone + Encode + Decode<()>,
{
    fn drop(&mut self) {
        if let InitType::RegularFile(file_path) = &self.init_type {
            std::fs::remove_file(file_path).unwrap()
        }
    }
}

impl<K, V> Database<K, V> for DiskDatabase<K, V>
where
    K: ChunkHash,
    V: Clone + Encode + Decode<()>,
{
    fn insert(&mut self, key: K, value: V) -> io::Result<()> {
        self.insert_multi(vec![(key, value)])
    }

    fn get(&self, key: &K) -> io::Result<V> {
        self.get_multi(&[key.clone()]).map(|mut vec| vec.remove(0))
    }

    fn insert_multi(&mut self, pairs: Vec<(K, V)>) -> io::Result<()> {
        let mut unique_keys = Vec::with_capacity(pairs.len());
        let mut unique_values = Vec::with_capacity(pairs.len());
        pairs.iter().for_each(|(k, v)| {
            if !self.database_map.contains_key(k) {
                unique_keys.push(k.clone());
                unique_values.push(v)
            }
        });
        let data_infos = self.write_multi(&unique_values)?;
        let written_pairs = unique_keys.into_iter().zip(data_infos);
        self.database_map.extend(written_pairs);
        Ok(())
    }

    fn get_multi(&self, keys: &[K]) -> io::Result<Vec<V>> {
        let data_infos = keys
            .iter()
            .map(|k| {
                self.database_map
                    .get(k)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Key not found"))
            })
            .collect::<io::Result<Vec<_>>>()?;
        self.read_multi(data_infos)
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
        self.used_size = 0;
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

        let mut db = DiskDatabase::init_on_regular_file(file_path, file_size, true).unwrap();
        let v1: Vec<u8> = vec![1; 8 * KB + 30];
        let v2: Vec<u8> = vec![2; 8 * KB + 70];

        let mut hasher = Sha256Hasher::default();
        let k1 = hasher.hash(&v1);
        let k2 = hasher.hash(&v2);

        db.insert(k1, v1.clone()).unwrap();
        db.insert(k2, v2.clone()).unwrap();
        let actual1 = db.get(&k1).unwrap();
        let actual2 = db.get(&k2).unwrap();
        assert_eq!(actual1, v1);
        assert_eq!(actual2, v2);

        db.clear().unwrap();
        let empty = db.get(&k1);
        assert!(empty.is_err());
    }
}
