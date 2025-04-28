use crate::{ChunkHash, Database, IterableDatabase};
use bincode::error::EncodeError;
use bincode::{decode_from_slice, encode_to_vec, Decode, Encode};
use libc::O_DIRECT;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::marker::PhantomData;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

/// Constant for requesting total size of the block device via ioctl
const BLKGETSIZE64: u64 = 0x80081272;
/// Constant for requesting size of the block in the block device via ioctl
const BLKSSZGET: u64 = 0x1268;

/// Information about the location of the data on the disk.
#[derive(Clone)]
struct DataInfo {
    /// Offset of the data on the block device.
    offset: u64,
    /// Serialized data length.
    data_length: u64,
}

/// Continuous data interval with information about sub-intervals. Offsets of the sub-intervals must be sequential and continuous.
/// Need for more convenient large aggregated read requests.
///
/// `Is not written to disk`, only used when processing read operations.
///
/// If device is opened with O_DIRECT flag, offset is a multiple of a block size, since it padded at start and end.
/// Also, with O_DIRECT it is possible that they may overlap during the processing of a multiple write/read operations.
struct SuperBlock {
    /// Actual data of the superblock.
    data: Vec<u8>,
    /// Superblock offset. First subinterval can
    offset: u64,
    /// Sub-intervals info. Must be sequential and continuous.
    data_infos: Vec<DataInfo>,
}

enum InitType {
    /// [`DiskDatabase`] is initialized on a block device.
    BlockDevice,
    /// [`DiskDatabase`] is initialized on regular file. Contains path to the file.
    RegularFile(String),
}

/// Database that stores data on a block device
pub struct DiskDatabase<K, V>
where
    K: ChunkHash,
    V: Clone + Encode + Decode<()>,
{
    /// Handle for an open block device (or regular file if initialized via `init_on_regular_file`.
    device: File,
    /// Type of the database initialization.
    init_type: InitType,
    /// A map that maps keys to the location of data on a disk.
    database_map: HashMap<K, DataInfo>,
    /// Size of the block device (or regular file).
    total_size: u64,
    /// Block size when initialized on a regular file, set to 512.
    block_size: u64,
    /// Number of occupied blocks.
    used_size: u64,
    /// Whether the device is opened with the O_DIRECT flag.
    o_direct: bool,
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
    /// Creates file with [`Self::create_db_file`]. Sets the size of the file specified in the path.
    /// You can specify `o_direct` flag for open file in a O_DIRECT mode. Considers the block size to be 512.
    /// File is removed on a drop() call.
    ///
    /// Intended for testing so that it does not require block device.
    pub fn init_on_regular_file<P>(file_path: P, db_size: u64, o_direct: bool) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = Self::create_db_file(&file_path, db_size, o_direct)?;

        let database_map = HashMap::new();
        let total_size = file.metadata()?.len();

        Ok(Self {
            device: file,
            init_type: InitType::RegularFile(file_path.as_ref().to_string_lossy().to_string()),
            database_map,
            total_size,
            block_size: 512,
            used_size: 0,
            o_direct,
            _data_type: PhantomData,
        })
    }

    /// Creates a regular file in the specified path with the specified size and o_direct flag, if specified.
    fn create_db_file<P>(file_path: P, db_size: u64, o_direct: bool) -> io::Result<File>
    where
        P: AsRef<Path>,
    {
        let file = if o_direct {
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .custom_flags(O_DIRECT)
                .open(file_path)?
        } else {
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(file_path)?
        };
        file.set_len(db_size)?;
        Ok(file)
    }

    /// Init database on a block device, with O_DIRECT flag, if specified.
    ///
    /// Takes information about the block device via ioctl.
    pub fn init<P>(blkdev_path: P, o_direct: bool) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let device = if o_direct {
            OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(O_DIRECT)
                .open(blkdev_path)?
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(blkdev_path)?
        };
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

        let database_map = HashMap::new();

        Ok(Self {
            device,
            init_type: InitType::BlockDevice,
            database_map,
            total_size,
            block_size,
            used_size: 0,
            o_direct,
            _data_type: PhantomData {},
        })
    }

    /// Looks for the complement of a number up to a multiple of the block size.
    ///
    /// For example, the result for 1000 with a block size of 512 would be 24.
    fn padding_to_multiple_block_size(&self, length: u64) -> u64 {
        if length % self.block_size == 0 {
            0
        } else {
            let blocks_number = length.div_ceil(self.block_size);
            blocks_number * self.block_size - length
        }
    }

    /// Constructs [`SuperBlock`] by vector of sequential and continuous [`DataInfo`].
    ///
    /// Padded at start and end, if block device is inited with O_DIRECT flag.
    fn superblock_from_data_infos(&self, data_infos: Vec<DataInfo>) -> io::Result<SuperBlock> {
        if data_infos.is_empty() {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        let first = data_infos.first().unwrap();
        let last = data_infos.last().unwrap();
        let (start_padding, end_padding) = if self.o_direct {
            (
                first.offset % self.block_size,
                self.padding_to_multiple_block_size(last.offset + last.data_length),
            )
        } else {
            (0, 0)
        };
        let total_len = last.offset + last.data_length - first.offset + start_padding + end_padding;

        Ok(SuperBlock {
            data: vec![0; total_len as usize],
            offset: first.offset - start_padding,
            data_infos,
        })
    }

    /// Split [`DataInfo`] vector into continuous intervals ([`SuperBlock`]'s).
    /// If some of the intervals follow each other by offsets, but don't follow each other in the vector, they are split into different intervals.
    fn split_to_superblocks(&self, data_infos: Vec<&DataInfo>) -> Vec<SuperBlock> {
        if data_infos.is_empty() {
            return vec![];
        }

        let mut sequential_data_infos = vec![vec![data_infos.first().unwrap()]];
        let mut last_end = data_infos[0].offset + data_infos[0].data_length;
        for data_info in data_infos[1..].iter() {
            if data_info.offset == last_end {
                sequential_data_infos.last_mut().unwrap().push(data_info)
            } else {
                sequential_data_infos.push(vec![data_info]);
            }
            last_end = data_info.offset + data_info.data_length
        }
        sequential_data_infos
            .into_iter()
            .map(|seq| {
                self.superblock_from_data_infos(seq.into_iter().map(|&di| di.clone()).collect())
            })
            .collect::<io::Result<Vec<SuperBlock>>>()
            .unwrap()
    }

    /// Read into superblocks from the block device based on their offsets.
    fn fill_superblocks(&self, superblocks: Vec<&mut SuperBlock>) -> io::Result<()> {
        superblocks
            .into_iter()
            .map(|superblock| self.device.read_at(&mut superblock.data, superblock.offset))
            .collect::<io::Result<Vec<_>>>()?;
        Ok(())
    }

    /// Decode each sub-interval of each superblock and concats them into vector of decoded values.
    fn decode_superblocks<T: Decode<()>>(superblocks: Vec<&SuperBlock>) -> io::Result<Vec<T>> {
        let mut decoded = vec![];
        superblocks.iter().try_for_each(|&superblock| {
            superblock.data_infos.iter().try_for_each(|data_info| {
                let start = (data_info.offset - superblock.offset) as usize;
                let end = start + data_info.data_length as usize;
                let (value, _) =
                    decode_from_slice(&superblock.data[start..end], bincode::config::standard())
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                decoded.push(value);
                Ok::<(), io::Error>(())
            })
        })?;
        Ok(decoded)
    }

    /// Read and decode multiple data from the disk.
    fn read_multi<T: Decode<()>>(&self, data_infos: Vec<&DataInfo>) -> io::Result<Vec<T>> {
        if data_infos.is_empty() {
            return Ok(Vec::new());
        }

        let mut superblocks = self.split_to_superblocks(data_infos);
        self.fill_superblocks(superblocks.iter_mut().collect())?;
        Self::decode_superblocks(superblocks.iter().collect())
    }

    /// Serializes and writes multiple data to disk. Returns `Vec<DataInfo>` with information about the allocated data.
    fn write_multi<T: Encode>(&mut self, values: &[&T]) -> io::Result<Vec<DataInfo>> {
        let encoded = values
            .iter()
            .map(|value| encode_to_vec(value, bincode::config::standard()))
            .collect::<Result<Vec<_>, EncodeError>>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let encoded_size: usize = encoded.iter().map(|vec| vec.len()).sum();
        if self.used_size + encoded_size as u64 >= self.total_size {
            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
        }
        let data_infos = encoded.iter().fold(vec![], |mut data_infos, vec| {
            data_infos.push(DataInfo {
                offset: self.used_size,
                data_length: vec.len() as u64,
            });
            self.used_size += vec.len() as u64;
            data_infos
        });

        let mut encoded = encoded.concat();
        if self.o_direct {
            let padding_size = self.padding_to_multiple_block_size(encoded.len() as u64);
            self.used_size += padding_size;
            encoded.extend(vec![0; padding_size as usize]); // padding for work with O_DIRECT flag
        }
        self.device
            .write_all_at(&encoded, self.used_size - encoded.len() as u64)?;

        Ok(data_infos)
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
        let mut unique_keys = vec![];
        let mut unique_values = vec![];
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
