use bincode::{decode_from_slice, Decode};
use std::io;

/// Information about the location of the data on the disk.
#[derive(Clone, Debug, PartialEq)]
pub struct DataInfo {
    /// Offset of the data on the block device.
    offset: u64,
    /// Serialized data length.
    data_length: u64,
}

impl DataInfo {
    fn new(offset: u64, data_length: u64) -> Self {
        Self {
            offset,
            data_length,
        }
    }
}

/// Type of the data alignment.
#[derive(Clone)]
pub enum Alignment {
    /// No alignment.
    None,
    /// Alignments by the block size.
    ByBlockSize(u64),
}

/// Continuous data interval with information about internal values. Offsets of the internal values must be sequential and continuous.
/// Need for more convenient large aggregated read requests.
///
/// `Is not written to disk`, only used when processing read operations.
///
/// DataBlock is either [`without alignment`][Alignment::None] when the first value starts at the DataBlock's offset,
/// or [`with alignment`][Alignment::ByBlockSize] by the block size, with padding at the beginning and end.
#[derive(Debug)]
pub struct DataBlock {
    /// Actual data of the DataBlock.
    data: Vec<u8>,
    /// DataBlock offset. The first value may not be at this offset but after the padding if the DataBlock is aligned by some block size.
    offset: u64,
    /// Internal values info. Must be sequential and continuous, so that each successive offset is equal to the previous offset plus the previous size.
    data_infos: Vec<DataInfo>,
}

impl DataBlock {
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn data_infos(self) -> Vec<DataInfo> {
        self.data_infos
    }

    /// Constructs a [`DataBlock`] from a vector of sequential and continuous [`DataInfo`].
    ///
    /// Padded at the start and end by the block size if the corresponding alignment is passed.
    /// If data_infos is empty or not sequential and continuous, then [`io::ErrorKind::InvalidData`] is returned.
    fn from_data_infos(alignment: Alignment, data_infos: Vec<DataInfo>) -> io::Result<Self> {
        if data_infos.is_empty() {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        for (i, data_info) in data_infos.iter().enumerate().skip(1) {
            if data_info.offset != data_infos[i - 1].offset + data_infos[i - 1].data_length {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
        }

        let first = data_infos.first().unwrap();
        let last = data_infos.last().unwrap();
        let (start_padding, end_padding) = start_and_end_padding_of_datablock(
            first.offset,
            last.offset + last.data_length,
            alignment,
        );
        let total_len = last.offset + last.data_length - first.offset + start_padding + end_padding;

        Ok(Self {
            data: vec![0; total_len as usize],
            offset: first.offset - start_padding,
            data_infos,
        })
    }

    /// Constructs a [`DataBlock`] from a vector of values and given offset.
    ///
    /// Padded at the start and end by the block size if the corresponding alignment is passed.
    /// Internal [data infos][`DataInfo`] will start from the given offset.
    ///
    /// Returns [`io::ErrorKind::InvalidData`] if data_infos is empty.
    pub fn from_values(
        alignment: Alignment,
        mut values: Vec<Vec<u8>>,
        mut offset: u64,
    ) -> io::Result<Self> {
        if values.is_empty() {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        let given_offset = offset;
        let data_infos = values.iter().fold(vec![], |mut data_infos, vec| {
            data_infos.push(DataInfo::new(offset, vec.len() as u64));
            offset += vec.len() as u64;
            data_infos
        });

        let last_data_info = data_infos.last().unwrap();
        let (start_padding, end_padding) = start_and_end_padding_of_datablock(
            given_offset,
            last_data_info.offset + last_data_info.data_length,
            alignment.clone(),
        );
        values.push(vec![0; end_padding as usize]);
        values.insert(0, vec![0; start_padding as usize]);
        let data = values.concat();

        Ok(Self {
            data,
            offset: given_offset - start_padding,
            data_infos,
        })
    }

    /// Split [`DataInfo`] vector into continuous intervals ([`DataBlock`]'s).
    ///
    /// If some intervals follow each other by offsets but don't follow each other in the given vector, they are split into different intervals.
    pub fn split_to_datablocks(alignment: Alignment, data_infos: Vec<&DataInfo>) -> Vec<Self> {
        if data_infos.is_empty() {
            return vec![];
        }

        let mut sequential_data_infos = vec![vec![data_infos[0].clone()]];
        for &data_info in data_infos[1..].iter() {
            let last_seq = sequential_data_infos.last_mut().unwrap();
            let last = last_seq.last().unwrap();

            if data_info.offset == last.offset + last.data_length {
                last_seq.push(data_info.clone());
                continue;
            }
            sequential_data_infos.push(vec![data_info.clone()]);
        }

        sequential_data_infos
            .into_iter()
            .map(|seq| DataBlock::from_data_infos(alignment.clone(), seq))
            .collect::<io::Result<Vec<DataBlock>>>()
            .unwrap()
    }

    /// Decode each internal value of each datablock and concat them into a vector of decoded values.
    pub fn decode_datablocks<T: Decode<()>>(datablocks: Vec<&Self>) -> io::Result<Vec<T>> {
        let mut decoded = vec![];
        datablocks.iter().try_for_each(|&datablock| {
            datablock.data_infos.iter().try_for_each(|data_info| {
                let start = (data_info.offset - datablock.offset) as usize;
                let end = start + data_info.data_length as usize;
                let (value, _) =
                    decode_from_slice(&datablock.data[start..end], bincode::config::standard())
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                decoded.push(value);
                Ok::<(), io::Error>(())
            })
        })?;
        Ok(decoded)
    }
}

/// Looks for the complement of a number up to a multiple of the block size.
///
/// For example, the result for 1000 with a block size of 512 would be 24.
fn padding_to_multiple_block_size(length: u64, block_size: u64) -> u64 {
    if length % block_size == 0 {
        0
    } else {
        let blocks_number = length.div_ceil(block_size);
        blocks_number * block_size - length
    }
}

/// Searches for alignment padding at the beginning and end by the given datablock's start and end.
fn start_and_end_padding_of_datablock(start: u64, end: u64, alignment: Alignment) -> (u64, u64) {
    if let Alignment::ByBlockSize(block_size) = alignment {
        let start_padding = start % block_size;
        (
            start_padding,
            padding_to_multiple_block_size(end - start + start_padding, block_size),
        )
    } else {
        (0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KB;
    use crate::MB;
    use bincode::encode_to_vec;

    #[test]
    fn padding_to_multiple_block_ok() {
        assert_eq!(20, padding_to_multiple_block_size(490, 510));
        assert_eq!(1500, padding_to_multiple_block_size(1200, 2700));
        assert_eq!(0, padding_to_multiple_block_size(500, 500));
        assert_eq!(1, padding_to_multiple_block_size(500, 501));
    }

    #[test]
    fn start_end_padding_ok() {
        let (st, end) = start_and_end_padding_of_datablock(10, 200, Alignment::ByBlockSize(3));
        assert_eq!((st, end), (1, 1));

        let (st, end) = start_and_end_padding_of_datablock(200, 805, Alignment::ByBlockSize(400));
        assert_eq!((st, end), (200, 395));

        let (st, end) = start_and_end_padding_of_datablock(200, 805, Alignment::None);
        assert_eq!((st, end), (0, 0));

        let (st, end) = start_and_end_padding_of_datablock(1400, 1500, Alignment::ByBlockSize(512));
        assert_eq!((st, end), (1400 - 512 * 2, 512 * 3 - 1500));

        let (st, end) = start_and_end_padding_of_datablock(700, 2500, Alignment::ByBlockSize(2500));
        assert_eq!((st, end), (700, 0));
    }

    #[test]
    fn from_data_infos_empty_fails() {
        let res = DataBlock::from_data_infos(Alignment::None, vec![]);
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn from_data_infos_not_sequential_fails() {
        let data_infos = vec![DataInfo::new(100, 100), DataInfo::new(0, 100)];
        let res = DataBlock::from_data_infos(Alignment::None, data_infos);
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn from_data_infos_not_continuous_fails() {
        let data_infos = vec![DataInfo::new(0, 100), DataInfo::new(101, 100)];
        let res = DataBlock::from_data_infos(Alignment::None, data_infos);
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn from_single_data_info_without_alignment_ok() {
        let data_info = DataInfo::new(50, 150);
        let data_infos = vec![data_info.clone()];
        let datablock = DataBlock::from_data_infos(Alignment::None, data_infos).unwrap();
        assert_eq!(datablock.data.len(), 150);
        assert_eq!(datablock.offset, 50);
        assert_eq!(datablock.data_infos, vec![data_info.clone()]);
    }

    #[test]
    fn from_single_data_info_with_alignment_ok() {
        let data_info = DataInfo::new(50, 150);
        let data_infos = vec![data_info.clone()];
        let datablock =
            DataBlock::from_data_infos(Alignment::ByBlockSize(512), data_infos).unwrap();
        assert_eq!(datablock.data.len(), 512);
        assert_eq!(datablock.offset, 0);
        assert_eq!(datablock.data_infos, vec![data_info.clone()]);
    }

    #[test]
    fn from_single_data_info_block_intersection_with_alignment_ok() {
        let data_info = DataInfo::new(400, 150);
        let data_infos = vec![data_info.clone()];
        let datablock =
            DataBlock::from_data_infos(Alignment::ByBlockSize(512), data_infos).unwrap();
        assert_eq!(datablock.data.len(), 512 * 2);
        assert_eq!(datablock.offset, 0);
        assert_eq!(datablock.data_infos, vec![data_info.clone()]);
    }

    #[test]
    fn from_multi_data_info_without_alignment_ok() {
        let data_infos = vec![
            DataInfo::new(50, 150),
            DataInfo::new(200, 500),
            DataInfo::new(700, 1024),
        ];
        let datablock = DataBlock::from_data_infos(Alignment::None, data_infos.clone()).unwrap();
        assert_eq!(datablock.data.len(), 150 + 500 + 1024);
        assert_eq!(datablock.offset, 50);
        assert_eq!(datablock.data_infos, data_infos.clone());
    }

    #[test]
    fn from_multi_data_info_with_alignment_ok() {
        let data_infos = vec![
            DataInfo::new(550, 150),
            DataInfo::new(700, 500),
            DataInfo::new(1200, 1024),
        ];
        let datablock =
            DataBlock::from_data_infos(Alignment::ByBlockSize(512), data_infos.clone()).unwrap();
        assert_eq!(datablock.data.len(), 512 * 4);
        assert_eq!(datablock.offset, 512);
        assert_eq!(datablock.data_infos, data_infos.clone());
    }

    #[test]
    fn from_values_empty_fails() {
        let res = DataBlock::from_values(Alignment::None, vec![], 10);
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn from_single_value_without_alignment_ok() {
        let data = vec![1; 1500];
        let values = vec![data.clone()];
        let datablock = DataBlock::from_values(Alignment::None, values, 150).unwrap();
        assert_eq!(datablock.data.len(), 1500);
        assert_eq!(datablock.offset, 150);
        assert_eq!(datablock.data, data);
        assert_eq!(datablock.data_infos, vec![DataInfo::new(150, 1500)]);
    }

    #[test]
    fn from_single_value_with_alignment_ok() {
        let data = vec![1; 1500];
        let values = vec![data.clone()];
        let datablock = DataBlock::from_values(Alignment::ByBlockSize(512), values, 150).unwrap();
        assert_eq!(datablock.data.len(), 512 * 4);
        assert_eq!(datablock.offset, 0);
        assert_eq!(
            datablock.data,
            [vec![0; 150], data, vec![0; 512 * 4 - 1500 - 150]].concat()
        );
        assert_eq!(datablock.data_infos, vec![DataInfo::new(150, 1500)]);
    }

    #[test]
    fn from_multi_values_without_alignment_ok() {
        let values = vec![vec![1; 150], vec![2; 500], vec![3; 1024]];
        let datablock = DataBlock::from_values(Alignment::None, values.clone(), 50).unwrap();
        assert_eq!(datablock.data.len(), 150 + 500 + 1024);
        assert_eq!(datablock.offset, 50);
        assert_eq!(datablock.data, values.concat());
        assert_eq!(
            datablock.data_infos,
            vec![
                DataInfo::new(50, 150),
                DataInfo::new(50 + 150, 500),
                DataInfo::new(50 + 150 + 500, 1024)
            ]
        );
    }

    #[test]
    fn from_multi_values_with_alignment_ok() {
        let values = vec![vec![1; 150], vec![2; 500], vec![3; 1024]];
        let datablock =
            DataBlock::from_values(Alignment::ByBlockSize(512), values.clone(), 50).unwrap();
        assert_eq!(datablock.data.len(), 512 * 4);
        assert_eq!(datablock.offset, 0);
        let datablock_expected_data = [
            vec![0; 50],
            values.concat(),
            vec![0; 512 * 4 - 50 - 150 - 500 - 1024],
        ]
        .concat();
        assert_eq!(datablock.data, datablock_expected_data);
        assert_eq!(
            datablock.data_infos,
            vec![
                DataInfo::new(50, 150),
                DataInfo::new(50 + 150, 500),
                DataInfo::new(50 + 150 + 500, 1024)
            ]
        );
    }

    #[test]
    fn split_to_datablocks_empty_ok() {
        let datablocks = DataBlock::split_to_datablocks(Alignment::None, vec![]);
        assert!(datablocks.is_empty());
    }

    #[test]
    fn split_to_datablocks_several_intervals_with_alignment_ok() {
        let data_infos = [
            DataInfo::new(2000, 1500),
            DataInfo::new(3500, 500),
            DataInfo::new(4000, 300),
            DataInfo::new(5000, 1024),
            DataInfo::new(6024, 1024),
        ];
        let datablocks = DataBlock::split_to_datablocks(
            Alignment::ByBlockSize(512),
            data_infos.iter().collect(),
        );
        assert_eq!(datablocks.len(), 2);

        assert_eq!(datablocks[0].offset, 512 * 3);
        assert_eq!(datablocks[0].data.len(), 512 * 6);
        assert_eq!(
            datablocks[0].data_infos,
            vec![
                DataInfo::new(2000, 1500),
                DataInfo::new(3500, 500),
                DataInfo::new(4000, 300)
            ]
        );

        assert_eq!(datablocks[1].offset, 512 * 9);
        assert_eq!(datablocks[1].data.len(), 512 * 5);
        assert_eq!(
            datablocks[1].data_infos,
            vec![DataInfo::new(5000, 1024), DataInfo::new(6024, 1024)]
        );
    }

    #[test]
    fn split_to_datablocks_several_intervals_without_alignment_ok() {
        let data_infos = [
            DataInfo::new(10200, 1500),
            DataInfo::new(11700, 550),
            DataInfo::new(12250, 5000),
            DataInfo::new(200, 700),
            DataInfo::new(900, 4500),
            DataInfo::new(4000, 30),
        ];
        let datablocks =
            DataBlock::split_to_datablocks(Alignment::None, data_infos.iter().collect());
        assert_eq!(datablocks.len(), 3);

        assert_eq!(datablocks[0].offset, 10200);
        assert_eq!(datablocks[0].data.len(), 1500 + 550 + 5000);
        assert_eq!(
            datablocks[0].data_infos,
            vec![
                DataInfo::new(10200, 1500),
                DataInfo::new(11700, 550),
                DataInfo::new(12250, 5000),
            ]
        );

        assert_eq!(datablocks[1].offset, 200);
        assert_eq!(datablocks[1].data.len(), 700 + 4500);
        assert_eq!(
            datablocks[1].data_infos,
            vec![DataInfo::new(200, 700), DataInfo::new(900, 4500),]
        );

        assert_eq!(datablocks[2].offset, 4000);
        assert_eq!(datablocks[2].data.len(), 30);
        assert_eq!(datablocks[2].data_infos, vec![DataInfo::new(4000, 30),]);
    }

    #[test]
    fn decode_aligned_and_not_aligned_datablocks_ok() {
        let data_vectors1 = vec![vec![1; MB], vec![2; 5 * MB], vec![3; 1024]];
        let data_vectors2 = vec![vec![4; 500 * KB], vec![2; 2 * MB], vec![10; 10]];
        let encoded1 = encode_to_vec(data_vectors1.clone(), bincode::config::standard()).unwrap();
        let encoded2 = encode_to_vec(data_vectors2.clone(), bincode::config::standard()).unwrap();
        let datablock1 =
            DataBlock::from_values(Alignment::ByBlockSize(2300), vec![encoded1, encoded2], 5000)
                .unwrap();

        let data_vectors3 = vec![vec![9; 400 * KB], vec![150; MB + 1], vec![]];
        let encoded3 = encode_to_vec(data_vectors3.clone(), bincode::config::standard()).unwrap();
        let datablock2 = DataBlock::from_values(Alignment::None, vec![encoded3], 200).unwrap();

        let mut decoded: Vec<Vec<Vec<i32>>> =
            DataBlock::decode_datablocks(vec![&datablock2, &datablock1]).unwrap();
        decoded.sort();
        let mut all_data_vectors = vec![data_vectors1, data_vectors2, data_vectors3];
        all_data_vectors.sort();
        assert_eq!(all_data_vectors, decoded);
    }
}
