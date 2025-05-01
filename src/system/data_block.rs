use bincode::{decode_from_slice, Decode};
use std::io;

/// Information about the location of the data on the disk.
#[derive(Clone)]
pub struct DataInfo {
    /// Offset of the data on the block device.
    offset: u64,
    /// Serialized data length.
    data_length: u64,
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
    /// Returns [`io::ErrorKind::InvalidData`] if data_infos is empty.
    fn from_data_infos(alignment: Alignment, data_infos: Vec<DataInfo>) -> io::Result<Self> {
        if data_infos.is_empty() {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
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
            data_infos.push(DataInfo {
                offset,
                data_length: vec.len() as u64,
            });
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
