use crate::{Chunker, FileSystem, Hasher};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{Read, Seek};

pub mod fio;

#[derive(Debug, Copy, Clone)]
struct Dataset<'a> {
    path: &'a str,
    name: &'a str,
    size: u64,
}

impl<'a> Dataset<'a> {
    fn new(path: &'a str, name: &'a str) -> io::Result<Self> {
        let size = File::open(path)?.metadata()?.len();
        Ok(Dataset { path, name, size })
    }

    fn open(&self) -> io::Result<File> {
        File::open(self.path)
    }
}

fn measure(chunker: Box<dyn Chunker>, hasher: impl Hasher, dataset: Dataset) -> io::Result<()> {
    let mut fs = FileSystem::new_with_key(HashMap::new(), hasher, 0);
    let mut file = fs.create_file("file", chunker)?;

    let mut data = dataset.open()?;
    fs.write_from_stream(&mut file, &mut data)?;

    let measurements = fs.close_file(file)?;
    println!("{:?}", measurements);

    let file = fs.open_file_readonly("file")?;
    let read = fs.read_file_complete(&file)?;

    assert_eq!(read.len(), dataset.size as usize);

    let mut buffer = Vec::with_capacity(dataset.size as usize);
    data.seek(io::SeekFrom::Start(0))?;
    data.read_to_end(&mut buffer)?;

    assert_eq!(read, buffer);

    println!("{}", fs.cdc_dedup_ratio());

    Ok(())
}
