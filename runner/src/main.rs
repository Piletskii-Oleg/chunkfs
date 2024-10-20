extern crate chunkfs;

use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::ops::AddAssign;
use std::time::{Duration, Instant};

use chunkfs::chunkers::LeapChunker;
use chunkfs::hashers::SimpleHasher;
use chunkfs::Chunker;
use chunkfs::FileSystem;
use chunkfs::Hasher;

#[derive(Default)]
struct Measurements {
    write_time: Duration,
    read_time: Duration,
    chunk_time: Duration,
    iteration_count: u32,
}

fn main() -> io::Result<()> {
    let base = HashMap::default();
    let mut fs = FileSystem::new_with_key(base, SimpleHasher, 0);

    let mut file = fs.create_file("file".to_string(), LeapChunker::default(), true)?;
    let data = vec![10; 1024 * 1024];
    fs.write_to_file(&mut file, &data)?;
    let measurements = fs.close_file(file)?;
    println!("{:?}", measurements);

    let mut file = fs.open_file("file", LeapChunker::default())?;
    let read = fs.read_from_file(&mut file)?;

    assert_eq!(read.len(), 1024 * 1024);
    assert_eq!(read, data);

    Ok(())
}

// fn main() -> io::Result<()> {
//     let mut fs = FileSystem::new_with_scrubber(
//         HashMap::default(),
//         Box::new(HashMap::default()),
//         Box::new(chunkfs::CopyScrubber),
//         SimpleHasher,
//     );
//
//     let mut handle = fs.create_file("file".to_string(), SuperChunker::new(), true)?;
//     let data = generate_data(10);
//     fs.write_to_file(&mut handle, &data)?;
//     fs.close_file(handle)?;
//
//     let res = fs.scrub().unwrap();
//     println!("{res:?}");
//
//     let mut handle = fs.open_file("file", SuperChunker::new())?;
//     let read = fs.read_file_complete(&mut handle)?;
//     assert_eq!(read.len(), data.len());
//     Ok(())
// }

const MB: usize = 1024 * 1024;

fn parametrized_write(
    chunker: impl Chunker + Debug,
    hasher: impl Hasher + Debug,
) -> io::Result<Measurements> {
    println!("Current chunker: {:?}", chunker);
    println!("Current hasher: {:?}", hasher);

    let base = HashMap::default();
    let mut fs = FileSystem::new_with_key(base, hasher, 0);

    let mut handle = fs.create_file("file".to_string(), chunker, true)?;

    const MB_COUNT: usize = 1024;

    let data = std::fs::read("linux.tar").unwrap();
    let watch = Instant::now();
    fs.write_to_file(&mut handle, &data)?;
    let write_time = watch.elapsed();

    let measurements = fs.close_file(handle)?;
    // println!(
    //     "Written {MB_COUNT} MB in {:.3} seconds => write speed is {:.3} MB/s",
    //     write_time.as_secs_f64(),
    //     MB_COUNT as f64 / write_time.as_secs_f64()
    // );
    let mb_size = data.len() as f64 / 1024.0 / 1024.0;
    let speed = mb_size / measurements.chunk_time().as_secs_f64();
    println!(
        "Chunked {} MB in {:.3} s => chunk speed is {:.3} MB/s",
        mb_size,
        measurements.chunk_time().as_secs_f64(),
        speed
    );

    let handle = fs.open_file("file", LeapChunker::default())?;
    let watch = Instant::now();
    let read = fs.read_file_complete(&handle)?;
    let read_time = watch.elapsed();
    // println!(
    //     "Read {MB_COUNT} MB in {:.3} seconds => chunk speed is {:.3} MB/s",
    //     read_time.as_secs_f64(),
    //     MB_COUNT as f64 / read_time.as_secs_f64()
    // );

    assert_eq!(read.len(), data.len());
    //assert_eq!(read, data);

    Ok(Measurements {
        write_time,
        read_time,
        chunk_time: measurements.chunk_time(),
        iteration_count: 1,
    })
}

fn generate_data(mb_size: usize) -> Vec<u8> {
    let bytes = mb_size * MB;
    (0..bytes).map(|_| rand::random::<u8>()).collect()
}

impl AddAssign for Measurements {
    fn add_assign(&mut self, rhs: Self) {
        self.read_time += rhs.read_time;
        self.write_time += rhs.write_time;
        self.chunk_time += rhs.chunk_time;
        self.iteration_count += 1;
    }
}

impl Debug for Measurements {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Read time: {:?}\nWrite time: {:?}\nChunk time: {:?}",
            self.read_time / self.iteration_count,
            self.write_time / self.iteration_count,
            self.chunk_time / self.iteration_count
        )
    }
}
