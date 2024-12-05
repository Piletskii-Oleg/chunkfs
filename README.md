**Chunkfs** is a file system that can be used to benchmark different chunking algorithms, utilizing different hashing
algorithms, hashing algorithms and storage types.

Chunkfs is currently under active development, breaking changes can always happen.

## Goals and ideas

The main idea behind **Chunkfs** is to provide a way to interchangeably use and compare different
data deduplication methods in a single controlled environment, notably Content Defined Chunking (CDC) algorithms and some optimization methods,
such as Frequency Based Chunking (FBC) and Similarity Based Chunking (SBC).

This is all implemented via a file system, in which user is able to write and read files onto a chosen means of storage,
be it a simple Hash Map, or something more complicated, like an LSM-tree. If it implements `Database` trait, 
it can be used as a storage for **Chunkfs**.

To create a CDC file system, only a hasher and a storage must be provided.
To create a file system to test and compare more complicated means of data deduplication, a target map and a scrubber must be provided.

When a file is created or opened with write access, a `chunker` must be provided by the user to split the 
data in chunks.

The end goal is to provide a *unified* and *easily customizable* instrument to compare CDC algorithms and data deduplication methods. 

## Features

- Supports different kinds of:
  - CDC algorithms
  - hashing algorithms
  - key-value storage types
  - chunk storage optimization methods (FBC, SBC, and others)
- Writing to and reading from files
  - file metadata is stored in RAM
  - no folders

## Chunking algorithms

To use different chunking algorithms with the file system, they must implement ``Chunker`` trait, which has the
following definition:

```rust
pub trait Chunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk>;
    fn rest(&self) -> &[u8];
    fn estimate_chunk_count(&self, data: &[u8]) -> usize;
}
```
Comments for each method are provided in [lib.rs](src/lib.rs).

## Chunking optimization methods (SBC, FBC)

To implement algorithms that optimize how chunks are stored and use them with the file system, the must
implement ``Scrub`` trait.
Details are provided in [scrub.rs](src/scrub.rs).

## Usage

Add the following dependency to your `Cargo.toml`:

```toml
[dependencies]
chunkfs = { git = "https://github.com/Piletskii-Oleg/chunkfs.git" }
```

To use provided chunkers and hashers, use the corresponding features:

```toml
[dependencies]
chunkfs = { git = "https://github.com/Piletskii-Oleg/chunkfs.git", features = ["chunkers", "hashers"] }
```

## Examples

Getting chunker measurements:
```rust
extern crate chunkfs;

use std::io;
use std::collections::HashMap;
use chunkfs::chunkers::LeapChunker;
use chunkfs::FileSystem;
use chunkfs::hashers::SimpleHasher;

fn main() -> io::Result<()> {
    let base = HashMap::default();
    let mut fs = FileSystem::new_with_key(base, SimpleHasher, 0);

    let mut file = fs.create_file("file", LeapChunker::default(), true)?;
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
```
Getting Scrubber measurements:
```rust
fn main() -> io::Result<()> {
    let mut fs = FileSystem::new_with_scrubber(
        HashMap::default(),
        Box::new(SBCMap::new()),
        Box::new(SBCScrubber::new()),
        Sha256Hasher::default()
    );

    let mut handle = fs.create_file("file", SuperChunker::new(), true)?;
    let data = vec![10; 1024 * 1024 * 10];
    fs.write_to_file(&mut handle, &data)?;
    fs.close_file(handle)?;

    let scrub_measurements = fs.scrub()?;
    println!("{scrub_measurements:?}");
    Ok(())
}
```