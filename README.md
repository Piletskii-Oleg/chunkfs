# ChunkFS

**Chunkfs** is a file system that can be used to benchmark different chunking algorithms, utilizing different hashing
algorithms, hashing algorithms and storage types.

Chunkfs is currently under active development, breaking changes can always happen.

## Goals and ideas

The main idea behind **Chunkfs** is to provide a way to interchangeably use and compare different
data deduplication methods in a single controlled environment, notably Content Defined Chunking (CDC) algorithms and
some optimization methods,
such as Frequency Based Chunking (FBC) and Similarity Based Chunking (SBC).

The end goal is to provide a *unified* and *easily customizable* instrument to compare CDC algorithms and data
deduplication methods.

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
    fn estimate_chunk_count(&self, data: &[u8]) -> usize;
}
```

Comments for each method are provided in [lib.rs](src/lib.rs).

## Chunking optimization methods (SBC, FBC)

To implement algorithms that optimize how chunks are stored and use them with the file system, 
the user must implement ``Scrub`` trait. 
Details are provided in [scrub.rs](src/system/scrub.rs).

## Usage

The main idea is implemented via an in-memory file system, in which user is able to write and read files onto a chosen means of storage,
be it a simple Hash Map, or something more complicated, like an LSM-tree. If it implements `Database` trait,
it can be used as a storage for **Chunkfs**.

To create a CDC file system, only a hasher and a storage must be provided.
To create a file system to test and compare more complicated means of data deduplication, a target map and a scrubber
must be provided.

When a file is created or opened with write access, a `chunker` must be provided by the user to split the
data in chunks.

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

Chunkfs provides methods to analyse chunking algorithms efficiency using different metrics. 
To access them, use the corresponding feature:
```toml
[dependencies]
chunkfs = { git = "https://github.com/Piletskii-Oleg/chunkfs.git", features = ["bench"] }
```

## Examples

Examples for chunkfs usage and benching are provided in [examples](examples) folder.

They include creating a file system and writing-reading a file, 
scrubbing a file with a CopyScrubber, and doing bench stuff.