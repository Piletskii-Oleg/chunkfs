[![Crates.io][crates-badge]][crates-url]
[![MIT licensed][mit-badge]][mit-url]

[crates-badge]: https://img.shields.io/crates/v/chunkfs.svg
[crates-url]: https://crates.io/crates/chunkfs
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: https://github.com/Piletskii-Oleg/chunkfs/blob/main/LICENSE

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
- Conducting benchmarks on different kinds of workloads and gathering reports 

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

To use it in your code, add the following dependency to your `Cargo.toml`:

```toml
[dependencies]
chunkfs = "0.1"
```

To use provided chunkers and hashers, use the corresponding features:

```toml
[dependencies]
chunkfs = { version = "0.1", features = ["chunkers", "hashers"] }
```

## FUSE native dependepcies

To use FuseFS structure, mountable with FUSE, you need to install the following native dependencies:

```bash
sudo apt update
sudo apt install -y fuse3 libfuse3-dev
```

To use the file system correctly, you need to allow other users to access the mounted file system.

```bash
echo "user_allow_other" | sudo tee -a /etc/fuse.conf
```

## Examples

Examples for chunkfs usage and benching are provided in [examples](examples) folder.

They include creating a file system and writing-reading a file, 
scrubbing a file with a CopyScrubber, and doing bench stuff.

# Benchmarking and CLI usage

Chunkfs provides methods to analyse chunking algorithms efficiency using different metrics.
To access them, use the corresponding feature:
```toml
[dependencies]
chunkfs = { version = "0.1", features = ["bench"] }
```

CLI can be used to easily conduct measurements:

```bash
Usage: chunkfscli [OPTIONS] --database <DATABASE> --hasher <HASHER> --chunker <CHUNKER> --min <MIN_CHUNK_SIZE> --avg <AVG_CHUNK_SIZE> --max <MAX_CHUNK_SIZE> --report-path <REPORT_PATH> <COMMAND>

Commands:
  measure      Conduct some amount of measurements
  dedup-ratio  Calculate dedup ratio
  run-config   Run a configuration from file
  help         Print this message or the help of the given subcommand(s)

Options:
      --config <CONFIG>            Path to a config (exclusive)
      --database <DATABASE>        Underlying database [possible values: hashmap]
      --hasher <HASHER>            Hasher used for chunks [possible values: sha256, simple]
      --chunker <CHUNKER>          Chunking algorithm [possible values: super, rabin, seq, ultra, leap, fixed-size, fast]
      --seq-mode <MODE>            Mode of operation for SeqCDC algorithm [possible values: increasing, decreasing]
      --min <MIN_CHUNK_SIZE>       Minimum chunk size (in KB)
      --avg <AVG_CHUNK_SIZE>       Average chunk size (in KB)
      --max <MAX_CHUNK_SIZE>       Maximum chunk size (in KB)
      --report-path <REPORT_PATH>  Path where report should be saved in .csv format
  -h, --help                       Print help
  -V, --version                    Print version

```

It supports two modes:
* Reading all configuration data from config file
  * File is in `toml` format, example is provided in the repository
  * Only `--config` parameter is used with `run-config` command, so that the command looks like
    
    `cargo run -p chunkfscli -- --config cli-config.toml run-config`
* Reading configuration from console input
  * All parameters must be provided except `--config`

It saves each run result to the `csv` file specified by `report-path` parameter, collecting the following metrics:
* Deduplication ratio
* Throughput of the chunker and hasher, of the file system; time taken
* Average chunk size

Besides the metrics, it provides information about the dataset and time when benchmark was finished. 

