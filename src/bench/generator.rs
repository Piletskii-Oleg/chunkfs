use super::Dataset;
use rand::distributions::Distribution;
use std::fs::File;
use std::io;
use std::io::{BufWriter, Write};
use std::process::{Command, Stdio};

/// Trait for structures that generate datasets.
pub trait DatasetGenerator {
    /// Parameters necessary for generating the dataset.
    type Parameters;

    /// Generates the dataset using given parameters.
    fn generate(&self, parameters: Self::Parameters) -> io::Result<Dataset>;
}

pub struct Fio;

pub struct FioParameters {
    name: String,
    size: usize,
    dedup_percentage: u8,
}

impl DatasetGenerator for Fio {
    type Parameters = FioParameters;

    fn generate(&self, params: Self::Parameters) -> io::Result<Dataset> {
        fio(&params.name, params.size, params.dedup_percentage)
    }
}

/// Generates a file using fio
///
/// # Parameters
/// * size - size of the file, in **KB**
/// * dedup_percentage - percentage of identical buffers when writing, from 0 to 100
pub fn fio(name: &str, size: usize, dedup_percentage: u8) -> io::Result<Dataset> {
    if dedup_percentage > 100 {
        let msg = "dedup_percentage must be between 0 and 100";
        return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
    }

    let size_arg = format!("--size={size}K");
    let dedup_ratio_arg = format!("--dedupe_percentage={dedup_percentage}");

    let dir = std::env::temp_dir();
    let name_arg = format!("--name={name}");
    let dir_arg = format!("--directory={}", dir.display());

    let mut output = Command::new("fio")
        .arg("--bs=1M")
        .arg("--rw=write")
        .arg(size_arg)
        .arg(dedup_ratio_arg)
        .arg(name_arg)
        .arg(dir_arg)
        .stdout(Stdio::null())
        .spawn()?;
    output.wait()?;

    let file_name = format!("{name}.0.0");
    let path = dir.join(file_name);

    Dataset::new(path.to_str().unwrap(), name)
}

/// Generates a dataset using a given distribution.
///
/// # Parameters
/// * name -- name of the dataset
/// * size -- size of the dataset
/// * distribution -- some distribution that implements rand::Distribution
pub fn random(name: &str, size: usize, distribution: impl Distribution<u8>) -> io::Result<Dataset> {
    let dir = std::env::temp_dir();
    let path = dir.join(name);

    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);

    let mut rng = rand::thread_rng();
    let mut written = 0;
    const MB: usize = 1024 * 1024 * 1024;

    while written < size {
        let to_write = std::cmp::min(MB, size - written);
        let buffer = (&distribution)
            .sample_iter(&mut rng)
            .take(to_write)
            .collect::<Vec<_>>();
        writer.write_all(&buffer)?;
        written += to_write;
    }

    Dataset::new(path.to_str().unwrap(), name)
}
