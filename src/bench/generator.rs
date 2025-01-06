use super::Dataset;
use std::io;
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
/// * dedup_ratio - percentage of identical buffers when writing, from 0 to 100
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
    let path = dir.join(&file_name);

    Dataset::new(&path.into_os_string().into_string().unwrap(), name)
}
