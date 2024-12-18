use std::fs::File;
use std::io;
use std::process::{Command, Stdio};

/// Generates a file using fio
///
/// # Parameters
/// * size - size of the file, in **KB**
/// * dedup_ratio - percentage of identical buffers when writing, from 0 to 100
pub fn fio(size: usize, dedup_percentage: u8) -> io::Result<File> {
    if dedup_percentage > 100 {
        let msg = "dedup_percentage must be between 0 and 100";
        return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
    }

    let size_arg = format!("--size={size}K");
    let dedup_ratio_arg = format!("--dedupe_percentage={dedup_percentage}");

    let name = "file";
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

    let name = format!("{name}.0.0");
    let path = dir.join(name);
    File::open(path)
}

#[cfg(test)]
mod tests {
    use super::fio;

    // fio should work
    // file must be opened
    #[test]
    fn fio_test() {
        let _ = fio(10000, 10).unwrap();
    }
}
