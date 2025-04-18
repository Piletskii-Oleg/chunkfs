//! Creates a dataset consisting of random data and puts it into datasets folder.

use std::io;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use chunkfs::MB;

fn main() -> io::Result<()> {
    let name = "random";
    let size = 100 * MB;
    let distribution = rand::distr::StandardUniform;

    let dataset_path = Path::new("datasets");
    let out_path = dataset_path.join(name);

    let dataset = chunkfs::bench::generator::random(name, size, distribution)?;
    let file = dataset.open()?;
    let mut reader = BufReader::new(file);

    std::fs::create_dir(dataset_path)?;

    let new_file = std::fs::File::create_new(out_path)?;
    let mut writer = BufWriter::new(new_file);
    std::io::copy(&mut reader, &mut writer)?;

    Ok(())
}
