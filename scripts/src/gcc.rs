use futures_util::TryStreamExt;
use reqwest::Client;
use std::fs::File;
use std::path::Path;

pub async fn load_to(path: &Path) -> std::io::Result<()> {
    let client = Client::new();

    let versions = ["gcc-4.0.4"];
    for version in versions {
        //download_version(client.clone(), version).await.map_err(std::io::Error::other)?;
    }

    let file = File::create("downloads/gcc.tar")?;
    let mut tar = tar::Builder::new(file);

    let path = Path::new("downloads/gcc");
    for dir in std::fs::read_dir(path)? {
        let dir = dir?;

        tar.append_dir_all(dir.file_name(), path.join(dir.file_name()))?;
    }
    tar.finish()?;

    Ok(())
}

async fn download_version(client: Client, version: &str) -> Result<(), reqwest::Error> {
    let tar_gz_path = format!("{WEB_PATH}/{version}/{version}.{TAR_GZ}");

    let response = client.get(tar_gz_path).send().await?;
    response.error_for_status_ref()?;

    let stream = response.bytes_stream().map_err(std::io::Error::other);
    let mut tokio_stream = tokio_util::io::StreamReader::new(stream);

    let write_path = format!("downloads/gcc/{version}.{TAR_GZ}");
    let file = tokio::fs::File::create(&write_path).await.unwrap();
    let mut writer = tokio::io::BufWriter::new(file);

    tokio::io::copy(&mut tokio_stream, &mut writer)
        .await
        .unwrap();
    Ok(())
}

const WEB_PATH: &str = "https://ftp.gnu.org/gnu/gcc/";

const TAR_GZ: &str = "tar.gz";
const TAR_BZ: &str = "tar.bz";

const VERSIONS: &[&str] = &[
    "gcc-4.0.4",
    "gcc-4.1.0",
    "gcc-4.1.1",
    "gcc-4.1.2",
    "gcc-4.2.0",
    "gcc-4.2.1",
    "gcc-4.2.2",
    "gcc-4.2.3",
    "gcc-4.2.4",
    "gcc-4.3.0",
    "gcc-4.3.1",
    "gcc-4.3.2",
    "gcc-4.3.3",
    "gcc-4.3.4",
    "gcc-4.3.5",
    "gcc-4.3.6",
    "gcc-4.4.0",
    "gcc-4.4.1",
    "gcc-4.4.2",
    "gcc-4.4.3",
    "gcc-4.4.4",
    "gcc-4.4.5",
    "gcc-4.4.6",
    "gcc-4.4.7",
    "gcc-4.5.0",
    "gcc-4.5.1",
    "gcc-4.5.2",
    "gcc-4.5.3",
    "gcc-4.5.4",
    "gcc-4.6.0",
    "gcc-4.6.1",
    "gcc-4.6.2",
    "gcc-4.6.3",
    "gcc-4.6.4",
    "gcc-4.7.0",
    "gcc-4.7.1",
    "gcc-4.7.2",
    "gcc-4.7.3",
    "gcc-4.7.4",
    "gcc-4.8.0",
    "gcc-4.8.1",
    "gcc-4.8.2",
    "gcc-4.8.3",
    "gcc-4.8.4",
    "gcc-4.8.5",
    "gcc-4.9.0",
    "gcc-4.9.1",
    "gcc-4.9.2",
    "gcc-4.9.3",
    "gcc-4.9.4",
    "gcc-5.1.0",
    "gcc-5.2.0",
    "gcc-5.3.0",
    "gcc-5.4.0",
    "gcc-5.5.0",
    "gcc-6.1.0",
    "gcc-6.2.0",
    "gcc-6.3.0",
    "gcc-6.4.0",
    "gcc-6.5.0",
    "gcc-7.1.0",
    "gcc-7.2.0",
    "gcc-7.3.0",
    "gcc-7.4.0",
    "gcc-7.5.0",
    "gcc-8.1.0",
    "gcc-8.2.0",
    "gcc-8.3.0",
    "gcc-8.4.0",
    "gcc-8.5.0",
    "gcc-9.1.0",
    "gcc-9.2.0",
    "gcc-9.3.0",
    "gcc-9.4.0",
    "gcc-9.5.0",
    "gcc-10.1.0",
    "gcc-10.2.0",
    "gcc-10.3.0",
    "gcc-10.4.0",
    "gcc-10.5.0",
    "gcc-11.1.0",
    "gcc-11.2.0",
    "gcc-11.3.0",
    "gcc-11.4.0",
    "gcc-11.5.0",
    "gcc-12.1.0",
    "gcc-12.2.0",
    "gcc-12.3.0",
    "gcc-12.4.0",
    "gcc-13.1.0",
    "gcc-13.2.0",
    "gcc-13.3.0",
    "gcc-14.1.0",
    "gcc-14.2.0",
];
