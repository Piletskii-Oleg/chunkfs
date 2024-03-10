pub use system::FileSystem;

mod file_layer;
mod storage;
mod system;

pub const SEG_SIZE: usize = 1024 * 1024; // 1MB

pub type Hash = Vec<u8>;
