pub use system::FileSystem;

mod file_layer;
mod storage;
mod system;

pub const SEG_SIZE: usize = 1024 * 1024; // 1MB

// type or struct Hash(Vec<u8>)?
pub type Hash = Vec<u8>;
