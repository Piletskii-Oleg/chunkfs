pub use storage::{base, chunker, hasher};
pub use system::{FileSystem, FileSystemBuilder};

mod file_layer;
mod storage;
mod system;

/// Block size, used by `read` and `write` methods in the `FileSystem`.
/// Blocks given to the user or by them must be of this size.
pub const SEG_SIZE: usize = 1024 * 1024; // 1MB

pub type VecHash = Vec<u8>;
