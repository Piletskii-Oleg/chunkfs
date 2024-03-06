use crate::storage::Storage;
use crate::{Base, Chunker, Hasher};

pub struct FileSystem<C, H, B>
where
    C: Chunker,
    H: Hasher,
    B: Base,
{
    storage: Storage<C, H, B>,
}

impl<C, H, B> FileSystem<C, H, B>
where
    C: Chunker,
    H: Hasher,
    B: Base,
{
}
