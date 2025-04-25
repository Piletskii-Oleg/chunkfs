use crate::system::file_layer::FileHandle;
use crate::{
    create_cdc_filesystem, ChunkHash, ChunkerRef, DataContainer, Database, FileSystem, Hasher, MB,
};
use fuser::consts::FUSE_BIG_WRITES;
use fuser::FileType::RegularFile;
use fuser::TimeOrNow::Now;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use libc::c_int;
use std::cmp::min;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::time::{Duration, SystemTime};

type Inode = u64;
type Fh = u64;

/// File is opened for execution.
const FMODE_EXEC: i32 = 0x20;
const FILESYSTEM_CACHE_MAX_SIZE: usize = 25 * MB;
const FILE_CACHE_MAX_SIZE: usize = 5 * MB;

#[derive(Clone)]
struct FuseFile {
    cache: Vec<u8>,
    attr: FileAttr,
    name: String,
    generation: u64,
    handles: u64,
}

struct FuseFileHandle {
    underlying_file_handle: FileHandle,
    read: bool,
    write: bool,
    inode: u64,
}

/// Wrap around [`FileSystem`] for implementing [`Filesystem`] trait.
///
/// After creation, it should be passed to [`mount2`][fuser::mount2] or [`spawn_mount2`][fuser::spawn_mount2].
pub struct FuseFS<B, Hash>
where
    B: Database<Hash, DataContainer<()>>,
    Hash: ChunkHash,
{
    underlying_fs: FileSystem<B, Hash, (), HashMap<(), Vec<u8>>>,
    files: HashMap<Inode, FuseFile>,
    inodes: HashMap<String, Inode>,
    /// Number for the next created file handle.
    next_fh: u64,
    file_handles: HashMap<Fh, FuseFileHandle>,
    chunker: ChunkerRef,
    total_cache: usize,
}

impl<B, Hash> FuseFS<B, Hash>
where
    B: Database<Hash, DataContainer<()>>,
    Hash: ChunkHash,
{
    /// Creates a file system with the given [`hasher`][Hasher], [`database`][Database] and [`chunker`][ChunkerRef].
    ///
    /// After creation, it should be passed to [`mount2`][fuser::mount2] or [`spawn_mount2`][fuser::spawn_mount2].
    pub fn new<H, C>(base: B, hasher: H, chunker: C) -> Self
    where
        H: Into<Box<dyn Hasher<Hash = Hash> + 'static>>,
        C: Into<ChunkerRef>,
    {
        let underlying_fs = create_cdc_filesystem(base, hasher);

        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let now = SystemTime::now();
        let root_attr = FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid,
            gid,
            rdev: 0,
            flags: 0,
            blksize: 512,
        };
        let root_dir = FuseFile {
            cache: Vec::new(),
            attr: root_attr,
            name: ".".to_string(),
            generation: 0,
            handles: 0,
        };
        let mut parent_dir = root_dir.clone();
        parent_dir.name = "..".to_string();
        parent_dir.attr.ino = 0;
        let files = HashMap::from([(0, parent_dir), (1, root_dir)]);

        let inodes = HashMap::from([("..".to_string(), 0), (".".to_string(), 1)]);
        Self {
            underlying_fs,
            files,
            inodes,
            file_handles: HashMap::default(),
            next_fh: 0,
            chunker: chunker.into(),
            total_cache: 0,
        }
    }

    fn get_new_inode(&self) -> Inode {
        self.inodes.len() as Inode
    }

    fn get_new_fh(&mut self) -> Fh {
        let next_fh = self.next_fh;
        self.next_fh += 1;
        next_fh
    }

    fn drop_and_shrink_cache(&mut self, file: Inode, handle: Fh) -> io::Result<()> {
        self.drop_cache(file, handle)?;

        let file = self.files.get_mut(&file).ok_or(io::ErrorKind::NotFound)?;
        self.total_cache -= file.cache.len();
        file.cache = vec![];
        Ok(())
    }

    fn drop_cache(&mut self, file: Inode, handle: Fh) -> io::Result<()> {
        let file = self.files.get_mut(&file).ok_or(io::ErrorKind::NotFound)?;
        let handle = self
            .file_handles
            .get_mut(&handle)
            .ok_or(io::ErrorKind::NotFound)?;
        self.underlying_fs
            .write_to_file(&mut handle.underlying_file_handle, &file.cache)?;

        file.cache.clear();
        Ok(())
    }

    fn drop_and_shrink_caches(&mut self) -> io::Result<()> {
        for handle in self.file_handles.values_mut() {
            let file = self
                .files
                .get_mut(&handle.inode)
                .ok_or(io::ErrorKind::NotFound)?;
            self.underlying_fs
                .write_to_file(&mut handle.underlying_file_handle, &file.cache)?;

            self.total_cache -= file.cache.len();
            file.cache = vec![];
        }
        Ok(())
    }
}

/// Checks the request rights for the file with the specified access mask (flags).
fn check_access(file_attr: &FileAttr, req: &Request, access_mask: i32) -> bool {
    let file_uid = file_attr.uid;
    let file_gid = file_attr.gid;
    let file_mode = file_attr.perm;
    let uid = req.uid();
    let gid = req.gid();

    let mut access_mask = access_mask;
    // F_OK tests for existence of file
    if access_mask == libc::F_OK {
        return true;
    }
    let file_mode = i32::from(file_mode);

    // root is allowed to read & write anything
    if uid == 0 {
        // root only allowed to exec if one of the Exec bits is set
        access_mask &= libc::X_OK;
        access_mask -= access_mask & (file_mode >> 6);
        access_mask -= access_mask & (file_mode >> 3);
        access_mask -= access_mask & file_mode;
        return access_mask == 0;
    }

    if uid == file_uid {
        access_mask -= access_mask & (file_mode >> 6);
    } else if gid == file_gid {
        access_mask -= access_mask & (file_mode >> 3);
    } else {
        access_mask -= access_mask & file_mode;
    }

    access_mask == 0
}

impl<B, Hash> Filesystem for FuseFS<B, Hash>
where
    B: Database<Hash, DataContainer<()>>,
    Hash: ChunkHash,
{
    fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), c_int> {
        let _ = config.add_capabilities(FUSE_BIG_WRITES);
        if let Err(nearest) = config.set_max_write(128 * MB as u32) {
            let _ = config.set_max_write(nearest);
        };
        Ok(())
    }
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_str().unwrap().to_owned();
        if parent != 1 {
            reply.error(libc::EINVAL);
            return;
        }

        let Some(inode) = self.inodes.get::<String>(&name) else {
            reply.error(libc::ENOENT);
            return;
        };
        let file = self.files.get(inode).unwrap();
        reply.entry(&Duration::new(0, 0), &file.attr, file.generation)
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.files.get(&ino) {
            Some(file) => reply.attr(&Duration::new(0, 0), &file.attr),
            None => reply.error(libc::ENOENT),
        }
    }

    fn setattr(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let Some(file) = self.files.get_mut(&ino) else {
            reply.error(libc::ENOENT);
            return;
        };

        let now = SystemTime::now();
        let attr = &mut file.attr;
        if let Some(mode) = mode {
            if req.uid() != 0 && req.uid() != attr.uid {
                reply.error(libc::EPERM);
                return;
            } else {
                attr.perm = mode as u16;
            }
            attr.ctime = now;
            reply.attr(&Duration::new(0, 0), &file.attr);
            return;
        }

        let set_time_with_check = |time: TimeOrNow| {
            if attr.uid != req.uid() && req.uid() != 0 && time != Now {
                return None;
            }

            match time {
                TimeOrNow::SpecificTime(time) => Some(time),
                Now => Some(now),
            }
        };

        if let Some(atime) = atime {
            let Some(atime) = set_time_with_check(atime) else {
                reply.error(libc::EPERM);
                return;
            };
            attr.atime = atime;
            attr.ctime = now;
        }

        if let Some(mtime) = mtime {
            let Some(mtime) = set_time_with_check(mtime) else {
                reply.error(libc::EPERM);
                return;
            };
            attr.mtime = mtime;
            attr.ctime = now;
        }

        if let Some(ctime) = ctime {
            let Some(ctime) = set_time_with_check(TimeOrNow::SpecificTime(ctime)) else {
                reply.error(libc::EPERM);
                return;
            };
            attr.ctime = ctime;
        }

        reply.attr(&Duration::new(0, 0), attr);
    }

    fn open(&mut self, req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let Some(file) = self.files.get_mut(&ino) else {
            reply.error(libc::ENOENT);
            return;
        };

        let (access_mask, read, write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => {
                if flags & libc::O_TRUNC != 0 {
                    reply.error(libc::EACCES);
                    return;
                }
                if flags & FMODE_EXEC != 0 {
                    // Open is from internal exec syscall
                    (libc::X_OK, true, false)
                } else {
                    (libc::R_OK, true, false)
                }
            }
            libc::O_WRONLY => (libc::W_OK, false, true),
            libc::O_RDWR => (libc::R_OK | libc::W_OK, true, true),
            // Exactly one access mode flag must be specified
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        if !check_access(&file.attr, req, access_mask) {
            reply.error(libc::EACCES);
            return;
        }

        let Ok(underlying_file_handle) = self
            .underlying_fs
            .open_file(&file.name, self.chunker.clone())
        else {
            reply.error(libc::EBADF);
            return;
        };

        let file_handle = FuseFileHandle {
            underlying_file_handle,
            inode: ino,
            read,
            write,
        };
        file.handles += 1;
        let fh = self.get_new_fh();
        self.file_handles.insert(fh, file_handle);

        reply.opened(fh, flags as u32)
    }

    fn read(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let Some(file_handle) = self.file_handles.get_mut(&fh) else {
            reply.error(libc::EBADF);
            return;
        };
        if file_handle.inode != ino {
            reply.error(libc::ESTALE);
            return;
        }
        let Some(file) = self.files.get_mut(&ino) else {
            reply.error(libc::ENOENT);
            return;
        };
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let offset = offset as usize;
        let size = size as usize;

        if !check_access(&file.attr, req, libc::R_OK) || !file_handle.read {
            reply.error(libc::EACCES);
            return;
        }
        let underlying_fh = &mut file_handle.underlying_file_handle;
        underlying_fh.set_offset(offset);

        let now = SystemTime::now();
        file.attr.atime = now;
        file.attr.ctime = now;

        let Ok(mut data) = self.underlying_fs.read(underlying_fh, size) else {
            reply.error(libc::EIO);
            return;
        };

        let read_size = data.len();
        let new_offset = offset + read_size;
        underlying_fh.set_offset(new_offset);

        if read_size > size || file.cache.len() > file.attr.size as usize {
            reply.error(libc::EIO);
            return;
        }
        if read_size == size || new_offset >= file.attr.size as usize {
            reply.data(&data);
            return;
        }

        let missing_size = size - read_size;
        let disk_data_size = file.attr.size as usize - file.cache.len();
        if new_offset < disk_data_size {
            reply.error(libc::EIO);
            return;
        }

        let cache_start_offset = new_offset - disk_data_size;
        let cache_end_offset = min(file.cache.len(), cache_start_offset + missing_size);
        data.extend_from_slice(&file.cache[cache_start_offset..cache_end_offset]);
        let new_offset = offset + data.len();

        underlying_fh.set_offset(new_offset);
        reply.data(&data);
    }

    fn write(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let Some(file_handle) = self.file_handles.get_mut(&fh) else {
            reply.error(libc::EBADF);
            return;
        };
        if file_handle.inode != ino {
            reply.error(libc::ESTALE);
            return;
        }
        let Some(file) = self.files.get_mut(&ino) else {
            reply.error(libc::ENOENT);
            return;
        };
        if offset < 0 || offset as u64 != file.attr.size {
            reply.error(libc::EINVAL);
            return;
        }

        if !check_access(&file.attr, req, libc::W_OK) || !file_handle.write {
            reply.error(libc::EACCES);
            return;
        }

        file.cache.extend_from_slice(data);
        if file.cache.len() > FILE_CACHE_MAX_SIZE && self.drop_cache(ino, fh).is_err() {
            reply.error(libc::EIO);
            return;
        }
        if self.total_cache > FILESYSTEM_CACHE_MAX_SIZE && self.drop_and_shrink_caches().is_err() {
            reply.error(libc::EIO);
            return;
        }

        let now = SystemTime::now();
        let file = self.files.get_mut(&ino).unwrap();
        file.attr.ctime = now;
        file.attr.mtime = now;
        file.attr.size += data.len() as u64;
        file.generation += 1;

        reply.written(data.len() as u32);
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let Some(file) = self.files.get(&ino) else {
            reply.error(libc::EINVAL);
            return;
        };
        if file.handles == 0 {
            reply.error(libc::EINVAL);
            return;
        }
        if !self.file_handles.contains_key(&fh) {
            reply.error(libc::EINVAL);
            return;
        }

        if self.drop_and_shrink_cache(ino, fh).is_err() {
            reply.error(libc::EIO);
            return;
        }

        let Some(file_handle) = self.file_handles.remove(&fh) else {
            reply.error(libc::EINVAL);
            return;
        };
        file_handle.underlying_file_handle.close();
        let file = self.files.get_mut(&ino).unwrap();
        file.handles -= 1;
        reply.ok()
    }

    fn readdir(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != 1 {
            reply.error(libc::EINVAL);
            return;
        }
        let dir = self.files.get(&ino).unwrap();
        if !check_access(&dir.attr, req, libc::R_OK) {
            reply.error(libc::EACCES);
            return;
        }

        let entries = self
            .files
            .iter()
            .map(|(inode, file)| (inode, file.attr.kind, &file.name));
        for (i, entry) in entries.enumerate().skip(offset as usize) {
            let (inode, kind, name) = entry;
            if reply.add(*inode, offset + i as i64 + 1, kind, name) {
                break;
            }
        }

        reply.ok()
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let name = name.to_str().unwrap().to_owned();
        if parent != 1 {
            reply.error(libc::EINVAL);
            return;
        }
        let Ok(underlying_file_handle) = self
            .underlying_fs
            .create_file(name.clone(), self.chunker.clone())
        else {
            reply.error(libc::EEXIST);
            return;
        };

        let ino = self.get_new_inode();
        let now = SystemTime::now();
        let attr = FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: RegularFile,
            perm: (mode & !umask) as u16,
            nlink: 1,
            uid: req.uid(),
            gid: req.gid(),
            rdev: 000,
            blksize: 000,
            flags: flags as u32,
        };

        let (read, write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => (true, false),
            libc::O_WRONLY => (false, true),
            libc::O_RDWR => (true, true),
            // Exactly one access mode flag must be specified
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let file_handle = FuseFileHandle {
            underlying_file_handle,
            inode: ino,
            read,
            write,
        };
        let file = FuseFile {
            cache: Vec::new(),
            attr,
            name: name.clone(),
            generation: 0,
            handles: 1,
        };

        let fh = self.get_new_fh();
        reply.created(&Duration::new(0, 0), &file.attr, 0, fh, flags as u32);

        self.files.insert(ino, file);
        self.inodes.insert(name, ino);
        self.file_handles.insert(fh, file_handle);
    }
}
