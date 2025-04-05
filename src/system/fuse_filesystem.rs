use crate::system::file_layer::FileHandle;
use crate::{
    create_cdc_filesystem, ChunkHash, ChunkerRef, DataContainer, Database, FileSystem, Hasher,
};
use fuser::FileType::RegularFile;
use fuser::TimeOrNow::Now;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use libc::{
    EACCES, EBADF, EEXIST, EINVAL, EIO, ENOENT, EPERM, ESTALE, O_ACCMODE, O_RDONLY, O_RDWR,
    O_WRONLY, R_OK, W_OK, X_OK,
};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::time::{Duration, SystemTime};

type Inode = u64;
type Fh = u64;

const FMODE_EXEC: i32 = 0x20;

#[derive(Clone)]
struct FuseFile {
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

/// Wrap around [`FileSystem`] for implementing [`fuser::Filesystem`] trait.
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
        access_mask &= X_OK;
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
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_str().unwrap().to_owned();
        if parent != 1 {
            reply.error(EINVAL);
            return;
        }

        let Some(inode) = self.inodes.get::<String>(&name) else {
            reply.error(ENOENT);
            return;
        };
        let file = self.files.get(inode).unwrap();
        reply.entry(&Duration::new(0, 0), &file.attr, file.generation)
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.files.get(&ino) {
            Some(file) => reply.attr(&Duration::new(0, 0), &file.attr),
            None => reply.error(ENOENT),
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
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let Some(file) = self.files.get_mut(&ino) else {
            reply.error(ENOENT);
            return;
        };

        let now = SystemTime::now();
        let attr = &mut file.attr;
        if let Some(mode) = mode {
            if req.uid() != 0 && req.uid() != attr.uid {
                reply.error(EPERM);
                return;
            } else {
                attr.perm = mode as u16;
            }
            attr.ctime = now;
            reply.attr(&Duration::new(0, 0), &file.attr);
            return;
        }

        if let Some(atime) = atime {
            if attr.uid != req.uid() && req.uid() != 0 && atime != Now {
                reply.error(EPERM);
                return;
            }

            if attr.uid != req.uid() && !check_access(&attr, req, W_OK) {
                reply.error(EACCES);
                return;
            }

            attr.atime = match atime {
                TimeOrNow::SpecificTime(time) => time,
                Now => now,
            };
            attr.ctime = now;
        }

        if let Some(mtime) = mtime {
            if attr.uid != req.uid() && req.uid() != 0 && mtime != Now {
                reply.error(EPERM);
                return;
            }

            if attr.uid != req.uid() && !check_access(&attr, req, W_OK) {
                reply.error(EACCES);
                return;
            }

            attr.mtime = match mtime {
                TimeOrNow::SpecificTime(time) => time,
                Now => now,
            };
            attr.ctime = now;
        }

        reply.attr(&Duration::new(0, 0), &attr);
        return;
    }

    fn open(&mut self, req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let Some(file) = self.files.get_mut(&ino) else {
            reply.error(ENOENT);
            return;
        };

        let (access_mask, read, write) = match flags & O_ACCMODE {
            O_RDONLY => {
                if flags & libc::O_TRUNC != 0 {
                    reply.error(EACCES);
                    return;
                }
                if flags & FMODE_EXEC != 0 {
                    // Open is from internal exec syscall
                    (X_OK, true, false)
                } else {
                    (R_OK, true, false)
                }
            }
            O_WRONLY => (W_OK, false, true),
            O_RDWR => (R_OK | W_OK, true, true),
            // Exactly one access mode flag must be specified
            _ => {
                reply.error(EINVAL);
                return;
            }
        };

        if !check_access(&file.attr, req, access_mask) {
            reply.error(EACCES);
            return;
        }

        let underlying_file_handle = if write {
            self.underlying_fs
                .open_file(&file.name, self.chunker.clone())
        } else {
            self.underlying_fs.open_file_readonly(&file.name)
        }
        .unwrap();

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
            reply.error(EBADF);
            return;
        };
        if file_handle.inode != ino {
            reply.error(ESTALE);
            return;
        }
        let Some(file) = self.files.get(&ino) else {
            reply.error(ENOENT);
            return;
        };
        if offset < 0 {
            reply.error(EINVAL);
            return;
        }

        if !check_access(&file.attr, req, R_OK) || !file_handle.read {
            reply.error(EACCES);
            return;
        }
        let underlying_fh = &mut file_handle.underlying_file_handle;
        underlying_fh.set_offset(offset as usize);

        if let Ok(data) = self.underlying_fs.read(underlying_fh, size as usize) {
            reply.data(&data);
            return;
        } else {
            reply.error(EIO);
            return;
        };
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
            reply.error(EBADF);
            return;
        };
        if file_handle.inode != ino {
            reply.error(ESTALE);
            return;
        }
        let Some(file) = self.files.get_mut(&ino) else {
            reply.error(ENOENT);
            return;
        };
        if offset < 0 || offset as u64 != file.attr.size {
            reply.error(EINVAL);
            return;
        }

        if !check_access(&file.attr, req, R_OK) || !file_handle.write {
            reply.error(EACCES);
            return;
        }

        if !self
            .underlying_fs
            .write_to_file(&mut file_handle.underlying_file_handle, data)
            .is_ok()
        {
            reply.error(EIO);
            return;
        }

        let now = SystemTime::now();
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
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let Some(file) = self.files.get_mut(&ino) else {
            reply.error(EINVAL);
            return;
        };
        if file.handles <= 0 {
            reply.error(EINVAL);
            return;
        }
        let Some(file_handle) = self.file_handles.remove(&ino) else {
            reply.error(EINVAL);
            return;
        };
        file_handle.underlying_file_handle.close();
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
            reply.error(EINVAL);
            return;
        }
        let dir = self.files.get(&ino).unwrap();
        if !check_access(&dir.attr, req, R_OK) {
            reply.error(EACCES);
            return;
        }

        let entries = self
            .files
            .iter()
            .map(|(inode, file)| (inode, file.attr.kind, &file.name));
        for (i, entry) in entries.enumerate().skip(offset as usize) {
            let (inode, kind, name) = entry;
            if reply.add(inode.clone(), offset + i as i64 + 1, kind, name) {
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
            reply.error(EINVAL);
            return;
        }
        let fh = self.get_new_fh();
        let Ok(underlying_file_handle) = self
            .underlying_fs
            .create_file(name.clone(), (&self.chunker).clone())
        else {
            reply.error(EEXIST);
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

        let (read, write) = match flags & O_ACCMODE {
            O_RDONLY => (true, false),
            O_WRONLY => (false, true),
            O_RDWR => (true, true),
            // Exactly one access mode flag must be specified
            _ => {
                reply.error(EINVAL);
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
            attr,
            name: name.clone(),
            generation: 0,
            handles: 1,
        };

        reply.created(
            &Duration::new(0, 0),
            &file.attr,
            0,
            fh.clone(),
            flags as u32,
        );

        self.files.insert(ino, file);
        self.inodes.insert(name, ino);
        self.file_handles.insert(fh, file_handle);
    }
}
