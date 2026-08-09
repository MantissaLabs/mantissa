use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use super::SegmentId;
use super::model::SEGMENT_ID_BYTES;

#[cfg(unix)]
const SEGMENT_FILE_MODE: u32 = 0o600;

/// File operations needed by one segment without exposing `std::fs::File`.
pub(crate) trait SegmentFile: Read + Write + Seek + Send {
    /// Flushes file data and metadata to durable storage.
    fn sync_all(&self) -> io::Result<()>;

    /// Returns the current file length.
    fn len(&self) -> io::Result<u64>;

    /// Changes the file length and removes any bytes after it.
    fn set_len(&self, length: u64) -> io::Result<()>;
}

impl SegmentFile for File {
    /// Flushes a standard file to durable storage.
    fn sync_all(&self) -> io::Result<()> {
        File::sync_all(self)
    }

    /// Reads the standard file's current length.
    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    /// Changes a standard file's length.
    fn set_len(&self, length: u64) -> io::Result<()> {
        File::set_len(self, length)
    }
}

/// File and random-ID operations that failure tests can control.
pub(crate) trait FileSystem: Send + Sync {
    /// Creates the group directory and reports whether it was new.
    fn create_directory(&self, path: &Path) -> io::Result<bool>;

    /// Flushes changes to one directory entry set.
    fn sync_directory(&self, path: &Path) -> io::Result<()>;

    /// Creates one new segment without replacing an existing file.
    fn create_segment(&self, path: &Path) -> io::Result<Box<dyn SegmentFile>>;

    /// Opens one existing segment for bounded reads.
    fn open_segment(&self, path: &Path) -> io::Result<Box<dyn SegmentFile>>;

    /// Opens one existing segment so recovery can shorten it.
    fn open_segment_for_update(&self, path: &Path) -> io::Result<Box<dyn SegmentFile>>;

    /// Lists files stored directly in one group directory.
    fn list_files(&self, directory: &Path) -> io::Result<Vec<std::path::PathBuf>>;

    /// Moves a completed replacement file to its final name.
    fn move_file(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Deletes one segment or unfinished replacement file.
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// Creates one random segment identity.
    fn new_segment_id(&self) -> Result<SegmentId, getrandom::Error>;
}

/// Uses the host filesystem and operating-system randomness.
pub(crate) struct StandardFileSystem;

impl FileSystem for StandardFileSystem {
    /// Creates exactly one group directory below an existing parent.
    fn create_directory(&self, path: &Path) -> io::Result<bool> {
        match fs::create_dir(path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if path.is_dir() {
                    Ok(false)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "Raft log group path is not a directory",
                    ))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Opens and flushes one directory.
    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()
    }

    /// Creates one owner-only segment file.
    fn create_segment(&self, path: &Path) -> io::Result<Box<dyn SegmentFile>> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        options.mode(SEGMENT_FILE_MODE);
        Ok(Box::new(options.open(path)?))
    }

    /// Opens one existing segment without write access.
    fn open_segment(&self, path: &Path) -> io::Result<Box<dyn SegmentFile>> {
        Ok(Box::new(OpenOptions::new().read(true).open(path)?))
    }

    /// Opens one existing segment with read and write access.
    fn open_segment_for_update(&self, path: &Path) -> io::Result<Box<dyn SegmentFile>> {
        Ok(Box::new(
            OpenOptions::new().read(true).write(true).open(path)?,
        ))
    }

    /// Lists direct children without following their contents.
    fn list_files(&self, directory: &Path) -> io::Result<Vec<std::path::PathBuf>> {
        fs::read_dir(directory)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect()
    }

    /// Moves a file without replacing an existing target.
    fn move_file(&self, from: &Path, to: &Path) -> io::Result<()> {
        if to.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "Raft log segment already exists",
            ));
        }
        fs::rename(from, to)
    }

    /// Deletes one regular file.
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    /// Reads a fresh 128-bit identity from the operating system.
    fn new_segment_id(&self) -> Result<SegmentId, getrandom::Error> {
        let mut bytes = [0; SEGMENT_ID_BYTES];
        getrandom::getrandom(&mut bytes)?;
        Ok(SegmentId::new(bytes))
    }
}
