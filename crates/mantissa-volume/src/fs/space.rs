//! Read-only filesystem space measurements.

use std::io;
use std::path::Path;

/// Space reported by one mounted filesystem.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FilesystemSpace {
    total_bytes: u64,
    used_bytes: u64,
    available_bytes: u64,
}

impl FilesystemSpace {
    /// Builds a checked measurement from byte counters.
    pub fn from_bytes(total_bytes: u64, used_bytes: u64, available_bytes: u64) -> io::Result<Self> {
        if total_bytes == 0 || used_bytes > total_bytes || available_bytes > total_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "filesystem returned invalid byte counters",
            ));
        }
        Ok(Self {
            total_bytes,
            used_bytes,
            available_bytes,
        })
    }

    /// Returns the filesystem's total data-block capacity.
    #[must_use]
    pub const fn total_bytes(self) -> u64 {
        self.total_bytes
    }

    /// Returns bytes held in allocated filesystem data blocks.
    #[must_use]
    pub const fn used_bytes(self) -> u64 {
        self.used_bytes
    }

    /// Returns bytes available to an unprivileged workload.
    #[must_use]
    pub const fn available_bytes(self) -> u64 {
        self.available_bytes
    }
}

/// Measures one mounted filesystem without walking its directory tree.
pub fn measure(path: &Path) -> io::Result<FilesystemSpace> {
    let stat = nix::sys::statvfs::statvfs(path).map_err(io::Error::from)?;
    checked_space(
        normalize_counter(stat.fragment_size()),
        normalize_counter(stat.blocks()),
        normalize_counter(stat.blocks_free()),
        normalize_counter(stat.blocks_available()),
    )
}

/// Normalizes platform-specific filesystem counter widths for checked arithmetic.
fn normalize_counter<T: Into<u64>>(value: T) -> u64 {
    value.into()
}

/// Converts filesystem block counts while rejecting invalid or overflowing results.
fn checked_space(
    block_bytes: u64,
    total_blocks: u64,
    free_blocks: u64,
    available_blocks: u64,
) -> io::Result<FilesystemSpace> {
    if block_bytes == 0 || free_blocks > total_blocks || available_blocks > free_blocks {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "filesystem returned invalid space counters",
        ));
    }
    let total_bytes = total_blocks
        .checked_mul(block_bytes)
        .ok_or_else(|| io::Error::other("filesystem total capacity overflowed u64"))?;
    let used_bytes = (total_blocks - free_blocks)
        .checked_mul(block_bytes)
        .ok_or_else(|| io::Error::other("filesystem used capacity overflowed u64"))?;
    let available_bytes = available_blocks
        .checked_mul(block_bytes)
        .ok_or_else(|| io::Error::other("filesystem available capacity overflowed u64"))?;
    FilesystemSpace::from_bytes(total_bytes, used_bytes, available_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Available space keeps filesystem-reserved blocks out of the workload value.
    #[test]
    fn space_keeps_reserved_blocks_out_of_available_bytes() {
        let space = checked_space(4_096, 1_000, 400, 350).expect("valid filesystem counters");

        assert_eq!(4_096_000, space.total_bytes());
        assert_eq!(2_457_600, space.used_bytes());
        assert_eq!(1_433_600, space.available_bytes());
    }

    /// Invalid kernel counters are rejected instead of producing misleading capacity.
    #[test]
    fn space_rejects_invalid_block_counts() {
        assert!(checked_space(4_096, 100, 101, 100).is_err());
        assert!(checked_space(4_096, 100, 50, 51).is_err());
        assert!(checked_space(0, 100, 50, 50).is_err());
    }
}
