//! Disk-space accounting for one fixed-offset replica file.
//!
//! Each replica reserves its full logical capacity, two checked header blocks,
//! and enough room to record every changed recovery region once. Rebuilds write
//! directly into the replacement replica, so they need no extra temporary copy.

use thiserror::Error;

/// Size of one independently stored data block.
pub const REPLICA_DATA_BLOCK_BYTES: u64 = 4 << 10;

/// Number of logical bytes covered by one crash-recovery region.
pub const CHANGED_REGION_BYTES: u64 = 16 << 20;

/// Smallest capacity used by the complete storage lifecycle tests.
pub const SMALLEST_TESTED_CAPACITY_BYTES: u64 = 64 << 20;

/// Largest block-aligned value representable by the descriptor.
pub const LARGEST_TESTED_CAPACITY_BYTES: u64 = u64::MAX - (REPLICA_DATA_BLOCK_BYTES - 1);

const FILE_HEADER_BYTES: u64 = 2 * REPLICA_DATA_BLOCK_BYTES;
const DIRECTORY_OVERHEAD_BYTES: u64 = REPLICA_DATA_BLOCK_BYTES;

/// Conservative fixed bytes in one framed changed-region record.
///
/// The encoder checks this bound, which keeps pool accounting tied to the
/// actual Cap'n Proto format instead of an estimate that can silently drift.
pub(crate) const MAX_CHANGED_REGION_RECORD_FIXED_BYTES: u64 = 512;

/// Encoded bytes added by each region number.
pub(crate) const MAX_CHANGED_REGION_RECORD_BYTES_PER_REGION: u64 = 8;

/// Pool space held for one fixed-offset local replica.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaSpace {
    data_bytes: u64,
    metadata_bytes: u64,
}

impl ReplicaSpace {
    /// Calculates the complete reservation for one logical capacity.
    pub fn for_capacity(capacity_bytes: u64) -> Result<Self, ReplicaSpaceError> {
        if capacity_bytes == 0 || !capacity_bytes.is_multiple_of(REPLICA_DATA_BLOCK_BYTES) {
            return Err(ReplicaSpaceError::InvalidCapacity { capacity_bytes });
        }

        let region_count = capacity_bytes.div_ceil(CHANGED_REGION_BYTES);
        // One region per record is the largest possible log because each
        // record then pays the full Cap'n Proto frame overhead.
        let changed_region_bytes = region_count
            .checked_mul(
                MAX_CHANGED_REGION_RECORD_FIXED_BYTES + MAX_CHANGED_REGION_RECORD_BYTES_PER_REGION,
            )
            .ok_or(ReplicaSpaceError::SizeOverflow)?;
        let changed_region_bytes = round_up(changed_region_bytes, REPLICA_DATA_BLOCK_BYTES)?;
        let metadata_bytes = FILE_HEADER_BYTES
            .checked_add(DIRECTORY_OVERHEAD_BYTES)
            .and_then(|bytes| bytes.checked_add(changed_region_bytes))
            .ok_or(ReplicaSpaceError::SizeOverflow)?;
        capacity_bytes
            .checked_add(metadata_bytes)
            .ok_or(ReplicaSpaceError::SizeOverflow)?;

        Ok(Self {
            data_bytes: capacity_bytes,
            metadata_bytes,
        })
    }

    /// Returns the logical data space promised to this replica.
    #[must_use]
    pub const fn data_bytes(self) -> u64 {
        self.data_bytes
    }

    /// Returns fixed headers and the worst-case changed-region log space.
    #[must_use]
    pub const fn metadata_bytes(self) -> u64 {
        self.metadata_bytes
    }

    /// Returns the complete reservation with checked addition.
    pub fn total_bytes(self) -> Result<u64, ReplicaSpaceError> {
        self.data_bytes
            .checked_add(self.metadata_bytes)
            .ok_or(ReplicaSpaceError::SizeOverflow)
    }
}

/// Explains why replica space cannot be calculated.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ReplicaSpaceError {
    /// Capacity must contain complete 4 KiB data blocks.
    #[error("capacity {capacity_bytes} must be a non-zero multiple of 4096 bytes")]
    InvalidCapacity {
        /// Invalid capacity supplied by the caller.
        capacity_bytes: u64,
    },

    /// A derived space value did not fit in 64 bits.
    #[error("replica space exceeds the 64-bit pool accounting range")]
    SizeOverflow,
}

/// Rounds a positive byte count up to a complete allocation block.
fn round_up(value: u64, block_bytes: u64) -> Result<u64, ReplicaSpaceError> {
    value
        .checked_add(block_bytes - 1)
        .map(|bytes| bytes / block_bytes * block_bytes)
        .ok_or(ReplicaSpaceError::SizeOverflow)
}

#[cfg(test)]
mod tests {
    use super::{
        CHANGED_REGION_BYTES, LARGEST_TESTED_CAPACITY_BYTES, REPLICA_DATA_BLOCK_BYTES,
        ReplicaSpace, ReplicaSpaceError, SMALLEST_TESTED_CAPACITY_BYTES,
    };

    /// Checks that every fixed size contains complete data blocks.
    #[test]
    fn selected_sizes_are_block_aligned() {
        assert!(CHANGED_REGION_BYTES.is_multiple_of(REPLICA_DATA_BLOCK_BYTES));
        assert!(SMALLEST_TESTED_CAPACITY_BYTES.is_multiple_of(REPLICA_DATA_BLOCK_BYTES));
        assert!(LARGEST_TESTED_CAPACITY_BYTES.is_multiple_of(REPLICA_DATA_BLOCK_BYTES));
    }

    /// Keeps the descriptor boundary separate from pool-accounting overflow.
    #[test]
    fn descriptor_capacity_has_no_arbitrary_product_cap() {
        assert_eq!(
            u64::MAX - (REPLICA_DATA_BLOCK_BYTES - 1),
            LARGEST_TESTED_CAPACITY_BYTES
        );
        assert_eq!(
            Err(ReplicaSpaceError::SizeOverflow),
            ReplicaSpace::for_capacity(LARGEST_TESTED_CAPACITY_BYTES)
        );
    }

    /// Records the fixed-file reservation for a representative large volume.
    #[test]
    fn replica_space_counts_data_headers_and_changed_regions() {
        let space = ReplicaSpace::for_capacity(1 << 40).expect("1 TiB must fit");
        assert_eq!(1 << 40, space.data_bytes());
        assert_eq!(34_091_008, space.metadata_bytes());
        assert_eq!(
            space.data_bytes() + space.metadata_bytes(),
            space.total_bytes().expect("total must fit")
        );
    }
}
