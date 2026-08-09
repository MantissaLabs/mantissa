use std::error::Error;
use std::fmt;

use zeroize::Zeroizing;

pub(crate) const GROUP_KEY_BYTES: usize = 32;

/// Holds one group encryption key and clears it when dropped.
pub struct GroupEncryptionKey {
    bytes: Zeroizing<[u8; GROUP_KEY_BYTES]>,
}

impl GroupEncryptionKey {
    /// Takes ownership of one 256-bit group encryption key.
    #[must_use]
    pub fn new(bytes: [u8; GROUP_KEY_BYTES]) -> Self {
        Self {
            bytes: Zeroizing::new(bytes),
        }
    }

    /// Borrows the key for per-segment key derivation.
    pub(crate) fn as_bytes(&self) -> &[u8; GROUP_KEY_BYTES] {
        &self.bytes
    }
}

impl fmt::Debug for GroupEncryptionKey {
    /// Hides key material from logs and diagnostics.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GroupEncryptionKey([REDACTED])")
    }
}

/// Loads the encryption key prepared for one Raft group.
pub trait GroupKeyProvider<GID>: Send + Sync {
    /// Error returned when the group key cannot be loaded.
    type Error: Error + Send + Sync + 'static;

    /// Returns the key used to derive unique keys for this group's segments.
    fn key_for_group(&self, group_id: &GID) -> Result<GroupEncryptionKey, Self::Error>;
}
