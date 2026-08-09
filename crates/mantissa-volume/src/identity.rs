use std::fmt;

use thiserror::Error;
use uuid::Uuid;

/// Volume ID that stays the same when the volume is rebuilt.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VolumeId(Uuid);

impl VolumeId {
    /// Creates a volume ID from a non-zero UUID.
    pub fn new(value: Uuid) -> Result<Self, IdentityError> {
        if value.is_nil() {
            return Err(IdentityError::NilVolumeId);
        }
        Ok(Self(value))
    }

    /// Returns the UUID form used by the rest of Mantissa.
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Returns the exact 16 bytes written to protocol messages.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Stable ID used to retry one multi-step volume operation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationId(Uuid);

impl OperationId {
    /// Creates an operation ID from a non-zero UUID.
    pub fn new(value: Uuid) -> Result<Self, IdentityError> {
        if value.is_nil() {
            return Err(IdentityError::NilOperationId);
        }
        Ok(Self(value))
    }

    /// Returns the exact 16 bytes written to protocol messages.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// UUID written into the ext4 filesystem created for one volume generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FilesystemId(Uuid);

impl FilesystemId {
    /// Creates a filesystem ID from a non-zero UUID.
    pub fn new(value: Uuid) -> Result<Self, IdentityError> {
        if value.is_nil() {
            return Err(IdentityError::NilFilesystemId);
        }
        Ok(Self(value))
    }

    /// Returns the UUID passed to filesystem tools.
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Returns the exact 16 bytes written to protocol messages.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Node selected to own one exclusive volume attachment.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VolumeNodeId(Uuid);

impl VolumeNodeId {
    /// Creates a volume node ID from a non-zero UUID.
    pub fn new(value: Uuid) -> Result<Self, IdentityError> {
        if value.is_nil() {
            return Err(IdentityError::NilVolumeNodeId);
        }
        Ok(Self(value))
    }

    /// Returns the exact 16 bytes written to protocol messages.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Returns the UUID used by node transport and cluster state.
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl fmt::Display for VolumeNodeId {
    /// Writes the ordinary UUID form used in errors and logs.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// One driver startup attempt within an attachment epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DriverSessionId(Uuid);

impl DriverSessionId {
    /// Creates a driver session ID from a non-zero UUID.
    pub fn new(value: Uuid) -> Result<Self, IdentityError> {
        if value.is_nil() {
            return Err(IdentityError::NilDriverSessionId);
        }
        Ok(Self(value))
    }

    /// Returns the exact 16 bytes written to protocol messages.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Monotonic value that fences every older data-plane writer and operation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FenceEpoch(u64);

impl FenceEpoch {
    /// Creates a non-zero data-plane fence.
    pub fn new(value: u64) -> Result<Self, IdentityError> {
        if value == 0 {
            return Err(IdentityError::ZeroFenceEpoch);
        }
        Ok(Self(value))
    }

    /// Returns the first fence installed when a volume is initialized.
    #[must_use]
    pub const fn initial() -> Self {
        Self(1)
    }

    /// Returns the integer stored in control state and replica-file headers.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next fencing value, or none after the largest value.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl fmt::Display for FenceEpoch {
    /// Formats the fence as its stored integer.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable authorization identity for one divergent-copy recovery attempt.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RecoveryId(Uuid);

impl RecoveryId {
    /// Creates a recovery identity from a non-zero UUID.
    pub fn new(value: Uuid) -> Result<Self, IdentityError> {
        if value.is_nil() {
            return Err(IdentityError::NilRecoveryId);
        }
        Ok(Self(value))
    }

    /// Returns the UUID used by control and data protocols.
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Returns the exact bytes written to protocol messages.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl From<RecoveryId> for OperationId {
    /// Preserves the UUID when passing a recovery grant to the fixed-file format.
    fn from(value: RecoveryId) -> Self {
        Self(value.0)
    }
}

/// Stable authorization identity for one replica replacement attempt.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReplacementId(Uuid);

impl ReplacementId {
    /// Creates a replacement identity from a non-zero UUID.
    pub fn new(value: Uuid) -> Result<Self, IdentityError> {
        if value.is_nil() {
            return Err(IdentityError::NilReplacementId);
        }
        Ok(Self(value))
    }

    /// Returns the UUID used by control and data protocols.
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Returns the exact bytes written to protocol messages.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl From<ReplacementId> for OperationId {
    /// Preserves the UUID when passing a replacement grant to the fixed-file format.
    fn from(value: ReplacementId) -> Self {
        Self(value.0)
    }
}

/// Number changed whenever a volume is rebuilt from scratch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VolumeGeneration(u64);

impl VolumeGeneration {
    /// Creates a non-zero volume generation.
    pub fn new(value: u64) -> Result<Self, IdentityError> {
        if value == 0 {
            return Err(IdentityError::ZeroGeneration);
        }
        Ok(Self(value))
    }

    /// Returns the number stored in protocol and local records.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for VolumeGeneration {
    /// Formats the generation as its stored integer.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Rejects invalid volume IDs and generation numbers.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum IdentityError {
    /// The all-zero UUID is not a usable stable volume identity.
    #[error("volume id must not be nil")]
    NilVolumeId,

    /// The all-zero UUID cannot identify retryable work.
    #[error("operation id must not be nil")]
    NilOperationId,

    /// The all-zero UUID cannot identify an ext4 filesystem.
    #[error("filesystem id must not be nil")]
    NilFilesystemId,

    /// The all-zero UUID cannot identify an attached node.
    #[error("volume node id must not be nil")]
    NilVolumeNodeId,

    /// The all-zero UUID cannot identify a driver startup attempt.
    #[error("driver session id must not be nil")]
    NilDriverSessionId,

    /// The all-zero UUID cannot identify recovery grant.
    #[error("recovery id must not be nil")]
    NilRecoveryId,

    /// The all-zero UUID cannot identify replacement grant.
    #[error("replacement id must not be nil")]
    NilReplacementId,

    /// Zero means that no data-plane fence has been allocated.
    #[error("fence epoch must be non-zero")]
    ZeroFenceEpoch,

    /// Zero is reserved as an invalid or missing volume generation.
    #[error("volume generation must be non-zero")]
    ZeroGeneration,
}
