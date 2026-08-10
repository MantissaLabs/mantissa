use std::collections::BTreeSet;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;

use capnp::message::{Builder, ReaderOptions};
use mantissa_protocol::volumes::{
    LocalReplicaHealth as StoredReplicaHealth, LocalReplicaPoolFilesystem as StoredPoolFilesystem,
    LocalReplicaState as StoredReplicaState, LocalVolumeMountState as StoredMountState,
    local_attachment_record, local_filesystem_format, local_replica_origin,
    local_replica_pool_record, local_replica_record, local_replica_retirement, local_ublk_device,
    local_volume_mount,
};
use thiserror::Error;
use uuid::Uuid;

use super::model::{
    InvalidLocalReplicaOrigin, LocalAttachmentRecord, LocalReplicaOrigin, LocalReplicaRetirement,
    ReplicaHealth, ReplicaKey, ReplicaRecord, ReplicaState, ReservedSpace, SavedFilesystemFormat,
    SavedMountState, SavedUblkDevice, SavedVolumeMount,
};
use super::pool::{PoolFilesystem, ReplicaPool};
use crate::protocol::{ProtocolError, read_descriptor, write_descriptor};
use crate::{IdentityError, OperationId, ReplacementId, VolumeGeneration, VolumeId};

const CATALOG_FORMAT_VERSION: u16 = 1;
const MAX_CATALOG_RECORD_BYTES: usize = 8 << 10;
const MAX_CATALOG_NESTING_LEVELS: i32 = 16;

/// Durable pool identity and all space totals.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StoredPool {
    pub path: PathBuf,
    pub device_id: u64,
    pub filesystem: PoolFilesystem,
    pub block_bytes: u32,
    pub managed_bytes: u64,
    pub data_bytes: u64,
    pub metadata_bytes: u64,
}

impl StoredPool {
    /// Creates the first durable row for a checked empty catalog.
    pub(super) fn new(pool: &ReplicaPool) -> Self {
        Self {
            path: pool.root().to_path_buf(),
            device_id: pool.device_id(),
            filesystem: pool.filesystem(),
            block_bytes: pool.block_bytes(),
            managed_bytes: pool.managed_bytes(),
            data_bytes: 0,
            metadata_bytes: 0,
        }
    }

    /// Checks that the configured path still names the saved pool.
    pub(super) fn matches(&self, pool: &ReplicaPool) -> bool {
        self.path == pool.root()
            && self.device_id == pool.device_id()
            && self.filesystem == pool.filesystem()
            && self.block_bytes == pool.block_bytes()
    }
}

/// Encodes one complete local replica row.
pub(super) fn encode_replica(record: &ReplicaRecord) -> Result<Vec<u8>, CatalogProtocolError> {
    let mut message = Builder::new_default();
    let mut root = message.init_root::<local_replica_record::Builder<'_>>();
    root.set_format_version(CATALOG_FORMAT_VERSION);
    write_descriptor(root.reborrow().init_descriptor(), record.descriptor());
    write_replica_origin(root.reborrow().init_origin(), record.origin());
    root.set_directory_name(record.directory_name());
    root.set_state(write_replica_state(record.state()));
    root.set_health(write_replica_health(record.health()));
    root.set_data_bytes(record.reserved_space().data_bytes());
    root.set_metadata_bytes(record.reserved_space().metadata_bytes());
    if let Some(format) = record.filesystem_format() {
        write_filesystem_format(root.reborrow().init_filesystem_format(), format);
    }
    finish_message(&message)
}

/// Decodes and checks one complete local replica row.
pub(super) fn decode_replica(bytes: &[u8]) -> Result<ReplicaRecord, CatalogProtocolError> {
    let message = read_message(bytes)?;
    let root = message.root::<local_replica_record::Reader<'_>>()?;
    check_version(root.get_format_version())?;

    let descriptor = read_descriptor(root.get_descriptor()?)?;
    let origin = read_replica_origin(root.get_origin()?)?;
    let directory_name = root.get_directory_name()?.to_str()?.to_owned();
    let state = read_replica_state(root.get_state())?;
    let health = read_replica_health(root.get_health())?;
    let reserved = ReservedSpace::new(root.get_data_bytes(), root.get_metadata_bytes());
    let filesystem_format = if root.has_filesystem_format() {
        Some(read_filesystem_format(root.get_filesystem_format()?)?)
    } else {
        None
    };
    let mut record = ReplicaRecord::new(descriptor, origin, directory_name, reserved);
    record.set_state(state);
    record.set_health(health);
    if let Some(format) = filesystem_format {
        record.set_filesystem_format(format);
    }
    Ok(record)
}

/// Writes one unfinished ext4 format.
fn write_filesystem_format(
    mut builder: local_filesystem_format::Builder<'_>,
    format: SavedFilesystemFormat,
) {
    builder.set_filesystem_id(format.filesystem_id().as_bytes());
    builder.set_profile_hash(&format.profile_hash());
}

/// Reads and checks one unfinished ext4 format.
fn read_filesystem_format(
    reader: local_filesystem_format::Reader<'_>,
) -> Result<SavedFilesystemFormat, CatalogProtocolError> {
    Ok(SavedFilesystemFormat::new(
        crate::FilesystemId::new(read_uuid(reader.get_filesystem_id()?, "filesystem ID")?)?,
        read_fixed(reader.get_profile_hash()?, "filesystem profile hash")?,
    ))
}

/// Writes the immutable reason a local replica was reserved.
fn write_replica_origin(
    mut builder: local_replica_origin::Builder<'_>,
    origin: &LocalReplicaOrigin,
) {
    match origin {
        LocalReplicaOrigin::Bootstrap(id) => builder.set_bootstrap_id(id.as_bytes()),
        LocalReplicaOrigin::Replacement { id, .. } => builder.set_replacement_id(id.as_bytes()),
    }
    if let Some(voter_node_ids) = origin.voter_node_ids() {
        let mut voters = builder.init_voter_node_ids(voter_node_ids.len() as u32);
        for (index, node_id) in voter_node_ids.iter().enumerate() {
            voters.set(index as u32, node_id.as_bytes());
        }
    }
}

/// Reads the immutable reason a local replica was reserved.
fn read_replica_origin(
    reader: local_replica_origin::Reader<'_>,
) -> Result<LocalReplicaOrigin, CatalogProtocolError> {
    let voter_node_ids = read_origin_voters(reader.get_voter_node_ids()?)?;
    match reader.which().map_err(|capnp::NotInSchema(value)| {
        CatalogProtocolError::UnknownUnion {
            name: "local replica origin",
            value,
        }
    })? {
        local_replica_origin::Which::BootstrapId(value) => {
            if !voter_node_ids.is_empty() {
                return Err(CatalogProtocolError::UnexpectedBootstrapVoters);
            }
            Ok(LocalReplicaOrigin::Bootstrap(OperationId::new(read_uuid(
                value?,
                "bootstrap ID",
            )?)?))
        }
        local_replica_origin::Which::ReplacementId(value) => LocalReplicaOrigin::replacement(
            ReplacementId::new(read_uuid(value?, "replacement ID")?)?,
            voter_node_ids,
        )
        .map_err(Into::into),
    }
}

/// Reads unique non-zero voters from one bounded local record.
fn read_origin_voters(
    reader: capnp::data_list::Reader<'_>,
) -> Result<BTreeSet<Uuid>, CatalogProtocolError> {
    if reader.len() > 3 {
        return Err(InvalidLocalReplicaOrigin::InvalidVoterCount {
            actual: reader.len() as usize,
        }
        .into());
    }
    let mut voter_node_ids = BTreeSet::new();
    for value in reader.iter() {
        let node_id = read_uuid(value?, "origin voter node ID")?;
        if node_id.is_nil() {
            return Err(InvalidLocalReplicaOrigin::ZeroVoter.into());
        }
        if !voter_node_ids.insert(node_id) {
            return Err(CatalogProtocolError::DuplicateOriginVoter);
        }
    }
    Ok(voter_node_ids)
}

/// Writes one restart-safe ext4 mount record.
fn write_volume_mount(
    mut builder: local_volume_mount::Builder<'_>,
    volume_mount: &SavedVolumeMount,
) {
    builder.set_state(match volume_mount.state() {
        SavedMountState::Mounting => StoredMountState::Mounting,
        SavedMountState::Mounted => StoredMountState::Mounted,
        SavedMountState::Unmounting => StoredMountState::Unmounting,
    });
    builder.set_fence(volume_mount.fence().get());
    builder.set_session_id(volume_mount.session_id().as_bytes());
    builder.set_path(volume_mount.path().as_os_str().as_bytes());
    builder.set_owner_uid(volume_mount.owner_uid());
    builder.set_owner_gid(volume_mount.owner_gid());
    builder.set_mode(volume_mount.mode());
}

/// Reads and checks one restart-safe ext4 mount record.
fn read_volume_mount(
    reader: local_volume_mount::Reader<'_>,
) -> Result<SavedVolumeMount, CatalogProtocolError> {
    let state = match reader.get_state() {
        Ok(StoredMountState::Mounting) => SavedMountState::Mounting,
        Ok(StoredMountState::Mounted) => SavedMountState::Mounted,
        Ok(StoredMountState::Unmounting) => SavedMountState::Unmounting,
        Err(capnp::NotInSchema(value)) => {
            return Err(CatalogProtocolError::UnknownEnum {
                name: "local volume mount state",
                value,
            });
        }
    };
    Ok(SavedVolumeMount::from_stored_parts(
        state,
        crate::FenceEpoch::new(reader.get_fence())?,
        crate::DriverSessionId::new(read_uuid(reader.get_session_id()?, "driver session ID")?)?,
        PathBuf::from(std::ffi::OsString::from_vec(reader.get_path()?.to_vec())),
        reader.get_owner_uid(),
        reader.get_owner_gid(),
        reader.get_mode(),
    )?)
}

/// Writes one saved ublk device and the attachment it serves.
fn write_ublk_device(mut builder: local_ublk_device::Builder<'_>, device: SavedUblkDevice) {
    builder.set_device_id(device.id().get());
    builder.set_fence(device.fence().get());
    builder.set_session_id(device.session_id().as_bytes());
    builder.set_queue_count(device.queue_count());
    builder.set_queue_depth(device.queue_depth());
    builder.set_max_request_bytes(device.max_request_bytes());
}

/// Reads one saved ublk device before checking it against the descriptor.
fn read_ublk_device(
    reader: local_ublk_device::Reader<'_>,
) -> Result<SavedUblkDevice, CatalogProtocolError> {
    let device_id = reader.get_device_id();
    if i32::try_from(device_id).is_err() {
        return Err(CatalogProtocolError::InvalidUblkDeviceId { actual: device_id });
    }
    Ok(SavedUblkDevice::from_stored_parts(
        crate::driver::UblkDeviceId::new(device_id),
        crate::FenceEpoch::new(reader.get_fence())?,
        crate::DriverSessionId::new(read_uuid(reader.get_session_id()?, "driver session ID")?)?,
        reader.get_queue_count(),
        reader.get_queue_depth(),
        reader.get_max_request_bytes(),
    ))
}

/// Encodes one durable local attachment row independently from replica files.
pub(super) fn encode_attachment(
    record: &LocalAttachmentRecord,
) -> Result<Vec<u8>, CatalogProtocolError> {
    let mut message = Builder::new_default();
    let mut root = message.init_root::<local_attachment_record::Builder<'_>>();
    root.set_format_version(CATALOG_FORMAT_VERSION);
    write_descriptor(root.reborrow().init_descriptor(), record.descriptor());
    root.set_session_id(record.session_id().as_bytes());
    if let Some(fence) = record.granted_fence() {
        root.set_granted_fence(fence.get());
    }
    if let Some(device) = record.ublk_device() {
        write_ublk_device(root.reborrow().init_ublk_device(), device);
    }
    if let Some(volume_mount) = record.volume_mount() {
        write_volume_mount(root.reborrow().init_volume_mount(), volume_mount);
    }
    root.set_detaching(record.is_detaching());
    finish_message(&message)
}

/// Decodes and cross-checks one durable local attachment row.
pub(super) fn decode_attachment(
    bytes: &[u8],
) -> Result<LocalAttachmentRecord, CatalogProtocolError> {
    let message = read_message(bytes)?;
    let root = message.root::<local_attachment_record::Reader<'_>>()?;
    check_version(root.get_format_version())?;
    let descriptor = read_descriptor(root.get_descriptor()?)?;
    let session_id =
        crate::DriverSessionId::new(read_uuid(root.get_session_id()?, "driver session ID")?)?;
    let granted_fence = match root.get_granted_fence() {
        0 => None,
        value => Some(crate::FenceEpoch::new(value)?),
    };
    let ublk_device = if root.has_ublk_device() {
        let device = read_ublk_device(root.get_ublk_device()?)?;
        device.settings(&descriptor)?;
        Some(device)
    } else {
        None
    };
    let volume_mount = if root.has_volume_mount() {
        Some(read_volume_mount(root.get_volume_mount()?)?)
    } else {
        None
    };
    if ublk_device.is_some() && granted_fence.is_none() {
        return Err(CatalogProtocolError::AttachmentFenceMismatch);
    }
    if let Some(device) = ublk_device
        && (device.session_id() != session_id || Some(device.fence()) != granted_fence)
    {
        return Err(CatalogProtocolError::AttachmentFenceMismatch);
    }
    if let Some(volume_mount) = volume_mount.as_ref()
        && ublk_device.is_none_or(|device| {
            device.fence() != volume_mount.fence()
                || device.session_id() != volume_mount.session_id()
        })
    {
        return Err(CatalogProtocolError::VolumeMountDeviceMismatch);
    }
    let mut record = LocalAttachmentRecord::new(descriptor, session_id);
    if let Some(fence) = granted_fence {
        record.set_granted_fence(fence);
    }
    if let Some(device) = ublk_device {
        record.set_ublk_device(device);
    }
    let mount_is_unmounting = volume_mount
        .as_ref()
        .is_some_and(|mount| mount.state() == SavedMountState::Unmounting);
    if let Some(volume_mount) = volume_mount {
        record.set_volume_mount(volume_mount);
    }
    if root.get_detaching() || mount_is_unmounting {
        record.begin_detach();
    }
    Ok(record)
}

/// Encodes proof that a former member must not follow the immutable bootstrap plan.
pub(super) fn encode_retirement(
    retirement: LocalReplicaRetirement,
) -> Result<Vec<u8>, CatalogProtocolError> {
    let mut message = Builder::new_default();
    let mut root = message.init_root::<local_replica_retirement::Builder<'_>>();
    root.set_format_version(CATALOG_FORMAT_VERSION);
    root.set_volume_id(retirement.key().volume_id().as_bytes());
    root.set_generation(retirement.key().generation().get());
    finish_message(&message)
}

/// Decodes one former-member proof from the node-local catalog.
pub(super) fn decode_retirement(
    bytes: &[u8],
) -> Result<LocalReplicaRetirement, CatalogProtocolError> {
    let message = read_message(bytes)?;
    let root = message.root::<local_replica_retirement::Reader<'_>>()?;
    check_version(root.get_format_version())?;
    Ok(LocalReplicaRetirement::new(ReplicaKey::new(
        VolumeId::new(read_uuid(root.get_volume_id()?, "volume ID")?)?,
        VolumeGeneration::new(root.get_generation())?,
    )))
}

/// Reads one UUID from its exact byte representation.
fn read_uuid(bytes: &[u8], field: &'static str) -> Result<Uuid, CatalogProtocolError> {
    Ok(Uuid::from_bytes(read_fixed(bytes, field)?))
}

/// Reads one fixed-width byte field.
fn read_fixed<const N: usize>(
    bytes: &[u8],
    field: &'static str,
) -> Result<[u8; N], CatalogProtocolError> {
    bytes
        .try_into()
        .map_err(|_| CatalogProtocolError::InvalidFieldLength {
            field,
            expected: N,
            actual: bytes.len(),
        })
}

/// Encodes the single durable pool row.
pub(super) fn encode_pool(pool: &StoredPool) -> Result<Vec<u8>, CatalogProtocolError> {
    let mut message = Builder::new_default();
    let mut root = message.init_root::<local_replica_pool_record::Builder<'_>>();
    root.set_format_version(CATALOG_FORMAT_VERSION);
    root.set_path(pool.path.as_os_str().as_bytes());
    root.set_device_id(pool.device_id);
    root.set_filesystem(match pool.filesystem {
        PoolFilesystem::Ext4 => StoredPoolFilesystem::Ext4,
        PoolFilesystem::Xfs => StoredPoolFilesystem::Xfs,
    });
    root.set_filesystem_block_bytes(pool.block_bytes);
    root.set_managed_bytes(pool.managed_bytes);
    root.set_data_bytes(pool.data_bytes);
    root.set_metadata_bytes(pool.metadata_bytes);
    finish_message(&message)
}

/// Decodes and checks the single durable pool row.
pub(super) fn decode_pool(bytes: &[u8]) -> Result<StoredPool, CatalogProtocolError> {
    let message = read_message(bytes)?;
    let root = message.root::<local_replica_pool_record::Reader<'_>>()?;
    check_version(root.get_format_version())?;
    let filesystem = match root.get_filesystem() {
        Ok(StoredPoolFilesystem::Ext4) => PoolFilesystem::Ext4,
        Ok(StoredPoolFilesystem::Xfs) => PoolFilesystem::Xfs,
        Err(capnp::NotInSchema(value)) => {
            return Err(CatalogProtocolError::UnknownEnum {
                name: "local replica pool filesystem",
                value,
            });
        }
    };
    let path = PathBuf::from(std::ffi::OsString::from_vec(root.get_path()?.to_vec()));
    Ok(StoredPool {
        path,
        device_id: root.get_device_id(),
        filesystem,
        block_bytes: root.get_filesystem_block_bytes(),
        managed_bytes: root.get_managed_bytes(),
        data_bytes: root.get_data_bytes(),
        metadata_bytes: root.get_metadata_bytes(),
    })
}

/// Converts the owned local state to its stored enum.
fn write_replica_state(state: ReplicaState) -> StoredReplicaState {
    match state {
        ReplicaState::Preparing => StoredReplicaState::Preparing,
        ReplicaState::Ready => StoredReplicaState::Ready,
        ReplicaState::Deleting => StoredReplicaState::Deleting,
        ReplicaState::Retiring => StoredReplicaState::Retiring,
        ReplicaState::Retained => StoredReplicaState::Retained,
    }
}

/// Converts and checks the stored local state.
fn read_replica_state(
    state: Result<StoredReplicaState, capnp::NotInSchema>,
) -> Result<ReplicaState, CatalogProtocolError> {
    match state {
        Ok(StoredReplicaState::Preparing) => Ok(ReplicaState::Preparing),
        Ok(StoredReplicaState::Ready) => Ok(ReplicaState::Ready),
        Ok(StoredReplicaState::Deleting) => Ok(ReplicaState::Deleting),
        Ok(StoredReplicaState::Retiring) => Ok(ReplicaState::Retiring),
        Ok(StoredReplicaState::Retained) => Ok(ReplicaState::Retained),
        Err(capnp::NotInSchema(value)) => Err(CatalogProtocolError::UnknownEnum {
            name: "local replica state",
            value,
        }),
    }
}

/// Converts the owned local health to its stored enum.
fn write_replica_health(health: ReplicaHealth) -> StoredReplicaHealth {
    match health {
        ReplicaHealth::Healthy => StoredReplicaHealth::Healthy,
        ReplicaHealth::NeedsRecovery => StoredReplicaHealth::NeedsRecovery,
    }
}

/// Converts and checks the stored local replica health.
fn read_replica_health(
    health: Result<StoredReplicaHealth, capnp::NotInSchema>,
) -> Result<ReplicaHealth, CatalogProtocolError> {
    match health {
        Ok(StoredReplicaHealth::Healthy) => Ok(ReplicaHealth::Healthy),
        Ok(StoredReplicaHealth::NeedsRecovery) => Ok(ReplicaHealth::NeedsRecovery),
        Err(capnp::NotInSchema(value)) => Err(CatalogProtocolError::UnknownEnum {
            name: "local replica health",
            value,
        }),
    }
}

/// Checks that a saved record uses the current catalog format.
fn check_version(version: u16) -> Result<(), CatalogProtocolError> {
    if version != CATALOG_FORMAT_VERSION {
        return Err(CatalogProtocolError::UnsupportedVersion { actual: version });
    }
    Ok(())
}

/// Finishes one independent Cap'n Proto record and checks its byte limit.
fn finish_message(
    message: &Builder<capnp::message::HeapAllocator>,
) -> Result<Vec<u8>, CatalogProtocolError> {
    let bytes = capnp::serialize::write_message_to_words(message);
    if bytes.len() > MAX_CATALOG_RECORD_BYTES {
        return Err(CatalogProtocolError::RecordTooLarge {
            actual: bytes.len(),
            maximum: MAX_CATALOG_RECORD_BYTES,
        });
    }
    Ok(bytes)
}

enum MessageReader<'a> {
    Borrowed(capnp::message::Reader<capnp::serialize::BufferSegments<&'a [u8]>>),
    Owned(capnp::message::Reader<capnp::serialize::OwnedSegments>),
}

impl MessageReader<'_> {
    /// Reads one generated root from aligned input or an owned aligned copy.
    fn root<'a, T>(&'a self) -> Result<T, capnp::Error>
    where
        T: capnp::traits::FromPointerReader<'a>,
    {
        match self {
            Self::Borrowed(message) => message.get_root(),
            Self::Owned(message) => message.get_root(),
        }
    }
}

/// Opens one bounded Cap'n Proto record and rejects trailing bytes.
fn read_message(bytes: &[u8]) -> Result<MessageReader<'_>, CatalogProtocolError> {
    if bytes.len() > MAX_CATALOG_RECORD_BYTES {
        return Err(CatalogProtocolError::RecordTooLarge {
            actual: bytes.len(),
            maximum: MAX_CATALOG_RECORD_BYTES,
        });
    }
    let mut options = ReaderOptions::new();
    options
        .traversal_limit_in_words(Some(MAX_CATALOG_RECORD_BYTES / 8))
        .nesting_limit(MAX_CATALOG_NESTING_LEVELS);
    let mut remaining = bytes;
    let message = if (bytes.as_ptr() as usize).is_multiple_of(std::mem::align_of::<capnp::Word>()) {
        MessageReader::Borrowed(capnp::serialize::read_message_from_flat_slice(
            &mut remaining,
            options,
        )?)
    } else {
        MessageReader::Owned(capnp::serialize::read_message(&mut remaining, options)?)
    };
    if !remaining.is_empty() {
        return Err(CatalogProtocolError::TrailingBytes {
            actual: remaining.len(),
        });
    }
    Ok(message)
}

/// Rejects damaged or unsupported local catalog records.
#[derive(Debug, Error)]
pub enum CatalogProtocolError {
    /// Cap'n Proto could not read a requested value.
    #[error("could not read local volume catalog Cap'n Proto value")]
    Capnp(#[from] capnp::Error),

    /// A text field did not contain valid UTF-8.
    #[error("local volume catalog text is not valid UTF-8")]
    Text(#[from] std::str::Utf8Error),

    /// A stored volume descriptor was invalid.
    #[error(transparent)]
    Volume(#[from] ProtocolError),

    /// A stored operation UUID was invalid.
    #[error(transparent)]
    Identity(#[from] IdentityError),

    /// Saved ublk settings cannot recreate or recover the kernel device.
    #[error(transparent)]
    InvalidUblkSettings(#[from] crate::driver::InvalidUblkSettings),

    /// A saved mount path, mode, or step was invalid.
    #[error(transparent)]
    InvalidVolumeMount(#[from] super::model::InvalidSavedVolumeMount),

    /// Saved provisioning provenance cannot route safe reconciliation.
    #[error(transparent)]
    InvalidReplicaOrigin(#[from] InvalidLocalReplicaOrigin),

    /// Voter routes must contain each node at most once.
    #[error("local replica origin contains a duplicate voter UUID")]
    DuplicateOriginVoter,

    /// Bootstrap routing comes from its immutable desired plan.
    #[error("bootstrap replica origin must not duplicate its voter set")]
    UnexpectedBootstrapVoters,

    /// A mount did not use the saved backend's writer session and fence.
    #[error("saved volume mount does not match the saved backend session")]
    VolumeMountDeviceMismatch,

    /// A saved device does not match the attachment's committed control state.
    #[error("saved ublk device does not match the attachment fence")]
    AttachmentFenceMismatch,

    /// libublk represents device numbers as non-negative signed integers.
    #[error("saved ublk device ID {actual} exceeds the libublk range")]
    InvalidUblkDeviceId {
        /// Device number read from the local record.
        actual: u32,
    },

    /// The saved record uses a catalog format this build cannot read.
    #[error("unsupported local volume catalog format version {actual}")]
    UnsupportedVersion {
        /// Version read from the record.
        actual: u16,
    },

    /// The complete record exceeds its fixed safety limit.
    #[error("local volume catalog record is {actual} bytes; maximum is {maximum}")]
    RecordTooLarge {
        /// Encoded bytes supplied by the record.
        actual: usize,

        /// Largest accepted complete record.
        maximum: usize,
    },

    /// Bytes after the first complete record are never accepted.
    #[error("local volume catalog record has {actual} trailing bytes")]
    TrailingBytes {
        /// Bytes left after decoding one message.
        actual: usize,
    },

    /// One stored enum value is not known by this build.
    #[error("unknown {name} value {value}")]
    UnknownEnum {
        /// Plain name of the enum field.
        name: &'static str,

        /// Unknown numeric value.
        value: u16,
    },

    /// One stored union discriminant is not known by this build.
    #[error("unknown {name} discriminant {value}")]
    UnknownUnion {
        /// Plain name of the union field.
        name: &'static str,

        /// Unknown numeric discriminant.
        value: u16,
    },

    /// A fixed-width field had the wrong byte count.
    #[error("{field} must contain {expected} bytes, got {actual}")]
    InvalidFieldLength {
        /// Plain field name.
        field: &'static str,

        /// Required byte count.
        expected: usize,

        /// Stored byte count.
        actual: usize,
    },
}
