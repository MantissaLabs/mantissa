use std::sync::Arc;

use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

use super::model::{
    LocalAttachmentRecord, LocalReplicaOrigin, LocalReplicaRetirement, PoolSpaceState, PoolStatus,
    ReplicaHealth, ReplicaKey, ReplicaRecord, ReplicaState, ReservedSpace, SavedFilesystemFormat,
    SavedMountState, SavedUblkDevice, SavedVolumeMount,
};
use super::protocol::{
    StoredPool, decode_attachment, decode_pool, decode_replica, decode_retirement,
    encode_attachment, encode_pool, encode_replica, encode_retirement,
};
use super::{CatalogError, ReplicaPool};
use crate::storage_format::ReplicaSpace;
use crate::{DriverSessionId, FenceEpoch, VolumeCapacity, VolumeDescriptor};

const REPLICAS: TableDefinition<'static, &'static [u8], &'static [u8]> =
    TableDefinition::new("mantissa_volume_local_replicas");
const POOL: TableDefinition<'static, &'static [u8], &'static [u8]> =
    TableDefinition::new("mantissa_volume_local_pool");
const ATTACHMENTS: TableDefinition<'static, &'static [u8], &'static [u8]> =
    TableDefinition::new("mantissa_volume_local_attachments");
const RETIREMENTS: TableDefinition<'static, &'static [u8], &'static [u8]> =
    TableDefinition::new("mantissa_volume_local_retirements");
const POOL_KEY: &[u8] = b"pool";

/// Shared node-local catalog for replica files and their reserved space.
#[derive(Clone)]
pub struct ReplicaCatalog {
    database: Arc<redb::Database>,
    pool: ReplicaPool,
}

impl ReplicaCatalog {
    /// Opens the local tables, binds them to the checked pool, and verifies
    /// every saved space total before callers start storage services.
    pub fn open(database: Arc<redb::Database>, pool: ReplicaPool) -> Result<Self, CatalogError> {
        let write = database.begin_write()?;
        let replicas = write.open_table(REPLICAS)?;
        let attachments = write.open_table(ATTACHMENTS)?;
        let retirements = write.open_table(RETIREMENTS)?;
        let mut pools = write.open_table(POOL)?;

        let stored_pool = if let Some(stored) = pools.get(POOL_KEY)? {
            let pool_record = decode_pool(stored.value())?;
            if !pool_record.matches(&pool) {
                return Err(CatalogError::PoolChanged);
            }
            pool_record
        } else {
            StoredPool::new(&pool)
        };

        let totals = checked_totals(&replicas)?;
        if totals.data_bytes != stored_pool.data_bytes
            || totals.metadata_bytes != stored_pool.metadata_bytes
        {
            return Err(CatalogError::SpaceTotalsMismatch);
        }
        check_saved_pool_space(&stored_pool)?;
        check_attachment_rows(&replicas, &attachments)?;
        check_retirement_rows(&replicas, &retirements)?;

        if pools.get(POOL_KEY)?.is_none() {
            let encoded = encode_pool(&stored_pool)?;
            pools.insert(POOL_KEY, encoded.as_slice())?;
        }
        drop(replicas);
        drop(attachments);
        drop(retirements);
        drop(pools);
        write.commit()?;

        Ok(Self { database, pool })
    }

    /// Returns the checked root used to build replica paths.
    #[must_use]
    pub fn pool_root(&self) -> &std::path::Path {
        self.pool.root()
    }

    /// Holds one replica without a catalog limit for internal storage tests.
    #[cfg(test)]
    fn reserve_replica(
        &self,
        descriptor: VolumeDescriptor,
        origin: LocalReplicaOrigin,
    ) -> Result<ReplicaRecord, CatalogError> {
        self.reserve_replica_inner(descriptor, origin, None)
    }

    /// Reserves one replica without allowing the durable catalog to exceed its limit.
    pub fn reserve_replica_bounded(
        &self,
        descriptor: VolumeDescriptor,
        origin: LocalReplicaOrigin,
        maximum: usize,
    ) -> Result<ReplicaRecord, CatalogError> {
        self.reserve_replica_inner(descriptor, origin, Some(maximum))
    }

    /// Performs one idempotent reservation and checks the optional admission limit atomically.
    fn reserve_replica_inner(
        &self,
        descriptor: VolumeDescriptor,
        origin: LocalReplicaOrigin,
        maximum: Option<usize>,
    ) -> Result<ReplicaRecord, CatalogError> {
        let key = ReplicaKey::from(&descriptor);
        let key_bytes = encode_key(key);
        let retirement_key = encode_volume_key(key.volume_id());
        let calculated = ReplicaSpace::for_capacity(descriptor.capacity().bytes())?;
        let reserved = ReservedSpace::new(calculated.data_bytes(), calculated.metadata_bytes());
        let directory_name = directory_name(key);
        let write = self.database.begin_write()?;
        let mut replicas = write.open_table(REPLICAS)?;
        let mut retirements = write.open_table(RETIREMENTS)?;
        let mut pools = write.open_table(POOL)?;

        let retirement = retirements
            .get(retirement_key.as_slice())?
            .map(|value| decode_retirement_checked(retirement_key.as_slice(), value.value()))
            .transpose()?;
        if retirement.is_some_and(|retirement| {
            retirement.key().generation() > key.generation()
                || (retirement.key() == key && matches!(&origin, LocalReplicaOrigin::Bootstrap(_)))
        }) {
            return Err(CatalogError::ReplicaRetired);
        }

        let existing = replicas
            .get(key_bytes.as_slice())?
            .map(|stored| decode_checked(key_bytes.as_slice(), stored.value()))
            .transpose()?;
        if let Some(record) = existing {
            if record.descriptor() != &descriptor || record.origin() != &origin {
                return Err(CatalogError::ConflictingReplica);
            }
            if retirement.is_some() {
                retirements.remove(retirement_key.as_slice())?;
                drop(replicas);
                drop(retirements);
                drop(pools);
                write.commit()?;
            }
            return Ok(record);
        }
        if let Some(maximum) = maximum {
            let current = replicas
                .len()?
                .checked_add(retirements.len()?)
                .ok_or(CatalogError::ReplicaCountOverflow)?;
            let actual = current
                .checked_add(u64::from(retirement.is_none()))
                .ok_or(CatalogError::ReplicaCountOverflow)?;
            if actual > u64::try_from(maximum).unwrap_or(u64::MAX) {
                return Err(CatalogError::TooManyReplicaSlots { actual, maximum });
            }
        }
        let mut pool = read_pool(&pools)?;
        let new_data = pool
            .data_bytes
            .checked_add(reserved.data_bytes())
            .ok_or(CatalogError::SpaceOverflow)?;
        let new_metadata = pool
            .metadata_bytes
            .checked_add(reserved.metadata_bytes())
            .ok_or(CatalogError::SpaceOverflow)?;
        let requested_bytes = calculated.total_bytes()?;
        let available_bytes = self.pool.available_bytes()?;
        if requested_bytes > available_bytes {
            return Err(CatalogError::NotEnoughFreeSpace {
                available_bytes,
                required_bytes: requested_bytes,
            });
        }
        check_new_replica_space(&pool, new_data, new_metadata)?;

        let record = ReplicaRecord::new(descriptor, origin, directory_name, reserved);
        let encoded_record = encode_replica(&record)?;
        replicas.insert(key_bytes.as_slice(), encoded_record.as_slice())?;
        if retirement.is_some() {
            retirements.remove(retirement_key.as_slice())?;
        }

        pool.data_bytes = new_data;
        pool.metadata_bytes = new_metadata;
        write_pool(&mut pools, &pool)?;
        drop(replicas);
        drop(retirements);
        drop(pools);
        write.commit()?;
        Ok(record)
    }

    /// Returns the number of durable replicas without decoding or opening them.
    pub fn replica_count(&self) -> Result<u64, CatalogError> {
        let read = self.database.begin_read()?;
        let replicas = read.open_table(REPLICAS)?;
        replicas.len().map_err(CatalogError::from)
    }

    /// Counts files and durable retirement proofs against one shared admission bound.
    pub fn replica_slot_count(&self) -> Result<u64, CatalogError> {
        let read = self.database.begin_read()?;
        let replicas = read.open_table(REPLICAS)?;
        let retirements = read.open_table(RETIREMENTS)?;
        replicas
            .len()?
            .checked_add(retirements.len()?)
            .ok_or(CatalogError::ReplicaCountOverflow)
    }

    /// Rejects startup when durable replica ownership exceeds the configured bound.
    pub fn check_replica_slot_limit(&self, maximum: usize) -> Result<(), CatalogError> {
        let actual = self.replica_slot_count()?;
        if actual > u64::try_from(maximum).unwrap_or(u64::MAX) {
            return Err(CatalogError::TooManyReplicaSlots { actual, maximum });
        }
        Ok(())
    }

    /// Returns one saved replica without consulting cluster or CRDT state.
    pub fn replica(&self, key: ReplicaKey) -> Result<Option<ReplicaRecord>, CatalogError> {
        let key_bytes = encode_key(key);
        let read = self.database.begin_read()?;
        let replicas = read.open_table(REPLICAS)?;
        let Some(stored) = replicas.get(key_bytes.as_slice())? else {
            return Ok(None);
        };
        Ok(Some(decode_checked(key_bytes.as_slice(), stored.value())?))
    }

    /// Holds complete pool space before this node extends its local replica file.
    pub fn reserve_replica_capacity(
        &self,
        key: ReplicaKey,
        target: VolumeCapacity,
    ) -> Result<ReplicaRecord, CatalogError> {
        self.update_replica(key, |record, pool| {
            if target < record.descriptor().capacity() {
                return Err(CatalogError::ReservationBelowAppliedCapacity);
            }
            if target.bytes() <= record.reserved_space().data_bytes() {
                return Ok(record.clone());
            }

            let requested = ReplicaSpace::for_capacity(target.bytes())?;
            let current = record.reserved_space();
            let added_data = requested
                .data_bytes()
                .checked_sub(current.data_bytes())
                .ok_or(CatalogError::SpaceOverflow)?;
            let added_metadata = requested
                .metadata_bytes()
                .checked_sub(current.metadata_bytes())
                .ok_or(CatalogError::SpaceOverflow)?;
            let added_bytes = added_data
                .checked_add(added_metadata)
                .ok_or(CatalogError::SpaceOverflow)?;
            let available_bytes = self.pool.available_bytes()?;
            if added_bytes > available_bytes {
                return Err(CatalogError::NotEnoughFreeSpace {
                    available_bytes,
                    required_bytes: added_bytes,
                });
            }

            let new_data = pool
                .data_bytes
                .checked_add(added_data)
                .ok_or(CatalogError::SpaceOverflow)?;
            let new_metadata = pool
                .metadata_bytes
                .checked_add(added_metadata)
                .ok_or(CatalogError::SpaceOverflow)?;
            check_new_replica_space(pool, new_data, new_metadata)?;
            pool.data_bytes = new_data;
            pool.metadata_bytes = new_metadata;
            record.set_reserved_space(ReservedSpace::new(
                requested.data_bytes(),
                requested.metadata_bytes(),
            ));
            Ok(record.clone())
        })
    }

    /// Releases only reservation above both the requested and applied capacities.
    pub fn reduce_replica_reservation(
        &self,
        key: ReplicaKey,
        target: VolumeCapacity,
    ) -> Result<ReplicaRecord, CatalogError> {
        self.update_replica(key, |record, pool| {
            if target < record.descriptor().capacity() {
                return Err(CatalogError::ReservationBelowAppliedCapacity);
            }
            if target.bytes() >= record.reserved_space().data_bytes() {
                return Ok(record.clone());
            }

            let requested = ReplicaSpace::for_capacity(target.bytes())?;
            let current = record.reserved_space();
            pool.data_bytes = pool
                .data_bytes
                .checked_sub(current.data_bytes() - requested.data_bytes())
                .ok_or(CatalogError::SpaceTotalsMismatch)?;
            pool.metadata_bytes = pool
                .metadata_bytes
                .checked_sub(current.metadata_bytes() - requested.metadata_bytes())
                .ok_or(CatalogError::SpaceTotalsMismatch)?;
            record.set_reserved_space(ReservedSpace::new(
                requested.data_bytes(),
                requested.metadata_bytes(),
            ));
            Ok(record.clone())
        })
    }

    /// Records a capacity already committed by Raft after local files cover it.
    pub fn apply_replica_capacity(
        &self,
        key: ReplicaKey,
        target: VolumeCapacity,
    ) -> Result<ReplicaRecord, CatalogError> {
        self.update_replica(key, |record, _pool| {
            let current = record.descriptor().capacity();
            if target < current {
                return Err(CatalogError::CapacityCannotShrink);
            }
            if target.bytes() > record.reserved_space().data_bytes() {
                return Err(CatalogError::CapacityExceedsReservation);
            }
            if target == current {
                return Ok(record.clone());
            }
            let descriptor = record.descriptor().with_capacity(target)?;
            record.set_descriptor(descriptor);
            Ok(record.clone())
        })
    }

    /// Reassigns one excluded local slot before its complete replacement rebuild.
    ///
    /// The caller must first prove from locally applied Raft state that this node
    /// is the inactive replacement target. This atomic change makes a crash leave
    /// the row in `Preparing`, so startup recreates the file instead of admitting
    /// its former contents.
    pub fn begin_replica_replacement(
        &self,
        key: ReplicaKey,
        descriptor: VolumeDescriptor,
        origin: LocalReplicaOrigin,
    ) -> Result<ReplicaRecord, CatalogError> {
        if !matches!(origin, LocalReplicaOrigin::Replacement { .. }) {
            return Err(CatalogError::ConflictingReplica);
        }
        let key_bytes = encode_key(key);
        let write = self.database.begin_write()?;
        let mut replicas = write.open_table(REPLICAS)?;
        let attachments = write.open_table(ATTACHMENTS)?;
        let mut record = {
            let stored = replicas
                .get(key_bytes.as_slice())?
                .ok_or(CatalogError::ReplicaNotFound)?;
            decode_checked(key_bytes.as_slice(), stored.value())?
        };
        if ReplicaKey::from(&descriptor) != key
            || !record.descriptor().has_same_storage_identity(&descriptor)
            || record.descriptor().capacity() > descriptor.capacity()
            || descriptor.capacity().bytes() > record.reserved_space().data_bytes()
            || !matches!(
                record.state(),
                ReplicaState::Preparing | ReplicaState::Ready
            )
        {
            return Err(CatalogError::ConflictingReplica);
        }
        if attachments.get(key_bytes.as_slice())?.is_some() {
            return Err(CatalogError::ReplicaStillAttached);
        }

        record.set_descriptor(descriptor);
        record.set_origin(origin);
        record.set_state(ReplicaState::Preparing);
        record.set_health(ReplicaHealth::Healthy);
        record.clear_filesystem_format();
        let encoded = encode_replica(&record)?;
        replicas.insert(key_bytes.as_slice(), encoded.as_slice())?;
        drop(replicas);
        drop(attachments);
        write.commit()?;
        Ok(record)
    }

    /// Returns durable proof that current control state removed this node's live copy.
    pub fn retirement(
        &self,
        key: ReplicaKey,
    ) -> Result<Option<LocalReplicaRetirement>, CatalogError> {
        let table_key = encode_volume_key(key.volume_id());
        let read = self.database.begin_read()?;
        let retirements = read.open_table(RETIREMENTS)?;
        let Some(stored) = retirements.get(table_key.as_slice())? else {
            return Ok(None);
        };
        let retirement = decode_retirement_checked(table_key.as_slice(), stored.value())?;
        Ok((retirement.key() == key).then_some(retirement))
    }

    /// Removes proof for an older generation superseded by current desired identity.
    pub fn forget_retirement_before(&self, current: ReplicaKey) -> Result<(), CatalogError> {
        let table_key = encode_volume_key(current.volume_id());
        let write = self.database.begin_write()?;
        let mut retirements = write.open_table(RETIREMENTS)?;
        let saved = retirements
            .get(table_key.as_slice())?
            .map(|value| decode_retirement_checked(table_key.as_slice(), value.value()))
            .transpose()?;
        if saved.is_none_or(|retirement| retirement.key().generation() >= current.generation()) {
            return Ok(());
        }
        retirements.remove(table_key.as_slice())?;
        drop(retirements);
        write.commit()?;
        Ok(())
    }

    /// Creates or reopens the one durable attachment session for a generation.
    pub fn open_or_create_attachment(
        &self,
        descriptor: VolumeDescriptor,
        session_id: DriverSessionId,
    ) -> Result<LocalAttachmentRecord, CatalogError> {
        let key = ReplicaKey::from(&descriptor);
        let key_bytes = encode_key(key);
        let write = self.database.begin_write()?;
        let replicas = write.open_table(REPLICAS)?;
        let mut attachments = write.open_table(ATTACHMENTS)?;
        let replica = {
            let stored = replicas
                .get(key_bytes.as_slice())?
                .ok_or(CatalogError::ReplicaNotFound)?;
            decode_checked(key_bytes.as_slice(), stored.value())?
        };
        if !replica.descriptor().has_same_storage_identity(&descriptor)
            || descriptor.capacity() > replica.descriptor().capacity()
        {
            return Err(CatalogError::ConflictingReplica);
        }
        let existing = {
            let stored = attachments.get(key_bytes.as_slice())?;
            stored
                .map(|value| decode_attachment_checked(key_bytes.as_slice(), value.value()))
                .transpose()?
        };
        if let Some(record) = existing {
            if !record.descriptor().has_same_storage_identity(&descriptor)
                || record.descriptor().capacity() > replica.descriptor().capacity()
                || record.session_id() != session_id
            {
                return Err(CatalogError::AttachmentChanged);
            }
            return Ok(record);
        }
        let record = LocalAttachmentRecord::new(descriptor, session_id);
        let encoded = encode_attachment(&record)?;
        attachments.insert(key_bytes.as_slice(), encoded.as_slice())?;
        drop(replicas);
        drop(attachments);
        write.commit()?;
        Ok(record)
    }

    /// Returns one durable attachment without consulting public cluster state.
    pub fn attachment(
        &self,
        key: ReplicaKey,
    ) -> Result<Option<LocalAttachmentRecord>, CatalogError> {
        let key_bytes = encode_key(key);
        let read = self.database.begin_read()?;
        let attachments = read.open_table(ATTACHMENTS)?;
        let Some(stored) = attachments.get(key_bytes.as_slice())? else {
            return Ok(None);
        };
        Ok(Some(decode_attachment_checked(
            key_bytes.as_slice(),
            stored.value(),
        )?))
    }

    /// Loads every unfinished local attachment for restart reconciliation.
    pub fn discover_attachments(
        &self,
        maximum: usize,
    ) -> Result<Vec<LocalAttachmentRecord>, CatalogError> {
        let read = self.database.begin_read()?;
        let attachments = read.open_table(ATTACHMENTS)?;
        let count = attachments.len()?;
        check_discovery_count(count, maximum, "attachments")?;
        let capacity = usize::try_from(count).map_err(|_| CatalogError::TooManyAttachments {
            actual: count,
            maximum,
        })?;
        let mut records = Vec::with_capacity(capacity);
        for row in attachments.iter()? {
            let (key, stored) = row?;
            records.push(decode_attachment_checked(key.value(), stored.value())?);
        }
        Ok(records)
    }

    /// Atomically records monotonic cleanup intent and advances any saved mount.
    pub fn begin_attachment_detach(&self, key: ReplicaKey) -> Result<(), CatalogError> {
        let key_bytes = encode_key(key);
        let write = self.database.begin_write()?;
        let mut attachments = write.open_table(ATTACHMENTS)?;
        let Some(mut record) = ({
            let stored = attachments.get(key_bytes.as_slice())?;
            stored
                .map(|value| decode_attachment_checked(key_bytes.as_slice(), value.value()))
                .transpose()?
        }) else {
            return Ok(());
        };
        record.begin_detach();
        if let Some(mount) = record.volume_mount().cloned() {
            let unmounting = mount
                .with_state(SavedMountState::Unmounting)
                .map_err(|_| CatalogError::VolumeMountChanged)?;
            record.set_volume_mount(unmounting);
        }
        let encoded = encode_attachment(&record)?;
        attachments.insert(key_bytes.as_slice(), encoded.as_slice())?;
        drop(attachments);
        write.commit()?;
        Ok(())
    }

    /// Saves the exact committed fence granted to the durable session.
    pub fn save_attachment_fence(
        &self,
        key: ReplicaKey,
        session_id: DriverSessionId,
        fence: FenceEpoch,
    ) -> Result<(), CatalogError> {
        self.update_attachment(key, |record| {
            if record.session_id() != session_id {
                return Err(CatalogError::AttachmentChanged);
            }
            match record.granted_fence() {
                Some(saved) if saved == fence => Ok(()),
                Some(_) => Err(CatalogError::AttachmentFenceChanged),
                None => {
                    record.set_granted_fence(fence);
                    Ok(())
                }
            }
        })
    }

    /// Atomically advances one session and all of its saved local resources.
    pub fn advance_attachment_fence(
        &self,
        key: ReplicaKey,
        session_id: DriverSessionId,
        expected: FenceEpoch,
        next: FenceEpoch,
    ) -> Result<(), CatalogError> {
        if next <= expected {
            return Err(CatalogError::AttachmentFenceChanged);
        }
        self.update_attachment(key, |record| {
            if record.session_id() != session_id {
                return Err(CatalogError::AttachmentChanged);
            }
            let current = record
                .granted_fence()
                .ok_or(CatalogError::AttachmentFenceChanged)?;
            if current == next {
                return check_attachment_resource_fence(record, next);
            }
            if current != expected {
                return Err(CatalogError::AttachmentFenceChanged);
            }
            check_attachment_resource_fence(record, expected)?;
            record.set_granted_fence(next);
            record.set_ublk_device_fence(next);
            if let Some(volume_mount) = record.volume_mount().cloned() {
                record.set_volume_mount(volume_mount.with_fence(next));
            }
            Ok(())
        })
    }

    /// Records a larger descriptor after device mapper proves it active.
    pub fn advance_attachment_capacity(
        &self,
        key: ReplicaKey,
        target: VolumeDescriptor,
    ) -> Result<(), CatalogError> {
        let key_bytes = encode_key(key);
        let write = self.database.begin_write()?;
        let replicas = write.open_table(REPLICAS)?;
        let mut attachments = write.open_table(ATTACHMENTS)?;
        let replica = {
            let stored = replicas
                .get(key_bytes.as_slice())?
                .ok_or(CatalogError::ReplicaNotFound)?;
            decode_checked(key_bytes.as_slice(), stored.value())?
        };
        let mut attachment = {
            let stored = attachments
                .get(key_bytes.as_slice())?
                .ok_or(CatalogError::AttachmentNotFound)?;
            decode_attachment_checked(key_bytes.as_slice(), stored.value())?
        };
        if !attachment.descriptor().has_same_storage_identity(&target)
            || !replica.descriptor().has_same_storage_identity(&target)
        {
            return Err(CatalogError::AttachmentChanged);
        }
        if target.capacity() < attachment.descriptor().capacity() {
            return Err(CatalogError::CapacityCannotShrink);
        }
        if target.capacity() > replica.descriptor().capacity() {
            return Err(CatalogError::CapacityExceedsReservation);
        }
        if target.capacity() > attachment.descriptor().capacity()
            && !attachment
                .ublk_devices()
                .iter()
                .any(|device| device.capacity() == target.capacity())
        {
            return Err(CatalogError::UblkDeviceChanged);
        }
        attachment.set_descriptor(target);
        let encoded = encode_attachment(&attachment)?;
        attachments.insert(key_bytes.as_slice(), encoded.as_slice())?;
        drop(replicas);
        drop(attachments);
        write.commit()?;
        Ok(())
    }

    /// Saves one active ublk device without replacing a different device.
    pub fn save_ublk_device(
        &self,
        key: ReplicaKey,
        device: SavedUblkDevice,
    ) -> Result<(), CatalogError> {
        let replica = self.replica(key)?.ok_or(CatalogError::ReplicaNotFound)?;
        replica.descriptor().with_capacity(device.capacity())?;
        if device.capacity() > replica.descriptor().capacity() {
            return Err(CatalogError::CapacityExceedsReservation);
        }
        self.update_attachment(key, |record| {
            check_device_fence(record, device)?;
            if record.ublk_devices().contains(&device) {
                return Ok(());
            }
            if record.ublk_devices().len() >= 2
                || record
                    .ublk_devices()
                    .iter()
                    .any(|saved| saved.id() == device.id() || saved.capacity() == device.capacity())
            {
                return Err(CatalogError::UblkDeviceChanged);
            }
            record.add_ublk_device(device);
            Ok(())
        })
    }

    /// Replaces one exact saved device after the old kernel device disappeared.
    pub fn replace_ublk_device(
        &self,
        key: ReplicaKey,
        expected: SavedUblkDevice,
        device: SavedUblkDevice,
    ) -> Result<(), CatalogError> {
        let replica = self.replica(key)?.ok_or(CatalogError::ReplicaNotFound)?;
        replica.descriptor().with_capacity(device.capacity())?;
        if device.capacity() > replica.descriptor().capacity() {
            return Err(CatalogError::CapacityExceedsReservation);
        }
        self.update_attachment(key, |record| {
            check_device_fence(record, device)?;
            if record.ublk_devices().contains(&device) {
                return Ok(());
            }
            if !record.ublk_devices().contains(&expected)
                || record.ublk_devices().iter().any(|saved| {
                    *saved != expected
                        && (saved.id() == device.id() || saved.capacity() == device.capacity())
                })
            {
                return Err(CatalogError::UblkDeviceChanged);
            }
            if record.volume_mount().is_some_and(|volume_mount| {
                device.fence() != volume_mount.fence()
                    || device.session_id() != volume_mount.session_id()
            }) {
                return Err(CatalogError::VolumeMountDeviceMismatch);
            }
            record.replace_ublk_device(expected, device);
            Ok(())
        })
    }

    /// Clears one exact saved device after it is no longer in the kernel.
    pub fn clear_ublk_device(
        &self,
        key: ReplicaKey,
        expected: SavedUblkDevice,
    ) -> Result<(), CatalogError> {
        self.update_attachment(key, |record| {
            if !record.ublk_devices().contains(&expected) {
                return if record.ublk_devices().is_empty() {
                    Ok(())
                } else {
                    Err(CatalogError::UblkDeviceChanged)
                };
            }
            if record.volume_mount().is_some()
                && (record.ublk_devices().len() == 1
                    || expected.capacity() == record.descriptor().capacity())
            {
                return Err(CatalogError::VolumeMountDeviceMismatch);
            }
            record.remove_ublk_device(expected);
            Ok(())
        })
    }

    /// Saves one unfinished ext4 format independently of transient kernel state.
    pub fn save_filesystem_format(
        &self,
        key: ReplicaKey,
        format: SavedFilesystemFormat,
    ) -> Result<(), CatalogError> {
        self.update_replica(key, |record, _pool| match record.filesystem_format() {
            Some(saved) if saved == format => Ok(()),
            Some(_) => Err(CatalogError::FilesystemFormatChanged),
            None => {
                record.set_filesystem_format(format);
                Ok(())
            }
        })
    }

    /// Clears one exact format after ext4 and its committed state are complete.
    pub fn clear_filesystem_format(
        &self,
        key: ReplicaKey,
        expected: SavedFilesystemFormat,
    ) -> Result<(), CatalogError> {
        self.update_replica(key, |record, _pool| match record.filesystem_format() {
            None => Ok(()),
            Some(saved) if saved == expected => {
                record.clear_filesystem_format();
                Ok(())
            }
            Some(_) => Err(CatalogError::FilesystemFormatChanged),
        })
    }

    /// Saves one ext4 mount only for the matching durable attachment.
    pub fn save_volume_mount(
        &self,
        key: ReplicaKey,
        volume_mount: SavedVolumeMount,
    ) -> Result<(), CatalogError> {
        self.update_attachment(key, |record| {
            if volume_mount.filesystem_expanded_to_bytes() > record.descriptor().capacity().bytes()
            {
                return Err(CatalogError::FilesystemCapacityExceedsDevice);
            }
            if !record.ublk_devices().iter().any(|device| {
                device.fence() == volume_mount.fence()
                    && device.session_id() == volume_mount.session_id()
            }) {
                return Err(CatalogError::VolumeMountDeviceMismatch);
            }
            match record.volume_mount() {
                Some(saved) if saved == &volume_mount => Ok(()),
                Some(_) => Err(CatalogError::VolumeMountChanged),
                None => {
                    if volume_mount.state() != SavedMountState::Mounting {
                        return Err(CatalogError::VolumeMountChanged);
                    }
                    record.set_volume_mount(volume_mount);
                    Ok(())
                }
            }
        })
    }

    /// Replaces one exact saved mount after its next local step completes.
    pub fn replace_volume_mount(
        &self,
        key: ReplicaKey,
        expected: &SavedVolumeMount,
        volume_mount: SavedVolumeMount,
    ) -> Result<(), CatalogError> {
        self.update_attachment(key, |record| {
            if record.volume_mount() != Some(expected) {
                return Err(CatalogError::VolumeMountChanged);
            }
            if !expected.can_advance_to(&volume_mount) {
                return Err(CatalogError::VolumeMountChanged);
            }
            if volume_mount.filesystem_expanded_to_bytes() > record.descriptor().capacity().bytes()
            {
                return Err(CatalogError::FilesystemCapacityExceedsDevice);
            }
            if !record.ublk_devices().iter().any(|device| {
                device.fence() == volume_mount.fence()
                    && device.session_id() == volume_mount.session_id()
            }) {
                return Err(CatalogError::VolumeMountDeviceMismatch);
            }
            if volume_mount.state() == SavedMountState::Unmounting {
                record.begin_detach();
            }
            record.set_volume_mount(volume_mount);
            Ok(())
        })
    }

    /// Clears one exact saved mount after the kernel mount was removed.
    pub fn clear_volume_mount(
        &self,
        key: ReplicaKey,
        expected: &SavedVolumeMount,
    ) -> Result<(), CatalogError> {
        self.update_attachment(key, |record| match record.volume_mount() {
            None => Ok(()),
            Some(saved) if saved == expected => {
                record.clear_volume_mount();
                Ok(())
            }
            Some(_) => Err(CatalogError::VolumeMountChanged),
        })
    }

    /// Removes an empty attachment after its mount and device are gone.
    pub fn remove_attachment(
        &self,
        key: ReplicaKey,
        session_id: DriverSessionId,
    ) -> Result<(), CatalogError> {
        let key_bytes = encode_key(key);
        let write = self.database.begin_write()?;
        let mut attachments = write.open_table(ATTACHMENTS)?;
        let Some(record) = ({
            let stored = attachments.get(key_bytes.as_slice())?;
            stored
                .map(|value| decode_attachment_checked(key_bytes.as_slice(), value.value()))
                .transpose()?
        }) else {
            return Ok(());
        };
        if record.session_id() != session_id {
            return Err(CatalogError::AttachmentChanged);
        }
        if !record.ublk_devices().is_empty() || record.volume_mount().is_some() {
            return Err(CatalogError::AttachmentStillActive);
        }
        attachments.remove(key_bytes.as_slice())?;
        drop(attachments);
        write.commit()?;
        Ok(())
    }

    /// Loads every local replica before the storage listener starts.
    pub fn discover_replicas(&self, maximum: usize) -> Result<Vec<ReplicaRecord>, CatalogError> {
        let read = self.database.begin_read()?;
        let replicas = read.open_table(REPLICAS)?;
        let count = replicas.len()?;
        if count > u64::try_from(maximum).unwrap_or(u64::MAX) {
            return Err(CatalogError::TooManyReplicas {
                actual: count,
                maximum,
            });
        }
        let capacity = usize::try_from(count).map_err(|_| CatalogError::TooManyReplicas {
            actual: count,
            maximum,
        })?;
        let mut records = Vec::with_capacity(capacity);
        for row in replicas.iter()? {
            let (key, stored) = row?;
            records.push(decode_checked(key.value(), stored.value())?);
        }
        Ok(records)
    }

    /// Stores one allowed node-local file-state change.
    pub fn set_replica_state(
        &self,
        key: ReplicaKey,
        requested: ReplicaState,
    ) -> Result<(), CatalogError> {
        self.update_replica(key, |record, _pool| {
            let current = record.state();
            if !current.can_change_to(requested) {
                return Err(CatalogError::InvalidStateChange { current, requested });
            }
            record.set_state(requested);
            Ok(())
        })
    }

    /// Stores one local file-health level when it differs from the current row.
    pub fn set_replica_health(
        &self,
        key: ReplicaKey,
        requested: ReplicaHealth,
    ) -> Result<(), CatalogError> {
        let Some(current) = self.replica(key)? else {
            return Err(CatalogError::ReplicaNotFound);
        };
        if current.health() == requested {
            return Ok(());
        }
        self.update_replica(key, |record, _pool| {
            record.set_health(requested);
            Ok(())
        })
    }

    /// Removes a deleting replica and releases its reservation idempotently.
    ///
    /// Desired-generation admission, rather than local history, rejects stale
    /// bootstrap and activation work. An absent exact key therefore proves
    /// this local effect complete without retaining one row per generation.
    pub fn remove_deleted_replica(&self, key: ReplicaKey) -> Result<(), CatalogError> {
        let key_bytes = encode_key(key);
        let retirement_key = encode_volume_key(key.volume_id());
        let write = self.database.begin_write()?;
        let mut replicas = write.open_table(REPLICAS)?;
        let attachments = write.open_table(ATTACHMENTS)?;
        let mut retirements = write.open_table(RETIREMENTS)?;
        let mut pools = write.open_table(POOL)?;
        let retirement = retirements
            .get(retirement_key.as_slice())?
            .map(|value| decode_retirement_checked(retirement_key.as_slice(), value.value()))
            .transpose()?;
        let record = if let Some(stored) = replicas.get(key_bytes.as_slice())? {
            decode_checked(key_bytes.as_slice(), stored.value())?
        } else if retirement.is_some_and(|saved| saved.key().generation() <= key.generation()) {
            retirements.remove(retirement_key.as_slice())?;
            drop(replicas);
            drop(attachments);
            drop(retirements);
            drop(pools);
            write.commit()?;
            return Ok(());
        } else {
            return Ok(());
        };
        if record.state() != ReplicaState::Deleting {
            return Err(CatalogError::ReplicaNotDeleting);
        }
        if attachments.get(key_bytes.as_slice())?.is_some() || record.filesystem_format().is_some()
        {
            return Err(CatalogError::ReplicaStillAttached);
        }

        let mut pool = read_pool(&pools)?;
        pool.data_bytes = pool
            .data_bytes
            .checked_sub(record.reserved_space().data_bytes())
            .ok_or(CatalogError::SpaceTotalsMismatch)?;
        pool.metadata_bytes = pool
            .metadata_bytes
            .checked_sub(record.reserved_space().metadata_bytes())
            .ok_or(CatalogError::SpaceTotalsMismatch)?;
        replicas.remove(key_bytes.as_slice())?;
        if retirement.is_some_and(|saved| saved.key().generation() <= key.generation()) {
            retirements.remove(retirement_key.as_slice())?;
        }
        write_pool(&mut pools, &pool)?;
        drop(replicas);
        drop(attachments);
        drop(retirements);
        drop(pools);
        write.commit()?;
        Ok(())
    }

    /// Replaces one former-member file slot with durable bootstrap suppression.
    pub fn remove_retired_replica(&self, key: ReplicaKey) -> Result<(), CatalogError> {
        let key_bytes = encode_key(key);
        let retirement_key = encode_volume_key(key.volume_id());
        let write = self.database.begin_write()?;
        let mut replicas = write.open_table(REPLICAS)?;
        let attachments = write.open_table(ATTACHMENTS)?;
        let mut retirements = write.open_table(RETIREMENTS)?;
        let mut pools = write.open_table(POOL)?;
        let retirement = retirements
            .get(retirement_key.as_slice())?
            .map(|value| decode_retirement_checked(retirement_key.as_slice(), value.value()))
            .transpose()?;
        let replica = replicas
            .get(key_bytes.as_slice())?
            .map(|stored| decode_checked(key_bytes.as_slice(), stored.value()))
            .transpose()?;
        if retirement.is_some_and(|saved| saved.key().generation() >= key.generation()) {
            if replica.is_none() {
                return Ok(());
            }
            return Err(CatalogError::ConflictingReplica);
        }
        let record = replica.ok_or(CatalogError::ReplicaNotFound)?;
        if record.state() != ReplicaState::Retiring {
            return Err(CatalogError::ReplicaNotRetiring);
        }
        if attachments.get(key_bytes.as_slice())?.is_some() || record.filesystem_format().is_some()
        {
            return Err(CatalogError::ReplicaStillAttached);
        }
        let mut pool = read_pool(&pools)?;
        pool.data_bytes = pool
            .data_bytes
            .checked_sub(record.reserved_space().data_bytes())
            .ok_or(CatalogError::SpaceTotalsMismatch)?;
        pool.metadata_bytes = pool
            .metadata_bytes
            .checked_sub(record.reserved_space().metadata_bytes())
            .ok_or(CatalogError::SpaceTotalsMismatch)?;
        replicas.remove(key_bytes.as_slice())?;
        let retirement = LocalReplicaRetirement::new(key);
        let encoded = encode_retirement(retirement)?;
        retirements.insert(retirement_key.as_slice(), encoded.as_slice())?;
        write_pool(&mut pools, &pool)?;
        drop(replicas);
        drop(attachments);
        drop(retirements);
        drop(pools);
        write.commit()?;
        Ok(())
    }

    /// Records bootstrap suppression when an already-excluded copy has no full row left.
    pub fn record_missing_replica_retirement(
        &self,
        key: ReplicaKey,
        maximum: usize,
    ) -> Result<(), CatalogError> {
        let key_bytes = encode_key(key);
        let retirement_key = encode_volume_key(key.volume_id());
        let write = self.database.begin_write()?;
        let replicas = write.open_table(REPLICAS)?;
        let attachments = write.open_table(ATTACHMENTS)?;
        let mut retirements = write.open_table(RETIREMENTS)?;
        if replicas.get(key_bytes.as_slice())?.is_some() {
            return Err(CatalogError::ConflictingReplica);
        }
        if attachments.get(key_bytes.as_slice())?.is_some() {
            return Err(CatalogError::ReplicaStillAttached);
        }
        let saved = retirements
            .get(retirement_key.as_slice())?
            .map(|value| decode_retirement_checked(retirement_key.as_slice(), value.value()))
            .transpose()?;
        if saved.is_some_and(|saved| saved.key().generation() >= key.generation()) {
            return Ok(());
        }
        let current = replicas
            .len()?
            .checked_add(retirements.len()?)
            .ok_or(CatalogError::ReplicaCountOverflow)?;
        let actual = current
            .checked_add(u64::from(saved.is_none()))
            .ok_or(CatalogError::ReplicaCountOverflow)?;
        if actual > u64::try_from(maximum).unwrap_or(u64::MAX) {
            return Err(CatalogError::TooManyReplicaSlots { actual, maximum });
        }
        let encoded = encode_retirement(LocalReplicaRetirement::new(key))?;
        retirements.insert(retirement_key.as_slice(), encoded.as_slice())?;
        drop(replicas);
        drop(attachments);
        drop(retirements);
        write.commit()?;
        Ok(())
    }

    /// Combines durable reservations with current filesystem free space.
    pub fn pool_status(&self) -> Result<PoolStatus, CatalogError> {
        let read = self.database.begin_read()?;
        let pools = read.open_table(POOL)?;
        let pool = read_pool(&pools)?;
        let available_bytes = self.pool.available_bytes()?;
        let durable_used = pool
            .data_bytes
            .checked_add(pool.metadata_bytes)
            .ok_or(CatalogError::SpaceOverflow)?;
        let durable_free = pool.managed_bytes.saturating_sub(durable_used);
        let state = if available_bytes < u64::from(self.pool.block_bytes()) {
            PoolSpaceState::DeleteOnly
        } else if durable_free < u64::from(self.pool.block_bytes()) {
            PoolSpaceState::NoNewReplicas
        } else {
            PoolSpaceState::Ready
        };
        Ok(PoolStatus::new(
            state,
            available_bytes,
            pool.data_bytes,
            pool.metadata_bytes,
        ))
    }

    /// Applies one atomic change to an existing replica and the shared pool row.
    fn update_replica<T>(
        &self,
        key: ReplicaKey,
        update: impl FnOnce(&mut ReplicaRecord, &mut StoredPool) -> Result<T, CatalogError>,
    ) -> Result<T, CatalogError> {
        let key_bytes = encode_key(key);
        let write = self.database.begin_write()?;
        let mut replicas = write.open_table(REPLICAS)?;
        let mut pools = write.open_table(POOL)?;
        let mut record = {
            let stored = replicas
                .get(key_bytes.as_slice())?
                .ok_or(CatalogError::ReplicaNotFound)?;
            decode_checked(key_bytes.as_slice(), stored.value())?
        };
        let mut pool = read_pool(&pools)?;
        let result = update(&mut record, &mut pool)?;
        let encoded_record = encode_replica(&record)?;
        replicas.insert(key_bytes.as_slice(), encoded_record.as_slice())?;
        write_pool(&mut pools, &pool)?;
        drop(replicas);
        drop(pools);
        write.commit()?;
        Ok(result)
    }

    /// Applies one atomic exact change to an existing attachment record.
    fn update_attachment<T>(
        &self,
        key: ReplicaKey,
        update: impl FnOnce(&mut LocalAttachmentRecord) -> Result<T, CatalogError>,
    ) -> Result<T, CatalogError> {
        let key_bytes = encode_key(key);
        let write = self.database.begin_write()?;
        let mut attachments = write.open_table(ATTACHMENTS)?;
        let mut record = {
            let stored = attachments
                .get(key_bytes.as_slice())?
                .ok_or(CatalogError::AttachmentNotFound)?;
            decode_attachment_checked(key_bytes.as_slice(), stored.value())?
        };
        let result = update(&mut record)?;
        let encoded = encode_attachment(&record)?;
        attachments.insert(key_bytes.as_slice(), encoded.as_slice())?;
        drop(attachments);
        write.commit()?;
        Ok(result)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Totals {
    data_bytes: u64,
    metadata_bytes: u64,
}

/// Reads every row once and verifies its reservation against the selected format.
fn checked_totals(replicas: &redb::Table<'_, &[u8], &[u8]>) -> Result<Totals, CatalogError> {
    let mut totals = Totals::default();
    for row in replicas.iter()? {
        let (key, stored) = row?;
        let record = decode_checked(key.value(), stored.value())?;
        let applied = ReplicaSpace::for_capacity(record.descriptor().capacity().bytes())?;
        let reserved = ReplicaSpace::for_capacity(record.reserved_space().data_bytes())?;
        if record.reserved_space().data_bytes() < applied.data_bytes()
            || record.reserved_space().metadata_bytes() != reserved.metadata_bytes()
        {
            return Err(CatalogError::SpaceTotalsMismatch);
        }
        totals.data_bytes = totals
            .data_bytes
            .checked_add(record.reserved_space().data_bytes())
            .ok_or(CatalogError::SpaceOverflow)?;
        totals.metadata_bytes = totals
            .metadata_bytes
            .checked_add(record.reserved_space().metadata_bytes())
            .ok_or(CatalogError::SpaceOverflow)?;
    }
    Ok(totals)
}

/// Verifies every attachment belongs to one live local replica.
fn check_attachment_rows(
    replicas: &redb::Table<'_, &[u8], &[u8]>,
    attachments: &redb::Table<'_, &[u8], &[u8]>,
) -> Result<(), CatalogError> {
    for row in attachments.iter()? {
        let (key, stored) = row?;
        let record = decode_attachment_checked(key.value(), stored.value())?;
        let replica = replicas
            .get(key.value())?
            .ok_or(CatalogError::ReplicaNotFound)?;
        let replica = decode_checked(key.value(), replica.value())?;
        if !replica
            .descriptor()
            .has_same_storage_identity(record.descriptor())
            || record.descriptor().capacity() > replica.descriptor().capacity()
        {
            return Err(CatalogError::AttachmentIdentityMismatch);
        }
        for device in record.ublk_devices() {
            replica.descriptor().with_capacity(device.capacity())?;
            if device.capacity() > replica.descriptor().capacity() {
                return Err(CatalogError::AttachmentIdentityMismatch);
            }
        }
        if !record.ublk_devices().is_empty()
            && !record
                .ublk_devices()
                .iter()
                .any(|device| device.capacity() == record.descriptor().capacity())
        {
            return Err(CatalogError::AttachmentIdentityMismatch);
        }
        if record.volume_mount().is_some_and(|mount| {
            mount.filesystem_expanded_to_bytes() > record.descriptor().capacity().bytes()
        }) {
            return Err(CatalogError::AttachmentIdentityMismatch);
        }
    }
    Ok(())
}

/// Verifies retirement keys and their mutual exclusion with full local rows.
fn check_retirement_rows(
    replicas: &redb::Table<'_, &[u8], &[u8]>,
    retirements: &redb::Table<'_, &[u8], &[u8]>,
) -> Result<(), CatalogError> {
    for row in retirements.iter()? {
        let (key, stored) = row?;
        let retirement = decode_retirement_checked(key.value(), stored.value())?;
        let replica_key = encode_key(retirement.key());
        if replicas.get(replica_key.as_slice())?.is_some() {
            return Err(CatalogError::ConflictingReplica);
        }
    }
    Ok(())
}

/// Checks a device against the durable session and committed fence.
fn check_device_fence(
    record: &LocalAttachmentRecord,
    device: SavedUblkDevice,
) -> Result<(), CatalogError> {
    if record.session_id() != device.session_id() || record.granted_fence() != Some(device.fence())
    {
        return Err(CatalogError::AttachmentFenceChanged);
    }
    Ok(())
}

/// Checks every saved local resource against one attachment fence.
fn check_attachment_resource_fence(
    record: &LocalAttachmentRecord,
    fence: FenceEpoch,
) -> Result<(), CatalogError> {
    if record
        .ublk_devices()
        .iter()
        .any(|device| device.session_id() != record.session_id() || device.fence() != fence)
    {
        return Err(CatalogError::AttachmentFenceChanged);
    }
    if record.volume_mount().is_some_and(|volume_mount| {
        volume_mount.session_id() != record.session_id() || volume_mount.fence() != fence
    }) {
        return Err(CatalogError::AttachmentFenceChanged);
    }
    Ok(())
}

/// Applies one bounded discovery limit without allocation surprises.
fn check_discovery_count(
    actual: u64,
    maximum: usize,
    kind: &'static str,
) -> Result<(), CatalogError> {
    if actual > u64::try_from(maximum).unwrap_or(u64::MAX) {
        return match kind {
            "attachments" => Err(CatalogError::TooManyAttachments { actual, maximum }),
            _ => Err(CatalogError::TooManyReplicas { actual, maximum }),
        };
    }
    Ok(())
}

/// Reads the required singleton pool row.
fn read_pool(
    pools: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<StoredPool, CatalogError> {
    let stored = pools
        .get(POOL_KEY)?
        .ok_or(CatalogError::SpaceTotalsMismatch)?;
    Ok(decode_pool(stored.value())?)
}

/// Replaces the complete singleton pool row.
fn write_pool(
    pools: &mut redb::Table<'_, &[u8], &[u8]>,
    pool: &StoredPool,
) -> Result<(), CatalogError> {
    let encoded = encode_pool(pool)?;
    pools.insert(POOL_KEY, encoded.as_slice())?;
    Ok(())
}

/// Checks one new total against the managed pool capacity.
fn check_new_replica_space(
    pool: &StoredPool,
    data_bytes: u64,
    metadata_bytes: u64,
) -> Result<(), CatalogError> {
    let current_bytes = pool
        .data_bytes
        .checked_add(pool.metadata_bytes)
        .ok_or(CatalogError::SpaceOverflow)?;
    let required_bytes = data_bytes
        .checked_add(metadata_bytes)
        .ok_or(CatalogError::SpaceOverflow)?;
    if required_bytes > pool.managed_bytes {
        return Err(CatalogError::NotEnoughSpace {
            available_bytes: pool.managed_bytes.saturating_sub(current_bytes),
            required_bytes: required_bytes.saturating_sub(current_bytes),
        });
    }
    Ok(())
}

/// Checks that every saved promise still fits the durable pool capacity.
fn check_saved_pool_space(pool: &StoredPool) -> Result<(), CatalogError> {
    let required_bytes = pool
        .data_bytes
        .checked_add(pool.metadata_bytes)
        .ok_or(CatalogError::SpaceOverflow)?;
    if required_bytes > pool.managed_bytes {
        return Err(CatalogError::SpaceTotalsMismatch);
    }
    Ok(())
}

/// Checks one stored key against its complete decoded descriptor and path.
fn decode_checked(key: &[u8], bytes: &[u8]) -> Result<ReplicaRecord, CatalogError> {
    let record = decode_replica(bytes)?;
    let expected_key = encode_key(record.key());
    if expected_key != key || record.directory_name() != directory_name(record.key()) {
        return Err(CatalogError::ReplicaIdentityMismatch);
    }
    Ok(record)
}

/// Checks one attachment table key against its complete descriptor.
fn decode_attachment_checked(
    key: &[u8],
    bytes: &[u8],
) -> Result<LocalAttachmentRecord, CatalogError> {
    let record = decode_attachment(bytes)?;
    if encode_key(record.key()) != key {
        return Err(CatalogError::AttachmentIdentityMismatch);
    }
    Ok(record)
}

/// Checks one retirement table key against its encoded generation identity.
fn decode_retirement_checked(
    key: &[u8],
    bytes: &[u8],
) -> Result<LocalReplicaRetirement, CatalogError> {
    let retirement = decode_retirement(bytes)?;
    if encode_volume_key(retirement.key().volume_id()) != key {
        return Err(CatalogError::RetirementIdentityMismatch);
    }
    Ok(retirement)
}

/// Encodes the stable volume identity used to bound retirement proof to one row.
fn encode_volume_key(volume_id: crate::VolumeId) -> [u8; 16] {
    *volume_id.as_bytes()
}

/// Encodes UUID bytes followed by a big-endian generation.
fn encode_key(key: ReplicaKey) -> [u8; 24] {
    let mut bytes = [0_u8; 24];
    bytes[..16].copy_from_slice(key.volume_id().as_bytes());
    bytes[16..].copy_from_slice(&key.generation().get().to_be_bytes());
    bytes
}

/// Builds the one directory component allowed for this local replica.
fn directory_name(key: ReplicaKey) -> String {
    format!("{}-{}", key.volume_id().as_uuid(), key.generation())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Barrier};

    use tempfile::TempDir;
    use uuid::Uuid;

    use super::ReplicaCatalog;
    use crate::catalog::{
        CatalogError, LocalReplicaOrigin, PoolSpaceState, ReplicaHealth, ReplicaKey, ReplicaPool,
        ReplicaState, SavedFilesystemFormat, SavedMountState, SavedUblkDevice, SavedVolumeMount,
    };
    use crate::driver::{UblkDeviceId, UblkQueueSettings, UblkSettings};
    use crate::fs::volume::ReplicatedVolumeFilesystem;
    use crate::storage_format::ReplicaSpace;
    use crate::{
        DriverSessionId, FenceEpoch, FilesystemId, OperationId, VolumeBlockSizes, VolumeCapacity,
        VolumeDescriptor, VolumeGeneration, VolumeId,
    };

    /// Creates one deterministic descriptor for catalog tests.
    fn descriptor(id: u128, capacity_bytes: u64) -> VolumeDescriptor {
        descriptor_generation(id, 1, capacity_bytes)
    }

    /// Creates one deterministic descriptor at an explicit storage generation.
    fn descriptor_generation(id: u128, generation: u64, capacity_bytes: u64) -> VolumeDescriptor {
        VolumeDescriptor::new(
            VolumeId::new(Uuid::from_u128(id)).expect("non-zero volume id"),
            VolumeGeneration::new(generation).expect("non-zero generation"),
            capacity_bytes,
            VolumeBlockSizes::supported(),
        )
        .expect("valid test descriptor")
    }

    /// Returns the stable setup operation used by catalog tests.
    fn setup_operation_id() -> LocalReplicaOrigin {
        LocalReplicaOrigin::Bootstrap(
            OperationId::new(Uuid::from_u128(500)).expect("setup operation ID"),
        )
    }

    /// Returns three fixed voters recorded for local provisioning validation.
    fn origin_voters() -> BTreeSet<Uuid> {
        BTreeSet::from([
            Uuid::from_u128(1_001),
            Uuid::from_u128(1_002),
            Uuid::from_u128(1_003),
        ])
    }

    /// Creates a Redb handle and controlled pool in one temporary directory.
    fn test_catalog(
        directory: &TempDir,
        managed_bytes: u64,
        available_bytes: Arc<AtomicU64>,
    ) -> (Arc<redb::Database>, ReplicaPool, ReplicaCatalog) {
        let database = Arc::new(
            redb::Database::create(directory.path().join("local.redb"))
                .expect("create test database"),
        );
        let pool = ReplicaPool::for_test(
            directory.path().join("pool"),
            managed_bytes,
            available_bytes,
        );
        let catalog =
            ReplicaCatalog::open(database.clone(), pool.clone()).expect("open test catalog");
        (database, pool, catalog)
    }

    /// Builds bounded ublk settings at one exact test capacity.
    fn ublk_settings(descriptor: &VolumeDescriptor) -> UblkSettings {
        UblkSettings::new(
            descriptor,
            UblkQueueSettings {
                queue_count: 2,
                queue_depth: 32,
                max_request_bytes: 128 << 10,
                memory_limit_bytes: 8 << 20,
            },
        )
        .expect("valid test ublk settings")
    }

    /// Builds one saved device for an exact descriptor and writer session.
    fn saved_device(
        id: u32,
        descriptor: &VolumeDescriptor,
        fence: FenceEpoch,
        session: DriverSessionId,
    ) -> SavedUblkDevice {
        SavedUblkDevice::new(
            UblkDeviceId::new(id),
            fence,
            session,
            ublk_settings(descriptor),
        )
    }

    /// Reservations may lead Raft capacity but committed capacity never exceeds them.
    #[test]
    fn replica_capacity_reservation_and_application_are_monotonic() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (_database, _pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let initial = descriptor(90, 64 << 20);
        let key = ReplicaKey::from(&initial);
        catalog
            .reserve_replica(initial.clone(), setup_operation_id())
            .expect("reserve initial replica");
        let target = VolumeCapacity::new(128 << 20).expect("aligned target capacity");

        let reserved = catalog
            .reserve_replica_capacity(key, target)
            .expect("reserve larger capacity");
        assert_eq!(reserved.descriptor(), &initial);
        assert_eq!(reserved.reserved_space().data_bytes(), target.bytes());
        assert_eq!(
            catalog
                .apply_replica_capacity(key, target)
                .expect("apply reserved capacity")
                .descriptor()
                .capacity(),
            target
        );
        catalog
            .apply_replica_capacity(key, target)
            .expect("repeat capacity application");
        assert!(matches!(
            catalog.apply_replica_capacity(
                key,
                VolumeCapacity::new(96 << 20).expect("smaller aligned capacity")
            ),
            Err(CatalogError::CapacityCannotShrink)
        ));
        assert!(matches!(
            catalog.reduce_replica_reservation(
                key,
                VolumeCapacity::new(96 << 20).expect("smaller aligned capacity")
            ),
            Err(CatalogError::ReservationBelowAppliedCapacity)
        ));
    }

    /// Correcting an uncommitted target releases only its excess durable promise.
    #[test]
    fn pending_capacity_reservation_can_be_corrected_before_commit() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (_database, _pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let initial = descriptor(91, 64 << 20);
        let key = ReplicaKey::from(&initial);
        catalog
            .reserve_replica(initial.clone(), setup_operation_id())
            .expect("reserve initial replica");
        catalog
            .reserve_replica_capacity(key, VolumeCapacity::new(128 << 20).expect("larger target"))
            .expect("reserve larger target");
        let corrected = VolumeCapacity::new(96 << 20).expect("corrected target");
        let record = catalog
            .reduce_replica_reservation(key, corrected)
            .expect("release uncommitted excess");
        assert_eq!(record.descriptor(), &initial);
        assert_eq!(record.reserved_space().data_bytes(), corrected.bytes());
        let pool = catalog.pool_status().expect("read pool status");
        let expected = ReplicaSpace::for_capacity(corrected.bytes()).expect("corrected space");
        assert_eq!(pool.data_bytes(), expected.data_bytes());
        assert_eq!(pool.metadata_bytes(), expected.metadata_bytes());
    }

    /// A failed larger reservation leaves both the replica and pool totals unchanged.
    #[test]
    fn insufficient_pool_space_does_not_partially_reserve_capacity() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let initial = descriptor(92, 64 << 20);
        let initial_space =
            ReplicaSpace::for_capacity(initial.capacity().bytes()).expect("initial replica space");
        let managed = initial_space
            .total_bytes()
            .expect("initial total space")
            .checked_add(4096)
            .expect("test managed space");
        let (_database, _pool, catalog) = test_catalog(&directory, managed, available);
        let key = ReplicaKey::from(&initial);
        catalog
            .reserve_replica(initial.clone(), setup_operation_id())
            .expect("reserve initial replica");
        let before = catalog.pool_status().expect("read initial pool status");
        assert!(matches!(
            catalog.reserve_replica_capacity(
                key,
                VolumeCapacity::new(128 << 20).expect("larger target")
            ),
            Err(CatalogError::NotEnoughSpace { .. })
        ));
        let after = catalog.pool_status().expect("read unchanged pool status");
        assert_eq!(after.data_bytes(), before.data_bytes());
        assert_eq!(after.metadata_bytes(), before.metadata_bytes());
        assert_eq!(
            catalog
                .replica(key)
                .expect("read replica")
                .expect("replica exists")
                .descriptor(),
            &initial
        );
    }

    /// The active-capacity device remains owned until the mapped descriptor advances.
    #[test]
    fn attachment_capacity_switch_keeps_exactly_one_active_device() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (database, pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let initial = descriptor(93, 64 << 20);
        let target = initial
            .with_capacity(VolumeCapacity::new(128 << 20).expect("larger target"))
            .expect("compatible target descriptor");
        let key = ReplicaKey::from(&initial);
        let session = DriverSessionId::new(Uuid::from_u128(930)).expect("test session");
        let fence = FenceEpoch::new(7).expect("test fence");
        catalog
            .reserve_replica(initial.clone(), setup_operation_id())
            .expect("reserve initial replica");
        catalog
            .reserve_replica_capacity(key, target.capacity())
            .expect("reserve target capacity");
        catalog
            .apply_replica_capacity(key, target.capacity())
            .expect("apply target capacity");
        catalog
            .open_or_create_attachment(initial.clone(), session)
            .expect("save attachment");
        catalog
            .save_attachment_fence(key, session, fence)
            .expect("save writer fence");
        let old = saved_device(31, &initial, fence, session);
        let larger = saved_device(32, &target, fence, session);
        catalog.save_ublk_device(key, old).expect("save old device");
        catalog
            .save_ublk_device(key, larger)
            .expect("save larger device");
        drop(catalog);
        let catalog = ReplicaCatalog::open(database.clone(), pool.clone())
            .expect("reopen catalog after saving both capacity devices");
        assert_eq!(
            catalog
                .attachment(key)
                .expect("read interrupted device switch")
                .expect("attachment exists")
                .ublk_devices(),
            &[old, larger]
        );
        let mounting = SavedVolumeMount::mounting(
            fence,
            session,
            directory.path().join("mount"),
            0,
            0,
            0o700,
            ReplicatedVolumeFilesystem::Xfs,
        )
        .expect("valid saved mount");
        catalog
            .save_volume_mount(key, mounting)
            .expect("save mount");

        assert!(matches!(
            catalog.clear_ublk_device(key, old),
            Err(CatalogError::VolumeMountDeviceMismatch)
        ));
        catalog
            .advance_attachment_capacity(key, target.clone())
            .expect("record active larger mapping");
        drop(catalog);
        let catalog = ReplicaCatalog::open(database, pool)
            .expect("reopen catalog after recording the larger mapping");
        assert!(matches!(
            catalog.clear_ublk_device(key, larger),
            Err(CatalogError::VolumeMountDeviceMismatch)
        ));
        catalog
            .clear_ublk_device(key, old)
            .expect("remove inactive old device");
        let saved = catalog
            .attachment(key)
            .expect("read attachment")
            .expect("attachment exists");
        assert_eq!(saved.descriptor(), &target);
        assert_eq!(saved.ublk_devices(), &[larger]);
    }

    /// Finds replicas and unfinished work after closing and reopening Redb.
    #[test]
    fn restart_discovers_replicas_without_cluster_state() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (database, pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let first = descriptor(1, 64 << 20);
        let second = descriptor(2, 128 << 20);
        catalog
            .reserve_replica(first.clone(), setup_operation_id())
            .expect("reserve first replica");
        catalog
            .reserve_replica(second.clone(), setup_operation_id())
            .expect("reserve second replica");
        catalog
            .set_replica_state(ReplicaKey::from(&first), ReplicaState::Deleting)
            .expect("mark first replica deleting");
        catalog
            .set_replica_health(ReplicaKey::from(&second), ReplicaHealth::NeedsRecovery)
            .expect("mark second replica unhealthy");
        drop(catalog);
        drop(database);

        let reopened_database = Arc::new(
            redb::Database::open(directory.path().join("local.redb"))
                .expect("reopen test database"),
        );
        let reopened = ReplicaCatalog::open(reopened_database, pool).expect("reopen local catalog");
        let records = reopened
            .discover_replicas(10)
            .expect("discover local replicas");
        assert_eq!(2, records.len());
        let recovered = reopened
            .replica(ReplicaKey::from(&first))
            .expect("read recovered replica")
            .expect("recovered replica exists");
        assert_eq!(ReplicaState::Deleting, recovered.state());
        assert_eq!(
            ReplicaHealth::NeedsRecovery,
            reopened
                .replica(ReplicaKey::from(&second))
                .expect("read unhealthy replica")
                .expect("unhealthy replica exists")
                .health()
        );
    }

    /// Admission rejects a new row at the limit but preserves exact retry semantics.
    #[test]
    fn bounded_reservation_never_creates_an_unrecoverable_catalog() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (_database, _pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let first = descriptor(101, 64 << 20);
        let second = descriptor(102, 64 << 20);

        catalog
            .reserve_replica_bounded(first.clone(), setup_operation_id(), 1)
            .expect("reserve first bounded replica");
        catalog
            .reserve_replica_bounded(first, setup_operation_id(), 1)
            .expect("retry existing bounded replica");
        let error = catalog
            .reserve_replica_bounded(second, setup_operation_id(), 1)
            .expect_err("second bounded replica must be rejected");

        assert!(matches!(
            error,
            CatalogError::TooManyReplicaSlots {
                actual: 2,
                maximum: 1
            }
        ));
        assert_eq!(catalog.replica_count().expect("count replicas"), 1);
        assert_eq!(
            catalog
                .discover_replicas(1)
                .expect("bounded catalog remains restartable")
                .len(),
            1
        );
    }

    /// A retained replica can return to Ready without changing its reservation.
    #[test]
    fn retained_replica_can_be_restored() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (_database, _pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let descriptor = descriptor(1, 64 << 20);
        let key = ReplicaKey::from(&descriptor);
        catalog
            .reserve_replica(descriptor, setup_operation_id())
            .expect("reserve replica");
        catalog
            .set_replica_state(key, ReplicaState::Ready)
            .expect("mark replica ready");
        catalog
            .set_replica_state(key, ReplicaState::Retained)
            .expect("retain replica");
        catalog
            .set_replica_state(key, ReplicaState::Ready)
            .expect("restore retained replica");

        assert_eq!(
            catalog
                .replica(key)
                .expect("read restored replica")
                .expect("restored replica exists")
                .state(),
            ReplicaState::Ready
        );
    }

    /// The exact ublk device is durable and cannot be replaced accidentally.
    #[test]
    fn ublk_device_is_saved_replaced_and_cleared_exactly() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (database, pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let descriptor = descriptor(14, 64 << 20);
        let key = ReplicaKey::from(&descriptor);
        catalog
            .reserve_replica(descriptor.clone(), setup_operation_id())
            .expect("reserve replica");
        let settings = UblkSettings::new(
            &descriptor,
            UblkQueueSettings {
                queue_count: 2,
                queue_depth: 32,
                max_request_bytes: 128 << 10,
                memory_limit_bytes: 8 << 20,
            },
        )
        .expect("fixed ublk settings");
        let epoch = FenceEpoch::new(3).expect("non-zero fence");
        let session = DriverSessionId::new(Uuid::from_u128(700)).expect("non-zero driver session");
        let first = SavedUblkDevice::new(UblkDeviceId::new(4), epoch, session, settings);
        let second = SavedUblkDevice::new(UblkDeviceId::new(5), epoch, session, settings);

        catalog
            .open_or_create_attachment(descriptor.clone(), session)
            .expect("save attachment session");
        catalog
            .save_attachment_fence(key, session, epoch)
            .expect("save attachment fence");

        catalog
            .save_ublk_device(key, first)
            .expect("save first ublk device");
        catalog
            .save_ublk_device(key, first)
            .expect("repeat exact ublk save");
        assert!(matches!(
            catalog.save_ublk_device(key, second),
            Err(CatalogError::UblkDeviceChanged)
        ));
        catalog
            .replace_ublk_device(key, first, second)
            .expect("replace exact missing device");
        catalog
            .replace_ublk_device(key, first, second)
            .expect("repeat exact ublk replacement");
        assert!(matches!(
            catalog.clear_ublk_device(key, first),
            Err(CatalogError::UblkDeviceChanged)
        ));
        drop(catalog);
        drop(database);

        let reopened_database = Arc::new(
            redb::Database::open(directory.path().join("local.redb"))
                .expect("reopen test database"),
        );
        let reopened = ReplicaCatalog::open(reopened_database, pool).expect("reopen local catalog");
        assert_eq!(
            &[second],
            reopened
                .attachment(key)
                .expect("read attachment")
                .expect("attachment must exist")
                .ublk_devices()
        );
        reopened
            .clear_ublk_device(key, second)
            .expect("clear exact ublk device");
        assert!(
            reopened
                .attachment(key)
                .expect("read cleared attachment")
                .expect("attachment must exist")
                .ublk_devices()
                .is_empty()
        );
    }

    /// Mount progress survives restart and keeps its ublk device alive.
    #[test]
    fn volume_mount_is_saved_advanced_and_cleared_exactly() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (database, pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let descriptor = descriptor(15, 64 << 20);
        let key = ReplicaKey::from(&descriptor);
        catalog
            .reserve_replica(descriptor.clone(), setup_operation_id())
            .expect("reserve replica");
        let settings = UblkSettings::new(
            &descriptor,
            UblkQueueSettings {
                queue_count: 2,
                queue_depth: 32,
                max_request_bytes: 128 << 10,
                memory_limit_bytes: 8 << 20,
            },
        )
        .expect("fixed ublk settings");
        let epoch = FenceEpoch::new(4).expect("non-zero fence");
        let session = DriverSessionId::new(Uuid::from_u128(701)).expect("non-zero driver session");
        let device = SavedUblkDevice::new(UblkDeviceId::new(6), epoch, session, settings);
        let mounting = SavedVolumeMount::mounting(
            epoch,
            session,
            directory.path().join("mount"),
            1000,
            1001,
            0o2770,
            ReplicatedVolumeFilesystem::Xfs,
        )
        .expect("valid saved mount");

        catalog
            .open_or_create_attachment(descriptor.clone(), session)
            .expect("save attachment session");
        catalog
            .save_attachment_fence(key, session, epoch)
            .expect("save attachment fence");

        assert!(matches!(
            catalog.save_volume_mount(key, mounting.clone()),
            Err(CatalogError::VolumeMountDeviceMismatch)
        ));
        catalog
            .save_ublk_device(key, device)
            .expect("save ublk device");
        catalog
            .save_volume_mount(key, mounting.clone())
            .expect("save mounting step");
        let format = SavedFilesystemFormat::new(
            ReplicatedVolumeFilesystem::Xfs,
            FilesystemId::new(Uuid::from_u128(702)).expect("non-zero filesystem ID"),
            [0x5a; 32],
        );
        catalog
            .save_filesystem_format(key, format)
            .expect("save unfinished filesystem format");
        assert!(matches!(
            catalog.clear_ublk_device(key, device),
            Err(CatalogError::VolumeMountDeviceMismatch)
        ));
        catalog
            .clear_filesystem_format(key, format)
            .expect("clear completed filesystem format");
        catalog
            .save_volume_mount(key, mounting.clone())
            .expect("save same mounting step");
        let mounted = mounting
            .with_state(SavedMountState::Mounted)
            .expect("advance to mounted")
            .with_filesystem_expanded_to(descriptor.capacity().bytes())
            .expect("save initial filesystem capacity");
        let impossible = mounting
            .with_state(SavedMountState::Mounted)
            .expect("advance to mounted")
            .with_filesystem_expanded_to(descriptor.capacity().bytes() + 4096)
            .expect("model permits catalog-level capacity validation");
        assert!(matches!(
            catalog.replace_volume_mount(key, &mounting, impossible),
            Err(CatalogError::FilesystemCapacityExceedsDevice)
        ));
        assert!(matches!(
            mounted.with_filesystem_expanded_to(descriptor.capacity().bytes() - 4096),
            Err(crate::catalog::InvalidSavedVolumeMount::FilesystemCapacityMovedBackwards)
        ));
        catalog
            .replace_volume_mount(key, &mounting, mounted.clone())
            .expect("save mounted step");
        assert!(matches!(
            catalog.clear_ublk_device(key, device),
            Err(CatalogError::VolumeMountDeviceMismatch)
        ));
        drop(catalog);
        drop(database);

        let reopened_database = Arc::new(
            redb::Database::open(directory.path().join("local.redb"))
                .expect("reopen test database"),
        );
        let reopened = ReplicaCatalog::open(reopened_database, pool).expect("reopen local catalog");
        assert_eq!(
            Some(&mounted),
            reopened
                .attachment(key)
                .expect("read attachment")
                .expect("attachment must exist")
                .volume_mount()
        );
        assert!(matches!(
            reopened.replace_volume_mount(key, &mounted, mounting),
            Err(CatalogError::VolumeMountChanged)
        ));
        let unmounting = mounted
            .with_state(SavedMountState::Unmounting)
            .expect("advance to unmounting");
        reopened
            .begin_attachment_detach(key)
            .expect("save attachment detach intent");
        let detaching = reopened
            .attachment(key)
            .expect("read detaching attachment")
            .expect("detaching attachment must exist");
        assert!(detaching.is_detaching());
        assert_eq!(detaching.volume_mount(), Some(&unmounting));
        reopened
            .clear_volume_mount(key, &unmounting)
            .expect("clear exact mount");
        reopened
            .clear_ublk_device(key, device)
            .expect("clear device after mount");
    }

    /// Detach intent survives without inventing a device or mount resource.
    #[test]
    fn attachment_detach_intent_survives_before_device_creation() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (database, pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let descriptor = descriptor(18, 64 << 20);
        let key = ReplicaKey::from(&descriptor);
        let session = DriverSessionId::new(Uuid::from_u128(705)).expect("driver session");
        let fence = FenceEpoch::new(11).expect("writer fence");
        catalog
            .reserve_replica(descriptor.clone(), setup_operation_id())
            .expect("reserve replica");
        catalog
            .open_or_create_attachment(descriptor, session)
            .expect("save attachment session");
        catalog
            .save_attachment_fence(key, session, fence)
            .expect("save writer fence");
        catalog
            .begin_attachment_detach(key)
            .expect("save detach intent");
        catalog
            .begin_attachment_detach(key)
            .expect("repeat detach intent");
        drop(catalog);
        drop(database);

        let reopened_database = Arc::new(
            redb::Database::open(directory.path().join("local.redb"))
                .expect("reopen test database"),
        );
        let reopened = ReplicaCatalog::open(reopened_database, pool).expect("reopen local catalog");
        let saved = reopened
            .attachment(key)
            .expect("read saved attachment")
            .expect("saved attachment must exist");
        assert!(saved.is_detaching());
        assert_eq!(saved.granted_fence(), Some(fence));
        assert!(saved.ublk_devices().is_empty());
        assert_eq!(saved.volume_mount(), None);
    }

    /// A writer handoff advances the attachment, device, and mount together.
    #[test]
    fn attachment_fence_advance_is_atomic_and_idempotent() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (_database, _pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let descriptor = descriptor(16, 64 << 20);
        let key = ReplicaKey::from(&descriptor);
        catalog
            .reserve_replica(descriptor.clone(), setup_operation_id())
            .expect("reserve replica");
        let settings = UblkSettings::new(
            &descriptor,
            UblkQueueSettings {
                queue_count: 2,
                queue_depth: 32,
                max_request_bytes: 128 << 10,
                memory_limit_bytes: 8 << 20,
            },
        )
        .expect("fixed ublk settings");
        let old = FenceEpoch::new(7).expect("old fence");
        let new = FenceEpoch::new(9).expect("new fence");
        let session = DriverSessionId::new(Uuid::from_u128(703)).expect("driver session");
        let device = SavedUblkDevice::new(UblkDeviceId::new(7), old, session, settings);
        let mounting = SavedVolumeMount::mounting(
            old,
            session,
            directory.path().join("mount"),
            1000,
            1001,
            0o2770,
            ReplicatedVolumeFilesystem::Ext4,
        )
        .expect("valid saved mount");
        let mounted = mounting
            .with_state(SavedMountState::Mounted)
            .expect("mounted state");

        catalog
            .open_or_create_attachment(descriptor, session)
            .expect("save attachment session");
        catalog
            .save_attachment_fence(key, session, old)
            .expect("save old fence");
        catalog.save_ublk_device(key, device).expect("save device");
        catalog
            .save_volume_mount(key, mounting.clone())
            .expect("save mounting state");
        catalog
            .replace_volume_mount(key, &mounting, mounted)
            .expect("save mounted state");

        catalog
            .advance_attachment_fence(key, session, old, new)
            .expect("advance complete attachment");
        catalog
            .advance_attachment_fence(key, session, old, new)
            .expect("repeat exact advance");
        let saved = catalog
            .attachment(key)
            .expect("read attachment")
            .expect("attachment exists");
        assert_eq!(saved.granted_fence(), Some(new));
        assert_eq!(
            saved
                .ublk_devices()
                .iter()
                .copied()
                .map(SavedUblkDevice::fence)
                .collect::<Vec<_>>(),
            vec![new],
        );
        assert_eq!(saved.volume_mount().map(SavedVolumeMount::fence), Some(new));
        assert!(matches!(
            catalog.advance_attachment_fence(
                key,
                session,
                FenceEpoch::new(8).expect("wrong fence"),
                FenceEpoch::new(10).expect("later fence"),
            ),
            Err(CatalogError::AttachmentFenceChanged)
        ));
    }

    /// A setup retry must use the operation that owns the saved reservation.
    #[test]
    fn replica_reservation_rejects_another_setup_operation() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (_database, _pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let descriptor = descriptor(13, 64 << 20);
        let owner = setup_operation_id();
        let other = LocalReplicaOrigin::Bootstrap(
            OperationId::new(Uuid::from_u128(501)).expect("other setup operation ID"),
        );

        catalog
            .reserve_replica(descriptor.clone(), owner.clone())
            .expect("reserve replica");
        catalog
            .reserve_replica(descriptor.clone(), owner)
            .expect("repeat owning reservation");
        assert!(matches!(
            catalog.reserve_replica(descriptor, other),
            Err(CatalogError::ConflictingReplica)
        ));
    }

    /// Replacement provenance survives restart and is exact across retries.
    #[test]
    fn replacement_reservation_retains_its_voter_route() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (database, pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let descriptor = descriptor(14, 64 << 20);
        let key = ReplicaKey::from(&descriptor);
        let replacement_id =
            crate::ReplacementId::new(Uuid::from_u128(700)).expect("non-zero replacement ID");
        let origin = LocalReplicaOrigin::replacement(
            replacement_id,
            BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2)]),
        )
        .expect("valid degraded replacement origin");

        catalog
            .reserve_replica(descriptor.clone(), origin.clone())
            .expect("reserve replacement replica");
        drop(catalog);
        drop(database);

        let reopened_database = Arc::new(
            redb::Database::open(directory.path().join("local.redb"))
                .expect("reopen local database"),
        );
        let reopened = ReplicaCatalog::open(reopened_database, pool).expect("reopen catalog");
        let record = reopened
            .replica(key)
            .expect("read replacement replica")
            .expect("replacement replica exists");
        assert_eq!(record.origin(), &origin);

        let changed_route = LocalReplicaOrigin::replacement(
            replacement_id,
            BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(3)]),
        )
        .expect("valid conflicting replacement origin");
        assert!(matches!(
            reopened.reserve_replica(descriptor, changed_route),
            Err(CatalogError::ConflictingReplica)
        ));
    }

    /// An excluded bootstrap slot becomes crash-retryable replacement work atomically.
    #[test]
    fn existing_replica_can_be_rebuilt_for_an_applied_replacement() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (_database, _pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let initial = descriptor(15, 64 << 20);
        let expanded = descriptor(15, 128 << 20);
        let key = ReplicaKey::from(&initial);
        let origin = LocalReplicaOrigin::replacement(
            crate::ReplacementId::new(Uuid::from_u128(701)).expect("non-zero replacement ID"),
            BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2)]),
        )
        .expect("valid replacement origin");

        catalog
            .reserve_replica(initial, setup_operation_id())
            .expect("reserve original replica");
        catalog
            .set_replica_state(key, ReplicaState::Ready)
            .expect("finish original replica");
        catalog
            .reserve_replica_capacity(key, expanded.capacity())
            .expect("reserve expanded capacity");

        let rebuilding = catalog
            .begin_replica_replacement(key, expanded.clone(), origin.clone())
            .expect("begin replacement rebuild");
        assert_eq!(rebuilding.descriptor(), &expanded);
        assert_eq!(rebuilding.origin(), &origin);
        assert_eq!(rebuilding.state(), ReplicaState::Preparing);
        assert_eq!(rebuilding.health(), ReplicaHealth::Healthy);
        assert_eq!(
            catalog
                .begin_replica_replacement(key, expanded.clone(), origin.clone())
                .expect("repeat replacement rebuild"),
            rebuilding
        );

        catalog
            .set_replica_state(key, ReplicaState::Ready)
            .expect("finish rebuilt replica");
        let session = DriverSessionId::new(Uuid::from_u128(702)).expect("driver session");
        catalog
            .open_or_create_attachment(expanded.clone(), session)
            .expect("save attachment");
        assert!(matches!(
            catalog.begin_replica_replacement(key, expanded, origin),
            Err(CatalogError::ReplicaStillAttached)
        ));
    }

    /// Redb serializes competing reservations so the pool cannot be promised twice.
    #[test]
    fn concurrent_reservations_do_not_oversubscribe_the_pool() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let capacity = 64 << 20;
        let one = ReplicaSpace::for_capacity(capacity)
            .expect("space calculation")
            .total_bytes()
            .expect("space total");
        let managed = one * 3;
        let (_database, _pool, catalog) = test_catalog(&directory, managed, available);
        let catalog = Arc::new(catalog);
        let barrier = Arc::new(Barrier::new(8));
        let mut threads = Vec::new();
        for number in 1..=8_u128 {
            let catalog = catalog.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                catalog.reserve_replica(descriptor(number, capacity), setup_operation_id())
            }));
        }
        let successes = threads
            .into_iter()
            .map(|thread| thread.join().expect("reservation thread"))
            .filter(Result::is_ok)
            .count();
        assert_eq!(3, successes);
        assert_eq!(
            3,
            catalog
                .discover_replicas(10)
                .expect("discover reserved replicas")
                .len()
        );
    }

    /// Free-space pressure stops new replicas before it blocks deletion.
    #[test]
    fn disk_pressure_keeps_deletion_available() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let capacity = 64 << 20;
        let space = ReplicaSpace::for_capacity(capacity)
            .expect("space calculation")
            .total_bytes()
            .expect("space total");
        let available = Arc::new(AtomicU64::new(space));
        let (_database, _pool, catalog) = test_catalog(&directory, space, available.clone());
        let first = descriptor(1, capacity);
        let key = ReplicaKey::from(&first);
        catalog
            .reserve_replica(first.clone(), setup_operation_id())
            .expect("reserve local replica");

        assert_eq!(
            PoolSpaceState::NoNewReplicas,
            catalog.pool_status().expect("pool status").state()
        );
        assert!(matches!(
            catalog.reserve_replica(descriptor(2, capacity), setup_operation_id()),
            Err(CatalogError::NotEnoughSpace { .. })
        ));
        available.store(0, std::sync::atomic::Ordering::Release);
        assert_eq!(
            PoolSpaceState::DeleteOnly,
            catalog.pool_status().expect("full pool status").state()
        );
        assert_eq!(
            first,
            *catalog
                .reserve_replica(first.clone(), setup_operation_id())
                .expect("repeat existing reservation at zero free space")
                .descriptor()
        );

        catalog
            .set_replica_state(key, ReplicaState::Deleting)
            .expect("mark replica deleting");
        catalog
            .remove_deleted_replica(key)
            .expect("release deleted replica");
        catalog
            .remove_deleted_replica(key)
            .expect("repeat deleted replica release");
        assert!(
            catalog
                .discover_replicas(1)
                .expect("discover empty catalog")
                .is_empty()
        );
    }

    /// Retirement releases space, blocks bootstrap, and permits a later replacement.
    #[test]
    fn retired_replica_is_not_recreated_by_bootstrap() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (database, pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let descriptor = descriptor(3, 64 << 20);
        let key = ReplicaKey::from(&descriptor);
        catalog
            .reserve_replica(descriptor.clone(), setup_operation_id())
            .expect("reserve bootstrap replica");
        catalog
            .set_replica_state(key, ReplicaState::Retiring)
            .expect("mark former member retiring");
        catalog
            .remove_retired_replica(key)
            .expect("retire former member");

        drop(catalog);
        drop(database);
        let reopened_database = Arc::new(
            redb::Database::open(directory.path().join("local.redb"))
                .expect("reopen retirement catalog"),
        );
        let catalog = ReplicaCatalog::open(reopened_database, pool)
            .expect("validate persisted retirement catalog");

        assert!(catalog.replica(key).expect("read replica").is_none());
        assert_eq!(
            Some(crate::catalog::LocalReplicaRetirement::new(key)),
            catalog.retirement(key).expect("read retirement")
        );
        assert!(matches!(
            catalog.reserve_replica(descriptor.clone(), setup_operation_id()),
            Err(CatalogError::ReplicaRetired)
        ));

        let replacement = LocalReplicaOrigin::replacement(
            crate::ReplacementId::new(Uuid::from_u128(900)).expect("replacement ID"),
            origin_voters(),
        )
        .expect("valid replacement origin");
        catalog
            .reserve_replica(descriptor, replacement)
            .expect("later replacement may reuse retired node");
        assert!(catalog.retirement(key).expect("read retirement").is_none());
    }

    /// A proven missing former copy retains bounded bootstrap suppression.
    #[test]
    fn missing_retired_replica_cannot_be_recreated_by_bootstrap() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (_database, _pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let descriptor = descriptor(30, 64 << 20);
        let key = ReplicaKey::from(&descriptor);

        catalog
            .record_missing_replica_retirement(key, 1)
            .expect("record missing former copy");
        catalog
            .record_missing_replica_retirement(key, 1)
            .expect("repeat missing former copy retirement");

        assert_eq!(
            Some(crate::catalog::LocalReplicaRetirement::new(key)),
            catalog.retirement(key).expect("read retirement")
        );
        assert!(matches!(
            catalog.reserve_replica(descriptor, setup_operation_id()),
            Err(CatalogError::ReplicaRetired)
        ));
    }

    /// Files and retirement proofs share a slot bound without blocking cleanup.
    #[test]
    fn retirement_preserves_the_shared_replica_slot_bound() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (_database, _pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let first = descriptor(30, 64 << 20);
        let second = descriptor(31, 64 << 20);
        let third = descriptor(32, 64 << 20);
        let first_key = ReplicaKey::from(&first);
        let second_key = ReplicaKey::from(&second);
        for descriptor in [first, second] {
            let key = ReplicaKey::from(&descriptor);
            catalog
                .reserve_replica_bounded(descriptor, setup_operation_id(), 2)
                .expect("reserve replica before retirement");
            catalog
                .set_replica_state(key, ReplicaState::Retiring)
                .expect("mark replica retiring");
        }

        catalog
            .remove_retired_replica(first_key)
            .expect("replace first file with retirement proof");
        assert_eq!(
            2,
            catalog
                .replica_slot_count()
                .expect("count occupied replica slots")
        );
        catalog
            .check_replica_slot_limit(2)
            .expect("accept the exact startup slot limit");
        assert!(matches!(
            catalog.check_replica_slot_limit(1),
            Err(CatalogError::TooManyReplicaSlots {
                actual: 2,
                maximum: 1
            })
        ));
        assert!(matches!(
            catalog.reserve_replica_bounded(third, setup_operation_id(), 2),
            Err(CatalogError::TooManyReplicaSlots {
                actual: 3,
                maximum: 2
            })
        ));

        let replacement = LocalReplicaOrigin::replacement(
            crate::ReplacementId::new(Uuid::from_u128(901)).expect("replacement ID"),
            origin_voters(),
        )
        .expect("valid replacement origin");
        catalog
            .reserve_replica_bounded(descriptor(30, 64 << 20), replacement, 2)
            .expect("replacement consumes its existing retirement slot");
        assert!(catalog.retirement(first_key).expect("read proof").is_none());
        catalog
            .remove_retired_replica(second_key)
            .expect("second cleanup exchanges a file for a proof at the limit");
        assert!(
            catalog
                .retirement(second_key)
                .expect("read second proof")
                .is_some()
        );
    }

    /// A new public generation atomically supersedes old local retirement proof.
    #[test]
    fn newer_generation_forgets_old_retirement() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (_database, _pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let old = descriptor_generation(32, 1, 64 << 20);
        let new = descriptor_generation(32, 2, 64 << 20);
        let old_key = ReplicaKey::from(&old);
        let new_key = ReplicaKey::from(&new);
        catalog
            .reserve_replica(old, setup_operation_id())
            .expect("reserve old generation");
        catalog
            .set_replica_state(old_key, ReplicaState::Retiring)
            .expect("mark old generation retiring");
        catalog
            .remove_retired_replica(old_key)
            .expect("retire old generation");

        catalog
            .forget_retirement_before(new_key)
            .expect("prune superseded retirement without a new local replica");
        assert!(
            catalog
                .retirement(old_key)
                .expect("read old proof")
                .is_none()
        );
        catalog
            .reserve_replica(new, setup_operation_id())
            .expect("reserve new generation");
        assert!(
            catalog
                .replica(new_key)
                .expect("read new replica")
                .is_some()
        );
    }

    /// Terminal deletion removes obsolete bootstrap-suppression state idempotently.
    #[test]
    fn deleted_retirement_forgets_local_history() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (_database, _pool, catalog) = test_catalog(&directory, 1 << 40, available);
        let descriptor = descriptor(4, 64 << 20);
        let key = ReplicaKey::from(&descriptor);
        catalog
            .reserve_replica(descriptor, setup_operation_id())
            .expect("reserve bootstrap replica");
        catalog
            .set_replica_state(key, ReplicaState::Retiring)
            .expect("mark former member retiring");
        catalog
            .remove_retired_replica(key)
            .expect("retire former member");
        catalog
            .remove_deleted_replica(key)
            .expect("convert retirement to deletion");
        catalog
            .remove_deleted_replica(key)
            .expect("repeat terminal deletion");

        assert!(catalog.retirement(key).expect("read retirement").is_none());
    }

    /// Discovery rejects a caller limit before allocating the result.
    #[test]
    fn discovery_limit_is_checked() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let available = Arc::new(AtomicU64::new(1 << 40));
        let (_database, _pool, catalog) = test_catalog(&directory, 1 << 40, available);
        catalog
            .reserve_replica(descriptor(1, 64 << 20), setup_operation_id())
            .expect("reserve replica");
        assert!(matches!(
            catalog.discover_replicas(0),
            Err(CatalogError::TooManyReplicas {
                actual: 1,
                maximum: 0
            })
        ));
    }
}
