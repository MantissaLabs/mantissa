use std::cmp::Ordering;
use std::marker::PhantomData;
use std::sync::Arc;

use openraft::{EmptyNode, LogId, NodeId, StoredMembership, Vote};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

use super::{CatalogError, GroupActivation, GroupIdAdapter, GroupRecord};
use crate::protocol::catalog_record::{decode_group_record, encode_group_record};
use crate::protocol::{NodeIdAdapter, ProtocolLimits, encode_group_id};

const GROUPS: TableDefinition<'static, &'static [u8], &'static [u8]> =
    TableDefinition::new("mantissa_raft_groups");

/// Shared durable catalog for all local Raft groups of one application.
///
/// The catalog owns one Redb handle. Durable rows do not create group
/// runtimes or any other per-group resources.
pub struct GroupCatalog<GID, NID, G, N>
where
    NID: NodeId,
{
    database: Arc<redb::Database>,
    group_ids: G,
    node_ids: N,
    limits: ProtocolLimits,
    group_types: PhantomData<fn() -> (GID, NID)>,
}

impl<GID, NID, G, N> Clone for GroupCatalog<GID, NID, G, N>
where
    NID: NodeId,
    G: Clone,
    N: Clone,
{
    /// Clones a handle to the same durable group catalog.
    fn clone(&self) -> Self {
        Self {
            database: Arc::clone(&self.database),
            group_ids: self.group_ids.clone(),
            node_ids: self.node_ids.clone(),
            limits: self.limits,
            group_types: PhantomData,
        }
    }
}

impl<GID, NID, G, N> GroupCatalog<GID, NID, G, N>
where
    GID: Clone + Eq,
    NID: NodeId,
    G: GroupIdAdapter<GID>,
    N: NodeIdAdapter<NID>,
{
    /// Opens the one shared table without loading or starting any group.
    pub fn open(
        database: Arc<redb::Database>,
        group_ids: G,
        node_ids: N,
        limits: ProtocolLimits,
    ) -> Result<Self, CatalogError> {
        let write = database.begin_write()?;
        {
            let _groups = write.open_table(GROUPS)?;
        }
        // Redb write transactions use immediate durability by default.
        write.commit()?;

        Ok(Self {
            database,
            group_ids,
            node_ids,
            limits,
            group_types: PhantomData,
        })
    }

    /// Creates an idle or active group record when it does not already exist.
    ///
    /// Repeating the call for the same group returns its existing record. It
    /// does not reset a saved vote, membership, or activation state.
    pub fn ensure_group(
        &self,
        group_id: &GID,
        activation: GroupActivation,
    ) -> Result<GroupRecord<GID, NID>, CatalogError> {
        self.ensure_group_inner(group_id, activation, None)
    }

    /// Creates one group without allowing the durable catalog to exceed its limit.
    pub fn ensure_group_bounded(
        &self,
        group_id: &GID,
        activation: GroupActivation,
        maximum: usize,
    ) -> Result<GroupRecord<GID, NID>, CatalogError> {
        self.ensure_group_inner(group_id, activation, Some(maximum))
    }

    /// Performs one idempotent group ensure and checks optional admission atomically.
    fn ensure_group_inner(
        &self,
        group_id: &GID,
        activation: GroupActivation,
        maximum: Option<usize>,
    ) -> Result<GroupRecord<GID, NID>, CatalogError> {
        let key = encode_group_id(group_id, &self.group_ids, self.limits)?;
        let write = self.database.begin_write()?;
        let mut groups = write.open_table(GROUPS)?;

        if let Some(stored) = groups.get(key.as_slice())? {
            let record = self.decode_checked(key.as_slice(), stored.value())?;
            if record.group_id() != group_id {
                return Err(CatalogError::GroupIdentityMismatch);
            }
            return Ok(record);
        }
        if let Some(maximum) = maximum {
            let count = groups.len()?;
            if count >= u64::try_from(maximum).unwrap_or(u64::MAX) {
                return Err(CatalogError::TooManyGroups {
                    actual: count.saturating_add(1),
                    maximum,
                });
            }
        }

        let record = GroupRecord::new(group_id.clone(), activation);
        let encoded = encode_group_record(&record, &self.group_ids, &self.node_ids, self.limits)?;
        groups.insert(key.as_slice(), encoded.as_slice())?;
        drop(groups);
        write.commit()?;
        Ok(record)
    }

    /// Creates many missing group rows in one durable catalog change.
    ///
    /// Existing rows keep their vote, membership, and activation state.
    pub fn ensure_groups(
        &self,
        groups_to_ensure: impl IntoIterator<Item = (GID, GroupActivation)>,
    ) -> Result<(), CatalogError> {
        let write = self.database.begin_write()?;
        let mut groups = write.open_table(GROUPS)?;
        for (group_id, activation) in groups_to_ensure {
            let key = encode_group_id(&group_id, &self.group_ids, self.limits)?;
            if let Some(stored) = groups.get(key.as_slice())? {
                let record = self.decode_checked(key.as_slice(), stored.value())?;
                if record.group_id() != &group_id {
                    return Err(CatalogError::GroupIdentityMismatch);
                }
                continue;
            }

            let record = GroupRecord::new(group_id, activation);
            let encoded =
                encode_group_record(&record, &self.group_ids, &self.node_ids, self.limits)?;
            groups.insert(key.as_slice(), encoded.as_slice())?;
        }
        drop(groups);
        write.commit()?;
        Ok(())
    }

    /// Returns one durable group without creating its runtime.
    pub fn group(&self, group_id: &GID) -> Result<Option<GroupRecord<GID, NID>>, CatalogError> {
        let key = encode_group_id(group_id, &self.group_ids, self.limits)?;
        let read = self.database.begin_read()?;
        let groups = read.open_table(GROUPS)?;
        let Some(stored) = groups.get(key.as_slice())? else {
            return Ok(None);
        };
        let record = self.decode_checked(key.as_slice(), stored.value())?;
        if record.group_id() != group_id {
            return Err(CatalogError::GroupIdentityMismatch);
        }
        Ok(Some(record))
    }

    /// Loads every durable group before the caller starts serving requests.
    ///
    /// This method only returns owned records. Starting active groups remains
    /// an explicit caller action.
    pub fn discover_groups(
        &self,
        max_groups: usize,
    ) -> Result<Vec<GroupRecord<GID, NID>>, CatalogError> {
        let read = self.database.begin_read()?;
        let groups = read.open_table(GROUPS)?;
        let count = groups.len()?;
        let maximum = u64::try_from(max_groups).unwrap_or(u64::MAX);
        if count > maximum {
            return Err(CatalogError::TooManyGroups {
                actual: count,
                maximum: max_groups,
            });
        }

        let capacity = usize::try_from(count).map_err(|_| CatalogError::TooManyGroups {
            actual: count,
            maximum: max_groups,
        })?;
        let mut records = Vec::with_capacity(capacity);
        for row in groups.iter()? {
            let (key, stored) = row?;
            records.push(self.decode_checked(key.value(), stored.value())?);
        }
        Ok(records)
    }

    /// Returns the number of durable groups without decoding or starting them.
    pub fn group_count(&self) -> Result<u64, CatalogError> {
        let read = self.database.begin_read()?;
        let groups = read.open_table(GROUPS)?;
        groups.len().map_err(CatalogError::from)
    }

    /// Stores whether startup should leave a group idle or start it.
    pub fn set_activation(
        &self,
        group_id: &GID,
        activation: GroupActivation,
    ) -> Result<(), CatalogError> {
        self.update_group(group_id, |record| {
            record.set_activation(activation);
            Ok(())
        })
    }

    /// Removes one stopped group row after its application files are gone.
    pub fn remove_inactive_group(&self, group_id: &GID) -> Result<bool, CatalogError> {
        let key = encode_group_id(group_id, &self.group_ids, self.limits)?;
        let write = self.database.begin_write()?;
        let mut groups = write.open_table(GROUPS)?;
        let Some(stored) = groups.get(key.as_slice())? else {
            return Ok(false);
        };
        let record = self.decode_checked(key.as_slice(), stored.value())?;
        if record.activation() == GroupActivation::Active {
            return Err(CatalogError::GroupActive);
        }
        drop(stored);
        groups.remove(key.as_slice())?;
        drop(groups);
        write.commit()?;
        Ok(true)
    }

    /// Durably stores a vote without allowing it to move backwards.
    pub fn save_vote(&self, group_id: &GID, vote: &Vote<NID>) -> Result<(), CatalogError> {
        self.update_group(group_id, |record| {
            if let Some(stored) = record.vote()
                && vote < stored
            {
                return Err(CatalogError::StaleVote);
            }
            record.set_vote(vote.clone());
            Ok(())
        })
    }

    /// Stores the newest membership and rejects stale or conflicting writes.
    pub fn save_membership(
        &self,
        group_id: &GID,
        membership: &StoredMembership<NID, EmptyNode>,
    ) -> Result<(), CatalogError> {
        self.update_group(group_id, |record| {
            if let Some(stored) = record.membership() {
                match membership.log_id().cmp(stored.log_id()) {
                    Ordering::Less => {
                        return Err(CatalogError::StaleMembership);
                    }
                    Ordering::Equal if membership != stored => {
                        return Err(CatalogError::ConflictingMembership);
                    }
                    Ordering::Equal | Ordering::Greater => {}
                }
            }
            record.set_membership(membership.clone());
            Ok(())
        })
    }

    /// Stores the newest state-machine position without allowing it to move back.
    pub fn save_applied_log_id(
        &self,
        group_id: &GID,
        log_id: &LogId<NID>,
    ) -> Result<(), CatalogError> {
        self.update_group(group_id, |record| {
            if let Some(stored) = record.applied_log_id() {
                if log_id.index < stored.index
                    || (log_id.index > stored.index
                        && log_id.leader_id.term < stored.leader_id.term)
                {
                    return Err(CatalogError::StaleAppliedLogId);
                }
                if log_id.index == stored.index && log_id != stored {
                    return Err(CatalogError::ConflictingAppliedLogId);
                }
            }
            record.set_applied_log_id(log_id.clone());
            Ok(())
        })
    }

    /// Applies one atomic update to an existing group row.
    fn update_group(
        &self,
        group_id: &GID,
        update: impl FnOnce(&mut GroupRecord<GID, NID>) -> Result<(), CatalogError>,
    ) -> Result<(), CatalogError> {
        let key = encode_group_id(group_id, &self.group_ids, self.limits)?;
        let write = self.database.begin_write()?;
        let mut groups = write.open_table(GROUPS)?;
        let mut record = {
            let stored = groups
                .get(key.as_slice())?
                .ok_or(CatalogError::GroupNotFound)?;
            self.decode_checked(key.as_slice(), stored.value())?
        };
        if record.group_id() != group_id {
            return Err(CatalogError::GroupIdentityMismatch);
        }

        update(&mut record)?;
        let encoded = encode_group_record(&record, &self.group_ids, &self.node_ids, self.limits)?;
        groups.insert(key.as_slice(), encoded.as_slice())?;
        drop(groups);
        write.commit()?;
        Ok(())
    }

    /// Decodes one row and checks that its key names the stored group.
    fn decode_checked(
        &self,
        key: &[u8],
        bytes: &[u8],
    ) -> Result<GroupRecord<GID, NID>, CatalogError> {
        let record = decode_group_record(bytes, &self.group_ids, &self.node_ids, self.limits)?;
        let expected = encode_group_id(record.group_id(), &self.group_ids, self.limits)?;
        if expected != key {
            return Err(CatalogError::GroupIdentityMismatch);
        }
        Ok(record)
    }
}
