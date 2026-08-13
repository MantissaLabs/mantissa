use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use mantissa_raft::ApplicationStateMachine;
use mantissa_raft::durable_log::{
    EncryptedLog, EncryptedLogSettings, GroupEncryptionKey, GroupKeyProvider, LogLimits,
};
use mantissa_raft::protocol::ProtocolLimits;
use mantissa_raft::runtime::{GroupStarter, RunningGroup};
use mantissa_raft::transport::{IncomingSnapshots, TcpNode, TcpNodeError, TransportError};
use mantissa_volume::catalog::{ReplicaCatalog, ReplicaKey, ReplicaRecord};
use mantissa_volume::protocol::{ReplicaKeyAdapter, UuidNodeIdAdapter, VolumeCommandAdapter};
use mantissa_volume::state_machine::{VolumeControlStateMachine, VolumeSnapshot};
use mantissa_volume::storage::remove_control_state;
use mantissa_volume::storage::replica_file::io_admission::AppliedVolumeStateRegistry;
use mantissa_volume::storage::{VolumeControlStateReader, VolumeControlStateStore};
use openraft::{Config as RaftConfig, EmptyNode, EntryPayload, LogId};
use thiserror::Error;
use tracing::{debug, info};
use uuid::Uuid;

use super::{VolumeApplication, VolumeCatalog, VolumeTransport};

type VolumeNode = TcpNode<
    VolumeApplication,
    ReplicaKey,
    ReplicaKeyAdapter,
    UuidNodeIdAdapter,
    VolumeCommandAdapter,
    super::StoragePeerDirectory,
>;

/// Creates bounded receivers for snapshots sent by another volume member.
struct VolumeIncomingSnapshots {
    max_state_bytes: usize,
}

impl IncomingSnapshots<VolumeApplication> for VolumeIncomingSnapshots {
    /// Creates an empty receiver bounded by the configured control-state size.
    fn begin(
        &self,
        _meta: openraft::SnapshotMeta<Uuid, EmptyNode>,
    ) -> BoxFuture<'static, Result<VolumeSnapshot, TransportError>> {
        let snapshot = VolumeSnapshot::receive(self.max_state_bytes);
        Box::pin(std::future::ready(Ok(snapshot)))
    }
}

/// Opens one saved local volume and its durable Raft member.
#[derive(Clone)]
pub(super) struct VolumeGroupStarter {
    pub(super) node_id: Uuid,
    pub(super) replicas: ReplicaCatalog,
    pub(super) groups: VolumeCatalog,
    pub(super) database: Arc<redb::Database>,
    pub(super) transport: Arc<VolumeTransport>,
    pub(super) commands: Arc<VolumeCommandAdapter>,
    pub(super) log_key_seed: [u8; 32],
    pub(super) raft_config: RaftConfig,
    pub(super) protocol_limits: ProtocolLimits,
    pub(super) log_limits: LogLimits,
    pub(super) max_state_bytes: usize,
    pub(super) applied_volume_states: Arc<AppliedVolumeStateRegistry>,
    pub(super) replay_batch_entries: usize,
}

impl GroupStarter<ReplicaKey> for VolumeGroupStarter {
    type Group = RunningVolumeNode;
    type Error = VolumeGroupError;

    /// Recovers control state and committed entries before starting one member.
    async fn start(&self, group_id: ReplicaKey) -> Result<Self::Group, Self::Error> {
        let record = self
            .replicas
            .replica(group_id)?
            .ok_or(VolumeGroupError::ReplicaMissing)?;
        let starter = self.clone();
        let opened = tokio::task::spawn_blocking(move || starter.open_group(record))
            .await
            .map_err(VolumeGroupError::Join)??;
        let node = TcpNode::start_stored(
            self.node_id,
            group_id,
            self.raft_config.clone(),
            Arc::clone(&self.transport),
            self.groups.clone(),
            opened.log,
            Arc::clone(&self.commands),
            opened.state,
            opened.last_applied,
            Some(Arc::new(VolumeIncomingSnapshots {
                max_state_bytes: self.max_state_bytes,
            })),
        )
        .await
        .map_err(|error| VolumeGroupError::Raft(Box::new(error)))?;
        let metrics = node.metrics();
        info!(
            target: "mantissa::volumes::raft",
            volume_id = %group_id.volume_id().as_uuid(),
            generation = group_id.generation().get(),
            local_node_id = %self.node_id,
            term = metrics.term,
            state = ?metrics.state,
            leader_node_id = ?metrics.leader,
            applied_log_index = ?metrics.last_applied_index,
            "started local volume Raft member"
        );
        Ok(RunningVolumeNode {
            group_id,
            node,
            reader: opened.reader,
        })
    }
}

/// Gives the shared group runtime one error type for start and stop.
pub(super) struct RunningVolumeNode {
    group_id: ReplicaKey,
    node: VolumeNode,
    reader: VolumeControlStateReader,
}

impl RunningVolumeNode {
    /// Idempotently initializes a pristine group with its exact three voters.
    pub(super) async fn initialize(
        &self,
        voter_node_ids: BTreeSet<Uuid>,
    ) -> Result<(), VolumeGroupError> {
        if voter_node_ids.len() != 3 {
            return Err(VolumeGroupError::InvalidBootstrapVoters);
        }
        self.node
            .initialize(
                voter_node_ids
                    .iter()
                    .copied()
                    .map(|node_id| (node_id, EmptyNode {}))
                    .collect::<BTreeMap<_, _>>(),
            )
            .await
            .map_err(|error| VolumeGroupError::Raft(Box::new(error)))?;
        let metrics = self.node.metrics();
        info!(
            target: "mantissa::volumes::raft",
            volume_id = %self.group_id.volume_id().as_uuid(),
            generation = self.group_id.generation().get(),
            local_node_id = %metrics.node_id,
            term = metrics.term,
            leader_node_id = ?metrics.leader,
            voter_node_ids = ?voter_node_ids,
            "initialized volume Raft group"
        );
        Ok(())
    }

    /// Waits until this member observes a current leader without starting an election.
    pub(super) async fn wait_for_leader(
        &self,
        timeout: Duration,
    ) -> Result<Uuid, VolumeGroupError> {
        let leader = self
            .node
            .wait_for_leader(timeout)
            .await
            .map_err(VolumeGroupError::Wait)?;
        let metrics = self.node.metrics();
        debug!(
            target: "mantissa::volumes::raft",
            volume_id = %self.group_id.volume_id().as_uuid(),
            generation = self.group_id.generation().get(),
            local_node_id = %metrics.node_id,
            term = metrics.term,
            leader_node_id = %leader,
            "volume Raft leader is ready"
        );
        Ok(leader)
    }

    /// Waits until this member has applied the leader's reported control entry.
    pub(super) async fn wait_for_applied(
        &self,
        log_index: u64,
        timeout: Duration,
    ) -> Result<(), VolumeGroupError> {
        self.node
            .wait_for_applied(log_index, timeout)
            .await
            .map(|_| ())
            .map_err(VolumeGroupError::Wait)
    }

    /// Commits one volume command through the current Raft leader.
    pub(super) async fn write(
        &self,
        command: mantissa_volume::control_state::VolumeCommand,
    ) -> Result<
        mantissa_raft::memory::WriteResult<
            Uuid,
            mantissa_volume::control_state::VolumeCommandResponse,
        >,
        VolumeGroupError,
    > {
        validate_volume_command_key(self.group_id, &command)?;
        self.node
            .write(command)
            .await
            .map_err(|error| VolumeGroupError::Raft(Box::new(error)))
    }

    /// Confirms this member still leads a quorum before returning local control state.
    pub(super) async fn leader_state(
        &self,
        timeout: Duration,
    ) -> Result<mantissa_volume::control_state::VolumeControlState, VolumeGroupError> {
        self.node
            .confirm_leadership_for_read(timeout)
            .await
            .map_err(|error| VolumeGroupError::Raft(Box::new(error)))?;
        Ok(self.reader.state())
    }

    /// Adds one non-voting replica and waits for it to catch up.
    pub(super) async fn add_learner(&self, node_id: Uuid) -> Result<(), VolumeGroupError> {
        let previous_members = self.node.member_ids();
        self.node
            .add_learner(node_id)
            .await
            .map_err(|error| VolumeGroupError::Raft(Box::new(error)))?;
        if !previous_members.contains(&node_id) {
            let metrics = self.node.metrics();
            info!(
                target: "mantissa::volumes::raft",
                volume_id = %self.group_id.volume_id().as_uuid(),
                generation = self.group_id.generation().get(),
                local_node_id = %metrics.node_id,
                term = metrics.term,
                learner_node_id = %node_id,
                "added learner to volume Raft group"
            );
        }
        Ok(())
    }

    /// Replaces the current voters with the exact requested set.
    pub(super) async fn set_voters(&self, voters: BTreeSet<Uuid>) -> Result<(), VolumeGroupError> {
        let previous_voters = self.node.voter_ids();
        self.node
            .set_voters(voters.clone())
            .await
            .map_err(|error| VolumeGroupError::Raft(Box::new(error)))?;
        if previous_voters != voters {
            let metrics = self.node.metrics();
            info!(
                target: "mantissa::volumes::raft",
                volume_id = %self.group_id.volume_id().as_uuid(),
                generation = self.group_id.generation().get(),
                local_node_id = %metrics.node_id,
                term = metrics.term,
                previous_voter_node_ids = ?previous_voters,
                voter_node_ids = ?voters,
                "changed volume Raft voters"
            );
        }
        Ok(())
    }

    /// Removes one cancelled replacement while it is still only a learner.
    pub(super) async fn remove_learner(&self, node_id: Uuid) -> Result<(), VolumeGroupError> {
        let previous_members = self.node.member_ids();
        self.node
            .remove_learner(node_id)
            .await
            .map_err(|error| VolumeGroupError::Raft(Box::new(error)))?;
        if previous_members.contains(&node_id) {
            let metrics = self.node.metrics();
            info!(
                target: "mantissa::volumes::raft",
                volume_id = %self.group_id.volume_id().as_uuid(),
                generation = self.group_id.generation().get(),
                local_node_id = %metrics.node_id,
                term = metrics.term,
                learner_node_id = %node_id,
                "removed learner from volume Raft group"
            );
        }
        Ok(())
    }

    /// Returns the latest local Raft metrics used by health reports.
    pub(super) fn metrics(&self) -> mantissa_raft::memory::GroupMetrics<Uuid> {
        self.node.metrics()
    }

    /// Returns the current copies allowed to receive ordered data writes.
    pub(super) fn voter_node_ids(&self) -> BTreeSet<Uuid> {
        self.node.voter_ids()
    }

    /// Returns every current Raft member, including non-voting learners.
    pub(super) fn member_node_ids(&self) -> BTreeSet<Uuid> {
        self.node.member_ids()
    }

    /// Returns whether Raft is between two committed voter configurations.
    pub(super) fn membership_is_joint(&self) -> bool {
        self.node.membership_is_joint()
    }

    /// Returns a copy of the newest locally applied volume state.
    pub(super) fn state(&self) -> mantissa_volume::control_state::VolumeControlState {
        self.reader.state()
    }
}

/// Rejects initialization routed to a different durable Raft group.
fn validate_volume_command_key(
    expected: ReplicaKey,
    command: &mantissa_volume::control_state::VolumeCommand,
) -> Result<(), VolumeGroupError> {
    if let mantissa_volume::control_state::VolumeCommand::Initialize(initialize) = command
        && ReplicaKey::from(&initialize.descriptor) != expected
    {
        return Err(VolumeGroupError::WrongVolumeKey);
    }
    Ok(())
}

impl RunningGroup for RunningVolumeNode {
    type Error = VolumeGroupError;

    /// Stops the recovered OpenRaft member and its inbound route.
    async fn shutdown(&self) -> Result<(), Self::Error> {
        let metrics = self.node.metrics();
        self.node
            .shutdown()
            .await
            .map_err(|error| VolumeGroupError::Raft(Box::new(error)))?;
        debug!(
            target: "mantissa::volumes::raft",
            volume_id = %self.group_id.volume_id().as_uuid(),
            generation = self.group_id.generation().get(),
            local_node_id = %metrics.node_id,
            term = metrics.term,
            leader_node_id = ?metrics.leader,
            "stopped local volume Raft member"
        );
        Ok(())
    }
}

impl VolumeGroupStarter {
    /// Opens the small control store, encrypted log, and state machine.
    fn open_group(&self, record: ReplicaRecord) -> Result<OpenedVolumeGroup, VolumeGroupError> {
        let group_id = record.key();
        let saved_applied = self
            .groups
            .group(&group_id)?
            .ok_or(VolumeGroupError::GroupMissing)?
            .applied_log_id()
            .copied();
        let log_directory = record.path(self.replicas.pool_root()).join("raft");
        let (replica, reader, application_applied) = VolumeControlStateStore::new(
            Arc::clone(&self.database),
            group_id,
            self.max_state_bytes,
            Arc::clone(&self.applied_volume_states),
        )?;
        std::fs::create_dir_all(&log_directory)?;
        let key_provider = VolumeLogKey {
            group_id,
            seed: self.log_key_seed,
        };
        let mut log = EncryptedLog::open(
            EncryptedLogSettings::new(
                log_directory,
                Arc::clone(&self.database),
                group_id,
                ReplicaKeyAdapter,
                UuidNodeIdAdapter,
                self.protocol_limits,
                self.log_limits,
            ),
            &key_provider,
        )?;
        let state = VolumeControlStateMachine::open(replica)?;
        let last_applied = newest_applied_log_id(&log, application_applied, saved_applied)?;
        let (state, last_applied) = replay_committed(
            state,
            &mut log,
            last_applied,
            ReplayContext {
                groups: &self.groups,
                group_id,
                batch_entries: self.replay_batch_entries,
                commands: self.commands.as_ref(),
            },
        )?;
        Ok(OpenedVolumeGroup {
            log,
            state,
            reader,
            last_applied,
        })
    }

    /// Removes durable log and application rows after the runtime group stops.
    pub(super) fn remove_closed_storage(
        &self,
        record: &ReplicaRecord,
    ) -> Result<(), VolumeGroupError> {
        let group_id = record.key();
        EncryptedLog::<ReplicaKey, Uuid, ReplicaKeyAdapter, UuidNodeIdAdapter>::remove_closed(
            EncryptedLogSettings::new(
                record.path(self.replicas.pool_root()).join("raft"),
                Arc::clone(&self.database),
                group_id,
                ReplicaKeyAdapter,
                UuidNodeIdAdapter,
                self.protocol_limits,
                self.log_limits,
            ),
        )?;
        remove_control_state(&self.database, group_id)?;
        Ok(())
    }
}

/// Open files and state passed into OpenRaft after recovery.
struct OpenedVolumeGroup {
    log: EncryptedLog<ReplicaKey, Uuid, ReplicaKeyAdapter, UuidNodeIdAdapter>,
    state: VolumeControlStateMachine<VolumeControlStateStore>,
    reader: VolumeControlStateReader,
    last_applied: Option<LogId<Uuid>>,
}

/// Derives one node-local control-log key for the matching volume generation.
struct VolumeLogKey {
    group_id: ReplicaKey,
    seed: [u8; 32],
}

impl GroupKeyProvider<ReplicaKey> for VolumeLogKey {
    type Error = VolumeGroupError;

    /// Returns a separate key for the exact volume generation being opened.
    fn key_for_group(&self, group_id: &ReplicaKey) -> Result<GroupEncryptionKey, Self::Error> {
        if group_id != &self.group_id {
            return Err(VolumeGroupError::WrongGroup);
        }
        let mut hasher = blake3::Hasher::new_keyed(&self.seed);
        hasher.update(b"mantissa volume control log key v1");
        hasher.update(group_id.volume_id().as_bytes());
        hasher.update(&group_id.generation().get().to_le_bytes());
        Ok(GroupEncryptionKey::new(*hasher.finalize().as_bytes()))
    }
}

/// Shared catalog and decoding values used while replaying one group.
struct ReplayContext<'a> {
    groups: &'a VolumeCatalog,
    group_id: ReplicaKey,
    batch_entries: usize,
    commands: &'a VolumeCommandAdapter,
}

/// Replays every committed entry not yet saved in the local volume files.
fn replay_committed(
    mut state: VolumeControlStateMachine<VolumeControlStateStore>,
    log: &mut EncryptedLog<ReplicaKey, Uuid, ReplicaKeyAdapter, UuidNodeIdAdapter>,
    applied: Option<LogId<Uuid>>,
    context: ReplayContext<'_>,
) -> Result<
    (
        VolumeControlStateMachine<VolumeControlStateStore>,
        Option<LogId<Uuid>>,
    ),
    VolumeGroupError,
> {
    let mut cursor = applied
        .as_ref()
        .map(|log_id| mantissa_raft::ApplyContext::new(log_id.leader_id.term, log_id.index));
    let mut last_applied = applied;
    loop {
        let entries = log.read_committed_after::<VolumeApplication, _>(
            cursor,
            context.batch_entries,
            context.commands,
        )?;
        if entries.is_empty() {
            return Ok((state, last_applied));
        }
        for entry in entries {
            let log_id = entry.log_id;
            match entry.payload {
                EntryPayload::Normal(command) => {
                    let context =
                        mantissa_raft::ApplyContext::new(log_id.leader_id.term, log_id.index);
                    futures::executor::block_on(state.apply(context, command))?;
                }
                EntryPayload::Membership(membership) => {
                    context.groups.save_membership(
                        &context.group_id,
                        &openraft::StoredMembership::new(Some(log_id), membership),
                    )?;
                }
                EntryPayload::Blank => {}
            }
            context
                .groups
                .save_applied_log_id(&context.group_id, &log_id)?;
            cursor = Some(mantissa_raft::ApplyContext::new(
                log_id.leader_id.term,
                log_id.index,
            ));
            last_applied = Some(log_id);
        }
    }
}

/// Chooses the newest saved apply position and checks it against the log.
fn newest_applied_log_id(
    log: &EncryptedLog<ReplicaKey, Uuid, ReplicaKeyAdapter, UuidNodeIdAdapter>,
    application_applied: Option<mantissa_raft::ApplyContext>,
    raft_applied: Option<LogId<Uuid>>,
) -> Result<Option<LogId<Uuid>>, VolumeGroupError> {
    let application_applied = application_applied
        .map(|context| full_log_id(log, context))
        .transpose()?;
    let raft_applied = raft_applied
        .map(|saved| check_saved_applied_log_id(log, saved))
        .transpose()?;
    match (application_applied, raft_applied) {
        (None, None) => Ok(None),
        (Some(log_id), None) | (None, Some(log_id)) => Ok(Some(log_id)),
        (Some(application), Some(raft)) if application.index > raft.index => Ok(Some(application)),
        (Some(application), Some(raft)) if raft.index > application.index => Ok(Some(raft)),
        (Some(application), Some(raft)) if application == raft => Ok(Some(application)),
        (Some(application), Some(raft)) => {
            Err(VolumeGroupError::AppliedLogIdConflict { application, raft })
        }
    }
}

/// Checks a group-catalog apply position against the encrypted log.
fn check_saved_applied_log_id(
    log: &EncryptedLog<ReplicaKey, Uuid, ReplicaKeyAdapter, UuidNodeIdAdapter>,
    saved: LogId<Uuid>,
) -> Result<LogId<Uuid>, VolumeGroupError> {
    let stored = match log.location(saved.index)? {
        Some(location) => *location.log_id(),
        None if log
            .last_removed_log_id()?
            .is_some_and(|removed| removed.index == saved.index) =>
        {
            log.last_removed_log_id()?
                .ok_or(VolumeGroupError::SavedAppliedEntryMissing { index: saved.index })?
        }
        None => {
            return Err(VolumeGroupError::SavedAppliedEntryMissing { index: saved.index });
        }
    };
    if stored != saved {
        return Err(VolumeGroupError::SavedAppliedLogIdMismatch { saved, log: stored });
    }
    Ok(saved)
}

/// Restores the leader node ID omitted from the volume's saved apply position.
fn full_log_id(
    log: &EncryptedLog<ReplicaKey, Uuid, ReplicaKeyAdapter, UuidNodeIdAdapter>,
    context: mantissa_raft::ApplyContext,
) -> Result<LogId<Uuid>, VolumeGroupError> {
    let stored = match log.location(context.index())? {
        Some(location) => *location.log_id(),
        None if log
            .last_removed_log_id()?
            .is_some_and(|removed| removed.index == context.index()) =>
        {
            log.last_removed_log_id()?
                .ok_or(VolumeGroupError::AppliedEntryMissing {
                    index: context.index(),
                })?
        }
        None => {
            return Err(VolumeGroupError::AppliedEntryMissing {
                index: context.index(),
            });
        }
    };
    if stored.leader_id.term != context.term() {
        return Err(VolumeGroupError::AppliedTermMismatch {
            index: context.index(),
            saved: context.term(),
            log: stored.leader_id.term,
        });
    }
    Ok(stored)
}

/// Reports why one saved volume group could not start.
#[derive(Debug, Error)]
pub(super) enum VolumeGroupError {
    /// Initialization descriptor belongs to another durable group.
    #[error("volume control state command targets another Raft group")]
    WrongVolumeKey,

    /// A pristine volume group must begin with exactly three voters.
    #[error("volume bootstrap requires exactly three voters")]
    InvalidBootstrapVoters,

    /// No local replica row matches the active group.
    #[error("active volume Raft group has no local replica")]
    ReplicaMissing,

    /// No durable Raft group row matches the active local replica.
    #[error("active local replica has no Raft group row")]
    GroupMissing,

    /// The key provider was asked for another group.
    #[error("volume log key was requested for the wrong group")]
    WrongGroup,

    /// The local application refers to a missing Raft log entry.
    #[error("local replica refers to missing Raft log index {index}")]
    AppliedEntryMissing {
        /// Missing log index.
        index: u64,
    },

    /// The local application and encrypted log disagree about one term.
    #[error(
        "local replica log index {index} has term {saved}, but the encrypted log has term {log}"
    )]
    AppliedTermMismatch {
        /// Conflicting log index.
        index: u64,

        /// Term saved by the application.
        saved: u64,

        /// Term stored in the encrypted log.
        log: u64,
    },

    /// The group row refers to a missing encrypted log entry.
    #[error("saved applied Raft log index {index} is missing")]
    SavedAppliedEntryMissing {
        /// Missing log index.
        index: u64,
    },

    /// The group row and encrypted log disagree about one applied entry.
    #[error("saved applied Raft log ID {saved} does not match encrypted log ID {log}")]
    SavedAppliedLogIdMismatch {
        /// Log ID stored in the local group row.
        saved: LogId<Uuid>,

        /// Log ID stored in the encrypted frame.
        log: LogId<Uuid>,
    },

    /// The volume files and group row name different entries at one index.
    #[error("volume files applied {application}, but the local Raft group recorded {raft}")]
    AppliedLogIdConflict {
        /// Log ID restored from the volume files.
        application: LogId<Uuid>,

        /// Log ID restored from the local group row.
        raft: LogId<Uuid>,
    },

    /// The local replica catalog could not be read or changed.
    #[error(transparent)]
    Catalog(#[from] mantissa_volume::catalog::CatalogError),

    /// The Raft group catalog could not be read or changed.
    #[error(transparent)]
    GroupCatalog(#[from] mantissa_raft::catalog::CatalogError),

    /// The encrypted Raft log could not be opened or replayed.
    #[error(transparent)]
    Log(#[from] mantissa_raft::durable_log::LogError),

    /// The volume state machine could not recover or apply an entry.
    #[error(transparent)]
    State(
        #[from]
        mantissa_volume::state_machine::VolumeStateMachineError<
            mantissa_volume::storage::VolumeControlStateStoreError,
        >,
    ),

    /// The durable volume control-state store could not open.
    #[error(transparent)]
    ControlStateStore(#[from] mantissa_volume::storage::VolumeControlStateStoreError),

    /// A local directory could not be created.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// A blocking recovery task could not be joined.
    #[error("could not join volume group recovery")]
    Join(#[source] tokio::task::JoinError),

    /// OpenRaft could not start the recovered member.
    #[error(transparent)]
    Raft(#[from] Box<TcpNodeError<Uuid, EmptyNode>>),

    /// A metrics-based group wait did not finish safely.
    #[error(transparent)]
    Wait(#[from] mantissa_raft::memory::WaitError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use mantissa_volume::control_state::{InitializeVolume, VolumeCommand};
    use mantissa_volume::{
        VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId, VolumeNodeId,
    };
    use uuid::Uuid;

    use super::{VolumeGroupError, validate_volume_command_key};
    use mantissa_volume::catalog::ReplicaKey;

    /// Builds one descriptor for command-to-group binding tests.
    fn descriptor(volume_id: u128) -> VolumeDescriptor {
        VolumeDescriptor::new(
            VolumeId::new(Uuid::from_u128(volume_id)).expect("non-zero test volume ID"),
            VolumeGeneration::new(1).expect("non-zero test generation"),
            64 << 20,
            VolumeBlockSizes::supported(),
        )
        .expect("valid test descriptor")
    }

    /// Initialization is rejected before Raft when its descriptor names another group.
    #[test]
    fn initialization_command_is_bound_to_its_raft_group() {
        let expected = descriptor(1);
        let matching = VolumeCommand::Initialize(InitializeVolume {
            descriptor: expected.clone(),
            initial_copies: [1_u128, 2, 3]
                .into_iter()
                .map(|value| {
                    VolumeNodeId::new(Uuid::from_u128(value)).expect("non-zero test node ID")
                })
                .collect::<BTreeSet<_>>(),
        });
        assert!(validate_volume_command_key(ReplicaKey::from(&expected), &matching).is_ok());

        let mismatched = VolumeCommand::Initialize(InitializeVolume {
            descriptor: descriptor(2),
            initial_copies: match matching {
                VolumeCommand::Initialize(command) => command.initial_copies,
                _ => unreachable!("test command is initialization"),
            },
        });
        assert!(matches!(
            validate_volume_command_key(ReplicaKey::from(&expected), &mismatched),
            Err(VolumeGroupError::WrongVolumeKey)
        ));
    }
}
