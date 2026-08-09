use std::io;
use std::marker::PhantomData;

use openraft::storage::RaftStateMachine;
use openraft::{
    Entry, EntryPayload, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};

use crate::state_machine_shutdown::StateMachineOwner;
use crate::{ApplicationStateMachine, ApplyContext, EntryResponse, RaftApplication, TypeConfig};

/// Returns the fatal storage error used when disabled snapshots are requested.
pub(crate) fn snapshots_disabled_error<NID>() -> StorageError<NID>
where
    NID: openraft::NodeId,
{
    let source = io::Error::new(
        io::ErrorKind::Unsupported,
        "snapshots are disabled for the in-memory Raft runtime",
    );
    StorageIOError::read_snapshot(None, &source).into()
}

/// Snapshot builder used while the in-memory runtime has snapshots disabled.
pub(crate) struct DisabledSnapshotBuilder<A>
where
    A: RaftApplication,
{
    application: PhantomData<fn() -> A>,
}

impl<A> Default for DisabledSnapshotBuilder<A>
where
    A: RaftApplication,
{
    /// Creates a disabled snapshot builder marker.
    fn default() -> Self {
        Self {
            application: PhantomData,
        }
    }
}

impl<A> RaftSnapshotBuilder<TypeConfig<A>> for DisabledSnapshotBuilder<A>
where
    A: RaftApplication,
{
    /// Rejects snapshot construction because Step 2 stores no snapshot format.
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig<A>>, StorageError<A::NodeId>> {
        Err(snapshots_disabled_error())
    }
}

/// Adapts the typed application state machine to OpenRaft.
pub(crate) struct MemoryStateMachine<A, S>
where
    A: RaftApplication,
    S: ApplicationStateMachine<A>,
{
    application: S,
    last_applied: Option<LogId<A::NodeId>>,
    last_membership: StoredMembership<A::NodeId, A::Node>,
    // This field stays last so application resources are released before
    // successful node shutdown becomes observable.
    _shutdown_owner: StateMachineOwner,
}

impl<A, S> MemoryStateMachine<A, S>
where
    A: RaftApplication,
    S: ApplicationStateMachine<A>,
{
    /// Creates an empty Raft adapter around one application state machine.
    pub(crate) fn new(application: S, shutdown_owner: StateMachineOwner) -> Self {
        Self {
            application,
            last_applied: None,
            last_membership: StoredMembership::default(),
            _shutdown_owner: shutdown_owner,
        }
    }
}

impl<A, S> RaftStateMachine<TypeConfig<A>> for MemoryStateMachine<A, S>
where
    A: RaftApplication,
    S: ApplicationStateMachine<A>,
{
    type SnapshotBuilder = DisabledSnapshotBuilder<A>;

    /// Returns the latest applied entry and membership.
    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<A::NodeId>>,
            StoredMembership<A::NodeId, A::Node>,
        ),
        StorageError<A::NodeId>,
    > {
        Ok((self.last_applied.clone(), self.last_membership.clone()))
    }

    /// Applies committed entries to the typed application in log order.
    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<EntryResponse<A::Response>>, StorageError<A::NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig<A>>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;
            let response = match entry.payload {
                EntryPayload::Blank => EntryResponse::Internal,
                EntryPayload::Membership(membership) => {
                    self.last_membership = StoredMembership::new(Some(log_id.clone()), membership);
                    EntryResponse::Internal
                }
                EntryPayload::Normal(command) => {
                    let context = ApplyContext::new(log_id.leader_id.term, log_id.index);
                    let response = self
                        .application
                        .apply(context, command)
                        .await
                        .map_err(|error| StorageIOError::apply(log_id.clone(), &error))?;
                    EntryResponse::Application(response)
                }
            };

            self.last_applied = Some(log_id);
            responses.push(response);
        }

        Ok(responses)
    }

    /// Returns a builder that explicitly rejects snapshot requests.
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        DisabledSnapshotBuilder::default()
    }

    /// Rejects incoming snapshot allocation because snapshots are disabled.
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<A::Snapshot>, StorageError<A::NodeId>> {
        Err(snapshots_disabled_error())
    }

    /// Rejects snapshot installation because snapshots are disabled.
    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<A::NodeId, A::Node>,
        _snapshot: Box<A::Snapshot>,
    ) -> Result<(), StorageError<A::NodeId>> {
        Err(snapshots_disabled_error())
    }

    /// Reports that no snapshot exists in this non-durable runtime.
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig<A>>>, StorageError<A::NodeId>> {
        Ok(None)
    }
}
