use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    Entry, EntryPayload, LogId, LogState, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use parking_lot::Mutex;
use thiserror::Error;

use crate::catalog::{CatalogError, GroupCatalog, GroupIdAdapter};
use crate::durable_log::{EncryptedLog, LogError};
use crate::protocol::{ApplicationCommandAdapter, NodeIdAdapter};
use crate::state_machine_shutdown::StateMachineOwner;
use crate::{
    ApplyContext, DurableApplicationStateMachine, EntryResponse, RaftApplication, TypeConfig,
};

type SharedEncryptedLog<GID, NID, G, N> = Arc<SharedDurableLog<GID, NID, G, N>>;

/// One log allocation retained by every OpenRaft reader and blocking call.
struct SharedDurableLog<GID, NID, G, N>
where
    NID: openraft::NodeId,
{
    log: Mutex<EncryptedLog<GID, NID, G, N>>,
    // Keep this last so all file handles are released before completion is visible.
    _shutdown_owner: DurableLogShutdownOwner,
}

impl<GID, NID, G, N> SharedDurableLog<GID, NID, G, N>
where
    NID: openraft::NodeId,
{
    /// Locks the exact encrypted log shared by all OpenRaft readers.
    fn lock(&self) -> parking_lot::MutexGuard<'_, EncryptedLog<GID, NID, G, N>> {
        self.log.lock()
    }
}

/// Reusable completion for every durable log owner and accepted blocking call.
pub(crate) struct DurableLogShutdown {
    finished: AtomicBool,
    changed: tokio::sync::Notify,
}

impl DurableLogShutdown {
    /// Creates the first durable owner and its independent shutdown observer.
    fn pair() -> (DurableLogShutdownOwner, Arc<Self>) {
        let shutdown = Arc::new(Self {
            finished: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
        });
        let owner = DurableLogShutdownOwner {
            shutdown: Arc::clone(&shutdown),
        };
        (owner, shutdown)
    }

    /// Waits repeatedly until no log reader or accepted disk call remains.
    pub(crate) async fn wait(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.finished.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

/// Publishes when the last shared durable-log allocation is released.
struct DurableLogShutdownOwner {
    shutdown: Arc<DurableLogShutdown>,
}

impl Drop for DurableLogShutdownOwner {
    /// Makes terminal durable-log release visible to every current or later waiter.
    fn drop(&mut self) {
        self.shutdown.finished.store(true, Ordering::Release);
        self.shutdown.changed.notify_waiters();
    }
}

/// Internal snapshot error converted into an OpenRaft storage failure.
#[derive(Debug, Error)]
enum DurableSnapshotError {
    /// OpenRaft supplied a snapshot without an applied log position.
    #[error("a durable Raft snapshot must include its last applied log entry")]
    MissingLogId,

    /// OpenRaft asked one builder to return its owned snapshot twice.
    #[error("a durable Raft snapshot builder can only be consumed once")]
    BuilderAlreadyUsed,
}

/// One immutable application snapshot and the Raft state captured with it.
pub(crate) struct DurableSnapshotBuilder<A>
where
    A: RaftApplication,
{
    snapshot: Mutex<Option<Result<A::Snapshot, A::Error>>>,
    meta: SnapshotMeta<A::NodeId, A::Node>,
}

impl<A> RaftSnapshotBuilder<TypeConfig<A>> for DurableSnapshotBuilder<A>
where
    A: RaftApplication,
{
    /// Returns the exact application state copied when this builder was made.
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig<A>>, StorageError<A::NodeId>> {
        let snapshot = self.snapshot.lock().take().ok_or_else(|| {
            StorageIOError::read_snapshot(None, &DurableSnapshotError::BuilderAlreadyUsed)
        })?;
        let snapshot = snapshot.map_err(|error| StorageIOError::read_snapshot(None, &error))?;
        Ok(Snapshot {
            meta: self.meta.clone(),
            snapshot: Box::new(snapshot),
        })
    }
}

/// Keeps the failed log index without returning OpenRaft's large error type
/// from a blocking task.
enum ReadEntriesError {
    Locations(Box<LogError>),
    Entry { index: u64, source: Box<LogError> },
}

/// OpenRaft log storage backed by encrypted segment files and the group catalog.
pub(crate) struct DurableLogStore<A, GID, G, N, C>
where
    A: RaftApplication,
{
    group_id: GID,
    catalog: GroupCatalog<GID, A::NodeId, G, N>,
    log: SharedEncryptedLog<GID, A::NodeId, G, N>,
    commands: Arc<C>,
}

impl<A, GID, G, N, C> DurableLogStore<A, GID, G, N, C>
where
    A: RaftApplication,
{
    /// Connects an opened encrypted log and durable group row to OpenRaft.
    pub fn new(
        group_id: GID,
        catalog: GroupCatalog<GID, A::NodeId, G, N>,
        log: EncryptedLog<GID, A::NodeId, G, N>,
        commands: Arc<C>,
    ) -> (Self, Arc<DurableLogShutdown>) {
        let (shutdown_owner, shutdown) = DurableLogShutdown::pair();
        (
            Self {
                group_id,
                catalog,
                log: Arc::new(SharedDurableLog {
                    log: Mutex::new(log),
                    _shutdown_owner: shutdown_owner,
                }),
                commands,
            },
            shutdown,
        )
    }
}

impl<A, GID, G, N, C> Clone for DurableLogStore<A, GID, G, N, C>
where
    A: RaftApplication,
    GID: Clone,
    G: Clone,
    N: Clone,
{
    /// Clones a handle to the same encrypted log and catalog row.
    fn clone(&self) -> Self {
        Self {
            group_id: self.group_id.clone(),
            catalog: self.catalog.clone(),
            log: Arc::clone(&self.log),
            commands: Arc::clone(&self.commands),
        }
    }
}

impl<A, GID, G, N, C> RaftLogReader<TypeConfig<A>> for DurableLogStore<A, GID, G, N, C>
where
    A: RaftApplication<Node = openraft::EmptyNode>,
    A::Command: Clone,
    GID: Clone + Eq + Send + Sync + 'static,
    G: Clone + GroupIdAdapter<GID> + Send + Sync + 'static,
    N: Clone + NodeIdAdapter<A::NodeId> + Send + Sync + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
{
    /// Reads and authenticates the requested log entries in index order.
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig<A>>>, StorageError<A::NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let first_index = match range.start_bound() {
            Bound::Included(index) => *index,
            Bound::Excluded(index) => match index.checked_add(1) {
                Some(index) => index,
                None => return Ok(Vec::new()),
            },
            Bound::Unbounded => 0,
        };
        let last_index = match range.end_bound() {
            Bound::Included(index) => *index,
            Bound::Excluded(index) => match index.checked_sub(1) {
                Some(index) => index,
                None => return Ok(Vec::new()),
            },
            Bound::Unbounded => u64::MAX,
        };
        if first_index > last_index {
            return Ok(Vec::new());
        }
        let log = Arc::clone(&self.log);
        let commands = Arc::clone(&self.commands);
        tokio::task::spawn_blocking(move || {
            let log = log.lock();
            let locations = log
                .locations_between(first_index, last_index)
                .map_err(|error| ReadEntriesError::Locations(Box::new(error)))?;
            let mut entries = Vec::with_capacity(locations.len());
            for location in locations {
                let index = location.log_id().index;
                let entry = log
                    .read_entry_at_location::<A, _>(&location, commands.as_ref())
                    .map_err(|error| ReadEntriesError::Entry {
                        index,
                        source: Box::new(error),
                    })?;
                entries.push(entry);
            }
            Ok(entries)
        })
        .await
        .map_err(|error| StorageIOError::read_logs(&error))?
        .map_err(|error| -> StorageError<A::NodeId> {
            match error {
                ReadEntriesError::Locations(source) => StorageIOError::read_logs(&source),
                ReadEntriesError::Entry { index, source } => {
                    StorageIOError::read_log_at_index(index, &source)
                }
            }
            .into()
        })
    }
}

impl<A, GID, G, N, C> RaftLogStorage<TypeConfig<A>> for DurableLogStore<A, GID, G, N, C>
where
    A: RaftApplication<Node = openraft::EmptyNode>,
    A::Command: Clone,
    GID: Clone + Eq + Send + Sync + 'static,
    G: Clone + GroupIdAdapter<GID> + Send + Sync + 'static,
    N: Clone + NodeIdAdapter<A::NodeId> + Send + Sync + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
{
    type LogReader = Self;

    /// Returns the saved purge boundary and newest remaining log entry.
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig<A>>, StorageError<A::NodeId>> {
        let log = Arc::clone(&self.log);
        tokio::task::spawn_blocking(move || {
            let log = log.lock();
            let last_purged_log_id = log.last_removed_log_id()?;
            let last_log_id = log
                .last_location()?
                .map(|location| location.log_id().clone())
                .or_else(|| last_purged_log_id.clone());
            Ok(LogState {
                last_purged_log_id,
                last_log_id,
            })
        })
        .await
        .map_err(|error| StorageIOError::read_logs(&error))?
        .map_err(|error: LogError| StorageIOError::read_logs(&error).into())
    }

    /// Returns another reader for the same durable log.
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    /// Saves a vote in the group catalog without allowing it to move backwards.
    async fn save_vote(&mut self, vote: &Vote<A::NodeId>) -> Result<(), StorageError<A::NodeId>> {
        let catalog = self.catalog.clone();
        let group_id = self.group_id.clone();
        let vote = vote.clone();
        let log = Arc::clone(&self.log);
        tokio::task::spawn_blocking(move || {
            let _log_owner = log;
            catalog.save_vote(&group_id, &vote)
        })
        .await
        .map_err(|error| StorageIOError::write_vote(&error))?
        .map_err(|error: CatalogError| StorageIOError::write_vote(&error).into())
    }

    /// Reads the latest vote from the durable group row.
    async fn read_vote(&mut self) -> Result<Option<Vote<A::NodeId>>, StorageError<A::NodeId>> {
        let catalog = self.catalog.clone();
        let group_id = self.group_id.clone();
        let log = Arc::clone(&self.log);
        tokio::task::spawn_blocking(move || {
            let _log_owner = log;
            catalog
                .group(&group_id)
                .map(|record| record.and_then(|record| record.vote().cloned()))
        })
        .await
        .map_err(|error| StorageIOError::read_vote(&error))?
        .map_err(|error: CatalogError| StorageIOError::read_vote(&error).into())
    }

    /// Saves the newest committed entry after verifying its durable frame.
    async fn save_committed(
        &mut self,
        committed: Option<LogId<A::NodeId>>,
    ) -> Result<(), StorageError<A::NodeId>> {
        let Some(committed) = committed else {
            return Ok(());
        };
        let log = Arc::clone(&self.log);
        tokio::task::spawn_blocking(move || log.lock().save_committed(committed))
            .await
            .map_err(|error| StorageIOError::write_logs(&error))?
            .map_err(|error: LogError| StorageIOError::write_logs(&error).into())
    }

    /// Reads the newest saved commit point.
    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<A::NodeId>>, StorageError<A::NodeId>> {
        let log = Arc::clone(&self.log);
        tokio::task::spawn_blocking(move || log.lock().committed_log_id())
            .await
            .map_err(|error| StorageIOError::read_logs(&error))?
            .map_err(|error: LogError| StorageIOError::read_logs(&error).into())
    }

    /// Appends one received group and syncs each touched segment once.
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig<A>>,
    ) -> Result<(), StorageError<A::NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig<A>>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let log = Arc::clone(&self.log);
        let commands = Arc::clone(&self.commands);
        let append = tokio::task::spawn_blocking(move || {
            log.lock().append_batch::<A, _>(&entries, commands.as_ref())
        })
        .await
        .map_err(|error| StorageIOError::write_logs(&error))?;
        if let Err(error) = append {
            callback.log_io_completed(Err(std::io::Error::other(error.to_string())));
            return Err(StorageIOError::write_logs(&error).into());
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    /// Removes a conflicting entry and every entry after it.
    async fn truncate(&mut self, log_id: LogId<A::NodeId>) -> Result<(), StorageError<A::NodeId>> {
        let log = Arc::clone(&self.log);
        tokio::task::spawn_blocking(move || log.lock().delete_from(log_id.index))
            .await
            .map_err(|error| StorageIOError::write_logs(&error))?
            .map_err(|error: LogError| StorageIOError::write_logs(&error).into())
    }

    /// Removes entries through the requested committed entry.
    async fn purge(&mut self, log_id: LogId<A::NodeId>) -> Result<(), StorageError<A::NodeId>> {
        let log = Arc::clone(&self.log);
        tokio::task::spawn_blocking(move || log.lock().delete_through(log_id))
            .await
            .map_err(|error| StorageIOError::write_logs(&error))?
            .map_err(|error: LogError| StorageIOError::write_logs(&error).into())
    }
}

/// OpenRaft state-machine storage backed by an application and group catalog.
pub(crate) struct DurableStateMachine<A, S, GID, G, N>
where
    A: RaftApplication,
    S: DurableApplicationStateMachine<A>,
{
    application: S,
    group_id: GID,
    catalog: GroupCatalog<GID, A::NodeId, G, N>,
    last_applied: Option<LogId<A::NodeId>>,
    last_membership: StoredMembership<A::NodeId, A::Node>,
    // This field stays last so application resources are released before
    // successful node shutdown becomes observable.
    _shutdown_owner: StateMachineOwner,
}

impl<A, S, GID, G, N> DurableStateMachine<A, S, GID, G, N>
where
    A: RaftApplication,
    S: DurableApplicationStateMachine<A>,
{
    /// Opens an application at its saved log ID and membership.
    pub fn new(
        application: S,
        group_id: GID,
        catalog: GroupCatalog<GID, A::NodeId, G, N>,
        last_applied: Option<LogId<A::NodeId>>,
        last_membership: StoredMembership<A::NodeId, A::Node>,
        shutdown_owner: StateMachineOwner,
    ) -> Self {
        Self {
            application,
            group_id,
            catalog,
            last_applied,
            last_membership,
            _shutdown_owner: shutdown_owner,
        }
    }
}

impl<A, S, GID, G, N> RaftStateMachine<TypeConfig<A>> for DurableStateMachine<A, S, GID, G, N>
where
    A: RaftApplication<Node = openraft::EmptyNode>,
    S: DurableApplicationStateMachine<A>,
    GID: Clone + Eq + Send + Sync + 'static,
    G: Clone + GroupIdAdapter<GID> + Send + Sync + 'static,
    N: Clone + NodeIdAdapter<A::NodeId> + Send + Sync + 'static,
{
    type SnapshotBuilder = DurableSnapshotBuilder<A>;

    /// Returns the application log position and latest saved membership.
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

    /// Applies committed entries and saves membership changes.
    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<EntryResponse<A::Response>>, StorageError<A::NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig<A>>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();
        for entry in entries {
            let log_id = entry.log_id;
            let response = match entry.payload {
                EntryPayload::Blank => EntryResponse::Internal,
                EntryPayload::Membership(membership) => {
                    let stored = StoredMembership::new(Some(log_id.clone()), membership);
                    let catalog = self.catalog.clone();
                    let group_id = self.group_id.clone();
                    let saved = stored.clone();
                    let saved_at = log_id.clone();
                    tokio::task::spawn_blocking(move || catalog.save_membership(&group_id, &saved))
                        .await
                        .map_err(|error| StorageIOError::apply(log_id.clone(), &error))?
                        .map_err(|error| StorageIOError::apply(saved_at, &error))?;
                    self.last_membership = stored;
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
            let catalog = self.catalog.clone();
            let group_id = self.group_id.clone();
            let saved_at = log_id.clone();
            tokio::task::spawn_blocking(move || catalog.save_applied_log_id(&group_id, &saved_at))
                .await
                .map_err(|error| StorageIOError::apply(log_id.clone(), &error))?
                .map_err(|error| StorageIOError::apply(log_id.clone(), &error))?;
            self.last_applied = Some(log_id);
            responses.push(response);
        }
        Ok(responses)
    }

    /// Copies current application and Raft state into one immutable builder.
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        DurableSnapshotBuilder {
            snapshot: Mutex::new(Some(self.application.build_snapshot())),
            meta: snapshot_meta(&self.last_applied, &self.last_membership),
        }
    }

    /// Creates one application-owned bounded snapshot receiver.
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<A::Snapshot>, StorageError<A::NodeId>> {
        self.application
            .begin_receiving_snapshot()
            .map(Box::new)
            .map_err(|error| StorageIOError::write_snapshot(None, &error).into())
    }

    /// Installs application state and then advances its saved Raft position.
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<A::NodeId, A::Node>,
        snapshot: Box<A::Snapshot>,
    ) -> Result<(), StorageError<A::NodeId>> {
        let log_id = meta.last_log_id.clone().ok_or_else(|| {
            StorageIOError::write_snapshot(None, &DurableSnapshotError::MissingLogId)
        })?;
        let context = ApplyContext::new(log_id.leader_id.term, log_id.index);
        self.application
            .install_snapshot(context, *snapshot)
            .await
            .map_err(|error| StorageIOError::write_snapshot(None, &error))?;

        let catalog = self.catalog.clone();
        let group_id = self.group_id.clone();
        let membership = meta.last_membership.clone();
        let applied = log_id.clone();
        tokio::task::spawn_blocking(move || {
            catalog.save_membership(&group_id, &membership)?;
            catalog.save_applied_log_id(&group_id, &applied)
        })
        .await
        .map_err(|error| StorageIOError::write_snapshot(None, &error))?
        .map_err(|error: CatalogError| StorageIOError::write_snapshot(None, &error))?;
        self.last_applied = Some(log_id);
        self.last_membership = meta.last_membership.clone();
        Ok(())
    }

    /// Returns a fresh read handle for the latest durable application state.
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig<A>>>, StorageError<A::NodeId>> {
        if self.last_applied.is_none() {
            return Ok(None);
        }
        let snapshot = self
            .application
            .build_snapshot()
            .map_err(|error| StorageIOError::read_snapshot(None, &error))?;
        Ok(Some(Snapshot {
            meta: snapshot_meta(&self.last_applied, &self.last_membership),
            snapshot: Box::new(snapshot),
        }))
    }
}

/// Builds one stable snapshot identity from its included Raft position.
fn snapshot_meta<NID, N>(
    last_applied: &Option<LogId<NID>>,
    last_membership: &StoredMembership<NID, N>,
) -> SnapshotMeta<NID, N>
where
    NID: openraft::NodeId,
    N: openraft::Node,
{
    let snapshot_id = match last_applied {
        Some(log_id) => format!("{}-{}", log_id.leader_id.term, log_id.index),
        None => "empty".to_string(),
    };
    SnapshotMeta {
        last_log_id: last_applied.clone(),
        last_membership: last_membership.clone(),
        snapshot_id,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::DurableLogShutdown;

    /// A cancelled wait retains the same exact durable-owner completion state.
    #[tokio::test]
    async fn durable_log_shutdown_wait_is_retryable() {
        let (owner, shutdown) = DurableLogShutdown::pair();
        let first = std::sync::Arc::new(owner);
        let outstanding = std::sync::Arc::clone(&first);
        drop(first);

        assert!(
            tokio::time::timeout(Duration::from_millis(10), shutdown.wait())
                .await
                .is_err()
        );

        drop(outstanding);
        tokio::time::timeout(Duration::from_secs(1), shutdown.wait())
            .await
            .expect("retry durable log shutdown wait");
    }
}
