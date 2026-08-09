use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    Entry, LogId, LogState, OptionalSend, RaftLogId, RaftLogReader, StorageError, Vote,
};
use parking_lot::RwLock;

use crate::{RaftApplication, TypeConfig};

/// Shared typed state for one non-durable Raft log.
struct LogStateData<A>
where
    A: RaftApplication,
{
    vote: Option<Vote<A::NodeId>>,
    committed: Option<LogId<A::NodeId>>,
    last_purged: Option<LogId<A::NodeId>>,
    entries: BTreeMap<u64, Entry<TypeConfig<A>>>,
}

impl<A> Default for LogStateData<A>
where
    A: RaftApplication,
{
    /// Creates an empty in-memory log.
    fn default() -> Self {
        Self {
            vote: None,
            committed: None,
            last_purged: None,
            entries: BTreeMap::new(),
        }
    }
}

/// Stores OpenRaft entries directly without serialization.
pub(crate) struct MemoryLogStore<A>
where
    A: RaftApplication,
{
    state: Arc<RwLock<LogStateData<A>>>,
}

impl<A> MemoryLogStore<A>
where
    A: RaftApplication,
{
    /// Creates an empty typed log store.
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(LogStateData::default())),
        }
    }
}

impl<A> Clone for MemoryLogStore<A>
where
    A: RaftApplication,
{
    /// Clones the handle to the same in-memory log.
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<A> RaftLogReader<TypeConfig<A>> for MemoryLogStore<A>
where
    A: RaftApplication,
    A::Command: Clone,
{
    /// Returns owned typed entries in the requested half-open range.
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig<A>>>, StorageError<A::NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let state = self.state.read();
        Ok(state
            .entries
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl<A> RaftLogStorage<TypeConfig<A>> for MemoryLogStore<A>
where
    A: RaftApplication,
    A::Command: Clone,
{
    type LogReader = Self;

    /// Returns the purge boundary and the newest stored log ID.
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig<A>>, StorageError<A::NodeId>> {
        let state = self.state.read();
        let last_log_id = state
            .entries
            .last_key_value()
            .map(|(_, entry)| entry.get_log_id().clone())
            .or_else(|| state.last_purged.clone());

        Ok(LogState {
            last_purged_log_id: state.last_purged.clone(),
            last_log_id,
        })
    }

    /// Returns another reader for the same typed log.
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    /// Stores the latest vote in memory.
    async fn save_vote(&mut self, vote: &Vote<A::NodeId>) -> Result<(), StorageError<A::NodeId>> {
        self.state.write().vote = Some(vote.clone());
        Ok(())
    }

    /// Returns the latest stored vote.
    async fn read_vote(&mut self) -> Result<Option<Vote<A::NodeId>>, StorageError<A::NodeId>> {
        Ok(self.state.read().vote.clone())
    }

    /// Stores the highest committed log ID in memory.
    async fn save_committed(
        &mut self,
        committed: Option<LogId<A::NodeId>>,
    ) -> Result<(), StorageError<A::NodeId>> {
        self.state.write().committed = committed;
        Ok(())
    }

    /// Returns the highest committed log ID.
    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<A::NodeId>>, StorageError<A::NodeId>> {
        Ok(self.state.read().committed.clone())
    }

    /// Makes appended entries visible and immediately reports memory durability.
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig<A>>,
    ) -> Result<(), StorageError<A::NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig<A>>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut state = self.state.write();
        for entry in entries {
            state.entries.insert(entry.log_id.index, entry);
        }
        drop(state);

        callback.log_io_completed(Ok(()));
        Ok(())
    }

    /// Removes the conflicting suffix beginning at the supplied log ID.
    async fn truncate(&mut self, log_id: LogId<A::NodeId>) -> Result<(), StorageError<A::NodeId>> {
        self.state.write().entries.split_off(&log_id.index);
        Ok(())
    }

    /// Removes entries through the supplied log ID and records the purge boundary.
    async fn purge(&mut self, log_id: LogId<A::NodeId>) -> Result<(), StorageError<A::NodeId>> {
        let mut state = self.state.write();
        state.entries.retain(|index, _| *index > log_id.index);
        state.last_purged = Some(log_id);
        Ok(())
    }
}
