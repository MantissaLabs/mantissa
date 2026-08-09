use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use openraft::{LogId, NodeId};
use redb::ReadableDatabase;

use super::{
    AEAD_TAG_BYTES, EncryptedLog, LOCATIONS, LOG_STATES, complete_frame_size,
    encode_segment_file_header,
};
use crate::catalog::GroupIdAdapter;
use crate::durable_log::model::SavedLogState;
use crate::durable_log::record::{
    decode_log_state, encode_frame_header, encode_location, encode_log_state, encode_segment_header,
};
use crate::durable_log::{LogError, LogLocation, SegmentId};
use crate::protocol::NodeIdAdapter;

struct SegmentChange<NID>
where
    NID: NodeId,
{
    removed: Vec<LogLocation<NID>>,
    kept: Vec<LogLocation<NID>>,
}

impl<NID> Default for SegmentChange<NID>
where
    NID: NodeId,
{
    /// Creates an empty list of removed and kept entries.
    fn default() -> Self {
        Self {
            removed: Vec::new(),
            kept: Vec::new(),
        }
    }
}

struct Replacement<NID>
where
    NID: NodeId,
{
    new_path: PathBuf,
    locations: Vec<LogLocation<NID>>,
}

impl<GID, NID, G, N> EncryptedLog<GID, NID, G, N>
where
    GID: Eq,
    NID: NodeId,
    G: GroupIdAdapter<GID>,
    N: NodeIdAdapter<NID>,
{
    /// Returns the newest entry removed from the start of this log.
    pub fn last_removed_log_id(&self) -> Result<Option<LogId<NID>>, LogError> {
        Ok(self.saved_log_state()?.last_removed_log_id)
    }

    /// Returns the newest entry saved as committed by this group.
    pub fn committed_log_id(&self) -> Result<Option<LogId<NID>>, LogError> {
        Ok(self.saved_log_state()?.committed_log_id)
    }

    /// Saves a newer committed entry after checking its durable log location.
    pub fn save_committed(&self, log_id: LogId<NID>) -> Result<(), LogError> {
        let mut state = self.saved_log_state()?;
        if let Some(current) = &state.committed_log_id {
            if current.index > log_id.index {
                return Ok(());
            }
            if current.index == log_id.index {
                if current != &log_id {
                    return Err(LogError::ConflictingLogId {
                        index: log_id.index,
                    });
                }
                return Ok(());
            }
        }

        let location = self
            .location(log_id.index)?
            .ok_or(LogError::MissingLogIndex {
                index: log_id.index,
            })?;
        if location.log_id() != &log_id {
            return Err(LogError::ConflictingLogId {
                index: log_id.index,
            });
        }
        state.committed_log_id = Some(log_id);
        self.save_log_state(&state)
    }

    /// Installs one copied application position into a new empty log.
    ///
    /// The copied application state already contains this entry, so the local
    /// log starts immediately after it. Repeating the exact install is safe.
    pub fn install_base_log_id(&self, log_id: LogId<NID>) -> Result<(), LogError> {
        if !self.all_locations()?.is_empty() {
            return Err(LogError::BaseInstallLogNotEmpty);
        }
        let state = self.saved_log_state()?;
        match (
            state.last_removed_log_id.as_ref(),
            state.committed_log_id.as_ref(),
        ) {
            (None, None) => self.save_log_state(&SavedLogState {
                last_removed_log_id: Some(log_id.clone()),
                committed_log_id: Some(log_id),
            }),
            (Some(removed), Some(committed)) if removed == &log_id && committed == &log_id => {
                Ok(())
            }
            _ => Err(LogError::BaseInstallStateChanged),
        }
    }

    /// Reads the complete saved state or returns its empty initial value.
    fn saved_log_state(&self) -> Result<SavedLogState<NID>, LogError> {
        let read = self.database.begin_read()?;
        let states = read.open_table(LOG_STATES)?;
        let Some(stored) = states.get(self.encoded_group_id.as_slice())? else {
            return Ok(SavedLogState {
                last_removed_log_id: None,
                committed_log_id: None,
            });
        };
        let (group_id, state) = decode_log_state(
            stored.value(),
            &self.group_ids,
            &self.node_ids,
            self.protocol_limits,
        )?;
        if group_id != self.group_id {
            return Err(LogError::RecordMismatch {
                record: "log state",
            });
        }
        Ok(state)
    }

    /// Replaces the complete saved log state in one Redb transaction.
    fn save_log_state(&self, state: &SavedLogState<NID>) -> Result<(), LogError> {
        let encoded = encode_log_state(
            &self.group_id,
            state,
            &self.group_ids,
            &self.node_ids,
            self.protocol_limits,
        )?;
        let write = self.database.begin_write()?;
        {
            let mut states = write.open_table(LOG_STATES)?;
            states.insert(self.encoded_group_id.as_slice(), encoded.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    /// Removes the requested log index and every newer entry.
    pub fn delete_from(&mut self, index: u64) -> Result<(), LogError> {
        if let Some(committed) = self.committed_log_id()?
            && committed.index >= index
        {
            return Err(LogError::CommittedLogWouldBeRemoved {
                from: index,
                committed: committed.index,
            });
        }
        self.active = None;
        self.recover_files()?;
        let removed = self
            .all_locations()?
            .into_iter()
            .filter(|location| location.log_id().index >= index)
            .collect::<Vec<_>>();
        if removed.is_empty() {
            return Ok(());
        }

        let write = self.database.begin_write()?;
        {
            let mut locations = write.open_table(LOCATIONS)?;
            for location in &removed {
                let key = (self.encoded_group_id.as_slice(), location.log_id().index);
                locations.remove(&key)?;
            }
        }
        write.commit()?;
        self.recover_files()
    }

    /// Removes the requested log entry and every older entry.
    pub fn delete_through(&mut self, log_id: LogId<NID>) -> Result<(), LogError> {
        let committed = self
            .committed_log_id()?
            .ok_or(LogError::NoCommittedLogToRemove {
                through: log_id.index,
            })?;
        if log_id.index > committed.index {
            return Err(LogError::UncommittedLogWouldBeRemoved {
                through: log_id.index,
                committed: committed.index,
            });
        }
        self.active = None;
        self.recover_files()?;
        if let Some(last_removed) = self.last_removed_log_id()? {
            if last_removed.index > log_id.index {
                return Ok(());
            }
            if last_removed.index == log_id.index {
                if last_removed != log_id {
                    return Err(LogError::ConflictingLogId {
                        index: log_id.index,
                    });
                }
                return Ok(());
            }
        }

        let all_locations = self.all_locations()?;
        let Some(last_entry_to_remove) = all_locations
            .iter()
            .find(|location| location.log_id().index == log_id.index)
        else {
            return Err(LogError::MissingLogIndex {
                index: log_id.index,
            });
        };
        if last_entry_to_remove.log_id() != &log_id {
            return Err(LogError::ConflictingLogId {
                index: log_id.index,
            });
        }

        let mut changes = BTreeMap::<SegmentId, SegmentChange<NID>>::new();
        for location in all_locations {
            let change = changes.entry(location.segment_id()).or_default();
            if location.log_id().index <= log_id.index {
                change.removed.push(location);
            } else {
                change.kept.push(location);
            }
        }

        let mut replacements = Vec::new();
        for (segment_id, change) in &changes {
            if !change.removed.is_empty() && !change.kept.is_empty() {
                match self.prepare_replacement(*segment_id, &change.kept) {
                    Ok(replacement) => replacements.push(replacement),
                    Err(error) => {
                        self.remove_new_files(&replacements)?;
                        return Err(error);
                    }
                }
            }
        }

        self.save_removal(&log_id, &changes, &replacements)?;

        let old_segments = changes
            .iter()
            .filter(|(_, change)| !change.removed.is_empty())
            .map(|(segment_id, _)| *segment_id)
            .collect::<Vec<_>>();
        self.remove_old_segments(&old_segments)?;
        self.recover_files()
    }

    /// Creates and syncs a new segment containing only the entries being kept.
    fn prepare_replacement(
        &self,
        old_segment_id: SegmentId,
        kept: &[LogLocation<NID>],
    ) -> Result<Replacement<NID>, LogError> {
        let new_segment_id = self
            .file_system
            .new_segment_id()
            .map_err(|source| LogError::Random { source })?;
        let new_path = self.segment_path(new_segment_id);
        let temporary_path = replacement_path(&new_path);
        let result = self.write_replacement(
            old_segment_id,
            new_segment_id,
            kept,
            &temporary_path,
            &new_path,
        );
        if let Err(error) = result {
            self.remove_if_present(&temporary_path)?;
            self.remove_if_present(&new_path)?;
            self.sync_log_directory()?;
            return Err(error);
        }
        result
    }

    /// Writes, syncs, and installs one replacement segment.
    fn write_replacement(
        &self,
        old_segment_id: SegmentId,
        new_segment_id: SegmentId,
        kept: &[LogLocation<NID>],
        temporary_path: &Path,
        new_path: &Path,
    ) -> Result<Replacement<NID>, LogError> {
        let old_path = self.segment_path(old_segment_id);
        let mut old_file = self
            .file_system
            .open_segment(&old_path)
            .map_err(|error| LogError::io("open old Raft log segment", &old_path, error))?;
        let old_header_bytes =
            self.read_and_check_segment_header(&mut *old_file, &old_path, old_segment_id)?;

        let header = encode_segment_header(
            &self.group_id,
            new_segment_id,
            &self.group_ids,
            self.protocol_limits,
        )?;
        let encoded_header = encode_segment_file_header(&header)?;
        let mut new_file = self
            .file_system
            .create_segment(temporary_path)
            .map_err(|error| {
                LogError::io("create replacement Raft log segment", temporary_path, error)
            })?;
        new_file.write_all(&encoded_header).map_err(|error| {
            LogError::io("write replacement Raft log header", temporary_path, error)
        })?;

        let mut frame_offset =
            u64::try_from(encoded_header.len()).map_err(|_| LogError::LengthOverflow)?;
        let mut new_locations = Vec::with_capacity(kept.len());
        for (position, old_location) in kept.iter().enumerate() {
            let frame_number = u64::try_from(position).map_err(|_| LogError::LengthOverflow)?;
            let plaintext =
                self.read_frame(&mut *old_file, &old_path, old_header_bytes, old_location)?;
            let plaintext_bytes =
                u32::try_from(plaintext.len()).map_err(|_| LogError::LengthOverflow)?;
            let ciphertext_bytes = plaintext
                .len()
                .checked_add(AEAD_TAG_BYTES)
                .and_then(|length| u32::try_from(length).ok())
                .ok_or(LogError::LengthOverflow)?;
            let frame_header = encode_frame_header(
                new_segment_id,
                frame_number,
                old_location.log_id(),
                plaintext_bytes,
                ciphertext_bytes,
                &self.node_ids,
                self.protocol_limits,
            )?;
            let expected_bytes =
                complete_frame_size(frame_header.len(), ciphertext_bytes as usize)?;
            if expected_bytes > self.log_limits.max_frame_bytes() {
                return Err(LogError::FrameTooLarge {
                    actual: expected_bytes,
                    maximum: self.log_limits.max_frame_bytes(),
                });
            }
            let frame =
                self.encrypt_frame(&frame_header, new_segment_id, frame_number, plaintext)?;
            if frame.len() != expected_bytes {
                return Err(LogError::RecordMismatch {
                    record: "replacement frame length",
                });
            }
            let stored_frame_bytes =
                u64::try_from(frame.len()).map_err(|_| LogError::LengthOverflow)?;
            let next_offset = frame_offset
                .checked_add(stored_frame_bytes)
                .ok_or(LogError::LengthOverflow)?;
            if next_offset > self.log_limits.max_segment_bytes() {
                return Err(LogError::SegmentTooSmall {
                    required: next_offset,
                    maximum: self.log_limits.max_segment_bytes(),
                });
            }
            new_file.write_all(&frame).map_err(|error| {
                LogError::io("write replacement Raft log frame", temporary_path, error)
            })?;
            new_locations.push(LogLocation {
                log_id: old_location.log_id().clone(),
                segment_id: new_segment_id,
                frame_offset,
                frame_bytes: u32::try_from(frame.len()).map_err(|_| LogError::LengthOverflow)?,
                frame_number,
            });
            frame_offset = next_offset;
        }

        new_file.sync_all().map_err(|error| {
            LogError::io("sync replacement Raft log segment", temporary_path, error)
        })?;
        drop(new_file);
        self.file_system
            .move_file(temporary_path, new_path)
            .map_err(|error| {
                LogError::io("install replacement Raft log segment", new_path, error)
            })?;
        self.sync_log_directory()?;
        Ok(Replacement {
            new_path: new_path.to_path_buf(),
            locations: new_locations,
        })
    }

    /// Saves removed entries, replacement locations, and the new removal point together.
    fn save_removal(
        &self,
        log_id: &LogId<NID>,
        changes: &BTreeMap<SegmentId, SegmentChange<NID>>,
        replacements: &[Replacement<NID>],
    ) -> Result<(), LogError> {
        let mut state = self.saved_log_state()?;
        state.last_removed_log_id = Some(log_id.clone());
        let encoded_state = encode_log_state(
            &self.group_id,
            &state,
            &self.group_ids,
            &self.node_ids,
            self.protocol_limits,
        )?;
        let write = self.database.begin_write()?;
        {
            let mut locations = write.open_table(LOCATIONS)?;
            for change in changes.values() {
                for location in &change.removed {
                    let key = (self.encoded_group_id.as_slice(), location.log_id().index);
                    locations.remove(&key)?;
                }
            }
            for replacement in replacements {
                for location in &replacement.locations {
                    let encoded = encode_location(
                        &self.group_id,
                        location,
                        &self.group_ids,
                        &self.node_ids,
                        self.protocol_limits,
                    )?;
                    let key = (self.encoded_group_id.as_slice(), location.log_id().index);
                    locations.insert(&key, encoded.as_slice())?;
                }
            }
            drop(locations);

            let mut states = write.open_table(LOG_STATES)?;
            states.insert(self.encoded_group_id.as_slice(), encoded_state.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    /// Deletes old segment files after their Redb records were changed.
    fn remove_old_segments(&self, segment_ids: &[SegmentId]) -> Result<(), LogError> {
        let mut removed = false;
        for segment_id in segment_ids {
            let path = self.segment_path(*segment_id);
            match self.file_system.remove_file(&path) {
                Ok(()) => removed = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(LogError::io("remove old Raft log segment", &path, error));
                }
            }
        }
        if removed {
            self.sync_log_directory()?;
        }
        Ok(())
    }

    /// Deletes new files when their Redb changes did not finish.
    fn remove_new_files(&self, replacements: &[Replacement<NID>]) -> Result<(), LogError> {
        for replacement in replacements {
            self.remove_if_present(&replacement.new_path)?;
        }
        if !replacements.is_empty() {
            self.sync_log_directory()?;
        }
        Ok(())
    }

    /// Deletes one file while accepting that recovery already removed it.
    fn remove_if_present(&self, path: &Path) -> Result<(), LogError> {
        match self.file_system.remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(LogError::io("remove unused Raft log file", path, error)),
        }
    }

    /// Syncs changes made to the group directory.
    fn sync_log_directory(&self) -> Result<(), LogError> {
        self.file_system
            .sync_directory(&self.directory)
            .map_err(|error| LogError::io("sync Raft log directory", &self.directory, error))
    }
}

/// Returns the unfinished file name used while a segment is being replaced.
fn replacement_path(final_path: &Path) -> PathBuf {
    let mut name = final_path.as_os_str().to_owned();
    name.push(".new");
    PathBuf::from(name)
}
