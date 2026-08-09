use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use openraft::NodeId;
use redb::ReadableDatabase;

use super::{EncryptedLog, LOCATIONS};
use crate::catalog::GroupIdAdapter;
use crate::durable_log::record::decode_location;
use crate::durable_log::{LogError, LogLocation, SegmentId};
use crate::protocol::NodeIdAdapter;

impl<GID, NID, G, N> EncryptedLog<GID, NID, G, N>
where
    GID: Eq,
    NID: NodeId,
    G: GroupIdAdapter<GID>,
    N: NodeIdAdapter<NID>,
{
    /// Checks every stored entry and removes files or tail bytes that are not used.
    pub(super) fn recover_files(&self) -> Result<(), LogError> {
        let last_removed = self.last_removed_log_id()?;
        let locations = self.all_locations()?;
        if let Some(last_removed) = last_removed
            && locations
                .first()
                .is_some_and(|location| location.log_id().index <= last_removed.index)
        {
            return Err(LogError::RecordMismatch {
                record: "removed log entries",
            });
        }
        let mut by_segment = BTreeMap::<SegmentId, Vec<LogLocation<NID>>>::new();
        for location in locations {
            by_segment
                .entry(location.segment_id())
                .or_default()
                .push(location);
        }

        self.remove_unused_files(&by_segment)?;
        for (segment_id, mut locations) in by_segment {
            locations.sort_by_key(LogLocation::frame_offset);
            self.check_and_shorten_segment(segment_id, &locations)?;
        }
        Ok(())
    }

    /// Reads every saved frame location for this group in log-index order.
    pub(crate) fn all_locations(&self) -> Result<Vec<LogLocation<NID>>, LogError> {
        self.locations_between(0, u64::MAX)
    }

    /// Reads saved frame locations only inside one inclusive log-index range.
    pub(crate) fn locations_between(
        &self,
        first_index: u64,
        last_index: u64,
    ) -> Result<Vec<LogLocation<NID>>, LogError> {
        if first_index > last_index {
            return Ok(Vec::new());
        }
        let read = self.database.begin_read()?;
        let table = read.open_table(LOCATIONS)?;
        let start = (self.encoded_group_id.as_slice(), first_index);
        let end = (self.encoded_group_id.as_slice(), last_index);
        let mut locations = Vec::new();
        for row in table.range(start..=end)? {
            let (key, value) = row?;
            let (stored_group_id, index) = key.value();
            if stored_group_id != self.encoded_group_id.as_slice() {
                return Err(LogError::RecordMismatch {
                    record: "location key",
                });
            }
            let (group_id, location) = decode_location(
                value.value(),
                &self.group_ids,
                &self.node_ids,
                self.protocol_limits,
            )?;
            self.check_location(index, &group_id, &location)?;
            locations.push(location);
        }
        Ok(locations)
    }

    /// Returns the newest saved frame location without scanning older entries.
    pub(crate) fn last_location(&self) -> Result<Option<LogLocation<NID>>, LogError> {
        let read = self.database.begin_read()?;
        let table = read.open_table(LOCATIONS)?;
        let start = (self.encoded_group_id.as_slice(), 0);
        let end = (self.encoded_group_id.as_slice(), u64::MAX);
        let Some(row) = table.range(start..=end)?.next_back() else {
            return Ok(None);
        };
        let (key, value) = row?;
        let (stored_group_id, index) = key.value();
        if stored_group_id != self.encoded_group_id.as_slice() {
            return Err(LogError::RecordMismatch {
                record: "location key",
            });
        }
        let (group_id, location) = decode_location(
            value.value(),
            &self.group_ids,
            &self.node_ids,
            self.protocol_limits,
        )?;
        self.check_location(index, &group_id, &location)?;
        Ok(Some(location))
    }

    /// Removes unfinished replacement files and segments with no saved entries.
    fn remove_unused_files(
        &self,
        by_segment: &BTreeMap<SegmentId, Vec<LogLocation<NID>>>,
    ) -> Result<(), LogError> {
        let expected = by_segment
            .keys()
            .map(|segment_id| self.segment_path(*segment_id))
            .collect::<BTreeSet<_>>();
        let paths = self
            .file_system
            .list_files(&self.directory)
            .map_err(|error| LogError::io("list Raft log directory", &self.directory, error))?;
        let mut removed = false;
        for path in paths {
            let is_temporary = file_name_ends_with(&path, ".raftlog.new");
            let is_segment = file_name_ends_with(&path, ".raftlog");
            if is_temporary || (is_segment && !expected.contains(&path)) {
                self.file_system
                    .remove_file(&path)
                    .map_err(|error| LogError::io("remove unused Raft log file", &path, error))?;
                removed = true;
            }
        }
        if removed {
            self.file_system
                .sync_directory(&self.directory)
                .map_err(|error| LogError::io("sync Raft log directory", &self.directory, error))?;
        }
        Ok(())
    }

    /// Checks all live frames in one segment and removes unused tail bytes.
    fn check_and_shorten_segment(
        &self,
        segment_id: SegmentId,
        locations: &[LogLocation<NID>],
    ) -> Result<(), LogError> {
        let path = self.segment_path(segment_id);
        let mut file = self
            .file_system
            .open_segment_for_update(&path)
            .map_err(|error| LogError::io("open Raft log segment for recovery", &path, error))?;
        let header_bytes = self.read_and_check_segment_header(&mut *file, &path, segment_id)?;
        let mut expected_offset = header_bytes;
        let mut expected_frame_number = 0;
        let mut previous_log_index = None;
        for location in locations {
            if location.frame_offset() != expected_offset {
                return Err(LogError::RecordMismatch {
                    record: "frame offset",
                });
            }
            if location.frame_number() != expected_frame_number {
                return Err(LogError::RecordMismatch {
                    record: "frame number",
                });
            }
            if previous_log_index.is_some_and(|log_index| location.log_id().index <= log_index) {
                return Err(LogError::RecordMismatch {
                    record: "log index order",
                });
            }
            self.read_frame(&mut *file, &path, header_bytes, location)?;
            expected_offset = location
                .frame_offset()
                .checked_add(u64::from(location.frame_bytes()))
                .ok_or(LogError::LengthOverflow)?;
            expected_frame_number = expected_frame_number
                .checked_add(1)
                .ok_or(LogError::FrameNumberExhausted)?;
            previous_log_index = Some(location.log_id().index);
        }

        let file_bytes = file
            .len()
            .map_err(|error| LogError::io("inspect Raft log segment", &path, error))?;
        if expected_offset > self.log_limits.max_segment_bytes() {
            return Err(LogError::RecordMismatch {
                record: "segment size",
            });
        }
        if file_bytes > expected_offset {
            file.set_len(expected_offset)
                .and_then(|()| file.sync_all())
                .map_err(|error| LogError::io("shorten Raft log segment", &path, error))?;
        }
        Ok(())
    }
}

/// Checks a file suffix without requiring its name to be valid UTF-8.
fn file_name_ends_with(path: &Path, suffix: &str) -> bool {
    path.file_name()
        .map(|name| name.as_encoded_bytes().ends_with(suffix.as_bytes()))
        .unwrap_or(false)
}
