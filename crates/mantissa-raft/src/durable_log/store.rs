use std::io::{SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chacha20poly1305::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use openraft::{EmptyNode, Entry, NodeId};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use zeroize::Zeroizing;

use super::file_system::{FileSystem, SegmentFile, StandardFileSystem};
use super::model::FrameHeader;
use super::record::{
    decode_frame_header, decode_location, decode_segment_header, encode_frame_header,
    encode_location, encode_segment_header,
};
use super::{GroupEncryptionKey, GroupKeyProvider, LogError, LogLimits, LogLocation, SegmentId};
use crate::catalog::GroupIdAdapter;
use crate::protocol::{
    ApplicationCommandAdapter, NodeIdAdapter, ProtocolError, ProtocolLimits, decode_log_entry,
    encode_group_id, encode_log_entry,
};
use crate::{ApplyContext, RaftApplication, TypeConfig};

mod cleanup;
mod recovery;

const LOCATIONS: TableDefinition<'static, (&'static [u8], u64), &'static [u8]> =
    TableDefinition::new("mantissa_raft_log_locations");
const LOG_STATES: TableDefinition<'static, &'static [u8], &'static [u8]> =
    TableDefinition::new("mantissa_raft_log_states");
const SEGMENT_MAGIC: [u8; 8] = *b"MRAFSEG1";
const FRAME_MAGIC: [u8; 4] = *b"MRF1";
const SEGMENT_PREFIX_BYTES: usize = 16;
const FRAME_PREFIX_BYTES: usize = 16;
const AEAD_TAG_BYTES: usize = 16;
const SEGMENT_KEY_DOMAIN: &[u8] = b"mantissa raft log segment key v1";
const FRAME_AAD_DOMAIN: &[u8] = b"mantissa raft log frame aad v1";

struct ActiveSegment {
    id: SegmentId,
    file: Box<dyn SegmentFile>,
    bytes_written: u64,
    next_frame_number: u64,
    has_frames: bool,
}

/// Values needed to open one encrypted Raft log.
pub struct EncryptedLogSettings<GID, G, N> {
    directory: PathBuf,
    database: Arc<redb::Database>,
    group_id: GID,
    group_ids: G,
    node_ids: N,
    protocol_limits: ProtocolLimits,
    log_limits: LogLimits,
}

impl<GID, G, N> EncryptedLogSettings<GID, G, N> {
    /// Collects the storage location, group identity, adapters, and limits.
    pub fn new(
        directory: impl Into<PathBuf>,
        database: Arc<redb::Database>,
        group_id: GID,
        group_ids: G,
        node_ids: N,
        protocol_limits: ProtocolLimits,
        log_limits: LogLimits,
    ) -> Self {
        Self {
            directory: directory.into(),
            database,
            group_id,
            group_ids,
            node_ids,
            protocol_limits,
            log_limits,
        }
    }
}

/// Stores typed Raft entries in encrypted append-only segment files.
pub struct EncryptedLog<GID, NID, G, N>
where
    NID: NodeId,
{
    directory: PathBuf,
    database: Arc<redb::Database>,
    group_id: GID,
    encoded_group_id: Vec<u8>,
    group_key: GroupEncryptionKey,
    group_ids: G,
    node_ids: N,
    protocol_limits: ProtocolLimits,
    log_limits: LogLimits,
    file_system: Arc<dyn FileSystem>,
    active: Option<ActiveSegment>,
    node_type: PhantomData<fn() -> NID>,
}

impl<GID, NID, G, N> EncryptedLog<GID, NID, G, N>
where
    GID: Eq,
    NID: NodeId,
    G: GroupIdAdapter<GID>,
    N: NodeIdAdapter<NID>,
{
    /// Opens one group log using the host filesystem.
    ///
    /// `directory` is dedicated to this group and its parent must already
    /// exist. No segment file is created until the first append.
    pub fn open<K>(
        settings: EncryptedLogSettings<GID, G, N>,
        key_provider: &K,
    ) -> Result<Self, LogError>
    where
        K: GroupKeyProvider<GID>,
    {
        Self::open_with_file_system(settings, key_provider, Arc::new(StandardFileSystem))
    }

    /// Removes every durable row and file for a group that has already stopped.
    ///
    /// The caller must first remove all runtime owners of this group. Database
    /// rows are removed before files so a failed or interrupted filesystem
    /// cleanup is harmless to retry and cannot make stale frames discoverable.
    pub fn remove_closed(settings: EncryptedLogSettings<GID, G, N>) -> Result<(), LogError> {
        let EncryptedLogSettings {
            directory,
            database,
            group_id,
            group_ids,
            protocol_limits,
            ..
        } = settings;
        let encoded_group_id = encode_group_id(&group_id, &group_ids, protocol_limits)?;
        let write = database.begin_write()?;
        {
            let mut locations = write.open_table(LOCATIONS)?;
            let start = (encoded_group_id.as_slice(), 0);
            let end = (encoded_group_id.as_slice(), u64::MAX);
            let mut indices = Vec::new();
            for row in locations.range(start..=end)? {
                let (key, _value) = row?;
                let (saved_group, index) = key.value();
                if saved_group != encoded_group_id.as_slice() {
                    return Err(LogError::RecordMismatch {
                        record: "group log location key",
                    });
                }
                indices.push(index);
            }
            for index in indices {
                locations.remove(&(encoded_group_id.as_slice(), index))?;
            }
            drop(locations);

            let mut states = write.open_table(LOG_STATES)?;
            states.remove(encoded_group_id.as_slice())?;
        }
        write.commit()?;

        match std::fs::remove_dir_all(&directory) {
            Ok(()) => {
                let parent = directory.parent().ok_or_else(|| {
                    LogError::io(
                        "resolve removed Raft log parent",
                        &directory,
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "Raft log group directory must have a parent",
                        ),
                    )
                })?;
                std::fs::File::open(parent)
                    .and_then(|parent| parent.sync_all())
                    .map_err(|error| {
                        LogError::io("sync removed Raft log parent directory", parent, error)
                    })?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(LogError::io(
                    "remove stopped Raft log directory",
                    &directory,
                    error,
                ));
            }
        }
        Ok(())
    }

    /// Appends and acknowledges one typed entry after its frame and location
    /// are both durable.
    pub fn append<A, C>(
        &mut self,
        entry: &Entry<TypeConfig<A>>,
        commands: &C,
    ) -> Result<LogLocation<NID>, LogError>
    where
        A: RaftApplication<NodeId = NID, Node = EmptyNode>,
        C: ApplicationCommandAdapter<A::Command>,
    {
        self.append_batch(std::slice::from_ref(entry), commands)?;
        self.location(entry.log_id.index)?
            .ok_or(LogError::RecordMismatch {
                record: "saved log location",
            })
    }

    /// Appends one received group with one sync and Redb commit per segment.
    pub fn append_batch<A, C>(
        &mut self,
        entries: &[Entry<TypeConfig<A>>],
        commands: &C,
    ) -> Result<(), LogError>
    where
        A: RaftApplication<NodeId = NID, Node = EmptyNode>,
        C: ApplicationCommandAdapter<A::Command>,
    {
        let removed = self.last_removed_log_id()?;
        let mut pending = Vec::with_capacity(entries.len());
        let result = (|| {
            for entry in entries {
                if removed
                    .as_ref()
                    .is_some_and(|removed| entry.log_id.index <= removed.index)
                {
                    return Err(LogError::LogIndexAlreadyRemoved {
                        index: entry.log_id.index,
                    });
                }
                if let Some(existing) = self.location(entry.log_id.index)? {
                    if existing.log_id() == &entry.log_id {
                        continue;
                    }
                    return Err(LogError::ConflictingLogId {
                        index: entry.log_id.index,
                    });
                }
                if let Some(existing) = pending.iter().find(|location: &&LogLocation<NID>| {
                    location.log_id().index == entry.log_id.index
                }) {
                    if existing.log_id() == &entry.log_id {
                        continue;
                    }
                    return Err(LogError::ConflictingLogId {
                        index: entry.log_id.index,
                    });
                }

                let plaintext = Zeroizing::new(encode_log_entry::<A, _, _>(
                    entry,
                    &self.node_ids,
                    commands,
                    self.protocol_limits,
                )?);
                let plaintext_bytes =
                    u32::try_from(plaintext.len()).map_err(|_| LogError::LengthOverflow)?;
                let ciphertext_bytes = plaintext
                    .len()
                    .checked_add(AEAD_TAG_BYTES)
                    .and_then(|length| u32::try_from(length).ok())
                    .ok_or(LogError::LengthOverflow)?;

                let (header, frame_bytes, segment_id, frame_number, frame_offset) = loop {
                    self.ensure_active_segment()?;
                    let active = self.active.as_ref().ok_or(LogError::RecordMismatch {
                        record: "active segment",
                    })?;
                    let header = encode_frame_header(
                        active.id,
                        active.next_frame_number,
                        &entry.log_id,
                        plaintext_bytes,
                        ciphertext_bytes,
                        &self.node_ids,
                        self.protocol_limits,
                    )?;
                    let frame_bytes = complete_frame_size(header.len(), ciphertext_bytes as usize)?;
                    if frame_bytes > self.log_limits.max_frame_bytes() {
                        return Err(LogError::FrameTooLarge {
                            actual: frame_bytes,
                            maximum: self.log_limits.max_frame_bytes(),
                        });
                    }
                    let required = active
                        .bytes_written
                        .checked_add(frame_bytes as u64)
                        .ok_or(LogError::LengthOverflow)?;
                    if required <= self.log_limits.max_segment_bytes() {
                        break (
                            header,
                            frame_bytes,
                            active.id,
                            active.next_frame_number,
                            active.bytes_written,
                        );
                    }
                    if !active.has_frames {
                        return Err(LogError::SegmentTooSmall {
                            required,
                            maximum: self.log_limits.max_segment_bytes(),
                        });
                    }
                    self.sync_pending_locations(&mut pending)?;
                    self.active = None;
                };

                let next_frame_number = frame_number
                    .checked_add(1)
                    .ok_or(LogError::FrameNumberExhausted)?;
                let next_bytes_written = frame_offset
                    .checked_add(frame_bytes as u64)
                    .ok_or(LogError::LengthOverflow)?;
                let stored_frame_bytes =
                    u32::try_from(frame_bytes).map_err(|_| LogError::LengthOverflow)?;
                let frame = self.encrypt_frame(&header, segment_id, frame_number, plaintext)?;
                if frame.len() != frame_bytes {
                    return Err(LogError::RecordMismatch {
                        record: "encoded frame length",
                    });
                }

                let write_result = self
                    .active
                    .as_mut()
                    .ok_or(LogError::RecordMismatch {
                        record: "active segment",
                    })?
                    .file
                    .write_all(&frame);
                if let Err(error) = write_result {
                    self.active = None;
                    return Err(LogError::append_result_unknown(error));
                }
                let active = self.active.as_mut().ok_or(LogError::RecordMismatch {
                    record: "active segment",
                })?;
                active.bytes_written = next_bytes_written;
                active.next_frame_number = next_frame_number;
                active.has_frames = true;
                pending.push(LogLocation {
                    log_id: entry.log_id.clone(),
                    segment_id,
                    frame_offset,
                    frame_bytes: stored_frame_bytes,
                    frame_number,
                });
            }
            self.sync_pending_locations(&mut pending)
        })();
        if result.is_err() && !pending.is_empty() {
            self.active = None;
        }
        result
    }

    /// Returns one durable frame location without reading its segment.
    pub fn location(&self, index: u64) -> Result<Option<LogLocation<NID>>, LogError> {
        let read = self.database.begin_read()?;
        let locations = read.open_table(LOCATIONS)?;
        let key = (self.encoded_group_id.as_slice(), index);
        let Some(stored) = locations.get(&key)? else {
            return Ok(None);
        };
        let (group_id, location) = decode_location(
            stored.value(),
            &self.group_ids,
            &self.node_ids,
            self.protocol_limits,
        )?;
        self.check_location(index, &group_id, &location)?;
        Ok(Some(location))
    }

    /// Reads, authenticates, and decodes one durable entry by log index.
    pub fn read_entry<A, C>(
        &self,
        index: u64,
        commands: &C,
    ) -> Result<Option<Entry<TypeConfig<A>>>, LogError>
    where
        A: RaftApplication<NodeId = NID, Node = EmptyNode>,
        C: ApplicationCommandAdapter<A::Command>,
    {
        let Some(location) = self.location(index)? else {
            return Ok(None);
        };
        self.read_entry_at_location(&location, commands).map(Some)
    }

    /// Reads one entry from an already checked durable frame location.
    pub(crate) fn read_entry_at_location<A, C>(
        &self,
        location: &LogLocation<NID>,
        commands: &C,
    ) -> Result<Entry<TypeConfig<A>>, LogError>
    where
        A: RaftApplication<NodeId = NID, Node = EmptyNode>,
        C: ApplicationCommandAdapter<A::Command>,
    {
        let path = self.segment_path(location.segment_id());
        let mut file = self
            .file_system
            .open_segment(&path)
            .map_err(|error| LogError::io("open Raft log segment", &path, error))?;
        let segment_header_bytes =
            self.read_and_check_segment_header(&mut *file, &path, location.segment_id())?;
        let plaintext = self.read_frame(&mut *file, &path, segment_header_bytes, location)?;
        let entry = decode_log_entry::<A, _, _>(
            &plaintext,
            &self.node_ids,
            commands,
            self.protocol_limits,
        )?;
        if entry.log_id != *location.log_id() {
            return Err(LogError::RecordMismatch {
                record: "decrypted log ID",
            });
        }
        Ok(entry)
    }

    /// Reads the next bounded group of committed entries after local apply.
    ///
    /// The caller applies the returned entries in order, saves the new
    /// application state, then calls this method again with its new index.
    pub fn read_committed_after<A, C>(
        &self,
        applied_log_id: Option<ApplyContext>,
        maximum_entries: usize,
        commands: &C,
    ) -> Result<Vec<Entry<TypeConfig<A>>>, LogError>
    where
        A: RaftApplication<NodeId = NID, Node = EmptyNode>,
        C: ApplicationCommandAdapter<A::Command>,
    {
        if maximum_entries == 0 {
            return Err(LogError::EmptyReplayBatch);
        }
        let Some(committed) = self.committed_log_id()? else {
            return match applied_log_id {
                Some(applied) => Err(LogError::AppliedLogWithoutCommit {
                    applied: applied.index(),
                }),
                None => Ok(Vec::new()),
            };
        };
        let first_index = match applied_log_id {
            Some(applied) if applied.index() > committed.index => {
                return Err(LogError::AppliedLogAheadOfCommit {
                    applied: applied.index(),
                    committed: committed.index,
                });
            }
            Some(applied) => {
                let stored_term = if applied.index() == committed.index {
                    committed.leader_id.term
                } else if let Some(location) = self.location(applied.index())? {
                    location.log_id().leader_id.term
                } else if let Some(removed) = self.last_removed_log_id()? {
                    if removed.index > applied.index() {
                        return Err(LogError::MissingLogIndex {
                            index: applied
                                .index()
                                .checked_add(1)
                                .ok_or(LogError::LengthOverflow)?,
                        });
                    }
                    if removed.index != applied.index() {
                        return Err(LogError::MissingLogIndex {
                            index: applied.index(),
                        });
                    }
                    removed.leader_id.term
                } else {
                    return Err(LogError::MissingLogIndex {
                        index: applied.index(),
                    });
                };
                if applied.term() != stored_term {
                    return Err(LogError::AppliedLogTermMismatch {
                        index: applied.index(),
                        applied_term: applied.term(),
                        stored_term,
                    });
                }
                if applied.index() == committed.index {
                    return Ok(Vec::new());
                }
                applied
                    .index()
                    .checked_add(1)
                    .ok_or(LogError::LengthOverflow)?
            }
            None => 0,
        };
        let maximum_entries =
            u64::try_from(maximum_entries).map_err(|_| LogError::LengthOverflow)?;
        let last_index = first_index
            .saturating_add(maximum_entries.saturating_sub(1))
            .min(committed.index);
        let entry_count = last_index
            .checked_sub(first_index)
            .and_then(|count| count.checked_add(1))
            .and_then(|count| usize::try_from(count).ok())
            .ok_or(LogError::LengthOverflow)?;
        let mut entries = Vec::with_capacity(entry_count);
        for index in first_index..=last_index {
            let entry = self
                .read_entry(index, commands)?
                .ok_or(LogError::MissingLogIndex { index })?;
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Opens the store with a deterministic filesystem for failure testing.
    pub(super) fn open_with_file_system<K>(
        settings: EncryptedLogSettings<GID, G, N>,
        key_provider: &K,
        file_system: Arc<dyn FileSystem>,
    ) -> Result<Self, LogError>
    where
        K: GroupKeyProvider<GID>,
    {
        let EncryptedLogSettings {
            directory,
            database,
            group_id,
            group_ids,
            node_ids,
            protocol_limits,
            log_limits,
        } = settings;
        let encoded_group_id = encode_group_id(&group_id, &group_ids, protocol_limits)?;
        let group_key = key_provider
            .key_for_group(&group_id)
            .map_err(LogError::group_key)?;

        let parent = directory.parent().ok_or_else(|| {
            LogError::io(
                "resolve Raft log group parent",
                &directory,
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Raft log group directory must have a parent",
                ),
            )
        })?;
        let created = file_system
            .create_directory(&directory)
            .map_err(|error| LogError::io("create Raft log group directory", &directory, error))?;
        if created {
            file_system.sync_directory(parent).map_err(|error| {
                LogError::io("sync Raft log group parent directory", parent, error)
            })?;
        }

        let write = database.begin_write()?;
        {
            let _locations = write.open_table(LOCATIONS)?;
            let _states = write.open_table(LOG_STATES)?;
        }
        write.commit()?;

        let log = Self {
            directory,
            database,
            group_id,
            encoded_group_id,
            group_key,
            group_ids,
            node_ids,
            protocol_limits,
            log_limits,
            file_system,
            active: None,
            node_type: PhantomData,
        };
        log.recover_files()?;
        Ok(log)
    }

    /// Creates and durably publishes a fresh segment when none is active.
    fn ensure_active_segment(&mut self) -> Result<(), LogError> {
        if self.active.is_some() {
            return Ok(());
        }

        let segment_id = self
            .file_system
            .new_segment_id()
            .map_err(|source| LogError::Random { source })?;
        let path = self.segment_path(segment_id);
        let header = encode_segment_header(
            &self.group_id,
            segment_id,
            &self.group_ids,
            self.protocol_limits,
        )?;
        let encoded_header = encode_segment_file_header(&header)?;
        if encoded_header.len() as u64 >= self.log_limits.max_segment_bytes() {
            return Err(LogError::SegmentTooSmall {
                required: encoded_header.len() as u64 + 1,
                maximum: self.log_limits.max_segment_bytes(),
            });
        }

        let mut file = self
            .file_system
            .create_segment(&path)
            .map_err(|error| LogError::io("create Raft log segment", &path, error))?;
        file.write_all(&encoded_header)
            .map_err(|error| LogError::io("write Raft log segment header", &path, error))?;
        file.sync_all()
            .map_err(|error| LogError::io("sync Raft log segment header", &path, error))?;
        self.file_system
            .sync_directory(&self.directory)
            .map_err(|error| {
                LogError::io("sync Raft log segment directory", &self.directory, error)
            })?;
        self.active = Some(ActiveSegment {
            id: segment_id,
            file,
            bytes_written: encoded_header.len() as u64,
            next_frame_number: 0,
            has_frames: false,
        });
        Ok(())
    }

    /// Syncs pending frames before committing their locations together.
    fn sync_pending_locations(
        &mut self,
        pending: &mut Vec<LogLocation<NID>>,
    ) -> Result<(), LogError> {
        if pending.is_empty() {
            return Ok(());
        }
        let sync_result = self
            .active
            .as_ref()
            .ok_or(LogError::RecordMismatch {
                record: "active segment",
            })?
            .file
            .sync_all();
        if let Err(error) = sync_result {
            self.active = None;
            return Err(LogError::append_result_unknown(error));
        }
        if let Err(error) = self.save_locations(pending) {
            self.active = None;
            return Err(error);
        }
        pending.clear();
        Ok(())
    }

    /// Commits synced frame locations in one Redb transaction.
    fn save_locations(&self, locations: &[LogLocation<NID>]) -> Result<(), LogError> {
        let encoded = locations
            .iter()
            .map(|location| {
                encode_location(
                    &self.group_id,
                    location,
                    &self.group_ids,
                    &self.node_ids,
                    self.protocol_limits,
                )
                .map(|encoded| (location.log_id().index, encoded))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let write = self.database.begin_write()?;
        let mut locations = write.open_table(LOCATIONS)?;
        for (index, encoded) in &encoded {
            let key = (self.encoded_group_id.as_slice(), *index);
            if locations.get(&key)?.is_some() {
                return Err(LogError::ConflictingLogId { index: *index });
            }
            locations.insert(&key, encoded.as_slice())?;
        }
        drop(locations);
        write.commit()?;
        Ok(())
    }

    /// Checks a location against the group ID and log index in its Redb key.
    fn check_location(
        &self,
        index: u64,
        group_id: &GID,
        location: &LogLocation<NID>,
    ) -> Result<(), LogError> {
        if group_id != &self.group_id || location.log_id().index != index {
            return Err(LogError::RecordMismatch { record: "location" });
        }
        Ok(())
    }

    /// Reads and validates the Cap'n Proto header at the start of a segment.
    fn read_and_check_segment_header(
        &self,
        file: &mut dyn SegmentFile,
        path: &Path,
        expected_segment_id: SegmentId,
    ) -> Result<u64, LogError> {
        let mut prefix = [0; SEGMENT_PREFIX_BYTES];
        file.read_exact(&mut prefix)
            .map_err(|error| LogError::io("read Raft log segment prefix", path, error))?;
        if prefix[..8] != SEGMENT_MAGIC {
            return Err(LogError::RecordMismatch {
                record: "segment prefix",
            });
        }
        let header_len = read_u32(&prefix[8..12]) as usize;
        if header_len > self.protocol_limits.max_message_bytes() {
            return Err(ProtocolError::MessageTooLarge {
                actual: header_len,
                maximum: self.protocol_limits.max_message_bytes(),
            }
            .into());
        }
        let mut header = vec![0; header_len];
        file.read_exact(&mut header)
            .map_err(|error| LogError::io("read Raft log segment header", path, error))?;
        if prefix[12..16] != framing_checksum(&header) {
            return Err(LogError::ChecksumMismatch {
                record: "segment header",
            });
        }
        let (group_id, segment_id) =
            decode_segment_header(&header, &self.group_ids, self.protocol_limits)?;
        if group_id != self.group_id || segment_id != expected_segment_id {
            return Err(LogError::RecordMismatch {
                record: "segment header",
            });
        }
        u64::try_from(SEGMENT_PREFIX_BYTES + header_len).map_err(|_| LogError::LengthOverflow)
    }

    /// Reads and decrypts one frame from an already checked segment.
    fn read_frame(
        &self,
        file: &mut dyn SegmentFile,
        path: &Path,
        segment_header_bytes: u64,
        location: &LogLocation<NID>,
    ) -> Result<Zeroizing<Vec<u8>>, LogError> {
        if location.frame_offset() < segment_header_bytes {
            return Err(LogError::RecordMismatch {
                record: "location offset",
            });
        }
        if location.frame_bytes() as usize > self.log_limits.max_frame_bytes() {
            return Err(LogError::FrameTooLarge {
                actual: location.frame_bytes() as usize,
                maximum: self.log_limits.max_frame_bytes(),
            });
        }
        let frame_end = location
            .frame_offset()
            .checked_add(u64::from(location.frame_bytes()))
            .ok_or(LogError::LengthOverflow)?;
        let file_len = file
            .len()
            .map_err(|error| LogError::io("inspect Raft log segment", path, error))?;
        if frame_end > file_len {
            return Err(LogError::RecordMismatch {
                record: "location file range",
            });
        }

        file.seek(SeekFrom::Start(location.frame_offset()))
            .map_err(|error| LogError::io("seek Raft log frame", path, error))?;
        let mut frame = vec![0; location.frame_bytes() as usize];
        file.read_exact(&mut frame)
            .map_err(|error| LogError::io("read Raft log frame", path, error))?;
        let (header_bytes, ciphertext) = decode_frame(&frame)?;
        let mut plaintext = Zeroizing::new(ciphertext);
        let header = decode_frame_header(header_bytes, &self.node_ids, self.protocol_limits)?;
        self.check_frame_header(location, &header, plaintext.len())?;

        let segment_key = self.segment_key(location.segment_id());
        let cipher = XChaCha20Poly1305::new(Key::from_slice(segment_key.as_ref()));
        let nonce = frame_nonce(location.segment_id(), location.frame_number());
        let aad = frame_aad(&self.encoded_group_id, header_bytes)?;
        cipher
            .decrypt_in_place(XNonce::from_slice(&nonce), &aad, &mut *plaintext)
            .map_err(|_| LogError::AuthenticationFailed)?;
        if plaintext.len() != header.plaintext_bytes as usize {
            return Err(LogError::RecordMismatch {
                record: "decrypted frame length",
            });
        }
        Ok(plaintext)
    }

    /// Encrypts encoded entry bytes for one exact segment and frame number.
    fn encrypt_frame(
        &self,
        header: &[u8],
        segment_id: SegmentId,
        frame_number: u64,
        mut plaintext: Zeroizing<Vec<u8>>,
    ) -> Result<Vec<u8>, LogError> {
        let segment_key = self.segment_key(segment_id);
        let cipher = XChaCha20Poly1305::new(Key::from_slice(segment_key.as_ref()));
        let nonce = frame_nonce(segment_id, frame_number);
        let aad = frame_aad(&self.encoded_group_id, header)?;
        cipher
            .encrypt_in_place(XNonce::from_slice(&nonce), &aad, &mut *plaintext)
            .map_err(|_| LogError::EncryptionFailed)?;
        encode_frame(header, &plaintext)
    }

    /// Checks authenticated frame metadata against its durable location.
    fn check_frame_header(
        &self,
        location: &LogLocation<NID>,
        header: &FrameHeader<NID>,
        ciphertext_len: usize,
    ) -> Result<(), LogError> {
        if header.segment_id != location.segment_id()
            || header.frame_number != location.frame_number()
            || header.log_id != *location.log_id()
            || header.ciphertext_bytes as usize != ciphertext_len
        {
            return Err(LogError::RecordMismatch {
                record: "frame header",
            });
        }
        Ok(())
    }

    /// Derives one independent key from the group, its key, and the segment ID.
    fn segment_key(&self, segment_id: SegmentId) -> Zeroizing<[u8; 32]> {
        let mut hasher = blake3::Hasher::new_keyed(self.group_key.as_bytes());
        hasher.update(SEGMENT_KEY_DOMAIN);
        hasher.update(&(self.encoded_group_id.len() as u64).to_be_bytes());
        hasher.update(&self.encoded_group_id);
        hasher.update(segment_id.as_bytes());
        Zeroizing::new(*hasher.finalize().as_bytes())
    }

    /// Returns the path named by a checked segment ID.
    fn segment_path(&self, segment_id: SegmentId) -> PathBuf {
        self.directory.join(segment_id.file_name())
    }
}

/// Adds the fixed segment prefix and checksum to a Cap'n Proto header.
fn encode_segment_file_header(header: &[u8]) -> Result<Vec<u8>, LogError> {
    let header_len = u32::try_from(header.len()).map_err(|_| LogError::LengthOverflow)?;
    let mut encoded = Vec::with_capacity(SEGMENT_PREFIX_BYTES + header.len());
    encoded.extend_from_slice(&SEGMENT_MAGIC);
    encoded.extend_from_slice(&header_len.to_be_bytes());
    encoded.extend_from_slice(&framing_checksum(header));
    encoded.extend_from_slice(header);
    Ok(encoded)
}

/// Adds fixed frame lengths and a checksum around authenticated frame bytes.
fn encode_frame(header: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, LogError> {
    let header_len = u32::try_from(header.len()).map_err(|_| LogError::LengthOverflow)?;
    let ciphertext_len = u32::try_from(ciphertext.len()).map_err(|_| LogError::LengthOverflow)?;
    let mut checksum_input = Vec::with_capacity(header.len() + ciphertext.len());
    checksum_input.extend_from_slice(header);
    checksum_input.extend_from_slice(ciphertext);

    let mut encoded = Vec::with_capacity(FRAME_PREFIX_BYTES + checksum_input.len());
    encoded.extend_from_slice(&FRAME_MAGIC);
    encoded.extend_from_slice(&header_len.to_be_bytes());
    encoded.extend_from_slice(&ciphertext_len.to_be_bytes());
    encoded.extend_from_slice(&framing_checksum(&checksum_input));
    encoded.extend_from_slice(&checksum_input);
    Ok(encoded)
}

/// Splits and checks one complete frame read through a bounded location.
fn decode_frame(frame: &[u8]) -> Result<(&[u8], Vec<u8>), LogError> {
    if frame.len() < FRAME_PREFIX_BYTES || frame[..4] != FRAME_MAGIC {
        return Err(LogError::RecordMismatch {
            record: "frame prefix",
        });
    }
    let header_len = read_u32(&frame[4..8]) as usize;
    let ciphertext_len = read_u32(&frame[8..12]) as usize;
    let expected = complete_frame_size(header_len, ciphertext_len)?;
    if expected != frame.len() {
        return Err(LogError::RecordMismatch {
            record: "frame length",
        });
    }
    if frame[12..16] != framing_checksum(&frame[FRAME_PREFIX_BYTES..]) {
        return Err(LogError::ChecksumMismatch { record: "frame" });
    }
    let header_end = FRAME_PREFIX_BYTES + header_len;
    Ok((
        &frame[FRAME_PREFIX_BYTES..header_end],
        frame[header_end..].to_vec(),
    ))
}

/// Returns the complete encoded size for one frame.
fn complete_frame_size(header_len: usize, ciphertext_len: usize) -> Result<usize, LogError> {
    FRAME_PREFIX_BYTES
        .checked_add(header_len)
        .and_then(|length| length.checked_add(ciphertext_len))
        .ok_or(LogError::LengthOverflow)
}

/// Derives the unique 24-byte XChaCha nonce for one segment position.
fn frame_nonce(segment_id: SegmentId, frame_number: u64) -> [u8; 24] {
    let mut nonce = [0; 24];
    nonce[..16].copy_from_slice(segment_id.as_bytes());
    nonce[16..].copy_from_slice(&frame_number.to_be_bytes());
    nonce
}

/// Binds group identity and all stored frame metadata into AEAD.
fn frame_aad(group_id: &[u8], frame_header: &[u8]) -> Result<Vec<u8>, LogError> {
    let group_len = u64::try_from(group_id.len()).map_err(|_| LogError::LengthOverflow)?;
    let mut aad = Vec::with_capacity(
        FRAME_AAD_DOMAIN.len() + std::mem::size_of::<u64>() + group_id.len() + frame_header.len(),
    );
    aad.extend_from_slice(FRAME_AAD_DOMAIN);
    aad.extend_from_slice(&group_len.to_be_bytes());
    aad.extend_from_slice(group_id);
    aad.extend_from_slice(frame_header);
    Ok(aad)
}

/// Computes the non-security checksum used to spot an incomplete write.
fn framing_checksum(bytes: &[u8]) -> [u8; 4] {
    let digest = blake3::hash(bytes);
    let mut checksum = [0; 4];
    checksum.copy_from_slice(&digest.as_bytes()[..4]);
    checksum
}

/// Reads one big-endian frame length from a checked four-byte slice.
fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}
