use openraft::{LogId, NodeId};

pub(crate) const SEGMENT_ID_BYTES: usize = 16;

/// Random identity of one append-only encrypted log segment.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SegmentId([u8; SEGMENT_ID_BYTES]);

impl SegmentId {
    /// Creates an ID from exactly 16 random bytes.
    pub(crate) const fn new(bytes: [u8; SEGMENT_ID_BYTES]) -> Self {
        Self(bytes)
    }

    /// Borrows the bytes bound into keys, nonces, and stored records.
    pub(crate) const fn as_bytes(&self) -> &[u8; SEGMENT_ID_BYTES] {
        &self.0
    }

    /// Returns the stable file name used for this segment.
    pub(crate) fn file_name(self) -> String {
        format!("segment-{:032x}.raftlog", u128::from_be_bytes(self.0))
    }
}

/// Durable location of one encrypted log entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogLocation<NID>
where
    NID: NodeId,
{
    pub(crate) log_id: LogId<NID>,
    pub(crate) segment_id: SegmentId,
    pub(crate) frame_offset: u64,
    pub(crate) frame_bytes: u32,
    pub(crate) frame_number: u64,
}

impl<NID> LogLocation<NID>
where
    NID: NodeId,
{
    /// Returns the Raft log ID stored at this location.
    #[must_use]
    pub const fn log_id(&self) -> &LogId<NID> {
        &self.log_id
    }

    /// Returns the segment containing the encrypted frame.
    #[must_use]
    pub const fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    /// Returns the byte offset of the frame prefix.
    #[must_use]
    pub const fn frame_offset(&self) -> u64 {
        self.frame_offset
    }

    /// Returns the complete stored frame size.
    #[must_use]
    pub const fn frame_bytes(&self) -> u32 {
        self.frame_bytes
    }

    /// Returns the frame number used to make the encryption nonce unique.
    #[must_use]
    pub const fn frame_number(&self) -> u64 {
        self.frame_number
    }
}

/// Checked metadata read from one encrypted frame header.
pub(crate) struct FrameHeader<NID>
where
    NID: NodeId,
{
    pub(crate) segment_id: SegmentId,
    pub(crate) frame_number: u64,
    pub(crate) log_id: LogId<NID>,
    pub(crate) plaintext_bytes: u32,
    pub(crate) ciphertext_bytes: u32,
}

/// Small saved value needed after old entries are removed.
pub(crate) struct SavedLogState<NID>
where
    NID: NodeId,
{
    pub(crate) last_removed_log_id: Option<LogId<NID>>,
    pub(crate) committed_log_id: Option<LogId<NID>>,
}
