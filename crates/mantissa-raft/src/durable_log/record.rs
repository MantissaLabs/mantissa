use capnp::message::Builder;
use mantissa_protocol::raft::{
    raft_log_frame_header, raft_log_location, raft_log_segment_header, raft_log_state,
};
use openraft::{LogId, NodeId};

use super::model::{FrameHeader, SavedLogState};
use super::{LogError, LogLocation, SegmentId};
use crate::catalog::GroupIdAdapter;
use crate::protocol::{
    NodeIdAdapter, ProtocolError, ProtocolLimits, check_output_size, read_log_id, read_message,
    write_log_id,
};

const LOG_RECORD_FORMAT_VERSION: u16 = 2;

/// Encodes the identity written at the beginning of one segment.
pub(crate) fn encode_segment_header<GID, G>(
    group_id: &GID,
    segment_id: SegmentId,
    group_ids: &G,
    limits: ProtocolLimits,
) -> Result<Vec<u8>, LogError>
where
    G: GroupIdAdapter<GID>,
{
    let mut message = Builder::new_default();
    let mut root = message.init_root::<raft_log_segment_header::Builder<'_>>();
    root.set_format_version(LOG_RECORD_FORMAT_VERSION);
    group_ids
        .write(root.reborrow().init_group_id(), group_id)
        .map_err(ProtocolError::group_id)?;
    root.set_segment_id(segment_id.as_bytes());
    let bytes = capnp::serialize::write_message_to_words(&message);
    Ok(check_output_size(bytes, limits)?)
}

/// Decodes and checks one stored segment identity.
pub(crate) fn decode_segment_header<GID, G>(
    bytes: &[u8],
    group_ids: &G,
    limits: ProtocolLimits,
) -> Result<(GID, SegmentId), LogError>
where
    G: GroupIdAdapter<GID>,
{
    let message = read_message(bytes, limits)?;
    let root = message
        .root::<raft_log_segment_header::Reader<'_>>()
        .map_err(ProtocolError::from)?;
    check_format("segment header", root.get_format_version())?;
    let group_id = group_ids
        .read(root.get_group_id().map_err(ProtocolError::from)?)
        .map_err(ProtocolError::group_id)?;
    let segment_id = read_segment_id(root.get_segment_id().map_err(ProtocolError::from)?)?;
    Ok((group_id, segment_id))
}

/// Encodes authenticated metadata for one encrypted frame.
pub(crate) fn encode_frame_header<NID, N>(
    segment_id: SegmentId,
    frame_number: u64,
    log_id: &LogId<NID>,
    plaintext_bytes: u32,
    ciphertext_bytes: u32,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<Vec<u8>, LogError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    let mut message = Builder::new_default();
    let mut root = message.init_root::<raft_log_frame_header::Builder<'_>>();
    root.set_format_version(LOG_RECORD_FORMAT_VERSION);
    root.set_segment_id(segment_id.as_bytes());
    root.set_frame_number(frame_number);
    write_log_id(root.reborrow().init_log_id(), log_id, node_ids)?;
    root.set_plaintext_bytes(plaintext_bytes);
    root.set_ciphertext_bytes(ciphertext_bytes);
    let bytes = capnp::serialize::write_message_to_words(&message);
    Ok(check_output_size(bytes, limits)?)
}

/// Decodes authenticated metadata from one encrypted frame.
pub(crate) fn decode_frame_header<NID, N>(
    bytes: &[u8],
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<FrameHeader<NID>, LogError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    let message = read_message(bytes, limits)?;
    let root = message
        .root::<raft_log_frame_header::Reader<'_>>()
        .map_err(ProtocolError::from)?;
    check_format("frame header", root.get_format_version())?;
    Ok(FrameHeader {
        segment_id: read_segment_id(root.get_segment_id().map_err(ProtocolError::from)?)?,
        frame_number: root.get_frame_number(),
        log_id: read_log_id(root.get_log_id().map_err(ProtocolError::from)?, node_ids)?,
        plaintext_bytes: root.get_plaintext_bytes(),
        ciphertext_bytes: root.get_ciphertext_bytes(),
    })
}

/// Encodes one durable Redb location.
pub(crate) fn encode_location<GID, NID, G, N>(
    group_id: &GID,
    location: &LogLocation<NID>,
    group_ids: &G,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<Vec<u8>, LogError>
where
    NID: NodeId,
    G: GroupIdAdapter<GID>,
    N: NodeIdAdapter<NID>,
{
    let mut message = Builder::new_default();
    let mut root = message.init_root::<raft_log_location::Builder<'_>>();
    root.set_format_version(LOG_RECORD_FORMAT_VERSION);
    group_ids
        .write(root.reborrow().init_group_id(), group_id)
        .map_err(ProtocolError::group_id)?;
    write_log_id(root.reborrow().init_log_id(), location.log_id(), node_ids)?;
    root.set_segment_id(location.segment_id().as_bytes());
    root.set_frame_offset(location.frame_offset());
    root.set_frame_bytes(location.frame_bytes());
    root.set_frame_number(location.frame_number());
    let bytes = capnp::serialize::write_message_to_words(&message);
    Ok(check_output_size(bytes, limits)?)
}

/// Decodes one durable Redb location and its stored group identity.
pub(crate) fn decode_location<GID, NID, G, N>(
    bytes: &[u8],
    group_ids: &G,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<(GID, LogLocation<NID>), LogError>
where
    NID: NodeId,
    G: GroupIdAdapter<GID>,
    N: NodeIdAdapter<NID>,
{
    let message = read_message(bytes, limits)?;
    let root = message
        .root::<raft_log_location::Reader<'_>>()
        .map_err(ProtocolError::from)?;
    check_format("location", root.get_format_version())?;
    let group_id = group_ids
        .read(root.get_group_id().map_err(ProtocolError::from)?)
        .map_err(ProtocolError::group_id)?;
    let location = LogLocation {
        log_id: read_log_id(root.get_log_id().map_err(ProtocolError::from)?, node_ids)?,
        segment_id: read_segment_id(root.get_segment_id().map_err(ProtocolError::from)?)?,
        frame_offset: root.get_frame_offset(),
        frame_bytes: root.get_frame_bytes(),
        frame_number: root.get_frame_number(),
    };
    Ok((group_id, location))
}

/// Encodes the newest entry removed from the start of one log.
pub(crate) fn encode_log_state<GID, NID, G, N>(
    group_id: &GID,
    state: &SavedLogState<NID>,
    group_ids: &G,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<Vec<u8>, LogError>
where
    NID: NodeId,
    G: GroupIdAdapter<GID>,
    N: NodeIdAdapter<NID>,
{
    let mut message = Builder::new_default();
    let mut root = message.init_root::<raft_log_state::Builder<'_>>();
    root.set_format_version(LOG_RECORD_FORMAT_VERSION);
    group_ids
        .write(root.reborrow().init_group_id(), group_id)
        .map_err(ProtocolError::group_id)?;
    if let Some(log_id) = &state.last_removed_log_id {
        write_log_id(root.reborrow().init_last_removed_log_id(), log_id, node_ids)?;
    }
    if let Some(log_id) = &state.committed_log_id {
        write_log_id(root.reborrow().init_committed_log_id(), log_id, node_ids)?;
    }
    let bytes = capnp::serialize::write_message_to_words(&message);
    Ok(check_output_size(bytes, limits)?)
}

/// Decodes the saved removal point for one log.
pub(crate) fn decode_log_state<GID, NID, G, N>(
    bytes: &[u8],
    group_ids: &G,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<(GID, SavedLogState<NID>), LogError>
where
    NID: NodeId,
    G: GroupIdAdapter<GID>,
    N: NodeIdAdapter<NID>,
{
    let message = read_message(bytes, limits)?;
    let root = message
        .root::<raft_log_state::Reader<'_>>()
        .map_err(ProtocolError::from)?;
    check_format("log state", root.get_format_version())?;
    let group_id = group_ids
        .read(root.get_group_id().map_err(ProtocolError::from)?)
        .map_err(ProtocolError::group_id)?;
    let last_removed_log_id = if root.has_last_removed_log_id() {
        Some(read_log_id(
            root.get_last_removed_log_id()
                .map_err(ProtocolError::from)?,
            node_ids,
        )?)
    } else {
        None
    };
    let committed_log_id = if root.has_committed_log_id() {
        Some(read_log_id(
            root.get_committed_log_id().map_err(ProtocolError::from)?,
            node_ids,
        )?)
    } else {
        None
    };
    Ok((
        group_id,
        SavedLogState {
            last_removed_log_id,
            committed_log_id,
        },
    ))
}

/// Rejects a stored format number this build does not understand.
fn check_format(record: &'static str, actual: u16) -> Result<(), LogError> {
    if actual != LOG_RECORD_FORMAT_VERSION {
        return Err(LogError::UnsupportedFormat { record, actual });
    }
    Ok(())
}

/// Reads one exact 16-byte segment identity.
fn read_segment_id(bytes: &[u8]) -> Result<SegmentId, LogError> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| LogError::InvalidSegmentId {
        actual: bytes.len(),
    })?;
    Ok(SegmentId::new(bytes))
}
