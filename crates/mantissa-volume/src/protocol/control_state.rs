//! Strict Cap'n Proto conversion for bounded volume control state.

use std::collections::BTreeSet;

use capnp::message::{Builder, ReaderOptions};
use mantissa_protocol::raft::{raft_application_command, raft_application_command::Which};
use mantissa_protocol::volumes::{
    VolumeControlCommandRejection as WireRejection, VolumeDisposition as WireDisposition,
    adopt_replica_replacement, begin_replica_replacement, begin_volume_recovery,
    cancel_replica_replacement, expand_volume, expected_volume_revision, fence_volume_writer,
    grant_volume_writer, initialize_volume_control_state, revoke_volume_recovery,
    set_volume_disposition, volume_control_command, volume_control_command_response,
    volume_control_snapshot, volume_data_control_state, volume_recovery_grant,
    volume_replacement_grant, volume_writer_grant,
};
use mantissa_raft::protocol::ApplicationCommandAdapter;

use super::{ProtocolError, read_descriptor, read_uuid, write_descriptor};
use crate::control_state::{
    AdoptReplicaReplacement, BeginReplicaReplacement, BeginVolumeRecovery,
    CancelReplicaReplacement, DataControlState, ExpandVolume, ExpectedVolumeRevision,
    FenceVolumeWriter, GrantVolumeWriter, InitializeVolume, RecoveryGrant, ReplacementGrant,
    RevokeVolumeRecovery, SetVolumeDisposition, VolumeCommand, VolumeCommandRejection,
    VolumeCommandResponse, VolumeControlState, VolumeDisposition, WriterGrant,
};
use crate::{
    DriverSessionId, FenceEpoch, RecoveryId, ReplacementId, VolumeCapacity, VolumeGeneration,
    VolumeNodeId,
};

const CONTROL_STATE_FORMAT_VERSION: u16 = 1;
const MAX_NESTING_LEVELS: i32 = 24;
const TRAVERSAL_WORDS_PER_MESSAGE_WORD: usize = 8;

/// Converts bounded control state commands at the generic Raft log boundary.
#[derive(Clone, Copy, Debug, Default)]
pub struct VolumeCommandAdapter;

impl ApplicationCommandAdapter<VolumeCommand> for VolumeCommandAdapter {
    type Error = ProtocolError;

    /// Writes one semantic volume command into the Raft command union.
    fn write(
        &self,
        mut builder: raft_application_command::Builder<'_>,
        command: &VolumeCommand,
    ) -> Result<(), Self::Error> {
        write_volume_command(builder.reborrow().init_volume_control_command(), command);
        Ok(())
    }

    /// Reads one checked semantic volume command from the Raft union.
    fn read(
        &self,
        reader: raft_application_command::Reader<'_>,
    ) -> Result<VolumeCommand, Self::Error> {
        match reader.which() {
            Ok(Which::Invalid(())) => Err(ProtocolError::InvalidApplicationCommand),
            Ok(Which::VolumeControlCommand(Ok(command))) => read_volume_command(command),
            Ok(Which::VolumeControlCommand(Err(error))) => Err(error.into()),
            Err(capnp::NotInSchema(value)) => Err(ProtocolError::UnknownApplicationCommand(value)),
        }
    }
}

/// Writes one bounded semantic volume command.
pub fn write_volume_command(
    mut builder: volume_control_command::Builder<'_>,
    command: &VolumeCommand,
) {
    match command {
        VolumeCommand::Initialize(command) => {
            write_initialize(builder.reborrow().init_initialize(), command);
        }
        VolumeCommand::Expand(command) => {
            write_expand(builder.reborrow().init_expand(), *command);
        }
        VolumeCommand::SetDisposition(command) => {
            write_set_disposition(builder.reborrow().init_set_disposition(), *command);
        }
        VolumeCommand::GrantWriter(command) => {
            write_grant_writer(builder.reborrow().init_grant_writer(), *command);
        }
        VolumeCommand::FenceWriter(command) => {
            write_fence_writer(builder.reborrow().init_fence_writer(), *command);
        }
        VolumeCommand::BeginRecovery(command) => {
            write_begin_recovery(builder.reborrow().init_begin_recovery(), command);
        }
        VolumeCommand::RevokeRecovery(command) => {
            write_revoke_recovery(builder.reborrow().init_revoke_recovery(), *command);
        }
        VolumeCommand::BeginReplacement(command) => {
            write_begin_replacement(builder.reborrow().init_begin_replacement(), *command);
        }
        VolumeCommand::CancelReplacement(command) => {
            write_cancel_replacement(builder.reborrow().init_cancel_replacement(), *command);
        }
        VolumeCommand::AdoptReplacement(command) => {
            write_adopt_replacement(builder.reborrow().init_adopt_replacement(), command);
        }
    }
}

/// Reads one bounded semantic volume command with strict identities and sets.
pub fn read_volume_command(
    reader: volume_control_command::Reader<'_>,
) -> Result<VolumeCommand, ProtocolError> {
    match reader.which() {
        Ok(volume_control_command::Which::Invalid(())) => Err(ProtocolError::InvalidVolumeCommand),
        Ok(volume_control_command::Which::Initialize(command)) => {
            Ok(VolumeCommand::Initialize(read_initialize(command?)?))
        }
        Ok(volume_control_command::Which::Expand(command)) => {
            Ok(VolumeCommand::Expand(read_expand(command?)?))
        }
        Ok(volume_control_command::Which::SetDisposition(command)) => Ok(
            VolumeCommand::SetDisposition(read_set_disposition(command?)?),
        ),
        Ok(volume_control_command::Which::GrantWriter(command)) => {
            Ok(VolumeCommand::GrantWriter(read_grant_writer(command?)?))
        }
        Ok(volume_control_command::Which::FenceWriter(command)) => {
            Ok(VolumeCommand::FenceWriter(read_fence_writer(command?)?))
        }
        Ok(volume_control_command::Which::BeginRecovery(command)) => {
            Ok(VolumeCommand::BeginRecovery(read_begin_recovery(command?)?))
        }
        Ok(volume_control_command::Which::RevokeRecovery(command)) => Ok(
            VolumeCommand::RevokeRecovery(read_revoke_recovery(command?)?),
        ),
        Ok(volume_control_command::Which::BeginReplacement(command)) => Ok(
            VolumeCommand::BeginReplacement(read_begin_replacement(command?)?),
        ),
        Ok(volume_control_command::Which::CancelReplacement(command)) => Ok(
            VolumeCommand::CancelReplacement(read_cancel_replacement(command?)?),
        ),
        Ok(volume_control_command::Which::AdoptReplacement(command)) => Ok(
            VolumeCommand::AdoptReplacement(read_adopt_replacement(command?)?),
        ),
        Err(capnp::NotInSchema(value)) => Err(ProtocolError::UnknownVolumeCommand(value)),
    }
}

/// Writes one deterministic volume command response without retry history.
pub fn write_volume_command_response(
    mut builder: volume_control_command_response::Builder<'_>,
    response: VolumeCommandResponse,
) {
    match response {
        VolumeCommandResponse::Applied { revision, fence } => {
            write_result(builder.reborrow().init_applied(), revision, fence);
        }
        VolumeCommandResponse::Current { revision, fence } => {
            write_result(builder.reborrow().init_current(), revision, fence);
        }
        VolumeCommandResponse::Conflict { current_revision } => {
            builder.set_conflict(current_revision);
        }
        VolumeCommandResponse::Rejected(rejection) => {
            builder.set_rejected(write_rejection(rejection));
        }
    }
}

/// Reads one deterministic volume command response and rejects unknown enum values.
pub fn read_volume_command_response(
    reader: volume_control_command_response::Reader<'_>,
) -> Result<VolumeCommandResponse, ProtocolError> {
    match reader.which() {
        Ok(volume_control_command_response::Which::Invalid(())) => {
            Err(ProtocolError::InvalidVolumeResponse)
        }
        Ok(volume_control_command_response::Which::Applied(result)) => {
            let (revision, fence) = read_result(result?)?;
            Ok(VolumeCommandResponse::Applied { revision, fence })
        }
        Ok(volume_control_command_response::Which::Current(result)) => {
            let (revision, fence) = read_result(result?)?;
            Ok(VolumeCommandResponse::Current { revision, fence })
        }
        Ok(volume_control_command_response::Which::Conflict(current_revision)) => {
            Ok(VolumeCommandResponse::Conflict { current_revision })
        }
        Ok(volume_control_command_response::Which::Rejected(rejection)) => {
            let rejection =
                rejection.map_err(|capnp::NotInSchema(value)| ProtocolError::UnknownEnum {
                    field: "command rejection",
                    value,
                })?;
            Ok(VolumeCommandResponse::Rejected(read_rejection(rejection)?))
        }
        Err(capnp::NotInSchema(value)) => Err(ProtocolError::UnknownEnum {
            field: "volume command response",
            value,
        }),
    }
}

/// Encodes complete bounded control state within the selected snapshot limit.
pub fn encode_control_state(
    state: &VolumeControlState,
    maximum_bytes: usize,
) -> Result<Vec<u8>, ProtocolError> {
    state.validate()?;
    let mut message = Builder::new_default();
    let root = message.init_root::<volume_control_snapshot::Builder<'_>>();
    write_control_state(root, state);
    let encoded = capnp::serialize::write_message_to_words(&message);
    if encoded.len() > maximum_bytes {
        return Err(ProtocolError::StateTooLarge {
            actual: encoded.len(),
            maximum: maximum_bytes,
        });
    }
    Ok(encoded)
}

/// Writes complete bounded control state into an existing typed message field.
pub fn write_control_state(
    mut root: volume_control_snapshot::Builder<'_>,
    state: &VolumeControlState,
) {
    root.set_format_version(CONTROL_STATE_FORMAT_VERSION);
    root.set_revision(state.revision());
    root.set_disposition(write_disposition(state.disposition()));
    if let Some(descriptor) = state.descriptor() {
        write_descriptor(root.reborrow().init_descriptor(), descriptor);
    }
    if let Some(data) = state.data() {
        write_data_control_state(root.reborrow().init_data(), data);
    }
    if let Some(replacement) = state.replacement() {
        write_replacement(root.reborrow().init_replacement(), replacement);
    }
}

/// Decodes and validates complete bounded control state from one snapshot.
pub fn decode_control_state(
    bytes: &[u8],
    maximum_bytes: usize,
) -> Result<VolumeControlState, ProtocolError> {
    if bytes.len() > maximum_bytes {
        return Err(ProtocolError::StateTooLarge {
            actual: bytes.len(),
            maximum: maximum_bytes,
        });
    }
    let message_words = bytes
        .len()
        .checked_add(7)
        .and_then(|value| value.checked_div(8))
        .ok_or(ProtocolError::StateTooLarge {
            actual: bytes.len(),
            maximum: maximum_bytes,
        })?;
    let traversal_words = message_words
        .checked_mul(TRAVERSAL_WORDS_PER_MESSAGE_WORD)
        .ok_or(ProtocolError::StateTooLarge {
            actual: bytes.len(),
            maximum: maximum_bytes,
        })?;
    let mut options = ReaderOptions::new();
    options.traversal_limit_in_words(Some(traversal_words));
    options.nesting_limit(MAX_NESTING_LEVELS);
    let mut remaining = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(&mut remaining, options)?;
    if !remaining.is_empty() {
        return Err(ProtocolError::InvalidState(
            "bytes remain after the control-state message",
        ));
    }
    let root = message.get_root::<volume_control_snapshot::Reader<'_>>()?;
    read_control_state(root)
}

/// Reads and validates complete bounded control state from a typed message field.
pub fn read_control_state(
    root: volume_control_snapshot::Reader<'_>,
) -> Result<VolumeControlState, ProtocolError> {
    if root.get_format_version() != CONTROL_STATE_FORMAT_VERSION {
        return Err(ProtocolError::UnsupportedStateFormat(
            root.get_format_version(),
        ));
    }
    let disposition = match root.get_disposition() {
        Ok(disposition) => read_disposition(disposition)?,
        Err(capnp::NotInSchema(value)) => {
            return Err(ProtocolError::UnknownEnum {
                field: "control-state disposition",
                value,
            });
        }
    };
    let descriptor = root
        .has_descriptor()
        .then(|| read_descriptor(root.get_descriptor()?))
        .transpose()?;
    let data = root
        .has_data()
        .then(|| read_data_control_state(root.get_data()?))
        .transpose()?;
    let replacement = root
        .has_replacement()
        .then(|| read_replacement(root.get_replacement()?))
        .transpose()?;
    Ok(VolumeControlState::from_parts(
        descriptor,
        root.get_revision(),
        disposition,
        data,
        replacement,
    )?)
}

/// Writes initialization descriptor and exact three-copy set.
fn write_initialize(
    mut builder: initialize_volume_control_state::Builder<'_>,
    command: &InitializeVolume,
) {
    write_descriptor(builder.reborrow().init_descriptor(), &command.descriptor);
    write_nodes(
        builder
            .reborrow()
            .init_initial_copies(command.initial_copies.len() as u32),
        &command.initial_copies,
    );
}

/// Reads initialization descriptor and rejects any non-three-copy set.
fn read_initialize(
    reader: initialize_volume_control_state::Reader<'_>,
) -> Result<InitializeVolume, ProtocolError> {
    let initial_copies = read_nodes(reader.get_initial_copies()?, "initial copy node id", 3, 3)?;
    Ok(InitializeVolume {
        descriptor: read_descriptor(reader.get_descriptor()?)?,
        initial_copies,
    })
}

/// Writes compare-and-set identity shared by semantic changes.
fn write_expected(
    mut builder: expected_volume_revision::Builder<'_>,
    expected: ExpectedVolumeRevision,
) {
    builder.set_generation(expected.generation.get());
    builder.set_revision(expected.revision);
}

/// Reads a non-zero generation and exact control revision.
fn read_expected(
    reader: expected_volume_revision::Reader<'_>,
) -> Result<ExpectedVolumeRevision, ProtocolError> {
    Ok(ExpectedVolumeRevision {
        generation: VolumeGeneration::new(reader.get_generation())?,
        revision: reader.get_revision(),
    })
}

/// Writes one monotonic capacity change.
fn write_expand(mut builder: expand_volume::Builder<'_>, command: ExpandVolume) {
    write_expected(builder.reborrow().init_expected(), command.expected);
    builder.set_target_capacity_bytes(command.target_capacity.bytes());
}

/// Reads one non-zero target capacity and its compare-and-set revision.
fn read_expand(reader: expand_volume::Reader<'_>) -> Result<ExpandVolume, ProtocolError> {
    Ok(ExpandVolume {
        expected: read_expected(reader.get_expected()?)?,
        target_capacity: VolumeCapacity::new(reader.get_target_capacity_bytes())?,
    })
}

/// Writes one requested disposition transition.
fn write_set_disposition(
    mut builder: set_volume_disposition::Builder<'_>,
    command: SetVolumeDisposition,
) {
    write_expected(builder.reborrow().init_expected(), command.expected);
    builder.set_disposition(write_disposition(command.disposition));
}

/// Reads one requested disposition transition.
fn read_set_disposition(
    reader: set_volume_disposition::Reader<'_>,
) -> Result<SetVolumeDisposition, ProtocolError> {
    let disposition = match reader.get_disposition() {
        Ok(disposition) => read_disposition(disposition)?,
        Err(capnp::NotInSchema(value)) => {
            return Err(ProtocolError::UnknownEnum {
                field: "command disposition",
                value,
            });
        }
    };
    Ok(SetVolumeDisposition {
        expected: read_expected(reader.get_expected()?)?,
        disposition,
    })
}

/// Writes one writer grant after recovery grant is absent.
fn write_grant_writer(mut builder: grant_volume_writer::Builder<'_>, command: GrantVolumeWriter) {
    write_expected(builder.reborrow().init_expected(), command.expected);
    write_writer(builder.reborrow().init_writer(), command.writer);
}

/// Reads one writer grant after recovery grant is absent.
fn read_grant_writer(
    reader: grant_volume_writer::Reader<'_>,
) -> Result<GrantVolumeWriter, ProtocolError> {
    Ok(GrantVolumeWriter {
        expected: read_expected(reader.get_expected()?)?,
        writer: read_writer(reader.get_writer()?)?,
    })
}

/// Writes one exact writer revocation.
fn write_fence_writer(mut builder: fence_volume_writer::Builder<'_>, command: FenceVolumeWriter) {
    write_expected(builder.reborrow().init_expected(), command.expected);
    write_writer(builder.reborrow().init_writer(), command.writer);
}

/// Reads one exact writer revocation.
fn read_fence_writer(
    reader: fence_volume_writer::Reader<'_>,
) -> Result<FenceVolumeWriter, ProtocolError> {
    Ok(FenceVolumeWriter {
        expected: read_expected(reader.get_expected()?)?,
        writer: read_writer(reader.get_writer()?)?,
    })
}

/// Writes one recovery grant and the previous recovery identity it supersedes.
fn write_begin_recovery(
    mut builder: begin_volume_recovery::Builder<'_>,
    command: &BeginVolumeRecovery,
) {
    write_expected(builder.reborrow().init_expected(), command.expected);
    if let Some(writer) = command.expected_writer {
        write_writer(builder.reborrow().init_expected_writer(), writer);
    }
    if let Some(recovery_id) = command.replaced_recovery_id {
        builder.set_replaced_recovery_id(recovery_id.as_bytes());
    }
    write_recovery(builder.reborrow().init_recovery(), &command.recovery);
}

/// Reads one recovery authorization and optional superseded recovery identity.
fn read_begin_recovery(
    reader: begin_volume_recovery::Reader<'_>,
) -> Result<BeginVolumeRecovery, ProtocolError> {
    Ok(BeginVolumeRecovery {
        expected: read_expected(reader.get_expected()?)?,
        expected_writer: reader
            .has_expected_writer()
            .then(|| read_writer(reader.get_expected_writer()?))
            .transpose()?,
        replaced_recovery_id: read_optional_recovery_id(
            reader.get_replaced_recovery_id()?,
            "replaced recovery id",
        )?,
        recovery: read_recovery(reader.get_recovery()?)?,
    })
}

/// Writes one exact recovery-grant revocation.
fn write_revoke_recovery(
    mut builder: revoke_volume_recovery::Builder<'_>,
    command: RevokeVolumeRecovery,
) {
    write_expected(builder.reborrow().init_expected(), command.expected);
    builder.set_recovery_id(command.recovery_id.as_bytes());
}

/// Reads one exact recovery-grant revocation.
fn read_revoke_recovery(
    reader: revoke_volume_recovery::Reader<'_>,
) -> Result<RevokeVolumeRecovery, ProtocolError> {
    Ok(RevokeVolumeRecovery {
        expected: read_expected(reader.get_expected()?)?,
        recovery_id: RecoveryId::new(read_uuid(reader.get_recovery_id()?, "recovery id")?)?,
    })
}

/// Writes one replica replacement authorization.
fn write_begin_replacement(
    mut builder: begin_replica_replacement::Builder<'_>,
    command: BeginReplicaReplacement,
) {
    write_expected(builder.reborrow().init_expected(), command.expected);
    write_replacement(builder.reborrow().init_replacement(), command.replacement);
}

/// Reads one replica replacement authorization.
fn read_begin_replacement(
    reader: begin_replica_replacement::Reader<'_>,
) -> Result<BeginReplicaReplacement, ProtocolError> {
    Ok(BeginReplicaReplacement {
        expected: read_expected(reader.get_expected()?)?,
        replacement: read_replacement(reader.get_replacement()?)?,
    })
}

/// Writes one exact replacement cancellation.
fn write_cancel_replacement(
    mut builder: cancel_replica_replacement::Builder<'_>,
    command: CancelReplicaReplacement,
) {
    write_expected(builder.reborrow().init_expected(), command.expected);
    builder.set_replacement_id(command.replacement_id.as_bytes());
}

/// Reads one exact replacement cancellation.
fn read_cancel_replacement(
    reader: cancel_replica_replacement::Reader<'_>,
) -> Result<CancelReplicaReplacement, ProtocolError> {
    Ok(CancelReplicaReplacement {
        expected: read_expected(reader.get_expected()?)?,
        replacement_id: read_replacement_id(reader.get_replacement_id()?, "replacement id")?,
    })
}

/// Writes one verified replacement adoption.
fn write_adopt_replacement(
    mut builder: adopt_replica_replacement::Builder<'_>,
    command: &AdoptReplicaReplacement,
) {
    write_expected(builder.reborrow().init_expected(), command.expected);
    builder.set_replacement_id(command.replacement_id.as_bytes());
    write_nodes(
        builder
            .reborrow()
            .init_new_copies(command.new_copies.len() as u32),
        &command.new_copies,
    );
    if let Some(writer) = command.expected_writer {
        write_writer(builder.reborrow().init_expected_writer(), writer);
    }
}

/// Reads one verified replacement adoption.
fn read_adopt_replacement(
    reader: adopt_replica_replacement::Reader<'_>,
) -> Result<AdoptReplicaReplacement, ProtocolError> {
    Ok(AdoptReplicaReplacement {
        expected: read_expected(reader.get_expected()?)?,
        replacement_id: read_replacement_id(reader.get_replacement_id()?, "replacement id")?,
        new_copies: read_nodes(reader.get_new_copies()?, "adopted copy node id", 3, 3)?,
        expected_writer: reader
            .has_expected_writer()
            .then(|| read_writer(reader.get_expected_writer()?))
            .transpose()?,
    })
}

/// Writes one exact writer identity.
fn write_writer(mut builder: volume_writer_grant::Builder<'_>, writer: WriterGrant) {
    builder.set_node_id(writer.node_id.as_bytes());
    builder.set_session_id(writer.session_id.as_bytes());
}

/// Reads one exact writer identity.
fn read_writer(reader: volume_writer_grant::Reader<'_>) -> Result<WriterGrant, ProtocolError> {
    Ok(WriterGrant {
        node_id: read_node_id(reader.get_node_id()?, "writer node id")?,
        session_id: DriverSessionId::new(read_uuid(
            reader.get_session_id()?,
            "driver session id",
        )?)?,
    })
}

/// Writes one complete recovery grant.
fn write_recovery(mut builder: volume_recovery_grant::Builder<'_>, recovery: &RecoveryGrant) {
    builder.set_id(recovery.id.as_bytes());
    builder.set_coordinator_node_id(recovery.coordinator_node_id.as_bytes());
    builder.set_source_node_id(recovery.source_node_id.as_bytes());
    write_nodes(
        builder
            .reborrow()
            .init_target_node_ids(recovery.target_node_ids.len() as u32),
        &recovery.target_node_ids,
    );
}

/// Reads and bounds one complete recovery grant.
fn read_recovery(
    reader: volume_recovery_grant::Reader<'_>,
) -> Result<RecoveryGrant, ProtocolError> {
    Ok(RecoveryGrant {
        id: read_recovery_id(reader.get_id()?, "recovery id")?,
        coordinator_node_id: read_node_id(
            reader.get_coordinator_node_id()?,
            "recovery coordinator node id",
        )?,
        source_node_id: read_node_id(reader.get_source_node_id()?, "recovery source node id")?,
        target_node_ids: read_nodes(
            reader.get_target_node_ids()?,
            "recovery target node id",
            2,
            3,
        )?,
    })
}

/// Writes one complete replacement grant.
fn write_replacement(
    mut builder: volume_replacement_grant::Builder<'_>,
    replacement: ReplacementGrant,
) {
    builder.set_id(replacement.id.as_bytes());
    builder.set_coordinator_node_id(replacement.coordinator_node_id.as_bytes());
    if let Some(old_node_id) = replacement.old_node_id {
        builder.set_old_node_id(old_node_id.as_bytes());
    }
    builder.set_new_node_id(replacement.new_node_id.as_bytes());
    builder.set_source_node_id(replacement.source_node_id.as_bytes());
}

/// Reads one complete replacement grant.
fn read_replacement(
    reader: volume_replacement_grant::Reader<'_>,
) -> Result<ReplacementGrant, ProtocolError> {
    let old_node_id = if reader.get_old_node_id()?.is_empty() {
        None
    } else {
        Some(read_node_id(
            reader.get_old_node_id()?,
            "old replica node id",
        )?)
    };
    Ok(ReplacementGrant {
        id: read_replacement_id(reader.get_id()?, "replacement id")?,
        coordinator_node_id: read_node_id(
            reader.get_coordinator_node_id()?,
            "replacement coordinator node id",
        )?,
        old_node_id,
        new_node_id: read_node_id(reader.get_new_node_id()?, "new replica node id")?,
        source_node_id: read_node_id(reader.get_source_node_id()?, "replacement source node id")?,
    })
}

/// Writes current data control state into a bounded snapshot.
fn write_data_control_state(
    mut builder: volume_data_control_state::Builder<'_>,
    data: &DataControlState,
) {
    builder.set_fence(data.fence.get());
    write_nodes(
        builder.reborrow().init_copies(data.copies.len() as u32),
        &data.copies,
    );
    if let Some(writer) = data.writer {
        write_writer(builder.reborrow().init_writer(), writer);
    }
    if let Some(recovery) = data.recovery.as_ref() {
        write_recovery(builder.reborrow().init_recovery(), recovery);
    }
}

/// Reads and validates current data control state from a bounded snapshot.
fn read_data_control_state(
    reader: volume_data_control_state::Reader<'_>,
) -> Result<DataControlState, ProtocolError> {
    Ok(DataControlState {
        fence: FenceEpoch::new(reader.get_fence())?,
        copies: read_nodes(reader.get_copies()?, "active copy node id", 2, 3)?,
        writer: reader
            .has_writer()
            .then(|| read_writer(reader.get_writer()?))
            .transpose()?,
        recovery: reader
            .has_recovery()
            .then(|| read_recovery(reader.get_recovery()?))
            .transpose()?,
    })
}

/// Writes applied or current revision and optional pre-initialization fence.
fn write_result(
    mut builder: mantissa_protocol::volumes::volume_control_result::Builder<'_>,
    revision: u64,
    fence: Option<FenceEpoch>,
) {
    builder.set_revision(revision);
    builder.set_fence(fence.map_or(0, FenceEpoch::get));
}

/// Reads applied or current revision and optional pre-initialization fence.
fn read_result(
    reader: mantissa_protocol::volumes::volume_control_result::Reader<'_>,
) -> Result<(u64, Option<FenceEpoch>), ProtocolError> {
    let revision = reader.get_revision();
    let fence = match reader.get_fence() {
        0 => None,
        value => Some(FenceEpoch::new(value)?),
    };
    if (revision == 0) != fence.is_none() {
        return Err(ProtocolError::InvalidState(
            "volume command response revision and fence presence differ",
        ));
    }
    Ok((revision, fence))
}

/// Maps one owned disposition to its wire enum.
const fn write_disposition(disposition: VolumeDisposition) -> WireDisposition {
    match disposition {
        VolumeDisposition::Live => WireDisposition::Live,
        VolumeDisposition::Retained => WireDisposition::Retained,
    }
}

/// Maps one checked wire disposition to its owned form.
fn read_disposition(disposition: WireDisposition) -> Result<VolumeDisposition, ProtocolError> {
    match disposition {
        WireDisposition::Invalid => Err(ProtocolError::UnknownEnum {
            field: "volume disposition",
            value: 0,
        }),
        WireDisposition::Live => Ok(VolumeDisposition::Live),
        WireDisposition::Retained => Ok(VolumeDisposition::Retained),
    }
}

/// Maps one owned command rejection to its stable wire enum.
const fn write_rejection(rejection: VolumeCommandRejection) -> WireRejection {
    match rejection {
        VolumeCommandRejection::NotInitialized => WireRejection::NotInitialized,
        VolumeCommandRejection::AlreadyInitialized => WireRejection::AlreadyInitialized,
        VolumeCommandRejection::WrongGeneration => WireRejection::WrongGeneration,
        VolumeCommandRejection::VolumeNotLive => WireRejection::VolumeNotLive,
        VolumeCommandRejection::WriterOutsideCopySet => WireRejection::WriterOutsideCopySet,
        VolumeCommandRejection::WrongWriter => WireRejection::WrongWriter,
        VolumeCommandRejection::InvalidCopySet => WireRejection::InvalidCopySet,
        VolumeCommandRejection::InvalidRecovery => WireRejection::InvalidRecovery,
        VolumeCommandRejection::RecoveryInProgress => WireRejection::RecoveryInProgress,
        VolumeCommandRejection::NoRecovery => WireRejection::NoRecovery,
        VolumeCommandRejection::WrongRecovery => WireRejection::WrongRecovery,
        VolumeCommandRejection::RecoveryIdConflict => WireRejection::RecoveryIdConflict,
        VolumeCommandRejection::InvalidReplacement => WireRejection::InvalidReplacement,
        VolumeCommandRejection::ReplacementInProgress => WireRejection::ReplacementInProgress,
        VolumeCommandRejection::NoReplacement => WireRejection::NoReplacement,
        VolumeCommandRejection::WrongReplacement => WireRejection::WrongReplacement,
        VolumeCommandRejection::ReplacementIdConflict => WireRejection::ReplacementIdConflict,
        VolumeCommandRejection::RevisionExhausted => WireRejection::RevisionExhausted,
        VolumeCommandRejection::FenceExhausted => WireRejection::FenceExhausted,
        VolumeCommandRejection::CapacityCannotShrink => WireRejection::CapacityCannotShrink,
        VolumeCommandRejection::CapacityNotAligned => WireRejection::CapacityNotAligned,
    }
}

/// Maps one checked wire rejection to its owned form.
fn read_rejection(rejection: WireRejection) -> Result<VolumeCommandRejection, ProtocolError> {
    match rejection {
        WireRejection::Invalid => Err(ProtocolError::UnknownEnum {
            field: "command rejection",
            value: 0,
        }),
        WireRejection::NotInitialized => Ok(VolumeCommandRejection::NotInitialized),
        WireRejection::AlreadyInitialized => Ok(VolumeCommandRejection::AlreadyInitialized),
        WireRejection::WrongGeneration => Ok(VolumeCommandRejection::WrongGeneration),
        WireRejection::VolumeNotLive => Ok(VolumeCommandRejection::VolumeNotLive),
        WireRejection::WriterOutsideCopySet => Ok(VolumeCommandRejection::WriterOutsideCopySet),
        WireRejection::WrongWriter => Ok(VolumeCommandRejection::WrongWriter),
        WireRejection::InvalidCopySet => Ok(VolumeCommandRejection::InvalidCopySet),
        WireRejection::InvalidRecovery => Ok(VolumeCommandRejection::InvalidRecovery),
        WireRejection::RecoveryInProgress => Ok(VolumeCommandRejection::RecoveryInProgress),
        WireRejection::NoRecovery => Ok(VolumeCommandRejection::NoRecovery),
        WireRejection::WrongRecovery => Ok(VolumeCommandRejection::WrongRecovery),
        WireRejection::RecoveryIdConflict => Ok(VolumeCommandRejection::RecoveryIdConflict),
        WireRejection::InvalidReplacement => Ok(VolumeCommandRejection::InvalidReplacement),
        WireRejection::ReplacementInProgress => Ok(VolumeCommandRejection::ReplacementInProgress),
        WireRejection::NoReplacement => Ok(VolumeCommandRejection::NoReplacement),
        WireRejection::WrongReplacement => Ok(VolumeCommandRejection::WrongReplacement),
        WireRejection::ReplacementIdConflict => Ok(VolumeCommandRejection::ReplacementIdConflict),
        WireRejection::RevisionExhausted => Ok(VolumeCommandRejection::RevisionExhausted),
        WireRejection::FenceExhausted => Ok(VolumeCommandRejection::FenceExhausted),
        WireRejection::CapacityCannotShrink => Ok(VolumeCommandRejection::CapacityCannotShrink),
        WireRejection::CapacityNotAligned => Ok(VolumeCommandRejection::CapacityNotAligned),
    }
}

/// Writes one sorted set of node UUIDs.
fn write_nodes(mut builder: capnp::data_list::Builder<'_>, nodes: &BTreeSet<VolumeNodeId>) {
    for (index, node_id) in nodes.iter().enumerate() {
        builder.set(index as u32, node_id.as_bytes());
    }
}

/// Reads one explicitly bounded set and rejects duplicate node UUIDs.
fn read_nodes(
    reader: capnp::data_list::Reader<'_>,
    field: &'static str,
    minimum: usize,
    maximum: usize,
) -> Result<BTreeSet<VolumeNodeId>, ProtocolError> {
    let actual = usize::try_from(reader.len())
        .map_err(|_| ProtocolError::InvalidState("node set length does not fit usize"))?;
    if !(minimum..=maximum).contains(&actual) {
        return Err(ProtocolError::InvalidState(
            "control-state node set has an invalid count",
        ));
    }
    let mut nodes = BTreeSet::new();
    for encoded in reader.iter() {
        let node_id = read_node_id(encoded?, field)?;
        if !nodes.insert(node_id) {
            return Err(ProtocolError::InvalidState(
                "control-state node set contains a duplicate",
            ));
        }
    }
    Ok(nodes)
}

/// Reads one non-zero node identity.
fn read_node_id(bytes: &[u8], field: &'static str) -> Result<VolumeNodeId, ProtocolError> {
    Ok(VolumeNodeId::new(read_uuid(bytes, field)?)?)
}

/// Reads one required recovery identity.
fn read_recovery_id(bytes: &[u8], field: &'static str) -> Result<RecoveryId, ProtocolError> {
    Ok(RecoveryId::new(read_uuid(bytes, field)?)?)
}

/// Reads one optional recovery identity encoded as empty or exact UUID bytes.
fn read_optional_recovery_id(
    bytes: &[u8],
    field: &'static str,
) -> Result<Option<RecoveryId>, ProtocolError> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        Ok(Some(read_recovery_id(bytes, field)?))
    }
}

/// Reads one required replacement identity.
fn read_replacement_id(bytes: &[u8], field: &'static str) -> Result<ReplacementId, ProtocolError> {
    Ok(ReplacementId::new(read_uuid(bytes, field)?)?)
}
