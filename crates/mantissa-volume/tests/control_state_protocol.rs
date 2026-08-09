use std::collections::BTreeSet;

use capnp::message::{Builder, ReaderOptions};
use mantissa_protocol::volumes::{volume_control_command, volume_control_snapshot};
use mantissa_raft::protocol::ApplicationCommandAdapter;
use mantissa_volume::control_state::{
    AdoptReplicaReplacement, BeginReplicaReplacement, BeginVolumeRecovery,
    CancelReplicaReplacement, ExpectedVolumeRevision, FenceVolumeWriter, GrantVolumeWriter,
    InitializeVolume, RecoveryGrant, ReplacementGrant, RevokeVolumeRecovery, SetVolumeDisposition,
    VolumeCommand, VolumeCommandRejection, VolumeCommandResponse, VolumeControlState,
    VolumeDisposition, WriterGrant,
};
use mantissa_volume::protocol::{
    ProtocolError, VolumeCommandAdapter, decode_control_state, encode_control_state,
    read_volume_command, read_volume_command_response, write_descriptor, write_volume_command,
    write_volume_command_response,
};
use mantissa_volume::{
    DriverSessionId, FenceEpoch, RecoveryId, ReplacementId, VolumeBlockSizes, VolumeDescriptor,
    VolumeGeneration, VolumeId, VolumeNodeId,
};
use uuid::Uuid;

const MAXIMUM_STATE_BYTES: usize = 64 * 1024;

/// Returns one deterministic node identity.
fn node(value: u128) -> VolumeNodeId {
    VolumeNodeId::new(Uuid::from_u128(value)).expect("test node ID must be valid")
}

/// Returns the generation shared by protocol examples.
fn generation() -> VolumeGeneration {
    VolumeGeneration::new(9).expect("test generation must be valid")
}

/// Returns one deterministic descriptor with supported block sizes.
fn descriptor() -> VolumeDescriptor {
    VolumeDescriptor::new(
        VolumeId::new(Uuid::from_u128(10)).expect("test volume ID must be valid"),
        generation(),
        64 << 20,
        VolumeBlockSizes::supported(),
    )
    .expect("test descriptor must be valid")
}

/// Returns the exact initial three-copy set.
fn copies() -> BTreeSet<VolumeNodeId> {
    [node(1), node(2), node(3)].into_iter().collect()
}

/// Returns one deterministic driver session identity.
fn session(value: u128) -> DriverSessionId {
    DriverSessionId::new(Uuid::from_u128(100 + value)).expect("test session ID must be valid")
}

/// Returns one deterministic recovery identity.
fn recovery_id(value: u128) -> RecoveryId {
    RecoveryId::new(Uuid::from_u128(200 + value)).expect("test recovery ID must be valid")
}

/// Returns one deterministic replacement identity.
fn replacement_id(value: u128) -> ReplacementId {
    ReplacementId::new(Uuid::from_u128(300 + value)).expect("test replacement ID must be valid")
}

/// Encodes and decodes one command through the direct control state schema.
fn round_trip_command(command: &VolumeCommand) -> VolumeCommand {
    let mut message = Builder::new_default();
    write_volume_command(
        message.init_root::<volume_control_command::Builder<'_>>(),
        command,
    );
    let bytes = capnp::serialize::write_message_to_words(&message);
    let mut remaining = bytes.as_slice();
    let message =
        capnp::serialize::read_message_from_flat_slice(&mut remaining, ReaderOptions::new())
            .expect("volume command must decode as Cap'n Proto");
    assert!(remaining.is_empty());
    read_volume_command(
        message
            .get_root::<volume_control_command::Reader<'_>>()
            .expect("volume command root must decode"),
    )
    .expect("volume command fields must validate")
}

/// Encodes and decodes one response through the direct control state schema.
fn round_trip_response(response: VolumeCommandResponse) -> VolumeCommandResponse {
    let mut message = Builder::new_default();
    write_volume_command_response(
        message
            .init_root::<mantissa_protocol::volumes::volume_control_command_response::Builder<'_>>(
            ),
        response,
    );
    let bytes = capnp::serialize::write_message_to_words(&message);
    let mut remaining = bytes.as_slice();
    let message =
        capnp::serialize::read_message_from_flat_slice(&mut remaining, ReaderOptions::new())
            .expect("volume command response must decode as Cap'n Proto");
    assert!(remaining.is_empty());
    read_volume_command_response(
        message
            .get_root::<mantissa_protocol::volumes::volume_control_command_response::Reader<'_>>()
            .expect("volume command response root must decode"),
    )
    .expect("volume command response fields must validate")
}

/// Returns compare-and-set fields with a selected revision.
fn expected(revision: u64) -> ExpectedVolumeRevision {
    ExpectedVolumeRevision {
        generation: generation(),
        revision,
    }
}

#[test]
fn every_semantic_command_round_trips_without_history() {
    let writer = WriterGrant {
        node_id: node(1),
        session_id: session(1),
    };
    let recovery = RecoveryGrant {
        id: recovery_id(1),
        coordinator_node_id: node(2),
        source_node_id: node(1),
        target_node_ids: [node(1), node(2)].into_iter().collect(),
    };
    let replacement = ReplacementGrant {
        id: replacement_id(1),
        coordinator_node_id: node(1),
        old_node_id: Some(node(3)),
        new_node_id: node(4),
        source_node_id: node(1),
    };
    let commands = [
        VolumeCommand::Initialize(InitializeVolume {
            descriptor: descriptor(),
            initial_copies: copies(),
        }),
        VolumeCommand::SetDisposition(SetVolumeDisposition {
            expected: expected(1),
            disposition: VolumeDisposition::Retained,
        }),
        VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: expected(2),
            writer,
        }),
        VolumeCommand::FenceWriter(FenceVolumeWriter {
            expected: expected(3),
            writer,
        }),
        VolumeCommand::BeginRecovery(BeginVolumeRecovery {
            expected: expected(4),
            expected_writer: Some(writer),
            replaced_recovery_id: Some(recovery_id(2)),
            recovery: recovery.clone(),
        }),
        VolumeCommand::RevokeRecovery(RevokeVolumeRecovery {
            expected: expected(5),
            recovery_id: recovery.id,
        }),
        VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: expected(6),
            replacement,
        }),
        VolumeCommand::CancelReplacement(CancelReplicaReplacement {
            expected: expected(7),
            replacement_id: replacement.id,
        }),
        VolumeCommand::AdoptReplacement(AdoptReplicaReplacement {
            expected: expected(8),
            replacement_id: replacement.id,
            new_copies: [node(1), node(2), node(4)].into_iter().collect(),
            expected_writer: Some(writer),
        }),
    ];
    for command in commands {
        assert_eq!(round_trip_command(&command), command);
    }
}

#[test]
fn volume_command_uses_the_generic_raft_log_boundary() {
    let command = VolumeCommand::Initialize(InitializeVolume {
        descriptor: descriptor(),
        initial_copies: copies(),
    });
    let adapter = VolumeCommandAdapter;
    let mut message = Builder::new_default();
    adapter
        .write(
            message.init_root::<mantissa_protocol::raft::raft_application_command::Builder<'_>>(),
            &command,
        )
        .expect("volume command must encode through Raft");
    let bytes = capnp::serialize::write_message_to_words(&message);
    let mut remaining = bytes.as_slice();
    let message =
        capnp::serialize::read_message_from_flat_slice(&mut remaining, ReaderOptions::new())
            .expect("Raft command must decode as Cap'n Proto");
    let decoded = adapter
        .read(
            message
                .get_root::<mantissa_protocol::raft::raft_application_command::Reader<'_>>()
                .expect("Raft command root must decode"),
        )
        .expect("volume command must validate through Raft");
    assert_eq!(decoded, command);
    assert!(remaining.is_empty());
}

#[test]
fn every_response_and_rejection_round_trips() {
    let fence = FenceEpoch::new(11).expect("test fence must be valid");
    let responses = [
        VolumeCommandResponse::Applied {
            revision: 7,
            fence: Some(fence),
        },
        VolumeCommandResponse::Current {
            revision: 7,
            fence: Some(fence),
        },
        VolumeCommandResponse::Current {
            revision: 0,
            fence: None,
        },
        VolumeCommandResponse::Conflict {
            current_revision: 8,
        },
    ];
    for response in responses {
        assert_eq!(round_trip_response(response), response);
    }

    let rejections = [
        VolumeCommandRejection::NotInitialized,
        VolumeCommandRejection::AlreadyInitialized,
        VolumeCommandRejection::WrongGeneration,
        VolumeCommandRejection::VolumeNotLive,
        VolumeCommandRejection::WriterOutsideCopySet,
        VolumeCommandRejection::WrongWriter,
        VolumeCommandRejection::InvalidCopySet,
        VolumeCommandRejection::InvalidRecovery,
        VolumeCommandRejection::RecoveryInProgress,
        VolumeCommandRejection::NoRecovery,
        VolumeCommandRejection::WrongRecovery,
        VolumeCommandRejection::RecoveryIdConflict,
        VolumeCommandRejection::InvalidReplacement,
        VolumeCommandRejection::ReplacementInProgress,
        VolumeCommandRejection::NoReplacement,
        VolumeCommandRejection::WrongReplacement,
        VolumeCommandRejection::ReplacementIdConflict,
        VolumeCommandRejection::RevisionExhausted,
        VolumeCommandRejection::FenceExhausted,
    ];
    for rejection in rejections {
        let response = VolumeCommandResponse::Rejected(rejection);
        assert_eq!(round_trip_response(response), response);
    }
}

#[test]
fn snapshots_round_trip_pristine_and_operational_control_state() {
    let pristine = VolumeControlState::default();
    let pristine_bytes = encode_control_state(&pristine, MAXIMUM_STATE_BYTES)
        .expect("pristine control state must encode");
    assert_eq!(
        decode_control_state(&pristine_bytes, MAXIMUM_STATE_BYTES)
            .expect("pristine control state must decode"),
        pristine
    );

    let initialized = pristine
        .evaluate(&VolumeCommand::Initialize(InitializeVolume {
            descriptor: descriptor(),
            initial_copies: copies(),
        }))
        .state;
    let writer = WriterGrant {
        node_id: node(1),
        session_id: session(1),
    };
    let attached = initialized
        .evaluate(&VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: expected(initialized.revision()),
            writer,
        }))
        .state;
    let replacement = ReplacementGrant {
        id: replacement_id(1),
        coordinator_node_id: node(1),
        old_node_id: Some(node(3)),
        new_node_id: node(4),
        source_node_id: node(1),
    };
    let replacing = attached
        .evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: expected(attached.revision()),
            replacement,
        }))
        .state;
    let bytes = encode_control_state(&replacing, MAXIMUM_STATE_BYTES)
        .expect("operational control state must encode");
    assert_eq!(
        decode_control_state(&bytes, MAXIMUM_STATE_BYTES)
            .expect("operational control state must decode"),
        replacing
    );
}

#[test]
fn snapshot_decoder_rejects_unsupported_format_and_invalid_relationships() {
    let mut unsupported = Builder::new_default();
    unsupported
        .init_root::<volume_control_snapshot::Builder<'_>>()
        .set_format_version(2);
    let bytes = capnp::serialize::write_message_to_words(&unsupported);
    assert!(matches!(
        decode_control_state(&bytes, MAXIMUM_STATE_BYTES),
        Err(ProtocolError::UnsupportedStateFormat(2))
    ));

    let mut invalid = Builder::new_default();
    let mut root = invalid.init_root::<volume_control_snapshot::Builder<'_>>();
    root.set_format_version(1);
    root.set_revision(1);
    root.set_disposition(mantissa_protocol::volumes::VolumeDisposition::Live);
    write_descriptor(root.reborrow().init_descriptor(), &descriptor());
    let mut data = root.reborrow().init_data();
    data.set_fence(1);
    let mut stored_copies = data.reborrow().init_copies(2);
    stored_copies.set(0, node(1).as_bytes());
    stored_copies.set(1, node(2).as_bytes());
    let mut writer = data.reborrow().init_writer();
    writer.set_node_id(node(3).as_bytes());
    writer.set_session_id(session(1).as_bytes());
    let bytes = capnp::serialize::write_message_to_words(&invalid);
    assert!(matches!(
        decode_control_state(&bytes, MAXIMUM_STATE_BYTES),
        Err(ProtocolError::VolumeStateInvariant(_))
    ));
}

#[test]
fn command_decoder_rejects_duplicate_copy_nodes() {
    let mut message = Builder::new_default();
    let mut command = message
        .init_root::<volume_control_command::Builder<'_>>()
        .init_initialize();
    write_descriptor(command.reborrow().init_descriptor(), &descriptor());
    let mut stored = command.reborrow().init_initial_copies(3);
    stored.set(0, node(1).as_bytes());
    stored.set(1, node(1).as_bytes());
    stored.set(2, node(2).as_bytes());
    let bytes = capnp::serialize::write_message_to_words(&message);
    let mut remaining = bytes.as_slice();
    let message =
        capnp::serialize::read_message_from_flat_slice(&mut remaining, ReaderOptions::new())
            .expect("malformed semantic command is still valid Cap'n Proto");
    let error = read_volume_command(
        message
            .get_root::<volume_control_command::Reader<'_>>()
            .expect("command root must decode"),
    )
    .expect_err("duplicate copy nodes must fail closed");
    assert!(matches!(error, ProtocolError::InvalidState(_)));
}
