use std::collections::BTreeSet;

use uuid::Uuid;

use super::{
    AdoptReplicaReplacement, BeginReplicaReplacement, BeginVolumeRecovery,
    CancelReplicaReplacement, ExpandVolume, ExpectedVolumeRevision, FenceVolumeWriter,
    GrantVolumeWriter, InitializeVolume, RecoveryGrant, ReplacementGrant, RevokeVolumeRecovery,
    SetVolumeDisposition, VolumeCommand, VolumeCommandRejection, VolumeCommandResponse,
    VolumeControlState, VolumeDisposition, WriterGrant,
};
use crate::{
    DriverSessionId, FenceEpoch, RecoveryId, ReplacementId, VolumeBlockSizes, VolumeCapacity,
    VolumeDescriptor, VolumeGeneration, VolumeId, VolumeNodeId,
};

/// Returns one deterministic node identity.
fn node(value: u128) -> VolumeNodeId {
    VolumeNodeId::new(Uuid::from_u128(value)).expect("test node ID must be valid")
}

/// Returns the generation shared by this control state test suite.
fn generation() -> VolumeGeneration {
    VolumeGeneration::new(7).expect("test generation must be valid")
}

/// Returns one valid fixed volume descriptor.
fn descriptor() -> VolumeDescriptor {
    VolumeDescriptor::new(
        VolumeId::new(Uuid::from_u128(10)).expect("test volume ID must be valid"),
        generation(),
        64 << 20,
        VolumeBlockSizes::supported(),
    )
    .expect("test descriptor must be valid")
}

/// Returns the initial three-copy control state set.
fn initial_copies() -> BTreeSet<VolumeNodeId> {
    [node(1), node(2), node(3)].into_iter().collect()
}

/// Returns one deterministic driver session identity.
fn session(value: u128) -> DriverSessionId {
    DriverSessionId::new(Uuid::from_u128(100 + value)).expect("test driver session must be valid")
}

/// Returns one deterministic recovery identity.
fn recovery_id(value: u128) -> RecoveryId {
    RecoveryId::new(Uuid::from_u128(200 + value)).expect("test recovery ID must be valid")
}

/// Returns one deterministic replacement identity.
fn replacement_id(value: u128) -> ReplacementId {
    ReplacementId::new(Uuid::from_u128(300 + value)).expect("test replacement ID must be valid")
}

/// Initializes a state and verifies that the transition itself is valid.
fn initialized() -> VolumeControlState {
    let plan =
        VolumeControlState::default().evaluate(&VolumeCommand::Initialize(InitializeVolume {
            descriptor: descriptor(),
            initial_copies: initial_copies(),
        }));
    assert!(matches!(
        plan.response,
        VolumeCommandResponse::Applied {
            revision: 1,
            fence: Some(fence),
        } if fence == FenceEpoch::initial()
    ));
    plan.state
        .validate()
        .expect("initialized state must be valid");
    plan.state
}

/// Returns compare-and-set fields for the current test state.
fn expected(state: &VolumeControlState) -> ExpectedVolumeRevision {
    ExpectedVolumeRevision {
        generation: generation(),
        revision: state.revision(),
    }
}

/// Returns the current fence from initialized control state.
fn fence(state: &VolumeControlState) -> FenceEpoch {
    state
        .data()
        .expect("initialized state must have data")
        .fence
}

/// Applies one command and requires an actual valid state transition.
fn apply(state: &VolumeControlState, command: VolumeCommand) -> VolumeControlState {
    let plan = state.evaluate(&command);
    assert!(
        matches!(plan.response, VolumeCommandResponse::Applied { .. }),
        "command did not apply: {:?}",
        plan.response
    );
    plan.state.validate().expect("applied state must be valid");
    plan.state
}

/// Small deterministic generator used to compose control-state transitions.
struct SequenceRng(u64);

impl SequenceRng {
    /// Advances the generator without adding a property-test dependency.
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    /// Selects one deterministic index from a non-empty collection.
    fn index(&mut self, length: usize) -> usize {
        assert!(length > 0, "sequence selection requires a candidate");
        (self.next() as usize) % length
    }
}

/// Selects one active copy using stable set iteration order.
fn selected_copy(copies: &BTreeSet<VolumeNodeId>, rng: &mut SequenceRng) -> VolumeNodeId {
    *copies
        .iter()
        .nth(rng.index(copies.len()))
        .expect("validated control state must contain an active copy")
}

/// Selects one node that is not part of the current copy set.
fn unused_copy(copies: &BTreeSet<VolumeNodeId>, start: u128) -> VolumeNodeId {
    (0..32_u128)
        .map(|offset| node(10 + ((start + offset) % 32)))
        .find(|candidate| !copies.contains(candidate))
        .expect("the finite test copy set cannot exhaust candidate nodes")
}

/// Builds a valid recovery grant from the current active copies.
fn generated_recovery(
    state: &VolumeControlState,
    rng: &mut SequenceRng,
    serial: u128,
) -> RecoveryGrant {
    let copies = &state
        .data()
        .expect("sequence control state must be initialized")
        .copies;
    let source_node_id = selected_copy(copies, rng);
    let mut target_node_ids = copies.clone();
    if target_node_ids.len() == 3 && rng.next().is_multiple_of(2) {
        let removable = target_node_ids
            .iter()
            .copied()
            .find(|candidate| *candidate != source_node_id)
            .expect("three copies must contain a non-source target");
        target_node_ids.remove(&removable);
    }
    RecoveryGrant {
        id: recovery_id(10_000 + serial),
        coordinator_node_id: source_node_id,
        source_node_id,
        target_node_ids,
    }
}

/// Builds the only valid replacement shape for the current copy count.
fn generated_replacement(
    state: &VolumeControlState,
    rng: &mut SequenceRng,
    serial: u128,
) -> ReplacementGrant {
    let copies = &state
        .data()
        .expect("sequence control state must be initialized")
        .copies;
    let source_node_id = state
        .data()
        .and_then(|data| data.writer)
        .map_or_else(|| selected_copy(copies, rng), |writer| writer.node_id);
    let old_node_id = if copies.len() == 3 {
        Some(
            copies
                .iter()
                .copied()
                .find(|candidate| *candidate != source_node_id)
                .expect("three copies must contain a replaceable non-source copy"),
        )
    } else {
        None
    };
    ReplacementGrant {
        id: replacement_id(20_000 + serial),
        coordinator_node_id: source_node_id,
        old_node_id,
        new_node_id: unused_copy(copies, serial),
        source_node_id,
    }
}

/// Calculates the exact copy set authorized by one replacement grant.
fn replacement_result(
    state: &VolumeControlState,
    replacement: ReplacementGrant,
) -> BTreeSet<VolumeNodeId> {
    let mut copies = state
        .data()
        .expect("sequence control state must be initialized")
        .copies
        .clone();
    if let Some(old_node_id) = replacement.old_node_id {
        copies.remove(&old_node_id);
    }
    copies.insert(replacement.new_node_id);
    copies
}

/// Generates a command appropriate for the current control state shape.
fn generated_command(
    state: &VolumeControlState,
    rng: &mut SequenceRng,
    serial: u128,
) -> VolumeCommand {
    let current = expected(state);
    let data = state
        .data()
        .expect("sequence control state must be initialized");

    if state.disposition() == VolumeDisposition::Retained {
        return match rng.index(4) {
            0 => VolumeCommand::SetDisposition(SetVolumeDisposition {
                expected: current,
                disposition: VolumeDisposition::Live,
            }),
            1 => VolumeCommand::SetDisposition(SetVolumeDisposition {
                expected: current,
                disposition: VolumeDisposition::Retained,
            }),
            2 => VolumeCommand::GrantWriter(GrantVolumeWriter {
                expected: current,
                writer: WriterGrant {
                    node_id: selected_copy(&data.copies, rng),
                    session_id: session(30_000 + serial),
                },
            }),
            _ => VolumeCommand::Initialize(InitializeVolume {
                descriptor: descriptor(),
                initial_copies: initial_copies(),
            }),
        };
    }

    if let Some(replacement) = state.replacement() {
        return match rng.index(6) {
            0 => VolumeCommand::AdoptReplacement(AdoptReplicaReplacement {
                expected: current,
                replacement_id: replacement.id,
                new_copies: replacement_result(state, replacement),
                expected_writer: data.writer,
            }),
            1 => VolumeCommand::CancelReplacement(CancelReplicaReplacement {
                expected: current,
                replacement_id: replacement.id,
            }),
            2 => VolumeCommand::BeginReplacement(BeginReplicaReplacement {
                expected: current,
                replacement,
            }),
            3 => VolumeCommand::SetDisposition(SetVolumeDisposition {
                expected: current,
                disposition: VolumeDisposition::Retained,
            }),
            4 => VolumeCommand::CancelReplacement(CancelReplicaReplacement {
                expected: ExpectedVolumeRevision {
                    revision: current.revision.saturating_sub(1),
                    ..current
                },
                replacement_id: replacement.id,
            }),
            _ => VolumeCommand::BeginRecovery(BeginVolumeRecovery {
                expected: current,
                expected_writer: data.writer,
                replaced_recovery_id: None,
                recovery: generated_recovery(state, rng, serial),
            }),
        };
    }

    if let Some(recovery) = data.recovery.as_ref() {
        return match rng.index(7) {
            0 => VolumeCommand::RevokeRecovery(RevokeVolumeRecovery {
                expected: current,
                recovery_id: recovery.id,
            }),
            1 => VolumeCommand::GrantWriter(GrantVolumeWriter {
                expected: current,
                writer: WriterGrant {
                    node_id: recovery.source_node_id,
                    session_id: session(40_000 + serial),
                },
            }),
            2 => VolumeCommand::BeginRecovery(BeginVolumeRecovery {
                expected: current,
                expected_writer: None,
                replaced_recovery_id: Some(recovery.id),
                recovery: generated_recovery(state, rng, serial),
            }),
            3 => VolumeCommand::BeginRecovery(BeginVolumeRecovery {
                expected: current,
                expected_writer: None,
                replaced_recovery_id: Some(recovery.id),
                recovery: recovery.clone(),
            }),
            4 => VolumeCommand::SetDisposition(SetVolumeDisposition {
                expected: current,
                disposition: VolumeDisposition::Retained,
            }),
            5 => VolumeCommand::RevokeRecovery(RevokeVolumeRecovery {
                expected: current,
                recovery_id: recovery_id(60_000 + serial),
            }),
            _ => VolumeCommand::BeginReplacement(BeginReplicaReplacement {
                expected: current,
                replacement: generated_replacement(state, rng, serial),
            }),
        };
    }

    match rng.index(12) {
        0 => VolumeCommand::SetDisposition(SetVolumeDisposition {
            expected: current,
            disposition: VolumeDisposition::Live,
        }),
        1 => VolumeCommand::SetDisposition(SetVolumeDisposition {
            expected: current,
            disposition: VolumeDisposition::Retained,
        }),
        2 => VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: current,
            writer: WriterGrant {
                node_id: selected_copy(&data.copies, rng),
                session_id: session(70_000 + serial),
            },
        }),
        3 => VolumeCommand::FenceWriter(FenceVolumeWriter {
            expected: current,
            writer: data.writer.unwrap_or(WriterGrant {
                node_id: selected_copy(&data.copies, rng),
                session_id: session(80_000 + serial),
            }),
        }),
        4 | 5 => VolumeCommand::BeginRecovery(BeginVolumeRecovery {
            expected: current,
            expected_writer: data.writer,
            replaced_recovery_id: None,
            recovery: generated_recovery(state, rng, serial),
        }),
        6 => VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: current,
            replacement: generated_replacement(state, rng, serial),
        }),
        7 => VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: current,
            writer: WriterGrant {
                node_id: unused_copy(&data.copies, serial),
                session_id: session(90_000 + serial),
            },
        }),
        8 => VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: ExpectedVolumeRevision {
                revision: current.revision.saturating_sub(1),
                ..current
            },
            writer: WriterGrant {
                node_id: selected_copy(&data.copies, rng),
                session_id: session(100_000 + serial),
            },
        }),
        9 => VolumeCommand::Initialize(InitializeVolume {
            descriptor: descriptor(),
            initial_copies: initial_copies(),
        }),
        10 => VolumeCommand::Expand(ExpandVolume {
            expected: current,
            target_capacity: VolumeCapacity::new(
                state
                    .descriptor()
                    .expect("generated initialized state must have a descriptor")
                    .capacity()
                    .bytes()
                    .checked_mul(2)
                    .unwrap_or_else(|| {
                        state
                            .descriptor()
                            .expect("generated initialized state must have a descriptor")
                            .capacity()
                            .bytes()
                    }),
            )
            .expect("generated expansion capacity must remain non-zero"),
        }),
        _ => VolumeCommand::FenceWriter(FenceVolumeWriter {
            expected: ExpectedVolumeRevision {
                revision: current.revision.saturating_sub(1),
                ..current
            },
            writer: data.writer.unwrap_or(WriterGrant {
                node_id: selected_copy(&data.copies, rng),
                session_id: session(110_000 + serial),
            }),
        }),
    }
}

/// Maps each semantic command to a stable coverage slot.
const fn command_slot(command: &VolumeCommand) -> usize {
    match command {
        VolumeCommand::Initialize(_) => 0,
        VolumeCommand::SetDisposition(_) => 1,
        VolumeCommand::GrantWriter(_) => 2,
        VolumeCommand::FenceWriter(_) => 3,
        VolumeCommand::BeginRecovery(_) => 4,
        VolumeCommand::RevokeRecovery(_) => 5,
        VolumeCommand::BeginReplacement(_) => 6,
        VolumeCommand::CancelReplacement(_) => 7,
        VolumeCommand::AdoptReplacement(_) => 8,
        VolumeCommand::Expand(_) => 9,
    }
}

#[test]
fn exact_initialize_retry_is_current_and_conflicts_are_rejected() {
    let state = initialized();
    let exact = VolumeCommand::Initialize(InitializeVolume {
        descriptor: descriptor(),
        initial_copies: initial_copies(),
    });
    let retried = state.evaluate(&exact);
    assert_eq!(retried.state, state);
    assert!(matches!(
        retried.response,
        VolumeCommandResponse::Current {
            revision: 1,
            fence: Some(value),
        } if value == FenceEpoch::initial()
    ));

    let mut different_copies = initial_copies();
    different_copies.remove(&node(3));
    different_copies.insert(node(4));
    let conflict = state.evaluate(&VolumeCommand::Initialize(InitializeVolume {
        descriptor: descriptor(),
        initial_copies: different_copies,
    }));
    assert_eq!(conflict.state, state);
    assert_eq!(
        conflict.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::AlreadyInitialized)
    );
}

#[test]
fn expansion_changes_only_capacity_and_revision() {
    let initialized = initialized();
    let writer = WriterGrant {
        node_id: node(1),
        session_id: session(41),
    };
    let attached = apply(
        &initialized,
        VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: expected(&initialized),
            writer,
        }),
    );
    let target = VolumeCapacity::new(128 << 20).expect("aligned expansion capacity");
    let command = VolumeCommand::Expand(ExpandVolume {
        expected: expected(&attached),
        target_capacity: target,
    });
    let plan = attached.evaluate(&command);
    assert!(matches!(
        plan.response,
        VolumeCommandResponse::Applied { revision: 3, .. }
    ));
    assert_eq!(plan.state.revision(), 3);
    assert_eq!(
        plan.state
            .descriptor()
            .expect("expanded state has a descriptor")
            .capacity(),
        target
    );
    assert_eq!(plan.state.data(), attached.data());
    assert_eq!(plan.state.disposition(), attached.disposition());
    assert_eq!(plan.state.replacement(), attached.replacement());

    let retried = plan.state.evaluate(&command);
    assert_eq!(retried.state, plan.state);
    assert!(matches!(
        retried.response,
        VolumeCommandResponse::Current { revision: 3, .. }
    ));
}

#[test]
fn expansion_rejects_every_unsafe_control_state() {
    let state = initialized();
    let current_capacity = state
        .descriptor()
        .expect("initialized state has a descriptor")
        .capacity();
    let larger = VolumeCapacity::new(128 << 20).expect("aligned expansion capacity");
    let evaluate = |state: &VolumeControlState, target_capacity| {
        state.evaluate(&VolumeCommand::Expand(ExpandVolume {
            expected: expected(state),
            target_capacity,
        }))
    };

    let stale = state.evaluate(&VolumeCommand::Expand(ExpandVolume {
        expected: ExpectedVolumeRevision {
            generation: generation(),
            revision: 0,
        },
        target_capacity: larger,
    }));
    assert_eq!(
        stale.response,
        VolumeCommandResponse::Conflict {
            current_revision: state.revision(),
        }
    );
    assert_eq!(
        evaluate(
            &state,
            VolumeCapacity::new(current_capacity.bytes() - 4096).expect("smaller aligned capacity"),
        )
        .response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::CapacityCannotShrink)
    );
    assert_eq!(
        evaluate(
            &state,
            VolumeCapacity::new(current_capacity.bytes() + 1).expect("non-zero unaligned capacity"),
        )
        .response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::CapacityNotAligned)
    );

    let retained = apply(
        &state,
        VolumeCommand::SetDisposition(SetVolumeDisposition {
            expected: expected(&state),
            disposition: VolumeDisposition::Retained,
        }),
    );
    assert_eq!(
        evaluate(&retained, larger).response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::VolumeNotLive)
    );

    let recovery = RecoveryGrant {
        id: recovery_id(91),
        coordinator_node_id: node(1),
        source_node_id: node(1),
        target_node_ids: initial_copies(),
    };
    let recovering = apply(
        &state,
        VolumeCommand::BeginRecovery(BeginVolumeRecovery {
            expected: expected(&state),
            expected_writer: None,
            replaced_recovery_id: None,
            recovery,
        }),
    );
    assert_eq!(
        evaluate(&recovering, larger).response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::RecoveryInProgress)
    );

    let replacing = apply(
        &state,
        VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: expected(&state),
            replacement: ReplacementGrant {
                id: replacement_id(92),
                coordinator_node_id: node(1),
                old_node_id: Some(node(2)),
                new_node_id: node(4),
                source_node_id: node(1),
            },
        }),
    );
    assert_eq!(
        evaluate(&replacing, larger).response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::ReplacementInProgress)
    );

    let mut exhausted = state;
    exhausted.revision = u64::MAX;
    assert_eq!(
        evaluate(&exhausted, larger).response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::RevisionExhausted)
    );
}

#[test]
fn conflicts_and_rejections_leave_control_state_unchanged() {
    let state = initialized();
    let stale = state.evaluate(&VolumeCommand::GrantWriter(GrantVolumeWriter {
        expected: ExpectedVolumeRevision {
            generation: generation(),
            revision: 0,
        },
        writer: WriterGrant {
            node_id: node(1),
            session_id: session(1),
        },
    }));
    assert_eq!(stale.state, state);
    assert_eq!(
        stale.response,
        VolumeCommandResponse::Conflict {
            current_revision: 1,
        }
    );

    let invalid = state.evaluate(&VolumeCommand::GrantWriter(GrantVolumeWriter {
        expected: expected(&state),
        writer: WriterGrant {
            node_id: node(9),
            session_id: session(2),
        },
    }));
    assert_eq!(invalid.state, state);
    assert_eq!(
        invalid.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::WriterOutsideCopySet)
    );
}

#[test]
fn exact_postconditions_never_hide_a_wrong_generation() {
    let state = initialized();
    let wrong_generation = VolumeGeneration::new(generation().get() + 1)
        .expect("different test generation must be valid");
    let result = state.evaluate(&VolumeCommand::SetDisposition(SetVolumeDisposition {
        expected: ExpectedVolumeRevision {
            generation: wrong_generation,
            revision: state.revision(),
        },
        disposition: VolumeDisposition::Live,
    }));
    assert_eq!(result.state, state);
    assert_eq!(
        result.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::WrongGeneration)
    );
}

#[test]
fn grant_and_fence_are_idempotent_and_advance_one_fence_each() {
    let initial = initialized();
    let writer = WriterGrant {
        node_id: node(1),
        session_id: session(1),
    };
    let grant = VolumeCommand::GrantWriter(GrantVolumeWriter {
        expected: expected(&initial),
        writer,
    });
    let granted = apply(&initial, grant.clone());
    assert_eq!(granted.revision(), 2);
    assert_eq!(fence(&granted).get(), 2);
    assert_eq!(granted.data().and_then(|data| data.writer), Some(writer));

    let retry = granted.evaluate(&grant);
    assert_eq!(retry.state, granted);
    assert!(matches!(
        retry.response,
        VolumeCommandResponse::Current { revision: 2, .. }
    ));

    let fenced = apply(
        &granted,
        VolumeCommand::FenceWriter(FenceVolumeWriter {
            expected: expected(&granted),
            writer,
        }),
    );
    assert_eq!(fenced.revision(), 3);
    assert_eq!(fence(&fenced).get(), 3);
    assert!(fenced.data().is_some_and(|data| data.writer.is_none()));

    let retry = fenced.evaluate(&VolumeCommand::FenceWriter(FenceVolumeWriter {
        expected: expected(&granted),
        writer,
    }));
    assert_eq!(retry.state, fenced);
    assert!(matches!(
        retry.response,
        VolumeCommandResponse::Current { revision: 3, .. }
    ));
}

#[test]
fn retain_and_restore_do_not_wait_for_cleanup() {
    let initial = initialized();
    let writer = WriterGrant {
        node_id: node(1),
        session_id: session(1),
    };
    let granted = apply(
        &initial,
        VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: expected(&initial),
            writer,
        }),
    );
    let retained = apply(
        &granted,
        VolumeCommand::SetDisposition(SetVolumeDisposition {
            expected: expected(&granted),
            disposition: VolumeDisposition::Retained,
        }),
    );
    assert_eq!(retained.disposition(), VolumeDisposition::Retained);
    assert!(retained.data().is_some_and(|data| data.writer.is_none()));
    assert_eq!(fence(&retained).get(), 3);

    let restored = apply(
        &retained,
        VolumeCommand::SetDisposition(SetVolumeDisposition {
            expected: expected(&retained),
            disposition: VolumeDisposition::Live,
        }),
    );
    assert_eq!(restored.disposition(), VolumeDisposition::Live);
    assert_eq!(fence(&restored), fence(&retained));
}

#[test]
fn recovery_waits_for_replacement_membership_rollback() {
    let initial = initialized();
    let replacement = ReplacementGrant {
        id: replacement_id(1),
        coordinator_node_id: node(1),
        old_node_id: Some(node(3)),
        new_node_id: node(4),
        source_node_id: node(1),
    };
    let replacing = apply(
        &initial,
        VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: expected(&initial),
            replacement,
        }),
    );
    let targets: BTreeSet<_> = [node(1), node(2)].into_iter().collect();
    let recovery = RecoveryGrant {
        id: recovery_id(1),
        coordinator_node_id: node(1),
        source_node_id: node(1),
        target_node_ids: targets.clone(),
    };
    let result = replacing.evaluate(&VolumeCommand::BeginRecovery(BeginVolumeRecovery {
        expected: expected(&replacing),
        expected_writer: None,
        replaced_recovery_id: None,
        recovery,
    }));

    assert_eq!(result.state, replacing);
    assert_eq!(
        result.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::ReplacementInProgress)
    );
}

#[test]
fn maintenance_coordinator_must_own_the_selected_source() {
    let initial = initialized();
    let recovery = initial.evaluate(&VolumeCommand::BeginRecovery(BeginVolumeRecovery {
        expected: expected(&initial),
        expected_writer: None,
        replaced_recovery_id: None,
        recovery: RecoveryGrant {
            id: recovery_id(1),
            coordinator_node_id: node(2),
            source_node_id: node(1),
            target_node_ids: initial_copies(),
        },
    }));
    assert_eq!(recovery.state, initial);
    assert_eq!(
        recovery.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::InvalidRecovery)
    );

    let replacement = initial.evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
        expected: expected(&initial),
        replacement: ReplacementGrant {
            id: replacement_id(1),
            coordinator_node_id: node(2),
            old_node_id: Some(node(3)),
            new_node_id: node(4),
            source_node_id: node(1),
        },
    }));
    assert_eq!(replacement.state, initial);
    assert_eq!(
        replacement.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::InvalidReplacement)
    );
}

#[test]
fn attached_replacement_must_be_coordinated_by_the_writer() {
    let initial = initialized();
    let writer = WriterGrant {
        node_id: node(1),
        session_id: session(1),
    };
    let attached = apply(
        &initial,
        VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: expected(&initial),
            writer,
        }),
    );
    let replacement = ReplacementGrant {
        id: replacement_id(1),
        coordinator_node_id: node(2),
        old_node_id: Some(node(1)),
        new_node_id: node(4),
        source_node_id: node(2),
    };
    let result = attached.evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
        expected: expected(&attached),
        replacement,
    }));

    assert_eq!(result.state, attached);
    assert_eq!(
        result.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::InvalidReplacement)
    );

    let mut invalid_snapshot = attached;
    invalid_snapshot.replacement = Some(replacement);
    assert_eq!(
        invalid_snapshot.validate(),
        Err(super::VolumeControlStateInvariantError::InvalidReplacement)
    );
}

#[test]
fn recovery_id_cannot_be_reused_with_different_grant() {
    let initial = initialized();
    let targets: BTreeSet<_> = [node(1), node(2)].into_iter().collect();
    let recovery = RecoveryGrant {
        id: recovery_id(1),
        coordinator_node_id: node(1),
        source_node_id: node(1),
        target_node_ids: targets,
    };
    let recovering = apply(
        &initial,
        VolumeCommand::BeginRecovery(BeginVolumeRecovery {
            expected: expected(&initial),
            expected_writer: None,
            replaced_recovery_id: None,
            recovery: recovery.clone(),
        }),
    );
    let changed = RecoveryGrant {
        coordinator_node_id: node(2),
        ..recovery
    };
    let conflict = recovering.evaluate(&VolumeCommand::BeginRecovery(BeginVolumeRecovery {
        expected: expected(&recovering),
        expected_writer: None,
        replaced_recovery_id: Some(recovery_id(1)),
        recovery: changed,
    }));
    assert_eq!(conflict.state, recovering);
    assert_eq!(
        conflict.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::RecoveryIdConflict)
    );
}

#[test]
fn recovery_can_move_to_a_new_bound_source_without_saved_progress() {
    let initial = initialized();
    let first = RecoveryGrant {
        id: recovery_id(1),
        coordinator_node_id: node(1),
        source_node_id: node(1),
        target_node_ids: initial_copies(),
    };
    let recovering = apply(
        &initial,
        VolumeCommand::BeginRecovery(BeginVolumeRecovery {
            expected: expected(&initial),
            expected_writer: None,
            replaced_recovery_id: None,
            recovery: first.clone(),
        }),
    );
    let moved = RecoveryGrant {
        id: recovery_id(2),
        coordinator_node_id: node(2),
        source_node_id: node(2),
        target_node_ids: initial_copies(),
    };
    let handed_off = apply(
        &recovering,
        VolumeCommand::BeginRecovery(BeginVolumeRecovery {
            expected: expected(&recovering),
            expected_writer: None,
            replaced_recovery_id: Some(first.id),
            recovery: moved.clone(),
        }),
    );

    assert_eq!(
        handed_off.data().and_then(|data| data.recovery.as_ref()),
        Some(&moved)
    );
    assert!(fence(&handed_off) > fence(&recovering));
}

#[test]
fn detached_recovery_revocation_allows_third_copy_rebuild() {
    let initial = initialized();
    let targets = BTreeSet::from([node(1), node(2)]);
    let recovery = RecoveryGrant {
        id: recovery_id(1),
        coordinator_node_id: node(1),
        source_node_id: node(1),
        target_node_ids: targets.clone(),
    };
    let recovering = apply(
        &initial,
        VolumeCommand::BeginRecovery(BeginVolumeRecovery {
            expected: expected(&initial),
            expected_writer: None,
            replaced_recovery_id: None,
            recovery: recovery.clone(),
        }),
    );
    let recovery_fence = fence(&recovering);
    let writer = WriterGrant {
        node_id: node(1),
        session_id: session(1),
    };
    let premature_writer = recovering.evaluate(&VolumeCommand::GrantWriter(GrantVolumeWriter {
        expected: expected(&recovering),
        writer,
    }));
    assert_eq!(premature_writer.state, recovering);
    assert_eq!(
        premature_writer.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::RecoveryInProgress)
    );
    let wrong_recovery =
        recovering.evaluate(&VolumeCommand::RevokeRecovery(RevokeVolumeRecovery {
            expected: expected(&recovering),
            recovery_id: recovery_id(2),
        }));
    assert_eq!(wrong_recovery.state, recovering);
    assert_eq!(
        wrong_recovery.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::WrongRecovery)
    );
    let revoked = apply(
        &recovering,
        VolumeCommand::RevokeRecovery(RevokeVolumeRecovery {
            expected: expected(&recovering),
            recovery_id: recovery.id,
        }),
    );

    let data = revoked.data().expect("revoked recovery has control state");
    assert_eq!(data.copies, targets);
    assert_eq!(data.fence, recovery_fence);
    assert_eq!(data.writer, None);
    assert_eq!(data.recovery, None);

    let retry = revoked.evaluate(&VolumeCommand::RevokeRecovery(RevokeVolumeRecovery {
        expected: expected(&recovering),
        recovery_id: recovery.id,
    }));
    assert_eq!(retry.state, revoked);
    assert!(matches!(
        retry.response,
        VolumeCommandResponse::Current { .. }
    ));

    let replacement = revoked.evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
        expected: expected(&revoked),
        replacement: ReplacementGrant {
            id: replacement_id(1),
            coordinator_node_id: node(1),
            old_node_id: None,
            new_node_id: node(4),
            source_node_id: node(1),
        },
    }));
    assert!(matches!(
        replacement.response,
        VolumeCommandResponse::Applied { .. }
    ));
}

#[test]
fn replacement_adoption_changes_exactly_one_copy() {
    let initial = initialized();
    let replacement = ReplacementGrant {
        id: replacement_id(1),
        coordinator_node_id: node(1),
        old_node_id: Some(node(3)),
        new_node_id: node(4),
        source_node_id: node(1),
    };
    let replacing = apply(
        &initial,
        VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: expected(&initial),
            replacement,
        }),
    );
    let new_copies: BTreeSet<_> = [node(1), node(2), node(4)].into_iter().collect();
    let adopted = apply(
        &replacing,
        VolumeCommand::AdoptReplacement(AdoptReplicaReplacement {
            expected: expected(&replacing),
            replacement_id: replacement.id,
            new_copies: new_copies.clone(),
            expected_writer: None,
        }),
    );
    assert_eq!(adopted.replacement(), None);
    assert_eq!(adopted.data().map(|data| &data.copies), Some(&new_copies));
    assert_eq!(fence(&adopted).get(), 2);
}

#[test]
fn replacement_grant_must_produce_exactly_three_copies() {
    let initial = initialized();
    let missing_old = initial.evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
        expected: expected(&initial),
        replacement: ReplacementGrant {
            id: replacement_id(1),
            coordinator_node_id: node(1),
            old_node_id: None,
            new_node_id: node(4),
            source_node_id: node(1),
        },
    }));
    assert_eq!(missing_old.state, initial);
    assert_eq!(
        missing_old.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::InvalidReplacement)
    );

    let recovery = RecoveryGrant {
        id: recovery_id(1),
        coordinator_node_id: node(1),
        source_node_id: node(1),
        target_node_ids: BTreeSet::from([node(1), node(2)]),
    };
    let recovering = apply(
        &initial,
        VolumeCommand::BeginRecovery(BeginVolumeRecovery {
            expected: expected(&initial),
            expected_writer: None,
            replaced_recovery_id: None,
            recovery: recovery.clone(),
        }),
    );
    let writer = WriterGrant {
        node_id: node(1),
        session_id: session(1),
    };
    let recovered = apply(
        &recovering,
        VolumeCommand::RevokeRecovery(RevokeVolumeRecovery {
            expected: expected(&recovering),
            recovery_id: recovery.id,
        }),
    );
    let degraded = apply(
        &recovered,
        VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: expected(&recovered),
            writer,
        }),
    );
    let removes_remaining_copy =
        degraded.evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: expected(&degraded),
            replacement: ReplacementGrant {
                id: replacement_id(2),
                coordinator_node_id: node(1),
                old_node_id: Some(node(2)),
                new_node_id: node(4),
                source_node_id: node(1),
            },
        }));
    assert_eq!(removes_remaining_copy.state, degraded);
    assert_eq!(
        removes_remaining_copy.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::InvalidReplacement)
    );

    let restores_missing_copy =
        degraded.evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: expected(&degraded),
            replacement: ReplacementGrant {
                id: replacement_id(3),
                coordinator_node_id: node(1),
                old_node_id: Some(node(3)),
                new_node_id: node(4),
                source_node_id: node(1),
            },
        }));
    assert!(matches!(
        restores_missing_copy.response,
        VolumeCommandResponse::Applied { .. }
    ));
    restores_missing_copy
        .state
        .validate()
        .expect("replacement of a stale voter must remain valid");

    let mut invalid_snapshot = initial;
    invalid_snapshot.replacement = Some(ReplacementGrant {
        id: replacement_id(4),
        coordinator_node_id: node(1),
        old_node_id: None,
        new_node_id: node(4),
        source_node_id: node(1),
    });
    assert_eq!(
        invalid_snapshot.validate(),
        Err(super::VolumeControlStateInvariantError::InvalidReplacement)
    );
}

#[test]
fn degraded_two_copy_state_can_add_a_new_third_copy() {
    let initial = initialized();
    let targets = BTreeSet::from([node(1), node(2)]);
    let recovery = RecoveryGrant {
        id: recovery_id(1),
        coordinator_node_id: node(1),
        source_node_id: node(1),
        target_node_ids: targets,
    };
    let recovering = apply(
        &initial,
        VolumeCommand::BeginRecovery(BeginVolumeRecovery {
            expected: expected(&initial),
            expected_writer: None,
            replaced_recovery_id: None,
            recovery: recovery.clone(),
        }),
    );
    let writer = WriterGrant {
        node_id: node(1),
        session_id: session(1),
    };
    let recovered = apply(
        &recovering,
        VolumeCommand::RevokeRecovery(RevokeVolumeRecovery {
            expected: expected(&recovering),
            recovery_id: recovery.id,
        }),
    );
    let degraded = apply(
        &recovered,
        VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: expected(&recovered),
            writer,
        }),
    );
    let replacement = ReplacementGrant {
        id: replacement_id(1),
        coordinator_node_id: node(1),
        old_node_id: None,
        new_node_id: node(4),
        source_node_id: node(1),
    };
    let replacing = apply(
        &degraded,
        VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: expected(&degraded),
            replacement,
        }),
    );
    let new_copies = BTreeSet::from([node(1), node(2), node(4)]);
    let adopted = apply(
        &replacing,
        VolumeCommand::AdoptReplacement(AdoptReplicaReplacement {
            expected: expected(&replacing),
            replacement_id: replacement.id,
            new_copies: new_copies.clone(),
            expected_writer: Some(writer),
        }),
    );

    assert_eq!(adopted.data().map(|data| &data.copies), Some(&new_copies));
    assert_eq!(adopted.data().and_then(|data| data.writer), Some(writer));
    assert_eq!(adopted.replacement(), None);
}

#[test]
fn replacement_adoption_preserves_writer_session_at_new_fence() {
    let initial = initialized();
    let writer = WriterGrant {
        node_id: node(1),
        session_id: DriverSessionId::new(Uuid::from_u128(90)).expect("driver session"),
    };
    let attached = apply(
        &initial,
        VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: expected(&initial),
            writer,
        }),
    );
    let replacement = ReplacementGrant {
        id: replacement_id(1),
        coordinator_node_id: node(1),
        old_node_id: Some(node(3)),
        new_node_id: node(4),
        source_node_id: node(1),
    };
    let replacing = apply(
        &attached,
        VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: expected(&attached),
            replacement,
        }),
    );
    let new_copies: BTreeSet<_> = [node(1), node(2), node(4)].into_iter().collect();
    let adopted = apply(
        &replacing,
        VolumeCommand::AdoptReplacement(AdoptReplicaReplacement {
            expected: expected(&replacing),
            replacement_id: replacement.id,
            new_copies,
            expected_writer: Some(writer),
        }),
    );

    assert_eq!(adopted.data().and_then(|data| data.writer), Some(writer));
    assert!(fence(&adopted) > fence(&replacing));
}

#[test]
fn replacement_cancel_is_exact_and_idempotent() {
    let initial = initialized();
    let replacement = ReplacementGrant {
        id: replacement_id(1),
        coordinator_node_id: node(1),
        old_node_id: Some(node(3)),
        new_node_id: node(4),
        source_node_id: node(1),
    };
    let replacing = apply(
        &initial,
        VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: expected(&initial),
            replacement,
        }),
    );
    let wrong = replacing.evaluate(&VolumeCommand::CancelReplacement(
        CancelReplicaReplacement {
            expected: expected(&replacing),
            replacement_id: replacement_id(2),
        },
    ));
    assert_eq!(wrong.state, replacing);
    assert_eq!(
        wrong.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::WrongReplacement)
    );
    let cancelled = apply(
        &replacing,
        VolumeCommand::CancelReplacement(CancelReplicaReplacement {
            expected: expected(&replacing),
            replacement_id: replacement.id,
        }),
    );
    let retry = cancelled.evaluate(&VolumeCommand::CancelReplacement(
        CancelReplicaReplacement {
            expected: expected(&replacing),
            replacement_id: replacement.id,
        },
    ));
    assert_eq!(retry.state, cancelled);
    assert!(matches!(
        retry.response,
        VolumeCommandResponse::Current { .. }
    ));
}

#[test]
fn checked_counters_never_wrap_or_partially_mutate() {
    let mut revision_exhausted = initialized();
    revision_exhausted.revision = u64::MAX;
    let revision_result =
        revision_exhausted.evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: expected(&revision_exhausted),
            replacement: ReplacementGrant {
                id: replacement_id(1),
                coordinator_node_id: node(1),
                old_node_id: Some(node(3)),
                new_node_id: node(4),
                source_node_id: node(1),
            },
        }));
    assert_eq!(revision_result.state, revision_exhausted);
    assert_eq!(
        revision_result.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::RevisionExhausted)
    );

    let mut fence_exhausted = initialized();
    fence_exhausted
        .data
        .as_mut()
        .expect("initialized state must have data")
        .fence = FenceEpoch::new(u64::MAX).expect("maximum fence is non-zero");
    let writer = WriterGrant {
        node_id: node(1),
        session_id: session(1),
    };
    let fence_result = fence_exhausted.evaluate(&VolumeCommand::GrantWriter(GrantVolumeWriter {
        expected: expected(&fence_exhausted),
        writer,
    }));
    assert_eq!(fence_result.state, fence_exhausted);
    assert_eq!(
        fence_result.response,
        VolumeCommandResponse::Rejected(VolumeCommandRejection::FenceExhausted)
    );
}

#[test]
fn one_hundred_thousand_attach_cycles_keep_state_bounded() {
    let mut state = initialized();
    let structural_size = std::mem::size_of_val(&state);
    let encoded_size = crate::protocol::encode_control_state(&state, 64 * 1024)
        .expect("initial control state must encode")
        .len();
    for cycle in 0..100_000_u128 {
        let writer = WriterGrant {
            node_id: node(1),
            session_id: session(1_000 + cycle),
        };
        state = apply(
            &state,
            VolumeCommand::GrantWriter(GrantVolumeWriter {
                expected: expected(&state),
                writer,
            }),
        );
        state = apply(
            &state,
            VolumeCommand::FenceWriter(FenceVolumeWriter {
                expected: expected(&state),
                writer,
            }),
        );
        assert_eq!(std::mem::size_of_val(&state), structural_size);
    }
    assert_eq!(state.revision(), 200_001);
    assert_eq!(fence(&state).get(), 200_001);
    state.validate().expect("soaked state must remain valid");
    assert_eq!(
        crate::protocol::encode_control_state(&state, 64 * 1024)
            .expect("soaked control state must encode")
            .len(),
        encoded_size
    );
}

#[test]
fn arbitrary_valid_command_sequences_preserve_every_invariant() {
    let mut command_counts = [0_usize; 10];
    let mut response_counts = [0_usize; 4];

    for seed in 1..=64_u64 {
        let mut rng = SequenceRng(seed ^ 0xa076_1d64_78bd_642f);
        let mut state = initialized();
        let mut previous = None;

        for step in 0..256_u128 {
            let serial = u128::from(seed) * 1_000 + step;
            let command = if step > 0 && step.is_multiple_of(23) {
                previous
                    .clone()
                    .expect("a prior command must exist after the first step")
            } else {
                generated_command(&state, &mut rng, serial)
            };
            command_counts[command_slot(&command)] += 1;

            let before_bytes = crate::protocol::encode_control_state(&state, 64 * 1024)
                .expect("valid control state must encode before evaluation");
            let before_revision = state.revision();
            let before_fence = fence(&state);
            let plan = std::panic::catch_unwind(|| state.evaluate(&command)).unwrap_or_else(|_| {
                panic!("control state evaluation panicked for {command:?} against {state:?}")
            });
            plan.state
                .validate()
                .expect("every evaluated sequence state must preserve invariants");

            match plan.response {
                VolumeCommandResponse::Applied { revision, .. } => {
                    response_counts[0] += 1;
                    assert_eq!(revision, before_revision + 1);
                    assert_eq!(plan.state.revision(), revision);
                    assert!(fence(&plan.state) >= before_fence);
                }
                VolumeCommandResponse::Current { .. } => {
                    response_counts[1] += 1;
                    assert_eq!(plan.state, state);
                }
                VolumeCommandResponse::Conflict { .. } => {
                    response_counts[2] += 1;
                    assert_eq!(plan.state, state);
                }
                VolumeCommandResponse::Rejected(_) => {
                    response_counts[3] += 1;
                    assert_eq!(plan.state, state);
                }
            }

            if !matches!(plan.response, VolumeCommandResponse::Applied { .. }) {
                assert_eq!(
                    crate::protocol::encode_control_state(&plan.state, 64 * 1024)
                        .expect("valid control state must encode after evaluation"),
                    before_bytes,
                    "non-applied command changed encoded control state: {command:?}",
                );
            }

            state = plan.state;
            previous = Some(command);
        }
    }

    assert!(
        command_counts.into_iter().all(|count| count > 0),
        "sequence generator missed a command kind: {command_counts:?}",
    );
    assert!(
        response_counts.into_iter().all(|count| count > 0),
        "sequence generator missed a response kind: {response_counts:?}",
    );
}
