//! Unit tests for shared replicated-volume runtime decisions.

use super::{
    ReplicaCapacityStatus, SavedAttachmentMountAction, WriterReplicaRecoveryRequired,
    all_copies_serve_capacity, raft_group_idle_timeout, replacement_capacity_can_converge,
    replacement_voter_hint_is_valid, saved_attachment_mount_action, stale_replacement_learners,
    validate_replacement_rollback_voters, validate_replacement_target_reset,
    writer_fence_install_generation, writer_path_failure_requires_recovery,
};
use mantissa_volume::catalog::SavedMountState;
use mantissa_volume::control_state::{
    BeginReplicaReplacement, BeginVolumeRecovery, ExpectedVolumeRevision, InitializeVolume,
    RecoveryGrant, ReplacementGrant, RevokeVolumeRecovery, VolumeCommand, VolumeControlState,
    WriterGrant,
};
use mantissa_volume::{
    DriverSessionId, FenceEpoch, RecoveryId, ReplacementId, VolumeBlockSizes, VolumeCapacity,
    VolumeDescriptor, VolumeGeneration, VolumeId, VolumeNodeId,
};
use std::collections::BTreeSet;
use std::time::Duration;
use uuid::Uuid;

/// Creates one valid test volume-node identity.
fn volume_node(value: u128) -> VolumeNodeId {
    VolumeNodeId::new(Uuid::from_u128(value)).expect("non-zero test volume node")
}

/// Applies one command whose validity is part of this test's setup.
fn apply(state: &VolumeControlState, command: VolumeCommand) -> VolumeControlState {
    let result = state.evaluate(&command);
    assert!(matches!(
        result.response,
        mantissa_volume::control_state::VolumeCommandResponse::Applied { .. }
    ));
    result.state
}

/// The mapped writer waits until every active copy serves the committed range.
#[test]
fn writer_capacity_requires_every_copy_to_serve() {
    let target = VolumeCapacity::new(128 << 20).expect("valid test capacity");
    let ready = ReplicaCapacityStatus {
        reserved_capacity_bytes: target.bytes(),
        prepared_capacity_bytes: target.bytes(),
        served_capacity_bytes: target.bytes(),
        healthy: true,
        reason: String::new(),
    };
    assert!(!all_copies_serve_capacity(
        3,
        &[ready.clone(), ready.clone()],
        target
    ));
    assert!(all_copies_serve_capacity(
        3,
        &[ready.clone(), ready.clone(), ready.clone()],
        target
    ));

    let mut behind = ready.clone();
    behind.served_capacity_bytes = 64 << 20;
    assert!(!all_copies_serve_capacity(
        3,
        &[ready.clone(), ready.clone(), behind],
        target
    ));
    let mut unhealthy = ready.clone();
    unhealthy.healthy = false;
    assert!(!all_copies_serve_capacity(
        3,
        &[ready.clone(), ready, unhealthy],
        target
    ));
}

/// A returning voter may catch up from an older capacity but never from another identity.
#[test]
fn replacement_voter_accepts_only_convergent_capacity() {
    let initial = VolumeDescriptor::new(
        VolumeId::new(Uuid::from_u128(10)).expect("test volume ID"),
        VolumeGeneration::new(1).expect("test volume generation"),
        64 << 20,
        VolumeBlockSizes::supported(),
    )
    .expect("test initial descriptor");
    let expanded = initial
        .with_capacity(VolumeCapacity::new(128 << 20).expect("expanded test capacity"))
        .expect("compatible expanded descriptor");
    let other_generation = VolumeDescriptor::new(
        initial.volume_id(),
        VolumeGeneration::new(2).expect("other test generation"),
        expanded.capacity().bytes(),
        VolumeBlockSizes::supported(),
    )
    .expect("other generation descriptor");

    assert!(replacement_capacity_can_converge(&initial, &expanded));
    assert!(replacement_capacity_can_converge(&expanded, &expanded));
    assert!(!replacement_capacity_can_converge(&expanded, &initial));
    assert!(!replacement_capacity_can_converge(
        &initial,
        &other_generation
    ));
}

/// A fence handoff cannot erase evidence that selected copies diverged.
#[test]
fn differing_writer_progress_requires_recovery() {
    let old_fence = FenceEpoch::new(4).expect("old test fence");
    let next_fence = FenceEpoch::new(5).expect("next test fence");
    assert!(
        writer_fence_install_generation(false, old_fence, next_fence, 9)
            .expect_err("differing progress must require recovery")
            .is::<WriterReplicaRecoveryRequired>()
    );
    assert_eq!(
        writer_fence_install_generation(true, old_fence, next_fence, 9)
            .expect("matching progress may advance one fence"),
        Some(10)
    );
    assert_eq!(
        writer_fence_install_generation(true, next_fence, next_fence, 10)
            .expect("matching current progress needs no fence install"),
        None
    );
}

/// Mount retries clean a fenced session instead of granting it another writer fence.
#[test]
fn mount_retry_cleans_fenced_unmounted_attachment() {
    let local_node = volume_node(1);
    let session = DriverSessionId::new(Uuid::from_u128(2)).expect("test driver session");
    let old_fence = FenceEpoch::new(4).expect("old test fence");
    let recovery_fence = FenceEpoch::new(5).expect("recovery test fence");

    assert_eq!(
        saved_attachment_mount_action(
            local_node,
            session,
            Some(old_fence),
            None,
            None,
            recovery_fence,
        ),
        SavedAttachmentMountAction::Clean
    );
    assert_eq!(
        saved_attachment_mount_action(
            local_node,
            session,
            Some(old_fence),
            Some(SavedMountState::Mounted),
            None,
            recovery_fence,
        ),
        SavedAttachmentMountAction::RestartConsumer
    );
}

/// Only an exact current session may be reused, and a newer fence waits for handoff.
#[test]
fn mount_retry_preserves_exact_writer_handoff() {
    let local_node = volume_node(3);
    let session = DriverSessionId::new(Uuid::from_u128(4)).expect("test driver session");
    let current_fence = FenceEpoch::new(6).expect("current test fence");
    let next_fence = FenceEpoch::new(7).expect("next test fence");
    let writer = Some(WriterGrant {
        node_id: local_node,
        session_id: session,
    });

    assert_eq!(
        saved_attachment_mount_action(
            local_node,
            session,
            Some(current_fence),
            None,
            writer,
            current_fence,
        ),
        SavedAttachmentMountAction::Reuse
    );
    assert_eq!(
        saved_attachment_mount_action(
            local_node,
            session,
            Some(current_fence),
            Some(SavedMountState::Mounted),
            writer,
            next_fence,
        ),
        SavedAttachmentMountAction::WaitForFenceHandoff
    );
}

/// Local ownership and transport failures do not invent recovery grant.
#[test]
fn only_proven_copy_divergence_requests_writer_recovery() {
    let local_failure = anyhow::anyhow!("another local driver is still owned");
    assert!(!writer_path_failure_requires_recovery(&local_failure));

    let divergence = anyhow::Error::new(WriterReplicaRecoveryRequired);
    assert!(writer_path_failure_requires_recovery(&divergence));
}

/// An unhealthy voter can reset only under its exact inactive replacement grant.
#[test]
fn unhealthy_replacement_reset_requires_applied_data_exclusion() {
    let descriptor = VolumeDescriptor::new(
        VolumeId::new(Uuid::from_u128(10)).expect("test volume ID"),
        VolumeGeneration::new(1).expect("test volume generation"),
        64 << 20,
        VolumeBlockSizes::supported(),
    )
    .expect("test volume descriptor");
    let initial = apply(
        &VolumeControlState::default(),
        VolumeCommand::Initialize(InitializeVolume {
            descriptor,
            initial_copies: [volume_node(1), volume_node(2), volume_node(3)]
                .into_iter()
                .collect(),
        }),
    );
    let recovery_id = RecoveryId::new(Uuid::from_u128(20)).expect("test recovery ID");
    let recovering = apply(
        &initial,
        VolumeCommand::BeginRecovery(BeginVolumeRecovery {
            expected: ExpectedVolumeRevision {
                generation: initial
                    .descriptor()
                    .expect("initialized descriptor")
                    .generation(),
                revision: initial.revision(),
            },
            expected_writer: None,
            replaced_recovery_id: None,
            recovery: RecoveryGrant {
                id: recovery_id,
                coordinator_node_id: volume_node(1),
                source_node_id: volume_node(1),
                target_node_ids: [volume_node(1), volume_node(2)].into_iter().collect(),
            },
        }),
    );
    let degraded = apply(
        &recovering,
        VolumeCommand::RevokeRecovery(RevokeVolumeRecovery {
            expected: ExpectedVolumeRevision {
                generation: recovering
                    .descriptor()
                    .expect("recovering descriptor")
                    .generation(),
                revision: recovering.revision(),
            },
            recovery_id,
        }),
    );
    let replacement_id = ReplacementId::new(Uuid::from_u128(30)).expect("test replacement ID");
    let replacing = apply(
        &degraded,
        VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: ExpectedVolumeRevision {
                generation: degraded
                    .descriptor()
                    .expect("degraded descriptor")
                    .generation(),
                revision: degraded.revision(),
            },
            replacement: ReplacementGrant {
                id: replacement_id,
                coordinator_node_id: volume_node(1),
                old_node_id: None,
                new_node_id: volume_node(3),
                source_node_id: volume_node(1),
            },
        }),
    );

    validate_replacement_target_reset(&replacing, replacement_id, volume_node(3))
        .expect("the exact excluded replacement target may reset");
    assert!(
        validate_replacement_target_reset(
            &replacing,
            ReplacementId::new(Uuid::from_u128(31)).expect("other replacement ID"),
            volume_node(3),
        )
        .is_err()
    );
    assert!(validate_replacement_target_reset(&replacing, replacement_id, volume_node(2)).is_err());
    assert!(validate_replacement_target_reset(&initial, replacement_id, volume_node(3)).is_err());
}

#[test]
fn idle_timeout_covers_two_election_windows_and_one_wake_attempt() {
    let config = openraft::Config {
        election_timeout_max: 12_000,
        ..Default::default()
    };

    assert_eq!(raft_group_idle_timeout(&config), Duration::from_secs(26));
}

#[test]
fn replacement_rollback_accepts_exact_active_data_voters() {
    let copies = [volume_node(1), volume_node(2), volume_node(3)]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let original = [Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]
        .into_iter()
        .collect();
    let survivors = [Uuid::from_u128(1), Uuid::from_u128(2)]
        .into_iter()
        .collect();

    validate_replacement_rollback_voters(&copies, &original)
        .expect("all three active data copies are valid rollback voters");
    validate_replacement_rollback_voters(&copies, &survivors)
        .expect("two selected active survivors are valid rollback voters");
}

/// A later replacement removes an ungranted old learner before adding its target.
#[test]
fn replacement_prunes_only_obsolete_non_data_learners() {
    let voters = BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]);
    let members = BTreeSet::from([
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        Uuid::from_u128(3),
        Uuid::from_u128(4),
    ]);
    let copies = BTreeSet::from([volume_node(1), volume_node(2), volume_node(4)]);

    assert_eq!(
        stale_replacement_learners(&members, &voters, &copies, Uuid::from_u128(5)),
        Vec::<Uuid>::new(),
        "the old learner is still an active data copy"
    );

    let stable_copies = BTreeSet::from([volume_node(1), volume_node(2), volume_node(3)]);
    assert_eq!(
        stale_replacement_learners(&members, &voters, &stable_copies, Uuid::from_u128(5)),
        vec![Uuid::from_u128(4)]
    );
}

#[test]
fn replacement_rollback_rejects_unsafe_or_foreign_voters() {
    let copies = [volume_node(1), volume_node(2), volume_node(3)]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let one_voter = BTreeSet::from([Uuid::from_u128(1)]);
    let foreign_voter = BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(4)]);

    assert!(validate_replacement_rollback_voters(&copies, &one_voter).is_err());
    assert!(validate_replacement_rollback_voters(&copies, &foreign_voter).is_err());
}

/// Replacement accepts every membership shape produced by safe rollback and promotion.
#[test]
fn replacement_target_accepts_reachable_membership_hints() {
    let target = Uuid::from_u128(4);
    let degraded = BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2)]);
    let stable = [Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]
        .into_iter()
        .collect();
    let promoted = BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), target]);
    let joint = [
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        Uuid::from_u128(3),
        target,
    ]
    .into_iter()
    .collect();
    let unrelated_four = [
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        Uuid::from_u128(3),
        Uuid::from_u128(5),
    ]
    .into_iter()
    .collect();

    assert!(replacement_voter_hint_is_valid(&degraded, target));
    assert!(replacement_voter_hint_is_valid(&stable, target));
    assert!(replacement_voter_hint_is_valid(&promoted, target));
    assert!(replacement_voter_hint_is_valid(&joint, target));
    assert!(!replacement_voter_hint_is_valid(&unrelated_four, target));
    assert!(!replacement_voter_hint_is_valid(
        &BTreeSet::from([Uuid::from_u128(1), target]),
        target
    ));
    assert!(!replacement_voter_hint_is_valid(
        &BTreeSet::from([Uuid::from_u128(1)]),
        target
    ));
}
