use std::collections::BTreeSet;
use std::sync::Arc;

use futures::poll;
use mantissa_raft::ApplyContext;
use uuid::Uuid;

use super::{
    AppliedVolumeStatePublicationError, AppliedVolumeStateRegistry, AppliedVolumeStateRemovalError,
    FenceAdmission, IoAdmissionError, IoAdmissionRequest, IoRequestKind,
};
use crate::catalog::ReplicaKey;
use crate::control_state::{
    BeginReplicaReplacement, BeginVolumeRecovery, ExpectedVolumeRevision, FenceVolumeWriter,
    GrantVolumeWriter, InitializeVolume, RecoveryGrant, ReplacementGrant, VolumeCommand,
    VolumeControlState, WriterGrant,
};
use crate::{
    DriverSessionId, RecoveryId, ReplacementId, VolumeBlockSizes, VolumeDescriptor,
    VolumeGeneration, VolumeId, VolumeNodeId,
};

/// Returns one deterministic node identity.
fn node(value: u128) -> VolumeNodeId {
    VolumeNodeId::new(Uuid::from_u128(value)).expect("test node ID must be valid")
}

/// Returns the generation shared by admission tests.
fn generation() -> VolumeGeneration {
    VolumeGeneration::new(7).expect("test generation must be valid")
}

/// Returns one fixed valid descriptor.
fn descriptor() -> VolumeDescriptor {
    VolumeDescriptor::new(
        VolumeId::new(Uuid::from_u128(10)).expect("test volume ID must be valid"),
        generation(),
        64 << 20,
        VolumeBlockSizes::supported(),
    )
    .expect("test descriptor must be valid")
}

/// Returns the initial active-copy set.
fn copies() -> BTreeSet<VolumeNodeId> {
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

/// Returns pristine initialized control state.
fn initialized() -> VolumeControlState {
    VolumeControlState::default()
        .evaluate(&VolumeCommand::Initialize(InitializeVolume {
            descriptor: descriptor(),
            initial_copies: copies(),
        }))
        .state
}

/// Returns compare-and-set identity for the supplied state.
fn expected(state: &VolumeControlState) -> ExpectedVolumeRevision {
    ExpectedVolumeRevision {
        generation: generation(),
        revision: state.revision(),
    }
}

/// Returns control state with one exact foreground writer.
fn attached() -> (VolumeControlState, WriterGrant) {
    let state = initialized();
    let writer = WriterGrant {
        node_id: node(1),
        session_id: session(1),
    };
    let state = state
        .evaluate(&VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: expected(&state),
            writer,
        }))
        .state;
    (state, writer)
}

/// Builds one foreground request for current writer grant.
fn foreground_request<'a>(
    descriptor: &'a VolumeDescriptor,
    state: &VolumeControlState,
    writer: WriterGrant,
) -> IoAdmissionRequest<'a> {
    IoAdmissionRequest {
        descriptor,
        fence: state
            .data()
            .expect("initialized state must have data")
            .fence,
        session_id: writer.session_id,
        authenticated_peer: writer.node_id,
        kind: IoRequestKind::Foreground,
    }
}

#[test]
fn registry_rejects_stale_conflicting_and_regressing_publication() {
    let registry = AppliedVolumeStateRegistry::new();
    let initialized = initialized();
    let cell = registry
        .publish(ApplyContext::new(1, 1), &initialized)
        .expect("initialized control state must publish")
        .expect("initialized state must produce a cell");
    assert_eq!(registry.len(), 1);
    assert_eq!(cell.load().revision, 1);
    let same = registry
        .publish(ApplyContext::new(1, 1), &initialized)
        .expect("exact publication retry must succeed")
        .expect("exact publication retry must return its cell");
    assert!(Arc::ptr_eq(&cell, &same));

    let (attached, _) = attached();
    registry
        .publish(ApplyContext::new(1, 2), &attached)
        .expect("newer control state must publish");
    assert_eq!(cell.load().revision, 2);
    assert!(matches!(
        registry.publish(ApplyContext::new(1, 1), &initialized),
        Err(AppliedVolumeStatePublicationError::StaleAppliedIndex {
            current: 2,
            proposed: 1,
        })
    ));
    assert!(matches!(
        registry.publish(ApplyContext::new(1, 2), &initialized),
        Err(AppliedVolumeStatePublicationError::ConflictingAppliedEntry { index: 2 })
    ));
    assert!(matches!(
        registry.publish(ApplyContext::new(1, 3), &initialized),
        Err(AppliedVolumeStatePublicationError::ControlStateRegressed)
    ));
}

#[test]
fn applied_state_reconstructs_the_exact_bounded_state() {
    let registry = AppliedVolumeStateRegistry::new();
    let (attached, _) = attached();
    let cell = registry
        .publish(ApplyContext::new(2, 9), &attached)
        .expect("attached control state must publish")
        .expect("attached control state must produce a cell");

    assert_eq!(
        attached,
        cell.load()
            .control_state()
            .expect("published control state must remain valid")
    );
}

#[tokio::test]
async fn applied_state_publication_rejects_an_old_session_and_drains_admitted_work() {
    let registry = AppliedVolumeStateRegistry::new();
    let (attached, writer) = attached();
    let cell = registry
        .publish(ApplyContext::new(1, 2), &attached)
        .expect("attached control state must publish")
        .expect("attached control state must produce a cell");
    let gate = FenceAdmission::new(cell, node(1));
    gate.enable_for(2).expect("current active copy must enable");
    let descriptor = descriptor();
    let request = foreground_request(&descriptor, &attached, writer);
    let permit = gate
        .admit(&request)
        .expect("current writer request must be admitted");
    let worker_permit = permit.clone();

    let fenced = attached
        .evaluate(&VolumeCommand::FenceWriter(FenceVolumeWriter {
            expected: expected(&attached),
            writer,
        }))
        .state;
    registry
        .publish(ApplyContext::new(1, 3), &fenced)
        .expect("new fence must publish");
    assert!(matches!(
        gate.admit(&request),
        Err(IoAdmissionError::StaleFence { .. })
    ));

    let next_fence = fenced.data().expect("fenced state must have data").fence;
    {
        let wait = gate.wait_for_older_than(next_fence);
        tokio::pin!(wait);
        assert!(poll!(wait.as_mut()).is_pending());
    }
    assert_eq!(gate.in_flight().get(&request.fence), Some(&1));

    let retry = gate.wait_for_older_than(next_fence);
    tokio::pin!(retry);
    assert!(poll!(retry.as_mut()).is_pending());
    drop(permit);
    assert_eq!(gate.in_flight().get(&request.fence), Some(&1));
    drop(worker_permit);
    retry.await;
    assert!(gate.in_flight().is_empty());
}

/// A cancelled install wait retains old work and a retry observes its drain.
#[tokio::test]
async fn fence_install_wait_is_retryable_and_rechecks_applied_state() {
    let registry = AppliedVolumeStateRegistry::new();
    let (attached, writer) = attached();
    let cell = registry
        .publish(ApplyContext::new(1, 2), &attached)
        .expect("attached control state must publish")
        .expect("attached control state must produce a cell");
    let gate = FenceAdmission::new(cell, node(1));
    gate.enable_for(2).expect("current active copy must enable");
    let descriptor = descriptor();
    let old_request = foreground_request(&descriptor, &attached, writer);
    let old_permit = gate
        .admit(&old_request)
        .expect("old writer request must be admitted");

    let fenced = attached
        .evaluate(&VolumeCommand::FenceWriter(FenceVolumeWriter {
            expected: expected(&attached),
            writer,
        }))
        .state;
    let next_writer = WriterGrant {
        node_id: node(2),
        session_id: session(2),
    };
    let granted = fenced
        .evaluate(&VolumeCommand::GrantWriter(GrantVolumeWriter {
            expected: expected(&fenced),
            writer: next_writer,
        }))
        .state;
    registry
        .publish(ApplyContext::new(1, 4), &granted)
        .expect("new writer grant must publish");
    let new_fence = granted.data().expect("granted state must have data").fence;

    let mut cancelled = Box::pin(gate.prepare_fence_install(new_fence));
    assert!(poll!(cancelled.as_mut()).is_pending());
    drop(cancelled);
    let mut retry = Box::pin(gate.prepare_fence_install(new_fence));
    assert!(poll!(retry.as_mut()).is_pending());
    drop(old_permit);
    drop(
        retry
            .await
            .expect("install retry must observe old work drain"),
    );

    let superseded = granted
        .evaluate(&VolumeCommand::FenceWriter(FenceVolumeWriter {
            expected: expected(&granted),
            writer: next_writer,
        }))
        .state;
    registry
        .publish(ApplyContext::new(1, 5), &superseded)
        .expect("later fence must publish");
    assert!(matches!(
        gate.prepare_fence_install(new_fence).await,
        Err(IoAdmissionError::StaleFence { .. })
    ));
}

#[test]
fn local_disable_is_immediate_and_does_not_drop_existing_permits() {
    let registry = AppliedVolumeStateRegistry::new();
    let (attached, writer) = attached();
    let cell = registry
        .publish(ApplyContext::new(1, 2), &attached)
        .expect("attached control state must publish")
        .expect("attached control state must produce a cell");
    let gate = FenceAdmission::new(cell, node(1));
    gate.enable_for(2).expect("current active copy must enable");
    let descriptor = descriptor();
    let request = foreground_request(&descriptor, &attached, writer);
    let permit = gate
        .admit(&request)
        .expect("current writer request must be admitted");
    gate.disable();
    assert_eq!(
        gate.admit(&request).err(),
        Some(IoAdmissionError::LocallyDisabled)
    );
    assert_eq!(gate.in_flight().get(&request.fence), Some(&1));
    drop(permit);
    assert!(gate.in_flight().is_empty());
}

#[test]
fn recovery_and_replacement_require_exact_coordinator_and_local_role() {
    let initialized = initialized();
    let recovery = RecoveryGrant {
        id: recovery_id(1),
        coordinator_node_id: node(1),
        source_node_id: node(1),
        target_node_ids: [node(1), node(2)].into_iter().collect(),
    };
    let recovering = initialized
        .evaluate(&VolumeCommand::BeginRecovery(BeginVolumeRecovery {
            expected: expected(&initialized),
            expected_writer: None,
            replaced_recovery_id: None,
            recovery: recovery.clone(),
        }))
        .state;
    let registry = AppliedVolumeStateRegistry::new();
    let source_cell = registry
        .publish(ApplyContext::new(1, 2), &recovering)
        .expect("recovery grant must publish")
        .expect("recovery grant must produce a cell");
    let source_gate = FenceAdmission::new(source_cell, node(1));
    source_gate
        .enable_for(2)
        .expect("recovery source must enable");
    let descriptor = descriptor();
    let source_request = IoAdmissionRequest {
        descriptor: &descriptor,
        fence: recovering
            .data()
            .expect("recovering state must have data")
            .fence,
        session_id: session(9),
        authenticated_peer: recovery.coordinator_node_id,
        kind: IoRequestKind::RecoverySource(recovery.id),
    };
    drop(
        source_gate
            .admit(&source_request)
            .expect("exact recovery source request must be admitted"),
    );
    let wrong_peer = IoAdmissionRequest {
        authenticated_peer: node(3),
        ..source_request.clone()
    };
    assert_eq!(
        source_gate.admit(&wrong_peer).err(),
        Some(IoAdmissionError::Unauthorized)
    );

    let replacement = ReplacementGrant {
        id: replacement_id(1),
        coordinator_node_id: node(1),
        old_node_id: Some(node(3)),
        new_node_id: node(4),
        source_node_id: node(1),
    };
    let replacing = initialized
        .evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
            expected: expected(&initialized),
            replacement,
        }))
        .state;
    let replacement_registry = AppliedVolumeStateRegistry::new();
    let target_cell = replacement_registry
        .publish(ApplyContext::new(1, 2), &replacing)
        .expect("replacement grant must publish")
        .expect("replacement grant must produce a cell");
    let target_gate = FenceAdmission::new(target_cell, replacement.new_node_id);
    target_gate
        .enable_for(2)
        .expect("authorized replacement target must enable");
    let target_request = IoAdmissionRequest {
        descriptor: &descriptor,
        fence: replacing
            .data()
            .expect("replacing state must have data")
            .fence,
        session_id: session(10),
        authenticated_peer: replacement.coordinator_node_id,
        kind: IoRequestKind::ReplacementTarget(replacement.id),
    };
    drop(
        target_gate
            .admit(&target_request)
            .expect("exact replacement target request must be admitted"),
    );
    let foreground = IoAdmissionRequest {
        kind: IoRequestKind::Foreground,
        ..target_request
    };
    assert_eq!(
        target_gate.admit(&foreground).err(),
        Some(IoAdmissionError::Unauthorized)
    );
}

#[test]
fn registry_lookup_uses_the_exact_generation_key() {
    let registry = AppliedVolumeStateRegistry::new();
    let initialized = initialized();
    let published = registry
        .publish(ApplyContext::new(1, 1), &initialized)
        .expect("initialized control state must publish")
        .expect("initialized control state must produce a cell");
    let key = ReplicaKey::from(&descriptor());
    let loaded = registry.cell(key).expect("published key must be found");
    assert!(Arc::ptr_eq(&published, &loaded));
    let other = ReplicaKey::new(
        key.volume_id(),
        VolumeGeneration::new(generation().get() + 1)
            .expect("different test generation must be valid"),
    );
    assert!(registry.cell(other).is_none());
}

#[test]
fn local_revocation_retires_the_gate_and_waits_for_admitted_work() {
    let registry = AppliedVolumeStateRegistry::new();
    let (attached, writer) = attached();
    let cell = registry
        .publish(ApplyContext::new(1, 2), &attached)
        .expect("attached control state must publish")
        .expect("attached control state must produce a cell");
    let gate = FenceAdmission::new(cell, node(1));
    gate.enable_for(2).expect("current active copy must enable");
    let descriptor = descriptor();
    let request = foreground_request(&descriptor, &attached, writer);
    let permit = gate
        .admit(&request)
        .expect("current request must be admitted");
    let key = ReplicaKey::from(&descriptor);
    assert_eq!(
        registry.remove_locally_revoked(key, &gate),
        Err(AppliedVolumeStateRemovalError::RequestsInFlight)
    );
    assert_eq!(gate.enable_for(2), Err(IoAdmissionError::Retired));
    drop(permit);
    registry
        .remove_locally_revoked(key, &gate)
        .expect("retired drained control state may be removed");
    assert!(registry.is_empty());
}
