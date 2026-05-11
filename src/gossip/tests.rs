use super::dedupe::GossipDedupeState;
use super::message::Message;
use super::outbound::{coalesce_pending_messages, fanout_sample};
use super::plane::should_relay_inbound_message;
use super::service::message_for_forwarding;
use crate::cluster::{ClusterId, ClusterViewId};
use crate::network::types::NetworkEvent;
use crate::topology::PeerHandle;
use crate::topology::TopologyEvent;
use crate::topology::peer_provider::PeerProvider;
use crate::workload::model::{WorkloadAdmissionState, WorkloadEvent, WorkloadPhase, WorkloadSpec};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use std::collections::HashSet;
use uuid::Uuid;
use x25519_dalek::PublicKey;

#[derive(Clone)]
struct StaticPeerProvider {
    peers: Vec<PeerHandle>,
}

#[async_trait(?Send)]
impl PeerProvider for StaticPeerProvider {
    /// Returns one fixed peer list for deterministic fanout sampling tests.
    async fn get_peers(&self) -> Vec<PeerHandle> {
        self.peers.clone()
    }
}

/// Duplicate message ids should be rejected while the active view is unchanged.
#[test]
fn dedupe_state_rejects_duplicate_in_same_view() {
    let view = ClusterViewId::legacy_default();
    let id = Uuid::new_v4();
    let mut dedupe = GossipDedupeState::new(view);

    assert!(dedupe.record_inbound(view, id));
    assert!(!dedupe.record_inbound(view, id));
}

/// Outbound ids should be pre-registered so echoed inbound copies are dropped.
#[test]
fn dedupe_state_preseeds_outbound_ids() {
    let view = ClusterViewId::legacy_default();
    let id = Uuid::new_v4();
    let mut dedupe = GossipDedupeState::new(view);

    dedupe.record_outbound(view, id);
    assert!(!dedupe.record_inbound(view, id));
}

/// Switching active views should rotate the cache and accept ids again in the new view.
#[test]
fn dedupe_state_rotates_on_view_change() {
    let legacy_view = ClusterViewId::legacy_default();
    let next_view = ClusterViewId::new(ClusterId::from_uuid(Uuid::new_v4()), 1);
    let id = Uuid::new_v4();
    let mut dedupe = GossipDedupeState::new(legacy_view);

    assert!(dedupe.record_inbound(legacy_view, id));
    assert!(!dedupe.record_inbound(legacy_view, id));
    assert!(dedupe.record_inbound(next_view, id));
}

/// Rotating fanout sampling should cover all peers over successive ticks.
#[tokio::test]
async fn fanout_sample_rotates_across_population() {
    let mut expected = HashSet::new();
    let mut peers = Vec::new();
    for idx in 0..5 {
        let id = Uuid::new_v4();
        expected.insert(id);
        peers.push(PeerHandle {
            id,
            hostname: format!("peer-{idx}"),
            address: format!("127.0.0.1:{}", 5000 + idx),
            root_hash: String::new(),
            noise_static_pub: PublicKey::from([idx as u8; 32]),
        });
    }
    let provider = StaticPeerProvider { peers };

    let mut cursor = 0usize;
    let mut seen = HashSet::new();
    for _ in 0..3 {
        let selected = fanout_sample(&provider, 2, &mut cursor).await;
        for peer in selected {
            seen.insert(peer.id);
        }
    }

    assert_eq!(seen, expected);
}

/// Relayed topology join events should never carry imported server capabilities.
#[test]
fn message_for_forwarding_strips_join_client_capability() {
    let message = Message::Topology {
        id: Uuid::new_v4(),
        event: TopologyEvent::Join {
            id: Uuid::new_v4(),
            hostname: "peer-a".to_string(),
            address: "127.0.0.1:1234".to_string(),
            platform_os: "linux".to_string(),
            platform_arch: "amd64".to_string(),
            root_hash: String::new(),
            incarnation: 1,
            client: None,
            noise_static_pub: PublicKey::from([7u8; 32]),
            signing_pub: Box::new(
                ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]).verifying_key(),
            ),
            identity_sig: vec![0u8; 64],
            wireguard: None,
            scheduling: Box::new(
                crate::topology::peers::PeerSchedulingState::schedulable_default(Uuid::new_v4()),
            ),
            readiness: Box::new(crate::topology::peers::NodeReadiness::default()),
            labels: Box::new(crate::topology::peers::PeerLabelState::default()),
            runtime_support: Box::new(crate::runtime::types::RuntimeSupportProfile::default()),
            root_schema: crate::cluster::RootSchemaInfo::default(),
        },
    };

    let forwarded = message_for_forwarding(&message).expect("forwarded message");
    match forwarded {
        Message::Topology {
            event: TopologyEvent::Join { client, .. },
            ..
        } => assert!(client.is_none()),
        _ => panic!("unexpected forwarded message variant"),
    }
}

/// Network metadata is low-volume and should relay epidemically for fast peer convergence.
#[test]
fn network_gossip_relays_without_generic_relay_flag() {
    let message = Message::Network {
        id: Uuid::new_v4(),
        event: NetworkEvent::PeerRemove(Uuid::new_v4()),
    };

    assert!(should_relay_inbound_message(false, &message));
}

/// Task coalescing should keep the causally newest upsert and drop stale updates.
#[test]
fn coalesce_pending_messages_keeps_newest_task_upsert() {
    let task_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let now = Utc::now();

    let newer = WorkloadSpec {
        id: task_id,
        name: "task".to_string(),
        image: "img".to_string(),
        execution_platform: crate::workload::model::ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: now.to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id,
        node_name: "node".to_string(),
        slot_ids: vec![1],
        slot_id: Some(1),
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        ports: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        admission_group_id: None,
        admission_state: WorkloadAdmissionState::None,
        task_epoch: 4,
        phase_version: 7,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };

    let stale = WorkloadSpec {
        id: task_id,
        name: "task".to_string(),
        image: "img".to_string(),
        execution_platform: crate::workload::model::ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: (now + ChronoDuration::seconds(60)).to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id,
        node_name: "node".to_string(),
        slot_ids: vec![1],
        slot_id: Some(1),
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        ports: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        admission_group_id: None,
        admission_state: WorkloadAdmissionState::None,
        task_epoch: 4,
        phase_version: 6,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };

    let pending = vec![
        Message::Workload {
            id: Uuid::new_v4(),
            event: WorkloadEvent::UpsertSpec(Box::new(newer.clone())),
        },
        Message::Workload {
            id: Uuid::new_v4(),
            event: WorkloadEvent::UpsertSpec(Box::new(stale)),
        },
    ];

    let (coalesced, dropped) = coalesce_pending_messages(pending);
    assert_eq!(dropped, 1);
    assert_eq!(coalesced.len(), 1);
    match &coalesced[0] {
        Message::Workload {
            event: WorkloadEvent::UpsertSpec(spec),
            ..
        } => {
            assert_eq!(spec.state, WorkloadPhase::Running);
            assert_eq!(spec.phase_version, 7);
        }
        _ => panic!("unexpected coalesced message variant"),
    }
}

/// Task coalescing should keep one remove event for a task and drop intermediate upserts.
#[test]
fn coalesce_pending_messages_prefers_task_remove() {
    let task_id = Uuid::new_v4();
    let now = Utc::now();
    let upsert = WorkloadSpec {
        id: task_id,
        name: "task".to_string(),
        image: "img".to_string(),
        execution_platform: crate::workload::model::ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Stopping,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: now.to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: Uuid::new_v4(),
        node_name: "node".to_string(),
        slot_ids: vec![1],
        slot_id: Some(1),
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        ports: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        admission_group_id: None,
        admission_state: WorkloadAdmissionState::None,
        task_epoch: 2,
        phase_version: 9,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };

    let pending = vec![
        Message::Workload {
            id: Uuid::new_v4(),
            event: WorkloadEvent::UpsertSpec(Box::new(upsert)),
        },
        Message::Workload {
            id: Uuid::new_v4(),
            event: WorkloadEvent::Remove { id: task_id },
        },
    ];

    let (coalesced, dropped) = coalesce_pending_messages(pending);
    assert_eq!(dropped, 1);
    assert_eq!(coalesced.len(), 1);
    assert!(matches!(
        coalesced[0],
        Message::Workload {
            event: WorkloadEvent::Remove { .. },
            ..
        }
    ));
}

/// Burst workload lifecycle chatter should collapse to one causally newest workload update.
#[test]
fn coalesce_pending_messages_collapses_many_task_phase_updates() {
    let task_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let now = Utc::now();
    let mut pending = Vec::new();

    for phase_version in 1..=16u64 {
        let state = if phase_version >= 16 {
            WorkloadPhase::Running
        } else if phase_version >= 11 {
            WorkloadPhase::Creating
        } else if phase_version >= 6 {
            WorkloadPhase::Pulling
        } else {
            WorkloadPhase::Pending
        };

        let spec = WorkloadSpec {
            id: task_id,
            name: "task".to_string(),
            image: "img".to_string(),
            execution_platform: crate::workload::model::ExecutionPlatform::Oci,
            isolation_mode: crate::workload::model::IsolationMode::Standard,
            isolation_profile: None,
            state,
            phase_reason: None,
            phase_progress: None,
            created_at: now.to_rfc3339(),
            updated_at: (now + ChronoDuration::seconds(phase_version as i64)).to_rfc3339(),
            command: Vec::new(),
            tty: false,
            node_id,
            node_name: "node".to_string(),
            slot_ids: vec![1],
            slot_id: Some(1),
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            ports: Vec::new(),
            owner: None,
            lease_id: None,
            lease_coordinator_node_id: None,
            admission_group_id: None,
            admission_state: WorkloadAdmissionState::None,
            task_epoch: 2,
            phase_version,
            launch_attempt: 0,
            last_terminal_observed_launch: None,
        };
        pending.push(Message::Workload {
            id: Uuid::new_v4(),
            event: WorkloadEvent::UpsertSpec(Box::new(spec)),
        });
    }

    pending.push(Message::Void { id: Uuid::new_v4() });

    let (coalesced, dropped) = coalesce_pending_messages(pending);
    assert_eq!(dropped, 15);
    assert_eq!(coalesced.len(), 2);

    let newest_task = coalesced
        .iter()
        .find_map(|message| match message {
            Message::Workload {
                event: WorkloadEvent::UpsertSpec(spec),
                ..
            } => Some(spec),
            _ => None,
        })
        .expect("coalesced batch should keep one task upsert");

    assert_eq!(newest_task.phase_version, 16);
    assert_eq!(newest_task.state, WorkloadPhase::Running);
}
