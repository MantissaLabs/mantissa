pub(crate) use crate::common::convergence::{
    current_cluster_view, swim_down_transition_timeout, wait_for_cluster_view,
    wait_for_operation_stage, wait_until,
};
pub(crate) use crate::common::testkit::{
    ClusterConfig, InMemoryRuntimeBackend, RuntimeBackendOverrideGuard, TestNode,
};
pub(crate) use async_trait::async_trait;
pub(crate) use capnp::Error as CapnpError;
pub(crate) use chrono::{DateTime, Duration as ChronoDuration, Utc};
pub(crate) use mantissa::cluster::ClusterViewId;
pub(crate) use mantissa::config::{
    Config, ConfigSource, global_config, global_config_source, set_global_config_with_source,
};
pub(crate) use mantissa::network::types::{
    NetworkAttachmentState, NetworkAttachmentValue, NetworkDriver, NetworkSpecDraft,
    NetworkSpecValue, NetworkStatus,
};
pub(crate) use mantissa::node::id::set_node_id;
pub(crate) use mantissa::runtime::set::RuntimeSet;
pub(crate) use mantissa::runtime::testing::IN_MEMORY_RUNTIME_BACKEND_KIND;
pub(crate) use mantissa::runtime::types::{
    RuntimeBackend, RuntimeCapabilities, RuntimeCreateRequest, RuntimeError, RuntimeEvent,
    RuntimeInfo,
};
pub(crate) use mantissa::scheduler::SlotReservationRequest;
pub(crate) use mantissa::scheduler::SlotState;
pub(crate) use mantissa::server::headless::{
    HeadlessConfig, HeadlessKeys, HeadlessNode, HeadlessTransport,
};
pub(crate) use mantissa::services::ServiceController;
pub(crate) use mantissa::services::manager::ServiceDeploymentOutcome;
pub(crate) use mantissa::services::types::{
    ServiceRollingUpdatePolicy, ServiceRolloutOrder, ServiceRolloutPhase, ServiceRolloutState,
    ServiceSpecValue, ServiceStatus, ServiceUpdateStrategy, TaskTemplateNetworkRequirement,
    TaskTemplateRestartPolicy, TaskTemplateRestartPolicyKind, TaskTemplateSpecValue,
};
pub(crate) use mantissa::task::types::{
    TaskEnvironmentVariable, TaskSecretFile, TaskSecretReference, TaskStateFilter, TaskValue,
    TaskVolumeMount,
};
pub(crate) use mantissa::topology_capnp::topology;
pub(crate) use mantissa::workload::manager::WorkloadManager;
pub(crate) use mantissa::workload::model::WorkloadPhase;
pub(crate) use mantissa::workload::model::WorkloadSpec;
pub(crate) use mantissa::workload::types::{
    ExecutionSpec, WorkloadPortBinding, WorkloadPortProtocol,
};
pub(crate) use mantissa_client::services::manifest::{
    ManifestPortProtocol, RestartPolicyName as ManifestRestartPolicyName, SecretReference,
    ServiceManifest, load_manifest_from_path,
};
pub(crate) use mantissa_protocol::health::NodeStatus;
pub(crate) use mantissa_protocol::secrets::secrets;
pub(crate) use mantissa_protocol::services::services;
pub(crate) use mantissa_protocol::topology::{ClusterOperationStage, NodeDrainState};
pub(crate) use mantissa_protocol::volumes::volumes;
pub(crate) use mantissa_store::uuid_key::UuidKey;
pub(crate) use parking_lot::{Mutex, MutexGuard};
pub(crate) use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
pub(crate) use tempfile::tempdir;
pub(crate) use tokio::sync::Mutex as AsyncMutex;
pub(crate) use tokio::time::sleep;
pub(crate) use uuid::Uuid;

pub(crate) use mantissa_net::noise::NoiseKeys;

/// Restores the global Mantissa config after a test-scoped override.
pub(crate) struct ConfigOverrideGuard {
    previous: Config,
    source: ConfigSource,
    _lock: MutexGuard<'static, ()>,
}

/// Returns the global mutex used to serialize test-scoped config overrides.
pub(crate) fn config_override_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

impl ConfigOverrideGuard {
    /// Force networking into a control-plane-only mode so logical-network tests do not depend on
    /// privileged dataplane setup in CI.
    pub(crate) fn control_plane_network_only() -> Self {
        let lock = config_override_lock().lock();
        let previous = global_config();
        let source = global_config_source();

        let mut config = previous.clone();
        config.network.provision_kernel_interfaces = false;
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = false;
        config.network.nodeport.enabled = false;

        let mut override_source = source.clone();
        override_source.env_overrides = true;
        set_global_config_with_source(config, override_source);

        Self {
            previous,
            source,
            _lock: lock,
        }
    }
}

impl Drop for ConfigOverrideGuard {
    fn drop(&mut self) {
        set_global_config_with_source(self.previous.clone(), self.source.clone());
    }
}

pub(crate) fn empty_service_execution(
    image: &str,
) -> ExecutionSpec<TaskTemplateNetworkRequirement> {
    ExecutionSpec {
        image: image.to_string(),
        command: Vec::new(),
        tty: false,
        cpu_millis: 0,
        memory_bytes: 0,
        gpu_count: 0,
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        ports: Vec::new(),
        placement: Default::default(),
    }
}

/// Builds a lightweight backend template used by placement-focused integration tests.
pub(crate) fn demo_backend_task_template(name: &str, replicas: u16) -> TaskTemplateSpecValue {
    TaskTemplateSpecValue {
        name: name.to_string(),
        execution: ExecutionSpec {
            command: vec![
                "-listen".to_string(),
                ":8000".to_string(),
                "-text".to_string(),
                "hello from backend replica".to_string(),
            ],
            cpu_millis: 200,
            memory_bytes: 64 * 1024 * 1024,
            ..empty_service_execution("hashicorp/http-echo:1.0.0")
        },
        depends_on: Vec::new(),
        replicas,
        readiness: None,
        public_port: None,
        public_protocol: None,
        placement_preferences: Vec::new(),
    }
}

/// Builds the same backend template but attaches it to one logical overlay network.
pub(crate) fn demo_networked_backend_task_template(
    name: &str,
    replicas: u16,
    network_id: Uuid,
) -> TaskTemplateSpecValue {
    let mut template = demo_backend_task_template(name, replicas);
    template.execution.networks = vec![TaskTemplateNetworkRequirement::new("default", network_id)];
    template
}

pub(crate) async fn create_logical_test_network(cluster: &[TestNode], name: &str) -> Uuid {
    let network = NetworkSpecValue::new(NetworkSpecDraft {
        name: name.to_string(),
        description: "split/merge attachment publication test network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.42.0.0/16".to_string(),
        vni: ((Uuid::new_v4().as_u128() % 16_000_000) as u32).max(1),
        mtu: 1450,
        sealed: false,
        bpf_programs: Vec::new(),
    });

    for node in cluster {
        node.node
            .network_registry
            .upsert_spec(network.clone())
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "upsert logical network {} on node {} failed: {err:#}",
                    network.id,
                    node.id()
                )
            });
        node.node
            .network_controller
            .schedule_spec_change(network.id)
            .await;
    }

    assert!(
        wait_for_logical_network_ready_all(cluster, network.id, Duration::from_secs(60)).await,
        "logical network {} should become ready on all nodes before deployment",
        network.id
    );
    network.id
}

/// Waits until every node reports the logical network as ready with controller-driven peer
/// readiness converged and stable.
pub(crate) async fn wait_for_logical_network_ready_all(
    cluster: &[TestNode],
    network_id: Uuid,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut stable_rounds = 0usize;

    while Instant::now() < deadline {
        let mut healthy = true;
        for node in cluster {
            let Ok(Some(spec)) = node.node.network_registry.get_spec(network_id) else {
                healthy = false;
                break;
            };
            if spec.status != NetworkStatus::Ready {
                healthy = false;
                break;
            }

            let Ok(peers) = node
                .node
                .network_registry
                .list_peer_states(Some(network_id))
            else {
                healthy = false;
                break;
            };
            if peers.len() != cluster.len() || peers.iter().any(|peer| !peer.state.is_ready()) {
                healthy = false;
                break;
            }
        }

        if healthy {
            stable_rounds = stable_rounds.saturating_add(1);
            if stable_rounds >= 3 {
                return true;
            }
        } else {
            stable_rounds = 0;
        }

        sleep(Duration::from_millis(200)).await;
    }

    false
}

/// Waits until every node can reuse a cached cluster session for every other peer.
///
/// The networked split/merge publication regression deploys immediately after cluster bootstrap.
/// Pairwise membership convergence is not enough for that path: the initial batch launcher also
/// needs control-plane sessions to be cached so remote scheduler and task RPCs do not fail
/// transiently on slow CI workers.
pub(crate) async fn wait_for_cached_cluster_sessions_all(
    cluster: &[TestNode],
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut stable_rounds = 0usize;

    while Instant::now() < deadline {
        let mut ready = true;

        for node in cluster {
            if node.node.registry.connect_known_peers(true).await.is_err() {
                ready = false;
                break;
            }

            for peer in cluster {
                if node.id() == peer.id() {
                    continue;
                }

                if node
                    .node
                    .registry
                    .cached_session_for(peer.id())
                    .await
                    .is_none()
                {
                    ready = false;
                    break;
                }
            }

            if !ready {
                break;
            }
        }

        if ready {
            stable_rounds = stable_rounds.saturating_add(1);
            if stable_rounds >= 3 {
                return true;
            }
        } else {
            stable_rounds = 0;
        }

        sleep(Duration::from_millis(200)).await;
    }

    false
}

/// Returns true once every node sees the expected active service tasks with published attachments.
pub(crate) async fn wait_for_visible_service_attachments_published_refs(
    nodes: &[&TestNode],
    service_name: &str,
    network_id: Uuid,
    expected_task_count: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut stable_rounds = 0usize;
    let mut previous: Option<Vec<Vec<(Uuid, Uuid)>>> = None;

    while Instant::now() < deadline {
        let mut snapshot = Vec::with_capacity(nodes.len());
        let mut healthy = true;

        for node in nodes {
            let Some(node_snapshot) = collect_published_service_attachment_snapshot(
                node,
                service_name,
                network_id,
                expected_task_count,
            )
            .await
            else {
                healthy = false;
                break;
            };
            snapshot.push(node_snapshot);
        }

        if healthy && previous.as_ref() == Some(&snapshot) {
            stable_rounds = stable_rounds.saturating_add(1);
            if stable_rounds >= 3 {
                return true;
            }
        } else if healthy {
            stable_rounds = 1;
        } else {
            stable_rounds = 0;
        }

        previous = Some(snapshot);
        sleep(Duration::from_millis(200)).await;
    }

    false
}

/// Returns true when every node keeps at least `min_visible` published service attachments
/// visible for the full observation window.
pub(crate) async fn visible_service_attachment_presence_refs(
    nodes: &[&TestNode],
    service_name: &str,
    network_id: Uuid,
    min_visible: usize,
    window: Duration,
) -> bool {
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        for node in nodes {
            let visible =
                count_visible_published_service_attachments(node, service_name, network_id).await;
            if visible < min_visible {
                return false;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    true
}

/// Collects one node snapshot once every visible service task is running with a published attachment.
pub(crate) async fn collect_published_service_attachment_snapshot(
    node: &TestNode,
    service_name: &str,
    network_id: Uuid,
    expected_task_count: usize,
) -> Option<Vec<(Uuid, Uuid)>> {
    let mut tasks = list_active_service_tasks(&node.node.workload_manager, service_name).await;
    if tasks.len() != expected_task_count {
        return None;
    }
    tasks.sort_by_key(|task| task.id);

    let Ok(attachments) = node
        .node
        .network_registry
        .list_attachments(Some(network_id))
    else {
        return None;
    };

    let mut by_task = HashMap::with_capacity(attachments.len());
    for attachment in attachments {
        by_task.entry(attachment.task_id).or_insert(attachment);
    }

    let mut snapshot = Vec::with_capacity(expected_task_count);
    for task in tasks {
        if !matches!(task.state, WorkloadPhase::Running) {
            return None;
        }

        let attachment = by_task.get(&task.id)?;

        if attachment.node_id != task.node_id
            || attachment.state != NetworkAttachmentState::Ready
            || !attachment.traffic_published
        {
            return None;
        }

        snapshot.push((task.id, task.node_id));
    }

    Some(snapshot)
}

/// Counts visible running service tasks that currently have ready, published attachments.
pub(crate) async fn count_visible_published_service_attachments(
    node: &TestNode,
    service_name: &str,
    network_id: Uuid,
) -> usize {
    let tasks = list_active_service_tasks(&node.node.workload_manager, service_name).await;
    let attachments = node
        .node
        .network_registry
        .list_attachments(Some(network_id))
        .unwrap_or_default();

    let mut by_task = HashMap::with_capacity(attachments.len());
    for attachment in attachments {
        by_task.entry(attachment.task_id).or_insert(attachment);
    }

    tasks
        .into_iter()
        .filter(|task| matches!(task.state, WorkloadPhase::Running))
        .filter(|task| {
            by_task.get(&task.id).is_some_and(|attachment| {
                attachment.node_id == task.node_id
                    && attachment.state == NetworkAttachmentState::Ready
                    && attachment.traffic_published
            })
        })
        .count()
}

/// Collects one per-node debug snapshot for attachment publication assertions.
pub(crate) async fn collect_service_attachment_publication_debug(
    nodes: &[&TestNode],
    service_name: &str,
    network_id: Uuid,
) -> String {
    let mut out = Vec::with_capacity(nodes.len());
    for node in nodes {
        let tasks = list_active_service_tasks(&node.node.workload_manager, service_name).await;
        let service_status =
            node.node
                .service_controller
                .list_services()
                .ok()
                .and_then(|services| {
                    services
                        .into_iter()
                        .find(|spec| spec.service_name == service_name)
                        .map(|spec| spec.status())
                });
        let attachments = node
            .node
            .network_registry
            .list_attachments(Some(network_id))
            .unwrap_or_default();
        out.push(debug_service_attachment_publication_state(
            node,
            service_name,
            service_status,
            &tasks,
            &attachments,
        ));
    }
    out.join(" | ")
}

/// Renders one concise debug snapshot for the traffic publication helper.
pub(crate) fn debug_service_attachment_publication_state(
    node: &TestNode,
    service_name: &str,
    service_status: Option<ServiceStatus>,
    tasks: &[WorkloadSpec],
    attachments: &[NetworkAttachmentValue],
) -> String {
    let attachment_summary = attachments
        .iter()
        .map(|attachment| {
            format!(
                "{}:{}:{:?}:{}",
                &attachment.task_id.to_string()[..8],
                &attachment.node_id.to_string()[..8],
                attachment.state,
                if attachment.traffic_published {
                    "pub"
                } else {
                    "hidden"
                }
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let task_summary = tasks
        .iter()
        .map(|task| {
            format!(
                "{}:{}:{:?}",
                &task.id.to_string()[..8],
                &task.node_id.to_string()[..8],
                task.state
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "node={} service={} status={:?} tasks=[{}] attachments=[{}]",
        node.id(),
        service_name,
        service_status,
        task_summary,
        attachment_summary
    )
}

/// Lists active tasks that belong to one service according to service metadata.
pub(crate) async fn list_active_service_tasks(
    manager: &WorkloadManager,
    service_name: &str,
) -> Vec<WorkloadSpec> {
    let filter = TaskStateFilter::active_only();
    manager
        .list_workloads(&filter)
        .await
        .expect("list active tasks during service placement checks")
        .into_iter()
        .filter(|task| {
            task.service_owner()
                .map(|meta| meta.service_name == service_name)
                .unwrap_or(false)
        })
        .collect()
}

/// Returns true once every surviving node stops listing active service tasks on the failed node.
pub(crate) async fn surviving_nodes_observe_no_active_service_tasks_on_node(
    cluster: &[TestNode],
    service_name: &str,
    down_node_id: Uuid,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        for node in cluster {
            if node.id() == down_node_id {
                continue;
            }
            let tasks = list_active_service_tasks(&node.node.workload_manager, service_name).await;
            if tasks.iter().any(|task| task.node_id == down_node_id) {
                return false;
            }
        }
        true
    })
    .await
}

/// Lists active tasks for one task template within a service.
pub(crate) async fn list_active_task_template_tasks(
    manager: &WorkloadManager,
    service_name: &str,
    template_name: &str,
) -> Vec<WorkloadSpec> {
    list_active_service_tasks(manager, service_name)
        .await
        .into_iter()
        .filter(|task| {
            task.service_owner()
                .map(|meta| meta.template == template_name)
                .unwrap_or(false)
        })
        .collect()
}

/// Returns true when every replica of one task template is running with a ready, published
/// attachment on the provided logical network.
pub(crate) async fn template_attachments_published(
    node: &TestNode,
    service_name: &str,
    template_name: &str,
    network_id: Uuid,
    expected_task_count: usize,
) -> bool {
    let tasks =
        list_active_task_template_tasks(&node.node.workload_manager, service_name, template_name)
            .await;
    if tasks.len() != expected_task_count {
        return false;
    }

    let Ok(attachments) = node
        .node
        .network_registry
        .list_attachments(Some(network_id))
    else {
        return false;
    };
    let mut by_task = HashMap::with_capacity(attachments.len());
    for attachment in attachments {
        by_task.entry(attachment.task_id).or_insert(attachment);
    }

    tasks.iter().all(|task| {
        matches!(task.state, WorkloadPhase::Running)
            && by_task.get(&task.id).is_some_and(|attachment| {
                attachment.state == NetworkAttachmentState::Ready && attachment.traffic_published
            })
    })
}

/// Waits until the dependent template launches, failing if it appears before its dependency
/// template has published ready attachments.
pub(crate) async fn wait_for_template_launch_after_dependency_publication(
    node: &TestNode,
    service_name: &str,
    dependency_template: &str,
    dependency_task_count: usize,
    dependent_template: &str,
    network_id: Uuid,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        let dependency_ready = template_attachments_published(
            node,
            service_name,
            dependency_template,
            network_id,
            dependency_task_count,
        )
        .await;
        let dependent_tasks = list_active_task_template_tasks(
            &node.node.workload_manager,
            service_name,
            dependent_template,
        )
        .await;

        if !dependent_tasks.is_empty() && !dependency_ready {
            return false;
        }
        if dependency_ready && !dependent_tasks.is_empty() {
            return true;
        }

        sleep(Duration::from_millis(100)).await;
    }

    false
}

/// Waits until a dependent template replacement appears, failing if it starts before the full
/// upstream replacement stage is running with published attachments.
pub(crate) struct TemplateReplacementPublicationGate<'a> {
    pub(crate) dependency_template: &'a str,
    pub(crate) old_dependency_task_ids: &'a HashSet<Uuid>,
    pub(crate) dependency_task_count: usize,
    pub(crate) dependent_template: &'a str,
    pub(crate) old_dependent_task_ids: &'a HashSet<Uuid>,
    pub(crate) network_id: Uuid,
}

pub(crate) async fn wait_for_template_replacement_after_dependency_publication(
    node: &TestNode,
    service_name: &str,
    gate: &TemplateReplacementPublicationGate<'_>,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        let dependency_tasks = list_active_task_template_tasks(
            &node.node.workload_manager,
            service_name,
            gate.dependency_template,
        )
        .await;
        let dependency_replaced = dependency_tasks.len() == gate.dependency_task_count
            && dependency_tasks
                .iter()
                .all(|task| !gate.old_dependency_task_ids.contains(&task.id));
        let dependency_ready = dependency_replaced
            && template_attachments_published(
                node,
                service_name,
                gate.dependency_template,
                gate.network_id,
                gate.dependency_task_count,
            )
            .await;
        let dependent_tasks = list_active_task_template_tasks(
            &node.node.workload_manager,
            service_name,
            gate.dependent_template,
        )
        .await;
        let dependent_replaced = !dependent_tasks.is_empty()
            && dependent_tasks
                .iter()
                .all(|task| !gate.old_dependent_task_ids.contains(&task.id));

        if dependent_replaced && !dependency_ready {
            return false;
        }
        if dependency_ready && dependent_replaced {
            return true;
        }

        sleep(Duration::from_millis(100)).await;
    }

    false
}

/// Lists active tasks for one service that are assigned to a specific node id.
pub(crate) async fn list_local_active_service_tasks(
    manager: &WorkloadManager,
    service_name: &str,
    node_id: Uuid,
) -> Vec<WorkloadSpec> {
    list_active_service_tasks(manager, service_name)
        .await
        .into_iter()
        .filter(|task| task.node_id == node_id)
        .collect()
}

/// Returns true when every node reports the same active task count for a service.
pub(crate) async fn all_nodes_have_service_task_count(
    cluster: &[TestNode],
    service_name: &str,
    expected: usize,
) -> bool {
    for node in cluster {
        let count = list_active_service_tasks(&node.node.workload_manager, service_name)
            .await
            .len();
        if count != expected {
            return false;
        }
    }
    true
}

/// Waits until every node converges on the expected active task count for a service.
pub(crate) async fn wait_for_service_task_count_all(
    cluster: &[TestNode],
    service_name: &str,
    expected: usize,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        all_nodes_have_service_task_count(cluster, service_name, expected).await
    })
    .await
}

type ServiceTaskPlacementRow = (Uuid, Uuid, Vec<u64>, WorkloadPhase);
type ServiceTaskPlacementSnapshot = Vec<ServiceTaskPlacementRow>;

/// Waits until every node reports the same stable set of running tasks for the service.
pub(crate) async fn wait_for_service_running_tasks_stable_all(
    cluster: &[TestNode],
    service_name: &str,
    expected: usize,
    stable_rounds_required: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut stable_rounds = 0usize;
    let mut previous: Option<Vec<ServiceTaskPlacementSnapshot>> = None;

    while Instant::now() < deadline {
        let mut snapshot = Vec::with_capacity(cluster.len());
        let mut healthy = true;
        let mut canonical: Option<ServiceTaskPlacementSnapshot> = None;

        for node in cluster {
            let mut tasks =
                list_active_service_tasks(&node.node.workload_manager, service_name).await;
            tasks.sort_by_key(|task| task.id);
            if tasks.len() != expected
                || tasks
                    .iter()
                    .any(|task| !matches!(task.state, WorkloadPhase::Running))
            {
                healthy = false;
            }

            // Merge regressions can keep every node at the expected replica count while different
            // nodes still disagree on which owner/slot set wins for the same task id. Compare the
            // exact placement rows so this helper only succeeds once the whole cluster agrees.
            let task_rows: ServiceTaskPlacementSnapshot = tasks
                .into_iter()
                .map(|task| (task.id, task.node_id, task.slot_ids, task.state))
                .collect();
            if let Some(reference) = canonical.as_ref() {
                if reference != &task_rows {
                    healthy = false;
                }
            } else {
                canonical = Some(task_rows.clone());
            }

            snapshot.push(task_rows);
        }

        if healthy && previous.as_ref() == Some(&snapshot) {
            stable_rounds = stable_rounds.saturating_add(1);
            if stable_rounds >= stable_rounds_required {
                return true;
            }
        } else if healthy {
            stable_rounds = 1;
        } else {
            stable_rounds = 0;
        }

        previous = Some(snapshot);
        sleep(Duration::from_millis(200)).await;
    }

    false
}

/// Waits until each provided node owns at least `min_expected` active tasks for the service.
pub(crate) async fn wait_for_min_local_service_task_count_refs(
    cluster: &[&TestNode],
    service_name: &str,
    min_expected: usize,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        for node in cluster {
            let count = list_local_active_service_tasks(
                &node.node.workload_manager,
                service_name,
                node.id(),
            )
            .await
            .len();
            if count < min_expected {
                return false;
            }
        }
        true
    })
    .await
}

/// Creates one restartable headless node backed by the provided durable state and runtime.
pub(crate) async fn create_restartable_service_node(
    db: Arc<redb::Database>,
    self_id: Uuid,
    keys: HeadlessKeys,
    runtime_backend: Arc<dyn RuntimeBackend + Send + Sync>,
    local_volume_root: PathBuf,
) -> HeadlessNode {
    HeadlessNode::new_with(
        db,
        self_id,
        keys,
        HeadlessConfig {
            listen_addr: "127.0.0.1:0".to_string(),
            transport: HeadlessTransport::Inproc,
            root_schema_override: None,
            sync_tick: Some(Duration::from_millis(100)),
            sync_fanout: None,
            global_metadata_sync_tick: Some(Duration::from_millis(100)),
            global_metadata_sync_fanout: None,
            gossip_tick: Some(Duration::from_millis(100)),
            gossip_fanout: None,
            network_reconcile_tick: None,
            network_attachment_refresh_tick: None,
            gossip_channel_capacity: None,
            task_runtime: None,
            service_ready_stability: None,
            runtime_set: Some(RuntimeSet::singleton(
                IN_MEMORY_RUNTIME_BACKEND_KIND,
                runtime_backend,
            )),
            local_volume_root: Some(local_volume_root),
            master_key_kdf_params: None,
        },
    )
    .await
    .expect("start restartable service node")
}

/// Builds a rollout strategy used by redeploy integration tests.
pub(crate) fn rollout_strategy(
    parallelism: u16,
    order: ServiceRolloutOrder,
    monitor_secs: u32,
    max_failures: u16,
    auto_rollback: bool,
) -> ServiceUpdateStrategy {
    ServiceUpdateStrategy {
        rolling: ServiceRollingUpdatePolicy {
            parallelism,
            order,
            startup_timeout_secs: 600,
            monitor_secs,
            max_failures,
            auto_rollback,
        },
        ..ServiceUpdateStrategy::default()
    }
}

#[derive(Default)]
pub(crate) struct SlowCreateRuntimeBackend {
    inner: InMemoryRuntimeBackend,
}

#[async_trait]
impl RuntimeBackend for SlowCreateRuntimeBackend {
    async fn create_instance(&self, request: RuntimeCreateRequest) -> Result<String, RuntimeError> {
        sleep(Duration::from_secs(3)).await;
        self.inner.create_instance(request).await
    }

    async fn start_instance(&self, container_id: &str) -> Result<(), RuntimeError> {
        self.inner.start_instance(container_id).await
    }

    async fn stop_instance(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        self.inner.stop_instance(container_id, timeout).await
    }

    async fn restart_instance(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        self.inner.restart_instance(container_id, timeout).await
    }

    async fn remove_instance(
        &self,
        container_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> Result<(), RuntimeError> {
        self.inner
            .remove_instance(container_id, force, remove_volumes)
            .await
    }

    async fn list_instances(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> Result<Vec<RuntimeInfo>, RuntimeError> {
        self.inner.list_instances(filters).await
    }

    async fn inspect_instance(&self, container_id: &str) -> Result<RuntimeInfo, RuntimeError> {
        self.inner.inspect_instance(container_id).await
    }

    async fn pull_image(&self, _image: &str) -> Result<(), RuntimeError> {
        Ok(())
    }
}

/// Emits synthetic runtime exit events for started containers.
///
/// We use this manager to validate the runtime-event failure path deterministically
/// in tests, without depending on Docker timing or external process behavior.
#[derive(Default)]
pub(crate) struct ExitSignalRuntimeBackend {
    inner: InMemoryRuntimeBackend,
    task_ids_by_container: AsyncMutex<HashMap<String, Uuid>>,
    runtime_events_tx: AsyncMutex<Option<tokio::sync::mpsc::UnboundedSender<RuntimeEvent>>>,
    pending_runtime_events: AsyncMutex<Vec<RuntimeEvent>>,
}

#[async_trait]
impl RuntimeBackend for ExitSignalRuntimeBackend {
    async fn create_instance(&self, request: RuntimeCreateRequest) -> Result<String, RuntimeError> {
        let task_id = request
            .name
            .strip_prefix("mantissa-")
            .and_then(|raw| Uuid::parse_str(raw).ok());
        let container_id = self.inner.create_instance(request).await?;
        if let Some(task_id) = task_id {
            self.task_ids_by_container
                .lock()
                .await
                .insert(container_id.clone(), task_id);
        }
        Ok(container_id)
    }

    async fn start_instance(&self, container_id: &str) -> Result<(), RuntimeError> {
        self.inner.start_instance(container_id).await?;

        let task_id = self
            .task_ids_by_container
            .lock()
            .await
            .get(container_id)
            .copied();
        if let Some(task_id) = task_id {
            let event = RuntimeEvent::TaskExited {
                task_id,
                exit_code: 255,
            };
            let sender = self.runtime_events_tx.lock().await.clone();
            if let Some(sender) = sender {
                let _ = sender.send(event);
            } else {
                self.pending_runtime_events.lock().await.push(event);
            }
        }

        Ok(())
    }

    async fn stop_instance(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        self.inner.stop_instance(container_id, timeout).await
    }

    async fn restart_instance(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        self.inner.restart_instance(container_id, timeout).await
    }

    async fn remove_instance(
        &self,
        container_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> Result<(), RuntimeError> {
        self.task_ids_by_container.lock().await.remove(container_id);
        self.inner
            .remove_instance(container_id, force, remove_volumes)
            .await
    }

    async fn list_instances(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> Result<Vec<RuntimeInfo>, RuntimeError> {
        self.inner.list_instances(filters).await
    }

    async fn inspect_instance(&self, container_id: &str) -> Result<RuntimeInfo, RuntimeError> {
        self.inner.inspect_instance(container_id).await
    }

    async fn pull_image(&self, _image: &str) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            lifecycle_events: true,
            ..Default::default()
        }
    }

    async fn watch_runtime_events(
        &self,
        events_tx: tokio::sync::mpsc::UnboundedSender<RuntimeEvent>,
    ) -> Result<(), RuntimeError> {
        let pending = {
            let mut pending = self.pending_runtime_events.lock().await;
            std::mem::take(&mut *pending)
        };
        *self.runtime_events_tx.lock().await = Some(events_tx.clone());
        for event in pending {
            let _ = events_tx.send(event);
        }
        while !events_tx.is_closed() {
            sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }
}

#[derive(Default)]
/// Fails container creation only after explicit activation.
///
/// The rollback-failure test first deploys a healthy baseline, then enables
/// failures before submitting the redeploy, so failure and rollback behavior can
/// be isolated from initial bootstrap.
pub(crate) struct CreateFailureAfterBaselineRuntimeBackend {
    inner: InMemoryRuntimeBackend,
    fail_creates: AtomicBool,
}

impl CreateFailureAfterBaselineRuntimeBackend {
    /// Enables create failure injection for subsequent create requests.
    pub(crate) fn enable_create_failures(&self) {
        self.fail_creates.store(true, Ordering::Relaxed);
    }
}

#[async_trait]
impl RuntimeBackend for CreateFailureAfterBaselineRuntimeBackend {
    async fn create_instance(&self, request: RuntimeCreateRequest) -> Result<String, RuntimeError> {
        if self.fail_creates.load(Ordering::Relaxed) {
            return Err(RuntimeError::backend(Some(500), "injected create failure"));
        }
        self.inner.create_instance(request).await
    }

    async fn start_instance(&self, container_id: &str) -> Result<(), RuntimeError> {
        self.inner.start_instance(container_id).await
    }

    async fn stop_instance(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        self.inner.stop_instance(container_id, timeout).await
    }

    async fn restart_instance(
        &self,
        container_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        self.inner.restart_instance(container_id, timeout).await
    }

    async fn remove_instance(
        &self,
        container_id: &str,
        force: bool,
        remove_volumes: bool,
    ) -> Result<(), RuntimeError> {
        self.inner
            .remove_instance(container_id, force, remove_volumes)
            .await
    }

    async fn list_instances(
        &self,
        filters: Option<HashMap<String, Vec<String>>>,
    ) -> Result<Vec<RuntimeInfo>, RuntimeError> {
        self.inner.list_instances(filters).await
    }

    async fn inspect_instance(&self, container_id: &str) -> Result<RuntimeInfo, RuntimeError> {
        self.inner.inspect_instance(container_id).await
    }

    async fn pull_image(&self, image: &str) -> Result<(), RuntimeError> {
        self.inner.pull_image(image).await
    }
}

/// Waits until each provided node owns at least `min_expected` active tasks for the service.
pub(crate) async fn wait_for_min_local_service_task_count(
    cluster: &[TestNode],
    service_name: &str,
    min_expected: usize,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        for node in cluster {
            let count = list_local_active_service_tasks(
                &node.node.workload_manager,
                service_name,
                node.id(),
            )
            .await
            .len();
            if count < min_expected {
                return false;
            }
        }
        true
    })
    .await
}

/// Waits until the local scheduler reports the expected reserved slot count.
pub(crate) async fn wait_for_reserved_slots(
    node: &TestNode,
    expected: usize,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        if let Some(snapshot) = node.node.scheduler.snapshot().await {
            let reserved = snapshot
                .slots
                .iter()
                .filter(|slot| matches!(slot.state, SlotState::Reserved(_)))
                .count();
            if reserved == expected {
                return true;
            }
        }
        false
    })
    .await
}

/// Converts a task spec into a replicated task value for store-level fault-injection tests.
pub(crate) fn task_spec_to_value(spec: &WorkloadSpec) -> TaskValue {
    TaskValue {
        id: spec.id,
        name: spec.name.clone(),
        image: spec.image.clone(),
        execution_platform: spec.execution_platform,
        isolation_mode: spec.isolation_mode,
        isolation_profile: spec.isolation_profile.clone(),
        state: spec.state.clone(),
        phase_reason: spec.phase_reason.clone(),
        phase_progress: spec.phase_progress.clone(),
        created_at: spec.created_at.clone(),
        updated_at: spec.updated_at.clone(),
        command: spec.command.clone(),
        tty: spec.tty,
        node_id: spec.node_id,
        node_name: spec.node_name.clone(),
        slot_ids: spec.slot_ids.clone(),
        slot_id: spec.slot_id,
        cpu_millis: spec.cpu_millis,
        memory_bytes: spec.memory_bytes,
        gpu_count: spec.gpu_count,
        gpu_device_ids: spec.gpu_device_ids.clone(),
        restart_policy: spec.restart_policy.clone(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: spec.env.clone(),
        secret_files: spec.secret_files.clone(),
        volumes: Vec::new(),
        networks: spec.networks.clone(),
        ports: Vec::new(),
        owner: spec.owner.clone(),
        lease_id: spec.lease_id,
        lease_coordinator_node_id: spec.lease_coordinator_node_id,
        admission_group_id: spec.admission_group_id,
        admission_state: spec.admission_state,
        task_epoch: spec.task_epoch,
        phase_version: spec.phase_version,
        launch_attempt: spec.launch_attempt,
        last_terminal_observed_launch: spec.last_terminal_observed_launch,
        definition_complete: true,
    }
}

pub(crate) async fn remove_service_via_rpc(client: &services::Client, service_id: Uuid) {
    let mut delete = client.delete_request();
    {
        let mut ids = delete.get().init_ids(1);
        ids.set(0, service_id.as_bytes());
    }
    delete
        .send()
        .promise
        .await
        .expect("service delete should succeed");
}

pub(crate) async fn drain_node_via_topology(
    client: &topology::Client,
    node_id: Uuid,
    reason: &str,
) -> Result<(), CapnpError> {
    drain_node_with_timeout_via_topology(client, node_id, reason, None).await
}

pub(crate) async fn drain_node_with_timeout_via_topology(
    client: &topology::Client,
    node_id: Uuid,
    reason: &str,
    task_stop_timeout_secs: Option<u32>,
) -> Result<(), CapnpError> {
    let mut request = client.drain_node_request();
    let mut params = request.get();
    params
        .reborrow()
        .init_node_id()
        .set_bytes(node_id.as_bytes());
    params.set_reason(reason);
    params.set_task_stop_timeout_secs(task_stop_timeout_secs.unwrap_or(0));
    request.send().promise.await?;
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct TestDrainStatus {
    pub(crate) state: NodeDrainState,
    pub(crate) schedulable: bool,
    pub(crate) drain_requested: bool,
    pub(crate) task_stop_timeout_secs: Option<u32>,
    pub(crate) remaining_service_tasks: u32,
    pub(crate) last_scheduling_error: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct TestListedNodeState {
    pub(crate) schedulable: bool,
    pub(crate) drain_requested: bool,
    pub(crate) drain_state: NodeDrainState,
}

/// Reads the topology drain-status projection used by maintenance integration tests.
pub(crate) async fn drain_status_via_topology(
    client: &topology::Client,
    node_id: Uuid,
) -> Result<TestDrainStatus, CapnpError> {
    let mut request = client.get_node_drain_status_request();
    request.get().init_node_id().set_bytes(node_id.as_bytes());
    let response = request.send().promise.await?;
    let status = response.get()?.get_status()?;
    let last_scheduling_error = status
        .get_last_scheduling_error()?
        .to_str()?
        .trim()
        .to_string();

    Ok(TestDrainStatus {
        state: status.get_state()?,
        schedulable: status.get_schedulable(),
        drain_requested: status.get_drain_requested(),
        task_stop_timeout_secs: match status.get_task_stop_timeout_secs() {
            0 => None,
            value => Some(value),
        },
        remaining_service_tasks: status.get_remaining_service_tasks(),
        last_scheduling_error: if last_scheduling_error.is_empty() {
            None
        } else {
            Some(last_scheduling_error)
        },
    })
}

/// Reads one node row from `Topology.list` so tests can assert the list projection directly.
pub(crate) async fn listed_node_state_via_topology(
    client: &topology::Client,
    node_id: Uuid,
) -> Result<TestListedNodeState, CapnpError> {
    let response = client.list_request().send().promise.await?;
    let nodes = response.get()?.get_nodes()?.get_nodes()?;
    for node in nodes.iter() {
        let listed_id = Uuid::from_slice(node.get_id()?.get_bytes()?)
            .map_err(|err| CapnpError::failed(err.to_string()))?;
        if listed_id != node_id {
            continue;
        }
        let peer = node.get_peer()?;

        return Ok(TestListedNodeState {
            schedulable: peer.get_schedulable(),
            drain_requested: peer.get_drain_requested(),
            drain_state: node.get_drain_state()?,
        });
    }

    Err(CapnpError::failed(format!(
        "node {node_id} not found in topology list"
    )))
}

/// Reserves every local scheduler slot on one node so evacuation tests can create hard blockers.
pub(crate) async fn reserve_all_scheduler_slots(node: &TestNode, owner: Uuid) {
    let snapshot = node
        .node
        .scheduler
        .snapshot()
        .await
        .expect("scheduler snapshot should be present");
    let intents: Vec<SlotReservationRequest> = snapshot
        .slots
        .iter()
        .map(|slot| SlotReservationRequest {
            slot_id: slot.slot_id,
            owner,
            task_id: None,
            group_id: None,
        })
        .collect();
    if intents.is_empty() {
        return;
    }

    node.node
        .scheduler
        .reserve_resources(snapshot.version, intents, Vec::new())
        .await
        .expect("reserve all scheduler slots");
}

pub(crate) async fn wait_for_service_state(
    manager: &ServiceController,
    service_id: Uuid,
    expect_present: bool,
) -> bool {
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async {
            let specs = manager
                .list_services()
                .expect("service list should succeed during wait");
            let present = specs.iter().any(|spec| spec.id == service_id);
            present == expect_present
        },
    )
    .await
}

pub(crate) async fn wait_for_service_status(
    manager: &ServiceController,
    service_id: Uuid,
    expected: ServiceStatus,
) -> bool {
    wait_until(
        Duration::from_secs(20),
        Duration::from_millis(50),
        || async {
            if let Ok(Some(spec)) = manager.registry().get(service_id)
                && spec.status() == expected
            {
                return true;
            }
            false
        },
    )
    .await
}

/// Waits until the replicated service spec exposes a lifecycle detail containing any substring.
pub(crate) async fn wait_for_service_status_detail_any(
    manager: &ServiceController,
    service_id: Uuid,
    expected_substrings: &[&str],
) -> bool {
    wait_until(
        Duration::from_secs(20),
        Duration::from_millis(50),
        || async {
            match manager.registry().get(service_id) {
                Ok(Some(spec)) => spec.status_detail.as_deref().is_some_and(|detail| {
                    expected_substrings
                        .iter()
                        .any(|expected| detail.contains(expected))
                }),
                _ => false,
            }
        },
    )
    .await
}

pub(crate) async fn wait_for_service_spec_all(
    cluster: &[TestNode],
    service_id: Uuid,
    expected: &ServiceSpecValue,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(50), || async {
        for node in cluster {
            match node.node.service_controller.registry().get(service_id) {
                Ok(Some(spec)) if service_spec_matches_expected(&spec, expected) => {}
                _ => return false,
            }
        }
        true
    })
    .await
}

pub(crate) async fn wait_for_service_replica_ids_converged_all(
    cluster: &[TestNode],
    service_id: Uuid,
    expected_count: usize,
    stable_rounds_required: usize,
    timeout: Duration,
) -> Option<BTreeSet<Uuid>> {
    let deadline = Instant::now() + timeout;
    let mut previous = None::<BTreeSet<Uuid>>;
    let mut stable_rounds = 0usize;

    while Instant::now() < deadline {
        let mut canonical = None::<BTreeSet<Uuid>>;
        let mut all_match = true;

        for node in cluster {
            let spec = match node.node.service_controller.registry().get(service_id) {
                Ok(Some(spec)) if spec.status() == ServiceStatus::Running => spec,
                _ => {
                    all_match = false;
                    break;
                }
            };
            let replica_ids = spec.replica_ids.iter().copied().collect::<BTreeSet<_>>();
            if replica_ids.len() != expected_count {
                all_match = false;
                break;
            }
            match canonical.as_ref() {
                Some(current) if current != &replica_ids => {
                    all_match = false;
                    break;
                }
                Some(_) => {}
                None => canonical = Some(replica_ids),
            }
        }

        if all_match {
            let replica_ids = canonical.unwrap_or_default();
            if previous.as_ref() == Some(&replica_ids) {
                stable_rounds += 1;
            } else {
                previous = Some(replica_ids.clone());
                stable_rounds = 1;
            }
            if stable_rounds >= stable_rounds_required {
                return Some(replica_ids);
            }
        } else {
            stable_rounds = 0;
            previous = None;
        }

        sleep(Duration::from_millis(100)).await;
    }

    None
}

pub(crate) async fn ensure_demo_manifest_secrets(cluster: &[TestNode]) {
    assert!(
        !cluster.is_empty(),
        "cluster must contain at least one node to seed secrets"
    );

    let secrets: [(&str, &[u8]); 3] = [
        ("demo-api-token", b"demo-api-token-secret"),
        ("demo-db-password", b"demo-db-password"),
        ("demo-nginx-key", b"demo-nginx-key"),
    ];

    for (name, plaintext) in secrets {
        create_secret(&cluster[0].node.secrets_client, name, plaintext)
            .await
            .unwrap_or_else(|err| panic!("create secret '{name}' failed: {err}"));

        assert!(
            wait_for_secret(
                &cluster[0].node.secrets_client,
                name,
                Duration::from_secs(10)
            )
            .await,
            "anchor should observe secret '{name}'"
        );

        for peer in cluster.iter().skip(1) {
            assert!(
                wait_for_secret(&peer.node.secrets_client, name, Duration::from_secs(10)).await,
                "node {} should replicate secret '{name}'",
                peer.id()
            );
        }
    }
}

pub(crate) fn manifest_to_task_templates(manifest: &ServiceManifest) -> Vec<TaskTemplateSpecValue> {
    manifest
        .task_templates
        .iter()
        .map(|task| {
            // Tests run without kernel networking support, so we avoid provisioning
            // any overlay interfaces by submitting empty network requirements.
            let networks: Vec<TaskTemplateNetworkRequirement> = Vec::new();

            TaskTemplateSpecValue {
                name: task.name.clone(),
                execution: ExecutionSpec {
                    image: task.image.clone(),
                    command: task.command.clone(),
                    tty: false,
                    cpu_millis: task.resources.cpu_millis,
                    memory_bytes: task.resources.memory_bytes(),
                    gpu_count: 0,
                    restart_policy: task.restart_policy.as_ref().map(|policy| {
                        TaskTemplateRestartPolicy {
                            name: match policy.name {
                                ManifestRestartPolicyName::No => TaskTemplateRestartPolicyKind::No,
                                ManifestRestartPolicyName::Always => {
                                    TaskTemplateRestartPolicyKind::Always
                                }
                                ManifestRestartPolicyName::OnFailure => {
                                    TaskTemplateRestartPolicyKind::OnFailure
                                }
                                ManifestRestartPolicyName::UnlessStopped => {
                                    TaskTemplateRestartPolicyKind::UnlessStopped
                                }
                            },
                            max_retry_count: policy.max_retry_count.map(|value| {
                                i32::try_from(value).expect("validated manifest bound")
                            }),
                        }
                    }),
                    termination_grace_period_secs: None,
                    pre_stop_command: None,
                    liveness: None,
                    env: task
                        .env
                        .iter()
                        .map(|var| TaskEnvironmentVariable {
                            name: var.name.clone(),
                            value: var.value.clone(),
                            secret: var.secret.as_ref().map(|secret| TaskSecretReference {
                                name: secret.name.clone(),
                                version_id: parse_secret_version(secret),
                            }),
                        })
                        .collect(),
                    secret_files: task
                        .secret_files
                        .iter()
                        .map(|file| TaskSecretFile {
                            path: file.path.clone(),
                            secret: TaskSecretReference {
                                name: file.secret.name.clone(),
                                version_id: parse_secret_version(&file.secret),
                            },
                            mode: file.mode,
                            ownership: match &file.ownership {
                                mantissa_client::volumes::LocalVolumeOwnership::Daemon => {
                                    mantissa::volumes::types::LocalVolumeOwnership::Daemon
                                }
                                mantissa_client::volumes::LocalVolumeOwnership::User {
                                    uid,
                                    gid,
                                } => mantissa::volumes::types::LocalVolumeOwnership::User {
                                    uid: *uid,
                                    gid: *gid,
                                },
                                mantissa_client::volumes::LocalVolumeOwnership::FsGroup { gid } => {
                                    mantissa::volumes::types::LocalVolumeOwnership::FsGroup {
                                        gid: *gid,
                                    }
                                }
                            },
                            path_env_name: file.path_env_name.clone(),
                        })
                        .collect(),
                    volumes: Vec::new(),
                    networks,
                    ports: task
                        .ports
                        .iter()
                        .map(|port| WorkloadPortBinding {
                            name: port.name.clone(),
                            target_port: port.target,
                            host_port: port.host,
                            host_ip: port.host_ip.clone(),
                            protocol: match port.protocol {
                                ManifestPortProtocol::Tcp => WorkloadPortProtocol::Tcp,
                                ManifestPortProtocol::Udp => WorkloadPortProtocol::Udp,
                            },
                        })
                        .collect(),
                    placement: Default::default(),
                },
                depends_on: task.depends_on.clone(),
                replicas: task.replicas,
                readiness: None,
                public_port: None,
                public_protocol: None,
                placement_preferences: Vec::new(),
            }
        })
        .collect()
}

pub(crate) fn service_crdt_spec_at(
    service_name: &str,
    manifest_name: &str,
    manifest_id: Uuid,
    status: ServiceStatus,
    service_epoch: u64,
    phase_version: u64,
    updated_at: DateTime<Utc>,
) -> ServiceSpecValue {
    let mut spec = ServiceSpecValue::new(
        manifest_id,
        manifest_name,
        service_name,
        Vec::new(),
        Vec::new(),
    );
    spec.status = status;
    spec.service_epoch = service_epoch;
    spec.phase_version = phase_version;
    spec.updated_at = updated_at.to_rfc3339();
    spec
}

pub(crate) fn service_spec_matches_expected(
    actual: &ServiceSpecValue,
    expected: &ServiceSpecValue,
) -> bool {
    actual.id == expected.id
        && actual.manifest_id == expected.manifest_id
        && actual.manifest_name == expected.manifest_name
        && actual.service_name == expected.service_name
        && actual.task_templates == expected.task_templates
        && actual.replica_ids == expected.replica_ids
        && actual.update_strategy == expected.update_strategy
        && actual.service_epoch == expected.service_epoch
        && actual.phase_version == expected.phase_version
        && actual.rollout == expected.rollout
        && actual.status == expected.status
        && actual.status_detail == expected.status_detail
        && actual.previous_generation == expected.previous_generation
        && actual.reschedule_lock == expected.reschedule_lock
}

pub(crate) fn parse_secret_version(reference: &SecretReference) -> Option<Uuid> {
    reference
        .version
        .as_ref()
        .and_then(|v| Uuid::parse_str(v).ok())
}

pub(crate) async fn list_service_ids(client: &services::Client) -> Vec<Uuid> {
    let response = client
        .list_request()
        .send()
        .promise
        .await
        .expect("Services.list call should succeed");
    let reader = response
        .get()
        .expect("Services.list should yield result message");
    let specs = reader
        .get_services()
        .expect("Services.list should include services list");

    let mut ids = Vec::with_capacity(specs.len() as usize);
    for spec in specs.iter() {
        let data = spec.get_id().expect("service id data").to_owned();
        if data.len() != 16 {
            continue;
        }
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&data);
        ids.push(Uuid::from_bytes(bytes));
    }

    ids
}

pub(crate) async fn wait_for_task_count(
    manager: &WorkloadManager,
    expected: usize,
    timeout: Duration,
) -> bool {
    let filter = TaskStateFilter::all();
    wait_until(timeout, Duration::from_millis(50), || async {
        let specs = manager
            .list_workloads(&filter)
            .await
            .expect("task list during wait");
        if specs.len() == expected {
            return true;
        }
        false
    })
    .await
}

pub(crate) async fn wait_for_task_state(
    manager: &WorkloadManager,
    task_id: Uuid,
    expected: WorkloadPhase,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(50), || async {
        match manager.inspect_workload(task_id).await {
            Ok(spec) => spec.state == expected,
            Err(_) => false,
        }
    })
    .await
}

pub(crate) async fn import_local_volume_for_service(
    client: &volumes::Client,
    name: &str,
    node_id: Uuid,
    path: &Path,
) -> Uuid {
    let mut request = client.import_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name(name);
        inner.set_node_id(node_id.as_bytes());
        inner.set_path(
            path.to_str()
                .expect("imported volume path should be valid utf8"),
        );
        inner.set_requested_bytes(0);
    }

    let response = request.send().promise.await.expect("import volume send");
    let reader = response.get().expect("import volume response");
    let bytes = reader
        .get_volume()
        .expect("volume payload")
        .get_id()
        .expect("volume id");
    Uuid::from_slice(bytes).expect("decode volume id")
}

pub(crate) async fn create_secret(
    client: &secrets::Client,
    name: &str,
    plaintext: &[u8],
) -> Result<(), CapnpError> {
    let mut req = client.create_request();
    {
        let mut inner = req.get().init_request();
        inner.set_name(name);
        inner.set_plaintext(plaintext);
        inner.set_description("");
        inner.init_metadata(0);
    }
    let response = req.send().promise.await?;
    let _ = response.get()?.get_secret()?;
    Ok(())
}

pub(crate) async fn list_secret_names(client: &secrets::Client) -> Vec<String> {
    let response = client
        .list_request()
        .send()
        .promise
        .await
        .expect("secrets list request");
    let reader = response
        .get()
        .expect("secret list result")
        .get_secrets()
        .expect("secret list reader");
    let mut names = Vec::with_capacity(reader.len() as usize);
    for entry in reader.iter() {
        let name = entry
            .get_name()
            .expect("secret name data")
            .to_str()
            .expect("secret name utf8")
            .to_string();
        names.push(name);
    }
    names
}

pub(crate) async fn wait_for_secret(
    client: &secrets::Client,
    name: &str,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(50), || async {
        if list_secret_names(client)
            .await
            .into_iter()
            .any(|candidate| candidate == name)
        {
            return true;
        }
        false
    })
    .await
}
