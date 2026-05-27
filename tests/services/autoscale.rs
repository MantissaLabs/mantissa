use super::support::*;
use crate::common;
use mantissa::services::types::compute_service_id;

local_test!(services_autoscale_scales_memory_hot_template, {
    let runtime = Arc::new(InMemoryRuntimeBackend::default());
    runtime.set_default_usage_sample(0, 96 * 1024 * 1024).await;
    let _guard = RuntimeBackendOverrideGuard::install(runtime);
    let node = TestNode::new_inproc_with_config(ClusterConfig {
        service_timing: Some(
            ServiceControllerTiming::production().with_autoscale_tick(Duration::from_millis(100)),
        ),
        ..ClusterConfig::default()
    })
    .await;

    let service_name = "autoscale-memory-hot";
    let manifest_name = "autoscale-memory-hot";
    let mut template = demo_backend_task_template("api", 1);
    template.execution.cpu_millis = 100;
    template.execution.memory_bytes = 64 * 1024 * 1024;
    template.autoscale = Some(TaskTemplateAutoscalePolicyValue {
        min_replicas: 1,
        max_replicas: 3,
        cooldown_secs: 60,
        scale_down_stabilization_secs: 60,
        sample_window_secs: 1,
        trigger_windows: 1,
        metrics: vec![TaskTemplateAutoscaleMetricValue {
            kind: TaskTemplateAutoscaleMetricKindValue::Memory,
            target_percent: 50,
        }],
    });

    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, vec![template])
        .await
        .expect("submit autoscaled service");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "autoscaled service should reach initial running state"
    );

    assert!(
        wait_until(
            Duration::from_secs(10),
            Duration::from_millis(50),
            || async {
                node.node
                    .service_controller
                    .registry()
                    .get(service_id)
                    .ok()
                    .flatten()
                    .and_then(|spec| {
                        spec.task_templates
                            .iter()
                            .find(|template| template.name == "api")
                            .map(|template| spec.service_epoch > 0 && template.replicas == 2)
                    })
                    .unwrap_or(false)
            }
        )
        .await,
        "autoscale should persist a new desired replica count"
    );

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "autoscale generation should converge through normal rollout"
    );

    assert!(
        wait_for_service_task_count_all(
            std::slice::from_ref(&node),
            service_name,
            2,
            Duration::from_secs(10)
        )
        .await,
        "autoscale generation should run the extra replica"
    );
});

local_test!(services_autoscale_owner_failover_scales_on_new_owner, {
    let runtimes: Arc<Mutex<Vec<Arc<InMemoryRuntimeBackend>>>> = Arc::new(Mutex::new(Vec::new()));
    let factory_runtimes = runtimes.clone();
    let _guard = RuntimeBackendOverrideGuard::install_factory(Arc::new(
        move || -> Arc<dyn RuntimeBackend + Send + Sync> {
            let runtime = Arc::new(InMemoryRuntimeBackend::default());
            factory_runtimes.lock().push(runtime.clone());
            runtime
        },
    ));

    let health = fast_health_runtime_config();
    let mut cluster = TestNode::new_cluster_inproc_with_config(
        3,
        ClusterConfig {
            sync_tick_ms: Some(100),
            gossip_tick_ms: Some(100),
            service_timing: Some(
                ServiceControllerTiming::production()
                    .with_autoscale_tick(Duration::from_millis(100)),
            ),
            runtime_health: Some(health),
            ..ClusterConfig::default()
        },
    )
    .await
    .expect("create autoscale owner failover cluster");
    TestNode::wait_cluster_ready_all(&cluster, 3, Duration::from_secs(10))
        .await
        .expect("autoscale owner failover cluster should converge to three ready nodes");

    let failed_owner_id = cluster[2].id();
    let candidate_ids = cluster.iter().map(TestNode::id).collect::<Vec<_>>();
    let (service_name, service_id) = service_name_for_autoscale_owner(
        "autoscale-owner-failover",
        failed_owner_id,
        &candidate_ids,
    );
    assert_eq!(
        select_autoscale_owner_for_test(service_id, &candidate_ids),
        Some(failed_owner_id),
        "test setup should choose the node we shut down as the initial autoscale owner"
    );

    let mut template = demo_backend_task_template("api", 3);
    template.execution.cpu_millis = 100;
    template.execution.memory_bytes = 64 * 1024 * 1024;
    template.autoscale = Some(TaskTemplateAutoscalePolicyValue {
        min_replicas: 3,
        max_replicas: 4,
        cooldown_secs: 60,
        scale_down_stabilization_secs: 60,
        sample_window_secs: 1,
        trigger_windows: 1,
        metrics: vec![TaskTemplateAutoscaleMetricValue {
            kind: TaskTemplateAutoscaleMetricKindValue::Memory,
            target_percent: 50,
        }],
    });

    let service_id_from_submission = cluster[0]
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), &service_name, &service_name, vec![template])
        .await
        .expect("submit autoscaled service");
    assert_eq!(service_id_from_submission, service_id);

    assert!(
        wait_for_service_status_all(
            &cluster,
            service_id,
            ServiceStatus::Running,
            Duration::from_secs(20)
        )
        .await,
        "autoscaled service should reach initial running state on every node"
    );
    assert!(
        wait_for_service_running_tasks_stable_all(
            &cluster,
            &service_name,
            3,
            3,
            Duration::from_secs(20),
        )
        .await,
        "initial autoscaled service should converge on three running replicas"
    );

    let cluster_refs = cluster.iter().collect::<Vec<_>>();
    assert!(
        wait_for_min_local_service_task_count_refs(
            &cluster_refs,
            &service_name,
            1,
            Duration::from_secs(12)
        )
        .await,
        "initial replicas should be spread across every node before owner shutdown"
    );

    for observer in &cluster[..2] {
        observer
            .wait_status_of(failed_owner_id, NodeStatus::Alive, Duration::from_secs(5))
            .await
            .expect("remaining nodes should see selected autoscale owner alive before shutdown");
    }

    let failed_owner = cluster.remove(2);
    let failed_owner = *failed_owner.node;
    failed_owner
        .shutdown()
        .await
        .expect("shut down selected autoscale owner");

    for observer in &cluster {
        observer
            .wait_status_of(
                failed_owner_id,
                NodeStatus::Down,
                swim_down_transition_timeout_for(2, health),
            )
            .await
            .expect("remaining nodes should mark selected autoscale owner down");
    }

    let live_candidate_ids = cluster.iter().map(TestNode::id).collect::<Vec<_>>();
    let live_owner_id = select_autoscale_owner_for_test(service_id, &live_candidate_ids)
        .expect("live autoscale owner");
    assert_ne!(
        live_owner_id, failed_owner_id,
        "autoscale owner should move to a live node after the previous owner is down"
    );

    let runtime_handles = runtimes.lock().clone();
    for runtime in runtime_handles {
        runtime.set_default_usage_sample(0, 96 * 1024 * 1024).await;
    }

    assert!(
        wait_until(
            Duration::from_secs(20),
            Duration::from_millis(100),
            || async {
                for node in &cluster {
                    let Some(spec) = node
                        .node
                        .service_controller
                        .registry()
                        .get(service_id)
                        .ok()
                        .flatten()
                    else {
                        return false;
                    };
                    if spec.service_epoch == 0 || spec.status() != ServiceStatus::Running {
                        return false;
                    }
                    let scaled = spec
                        .task_templates
                        .iter()
                        .find(|template| template.name == "api")
                        .is_some_and(|template| template.replicas == 4);
                    if !scaled {
                        return false;
                    }
                }
                true
            }
        )
        .await,
        "new autoscale owner should persist a scale-up generation after failover"
    );

    assert!(
        wait_for_service_running_tasks_stable_all(
            &cluster,
            &service_name,
            4,
            3,
            Duration::from_secs(30),
        )
        .await,
        "remaining nodes should converge on the scaled replica set"
    );
    assert!(
        surviving_nodes_observe_no_active_service_tasks_on_node(
            &cluster,
            &service_name,
            failed_owner_id,
            Duration::from_secs(10),
        )
        .await,
        "failed autoscale owner should not host active tasks after live-owner rollout"
    );
});

/// Finds a deterministic service name whose autoscale owner is `owner_id`.
fn service_name_for_autoscale_owner(
    prefix: &str,
    owner_id: Uuid,
    candidates: &[Uuid],
) -> (String, Uuid) {
    for nonce in 0..10_000 {
        let service_name = format!("{prefix}-{nonce}");
        let service_id = compute_service_id(&service_name);
        if select_autoscale_owner_for_test(service_id, candidates) == Some(owner_id) {
            return (service_name, service_id);
        }
    }
    panic!("unable to find service name autoscale-owned by {owner_id}");
}

/// Selects the deterministic autoscale owner for one service using the production hash input.
fn select_autoscale_owner_for_test(service_id: Uuid, candidates: &[Uuid]) -> Option<Uuid> {
    let mut best: Option<(Uuid, u128)> = None;
    for node_id in candidates {
        let score = autoscale_owner_score_for_test(service_id, *node_id);
        match best {
            None => best = Some((*node_id, score)),
            Some((_, best_score)) if score > best_score => best = Some((*node_id, score)),
            _ => {}
        }
    }
    best.map(|(node_id, _)| node_id)
}

/// Computes the rendezvous score used by autoscale ownership selection.
fn autoscale_owner_score_for_test(service_id: Uuid, node_id: Uuid) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"autoscale");
    hasher.update(service_id.as_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}
