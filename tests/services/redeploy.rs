use super::support::*;
use crate::common;

local_test!(services_redeploy_scales_replicas, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "redeploy-scale";
    let manifest_name = "redeploy-scale";

    let mut tasks = vec![TaskTemplateSpecValue {
        name: "echo".into(),
        execution: ExecutionSpec {
            command: vec![
                "sh".into(),
                "-c".into(),
                "while true; do sleep 1; done".into(),
            ],
            cpu_millis: 100,
            memory_bytes: 32 * 1024 * 1024,
            ..empty_service_execution("alpine:3.20")
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
        placement_preferences: Vec::new(),
    }];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit initial deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should reach running state before redeploy"
    );
    assert!(
        wait_for_task_count(&node.node.workload_manager, 1, Duration::from_secs(5)).await,
        "initial deployment should launch a single replica"
    );

    let initial_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read initial spec")
        .expect("initial spec present");
    let initial_ids: BTreeSet<Uuid> = initial_spec.assigned_replica_ids().into_iter().collect();
    assert_eq!(
        initial_ids.len(),
        1,
        "initial deployment should track one replica id"
    );

    tasks[0].replicas = 3;

    let redeploy_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit redeployment");
    assert_eq!(
        redeploy_id, service_id,
        "service identifier should remain stable across redeploys"
    );

    let scaled_running = wait_for_service_status(
        &node.node.service_controller,
        service_id,
        ServiceStatus::Running,
    )
    .await;
    if !scaled_running {
        let current = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read scaled service spec")
            .expect("scaled service spec should exist");
        panic!(
            "service should return to running after scale-out redeploy; current status={:?}, task_ids={}, rollout_phase={:?}, rollout_failed_steps={}, rollout_error={:?}",
            current.status(),
            current.assigned_replica_count(),
            current.rollout.phase,
            current.rollout.failed_steps,
            current.rollout.last_error
        );
    }
    assert!(
        wait_for_task_count(&node.node.workload_manager, 3, Duration::from_secs(8)).await,
        "scaled service should eventually report three replicas"
    );

    let updated_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read updated spec")
        .expect("updated spec present");
    let updated_ids: BTreeSet<Uuid> = updated_spec.assigned_replica_ids().into_iter().collect();
    assert_eq!(
        updated_ids.len(),
        3,
        "scaled deployment should record three replica identifiers"
    );
    assert!(
        initial_ids.iter().all(|id| updated_ids.contains(id)),
        "existing replicas should be preserved during scale-out"
    );
});

local_test!(services_redeploy_updates_resources, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "redeploy-resources";
    let manifest_name = "redeploy-resources";

    let mut tasks = vec![TaskTemplateSpecValue {
        name: "echo".into(),
        execution: ExecutionSpec {
            command: vec![
                "sh".into(),
                "-c".into(),
                "while true; do sleep 1; done".into(),
            ],
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            ..empty_service_execution("alpine:3.20")
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
        placement_preferences: Vec::new(),
    }];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit initial deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should reach running state before resource update"
    );
    assert!(
        wait_for_task_count(&node.node.workload_manager, 1, Duration::from_secs(5)).await,
        "baseline deployment should launch a single replica"
    );

    let initial_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read baseline spec")
        .expect("baseline spec present");
    let initial_id = initial_spec
        .assigned_replica_id(0)
        .expect("baseline spec should include one task id");

    tasks[0].execution.cpu_millis = 750;
    tasks[0].execution.memory_bytes = 256 * 1024 * 1024;

    let redeploy_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit redeployment with new resources");
    assert_eq!(
        redeploy_id, service_id,
        "redeploy should target existing service identifier"
    );

    let refreshed_running = wait_for_service_status(
        &node.node.service_controller,
        service_id,
        ServiceStatus::Running,
    )
    .await;
    if !refreshed_running {
        let current = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read refreshed service spec")
            .expect("refreshed service spec should exist");
        panic!(
            "service should return to running after resource refresh; current status={:?}, task_ids={}, rollout_phase={:?}, rollout_failed_steps={}, rollout_error={:?}",
            current.status(),
            current.assigned_replica_count(),
            current.rollout.phase,
            current.rollout.failed_steps,
            current.rollout.last_error
        );
    }
    let updated_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read updated spec")
        .expect("updated spec present");
    assert_eq!(
        updated_spec.assigned_replica_count(),
        1,
        "resource refresh should maintain a single replica"
    );
    let replacement_id = updated_spec
        .assigned_replica_id(0)
        .expect("updated spec should include one task id");
    assert_ne!(
        replacement_id, initial_id,
        "resource change should replace the existing replica"
    );

    let replacement_spec = node
        .node
        .workload_manager
        .inspect_workload(replacement_id)
        .await
        .expect("inspect updated task");
    assert_eq!(
        replacement_spec.cpu_millis, tasks[0].cpu_millis,
        "updated task should honour new cpu allocation"
    );
    assert_eq!(
        replacement_spec.memory_bytes, tasks[0].memory_bytes,
        "updated task should honour new memory allocation"
    );
});

local_test!(services_redeploy_rejects_unchanged_running_spec, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "redeploy-unchanged";
    let manifest_name = "redeploy-unchanged";

    let tasks = vec![TaskTemplateSpecValue {
        name: "echo".into(),
        execution: ExecutionSpec {
            command: vec![
                "sh".into(),
                "-c".into(),
                "while true; do sleep 1; done".into(),
            ],
            cpu_millis: 100,
            memory_bytes: 32 * 1024 * 1024,
            ..empty_service_execution("alpine:3.20")
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
        placement_preferences: Vec::new(),
    }];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit initial deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "initial deployment should reach running state"
    );

    let baseline = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read baseline spec")
        .expect("baseline spec present");

    let submission = node
        .node
        .service_controller
        .submit_deployment_with_strategy_outcome(
            Uuid::new_v4(),
            manifest_name,
            service_name,
            tasks,
            ServiceUpdateStrategy::default(),
        )
        .await
        .expect("unchanged running redeploy should return a no-op outcome");
    assert_eq!(
        submission.outcome,
        ServiceDeploymentOutcome::Unchanged,
        "unchanged running redeploy should report unchanged outcome"
    );
    assert_eq!(
        submission.service_id, service_id,
        "unchanged running redeploy should target the existing service id"
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let current = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read service during no-op rejection")
            .expect("service should remain present");
        assert_eq!(
            current.status(),
            ServiceStatus::Running,
            "unchanged redeploy rejection should not flip service status"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let after = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read final spec after no-op rejection")
        .expect("final spec should remain present");

    assert_eq!(
        after.manifest_id, baseline.manifest_id,
        "unchanged redeploy should keep the active manifest id"
    );
    assert_eq!(
        after.assigned_replica_ids(),
        baseline.assigned_replica_ids(),
        "unchanged redeploy should not churn task assignments"
    );
    assert_eq!(
        after.service_epoch, baseline.service_epoch,
        "unchanged redeploy should not bump service generation"
    );
    assert_eq!(
        after.phase_version, baseline.phase_version,
        "unchanged redeploy should not mutate causal phase ordering"
    );
});

local_test!(services_redeploy_rolls_back_on_failed_replacement, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "redeploy-rollback";
    let manifest_name = "redeploy-rollback";

    let tasks = vec![TaskTemplateSpecValue {
        name: "echo".into(),
        execution: ExecutionSpec {
            command: vec![
                "sh".into(),
                "-c".into(),
                "while true; do sleep 1; done".into(),
            ],
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            ..empty_service_execution("alpine:3.20")
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
        placement_preferences: Vec::new(),
    }];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit baseline deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "baseline deployment should reach running"
    );

    let baseline_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read baseline spec")
        .expect("baseline spec present");
    let baseline_manifest_id = baseline_spec.manifest_id;
    let baseline_task_ids = baseline_spec.assigned_replica_ids();

    let mut failing_tasks = tasks;
    failing_tasks[0].execution.cpu_millis = 500_000;
    failing_tasks[0].execution.memory_bytes = 8 * 1024 * 1024 * 1024;

    node.node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, failing_tasks)
        .await
        .expect("submit failing redeployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "failed redeployment should roll back to running"
    );

    let rolled_back = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read rolled back spec")
        .expect("rolled back spec present");
    assert_eq!(
        rolled_back.manifest_id, baseline_manifest_id,
        "failed rollout should restore previous manifest generation"
    );
    assert_eq!(
        rolled_back.assigned_replica_ids(),
        baseline_task_ids,
        "failed rollout should restore previous task assignments"
    );
});

local_test!(services_redeploy_enforces_max_failures_budget, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "redeploy-max-failures";
    let manifest_name = "redeploy-max-failures";

    let tasks = vec![TaskTemplateSpecValue {
        name: "echo".into(),
        execution: ExecutionSpec {
            command: vec![
                "sh".into(),
                "-c".into(),
                "while true; do sleep 1; done".into(),
            ],
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            ..empty_service_execution("alpine:3.20")
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
        placement_preferences: Vec::new(),
    }];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit baseline deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "baseline deployment should reach running"
    );

    let baseline_spec = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read baseline spec")
        .expect("baseline spec present");
    let baseline_manifest_id = baseline_spec.manifest_id;
    let baseline_task_ids = baseline_spec.assigned_replica_ids();

    let mut failing_tasks = tasks;
    failing_tasks[0].execution.cpu_millis = 500_000;
    failing_tasks[0].execution.memory_bytes = 8 * 1024 * 1024 * 1024;

    let strategy = ServiceUpdateStrategy {
        rolling: ServiceRollingUpdatePolicy {
            parallelism: 1,
            order: ServiceRolloutOrder::StartFirst,
            max_failures: 2,
            auto_rollback: true,
        },
        ..ServiceUpdateStrategy::default()
    };

    node.node
        .service_controller
        .submit_deployment_with_strategy(
            Uuid::new_v4(),
            manifest_name,
            service_name,
            failing_tasks,
            strategy,
        )
        .await
        .expect("submit failing redeployment with max_failures");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "failing redeployment should roll back to running"
    );

    let rolled_back = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read rolled back spec")
        .expect("rolled back spec present");
    assert_eq!(
        rolled_back.manifest_id, baseline_manifest_id,
        "failing rollout should keep previous manifest after rollback"
    );
    assert_eq!(
        rolled_back.assigned_replica_ids(),
        baseline_task_ids,
        "failing rollout should keep previous task ids after rollback"
    );
    assert_eq!(
        rolled_back.rollout.phase,
        ServiceRolloutPhase::Idle,
        "rollback-complete state should return rollout phase to idle"
    );
    assert_eq!(
        rolled_back.rollout.failed_steps, 2,
        "rollout should fail once the configured failure budget is exhausted"
    );
    assert!(
        rolled_back.rollout.last_error.is_some(),
        "rollout diagnostics should include the last failure reason"
    );
});

local_test!(
    services_redeploy_stop_first_stops_previous_before_replacement_visible,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();
        let node = TestNode::new().await;

        let service_name = "redeploy-stop-first";
        let manifest_name = "redeploy-stop-first";

        let mut tasks = vec![TaskTemplateSpecValue {
            name: "echo".into(),
            execution: ExecutionSpec {
                command: vec![
                    "sh".into(),
                    "-c".into(),
                    "while true; do sleep 1; done".into(),
                ],
                cpu_millis: 100,
                memory_bytes: 64 * 1024 * 1024,
                ..empty_service_execution("alpine:3.20")
            },
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
            placement_preferences: Vec::new(),
        }];

        let service_id = node
            .node
            .service_controller
            .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
            .await
            .expect("submit baseline deployment");

        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "baseline deployment should reach running"
        );

        let baseline_spec = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read baseline spec")
            .expect("baseline spec present");
        let old_task_id = baseline_spec
            .assigned_replica_id(0)
            .expect("baseline spec should include one task id");

        tasks[0].execution.image = "alpine:3.19".into();
        let strategy = rollout_strategy(1, ServiceRolloutOrder::StopFirst, 1, true);

        node.node
            .service_controller
            .submit_deployment_with_strategy(
                Uuid::new_v4(),
                manifest_name,
                service_name,
                tasks,
                strategy,
            )
            .await
            .expect("submit stop-first redeployment");

        let deadline = Instant::now() + Duration::from_secs(12);
        let mut verified_order = false;
        while Instant::now() < deadline {
            let tasks = node
                .node
                .workload_manager
                .list_workloads(&TaskStateFilter::all())
                .await
                .expect("list tasks during stop-first rollout");
            let replacement_visible = tasks.iter().any(|task| {
                task.id != old_task_id
                    && task
                        .service_owner()
                        .map(|meta| meta.service_name == service_name)
                        .unwrap_or(false)
            });

            if replacement_visible {
                let states = node
                    .node
                    .workload_manager
                    .workload_phase_snapshot(&[old_task_id])
                    .await
                    .expect("snapshot old task state");
                let old_state = states.first().and_then(|(_, state)| state.clone());
                assert!(
                    !matches!(old_state, Some(WorkloadPhase::Running)),
                    "stop_first rollout should not expose replacement while previous task is still running"
                );
                verified_order = true;
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(
            verified_order,
            "replacement task should become visible during stop-first rollout"
        );

        assert!(
            wait_until(
                Duration::from_secs(30),
                Duration::from_millis(100),
                || async {
                    if let Ok(Some(spec)) = node.node.service_controller.registry().get(service_id)
                    {
                        return matches!(
                            spec.status(),
                            ServiceStatus::Running | ServiceStatus::Failed
                        );
                    }
                    false
                }
            )
            .await,
            "stop-first ordering test should drive rollout to a terminal state before shutdown"
        );
    }
);

local_test!(services_redeploy_parallelism_two_allows_batched_surge, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "redeploy-parallelism-two";
    let manifest_name = "redeploy-parallelism-two";

    let mut tasks = vec![TaskTemplateSpecValue {
        name: "echo".into(),
        execution: ExecutionSpec {
            command: vec![
                "sh".into(),
                "-c".into(),
                "while true; do sleep 1; done".into(),
            ],
            ..empty_service_execution("alpine:3.20")
        },
        depends_on: Vec::new(),
        replicas: 4,
        readiness: None,
        public_port: None,
        public_protocol: None,
        placement_preferences: Vec::new(),
    }];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit baseline deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "baseline deployment should reach running"
    );

    tasks[0].execution.image = "alpine:3.19".into();
    let strategy = rollout_strategy(2, ServiceRolloutOrder::StartFirst, 1, true);
    node.node
        .service_controller
        .submit_deployment_with_strategy(
            Uuid::new_v4(),
            manifest_name,
            service_name,
            tasks,
            strategy,
        )
        .await
        .expect("submit parallel rollout redeployment");

    let desired = 4usize;
    let deadline = Instant::now() + Duration::from_secs(16);
    let mut max_seen = 0usize;
    let mut deploying_seen = false;
    let mut terminal_seen = false;
    while Instant::now() < deadline {
        if let Ok(Some(spec)) = node.node.service_controller.registry().get(service_id) {
            match spec.status() {
                ServiceStatus::Deploying => deploying_seen = true,
                ServiceStatus::Running
                | ServiceStatus::Failed
                | ServiceStatus::VolumeUnavailable => {
                    if deploying_seen {
                        terminal_seen = true;
                    }
                }
                ServiceStatus::Stopping | ServiceStatus::Stopped => {}
            }
        }

        let count = list_active_service_tasks(&node.node.workload_manager, service_name)
            .await
            .len();
        max_seen = max_seen.max(count);

        if terminal_seen {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    assert!(
        deploying_seen,
        "parallel rollout should enter deploying phase"
    );
    assert!(
        terminal_seen,
        "parallel rollout should reach a terminal status after deploying"
    );
    assert!(
        max_seen >= desired + 2,
        "parallelism=2 start-first rollout should temporarily run at least two additional active tasks; saw max {max_seen}"
    );
});

local_test!(services_redeploy_auto_rollback_disabled_marks_failed, {
    let _guard = RuntimeBackendOverrideGuard::install_default();
    let node = TestNode::new().await;

    let service_name = "redeploy-no-rollback";
    let manifest_name = "redeploy-no-rollback";

    let tasks = vec![TaskTemplateSpecValue {
        name: "echo".into(),
        execution: ExecutionSpec {
            command: vec![
                "sh".into(),
                "-c".into(),
                "while true; do sleep 1; done".into(),
            ],
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            ..empty_service_execution("alpine:3.20")
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
        placement_preferences: Vec::new(),
    }];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit baseline deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "baseline deployment should reach running"
    );

    let failing_manifest_id = Uuid::new_v4();
    let mut failing_tasks = tasks;
    failing_tasks[0].execution.cpu_millis = 500_000;
    failing_tasks[0].execution.memory_bytes = 8 * 1024 * 1024 * 1024;
    let strategy = rollout_strategy(1, ServiceRolloutOrder::StartFirst, 1, false);

    node.node
        .service_controller
        .submit_deployment_with_strategy(
            failing_manifest_id,
            manifest_name,
            service_name,
            failing_tasks,
            strategy,
        )
        .await
        .expect("submit non-rollback failing redeployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Failed
        )
        .await,
        "failed rollout with auto_rollback=false should remain failed"
    );

    let failed = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read failed spec")
        .expect("failed spec present");
    assert_eq!(
        failed.manifest_id, failing_manifest_id,
        "auto_rollback=false should keep the failing manifest generation active"
    );
    assert_eq!(
        failed.rollout.phase,
        ServiceRolloutPhase::Failed,
        "failed rollout should expose failed rollout phase"
    );
});

local_test!(services_redeploy_rollback_failure_marks_failed, {
    let manager = Arc::new(CreateFailureAfterBaselineRuntimeBackend::default());
    let _guard = RuntimeBackendOverrideGuard::install(manager.clone());
    let node = TestNode::new().await;

    let service_name = "redeploy-rollback-failure";
    let manifest_name = "redeploy-rollback-failure";

    let tasks = vec![TaskTemplateSpecValue {
        name: "echo".into(),
        execution: ExecutionSpec {
            command: vec![
                "sh".into(),
                "-c".into(),
                "while true; do sleep 1; done".into(),
            ],
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            ..empty_service_execution("alpine:3.20")
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
        placement_preferences: Vec::new(),
    }];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks.clone())
        .await
        .expect("submit baseline deployment");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "baseline deployment should reach running"
    );

    let failing_manifest_id = Uuid::new_v4();
    let mut failing_tasks = tasks;
    failing_tasks[0].execution.image = "alpine:3.19".into();
    let strategy = rollout_strategy(1, ServiceRolloutOrder::StopFirst, 1, true);
    manager.enable_create_failures();

    node.node
        .service_controller
        .submit_deployment_with_strategy(
            failing_manifest_id,
            manifest_name,
            service_name,
            failing_tasks,
            strategy,
        )
        .await
        .expect("submit failing redeployment with rollback enabled");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Failed
        )
        .await,
        "rollback failure should mark service failed"
    );

    let failed = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read failed spec")
        .expect("failed spec present");
    assert_eq!(
        failed.manifest_id, failing_manifest_id,
        "failed rollback should not restore the previous manifest generation"
    );
    assert_eq!(
        failed.status(),
        ServiceStatus::Failed,
        "rollback-failure path should mark the service failed"
    );
});
