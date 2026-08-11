use super::support::*;
use crate::common;

local_test!(services_volume_unavailable_enters_and_recovers, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let cluster = TestNode::new_cluster_inproc_with_config(1, ClusterConfig::default())
        .await
        .expect("cluster should start");
    let node = &cluster[0];

    let imported = tempdir().expect("create imported volume path");
    let volume_name = "svc-imported-data";
    let volume_id = import_local_volume_for_service(
        &node.node.volumes_client,
        volume_name,
        node.id(),
        imported.path(),
    )
    .await;

    let service_name = "volume-unavailable-service";
    let manifest_name = "volume-unavailable-service";
    let tasks = vec![TaskTemplateSpecValue {
        name: "db".into(),
        execution: ExecutionSpec {
            command: vec![
                "sh".into(),
                "-c".into(),
                "while true; do sleep 1; done".into(),
            ],
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            volumes: vec![TaskVolumeMount {
                volume_id,
                volume_name: volume_name.to_string(),
                target: "/var/lib/postgresql/data".to_string(),
                read_only: false,
            }],
            ..empty_service_execution("postgres:16")
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: None,
        public_protocol: None,
        public_ingress: Default::default(),
        placement_preferences: Vec::new(),
        autoscale: None,
    }];

    let service_id = node
        .node
        .service_controller
        .submit_deployment(Uuid::new_v4(), manifest_name, service_name, tasks)
        .await
        .expect("submit service deployment with imported volume");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should reach running while the imported path exists"
    );

    let running = node
        .node
        .service_controller
        .registry()
        .get(service_id)
        .expect("read running service spec")
        .expect("running service spec present");
    let task_id = running
        .assigned_replica_id(0)
        .expect("service should own one task id");

    assert!(
        wait_for_task_state(
            &node.node.workload_manager,
            task_id,
            WorkloadPhase::Running,
            Duration::from_secs(10)
        )
        .await,
        "task should reach running while the imported path exists"
    );

    fs::remove_dir_all(imported.path()).expect("remove imported volume path");

    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::VolumeUnavailable
        )
        .await,
        "service should report volume_unavailable after the imported path disappears"
    );
    assert!(
        wait_for_task_state(
            &node.node.workload_manager,
            task_id,
            WorkloadPhase::VolumeUnavailable,
            Duration::from_secs(10)
        )
        .await,
        "task should report volume_unavailable after the imported path disappears"
    );

    fs::create_dir_all(imported.path()).expect("recreate imported volume path");

    assert!(
        wait_for_task_state(
            &node.node.workload_manager,
            task_id,
            WorkloadPhase::Running,
            Duration::from_secs(15)
        )
        .await,
        "task should recover once the imported path is restored"
    );
    assert!(
        wait_for_service_status(
            &node.node.service_controller,
            service_id,
            ServiceStatus::Running
        )
        .await,
        "service should recover to running once the imported path is restored"
    );
});

local_test!(
    services_rwo_rollout_stops_old_task_before_starting_new_task,
    {
        let _guard = RuntimeBackendOverrideGuard::install_default();

        let cluster = TestNode::new_cluster_inproc_with_config(1, ClusterConfig::default())
            .await
            .expect("cluster should start");
        let node = &cluster[0];

        let imported = tempdir().expect("create imported volume path");
        let volume_name = "svc-rollout-data";
        let volume_id = import_local_volume_for_service(
            &node.node.volumes_client,
            volume_name,
            node.id(),
            imported.path(),
        )
        .await;

        let service_name = "rwo-rollout-service";
        let manifest_name = "rwo-rollout-service";
        let mut tasks = vec![TaskTemplateSpecValue {
            name: "database".into(),
            execution: ExecutionSpec {
                command: vec![
                    "sh".into(),
                    "-c".into(),
                    "while true; do sleep 1; done".into(),
                ],
                cpu_millis: 100,
                memory_bytes: 64 * 1024 * 1024,
                volumes: vec![TaskVolumeMount {
                    volume_id,
                    volume_name: volume_name.to_string(),
                    target: "/var/lib/database".to_string(),
                    read_only: false,
                }],
                ..empty_service_execution("alpine:3.20")
            },
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
            public_ingress: Default::default(),
            placement_preferences: Vec::new(),
            autoscale: None,
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

        let baseline = node
            .node
            .service_controller
            .registry()
            .get(service_id)
            .expect("read baseline service")
            .expect("baseline service should exist");
        let old_task_id = baseline
            .assigned_replica_id(0)
            .expect("baseline service should own one task");

        tasks[0].execution.cpu_millis = 200;
        node.node
            .service_controller
            .submit_deployment_with_strategy(
                Uuid::new_v4(),
                manifest_name,
                service_name,
                tasks,
                rollout_strategy(1, ServiceRolloutOrder::StartFirst, 1, true),
            )
            .await
            .expect("submit read-write-once redeployment");

        let deadline = Instant::now() + Duration::from_secs(12);
        let mut replacement_seen = false;
        while Instant::now() < deadline {
            let workloads = node
                .node
                .workload_manager
                .list_workloads(&TaskStateFilter::all())
                .await
                .expect("list rollout tasks");
            let new_task_exists = workloads.iter().any(|task| {
                task.id != old_task_id
                    && task
                        .service_owner()
                        .is_some_and(|owner| owner.service_name == service_name)
            });
            if new_task_exists {
                let states = node
                    .node
                    .workload_manager
                    .workload_phase_snapshot(&[old_task_id])
                    .await
                    .expect("read old task state");
                assert!(
                    matches!(
                        states.first().and_then(|(_, state)| state.clone()),
                        None | Some(WorkloadPhase::Stopped)
                            | Some(WorkloadPhase::Failed)
                            | Some(WorkloadPhase::Exited(_))
                    ),
                    "replacement must wait until the old task and its volume mount are stopped"
                );
                replacement_seen = true;
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(replacement_seen, "replacement task should become visible");
        assert!(
            wait_for_service_status(
                &node.node.service_controller,
                service_id,
                ServiceStatus::Running
            )
            .await,
            "read-write-once rollout should finish"
        );
    }
);
