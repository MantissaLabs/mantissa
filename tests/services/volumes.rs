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
