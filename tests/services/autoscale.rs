use super::support::*;
use crate::common;

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
