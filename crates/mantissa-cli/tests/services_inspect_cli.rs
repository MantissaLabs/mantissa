use capnp::Error;
use mantissa_protocol::{server::cluster_session, services, workload};
use std::{cell::Cell, path::Path, process::Output, rc::Rc, time::Duration};
use tokio::{process::Command, task::LocalSet, time::timeout};
use uuid::Uuid;

const SERVICE_ID: Uuid = Uuid::from_u128(1);
const MANIFEST_ID: Uuid = Uuid::from_u128(2);
const TASK_ID: Uuid = Uuid::from_u128(3);
const NETWORK_ID: Uuid = Uuid::from_u128(4);
const VOLUME_ID: Uuid = Uuid::from_u128(5);
const SECRET_VERSION: Uuid = Uuid::from_u128(6);
const FAILURE: &str = "readiness check failed: the backend returned HTTP 503 while waiting for its database connection to become available";

#[derive(Clone, Copy)]
enum Example {
    Deploying,
    Stopped,
    Discovery,
}

struct InspectionServer {
    name_requests: Rc<Cell<usize>>,
    example: Example,
}

impl services::services::Server for InspectionServer {
    /// Returns only identity during name lookup so the CLI must fetch current configuration separately.
    async fn inspect(
        self: Rc<Self>,
        params: services::services::InspectParams,
        mut results: services::services::InspectResults,
    ) -> Result<(), Error> {
        self.name_requests.set(self.name_requests.get() + 1);

        let name = if matches!(self.example, Example::Discovery) {
            "discovery-demo"
        } else {
            "api"
        };
        if params.get()?.get_selector()?.to_str()? != name {
            return Err(Error::failed("service not found".to_string()));
        }

        results.get().init_service().set_id(SERVICE_ID.as_bytes());
        Ok(())
    }

    /// Supplies a known current snapshot without implementing list or any mutation RPC.
    async fn status(
        self: Rc<Self>,
        params: services::services::StatusParams,
        mut results: services::services::StatusResults,
    ) -> Result<(), Error> {
        if params.get()?.get_service_id()? != SERVICE_ID.as_bytes() {
            return Err(Error::failed("service not found".to_string()));
        }

        match self.example {
            Example::Deploying => write_snapshot(results.get().init_snapshot(), false),
            Example::Stopped => write_snapshot(results.get().init_snapshot(), true),
            Example::Discovery => write_discovery_snapshot(results.get().init_snapshot()),
        }
        Ok(())
    }
}

struct Session {
    services: services::services::Client,
}

impl cluster_session::Server for Session {
    /// Exposes only the service capability so inspection cannot depend on unrelated cluster APIs.
    async fn get_services(
        self: Rc<Self>,
        _params: cluster_session::GetServicesParams,
        mut results: cluster_session::GetServicesResults,
    ) -> Result<(), Error> {
        results.get().set_services(self.services.clone());
        Ok(())
    }
}

/// Covers configuration that the existing list response model does not preserve.
fn write_snapshot(mut snapshot: services::service_status_snapshot::Builder<'_>, stopped: bool) {
    let mut service = snapshot.reborrow().init_service();
    service.set_id(SERVICE_ID.as_bytes());
    service.set_manifest_id(MANIFEST_ID.as_bytes());
    service.set_manifest_name("api manifest");
    service.set_service_name("api");
    service.set_service_epoch(7);
    service.set_updated_at("2026-09-09T12:00:00Z");

    if stopped {
        service.set_status(services::ServiceStatus::Stopped);
        service.set_status_detail("stopped by operator");
        return;
    }

    service.set_status(services::ServiceStatus::Deploying);
    service.set_status_detail(FAILURE);
    service
        .reborrow()
        .init_replica_ids(1)
        .set(0, TASK_ID.as_bytes());

    let mut rollout = service.reborrow().init_rollout();
    rollout.set_phase(services::RolloutPhase::RollingForward);
    rollout.set_total_steps(3);
    rollout.set_completed_steps(1);
    rollout.set_failed_steps(1);
    rollout.set_max_failures(2);
    rollout.set_last_error(FAILURE);

    service
        .reborrow()
        .init_admission_policy()
        .set_mode(workload::AdmissionMode::Gang);

    let mut deployment = service.reborrow().init_deployment_policy();
    deployment.set_progress_deadline_secs(120);
    deployment.set_healthy_deadline_secs(60);
    deployment.set_min_healthy_secs(5);

    let mut update = service.reborrow().init_update_strategy().init_rolling();
    update.set_order(services::RolloutOrder::StartFirst);
    update.set_parallelism(2);
    update.set_max_failures(2);
    update.set_auto_rollback(true);

    let mut previous = service.reborrow().init_previous_generation();
    previous.set_manifest_id(Uuid::from_u128(10).as_bytes());
    previous.set_service_epoch(6);

    let mut template = service.init_task_templates(1).get(0);
    template.set_name("web");
    template.set_image("example.invalid/api:v2");
    template.set_replicas(3);
    template.set_cpu_millis(500);
    template.set_memory_bytes(536_870_912);
    template.set_gpu_count(1);
    template.set_tty(true);

    let mut command = template.reborrow().init_command(3);
    command.set(0, "sh");
    command.set(1, "-c");
    command.set(2, "echo 'two words'");

    template.reborrow().init_depends_on(1).set(0, "database");
    template.set_termination_grace_period_secs(15);
    template
        .reborrow()
        .init_pre_stop_command(1)
        .set(0, "/app/drain");

    let mut restart = template.reborrow().init_restart_policy();
    restart.set_name(services::RestartPolicyName::OnFailure);
    restart.set_max_retry_count(4);

    let mut autoscale = template.reborrow().init_autoscale();
    autoscale.set_min_replicas(2);
    autoscale.set_max_replicas(8);
    autoscale.set_cooldown_secs(30);
    autoscale.set_scale_down_stabilization_secs(120);
    autoscale.set_sample_window_secs(10);
    autoscale.set_trigger_windows(3);
    let mut metrics = autoscale.init_metrics(2);
    metrics
        .reborrow()
        .get(0)
        .set_kind(services::AutoscaleMetricKind::Cpu);
    metrics.reborrow().get(0).set_target_percent(75);
    metrics
        .reborrow()
        .get(1)
        .set_kind(services::AutoscaleMetricKind::Memory);
    metrics.get(1).set_target_percent(80);

    let mut placement = template.reborrow().init_placement();
    placement.set_strategy(workload::PlacementStrategy::Binpack);
    let mut constraint = placement.init_constraints(1).get(0);
    constraint.reborrow().init_selector().set_node_label("zone");
    constraint.set_operator(workload::PlacementConstraintOperator::Eq);
    constraint.set_value("west");
    template
        .reborrow()
        .init_service_placement_preferences(1)
        .set(0, services::ServicePlacementPreference::TaskAntiAffinity);

    let mut readiness = template.reborrow().init_readiness();
    readiness.set_kind(services::ReadinessProbeKind::Http);
    readiness.set_port(8080);
    readiness.set_path("/ready");
    readiness.set_interval_ms(2000);
    readiness.set_timeout_ms(300);
    readiness.set_failure_threshold(2);

    let mut liveness = template.reborrow().init_liveness();
    liveness.set_kind(services::LivenessProbeKind::Exec);
    liveness.reborrow().init_command(1).set(0, "/app/health");
    liveness.set_interval_ms(10000);
    liveness.set_timeout_ms(1000);
    liveness.set_failure_threshold(3);
    liveness.set_start_period_ms(5000);

    template.set_public_port(443);
    template.set_public_protocol(services::PublicProtocol::TcpUdp);
    template.set_public_ingress(services::PublicIngressPolicy::IngressPool);
    template.set_public_ingress_pool("public-web");

    let mut port = template.reborrow().init_ports(1).get(0);
    port.set_name("http");
    port.set_host_ip("::1");
    port.set_host_port(18080);
    port.set_target_port(8080);
    port.set_protocol(workload::PortProtocol::Tcp);

    let mut network = template.reborrow().init_networks(1).get(0);
    network.set_name("backend");
    network.set_network_id(NETWORK_ID.as_bytes());

    let mut mount = template.reborrow().init_volumes(1).get(0);
    mount.set_volume_name("data");
    mount.set_volume_id(VOLUME_ID.as_bytes());
    mount.set_target("/data");
    mount.set_read_only(true);

    let mut env = template.reborrow().init_env(3);
    env.reborrow().get(0).set_name("MODE");
    env.reborrow().get(0).set_value("production");
    env.reborrow().get(1).set_name("API_KEY");
    env.reborrow()
        .get(1)
        .set_value("must-not-print-secret-backed-value");
    env.reborrow().get(1).init_secret().set_name("api-key");
    env.reborrow().get(2).set_name("EMPTY");
    env.get(2).set_value("");

    let mut file = template.init_secret_files(1).get(0);
    file.set_path("/run/secrets/tls");
    let mut secret = file.reborrow().init_secret();
    secret.set_name("tls");
    secret.set_version_id(SECRET_VERSION.as_bytes());
    file.set_mode(0o440);
    file.init_ownership().init_fs_group().set_gid(1000);

    let mut progress = snapshot.init_tasks(1).get(0);
    progress.set_name("web");
    progress.set_desired(3);
    progress.set_assigned(1);
    progress.set_running(0);
    progress.set_failed(1);
    progress.set_detail(FAILURE);
}

/// Runs the real binary with an isolated socket and a deadline to catch stalled RPCs.
async fn run_inspect(state_dir: &Path, selector: &str, details: bool) -> Output {
    timeout(
        Duration::from_secs(15),
        Command::new(env!("CARGO_BIN_EXE_mantissa"))
            .args(["services", "inspect", selector])
            .args(details.then_some("--details"))
            .env("MANTISSA_STATE_DIR", state_dir)
            .env("RUST_LOG", "off")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("inspection should finish")
    .expect("run CLI")
}

#[tokio::test]
/// Checks complete output and name/UUID selection through the public CLI and local RPC transport.
async fn services_inspect_reports_configuration_and_progress() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            let name_requests = Rc::new(Cell::new(0));
            let services = capnp_rpc::new_client(InspectionServer {
                name_requests: name_requests.clone(),
                example: Example::Deploying,
            });
            let session = capnp_rpc::new_client(Session { services });
            mantissa_net::unix_socket::start_unix_socket_server_at(
                session,
                dir.path().join("mantissa.sock"),
            )
            .await
            .expect("start test service socket");

            let by_name = run_inspect(dir.path(), "api", true).await;
            assert!(
                by_name.status.success(),
                "{}",
                String::from_utf8_lossy(&by_name.stderr)
            );

            let stdout = String::from_utf8(by_name.stdout).expect("inspection output is UTF-8");
            let normalized = stdout.split_whitespace().collect::<Vec<_>>().join(" ");
            for expected in [
                "SERVICE api",
                "Generation 7",
                FAILURE,
                "1/3 steps",
                "Rollout failures 1",
                "Admission gang",
                "progress 2m",
                "healthy 1m",
                "minimum healthy 5s",
                "Update rolling, start first",
                "automatic rollback enabled",
                "generation 6",
                "Image example.invalid/api:v2",
                "Command sh",
                "Arguments -c",
                "echo 'two words'",
                "500m CPU, 512 MiB memory (536870912 bytes), GPU count 1",
                "Depends on database",
                "TTY enabled",
                "Grace period 15s",
                "Pre-stop /app/drain",
                "maximum retries 4",
                "Autoscaling 2-8 replicas",
                "CPU target 75%",
                "memory target 80%",
                "Placement binpack",
                "node.labels.zone == \"west\"",
                "prefer nodes with fewer replicas of this template",
                "Readiness HTTP :8080/ready",
                "Liveness exec /app/health",
                "start period 5s",
                "Public port 443/tcp+udp (ingress pool public-web)",
                "http [::1]:18080->8080/tcp",
                "read-only",
                "MODE=\"production\"",
                "API_KEY <- secret api-key (latest)",
                "EMPTY=\"\"",
                "mode 0440",
                "ownership filesystem group 1000",
                "web 3 1 0 1 failed",
                "REPLICA ASSIGNMENTS",
            ] {
                assert!(
                    normalized.contains(expected),
                    "missing {expected:?}:\n{stdout}"
                );
            }

            for id in [
                SERVICE_ID,
                MANIFEST_ID,
                TASK_ID,
                NETWORK_ID,
                VOLUME_ID,
                SECRET_VERSION,
            ] {
                assert!(
                    stdout.contains(&id.to_string()),
                    "missing full identifier {id}"
                );
            }

            assert!(!stdout.contains("must-not-print-secret-backed-value"));
            assert!(stdout.ends_with('\n'));
            assert_eq!(name_requests.get(), 1);

            let by_id = run_inspect(dir.path(), &SERVICE_ID.to_string(), true).await;
            assert!(by_id.status.success());
            assert_eq!(
                String::from_utf8(by_id.stdout).expect("inspection output is UTF-8"),
                stdout
            );
            assert_eq!(
                name_requests.get(),
                1,
                "UUID inspection should skip name lookup"
            );

            let compact = run_inspect(dir.path(), &SERVICE_ID.to_string(), false).await;
            assert!(compact.status.success());

            let compact = String::from_utf8(compact.stdout).expect("inspection output is UTF-8");

            let compact = compact.split_whitespace().collect::<Vec<_>>().join(" ");
            assert_eq!(
                compact.matches(FAILURE).count(),
                1,
                "the same failure should not be repeated in three sections"
            );
            assert!(compact.contains("web 3 1 0 1 failed"));
            assert!(compact.contains("API_KEY <- secret api-key (latest)"));

            for selector in ["missing", "", "00000000-0000-0000-0000-000000000099"] {
                let missing = run_inspect(dir.path(), selector, false).await;
                assert!(!missing.status.success());
                assert!(
                    missing.stdout.is_empty(),
                    "failed inspection should not print a partial report"
                );
                assert!(String::from_utf8_lossy(&missing.stderr).contains("service"));
            }
        })
        .await;
}

#[tokio::test]
/// Ensures a retained stopped service remains inspectable even without templates or progress.
async fn services_inspect_reports_stopped_service_without_optional_configuration() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            let services = capnp_rpc::new_client(InspectionServer {
                name_requests: Rc::new(Cell::new(0)),
                example: Example::Stopped,
            });
            let session = capnp_rpc::new_client(Session { services });
            mantissa_net::unix_socket::start_unix_socket_server_at(
                session,
                dir.path().join("mantissa.sock"),
            )
            .await
            .expect("start test service socket");

            let output = run_inspect(dir.path(), "api", true).await;
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );

            let stdout = String::from_utf8(output.stdout).expect("inspection output is UTF-8");
            let normalized = stdout.split_whitespace().collect::<Vec<_>>().join(" ");
            for expected in [
                "Status stopped",
                "stopped by operator",
                "Templates none",
                "Progress unavailable",
                "Assignments none",
                "Deadlines default",
            ] {
                assert!(
                    normalized.contains(expected),
                    "missing {expected:?}:\n{stdout}"
                );
            }
        })
        .await;
}

/// Reproduces the two-template example that exposed excessive empty fields and long command lines.
fn write_discovery_snapshot(mut snapshot: services::service_status_snapshot::Builder<'_>) {
    let mut service = snapshot.reborrow().init_service();
    service.set_id(SERVICE_ID.as_bytes());
    service.set_manifest_id(MANIFEST_ID.as_bytes());
    service.set_manifest_name("discovery-demo");
    service.set_service_name("discovery-demo");
    service.set_status(services::ServiceStatus::Running);
    service
        .reborrow()
        .init_admission_policy()
        .set_mode(workload::AdmissionMode::Incremental);
    service.set_updated_at("2026-09-09T17:51:08.223831604+00:00");

    let mut deployment = service.reborrow().init_deployment_policy();
    deployment.set_progress_deadline_secs(600);
    deployment.set_healthy_deadline_secs(600);
    deployment.set_min_healthy_secs(1);

    let mut update = service.reborrow().init_update_strategy().init_rolling();
    update.set_order(services::RolloutOrder::StartFirst);
    update.set_parallelism(1);
    update.set_max_failures(1);
    update.set_auto_rollback(true);

    let mut assignments = service.reborrow().init_replica_assignment_segments(2);
    for (index, (name, replicas)) in [("backend", 4), ("frontend", 1)].into_iter().enumerate() {
        let mut assignment = assignments.reborrow().get(index as u32);
        assignment.set_template_name(name);
        assignment.set_first_replica(1);
        assignment.set_replica_count(replicas);
    }

    let mut templates = service.reborrow().init_task_templates(2);
    for (index, (name, replicas, image, script)) in [
        ("backend", 4, "busybox:1.36", "mkdir -p /www && hostname >/www/index.html && exec httpd -f -p 8000 -h /www"),
        ("frontend", 1, "curlimages/curl:8.9.1", "while true; do printf '\\n---\\n' && date; curl --connect-timeout 5 --max-time 5 --no-keepalive --retry 0 -sS -w '\\n[curl] http=%{http_code} remote=%{remote_ip} dns=%{time_namelookup}s connect=%{time_connect}s start=%{time_starttransfer}s total=%{time_total}s\\n' http://backend.discovery-demo.demo-network.svc.mantissa:8000; rc=$?; echo \"[curl] rc=$rc\"; sleep 2; done"),
    ].into_iter().enumerate() {
        let mut template = templates.reborrow().get(index as u32);
        template.set_name(name);
        template.set_image(image);
        template.set_replicas(replicas);
        template.set_cpu_millis(200);
        template.set_memory_bytes(64 * 1024 * 1024);

        let mut command = template.reborrow().init_command(3);
        command.set(0, "sh");
        command.set(1, "-c");
        command.set(2, script);

        let mut network = template.reborrow().init_networks(1).get(0);
        network.set_name("demo-network");
        network.set_network_id(NETWORK_ID.as_bytes());
    }

    templates
        .reborrow()
        .get(1)
        .init_depends_on(1)
        .set(0, "backend");

    let mut backend = templates.get(0);
    backend.set_public_port(8000);

    let mut readiness = backend.reborrow().init_readiness();
    readiness.set_kind(services::ReadinessProbeKind::Http);
    readiness.set_port(8000);
    readiness.set_path("/");
    readiness.set_interval_ms(2000);
    readiness.set_timeout_ms(300);
    readiness.set_failure_threshold(1);

    let mut liveness = backend.init_liveness();
    liveness.set_kind(services::LivenessProbeKind::Http);
    liveness.set_port(8000);
    liveness.set_path("/");
    liveness.set_interval_ms(2000);
    liveness.set_timeout_ms(1000);
    liveness.set_failure_threshold(2);
    liveness.set_start_period_ms(1000);

    let mut progress = snapshot.init_tasks(2);
    for (index, (name, replicas)) in [("backend", 4), ("frontend", 1)].into_iter().enumerate() {
        let mut row = progress.reborrow().get(index as u32);
        row.set_name(name);
        row.set_desired(replicas);
        row.set_assigned(replicas);
        row.set_running(replicas);
    }
}

#[tokio::test]
/// Protects the readable default layout while checking that omitted settings remain available on demand.
async fn services_inspect_keeps_default_output_compact() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            let services = capnp_rpc::new_client(InspectionServer {
                name_requests: Rc::new(Cell::new(0)),
                example: Example::Discovery,
            });
            let session = capnp_rpc::new_client(Session { services });
            mantissa_net::unix_socket::start_unix_socket_server_at(
                session,
                dir.path().join("mantissa.sock"),
            )
            .await
            .expect("start test service socket");

            let compact = run_inspect(dir.path(), "discovery-demo", false).await;
            assert!(
                compact.status.success(),
                "{}",
                String::from_utf8_lossy(&compact.stderr)
            );

            let compact = String::from_utf8(compact.stdout).expect("inspection output is UTF-8");
            assert_eq!(compact, include_str!("fixtures/services_inspect.txt"));
            assert!(
                !compact.contains('\u{1b}'),
                "redirected output should not contain terminal styling"
            );

            let detailed = run_inspect(dir.path(), "discovery-demo", true).await;
            assert!(detailed.status.success());
            let detailed = String::from_utf8(detailed.stdout).expect("inspection output is UTF-8");

            for label in [
                "Volumes",
                "Environment",
                "Secret files",
                "Constraints",
                "Preferences",
            ] {
                assert!(
                    !compact
                        .lines()
                        .any(|line| line.trim_start().starts_with(label))
                );
                assert!(
                    detailed
                        .lines()
                        .any(|line| line.trim_start().starts_with(label)
                            && line.trim_end().ends_with("none")),
                    "empty {label} should fit on one line"
                );
            }

            assert!(detailed.contains("67108864 bytes"));
            assert!(detailed.contains("REPLICA ASSIGNMENTS"));
            assert!(
                !detailed.contains("1-1"),
                "a single assignment should not be written as a range"
            );
        })
        .await;
}
