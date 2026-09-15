use capnp::Error;
use mantissa_protocol::{server::cluster_session, task, workload};
use std::{path::Path, process::Output, rc::Rc, time::Duration};
use tokio::{process::Command, task::LocalSet, time::timeout};
use uuid::Uuid;

const TASK_ID: Uuid = Uuid::from_u128(1);
const NODE_ID: Uuid = Uuid::from_u128(2);
const VOLUME_ID: Uuid = Uuid::from_u128(3);

struct InspectionServer;

impl task::task::Server for InspectionServer {
    /// Supplies inspection directly; list and runtime operations are deliberately unavailable.
    async fn inspect(
        self: Rc<Self>,
        params: task::task::InspectParams,
        mut results: task::task::InspectResults,
    ) -> Result<(), Error> {
        let selector = params.get()?.get_selector()?.to_str()?;
        let mut result = results.get().init_result();
        match selector {
            "missing" => result.set_not_found(()),
            "ambiguous" => result.set_ambiguous(()),
            "disconnected" => return Err(Error::disconnected("daemon connection lost".into())),
            _ => write_task(result.init_spec(), selector == "finished"),
        }

        Ok(())
    }
}

struct Session {
    task: task::task::Client,
}

impl cluster_session::Server for Session {
    /// Exposes only task inspection so rendering cannot fetch secrets or contact other capabilities.
    async fn get_task(
        self: Rc<Self>,
        _params: cluster_session::GetTaskParams,
        mut results: cluster_session::GetTaskResults,
    ) -> Result<(), Error> {
        results.get().set_task(self.task.clone());
        Ok(())
    }
}

/// Covers the diagnostics and optional configuration omitted by task lists.
fn write_task(mut spec: task::task_spec::Builder<'_>, finished: bool) {
    spec.set_id(TASK_ID.as_bytes());
    spec.set_name("api");
    spec.set_node_id(NODE_ID.as_bytes());
    spec.set_node_name("worker-a");
    spec.set_image("alpine:3.20");
    spec.set_execution_platform("oci");
    spec.set_isolation_mode("standard");
    spec.set_created_at("2026-09-15T10:00:00Z");
    spec.set_updated_at("2026-09-15T10:01:00Z");
    spec.set_cpu_millis(500);
    spec.set_memory_bytes(67_108_895);
    spec.set_launch_attempt(3);
    spec.set_task_epoch(2);
    spec.set_phase_version(7);

    if finished {
        spec.set_state("exited:17");
        spec.set_last_terminal_observed_launch(3);
        return;
    }

    spec.set_state("volume_unavailable");
    spec.set_phase_reason("volume data is waiting for its local replica");
    spec.set_phase_progress("waiting for replica transfer");

    let mut command = spec.reborrow().init_command(3);
    command.set(0, "sh");
    command.set(1, "-c");
    command.set(2, "echo ready; sleep 60");
    spec.reborrow().init_slot_ids(1).set(0, 9);

    let mut restart = spec.reborrow().init_restart_policy();
    restart.set_name(workload::RestartPolicyName::OnFailure);
    restart.set_max_retry_count(4);

    let mut probe = spec.reborrow().init_liveness();
    probe.set_kind(workload::LivenessProbeKind::Http);
    probe.set_port(8080);
    probe.set_path("/health");
    probe.set_interval_ms(2000);
    probe.set_timeout_ms(300);
    probe.set_failure_threshold(2);
    probe.set_start_period_ms(1000);

    let mut mount = spec.reborrow().init_volumes(1).get(0);
    mount.set_volume_id(VOLUME_ID.as_bytes());
    mount.set_volume_name("data");
    mount.set_target("/data");

    let mut env = spec.init_env(1).get(0);
    env.set_name("PASSWORD");
    env.set_value("must-not-appear");
    env.init_secret().set_name("database-password");
}

/// Runs the actual command against a local socket without starting a container runtime.
async fn run_inspect(state_dir: &Path, selector: &str, details: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mantissa"));
    command.args(["tasks", "inspect", selector]);
    if details {
        command.arg("--details");
    }

    timeout(
        Duration::from_secs(15),
        command
            .env("MANTISSA_STATE_DIR", state_dir)
            .env("RUST_LOG", "off")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("inspection should finish")
    .expect("run CLI")
}

/// Starts an isolated inspection server for the real CLI tests.
async fn start_socket(state_dir: &Path) {
    let task = capnp_rpc::new_client(InspectionServer);
    let session = capnp_rpc::new_client(Session { task });
    mantissa_net::unix_socket::start_unix_socket_server_at(
        session,
        state_dir.join("mantissa.sock"),
    )
    .await
    .expect("start task socket");
}

/// Compact output retains failures and configured settings while expanded output exposes exact counters.
#[tokio::test]
async fn tasks_inspect_shows_diagnostics_and_references() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            start_socket(dir.path()).await;

            let output = run_inspect(dir.path(), "api", false).await;
            assert!(output.status.success(), "{output:?}");
            let text = String::from_utf8(output.stdout).expect("inspection output is UTF-8");

            assert!(text.starts_with("TASK api\n"));
            assert!(text.contains("volume data is waiting for its local replica"));
            assert!(text.contains("waiting for replica transfer"));
            assert!(text.contains(&format!("worker-a ({NODE_ID})")));
            assert!(text.contains("500m"));
            assert!(text.contains("64 MiB"));
            assert!(text.contains("on failure, maximum retries 4"));
            assert!(text.contains("http port 8080 path \"/health\""));
            assert!(text.contains(&format!("data ({VOLUME_ID}) -> \"/data\"")));
            assert!(text.contains("PASSWORD <- secret database-password (latest)"));
            assert!(!text.contains("must-not-appear"));
            assert!(!text.contains("INTERNAL STATE"));
            assert!(!text.contains("Memory bytes"));

            let output = run_inspect(dir.path(), &TASK_ID.to_string(), true).await;
            assert!(output.status.success(), "{output:?}");
            let text = String::from_utf8(output.stdout).expect("inspection output is UTF-8");

            assert!(text.contains("INTERNAL STATE"));
            assert!(text.contains("67108895"));
            assert!(text.contains("Assignment epoch  2"));
            assert!(text.contains("Secret files      none"));
            assert!(!text.contains("must-not-appear"));
        })
        .await;
}

/// Finished tasks still show their exit code without pages of unused configuration.
#[tokio::test]
async fn tasks_inspect_keeps_finished_output_compact() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            start_socket(dir.path()).await;

            let output = run_inspect(dir.path(), "finished", false).await;
            assert!(output.status.success(), "{output:?}");
            let text = String::from_utf8(output.stdout).expect("inspection output is UTF-8");

            assert!(text.contains("Status            exited"));
            assert!(text.contains("Exit code         17"));
            assert!(!text.contains("CONNECTIONS AND MOUNTS"));
            assert!(!text.contains("Restart policy"));
            assert!(text.lines().count() < 22, "{text}");
            assert!(!text.contains('\u{1b}'));
        })
        .await;
}

/// Selection and transport failures must not print a partial inspection as successful output.
#[tokio::test]
async fn tasks_inspect_reports_errors_without_partial_output() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            start_socket(dir.path()).await;

            for selector in ["missing", "ambiguous", "disconnected", " "] {
                let output = run_inspect(dir.path(), selector, false).await;

                assert!(
                    !output.status.success(),
                    "unexpected success for {selector}"
                );
                assert!(output.stdout.is_empty());
                assert!(!output.stderr.is_empty());
            }
        })
        .await;
}
