use capnp::Error;
use mantissa_protocol::{
    server::cluster_session,
    services::{service_task, services},
};
use std::{path::Path, process::Output, rc::Rc, time::Duration};
use tokio::{process::Command, task::LocalSet, time::timeout};
use uuid::Uuid;

const SERVICE_ID: Uuid = Uuid::from_u128(1);
const CURRENT_ID: Uuid = Uuid::from_u128(0x11111111_0000_4000_8000_000000000001);
const PREVIOUS_ID: Uuid = Uuid::from_u128(0x22222222_0000_4000_8000_000000000002);
const NODE_ID: Uuid = Uuid::from_u128(3);
const LONG_IMAGE: &str = "registry.example.test/team/backend:release-with-a-long-image-tag";

struct Services;

impl services::Server for Services {
    /// Serves task rows directly so the command cannot fall back to fetching the entire cluster list.
    async fn tasks(
        self: Rc<Self>,
        params: services::TasksParams,
        mut results: services::TasksResults,
    ) -> Result<(), Error> {
        let selector = params.get()?.get_selector()?.to_str()?;
        let mut result = results.get().init_result();
        match selector {
            "missing" => result.set_not_found(()),
            "unavailable" => return Err(Error::failed("workload store unavailable".into())),
            "disconnected" => return Err(Error::disconnected("daemon connection lost".into())),
            "empty" => {
                result.init_tasks(0);
            }
            selector if selector == "demo" || selector == SERVICE_ID.to_string() => {
                let mut rows = result.init_tasks(2);
                write_task(rows.reborrow().get(0), true);
                write_task(rows.get(1), false);
            }
            _ => {
                return Err(Error::failed(format!(
                    "unexpected service selector '{selector}'"
                )));
            }
        }

        Ok(())
    }
}

struct Session {
    services: services::Client,
}

impl cluster_session::Server for Session {
    /// Exposes only the service capability to exercise the command's targeted request.
    async fn get_services(
        self: Rc<Self>,
        _params: cluster_session::GetServicesParams,
        mut results: cluster_session::GetServicesResults,
    ) -> Result<(), Error> {
        results.get().set_services(self.services.clone());
        Ok(())
    }
}

/// Builds two generations of the same replica, with the older task retained after a failed exit.
fn write_task(mut row: service_task::Builder<'_>, previous: bool) {
    let id = if previous { PREVIOUS_ID } else { CURRENT_ID };
    row.set_id(id.as_bytes());
    row.set_name("demo-backend-1");
    row.set_template("backend");
    row.set_replica(1);
    row.set_generation(if previous { 2 } else { 3 });

    row.set_image(LONG_IMAGE);
    row.set_node_id(NODE_ID.as_bytes());
    row.set_node_name("worker-a");
    row.set_created_at("2026-09-16T10:00:00Z");
    row.set_updated_at("2026-09-16T10:01:00Z");
    row.set_state(if previous { "exited:17" } else { "running" });
    if previous {
        row.set_phase_reason("process exited with code 17");
        row.set_phase_progress("cleanup complete");
    }
}

/// Starts a local service capability without a container runtime or cluster.
async fn start_socket(state_dir: &Path) {
    let services = capnp_rpc::new_client(Services);
    let session = capnp_rpc::new_client(Session { services });
    mantissa_net::unix_socket::start_unix_socket_server_at(
        session,
        state_dir.join("mantissa.sock"),
    )
    .await
    .expect("start service tasks socket");
}

/// Executes the actual CLI against an isolated socket and bounds failures to a useful test timeout.
async fn run_tasks(state_dir: &Path, selector: &str, options: &[&str]) -> Output {
    timeout(
        Duration::from_secs(15),
        Command::new(env!("CARGO_BIN_EXE_mantissa"))
            .args(["services", "tasks", selector])
            .args(options)
            .env("MANTISSA_STATE_DIR", state_dir)
            .env("RUST_LOG", "off")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("service tasks command should finish")
    .expect("run CLI")
}

/// Keeps failed replica attempts visible and ordered behind their newer generation.
#[tokio::test]
async fn services_tasks_show_replica_identity_and_failure_details() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            start_socket(dir.path()).await;

            let output = run_tasks(dir.path(), "demo", &[]).await;
            assert!(output.status.success(), "{output:?}");
            let text = String::from_utf8(output.stdout).expect("table is UTF-8");
            let lines: Vec<_> = text.lines().collect();

            assert_eq!(lines.len(), 3, "{text}");
            assert!(lines[0].contains("TEMPLATE"));
            assert!(lines[0].contains("REPLICA"));
            assert!(lines[0].contains("GENERATION"));
            assert!(lines[1].starts_with("11111111"));
            assert!(lines[2].starts_with("22222222"));
            assert!(lines[2].contains("exited (17)"));
            assert!(lines[2].contains("process exited with code 17; cleanup complete"));
            assert!(!text.contains(LONG_IMAGE));
            assert!(!text.contains(&CURRENT_ID.to_string()));
            assert!(!text.contains('\u{1b}'));

            let output = run_tasks(
                dir.path(),
                &SERVICE_ID.to_string(),
                &["-o", "wide", "--no-trunc"],
            )
            .await;
            assert!(output.status.success(), "{output:?}");
            let text = String::from_utf8(output.stdout).expect("wide table is UTF-8");

            assert!(text.contains(&CURRENT_ID.to_string()));
            assert!(text.contains(&PREVIOUS_ID.to_string()));
            assert!(text.contains(LONG_IMAGE));
            assert!(text.contains("demo-backend-1"));
            assert!(text.contains("2026-09-16T10:01:00Z"));
        })
        .await;
}

/// An existing service without visible tasks is a successful empty result.
#[tokio::test]
async fn services_tasks_report_empty_results() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            start_socket(dir.path()).await;

            let output = run_tasks(dir.path(), "empty", &[]).await;

            assert!(output.status.success(), "{output:?}");
            assert_eq!(
                String::from_utf8(output.stdout).expect("output is UTF-8"),
                "no tasks found for service\n"
            );
        })
        .await;
}

/// Missing services and failed reads must not be mistaken for successful empty lists.
#[tokio::test]
async fn services_tasks_preserve_selection_and_rpc_errors() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            start_socket(dir.path()).await;

            for (selector, expected) in [
                ("missing", "service 'missing' not found"),
                ("unavailable", "workload store unavailable"),
                ("disconnected", "daemon connection lost"),
                (" ", "service name or ID must not be empty"),
            ] {
                let output = run_tasks(dir.path(), selector, &[]).await;
                let error = String::from_utf8(output.stderr).expect("error is UTF-8");

                assert!(
                    !output.status.success(),
                    "unexpected success for {selector}"
                );
                assert!(output.stdout.is_empty());
                assert!(error.contains(expected), "{error}");
            }
        })
        .await;
}
