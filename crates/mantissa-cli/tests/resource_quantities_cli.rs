use capnp::Error;
use mantissa_protocol::{server::cluster_session, task};
use std::{cell::RefCell, path::Path, process::Output, rc::Rc, time::Duration};
use tokio::{process::Command, task::LocalSet, time::timeout};
use uuid::Uuid;

struct TaskServer {
    requests: Rc<RefCell<Vec<(u64, u64)>>>,
}

impl task::task::Server for TaskServer {
    /// Records exact request quantities and returns them to exercise decoding and CLI formatting.
    async fn start(
        self: Rc<Self>,
        params: task::task::StartParams,
        mut results: task::task::StartResults,
    ) -> Result<(), Error> {
        let request = params.get()?.get_request()?;
        let cpu_millis = request.get_cpu_millis();
        let memory_bytes = request.get_memory_bytes();
        self.requests.borrow_mut().push((cpu_millis, memory_bytes));

        let mut spec = results.get().init_spec();
        spec.set_id(Uuid::from_u128(1).as_bytes());
        spec.set_node_id(Uuid::from_u128(2).as_bytes());
        spec.set_name(request.get_name()?);
        spec.set_image(request.get_image()?);
        spec.set_cpu_millis(cpu_millis);
        spec.set_memory_bytes(memory_bytes);
        spec.set_state("running");

        Ok(())
    }
}

struct Session {
    task: task::task::Client,
}

impl cluster_session::Server for Session {
    /// Exposes the task capability without starting a runtime or scheduler.
    async fn get_task(
        self: Rc<Self>,
        _params: cluster_session::GetTaskParams,
        mut results: cluster_session::GetTaskResults,
    ) -> Result<(), Error> {
        results.get().set_task(self.task.clone());
        Ok(())
    }
}

/// Runs argument validation and task submission through the real CLI with an isolated socket.
async fn start_task(state_dir: &Path, quantities: &[&str]) -> Output {
    timeout(
        Duration::from_secs(15),
        Command::new(env!("CARGO_BIN_EXE_mantissa"))
            .args(["tasks", "start", "demo", "--image", "alpine:3.20"])
            .args(quantities)
            .env("MANTISSA_STATE_DIR", state_dir)
            .env("RUST_LOG", "off")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("task submission should finish")
    .expect("run CLI")
}

/// Requests below one MiB must survive the complete CLI-to-RPC-to-display path.
#[tokio::test]
async fn readable_quantities_preserve_requests_and_reject_invalid_input() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            let requests = Rc::new(RefCell::new(Vec::new()));
            let task = capnp_rpc::new_client(TaskServer {
                requests: requests.clone(),
            });
            let session = capnp_rpc::new_client(Session { task });
            mantissa_net::unix_socket::start_unix_socket_server_at(
                session,
                dir.path().join("mantissa.sock"),
            )
            .await
            .expect("start test task socket");

            let output = start_task(dir.path(), &["--cpu", "1.25", "--memory", "512KiB"]).await;
            assert!(output.status.success(), "{output:?}");
            assert_eq!(*requests.borrow(), [(1250, 512 << 10)]);

            let stdout = String::from_utf8(output.stdout).expect("task output is UTF-8");
            assert!(stdout.contains("1.25"));
            assert!(stdout.contains("512 KiB"));
            assert!(!stdout.contains("CPU(m)"));
            assert!(!stdout.contains("MEM(MiB)"));

            for (flag, value) in [
                ("--cpu", "0.0001"),
                ("--memory", "0.1B"),
                ("--memory", "16EiB"),
            ] {
                let output = start_task(dir.path(), &[flag, value]).await;

                assert_eq!(output.status.code(), Some(2), "{output:?}");
                assert!(String::from_utf8_lossy(&output.stderr).contains("invalid"));
                assert_eq!(
                    requests.borrow().len(),
                    1,
                    "invalid input must not submit a task"
                );
            }
        })
        .await;
}
