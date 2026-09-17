use capnp::Error;
use mantissa_client::{config::ClientConfig, tasks::TaskLogsOptions};
use mantissa_protocol::{
    server::cluster_session,
    services::{service_task, services},
    task::{TaskLogStream, task},
};
use std::{
    cell::{Cell, RefCell},
    path::Path,
    process::Output,
    rc::Rc,
    time::Duration,
};
use tokio::{
    process::Command,
    sync::{Barrier, Notify},
    task::LocalSet,
    time::timeout,
};
use uuid::Uuid;

const SERVICE_ID: Uuid = Uuid::from_u128(10);
const FIRST: Uuid = Uuid::from_u128(0x11111111_0000_4000_8000_000000000001);
const SECOND: Uuid = Uuid::from_u128(0x22222222_0000_4000_8000_000000000002);
const FRONTEND: Uuid = Uuid::from_u128(0x33333333_0000_4000_8000_000000000003);

struct LogRequest {
    follow: bool,
    tail: String,
    stdout: bool,
    stderr: bool,
    timestamps: bool,
}

#[derive(Default)]
struct State {
    selections: Cell<usize>,
    requests: RefCell<Vec<LogRequest>>,
    fail_second: bool,
    wait_for_cancel: bool,
    finished: Cell<usize>,
    changed: Notify,
}

struct Server {
    state: Rc<State>,
    backend_ready: Barrier,
}

impl services::Server for Server {
    /// Later selections expose an extra task, making accidental rediscovery observable.
    async fn tasks(
        self: Rc<Self>,
        params: services::TasksParams,
        mut results: services::TasksResults,
    ) -> Result<(), Error> {
        let selector = params.get()?.get_selector()?.to_str()?;
        let mut result = results.get().init_result();
        if selector == "missing" {
            result.set_not_found(());
            return Ok(());
        }
        if selector == "empty" {
            result.init_tasks(0);
            return Ok(());
        }
        assert!(selector == "demo" || selector == SERVICE_ID.to_string());

        let selections = self.state.selections.get();
        self.state.selections.set(selections + 1);
        let mut tasks = result.init_tasks(if selections == 0 { 3 } else { 4 });
        write_task(tasks.reborrow().get(0), FIRST, "backend", 1);
        write_task(tasks.reborrow().get(1), SECOND, "backend", 2);
        write_task(tasks.reborrow().get(2), FRONTEND, "frontend", 1);
        if selections != 0 {
            write_task(tasks.get(3), Uuid::from_u128(4), "backend", 3);
        }

        Ok(())
    }
}

impl task::Server for Server {
    /// Requires both backend requests to start before either finishes, exercising concurrent reads.
    async fn logs(
        self: Rc<Self>,
        params: task::LogsParams,
        _results: task::LogsResults,
    ) -> Result<(), Error> {
        let request = params.get()?.get_request()?;
        let id = request.get_selector()?.to_str()?.to_string();
        let options = request.get_options()?;
        self.state.requests.borrow_mut().push(LogRequest {
            follow: options.get_follow(),
            tail: options.get_tail()?.to_str()?.to_string(),
            stdout: options.get_stdout(),
            stderr: options.get_stderr(),
            timestamps: options.get_timestamps(),
        });
        self.state.changed.notify_one();
        let _completion = Completion(self.state.clone());

        if self.state.wait_for_cancel {
            std::future::pending::<()>().await;
        }
        if id != FRONTEND.to_string() {
            self.backend_ready.wait().await;
        }
        if id == SECOND.to_string() && self.state.fail_second {
            return Err(Error::failed("runtime logs unavailable".into()));
        }

        let frames: Vec<_> = if id == FIRST.to_string() {
            vec![
                (TaskLogStream::Stdout, b"first \xc3".as_slice()),
                (TaskLogStream::Stdout, b"\xa9\nlast fragment".as_slice()),
            ]
        } else if id == SECOND.to_string() {
            vec![
                (TaskLogStream::Stdout, b"second\n".as_slice()),
                (TaskLogStream::Stderr, b"warning\n".as_slice()),
            ]
        } else {
            assert_eq!(id, FRONTEND.to_string());
            vec![(TaskLogStream::Console, b"frontend\n".as_slice())]
        };
        let sink = request.get_sink()?;
        for (stream, data) in frames {
            let mut push = sink.push_frame_request();
            let mut frame = push.get().init_frame();
            frame.set_stream(stream);
            frame.set_data(data);
            push.send().await?;
        }

        sink.end_request().send().promise.await?;
        Ok(())
    }
}

struct Completion(Rc<State>);

impl Drop for Completion {
    /// Records cancellation as well as normal completion of mock task log requests.
    fn drop(&mut self) {
        self.0.finished.set(self.0.finished.get() + 1);
        self.0.changed.notify_one();
    }
}

struct Session {
    services: services::Client,
    task: task::Client,
}

impl cluster_session::Server for Session {
    /// Exposes the service selection capability used before opening log streams.
    async fn get_services(
        self: Rc<Self>,
        _params: cluster_session::GetServicesParams,
        mut results: cluster_session::GetServicesResults,
    ) -> Result<(), Error> {
        results.get().set_services(self.services.clone());
        Ok(())
    }

    /// Reuses one capability for every selected task's log request.
    async fn get_task(
        self: Rc<Self>,
        _params: cluster_session::GetTaskParams,
        mut results: cluster_session::GetTaskResults,
    ) -> Result<(), Error> {
        results.get().set_task(self.task.clone());
        Ok(())
    }
}

/// Provides the minimum service task identity needed to select and label log sources.
fn write_task(mut row: service_task::Builder<'_>, id: Uuid, template: &str, replica: u16) {
    row.set_id(id.as_bytes());
    row.set_node_id(Uuid::nil().as_bytes());
    row.set_template(template);
    row.set_replica(replica);
    row.set_generation(2);
    row.set_state("running");
}

/// Starts an isolated socket shared by the CLI subprocess and cancellation checks.
async fn start_socket(path: &Path, state: Rc<State>) {
    let services = capnp_rpc::new_client(Server {
        state: state.clone(),
        backend_ready: Barrier::new(2),
    });
    let task = capnp_rpc::new_client(Server {
        state,
        backend_ready: Barrier::new(2),
    });
    let session = capnp_rpc::new_client(Session { services, task });

    mantissa_net::unix_socket::start_unix_socket_server_at(session, path.join("mantissa.sock"))
        .await
        .expect("start socket");
}

/// Bounds command execution so sequential follow requests fail instead of hanging the test suite.
async fn run_logs(path: &Path, selector: &str, options: &[&str]) -> Output {
    timeout(
        Duration::from_secs(10),
        Command::new(env!("CARGO_BIN_EXE_mantissa"))
            .args(["services", "logs", selector])
            .args(options)
            .env("MANTISSA_STATE_DIR", path)
            .env("RUST_LOG", "off")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("command should finish")
    .expect("run CLI")
}

/// Follows a fixed snapshot concurrently and preserves line boundaries and output streams.
#[tokio::test]
async fn service_logs_follow_initial_tasks_with_source_prefixes() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create socket directory");
            let state = Rc::new(State::default());
            start_socket(dir.path(), state.clone()).await;

            let output = run_logs(dir.path(), "demo", &["-f", "--tail", "5", "--timestamps"]).await;
            let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
            let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");

            assert!(output.status.success(), "{stderr}");
            assert_eq!(state.selections.get(), 1);
            assert_eq!(state.requests.borrow().len(), 3);
            assert!(state.requests.borrow().iter().all(|request| request.follow
                && request.tail == "5"
                && request.stdout
                && request.stderr
                && request.timestamps));
            assert!(
                stdout.contains("backend/1 [11111111] | first é\n"),
                "{stdout}"
            );
            assert!(
                stdout.contains("backend/1 [11111111] | last fragment\n"),
                "{stdout}"
            );
            assert!(
                stdout.contains("backend/2 [22222222] | second\n"),
                "{stdout}"
            );
            assert!(
                stdout.contains("frontend/1 [33333333] | frontend\n"),
                "{stdout}"
            );
            assert_eq!(stderr, "backend/2 [22222222] | warning\n");
        })
        .await;
}

/// A failed replica must not hide another replica's logs or make the command report success.
#[tokio::test]
async fn service_logs_filter_templates_and_report_partial_failure() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create socket directory");
            let state = Rc::new(State {
                fail_second: true,
                ..State::default()
            });
            start_socket(dir.path(), state.clone()).await;

            let output = run_logs(
                dir.path(),
                &SERVICE_ID.to_string(),
                &["--template", "backend", "--stdout"],
            )
            .await;
            let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
            let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");

            assert!(!output.status.success());
            assert_eq!(state.requests.borrow().len(), 2);
            assert!(state.requests.borrow().iter().all(|request| !request.follow
                && request.tail == "all"
                && request.stdout
                && !request.stderr));
            assert!(stdout.contains("backend/1 [11111111] | last fragment\n"));
            assert!(!stdout.contains("frontend"));
            assert!(stderr.contains("backend/2 [22222222] | error:"), "{stderr}");
            assert!(stderr.contains("runtime logs unavailable"), "{stderr}");
        })
        .await;
}

/// Missing services and invalid filters remain distinct from an empty task selection.
#[tokio::test]
async fn service_logs_handle_empty_and_invalid_selection() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create socket directory");
            start_socket(dir.path(), Rc::new(State::default())).await;

            for (selector, options, success, expected) in [
                ("empty", vec![], true, "no tasks found"),
                ("demo", vec!["--template", "absent"], true, "no tasks found"),
                ("missing", vec![], false, "not found"),
                (
                    "demo",
                    vec!["--template", " "],
                    false,
                    "template must not be empty",
                ),
                ("demo", vec!["--tail", "never"], false, "invalid"),
            ] {
                let output = run_logs(dir.path(), selector, &options).await;
                let text = String::from_utf8(if success {
                    output.stdout
                } else {
                    output.stderr
                })
                .expect("command output is UTF-8");

                assert_eq!(output.status.success(), success, "{text}");
                assert!(text.contains(expected), "{text}");
            }
        })
        .await;
}

/// Dropping an HTTP stream must cancel quiet requests without waiting for another log frame.
#[tokio::test]
async fn service_logs_cancel_all_requests_when_dropped() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create socket directory");
            let state = Rc::new(State {
                wait_for_cancel: true,
                ..State::default()
            });
            start_socket(dir.path(), state.clone()).await;
            let config = ClientConfig {
                socket: Some(dir.path().join("mantissa.sock")),
                ..ClientConfig::default()
            };
            let options = TaskLogsOptions {
                follow: true,
                tail: "0",
                stdout: false,
                stderr: false,
                timestamps: false,
            };

            let stream = mantissa_client::services::logs(&config, "demo", None, &options)
                .await
                .expect("start service log stream");
            let stream = mantissa_rest::stream::service_logs::ServiceLogHttpStream(stream);

            timeout(Duration::from_secs(5), async {
                while state.requests.borrow().len() != 3 {
                    state.changed.notified().await;
                }
            })
            .await
            .expect("all streams should start");
            drop(stream);

            timeout(Duration::from_secs(5), async {
                while state.finished.get() != 3 {
                    state.changed.notified().await;
                }
            })
            .await
            .expect("all requests should be cancelled");
        })
        .await;
}
