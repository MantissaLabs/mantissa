use capnp::Error;
use mantissa_client::{
    config::ClientConfig,
    error::{ClientError, ClientErrorKind},
    tasks::TaskExecOptions,
};
use mantissa_protocol::{
    server::cluster_session,
    services::services,
    task::{TaskLogStream, task, task_exec_session},
};
use std::{cell::RefCell, path::Path, process::Output, rc::Rc, time::Duration};
use tokio::{process::Command, task::LocalSet, time::timeout};
use uuid::Uuid;

const SERVICE_ID: Uuid = Uuid::from_u128(1);
const TASK_ID: Uuid = Uuid::from_u128(21);

#[derive(Debug, PartialEq, Eq)]
struct SelectionRequest {
    selector: String,
    template: Option<String>,
    replica: Option<u16>,
}

enum SelectionFailure {
    NotFound,
    InvalidRequest,
    NoRunningTask,
    Rpc(Error),
}

#[derive(Default)]
struct State {
    selections: RefCell<Vec<SelectionRequest>>,
    selection_failure: Option<SelectionFailure>,
    executions: RefCell<Vec<(String, TaskExecOptions)>>,
    failure: Option<Error>,
    exit_code: i32,
}

struct Server(Rc<State>);

impl services::Server for Server {
    /// Exposes only selection so any client attempt to inspect or list service tasks fails the test.
    async fn select_exec_task(
        self: Rc<Self>,
        params: services::SelectExecTaskParams,
        mut results: services::SelectExecTaskResults,
    ) -> Result<(), Error> {
        let request = params.get()?;
        let template = if request.has_template() {
            Some(request.get_template()?.to_str()?.to_string())
        } else {
            None
        };
        self.0.selections.borrow_mut().push(SelectionRequest {
            selector: request.get_selector()?.to_str()?.to_string(),
            template,
            replica: request.get_has_replica().then(|| request.get_replica()),
        });

        let mut result = results.get().init_result();
        match &self.0.selection_failure {
            Some(SelectionFailure::NotFound) => result.set_not_found("service not found"),
            Some(SelectionFailure::InvalidRequest) => {
                result.set_invalid_request("specify a template")
            }
            Some(SelectionFailure::NoRunningTask) => result.set_no_running_task("no running task"),
            Some(SelectionFailure::Rpc(error)) => return Err(error.clone()),
            None => {
                let mut task = result.init_task();
                task.set_id(TASK_ID.as_bytes());
                task.set_template("backend");
                task.set_replica(2);
            }
        }

        Ok(())
    }
}

impl task::Server for Server {
    /// Captures the actual task request and can fail after selection to detect unsafe retries.
    async fn exec(
        self: Rc<Self>,
        params: task::ExecParams,
        mut results: task::ExecResults,
    ) -> Result<(), Error> {
        let request = params.get()?.get_request()?;
        let options = request.get_options()?;
        let command = options
            .get_command()?
            .iter()
            .map(|arg| Ok(arg?.to_str()?.to_string()))
            .collect::<Result<Vec<_>, Error>>()?;
        let detach_keys = options.get_detach_keys()?.to_str()?;
        self.0.executions.borrow_mut().push((
            request.get_selector()?.to_str()?.to_string(),
            TaskExecOptions {
                command,
                stdin: options.get_stdin(),
                stdout: options.get_stdout(),
                stderr: options.get_stderr(),
                tty: options.get_tty(),
                detach_keys: (!detach_keys.is_empty()).then(|| detach_keys.to_string()),
                tty_width: Some(options.get_tty_width()),
                tty_height: Some(options.get_tty_height()),
            },
        ));

        if let Some(error) = &self.0.failure {
            return Err(error.clone());
        }

        let sink = request.get_sink()?;
        let mut push = sink.push_frame_request();
        let mut frame = push.get().init_frame();
        frame.set_stream(TaskLogStream::Stdout);
        frame.set_data(b"command output\n");
        push.send().await?;
        sink.end_request().send().promise.await?;

        results
            .get()
            .set_session(capnp_rpc::new_client(ExecSession(self.0.exit_code)));
        Ok(())
    }
}

struct ExecSession(i32);

impl task_exec_session::Server for ExecSession {
    /// Accepts EOF from the CLI subprocess's empty stdin.
    async fn close_input(
        self: Rc<Self>,
        _params: task_exec_session::CloseInputParams,
        _results: task_exec_session::CloseInputResults,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Provides a runtime exit code so service exec must preserve task exec failure handling.
    async fn wait_result(
        self: Rc<Self>,
        _params: task_exec_session::WaitResultParams,
        mut results: task_exec_session::WaitResultResults,
    ) -> Result<(), Error> {
        results.get().set_has_exit_code(true);
        results.get().set_exit_code(self.0);
        Ok(())
    }
}

struct Session {
    services: services::Client,
    task: task::Client,
}

impl cluster_session::Server for Session {
    /// Exposes the dedicated selection RPC through the normal local socket.
    async fn get_services(
        self: Rc<Self>,
        _params: cluster_session::GetServicesParams,
        mut results: cluster_session::GetServicesResults,
    ) -> Result<(), Error> {
        results.get().set_services(self.services.clone());
        Ok(())
    }

    /// Exposes task execution after the service has chosen a target.
    async fn get_task(
        self: Rc<Self>,
        _params: cluster_session::GetTaskParams,
        mut results: cluster_session::GetTaskResults,
    ) -> Result<(), Error> {
        results.get().set_task(self.task.clone());
        Ok(())
    }
}

/// Runs the real CLI against an isolated service/task server without starting a cluster.
async fn start_socket(path: &Path, state: Rc<State>) {
    let services = capnp_rpc::new_client(Server(state.clone()));
    let task = capnp_rpc::new_client(Server(state));
    let session = capnp_rpc::new_client(Session { services, task });

    mantissa_net::unix_socket::start_unix_socket_server_at(session, path.join("mantissa.sock"))
        .await
        .expect("start socket");
}

/// Bounds interactive command failures and closes stdin to avoid depending on a terminal.
async fn run_exec(path: &Path, scope: &str, selector: &str, arguments: &[&str]) -> Output {
    timeout(
        Duration::from_secs(10),
        Command::new(env!("CARGO_BIN_EXE_mantissa"))
            .args([scope, "exec", selector])
            .args(arguments)
            .env("MANTISSA_STATE_DIR", path)
            .env("RUST_LOG", "off")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("exec should finish")
    .expect("run CLI")
}

/// The CLI must execute exactly the UUID returned by selection and keep its notice off stdout.
#[tokio::test]
async fn service_exec_uses_the_server_selected_task() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create socket directory");
            let state = Rc::new(State::default());
            start_socket(dir.path(), state.clone()).await;

            let output = run_exec(dir.path(), "services", "demo", &["hostname"]).await;
            let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");

            assert!(output.status.success(), "{stderr}");
            assert_eq!(output.stdout, b"command output\n");
            assert_eq!(stderr, format!("executing in backend/2 [{TASK_ID}]\n"));
            assert_eq!(
                *state.selections.borrow(),
                vec![SelectionRequest {
                    selector: "demo".into(),
                    template: None,
                    replica: None,
                }]
            );

            let executions = state.executions.borrow();
            assert_eq!(executions.len(), 1);
            assert_eq!(executions[0].0, TASK_ID.to_string());
            assert!(executions[0].1.stdin);
            assert!(executions[0].1.stdout);
            assert!(executions[0].1.stderr);
            assert!(!executions[0].1.tty);
        })
        .await;
}

/// Explicit replica selection and shared flags must preserve command arguments, including option names.
#[tokio::test]
async fn service_and_task_exec_share_command_and_terminal_options() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create socket directory");
            let state = Rc::new(State::default());
            start_socket(dir.path(), state.clone()).await;
            let command = [
                "--no-stdin",
                "-t",
                "--stdout",
                "--detach-keys",
                "ctrl-x",
                "--",
                "sh",
                "-c",
                "echo hello",
                "--template",
                "application-argument",
            ];
            let mut arguments = vec!["--template", "backend", "--replica", "2"];
            arguments.extend(command);

            let service =
                run_exec(dir.path(), "services", &SERVICE_ID.to_string(), &arguments).await;
            let task = run_exec(dir.path(), "tasks", &TASK_ID.to_string(), &command).await;

            assert!(service.status.success(), "{service:?}");
            assert!(task.status.success(), "{task:?}");
            assert_eq!(service.stdout, task.stdout);
            assert!(task.stderr.is_empty());
            assert_eq!(
                *state.selections.borrow(),
                vec![SelectionRequest {
                    selector: SERVICE_ID.to_string(),
                    template: Some("backend".into()),
                    replica: Some(2),
                }]
            );

            let executions = state.executions.borrow();
            assert_eq!(executions.len(), 2);
            assert_eq!(executions[0], executions[1]);
            assert_eq!(executions[0].0, TASK_ID.to_string());
            let options = &executions[0].1;
            assert_eq!(
                options.command,
                [
                    "sh",
                    "-c",
                    "echo hello",
                    "--template",
                    "application-argument"
                ]
            );
            assert!(!options.stdin);
            assert!(options.stdout);
            assert!(!options.stderr);
            assert!(options.tty);
            assert_eq!(options.detach_keys.as_deref(), Some("ctrl-x"));
        })
        .await;
}

/// Selection failures must preserve their classification and never start a command.
#[tokio::test]
async fn service_exec_preserves_selection_errors() {
    LocalSet::new()
        .run_until(async {
            for (failure, kind, message) in [
                (
                    SelectionFailure::NotFound,
                    Some(ClientErrorKind::NotFound),
                    "service not found",
                ),
                (
                    SelectionFailure::InvalidRequest,
                    Some(ClientErrorKind::InvalidRequest),
                    "specify a template",
                ),
                (
                    SelectionFailure::NoRunningTask,
                    Some(ClientErrorKind::Conflict),
                    "no running task",
                ),
                (
                    SelectionFailure::Rpc(Error::failed("workload store unavailable".into())),
                    None,
                    "workload store unavailable",
                ),
                (
                    SelectionFailure::Rpc(Error::disconnected("connection lost".into())),
                    None,
                    "connection lost",
                ),
            ] {
                let dir = tempfile::tempdir().expect("create socket directory");
                let state = Rc::new(State {
                    selection_failure: Some(failure),
                    ..State::default()
                });
                start_socket(dir.path(), state.clone()).await;
                let config = ClientConfig {
                    socket: Some(dir.path().join("mantissa.sock")),
                    ..ClientConfig::default()
                };

                let output = run_exec(dir.path(), "services", "demo", &["--", "hostname"]).await;
                let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
                let error =
                    mantissa_client::services::select_exec_task(&config, "demo", None, None)
                        .await
                        .expect_err("selection should fail");

                assert!(!output.status.success(), "{stderr}");
                assert!(stderr.contains(message), "{stderr}");
                assert!(output.stdout.is_empty());
                assert_eq!(
                    error.downcast_ref::<ClientError>().map(ClientError::kind),
                    kind
                );
                assert!(state.executions.borrow().is_empty());
            }
        })
        .await;
}

/// Runtime failures, uncertain transport failures, and command errors must never execute a second time.
#[tokio::test]
async fn service_exec_does_not_retry_failed_execution() {
    LocalSet::new()
        .run_until(async {
            for (failure, exit_code, expected) in [
                (
                    Some(Error::failed("task is no longer running".into())),
                    0,
                    "task is no longer running",
                ),
                (
                    Some(Error::disconnected("connection lost".into())),
                    0,
                    "connection lost",
                ),
                (None, 17, "task exec exited with status 17"),
            ] {
                let dir = tempfile::tempdir().expect("create socket directory");
                let state = Rc::new(State {
                    failure,
                    exit_code,
                    ..State::default()
                });
                start_socket(dir.path(), state.clone()).await;

                let output = run_exec(dir.path(), "services", "demo", &["--", "hostname"]).await;
                let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");

                assert!(!output.status.success(), "{stderr}");
                assert!(stderr.contains(expected), "{stderr}");
                assert_eq!(state.selections.borrow().len(), 1);
                assert_eq!(state.executions.borrow().len(), 1);
            }
        })
        .await;
}
