use capnp::Error;
use mantissa_protocol::{server::cluster_session, services};
use std::{
    cell::{Cell, RefCell},
    path::Path,
    process::Output,
    rc::Rc,
    time::Duration,
};
use tokio::{process::Command, task::LocalSet, time::timeout};
use uuid::Uuid;

const SERVICE_ID: Uuid = Uuid::from_u128(1);
const MANIFEST_ID: Uuid = Uuid::from_u128(2);

struct ServiceServer {
    stopped_ids: Rc<RefCell<Vec<Uuid>>>,
    status: Cell<services::ServiceStatus>,
}

impl services::services::Server for ServiceServer {
    /// Resolves only the exact name so partial matches cannot silently stop another service.
    async fn inspect(
        self: Rc<Self>,
        params: services::services::InspectParams,
        mut results: services::services::InspectResults,
    ) -> Result<(), Error> {
        if params.get()?.get_selector()?.to_str()? != "api" {
            return Err(Error::failed("service not found".to_string()));
        }

        results.get().init_service().set_id(SERVICE_ID.as_bytes());
        Ok(())
    }

    /// Returns the service state before stop so CLI guidance can distinguish repeated requests.
    async fn status(
        self: Rc<Self>,
        params: services::services::StatusParams,
        mut results: services::services::StatusResults,
    ) -> Result<(), Error> {
        if params.get()?.get_service_id()? != SERVICE_ID.as_bytes() {
            return Err(Error::failed("service not found".to_string()));
        }

        let mut service = results.get().init_snapshot().init_service();
        service.set_id(SERVICE_ID.as_bytes());
        service.set_manifest_id(MANIFEST_ID.as_bytes());
        service.set_service_name("api");
        service.set_status(self.status.get());

        Ok(())
    }

    /// Records the actual mutation target instead of trusting the CLI's success message.
    async fn delete(
        self: Rc<Self>,
        params: services::services::DeleteParams,
        _results: services::services::DeleteResults,
    ) -> Result<(), Error> {
        for id in params.get()?.get_ids()?.iter() {
            let id = Uuid::from_slice(id?).map_err(|error| Error::failed(error.to_string()))?;
            self.stopped_ids.borrow_mut().push(id);
        }

        self.status.set(services::ServiceStatus::Stopped);
        Ok(())
    }
}

struct Session {
    services: services::services::Client,
}

impl cluster_session::Server for Session {
    /// Exposes only services so the command cannot depend on unrelated cluster APIs.
    async fn get_services(
        self: Rc<Self>,
        _params: cluster_session::GetServicesParams,
        mut results: cluster_session::GetServicesResults,
    ) -> Result<(), Error> {
        results.get().set_services(self.services.clone());
        Ok(())
    }
}

/// Starts an isolated socket and records stop requests without launching containers.
async fn start_service_socket(state_dir: &Path, stopped_ids: Rc<RefCell<Vec<Uuid>>>) {
    let services = capnp_rpc::new_client(ServiceServer {
        stopped_ids,
        status: Cell::new(services::ServiceStatus::Running),
    });
    let session = capnp_rpc::new_client(Session { services });

    mantissa_net::unix_socket::start_unix_socket_server_at(
        session,
        state_dir.join("mantissa.sock"),
    )
    .await
    .expect("start test service socket");
}

/// Exercises argument parsing and client selection through the real CLI binary.
async fn run_stop(state_dir: &Path, selector: &str) -> Output {
    timeout(
        Duration::from_secs(15),
        Command::new(env!("CARGO_BIN_EXE_mantissa"))
            .args(["services", "stop", selector])
            .env("MANTISSA_STATE_DIR", state_dir)
            .env("RUST_LOG", "off")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("stop request should finish")
    .expect("run CLI")
}

/// Ensures names and UUIDs target the same service and retain guidance for an already stopped service.
#[tokio::test]
async fn services_stop_accepts_name_and_uuid() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            let stopped_ids = Rc::new(RefCell::new(Vec::new()));
            start_service_socket(dir.path(), stopped_ids.clone()).await;

            let by_name = run_stop(dir.path(), "api").await;
            assert!(by_name.status.success(), "{by_name:?}");

            let stdout = String::from_utf8(by_name.stdout).expect("stop output is UTF-8");
            assert!(stdout.contains(&format!("stop requested for service 'api' ({SERVICE_ID})")));
            assert!(stdout.contains("service status will move to 'stopping'"));
            assert_eq!(*stopped_ids.borrow(), [SERVICE_ID]);

            let by_id = run_stop(dir.path(), &SERVICE_ID.to_string()).await;
            assert!(by_id.status.success(), "{by_id:?}");

            let stdout = String::from_utf8(by_id.stdout).expect("stop output is UTF-8");
            assert!(stdout.contains("service is already stopped"));
            assert_eq!(*stopped_ids.borrow(), [SERVICE_ID, SERVICE_ID]);
        })
        .await;
}

/// Rejects incomplete or unknown selectors before sending any stop request.
#[tokio::test]
async fn services_stop_rejects_unresolved_services_without_mutation() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            let stopped_ids = Rc::new(RefCell::new(Vec::new()));
            start_service_socket(dir.path(), stopped_ids.clone()).await;

            for selector in ["ap", "00000000-0000-0000-0000-000000000099", " "] {
                let output = run_stop(dir.path(), selector).await;

                assert!(
                    !output.status.success(),
                    "unexpected success for {selector:?}"
                );
                assert!(
                    output.stdout.is_empty(),
                    "failed stop should not report success"
                );
                assert!(String::from_utf8_lossy(&output.stderr).contains("service"));
                assert!(
                    stopped_ids.borrow().is_empty(),
                    "failed selection must not stop a service"
                );
            }
        })
        .await;
}
