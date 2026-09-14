use capnp::Error;
use mantissa::network::types::compute_network_id;
use mantissa_protocol::{network::networks, server::cluster_session};
use std::{cell::RefCell, path::Path, process::Output, rc::Rc, time::Duration};
use tokio::{process::Command, task::LocalSet, time::timeout};
use uuid::Uuid;

struct NetworkServer {
    specs: Vec<(Uuid, String)>,
    deleted_batches: Rc<RefCell<Vec<Vec<Uuid>>>>,
}

impl networks::Server for NetworkServer {
    /// Looks up individual IDs so deletion cannot depend on listing all networks.
    async fn inspect(
        self: Rc<Self>,
        params: networks::InspectParams,
        mut results: networks::InspectResults,
    ) -> Result<(), Error> {
        let requested_id = params.get()?.get_id()?;
        let (id, name) = self
            .specs
            .iter()
            .find(|(id, _)| id.as_bytes() == requested_id)
            .ok_or_else(|| Error::failed("network not found".to_string()))?;

        let mut spec = results.get().init_network().init_spec();
        spec.set_id(id.as_bytes());
        spec.set_name(name.as_str());

        Ok(())
    }

    /// Records each batch to verify the selected IDs and the absence of partial requests.
    async fn delete(
        self: Rc<Self>,
        params: networks::DeleteParams,
        _results: networks::DeleteResults,
    ) -> Result<(), Error> {
        let ids = params
            .get()?
            .get_ids()?
            .iter()
            .map(|id| Uuid::from_slice(id?).map_err(|error| Error::failed(error.to_string())))
            .collect::<Result<Vec<_>, _>>()?;

        self.deleted_batches.borrow_mut().push(ids);

        Ok(())
    }
}

struct Session {
    networks: networks::Client,
}

impl cluster_session::Server for Session {
    /// Exposes only the network capability needed by the command.
    async fn get_networks(
        self: Rc<Self>,
        _params: cluster_session::GetNetworksParams,
        mut results: cluster_session::GetNetworksResults,
    ) -> Result<(), Error> {
        results.get().set_networks(self.networks.clone());
        Ok(())
    }
}

/// Starts an isolated socket with the same name-derived IDs used by network creation.
async fn start_network_socket(state_dir: &Path, deleted_batches: Rc<RefCell<Vec<Vec<Uuid>>>>) {
    let specs = ["frontend", "backend", "unrelated"]
        .into_iter()
        .map(|name| (compute_network_id(name), name.to_string()))
        .collect();
    let networks = capnp_rpc::new_client(NetworkServer {
        specs,
        deleted_batches,
    });
    let session = capnp_rpc::new_client(Session { networks });

    mantissa_net::unix_socket::start_unix_socket_server_at(
        session,
        state_dir.join("mantissa.sock"),
    )
    .await
    .expect("start test network socket");
}

/// Exercises CLI argument parsing and network selection without creating host interfaces.
async fn run_delete(state_dir: &Path, selectors: &[&str]) -> Output {
    timeout(
        Duration::from_secs(15),
        Command::new(env!("CARGO_BIN_EXE_mantissa"))
            .args(["networks", "delete"])
            .args(selectors)
            .env("MANTISSA_STATE_DIR", state_dir)
            .env("RUST_LOG", "off")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("delete request should finish")
    .expect("run CLI")
}

/// Names and UUIDs may be mixed, and repeated selections produce one deletion per network.
#[tokio::test]
async fn networks_delete_accepts_names_and_uuids_in_one_batch() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            let deleted_batches = Rc::new(RefCell::new(Vec::new()));
            start_network_socket(dir.path(), deleted_batches.clone()).await;
            let frontend_id = compute_network_id("frontend");
            let backend_id = compute_network_id("backend");

            let output = run_delete(
                dir.path(),
                &[
                    "frontend",
                    &backend_id.to_string(),
                    &frontend_id.to_string(),
                    "backend",
                    " frontend ",
                ],
            )
            .await;

            assert!(output.status.success(), "{output:?}");
            assert_eq!(
                String::from_utf8_lossy(&output.stdout).trim(),
                "requested deletion of 2 network(s)"
            );
            assert_eq!(*deleted_batches.borrow(), [vec![frontend_id, backend_id]]);
        })
        .await;
}

/// A bad target anywhere in a batch must prevent deletion of its valid targets.
#[tokio::test]
async fn networks_delete_rejects_unresolved_targets_without_mutation() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            let deleted_batches = Rc::new(RefCell::new(Vec::new()));
            start_network_socket(dir.path(), deleted_batches.clone()).await;

            for selector in [
                "missing",
                "front",
                "Frontend",
                "00000000-0000-0000-0000-000000000099",
                " ",
            ] {
                let output = run_delete(dir.path(), &["frontend", selector]).await;

                assert!(
                    !output.status.success(),
                    "unexpected success for {selector:?}"
                );
                assert!(output.stdout.is_empty());
                assert!(String::from_utf8_lossy(&output.stderr).contains("network"));
                assert!(
                    deleted_batches.borrow().is_empty(),
                    "failed selection must not delete any networks"
                );
            }
        })
        .await;
}
