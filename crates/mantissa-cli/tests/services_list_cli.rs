use capnp::Error;
use mantissa_protocol::{node, server::cluster_session, services};
use std::{
    cell::{Cell, RefCell},
    path::Path,
    rc::Rc,
    time::Duration,
};
use tokio::{process::Command, task::LocalSet, time::timeout};
use uuid::Uuid;

#[derive(Clone, Default)]
struct ListingServer {
    services: Rc<RefCell<Vec<(&'static str, services::ServiceStatus)>>>,
    node_requests: Rc<Cell<usize>>,
}

impl services::services::Server for ListingServer {
    /// Returns records according to the requested filter so the test exercises CLI-to-RPC forwarding.
    async fn list(
        self: Rc<Self>,
        params: services::services::ListParams,
        mut results: services::services::ListResults,
    ) -> Result<(), Error> {
        let include_stopped = params.get()?.get_include_stopped();
        let services = self.services.borrow();
        let selected = services
            .iter()
            .enumerate()
            .filter(|(_, (_, status))| {
                include_stopped || *status != services::ServiceStatus::Stopped
            })
            .collect::<Vec<_>>();

        let mut list = results.get().init_services(selected.len() as u32);
        for (index, (service_index, (name, status))) in selected.into_iter().enumerate() {
            let mut service = list.reborrow().get(index as u32);
            service.set_id(Uuid::from_u128(service_index as u128 + 1).as_bytes());
            service.set_manifest_id(Uuid::from_u128(100).as_bytes());
            service.set_service_name(*name);
            service.set_status(*status);

            let mut template = service.init_task_templates(1).get(0);
            template.set_name("web");
            template.set_replicas(1);
            template.set_public_port(8080);
        }

        Ok(())
    }
}

impl node::node::Server for ListingServer {
    /// Keeps stale endpoints for stopped services to verify they are not advertised in the list.
    async fn info(
        self: Rc<Self>,
        _params: node::node::InfoParams,
        mut results: node::node::InfoResults,
    ) -> Result<(), Error> {
        self.node_requests.set(self.node_requests.get() + 1);

        let services = self.services.borrow();
        let mut endpoints = results
            .get()
            .init_info()
            .init_public_endpoints(services.len() as u32);
        for (index, _) in services.iter().enumerate() {
            let mut endpoint = endpoints.reborrow().get(index as u32);
            endpoint.set_service_id(Uuid::from_u128(index as u128 + 1).to_string());
            endpoint.set_template_name("web");
            endpoint.set_node_ip("192.0.2.1");
            endpoint.set_public_port(8080);
            endpoint.set_protocol("tcp");
            endpoint.set_ingress_mode("all_nodes");
            endpoint.set_ready(true);
        }

        Ok(())
    }
}

struct Session {
    services: services::services::Client,
    node: node::node::Client,
}

impl cluster_session::Server for Session {
    /// Exposes the service list capability used by the command.
    async fn get_services(
        self: Rc<Self>,
        _params: cluster_session::GetServicesParams,
        mut results: cluster_session::GetServicesResults,
    ) -> Result<(), Error> {
        results.get().set_services(self.services.clone());
        Ok(())
    }

    /// Supplies public endpoint diagnostics independently from service state.
    async fn get_node(
        self: Rc<Self>,
        _params: cluster_session::GetNodeParams,
        mut results: cluster_session::GetNodeResults,
    ) -> Result<(), Error> {
        results.get().set_node(self.node.clone());
        Ok(())
    }
}

/// Runs the real CLI with an isolated socket and a deadline for stalled RPCs.
async fn run_list(state_dir: &Path, arguments: &[&str]) -> String {
    let output = timeout(
        Duration::from_secs(15),
        Command::new(env!("CARGO_BIN_EXE_mantissa"))
            .arg("services")
            .args(arguments)
            .env("MANTISSA_STATE_DIR", state_dir)
            .env("RUST_LOG", "off")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("service listing should finish")
    .expect("run CLI");

    assert!(output.status.success(), "{output:?}");
    String::from_utf8(output.stdout).expect("service list is UTF-8")
}

/// Reads the table's name column so filtering and sorting checks do not depend on padding.
fn service_names(output: &str) -> Vec<&str> {
    output
        .lines()
        .skip(1)
        .filter_map(|line| line.split_whitespace().next())
        .collect()
}

/// Covers default filtering, both flag forms, retained metadata, and empty-list guidance.
#[tokio::test]
async fn services_list_includes_stopped_only_when_requested() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("create isolated state directory");
            let server = ListingServer::default();
            server.services.borrow_mut().extend([
                ("retired", services::ServiceStatus::Stopped),
                ("waiting", services::ServiceStatus::Stopping),
                ("api", services::ServiceStatus::Running),
                ("broken", services::ServiceStatus::Failed),
            ]);

            let session = capnp_rpc::new_client(Session {
                services: capnp_rpc::new_client(server.clone()),
                node: capnp_rpc::new_client(server.clone()),
            });
            mantissa_net::unix_socket::start_unix_socket_server_at(
                session,
                dir.path().join("mantissa.sock"),
            )
            .await
            .expect("start service list socket");

            let active = run_list(dir.path(), &["list"]).await;
            assert_eq!(service_names(&active), ["api", "broken", "waiting"]);
            assert!(active.contains("192.0.2.1:8080"));

            let all = run_list(dir.path(), &["list", "--all"]).await;
            assert_eq!(service_names(&all), ["api", "broken", "retired", "waiting"]);

            let stopped = all
                .lines()
                .find(|line| line.starts_with("retired"))
                .expect("stopped service row");
            assert!(stopped.contains("stopped"));
            assert!(stopped.contains("web (1x)"));
            assert!(!stopped.contains("192.0.2.1"));

            let short = run_list(dir.path(), &["ls", "-a"]).await;
            assert_eq!(short, all);
            assert_eq!(server.node_requests.get(), 3);

            server
                .services
                .borrow_mut()
                .retain(|(_, status)| *status == services::ServiceStatus::Stopped);
            let active = run_list(dir.path(), &["list"]).await;
            assert!(active.contains("no active services"));
            assert!(active.contains("services list --all"));

            let all = run_list(dir.path(), &["list", "--all"]).await;
            assert_eq!(service_names(&all), ["retired"]);
            assert_eq!(
                server.node_requests.get(),
                3,
                "stopped-only lists need no endpoint lookup"
            );

            server.services.borrow_mut().clear();
            let empty = run_list(dir.path(), &["list", "--all"]).await;
            assert_eq!(empty, "no services registered\n");
        })
        .await;
}
