use crate::server_capnp::server;
use crate::server_capnp::server::Server;

use capnp::capability::Promise;
use capnp::Error;
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};

use futures::{AsyncReadExt, FutureExt};
use std::net::ToSocketAddrs;

#[derive(Clone)]
pub struct ServerImpl {
    addr: String,
}

impl server::Server for ServerImpl {
    /// Get the topology capability.
    ///
    /// We usually call this method when we want to have access to the
    /// topology service (membership management).
    fn get_topology(
        &mut self,
        _params: server::GetTopologyParams,
        mut results: server::GetTopologyResults,
    ) -> Promise<(), Error> {
        Promise::ok(())
    }

    /// Get the delegate capability.
    ///
    /// We usually call this method when we want to have access to the
    /// delegate service (task management and scheduling).
    fn get_delegate(
        &mut self,
        _params: server::GetDelegateParams,
        mut results: server::GetDelegateResults,
    ) -> Promise<(), Error> {
        Promise::ok(())
    }

    /// Get the gossip capability.
    ///
    /// We usually call this method when we want to have access to the
    /// gossip service (epidemic spread of information in the cluster).
    fn get_gossip(
        &mut self,
        _params: server::GetGossipParams,
        mut results: server::GetGossipResults,
    ) -> Promise<(), Error> {
        Promise::ok(())
    }
}

impl ServerImpl {
    /// Creates a new server.
    ///
    /// Returns the server and the memberlist actions to execute
    /// in a gossip loop.
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }

    /// Starts the server, bootstrapping all necessary sub-components
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error>> {
        tokio::task::LocalSet::new()
            .run_until(async move {
                let listener = tokio::net::TcpListener::bind(&self.addr).await?;
                println!("Server listening on {}", &self.addr);

                let server_handle: server::Client = capnp_rpc::new_client(ServerImpl {
                    addr: self.addr.clone(),
                });

                println!("Server running");
                loop {
                    let (stream, _) = listener.accept().await?;
                    stream.set_nodelay(true)?;
                    let (reader, writer) =
                        tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();

                    let network = twoparty::VatNetwork::new(
                        reader,
                        writer,
                        rpc_twoparty_capnp::Side::Server,
                        Default::default(),
                    );

                    let rpc_system =
                        RpcSystem::new(Box::new(network), Some(server_handle.clone().client));

                    tokio::task::spawn_local(Box::pin(rpc_system.map(|_| ())));
                }
            })
            .await
    }
}
