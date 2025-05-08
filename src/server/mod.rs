use crate::gossip::Gossip;
use crate::gossip_capnp::gossip::Client as GossipClient;
use crate::server_capnp::server;
use crate::topology_capnp::topology::Client as TopologyClient;
use capnp::capability::Promise;
use capnp::Error;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, FutureExt};

#[derive(Clone)]
pub struct ServerImpl {
    pub gossip_client: GossipClient,
    pub topology_client: TopologyClient,
    addr: String,
}

impl server::Server for ServerImpl {
    /// Get all capabilities.
    fn get_capabilities(
        &mut self,
        _params: server::GetCapabilitiesParams,
        mut results: server::GetCapabilitiesResults,
    ) -> Promise<(), capnp::Error> {
        let mut caps = results.get().init_caps();

        caps.set_gossip(self.gossip_client.clone());
        caps.set_topology(self.topology_client.clone());

        Promise::ok(())
    }

    /// Get the topology capability.
    ///
    /// We usually call this method when we want to have access to the
    /// topology service (membership management).
    fn get_topology(
        &mut self,
        _params: server::GetTopologyParams,
        mut results: server::GetTopologyResults,
    ) -> Promise<(), Error> {
        results.get().set_topology(self.topology_client.clone());
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
        results.get().set_gossip(self.gossip_client.clone());
        Promise::ok(())
    }
}

impl ServerImpl {
    /// Creates a new server.
    ///
    /// Returns the server and the memberlist actions to execute
    /// in a gossip loop.
    pub fn new(
        gossip_client: GossipClient,
        topology_client: TopologyClient,
        addr: impl Into<String>,
    ) -> Self {
        Self {
            gossip_client,
            topology_client,
            addr: addr.into(),
        }
    }

    /// Starts the server, bootstrapping all necessary sub-components
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error>> {
        tokio::task::LocalSet::new()
            .run_until(async move {
                let listener = tokio::net::TcpListener::bind(&self.addr).await?;

                println!("Server listening on {}", &self.addr);

                let server_handle: server::Client = capnp_rpc::new_client(self);

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
