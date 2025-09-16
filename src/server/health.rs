use capnp::capability::Promise;
use protocol::health::health;

#[derive(Clone)]
pub struct HealthImpl {
    topology: crate::topology::Topology,
}

impl HealthImpl {
    pub fn new(topology: crate::topology::Topology) -> Self {
        Self { topology }
    }
}

impl health::Server for HealthImpl {
    fn ping(
        &mut self,
        _params: health::PingParams,
        mut results: health::PingResults,
    ) -> Promise<(), capnp::Error> {
        let topo = self.topology.clone();
        Promise::from_future(async move {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| capnp::Error::failed(e.to_string()))?
                .as_secs();

            let digest = topo.peers_root_digest().await.unwrap_or([0u8; 16]);
            let mut out = results.get();
            out.set_ok(true);
            out.set_now(now);
            out.set_root_digest(&digest);
            Ok(())
        })
    }
}
