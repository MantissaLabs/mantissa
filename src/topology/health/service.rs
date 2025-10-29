use super::Health;
use protocol::health::health;
use std::time::{SystemTime, UNIX_EPOCH};

impl health::Server for Health {
    async fn ping(
        self: std::rc::Rc<Self>,
        _params: health::PingParams,
        mut results: health::PingResults,
    ) -> Result<(), capnp::Error> {
        let topo = self.clone_topology();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| capnp::Error::failed(e.to_string()))?
            .as_secs();

        let digest = topo.peers_root_digest().await.unwrap_or([0u8; 16]);

        let mut out = results.get();
        out.set_ok(true);
        out.set_now(now);
        out.set_root_digest(&digest);

        Ok(())
    }
}
