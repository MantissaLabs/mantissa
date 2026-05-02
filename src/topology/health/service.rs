use super::Health;
use mantissa_protocol::health::health;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

impl health::Server for Health {
    async fn ping(
        self: std::rc::Rc<Self>,
        _params: health::PingParams,
        mut results: health::PingResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;
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

    async fn indirect_ping(
        self: std::rc::Rc<Self>,
        params: health::IndirectPingParams,
        mut results: health::IndirectPingResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;
        let topo = self.clone_topology();
        let request = params.get()?;
        let target_id = request.get_target_id()?;
        let target = Uuid::from_slice(target_id)
            .map_err(|err| capnp::Error::failed(format!("invalid target id: {err}")))?;
        let timeout_ms = request.get_timeout_ms();
        let timeout = std::time::Duration::from_millis(timeout_ms.max(1));
        let ok = topo.health_indirect_ping(target, timeout).await;
        results.get().set_ok(ok);
        Ok(())
    }
}
