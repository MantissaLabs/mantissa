use crate::sync::ranges::{capnp_fill_ranges, page_ranges_from_capnp};
use crate::{
    store::peer_store::PeersStore,
    sync_capnp::{sync, Domain},
};
use capnp::capability::Promise;
use tracing::debug;

pub mod delta;
pub mod ranges;

impl SyncService {
    pub fn new(peers: PeersStore) -> Self {
        Self { peers }
    }
}

pub struct SyncService {
    peers: PeersStore,
}

impl sync::Server for SyncService {
    fn get_root(
        &mut self,
        params: sync::GetRootParams,
        mut results: sync::GetRootResults,
    ) -> Promise<(), capnp::Error> {
        let peers = self.peers.clone();
        Promise::from_future(async move {
            match params.get()?.get_domain()? {
                Domain::Peers => {
                    let root = peers.root_hex().await;
                    let mut out = results.get();
                    out.set_root_hex(&root);
                    Ok(())
                }
                _ => Err(capnp::Error::unimplemented(
                    "domain not implemented".to_string(),
                )),
            }
        })
    }

    fn get_ranges(
        &mut self,
        params: sync::GetRangesParams,
        mut results: sync::GetRangesResults,
    ) -> Promise<(), capnp::Error> {
        let peers = self.peers.clone();

        Promise::from_future(async move {
            let p = params.get()?;

            match p.get_domain()? {
                Domain::Peers => {
                    debug!("getRanges: received");
                    peers.debug_dump_root("server.before.get_ranges").await;
                    peers.debug_dump_ranges("server.before.get_ranges", 5).await;

                    let ranges = peers
                        .page_range_summary()
                        .await
                        .map_err(|e| capnp::Error::failed(e.to_string()))?;

                    let out = results.get().init_summary();
                    capnp_fill_ranges(&ranges, out)?;
                    Ok(())
                }
                _ => Err(capnp::Error::unimplemented(
                    "domain not implemented".to_string(),
                )),
            }
        })
    }

    fn open_delta(
        &mut self,
        params: sync::OpenDeltaParams,
        _results: sync::OpenDeltaResults,
    ) -> Promise<(), capnp::Error> {
        debug!("open_delta: received");

        Promise::from_future({
            let peers = self.peers.clone();
            async move {
                let p = params.get()?;

                // Domain gate
                match p.get_domain()? {
                    Domain::Peers => {}
                    _ => {
                        return Err(capnp::Error::unimplemented(
                            "domain not implemented".to_string(),
                        ))
                    }
                }

                debug!(target: "delta", "open_delta: received");
                peers.debug_dump_root("server.before.open_delta").await;
                peers.debug_dump_ranges("server.before.open_delta", 5).await;

                // Client sends the delta ranges it needs, not its full summary.
                let want = page_ranges_from_capnp(p.get_want()?)?;
                debug!(target: "delta", "open_delta: want ranges = {}", want.len());

                // If there's no delta to send, end immediately.
                if want.is_empty() {
                    debug!(target: "delta", "open_delta: no ranges requested; exporting regs=0, tombs=0");
                    p.get_sink()?.end_request().send().promise.await?;
                    return Ok(());
                }

                let (regs, tombs) = peers
                    .export_page_ranges_delta(&want)
                    .map_err(|e| capnp::Error::failed(e.to_string()))?;

                debug!(
                    target: "delta",
                    "open_delta: exporting regs={}, tombs={}",
                    regs.len(),
                    tombs.len()
                );

                // Pre-encode to wire bytes (keys are raw bytes of UuidKey, regs via bincode)
                let mut regs_wire: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(regs.len());
                for (k, r) in regs {
                    let key_bytes = k.as_ref().to_vec();
                    let reg_bytes =
                        bincode::serialize(&r).map_err(|e| capnp::Error::failed(e.to_string()))?;
                    regs_wire.push((key_bytes, reg_bytes));
                }

                let tombs_wire: Vec<(Vec<u8>, u64)> = tombs
                    .into_iter()
                    .map(|(k, ts)| (k.as_ref().to_vec(), ts))
                    .collect();

                // Create a simple cursor and stream to the client sink
                let sink = p.get_sink()?; // client-implemented DeltaSink
                const MAX: usize = 1000;
                let mut i_reg = 0usize;
                let mut i_tmb = 0usize;

                loop {
                    let end_r = (i_reg + MAX).min(regs_wire.len());
                    let chunk_regs = regs_wire[i_reg..end_r].to_vec();
                    i_reg = end_r;

                    let left = MAX.saturating_sub(chunk_regs.len());
                    let end_t = (i_tmb + left).min(tombs_wire.len());
                    let chunk_tombs = tombs_wire[i_tmb..end_t].to_vec();
                    i_tmb = end_t;

                    if chunk_regs.is_empty() && chunk_tombs.is_empty() {
                        break;
                    }

                    // Build and send one streaming chunk
                    let mut req = sink.push_chunk_request();
                    {
                        let mut ch = req.get().init_chunk();
                        let mut rl = ch.reborrow().init_regs(chunk_regs.len() as u32);
                        for (i, (k, r)) in chunk_regs.into_iter().enumerate() {
                            let mut it = rl.reborrow().get(i as u32);
                            it.set_key(&k);
                            it.set_reg(&r);
                        }
                        let mut tl = ch.reborrow().init_tombs(chunk_tombs.len() as u32);
                        for (i, (k, ts)) in chunk_tombs.into_iter().enumerate() {
                            let mut it = tl.reborrow().get(i as u32);
                            it.set_key(&k);
                            it.set_ts(ts);
                        }
                    }
                    // backpressure-aware: resolves when it's safe to enqueue next chunk
                    req.send().await?;
                }

                // Signal end-of-stream
                sink.end_request().send().promise.await?;
                Ok(())
            }
        })
    }
}
