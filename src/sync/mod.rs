use crate::{
    store::{
        crdt::{
            mst_store::{capnp_fill_ranges, owned_ranges_from_capnp},
            uuid_key::UuidKey,
        },
        peer_store::PeersStore,
    },
    sync_capnp::{sync, Domain},
};
use capnp::capability::Promise;

pub mod delta;

impl SyncService {
    pub fn new(peers: PeersStore) -> Self {
        Self { peers }
    }
}

pub struct SyncService {
    peers: PeersStore,
}

impl sync::Server for SyncService {
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
                    println!("getRanges: received");
                    peers.debug_dump_root("server.before.get_ranges").await;
                    peers.debug_dump_ranges("server.before.get_ranges", 5).await;

                    let owned = peers
                        .mst_ranges_owned()
                        .await
                        .map_err(|e| capnp::Error::failed(e.to_string()))?;

                    let mut out = results.get().init_summary();
                    capnp_fill_ranges::<UuidKey>(&owned, out)?;
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
        println!("open_delta: received");

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

                // Convert PageRangeSummary -> Vec<OwnedPageRange>
                let want = owned_ranges_from_capnp::<UuidKey>(p.get_want()?)?;

                println!("open_delta: received");
                peers.debug_dump_root("server.before.open_delta").await;
                peers.debug_dump_ranges("server.before.open_delta", 5).await;

                let want = owned_ranges_from_capnp::<UuidKey>(p.get_want()?)?;
                println!("open_delta: want ranges = {}", want.len());

                let (regs, tombs) = peers
                    .export_delta_for_owned(&want)
                    .map_err(|e| capnp::Error::failed(e.to_string()))?;

                println!(
                    "open_delta: exporting regs={}, tombs={}",
                    regs.len(),
                    tombs.len()
                );

                // Pre-encode to wire bytes
                let mut regs_wire: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(regs.len());
                for (k, r) in regs {
                    regs_wire.push((
                        peers.to_wire_key(&k),
                        peers
                            .to_wire_reg(&r)
                            .map_err(|e| capnp::Error::failed(e.to_string()))?,
                    ));
                }
                let tombs_wire: Vec<(Vec<u8>, u64)> = tombs
                    .into_iter()
                    .map(|(k, ts)| (peers.to_wire_key(&k), ts))
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
