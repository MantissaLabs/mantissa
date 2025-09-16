use crate::store::peer_store::PeersStore;
use crate::sync::ranges::{capnp_fill_ranges, page_ranges_from_capnp};
use bincode;
use capnp::capability::Promise;
use crdt_store::{compute_want_from_have, uuid_key::UuidKey};
use crdts::MVReg;
use protocol::sync::delta_sink;
use protocol::sync::{Domain, sync};
use tracing::debug;

pub struct DeltaSinkImpl {
    peers: PeersStore,
}

impl DeltaSinkImpl {
    pub fn new(peers: PeersStore) -> Self {
        Self { peers }
    }
}

impl delta_sink::Server for DeltaSinkImpl {
    fn push_chunk(&mut self, params: delta_sink::PushChunkParams) -> Promise<(), capnp::Error> {
        let peers = self.peers.clone();
        Promise::from_future(async move {
            let c = params.get()?.get_chunk()?;

            // Collect tombstones and registers, then apply in one batch write.
            let mut tombs = Vec::new();
            for it in c.get_tombs()?.iter() {
                let key = UuidKey::try_from(it.get_key()?)
                    .map_err(|e| capnp::Error::failed(e.to_string()))?;

                let tombstone = it.get_ts();

                tombs.push((key, tombstone));
            }

            let mut regs = Vec::new();
            for it in c.get_regs()?.iter() {
                let key = UuidKey::try_from(it.get_key()?)
                    .map_err(|e| capnp::Error::failed(e.to_string()))?;

                let register: MVReg<crate::topology::peers::PeerValue, uuid::Uuid> =
                    bincode::deserialize(it.get_reg()?)
                        .map_err(|e| capnp::Error::failed(e.to_string()))?;

                regs.push((key, register));
            }

            peers
                .apply_delta_chunk_update_mst(regs, tombs)
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            Ok(())
        })
    }

    fn end(
        &mut self,
        _params: delta_sink::EndParams,
        _results: delta_sink::EndResults,
    ) -> Promise<(), capnp::Error> {
        debug!(target: "delta", "delta stream end");

        // Incremental apply keeps MST up-to-date; nothing to finalize.
        Promise::ok(())
    }
}

fn to_capnp<E: std::fmt::Display>(e: E) -> capnp::Error {
    capnp::Error::failed(e.to_string())
}

pub async fn sync_peers_after_join(peers: PeersStore, sync_cap: sync::Client) {
    let res: Result<(), capnp::Error> = async {
        let mut gr = sync_cap.get_root_request();
        gr.get().set_domain(Domain::Peers);
        let root_resp = gr.send().promise.await?;

        let remote_root = root_resp.get()?.get_root_hex()?.to_string()?;
        let local_root = peers.root_hex().await;

        // Compare roots, if equal: nothing to sync.
        if remote_root == local_root {
            return Ok(());
        }

        // Fetch remote ranges
        let mut rr = sync_cap.get_ranges_request();
        rr.get().set_domain(Domain::Peers);
        let ranges_resp = rr.send().promise.await?;
        let remote_page_ranges = page_ranges_from_capnp(ranges_resp.get()?.get_summary()?)?;

        // Local ranges
        let local_page_ranges = peers.page_range_summary().await.map_err(to_capnp)?;

        // Compute want
        let want_ranges = compute_want_from_have(&remote_page_ranges, &local_page_ranges);
        if want_ranges.is_empty() {
            debug!(target: "sync", "want empty ranges, nothing to fetch");
            return Ok(());
        }

        debug!(target: "sync", "want ranges = {}", want_ranges.len());
        peers
            .debug_dump_root("client.local.before_open_delta")
            .await;
        peers
            .debug_dump_ranges("client.local.before_open_delta", 5)
            .await;

        // Stream delta into local sink
        let sink_client = capnp_rpc::new_client(DeltaSinkImpl::new(peers.clone()));
        let mut od = sync_cap.open_delta_request();
        {
            let mut p = od.get();
            p.set_domain(Domain::Peers);
            let want_builder = p.reborrow().init_want();
            capnp_fill_ranges(&want_ranges, want_builder)?;
            p.set_sink(sink_client);
        }

        debug!(target: "sync", "opening delta stream...");
        od.send().promise.await?;
        debug!(target: "sync", "delta stream finished");

        Ok(())
    }
    .await;

    if let Err(e) = res {
        println!("sync_after_join error: {e}");
    }
}
