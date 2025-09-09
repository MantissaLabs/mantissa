use crate::{
    includes::sync_capnp::{sync, Domain},
    store::{
        crdt::{
            mst_store::{capnp_fill_ranges, compute_want_from_owned, owned_ranges_from_capnp},
            uuid_key::UuidKey,
        },
        peer_store::PeersStore,
    },
    sync_capnp::delta_sink,
};
use capnp::capability::Promise;
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

            // tombstones
            for it in c.get_tombs()?.iter() {
                let k = peers.key_from_wire(it.get_key()?)?;
                let ts = it.get_ts();
                peers.apply_tombstone(&k, ts).await?;
            }

            // registers
            for it in c.get_regs()?.iter() {
                let k = peers.key_from_wire(it.get_key()?)?;
                let r = peers.reg_from_wire(it.get_reg()?)?;
                peers.merge_register(&k, &r).await?;
            }

            Ok(())
        })
    }

    fn end(
        &mut self,
        _params: delta_sink::EndParams,
        _results: delta_sink::EndResults,
    ) -> Promise<(), capnp::Error> {
        debug!(target: "delta", "delta stream end: rebuilding MST");

        let peers = self.peers.clone();
        Promise::from_future(async move {
            peers
                .finalize_after_stream()
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            debug!(target: "delta",  "finalized after stream");

            Ok(())
        })
    }
}

fn io_to_capnp(e: std::io::Error) -> capnp::Error {
    capnp::Error::failed(e.to_string())
}

pub async fn sync_peers_after_join(peers: PeersStore, sync_cap: sync::Client) {
    let res: Result<(), capnp::Error> = async {
        // Fast path: compare roots.
        let mut gr = sync_cap.get_root_request();
        gr.get().set_domain(Domain::Peers);
        let root_resp = gr.send().promise.await?;
        let remote_root: String = root_resp.get()?.get_root_hex()?.to_string()?;

        // root_hex() returns String, so no map_err here
        let local_root = peers.root_hex().await;

        if remote_root == local_root {
            return Ok(());
        }

        // Fetch remote ranges
        let mut rr = sync_cap.get_ranges_request();
        rr.get().set_domain(Domain::Peers);
        let ranges_resp = rr.send().promise.await?;
        let remote_owned = owned_ranges_from_capnp::<UuidKey>(ranges_resp.get()?.get_summary()?)?;

        // Local ranges (this is io::Result, so convert)
        let local_owned = peers.mst_ranges_owned().await.map_err(io_to_capnp)?;

        // Compute want
        let want_owned = compute_want_from_owned(&remote_owned, &local_owned);
        if want_owned.is_empty() {
            debug!(target: "sync", "want empty ranges, nothing to fetch");
            return Ok(());
        }

        debug!(target: "sync", "want ranges = {}", want_owned.len());
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
            capnp_fill_ranges::<UuidKey>(&want_owned, want_builder)?;
            p.set_sink(sink_client);
        }

        debug!(target: "sync", "opening delta stream...");
        od.send().promise.await?;
        debug!(target: "sync", "delta stream finished");

        Ok(())
    }
    .await;

    if let Err(e) = res {
        println!("sync_after_join error: {}", e);
    }
}
