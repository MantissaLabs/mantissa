use crate::{store::peer_store::PeersStore, sync_capnp::delta_sink};
use capnp::capability::Promise;

pub struct DeltaSinkImpl {
    peers: PeersStore, // already Arc
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

            // registers
            for it in c.get_regs()?.iter() {
                let k = peers.key_from_wire(it.get_key()?)?;
                let r = peers.reg_from_wire(it.get_reg()?)?;
                peers.merge_register(&k, &r).await?;
            }

            // tombstones
            for it in c.get_tombs()?.iter() {
                let k = peers.key_from_wire(it.get_key()?)?;
                let ts = it.get_ts();
                peers.apply_tombstone(&k, ts).await?;
            }

            Ok(())
        })
    }

    fn end(
        &mut self,
        _params: delta_sink::EndParams,
        _results: delta_sink::EndResults,
    ) -> Promise<(), capnp::Error> {
        log::debug!("delta stream end: rebuilding MST");
        println!("delta stream end: rebuilding MST");
        let peers = self.peers.clone();
        Promise::from_future(async move {
            peers
                .finalize_after_stream()
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            println!("finalized after stream");
            Ok(())
        })
    }
}
