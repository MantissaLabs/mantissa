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

        println!("push_chunks called");

        Promise::from_future(async move {
            let chunk = params.get()?.get_chunk()?;

            // Decode registers
            let regs = {
                let lst = chunk.get_regs()?;
                let mut out = Vec::with_capacity(lst.len() as usize);
                for i in 0..lst.len() {
                    let it = lst.get(i);
                    let key_bytes = it.get_key()?;
                    let reg_bytes = it.get_reg()?;
                    let key = peers
                        .from_wire_key(key_bytes)
                        .map_err(|e| capnp::Error::failed(e.to_string()))?;
                    let reg = peers
                        .from_wire_reg(reg_bytes)
                        .map_err(|e| capnp::Error::failed(e.to_string()))?;
                    out.push((key, reg));
                }
                out
            };

            // Decode tombstones
            let tombs = {
                let lst = chunk.get_tombs()?;
                let mut out = Vec::with_capacity(lst.len() as usize);
                for i in 0..lst.len() {
                    let it = lst.get(i);
                    let key_bytes = it.get_key()?;
                    let ts = it.get_ts();
                    let key = peers
                        .from_wire_key(key_bytes)
                        .map_err(|e| capnp::Error::failed(e.to_string()))?;
                    out.push((key, ts));
                }
                out
            };

            log::debug!("delta chunk: {} regs, {} tombs", regs.len(), tombs.len());
            println!("delta chunk: {} regs, {} tombs", regs.len(), tombs.len());

            // Apply to store
            peers
                .apply_delta_chunk(regs, tombs)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
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
