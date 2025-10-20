use crate::store::network_store::{NetworkAttachmentStore, NetworkPeerStore, NetworkSpecStore};
use crate::store::peer_store::PeersStore;
use crate::store::secret_store::SecretStore;
use crate::store::service_store::ServiceStore;
use crate::store::task_store::TaskStore;
use crate::sync::ranges::{capnp_fill_ranges, page_ranges_from_capnp};
use capnp::capability::Promise;
use crdt_store::mst_store::{Registers, Tombstones};
use crdt_store::uuid_key::UuidKey;
use protocol::sync::{Domain, delta_sink, sync};
use tracing::debug;

pub mod delta;
pub mod ranges;

// Chunk size used when streaming delta from server to client. Adjust as needed.
pub const DELTA_CHUNK_MAX: usize = 1024;

pub struct SyncService {
    peers: PeersStore,
    tasks: TaskStore,
    services: ServiceStore,
    secrets: SecretStore,
    networks: NetworkSpecStore,
    network_peers: NetworkPeerStore,
    network_attachments: NetworkAttachmentStore,
}

impl SyncService {
    pub fn new(
        peers: PeersStore,
        tasks: TaskStore,
        services: ServiceStore,
        secrets: SecretStore,
        networks: NetworkSpecStore,
        network_peers: NetworkPeerStore,
        network_attachments: NetworkAttachmentStore,
    ) -> Self {
        Self {
            peers,
            tasks,
            services,
            secrets,
            networks,
            network_peers,
            network_attachments,
        }
    }
}

impl sync::Server for SyncService {
    fn get_roots(
        &mut self,
        _params: sync::GetRootsParams,
        mut results: sync::GetRootsResults,
    ) -> Promise<(), capnp::Error> {
        let peers = self.peers.clone();
        let tasks = self.tasks.clone();
        let services = self.services.clone();
        let secrets = self.secrets.clone();
        let networks = self.networks.clone();
        let network_peers = self.network_peers.clone();
        let network_attachments = self.network_attachments.clone();
        Promise::from_future(async move {
            const DOMAINS: [Domain; 7] = [
                Domain::Peers,
                Domain::Tasks,
                Domain::Services,
                Domain::Secrets,
                Domain::Networks,
                Domain::NetworkPeers,
                Domain::NetworkAttachments,
            ];

            let mut list = results.get().init_roots(DOMAINS.len() as u32);
            for (idx, domain) in DOMAINS.iter().enumerate() {
                let root_hex = match domain {
                    Domain::Peers => peers.root_hex().await,
                    Domain::Tasks => tasks.root_hex().await,
                    Domain::Services => services.root_hex().await,
                    Domain::Secrets => secrets.root_hex().await,
                    Domain::Networks => networks.root_hex().await,
                    Domain::NetworkPeers => network_peers.root_hex().await,
                    Domain::NetworkAttachments => network_attachments.root_hex().await,
                };
                let mut entry = list.reborrow().get(idx as u32);
                entry.set_domain(*domain);
                entry.set_root_hex(&root_hex);
            }

            Ok(())
        })
    }

    fn get_ranges(
        &mut self,
        params: sync::GetRangesParams,
        mut results: sync::GetRangesResults,
    ) -> Promise<(), capnp::Error> {
        let peers = self.peers.clone();
        let tasks = self.tasks.clone();
        let services = self.services.clone();
        let secrets = self.secrets.clone();
        let networks = self.networks.clone();
        let network_peers = self.network_peers.clone();
        let network_attachments = self.network_attachments.clone();

        Promise::from_future(async move {
            let requested_domains: Vec<Domain> = {
                let domains_reader = params.get()?.get_domains()?;
                if domains_reader.is_empty() {
                    vec![
                        Domain::Peers,
                        Domain::Tasks,
                        Domain::Services,
                        Domain::Secrets,
                        Domain::Networks,
                        Domain::NetworkPeers,
                        Domain::NetworkAttachments,
                    ]
                } else {
                    let mut out = Vec::with_capacity(domains_reader.len() as usize);
                    for domain in domains_reader.iter() {
                        out.push(domain?);
                    }
                    out
                }
            };

            let mut list = results.get().init_ranges(requested_domains.len() as u32);
            for (idx, domain) in requested_domains.iter().enumerate() {
                match domain {
                    Domain::Peers => {
                        debug!("getRanges: received (peers)");
                        peers.debug_dump_root("server.before.get_ranges").await;
                        peers.debug_dump_ranges("server.before.get_ranges", 5).await;
                        let ranges = peers
                            .page_range_summary()
                            .await
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        let mut entry = list.reborrow().get(idx as u32);
                        entry.set_domain(Domain::Peers);
                        let summary = entry.reborrow().init_summary();
                        capnp_fill_ranges(&ranges, summary)?;
                    }
                    Domain::Tasks => {
                        debug!("getRanges: received (tasks)");
                        tasks
                            .debug_dump_root("server.before.get_ranges.tasks")
                            .await;
                        tasks
                            .debug_dump_ranges("server.before.get_ranges.tasks", 5)
                            .await;
                        let ranges = tasks
                            .page_range_summary()
                            .await
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        let mut entry = list.reborrow().get(idx as u32);
                        entry.set_domain(Domain::Tasks);
                        let summary = entry.reborrow().init_summary();
                        capnp_fill_ranges(&ranges, summary)?;
                    }
                    Domain::Services => {
                        debug!("getRanges: received (services)");
                        services
                            .debug_dump_root("server.before.get_ranges.services")
                            .await;
                        services
                            .debug_dump_ranges("server.before.get_ranges.services", 5)
                            .await;
                        let ranges = services
                            .page_range_summary()
                            .await
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        let mut entry = list.reborrow().get(idx as u32);
                        entry.set_domain(Domain::Services);
                        let summary = entry.reborrow().init_summary();
                        capnp_fill_ranges(&ranges, summary)?;
                    }
                    Domain::Secrets => {
                        debug!("getRanges: received (secrets)");
                        secrets
                            .debug_dump_root("server.before.get_ranges.secrets")
                            .await;
                        secrets
                            .debug_dump_ranges("server.before.get_ranges.secrets", 5)
                            .await;
                        let ranges = secrets
                            .page_range_summary()
                            .await
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        let mut entry = list.reborrow().get(idx as u32);
                        entry.set_domain(Domain::Secrets);
                        let summary = entry.reborrow().init_summary();
                        capnp_fill_ranges(&ranges, summary)?;
                    }
                    Domain::Networks => {
                        debug!("getRanges: received (networks)");
                        networks
                            .debug_dump_root("server.before.get_ranges.networks")
                            .await;
                        networks
                            .debug_dump_ranges("server.before.get_ranges.networks", 5)
                            .await;
                        let ranges = networks
                            .page_range_summary()
                            .await
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        let mut entry = list.reborrow().get(idx as u32);
                        entry.set_domain(Domain::Networks);
                        let summary = entry.reborrow().init_summary();
                        capnp_fill_ranges(&ranges, summary)?;
                    }
                    Domain::NetworkPeers => {
                        debug!("getRanges: received (network peers)");
                        network_peers
                            .debug_dump_root("server.before.get_ranges.network_peers")
                            .await;
                        network_peers
                            .debug_dump_ranges("server.before.get_ranges.network_peers", 5)
                            .await;
                        let ranges = network_peers
                            .page_range_summary()
                            .await
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        let mut entry = list.reborrow().get(idx as u32);
                        entry.set_domain(Domain::NetworkPeers);
                        let summary = entry.reborrow().init_summary();
                        capnp_fill_ranges(&ranges, summary)?;
                    }
                    Domain::NetworkAttachments => {
                        debug!("getRanges: received (network attachments)");
                        network_attachments
                            .debug_dump_root("server.before.get_ranges.network_attachments")
                            .await;
                        network_attachments
                            .debug_dump_ranges("server.before.get_ranges.network_attachments", 5)
                            .await;
                        let ranges = network_attachments
                            .page_range_summary()
                            .await
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        let mut entry = list.reborrow().get(idx as u32);
                        entry.set_domain(Domain::NetworkAttachments);
                        let summary = entry.reborrow().init_summary();
                        capnp_fill_ranges(&ranges, summary)?;
                    }
                }
            }

            Ok(())
        })
    }

    fn open_delta(
        &mut self,
        params: sync::OpenDeltaParams,
        _results: sync::OpenDeltaResults,
    ) -> Promise<(), capnp::Error> {
        let peers = self.peers.clone();
        let tasks = self.tasks.clone();
        let services = self.services.clone();
        let secrets = self.secrets.clone();
        let networks = self.networks.clone();
        let network_peers = self.network_peers.clone();
        let network_attachments = self.network_attachments.clone();

        Promise::from_future(async move {
            let p = params.get()?;
            let wants_reader = p.get_wants()?;
            let sink = p.get_sink()?;

            if wants_reader.is_empty() {
                sink.end_request().send().promise.await?;
                return Ok(());
            }

            let mut sent_chunks = false;

            for idx in 0..wants_reader.len() {
                let want = wants_reader.get(idx);
                let domain = want
                    .get_domain()
                    .map_err(|_| capnp::Error::failed("unknown sync domain".into()))?;
                let want_ranges = page_ranges_from_capnp(want.get_want()?)?;
                if want_ranges.is_empty() {
                    continue;
                }

                match domain {
                    Domain::Peers => {
                        debug!(target: "delta", "open_delta: received (peers)");
                        peers.debug_dump_root("server.before.open_delta").await;
                        peers.debug_dump_ranges("server.before.open_delta", 5).await;
                        let (regs, tombs) = peers
                            .export_page_ranges_delta(&want_ranges)
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        if send_chunks(domain, regs, tombs, &sink).await? {
                            sent_chunks = true;
                        }
                    }
                    Domain::Tasks => {
                        debug!(target: "delta", "open_delta: received (tasks)");
                        tasks
                            .debug_dump_root("server.before.open_delta.tasks")
                            .await;
                        tasks
                            .debug_dump_ranges("server.before.open_delta.tasks", 5)
                            .await;
                        let (regs, tombs) = tasks
                            .export_page_ranges_delta(&want_ranges)
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        if send_chunks(domain, regs, tombs, &sink).await? {
                            sent_chunks = true;
                        }
                    }
                    Domain::Services => {
                        debug!(target: "delta", "open_delta: received (services)");
                        services
                            .debug_dump_root("server.before.open_delta.services")
                            .await;
                        services
                            .debug_dump_ranges("server.before.open_delta.services", 5)
                            .await;
                        let (regs, tombs) = services
                            .export_page_ranges_delta(&want_ranges)
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        if send_chunks(domain, regs, tombs, &sink).await? {
                            sent_chunks = true;
                        }
                    }
                    Domain::Secrets => {
                        debug!(target: "delta", "open_delta: received (secrets)");
                        secrets
                            .debug_dump_root("server.before.open_delta.secrets")
                            .await;
                        secrets
                            .debug_dump_ranges("server.before.open_delta.secrets", 5)
                            .await;
                        let (regs, tombs) = secrets
                            .export_page_ranges_delta(&want_ranges)
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        if send_chunks(domain, regs, tombs, &sink).await? {
                            sent_chunks = true;
                        }
                    }
                    Domain::Networks => {
                        debug!(target: "delta", "open_delta: received (networks)");
                        networks
                            .debug_dump_root("server.before.open_delta.networks")
                            .await;
                        networks
                            .debug_dump_ranges("server.before.open_delta.networks", 5)
                            .await;
                        let (regs, tombs) = networks
                            .export_page_ranges_delta(&want_ranges)
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        if send_chunks(domain, regs, tombs, &sink).await? {
                            sent_chunks = true;
                        }
                    }
                    Domain::NetworkPeers => {
                        debug!(target: "delta", "open_delta: received (network peers)");
                        network_peers
                            .debug_dump_root("server.before.open_delta.network_peers")
                            .await;
                        network_peers
                            .debug_dump_ranges("server.before.open_delta.network_peers", 5)
                            .await;
                        let (regs, tombs) = network_peers
                            .export_page_ranges_delta(&want_ranges)
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        if send_chunks(domain, regs, tombs, &sink).await? {
                            sent_chunks = true;
                        }
                    }
                    Domain::NetworkAttachments => {
                        debug!(target: "delta", "open_delta: received (network attachments)");
                        network_attachments
                            .debug_dump_root("server.before.open_delta.network_attachments")
                            .await;
                        network_attachments
                            .debug_dump_ranges("server.before.open_delta.network_attachments", 5)
                            .await;
                        let (regs, tombs) = network_attachments
                            .export_page_ranges_delta(&want_ranges)
                            .map_err(|e| capnp::Error::failed(e.to_string()))?;
                        if send_chunks(domain, regs, tombs, &sink).await? {
                            sent_chunks = true;
                        }
                    }
                }
            }

            if !sent_chunks {
                debug!(target: "delta", "open_delta: no chunks emitted");
            }

            sink.end_request().send().promise.await?;
            Ok(())
        })
    }
}

fn encode_registers<R>(regs: Registers<UuidKey, R>) -> Result<Vec<(Vec<u8>, Vec<u8>)>, capnp::Error>
where
    R: serde::Serialize,
{
    let mut out = Vec::with_capacity(regs.len());
    for (k, r) in regs {
        let key_bytes = k.as_ref().to_vec();
        let reg_bytes = bincode::serialize(&r).map_err(|e| capnp::Error::failed(e.to_string()))?;
        out.push((key_bytes, reg_bytes));
    }
    Ok(out)
}

fn encode_tombstones(tombs: Tombstones<UuidKey>) -> Vec<(Vec<u8>, u64)> {
    tombs
        .into_iter()
        .map(|(k, ts)| (k.as_ref().to_vec(), ts))
        .collect()
}

async fn send_chunks<R>(
    domain: Domain,
    regs: Registers<UuidKey, R>,
    tombs: Tombstones<UuidKey>,
    sink: &delta_sink::Client,
) -> Result<bool, capnp::Error>
where
    R: serde::Serialize,
{
    let regs_wire = encode_registers(regs)?;
    let tombs_wire = encode_tombstones(tombs);

    if regs_wire.is_empty() && tombs_wire.is_empty() {
        return Ok(false);
    }

    let mut regs_slice = regs_wire.as_slice();
    let mut tombs_slice = tombs_wire.as_slice();

    while !regs_slice.is_empty() || !tombs_slice.is_empty() {
        let (regs_chunk, rest_regs) = if regs_slice.len() > DELTA_CHUNK_MAX {
            regs_slice.split_at(DELTA_CHUNK_MAX)
        } else {
            (regs_slice, &[][..])
        };

        let remaining = DELTA_CHUNK_MAX.saturating_sub(regs_chunk.len());
        let (tombs_chunk, rest_tombs) = if tombs_slice.len() > remaining {
            tombs_slice.split_at(remaining)
        } else {
            (tombs_slice, &[][..])
        };

        let mut req = sink.push_chunk_request();
        {
            let mut chunk_builder = req.get().init_chunk();
            chunk_builder.set_domain(domain);

            let mut regs_builder = chunk_builder.reborrow().init_regs(regs_chunk.len() as u32);
            for (idx, (key, reg)) in regs_chunk.iter().enumerate() {
                let mut entry = regs_builder.reborrow().get(idx as u32);
                entry.set_key(key);
                entry.set_reg(reg);
            }

            let mut tombs_builder = chunk_builder
                .reborrow()
                .init_tombs(tombs_chunk.len() as u32);
            for (idx, (key, ts)) in tombs_chunk.iter().enumerate() {
                let mut entry = tombs_builder.reborrow().get(idx as u32);
                entry.set_key(key);
                entry.set_ts(*ts);
            }
        }
        req.send().await?;

        regs_slice = rest_regs;
        tombs_slice = rest_tombs;
    }

    Ok(true)
}
