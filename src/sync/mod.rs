use crate::cluster::{ClusterViewId, ClusterViewState};
use crate::store::cluster_view_store::ClusterViewDomainStore;
use crate::store::network_store::{NetworkAttachmentStore, NetworkPeerStore, NetworkSpecStore};
use crate::store::peer_store::PeersStore;
use crate::store::secret_store::SecretStore;
use crate::store::service_store::ServiceStore;
use crate::store::task_store::TaskStore;
use crate::store::volume_store::{VolumeNodeStore, VolumeSpecStore};
use crate::sync::ranges::{capnp_fill_ranges, page_ranges_from_capnp};
use crdt_store::mst_store::{Registers, Tombstones};
use crdt_store::uuid_key::UuidKey;
use protocol::sync::{Domain, delta_sink, sync};
use std::rc::Rc;
use tracing::{debug, trace};

pub mod delta;
pub mod ranges;

type EncodedRegister = (Vec<u8>, Vec<u8>);
type EncodedRegisters = Vec<EncodedRegister>;
type EncodedTombstone = (Vec<u8>, u64);
type EncodedTombstones = Vec<EncodedTombstone>;

const ALL_DOMAINS: [Domain; 10] = [
    Domain::Peers,
    Domain::Tasks,
    Domain::Services,
    Domain::Secrets,
    Domain::Networks,
    Domain::NetworkPeers,
    Domain::NetworkAttachments,
    Domain::ClusterViews,
    Domain::Volumes,
    Domain::VolumeNodes,
];

/// Number of replicated domains exposed through view-scoped sync RPCs.
pub const VIEW_SCOPED_DOMAIN_COUNT: usize = ALL_DOMAINS.len();

// Default chunk size used when streaming delta from server to client.
pub const DEFAULT_DELTA_CHUNK_MAX: usize = 1024;

/// Reads the max register/tombstone entries per streamed delta chunk.
///
/// Keeping this configurable helps scale anti-entropy payload sizing without rebuilding.
fn delta_chunk_max() -> usize {
    std::env::var("MANTISSA_SYNC_DELTA_CHUNK_MAX")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DELTA_CHUNK_MAX)
}

#[derive(Clone)]
pub struct SyncService {
    cluster_view: ClusterViewState,
    peers: PeersStore,
    tasks: TaskStore,
    services: ServiceStore,
    secrets: SecretStore,
    networks: NetworkSpecStore,
    network_peers: NetworkPeerStore,
    network_attachments: NetworkAttachmentStore,
    cluster_views: ClusterViewDomainStore,
    volumes: VolumeSpecStore,
    volume_nodes: VolumeNodeStore,
}

#[derive(Clone)]
pub struct SyncStores {
    pub peers: PeersStore,
    pub tasks: TaskStore,
    pub services: ServiceStore,
    pub secrets: SecretStore,
    pub networks: NetworkSpecStore,
    pub network_peers: NetworkPeerStore,
    pub network_attachments: NetworkAttachmentStore,
    pub cluster_views: ClusterViewDomainStore,
    pub volumes: VolumeSpecStore,
    pub volume_nodes: VolumeNodeStore,
}

impl SyncService {
    /// Builds a sync service bound to the provided cluster view state and domain stores.
    pub fn new(cluster_view: ClusterViewState, stores: SyncStores) -> Self {
        let SyncStores {
            peers,
            tasks,
            services,
            secrets,
            networks,
            network_peers,
            network_attachments,
            cluster_views,
            volumes,
            volume_nodes,
        } = stores;
        Self {
            cluster_view,
            peers,
            tasks,
            services,
            secrets,
            networks,
            network_peers,
            network_attachments,
            cluster_views,
            volumes,
            volume_nodes,
        }
    }

    /// Returns an error when a peer requests legacy non-view-scoped sync methods.
    fn legacy_sync_method_error(method: &str) -> capnp::Error {
        capnp::Error::failed(format!(
            "{method} is no longer supported; use the view-scoped sync RPCs"
        ))
    }

    /// Validates that the requested view matches this node's active control-plane view.
    fn require_active_view(&self, requested: ClusterViewId) -> Result<ClusterViewId, capnp::Error> {
        let active = self.cluster_view.active_view();
        if requested != active {
            return Err(capnp::Error::failed(format!(
                "cluster view mismatch: requested {requested}, active {active}"
            )));
        }
        Ok(active)
    }

    /// Resolves a sync domain to its backing replicated store handle.
    fn domain_store(&self, domain: Domain) -> DomainStoreRef<'_> {
        match domain {
            Domain::Peers => DomainStoreRef::Peers(&self.peers),
            Domain::Tasks => DomainStoreRef::Tasks(&self.tasks),
            Domain::Services => DomainStoreRef::Services(&self.services),
            Domain::Secrets => DomainStoreRef::Secrets(&self.secrets),
            Domain::Networks => DomainStoreRef::Networks(&self.networks),
            Domain::NetworkPeers => DomainStoreRef::NetworkPeers(&self.network_peers),
            Domain::NetworkAttachments => {
                DomainStoreRef::NetworkAttachments(&self.network_attachments)
            }
            Domain::ClusterViews => DomainStoreRef::ClusterViews(&self.cluster_views),
            Domain::Volumes => DomainStoreRef::Volumes(&self.volumes),
            Domain::VolumeNodes => DomainStoreRef::VolumeNodes(&self.volume_nodes),
        }
    }
}

/// Typed reference to one sync domain's replicated store and diagnostics labels.
enum DomainStoreRef<'a> {
    Peers(&'a PeersStore),
    Tasks(&'a TaskStore),
    Services(&'a ServiceStore),
    Secrets(&'a SecretStore),
    Networks(&'a NetworkSpecStore),
    NetworkPeers(&'a NetworkPeerStore),
    NetworkAttachments(&'a NetworkAttachmentStore),
    ClusterViews(&'a ClusterViewDomainStore),
    Volumes(&'a VolumeSpecStore),
    VolumeNodes(&'a VolumeNodeStore),
}

macro_rules! with_domain_store {
    ($domain_store:expr, |$store:ident| $body:block) => {
        match $domain_store {
            DomainStoreRef::Peers($store) => $body,
            DomainStoreRef::Tasks($store) => $body,
            DomainStoreRef::Services($store) => $body,
            DomainStoreRef::Secrets($store) => $body,
            DomainStoreRef::Networks($store) => $body,
            DomainStoreRef::NetworkPeers($store) => $body,
            DomainStoreRef::NetworkAttachments($store) => $body,
            DomainStoreRef::ClusterViews($store) => $body,
            DomainStoreRef::Volumes($store) => $body,
            DomainStoreRef::VolumeNodes($store) => $body,
        }
    };
}

impl DomainStoreRef<'_> {
    /// Returns the protocol domain represented by this store reference.
    fn domain(&self) -> Domain {
        match self {
            Self::Peers(_) => Domain::Peers,
            Self::Tasks(_) => Domain::Tasks,
            Self::Services(_) => Domain::Services,
            Self::Secrets(_) => Domain::Secrets,
            Self::Networks(_) => Domain::Networks,
            Self::NetworkPeers(_) => Domain::NetworkPeers,
            Self::NetworkAttachments(_) => Domain::NetworkAttachments,
            Self::ClusterViews(_) => Domain::ClusterViews,
            Self::Volumes(_) => Domain::Volumes,
            Self::VolumeNodes(_) => Domain::VolumeNodes,
        }
    }

    /// Returns the human-readable label used in sync debug logs.
    fn log_label(&self) -> &'static str {
        match self {
            Self::Peers(_) => "peers",
            Self::Tasks(_) => "tasks",
            Self::Services(_) => "services",
            Self::Secrets(_) => "secrets",
            Self::Networks(_) => "networks",
            Self::NetworkPeers(_) => "network peers",
            Self::NetworkAttachments(_) => "network attachments",
            Self::ClusterViews(_) => "cluster views",
            Self::Volumes(_) => "volumes",
            Self::VolumeNodes(_) => "volume nodes",
        }
    }

    /// Returns the label used when dumping MST state before range responses.
    fn ranges_dump_label(&self) -> &'static str {
        match self {
            Self::Peers(_) => "server.before.get_ranges",
            Self::Tasks(_) => "server.before.get_ranges.tasks",
            Self::Services(_) => "server.before.get_ranges.services",
            Self::Secrets(_) => "server.before.get_ranges.secrets",
            Self::Networks(_) => "server.before.get_ranges.networks",
            Self::NetworkPeers(_) => "server.before.get_ranges.network_peers",
            Self::NetworkAttachments(_) => "server.before.get_ranges.network_attachments",
            Self::ClusterViews(_) => "server.before.get_ranges.cluster_views",
            Self::Volumes(_) => "server.before.get_ranges.volumes",
            Self::VolumeNodes(_) => "server.before.get_ranges.volume_nodes",
        }
    }

    /// Returns the label used when dumping MST state before delta responses.
    fn delta_dump_label(&self) -> &'static str {
        match self {
            Self::Peers(_) => "server.before.open_delta",
            Self::Tasks(_) => "server.before.open_delta.tasks",
            Self::Services(_) => "server.before.open_delta.services",
            Self::Secrets(_) => "server.before.open_delta.secrets",
            Self::Networks(_) => "server.before.open_delta.networks",
            Self::NetworkPeers(_) => "server.before.open_delta.network_peers",
            Self::NetworkAttachments(_) => "server.before.open_delta.network_attachments",
            Self::ClusterViews(_) => "server.before.open_delta.cluster_views",
            Self::Volumes(_) => "server.before.open_delta.volumes",
            Self::VolumeNodes(_) => "server.before.open_delta.volume_nodes",
        }
    }

    /// Reads the current MST root hash for this domain.
    async fn root_hex(&self) -> String {
        with_domain_store!(self, |store| { store.root_hex().await })
    }

    /// Produces digest ranges for anti-entropy while emitting domain diagnostics.
    async fn page_range_summary(&self) -> Result<Vec<crdt_store::PageDigestRange>, capnp::Error> {
        debug!("getRangesForView: received ({})", self.log_label());
        let dump_label = self.ranges_dump_label();
        with_domain_store!(self, |store| {
            store.debug_dump_root(dump_label).await;
            store.debug_dump_ranges(dump_label, 5).await;
            store
                .page_range_summary()
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))
        })
    }

    /// Exports and encodes delta payloads for the requested ranges.
    fn export_delta_encoded(
        &self,
        want_ranges: &[crdt_store::PageDigestRange],
    ) -> Result<(EncodedRegisters, EncodedTombstones), capnp::Error> {
        with_domain_store!(self, |store| {
            let (regs, tombs) = store
                .export_page_ranges_delta(want_ranges)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            Ok((encode_registers(regs)?, encode_tombstones(tombs)))
        })
    }

    /// Dumps domain-specific diagnostics for an incoming delta request.
    async fn debug_dump_delta_state(&self) {
        debug!(
            target: "delta",
            "open_delta_for_view: received ({})",
            self.log_label()
        );
        let dump_label = self.delta_dump_label();
        with_domain_store!(self, |store| {
            store.debug_dump_root(dump_label).await;
            store.debug_dump_ranges(dump_label, 5).await;
        })
    }
}

impl sync::Server for SyncService {
    async fn get_roots(
        self: Rc<Self>,
        _params: sync::GetRootsParams,
        _results: sync::GetRootsResults,
    ) -> Result<(), capnp::Error> {
        Err(Self::legacy_sync_method_error("getRoots"))
    }

    async fn get_ranges(
        self: Rc<Self>,
        _params: sync::GetRangesParams,
        _results: sync::GetRangesResults,
    ) -> Result<(), capnp::Error> {
        Err(Self::legacy_sync_method_error("getRanges"))
    }

    async fn open_delta(
        self: Rc<Self>,
        _params: sync::OpenDeltaParams,
        _results: sync::OpenDeltaResults,
    ) -> Result<(), capnp::Error> {
        Err(Self::legacy_sync_method_error("openDelta"))
    }

    /// Returns domain roots scoped to the caller-provided cluster view.
    async fn get_roots_for_view(
        self: Rc<Self>,
        params: sync::GetRootsForViewParams,
        mut results: sync::GetRootsForViewResults,
    ) -> Result<(), capnp::Error> {
        let req = params.get()?.get_req()?;
        let requested_view =
            ClusterViewId::from_capnp(req.get_view()?).map_err(capnp::Error::failed)?;
        let active_view = self.require_active_view(requested_view)?;
        trace!(
            target: "sync",
            requested_view = %requested_view,
            active_view = %active_view,
            "get_roots_for_view request received"
        );

        let mut list = results.get().init_roots(VIEW_SCOPED_DOMAIN_COUNT as u32);
        for (idx, domain) in ALL_DOMAINS.iter().copied().enumerate() {
            let root_hex = self.domain_store(domain).root_hex().await;
            let mut entry = list.reborrow().get(idx as u32);
            entry.set_domain(domain);
            entry.set_root_hex(&root_hex);
            active_view.write_capnp(entry.reborrow().init_view());
        }

        Ok(())
    }

    /// Returns range summaries scoped to the caller-provided cluster view.
    async fn get_ranges_for_view(
        self: Rc<Self>,
        params: sync::GetRangesForViewParams,
        mut results: sync::GetRangesForViewResults,
    ) -> Result<(), capnp::Error> {
        let req = params.get()?.get_req()?;
        let requested_view =
            ClusterViewId::from_capnp(req.get_view()?).map_err(capnp::Error::failed)?;
        let active_view = self.require_active_view(requested_view)?;
        trace!(
            target: "sync",
            requested_view = %requested_view,
            active_view = %active_view,
            "get_ranges_for_view request received"
        );

        let requested_domains: Vec<Domain> = {
            let domains_reader = req.get_domains()?;
            if domains_reader.is_empty() {
                ALL_DOMAINS.to_vec()
            } else {
                let mut out = Vec::with_capacity(domains_reader.len() as usize);
                for domain in domains_reader.iter() {
                    out.push(domain?);
                }
                out
            }
        };

        let mut list = results.get().init_ranges(requested_domains.len() as u32);
        for (idx, domain) in requested_domains.iter().copied().enumerate() {
            let store = self.domain_store(domain);
            let ranges = store.page_range_summary().await?;
            let mut entry = list.reborrow().get(idx as u32);
            entry.set_domain(store.domain());
            let summary = entry.reborrow().init_summary();
            capnp_fill_ranges(&ranges, summary)?;
            active_view.write_capnp(entry.reborrow().init_view());
        }

        Ok(())
    }

    /// Streams delta chunks scoped to the caller-provided cluster view.
    async fn open_delta_for_view(
        self: Rc<Self>,
        params: sync::OpenDeltaForViewParams,
        _results: sync::OpenDeltaForViewResults,
    ) -> Result<(), capnp::Error> {
        let req = params.get()?.get_req()?;
        let requested_view =
            ClusterViewId::from_capnp(req.get_view()?).map_err(capnp::Error::failed)?;
        let active_view = self.require_active_view(requested_view)?;
        debug!(
            target: "delta",
            requested_view = %requested_view,
            active_view = %active_view,
            "open_delta_for_view request received"
        );

        let wants_reader = req.get_wants()?;
        let sink = req.get_sink()?;

        if wants_reader.is_empty() {
            sink.end_request().send().promise.await?;
            return Ok(());
        }

        let mut sent_chunks = false;

        for idx in 0..wants_reader.len() {
            let want = wants_reader.get(idx);
            let want_view =
                ClusterViewId::from_capnp(want.get_view()?).map_err(capnp::Error::failed)?;
            if want_view != active_view {
                return Err(capnp::Error::failed(format!(
                    "domain want view mismatch: expected {active_view}, got {want_view}"
                )));
            }

            let domain = want
                .get_domain()
                .map_err(|_| capnp::Error::failed("unknown sync domain".into()))?;
            let want_ranges = page_ranges_from_capnp(want.get_want()?)?;
            if want_ranges.is_empty() {
                continue;
            }

            let store = self.domain_store(domain);
            store.debug_dump_delta_state().await;
            let (regs, tombs) = store.export_delta_encoded(&want_ranges)?;
            if send_chunks(domain, regs, tombs, active_view, &sink).await? {
                sent_chunks = true;
            }
        }

        if !sent_chunks {
            debug!(target: "delta", "open_delta_for_view: no chunks emitted");
        }

        sink.end_request().send().promise.await?;
        Ok(())
    }
}

fn encode_registers<R>(regs: Registers<UuidKey, R>) -> Result<EncodedRegisters, capnp::Error>
where
    R: serde::Serialize,
{
    let mut out = EncodedRegisters::with_capacity(regs.len());
    for (k, r) in regs {
        let key_bytes = k.as_ref().to_vec();
        let reg_bytes = bincode::serialize(&r).map_err(|e| capnp::Error::failed(e.to_string()))?;
        out.push((key_bytes, reg_bytes));
    }
    Ok(out)
}

fn encode_tombstones(tombs: Tombstones<UuidKey>) -> EncodedTombstones {
    tombs
        .into_iter()
        .map(|(k, ts)| (k.as_ref().to_vec(), ts))
        .collect()
}

async fn send_chunks(
    domain: Domain,
    regs_wire: EncodedRegisters,
    tombs_wire: EncodedTombstones,
    cluster_view: ClusterViewId,
    sink: &delta_sink::Client,
) -> Result<bool, capnp::Error> {
    let chunk_max = delta_chunk_max();

    if regs_wire.is_empty() && tombs_wire.is_empty() {
        return Ok(false);
    }

    let mut regs_slice = regs_wire.as_slice();
    let mut tombs_slice = tombs_wire.as_slice();

    while !regs_slice.is_empty() || !tombs_slice.is_empty() {
        let (regs_chunk, rest_regs) = if regs_slice.len() > chunk_max {
            regs_slice.split_at(chunk_max)
        } else {
            (regs_slice, &[][..])
        };

        let remaining = chunk_max.saturating_sub(regs_chunk.len());
        let (tombs_chunk, rest_tombs) = if tombs_slice.len() > remaining {
            tombs_slice.split_at(remaining)
        } else {
            (tombs_slice, &[][..])
        };

        let regs_payload_bytes: usize = regs_chunk
            .iter()
            .map(|(key, reg)| key.len().saturating_add(reg.len()))
            .sum();
        let tombs_payload_bytes: usize = tombs_chunk
            .iter()
            .map(|(key, _)| key.len().saturating_add(std::mem::size_of::<u64>()))
            .sum();
        debug!(
            target: "delta",
            ?domain,
            regs = regs_chunk.len(),
            tombs = tombs_chunk.len(),
            chunk_max,
            approx_payload_bytes = regs_payload_bytes.saturating_add(tombs_payload_bytes),
            "sending delta chunk"
        );

        let mut req = sink.push_chunk_request();
        {
            let mut chunk_builder = req.get().init_chunk();
            chunk_builder.set_domain(domain);
            cluster_view.write_capnp(chunk_builder.reborrow().init_view());

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
