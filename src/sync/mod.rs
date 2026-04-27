//! Server side of Mantissa's view-scoped anti-entropy protocol.
//!
//! The sync RPC intentionally runs in three phases so large clusters do not ship full
//! snapshots on every reconciliation pass:
//! 1. `get_roots_for_view` compares cheap MST roots per replicated domain.
//! 2. `get_ranges_for_view` narrows mismatches down to page digest ranges.
//! 3. `open_delta_for_view` streams only the missing register/tombstone fragments.
//!
//! All sync traffic is scoped to an explicit `ClusterViewId` so anti-entropy stays
//! inside one control-plane lineage.

use crate::cluster::{ClusterViewId, ClusterViewState, RootSchemaState};
use crate::store::agent_store::AgentStore;
use crate::store::cluster_view_store::ClusterViewDomainStore;
use crate::store::job_store::JobStore;
use crate::store::network_store::{NetworkAttachmentStore, NetworkPeerStore, NetworkSpecStore};
use crate::store::peer_store::PeersStore;
use crate::store::scheduler_digest_store::SchedulerDigestStore;
use crate::store::secret_store::SecretStore;
use crate::store::service_store::ServiceStore;
use crate::store::volume_store::{VolumeNodeStore, VolumeSpecStore};
use crate::store::workload_store::WorkloadStore;
use crate::sync::ranges::{capnp_fill_ranges, page_ranges_from_capnp};
use crdt_store::mst_store::{TombstonePruneFrontiers, Tombstones};
use crdt_store::uuid_key::UuidKey;
use protocol::sync::{Domain, delta_sink, sync};
use std::rc::Rc;
use tracing::{debug, trace};

pub mod delta;
pub mod gc_progress;
pub mod ranges;

pub use crdt_store::gc::GcBarrier;
pub use delta::{SyncRunner, SyncTraceContext};
pub use gc_progress::SyncGcProgress;

type EncodedRegister = (Vec<u8>, Vec<u8>);
type EncodedRegisters = Vec<EncodedRegister>;
type EncodedTombstone = (Vec<u8>, u64, Vec<u8>);
type EncodedTombstones = Vec<EncodedTombstone>;

/// Canonical full-sync domain set shared by both client and server sync paths.
///
/// Both client and server treat an empty domain list as "all domains in this order".
pub const ALL_DOMAINS: [Domain; 13] = [
    Domain::Peers,
    Domain::Workloads,
    Domain::Services,
    Domain::Jobs,
    Domain::Agents,
    Domain::Secrets,
    Domain::Networks,
    Domain::NetworkPeers,
    Domain::NetworkAttachments,
    Domain::ClusterViews,
    Domain::Volumes,
    Domain::VolumeNodes,
    Domain::SchedulerDigests,
];

/// Number of replicated domains exposed through view-scoped sync RPCs.
pub const VIEW_SCOPED_DOMAIN_COUNT: usize = ALL_DOMAINS.len();
/// Default max entries per streamed delta chunk.
pub const DEFAULT_DELTA_CHUNK_MAX: usize = 2048;
/// Default approximate payload target per streamed delta chunk.
pub const DEFAULT_DELTA_CHUNK_TARGET_BYTES: usize = 128 * 1024;

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

/// Reads the approximate payload target per streamed delta chunk.
///
/// Chunk sizing stays approximate because the sender already has the encoded entries on hand
/// and only needs a stable batching signal, not byte-perfect Cap'n Proto accounting.
fn delta_chunk_target_bytes() -> usize {
    std::env::var("MANTISSA_SYNC_DELTA_CHUNK_TARGET_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DELTA_CHUNK_TARGET_BYTES)
}

#[derive(Clone)]
/// Cap'n Proto server that exposes all replicated stores through one sync interface.
pub struct SyncService {
    cluster_view: ClusterViewState,
    root_schema: RootSchemaState,
    stores: SyncStores,
}

#[derive(Clone)]
/// Bundle of replicated stores served through `SyncService`.
///
/// Keeping the stores grouped here lets topology bootstrap and tests construct the sync
/// surface without threading ten separate arguments through every call site.
pub struct SyncStores {
    pub peers: PeersStore,
    pub workloads: WorkloadStore,
    pub jobs: JobStore,
    pub agents: AgentStore,
    pub services: ServiceStore,
    pub secrets: SecretStore,
    pub networks: NetworkSpecStore,
    pub network_peers: NetworkPeerStore,
    pub network_attachments: NetworkAttachmentStore,
    pub cluster_views: ClusterViewDomainStore,
    pub volumes: VolumeSpecStore,
    pub volume_nodes: VolumeNodeStore,
    pub scheduler_digests: SchedulerDigestStore,
}

impl SyncService {
    /// Builds a sync service bound to the provided cluster view state and domain stores.
    pub fn new(
        cluster_view: ClusterViewState,
        root_schema: RootSchemaState,
        stores: SyncStores,
    ) -> Self {
        Self {
            cluster_view,
            root_schema,
            stores,
        }
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

    /// Validates that the requested semantic root schema is supported by this binary.
    fn require_supported_root_schema_version(&self, requested: u32) -> Result<u32, capnp::Error> {
        if !self.root_schema.supports(requested) {
            return Err(capnp::Error::failed(format!(
                "root schema version {requested} is unsupported; local binary supports up to {}",
                self.root_schema.supported_version()
            )));
        }
        Ok(requested)
    }

    /// Resolves a sync domain to its backing replicated store handle.
    fn domain_store(&self, domain: Domain) -> DomainStoreRef<'_> {
        match domain {
            Domain::Peers => DomainStoreRef::Peers(&self.stores.peers),
            Domain::Workloads => DomainStoreRef::Workloads(&self.stores.workloads),
            Domain::Services => DomainStoreRef::Services(&self.stores.services),
            Domain::Jobs => DomainStoreRef::Jobs(&self.stores.jobs),
            Domain::Agents => DomainStoreRef::Agents(&self.stores.agents),
            Domain::Secrets => DomainStoreRef::Secrets(&self.stores.secrets),
            Domain::Networks => DomainStoreRef::Networks(&self.stores.networks),
            Domain::NetworkPeers => DomainStoreRef::NetworkPeers(&self.stores.network_peers),
            Domain::NetworkAttachments => {
                DomainStoreRef::NetworkAttachments(&self.stores.network_attachments)
            }
            Domain::ClusterViews => DomainStoreRef::ClusterViews(&self.stores.cluster_views),
            Domain::Volumes => DomainStoreRef::Volumes(&self.stores.volumes),
            Domain::VolumeNodes => DomainStoreRef::VolumeNodes(&self.stores.volume_nodes),
            Domain::SchedulerDigests => {
                DomainStoreRef::SchedulerDigests(&self.stores.scheduler_digests)
            }
        }
    }
}

/// Typed reference to one sync domain's replicated store and diagnostics labels.
enum DomainStoreRef<'a> {
    Peers(&'a PeersStore),
    Workloads(&'a WorkloadStore),
    Services(&'a ServiceStore),
    Jobs(&'a JobStore),
    Agents(&'a AgentStore),
    Secrets(&'a SecretStore),
    Networks(&'a NetworkSpecStore),
    NetworkPeers(&'a NetworkPeerStore),
    NetworkAttachments(&'a NetworkAttachmentStore),
    ClusterViews(&'a ClusterViewDomainStore),
    Volumes(&'a VolumeSpecStore),
    VolumeNodes(&'a VolumeNodeStore),
    SchedulerDigests(&'a SchedulerDigestStore),
}

macro_rules! with_domain_store {
    ($domain_store:expr, |$store:ident| $body:block) => {
        match $domain_store {
            DomainStoreRef::Peers($store) => $body,
            DomainStoreRef::Workloads($store) => $body,
            DomainStoreRef::Services($store) => $body,
            DomainStoreRef::Jobs($store) => $body,
            DomainStoreRef::Agents($store) => $body,
            DomainStoreRef::Secrets($store) => $body,
            DomainStoreRef::Networks($store) => $body,
            DomainStoreRef::NetworkPeers($store) => $body,
            DomainStoreRef::NetworkAttachments($store) => $body,
            DomainStoreRef::ClusterViews($store) => $body,
            DomainStoreRef::Volumes($store) => $body,
            DomainStoreRef::VolumeNodes($store) => $body,
            DomainStoreRef::SchedulerDigests($store) => $body,
        }
    };
}

impl DomainStoreRef<'_> {
    /// Returns the protocol domain represented by this store reference.
    fn domain(&self) -> Domain {
        match self {
            Self::Peers(_) => Domain::Peers,
            Self::Workloads(_) => Domain::Workloads,
            Self::Services(_) => Domain::Services,
            Self::Jobs(_) => Domain::Jobs,
            Self::Agents(_) => Domain::Agents,
            Self::Secrets(_) => Domain::Secrets,
            Self::Networks(_) => Domain::Networks,
            Self::NetworkPeers(_) => Domain::NetworkPeers,
            Self::NetworkAttachments(_) => Domain::NetworkAttachments,
            Self::ClusterViews(_) => Domain::ClusterViews,
            Self::Volumes(_) => Domain::Volumes,
            Self::VolumeNodes(_) => Domain::VolumeNodes,
            Self::SchedulerDigests(_) => Domain::SchedulerDigests,
        }
    }

    /// Builds the diagnostic label used when dumping MST state for one sync phase.
    fn dump_label(&self, prefix: &str) -> String {
        format!("{prefix}.{}", domain_debug_label(self.domain()))
    }

    /// Reads the current MST root digest for this domain at one semantic version.
    async fn root_digest(&self, root_schema_version: u32) -> Result<[u8; 16], capnp::Error> {
        with_domain_store!(self, |store| {
            store
                .root_digest_at_version(root_schema_version)
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))
        })
    }

    /// Loads durable tombstone prune frontiers advertised during the roots phase.
    fn tombstone_prune_frontiers(&self) -> Result<TombstonePruneFrontiers, capnp::Error> {
        with_domain_store!(self, |store| {
            store
                .load_tombstone_prune_frontiers()
                .map_err(|e| capnp::Error::failed(e.to_string()))
        })
    }

    /// Produces digest ranges for anti-entropy while emitting domain diagnostics.
    async fn page_range_summary(
        &self,
        root_schema_version: u32,
    ) -> Result<Vec<crdt_store::PageDigestRange>, capnp::Error> {
        let domain = self.domain();
        debug!(
            "getRangesForView: received ({})",
            domain_debug_label(domain)
        );
        let dump_label = self.dump_label("server.before.get_ranges");
        with_domain_store!(self, |store| {
            store.debug_dump_root(&dump_label).await;
            store.debug_dump_ranges(&dump_label, 5).await;
            store
                .page_range_summary_at_version(root_schema_version)
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
            let regs = store
                .encode_register_delta(regs)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            Ok((regs, encode_tombstones(tombs)))
        })
    }

    /// Dumps domain-specific diagnostics for an incoming delta request.
    async fn debug_dump_delta_state(&self) {
        let domain = self.domain();
        debug!(
            target: "delta",
            "open_delta_for_view: received ({})",
            domain_debug_label(domain)
        );
        let dump_label = self.dump_label("server.before.open_delta");
        with_domain_store!(self, |store| {
            store.debug_dump_root(&dump_label).await;
            store.debug_dump_ranges(&dump_label, 5).await;
        })
    }
}

impl sync::Server for SyncService {
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
        let requested_root_schema_version =
            self.require_supported_root_schema_version(req.get_root_schema_version())?;
        trace!(
            target: "sync",
            requested_view = %requested_view,
            active_view = %active_view,
            root_schema_version = requested_root_schema_version,
            "get_roots_for_view request received"
        );

        let mut list = results.get().init_roots(VIEW_SCOPED_DOMAIN_COUNT as u32);
        for (idx, domain) in ALL_DOMAINS.iter().copied().enumerate() {
            let store = self.domain_store(domain);
            let root_digest = store.root_digest(requested_root_schema_version).await?;
            let frontiers = store.tombstone_prune_frontiers()?;
            let mut entry = list.reborrow().get(idx as u32);
            entry.set_domain(domain);
            entry.set_root_digest(&root_digest);
            active_view.write_capnp(entry.reborrow().init_view());
            entry.set_root_schema_version(requested_root_schema_version);
            let mut frontier_list = entry
                .reborrow()
                .init_tombstone_prune_frontiers(frontiers.len() as u32);
            for (frontier_idx, (origin_actor, sequence)) in frontiers.iter().enumerate() {
                let mut frontier = frontier_list.reborrow().get(frontier_idx as u32);
                frontier.set_origin_actor(origin_actor);
                frontier.set_sequence(*sequence);
            }
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
        let requested_root_schema_version =
            self.require_supported_root_schema_version(req.get_root_schema_version())?;
        trace!(
            target: "sync",
            requested_view = %requested_view,
            active_view = %active_view,
            root_schema_version = requested_root_schema_version,
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
            let ranges = store
                .page_range_summary(requested_root_schema_version)
                .await?;
            let mut entry = list.reborrow().get(idx as u32);
            entry.set_domain(store.domain());
            let summary = entry.reborrow().init_summary();
            capnp_fill_ranges(&ranges, summary)?;
            active_view.write_capnp(entry.reborrow().init_view());
            entry.set_root_schema_version(requested_root_schema_version);
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
        let requested_root_schema_version =
            self.require_supported_root_schema_version(req.get_root_schema_version())?;
        debug!(
            target: "delta",
            requested_view = %requested_view,
            active_view = %active_view,
            root_schema_version = requested_root_schema_version,
            "open_delta_for_view request received"
        );

        let wants_reader = req.get_wants()?;
        let sink = req.get_sink()?;

        // The caller already proved convergence after the roots/ranges phases.
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
            if want.get_root_schema_version() != requested_root_schema_version {
                return Err(capnp::Error::failed(format!(
                    "domain want root schema mismatch: expected {requested_root_schema_version}, got {}",
                    want.get_root_schema_version()
                )));
            }

            let domain = want
                .get_domain()
                .map_err(|_| capnp::Error::failed("unknown sync domain".into()))?;
            let want_ranges = page_ranges_from_capnp(want.get_want()?)?;
            if want_ranges.is_empty() {
                continue;
            }

            // Export only the pages the caller proved it is missing for this domain.
            let store = self.domain_store(domain);
            store.debug_dump_delta_state().await;
            let (regs, tombs) = store.export_delta_encoded(&want_ranges)?;
            if send_chunks(
                domain,
                regs,
                tombs,
                active_view,
                requested_root_schema_version,
                &sink,
            )
            .await?
            {
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

/// Converts tombstone rows into the compact wire format used by `DeltaChunk.tombs`.
fn encode_tombstones(tombs: Tombstones<UuidKey>) -> EncodedTombstones {
    tombs
        .into_iter()
        .map(|(k, tombstone)| {
            (
                k.as_ref().to_vec(),
                tombstone.sequence,
                tombstone.origin_actor,
            )
        })
        .collect()
}

/// Returns the approximate payload bytes for one encoded register entry.
///
/// The estimate intentionally ignores Cap'n Proto framing overhead because chunk planning only
/// needs a stable relative size signal to batch enough plaintext per request.
fn encoded_register_payload_bytes((key, reg): &EncodedRegister) -> usize {
    key.len().saturating_add(reg.len())
}

/// Returns the approximate payload bytes for one encoded tombstone entry.
///
/// The timestamp is fixed-width on the wire, so the estimate only needs the key length plus
/// the replicated tombstone scalar payload.
fn encoded_tombstone_payload_bytes((key, _ts, origin_actor): &EncodedTombstone) -> usize {
    key.len()
        .saturating_add(std::mem::size_of::<u64>())
        .saturating_add(origin_actor.len())
}

/// Selects the next delta chunk prefix using both entry and approximate payload limits.
///
/// Registers stay ahead of tombstones to preserve the current stream ordering, while the byte
/// target pushes each outbound RPC toward a fuller plaintext payload before encryption. The
/// planner always admits at least one entry so a single large row cannot stall replication.
fn take_delta_chunk_prefix(
    regs: &[EncodedRegister],
    tombs: &[EncodedTombstone],
    max_entries: usize,
    target_bytes: usize,
) -> (usize, usize, usize) {
    let mut regs_len = 0usize;
    let mut tombs_len = 0usize;
    let mut approx_payload_bytes = 0usize;

    while regs_len + tombs_len < max_entries && regs_len < regs.len() {
        let entry_bytes = encoded_register_payload_bytes(&regs[regs_len]);
        if approx_payload_bytes > 0
            && approx_payload_bytes.saturating_add(entry_bytes) > target_bytes
        {
            break;
        }
        approx_payload_bytes = approx_payload_bytes.saturating_add(entry_bytes);
        regs_len += 1;
    }

    while regs_len + tombs_len < max_entries && tombs_len < tombs.len() {
        let entry_bytes = encoded_tombstone_payload_bytes(&tombs[tombs_len]);
        if approx_payload_bytes > 0
            && approx_payload_bytes.saturating_add(entry_bytes) > target_bytes
        {
            break;
        }
        approx_payload_bytes = approx_payload_bytes.saturating_add(entry_bytes);
        tombs_len += 1;
    }

    if regs_len == 0 && tombs_len == 0 {
        if let Some(first_reg) = regs.first() {
            return (1, 0, encoded_register_payload_bytes(first_reg));
        }
        if let Some(first_tomb) = tombs.first() {
            return (0, 1, encoded_tombstone_payload_bytes(first_tomb));
        }
    }

    (regs_len, tombs_len, approx_payload_bytes)
}

/// Streams one domain delta to the caller in bounded chunks.
///
/// Chunking uses both entry and approximate payload limits so the sender can ship fewer,
/// fatter requests without needing byte-perfect Cap'n Proto sizing.
async fn send_chunks(
    domain: Domain,
    regs_wire: EncodedRegisters,
    tombs_wire: EncodedTombstones,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    sink: &delta_sink::Client,
) -> Result<bool, capnp::Error> {
    let chunk_max = delta_chunk_max();
    let chunk_target_bytes = delta_chunk_target_bytes();

    if regs_wire.is_empty() && tombs_wire.is_empty() {
        return Ok(false);
    }

    let mut regs_slice = regs_wire.as_slice();
    let mut tombs_slice = tombs_wire.as_slice();

    while !regs_slice.is_empty() || !tombs_slice.is_empty() {
        let (regs_len, tombs_len, approx_payload_bytes) =
            take_delta_chunk_prefix(regs_slice, tombs_slice, chunk_max, chunk_target_bytes);
        let (regs_chunk, rest_regs) = regs_slice.split_at(regs_len);
        let (tombs_chunk, rest_tombs) = tombs_slice.split_at(tombs_len);
        debug!(
            target: "delta",
            ?domain,
            regs = regs_chunk.len(),
            tombs = tombs_chunk.len(),
            chunk_max,
            chunk_target_bytes,
            approx_payload_bytes,
            "sending delta chunk"
        );

        let mut req = sink.push_chunk_request();
        {
            let mut chunk_builder = req.get().init_chunk();
            chunk_builder.set_domain(domain);
            cluster_view.write_capnp(chunk_builder.reborrow().init_view());
            chunk_builder.set_root_schema_version(root_schema_version);

            let mut regs_builder = chunk_builder.reborrow().init_regs(regs_chunk.len() as u32);
            for (idx, (key, reg)) in regs_chunk.iter().enumerate() {
                let mut entry = regs_builder.reborrow().get(idx as u32);
                entry.set_key(key);
                entry.set_reg(reg);
            }

            let mut tombs_builder = chunk_builder
                .reborrow()
                .init_tombs(tombs_chunk.len() as u32);
            for (idx, (key, ts, origin_actor)) in tombs_chunk.iter().enumerate() {
                let mut entry = tombs_builder.reborrow().get(idx as u32);
                entry.set_key(key);
                entry.set_ts(*ts);
                entry.set_origin_actor(origin_actor);
            }
        }
        req.send().await?;

        regs_slice = rest_regs;
        tombs_slice = rest_tombs;
    }

    Ok(true)
}

/// Returns the debug label associated with one sync domain.
fn domain_debug_label(domain: Domain) -> &'static str {
    match domain {
        Domain::Peers => "peers",
        Domain::Workloads => "workloads",
        Domain::Services => "services",
        Domain::Jobs => "jobs",
        Domain::Agents => "agents",
        Domain::Secrets => "secrets",
        Domain::Networks => "networks",
        Domain::NetworkPeers => "network peers",
        Domain::NetworkAttachments => "network attachments",
        Domain::ClusterViews => "cluster views",
        Domain::Volumes => "volumes",
        Domain::VolumeNodes => "volume nodes",
        Domain::SchedulerDigests => "scheduler digests",
    }
}

#[cfg(test)]
mod tests {
    use super::{EncodedRegister, EncodedTombstone, take_delta_chunk_prefix};

    /// Returns one synthetic encoded register entry for chunk-planning tests.
    fn encoded_register(key_len: usize, reg_len: usize) -> EncodedRegister {
        (vec![0u8; key_len], vec![0u8; reg_len])
    }

    /// Returns one synthetic encoded tombstone entry for chunk-planning tests.
    fn encoded_tombstone(key_len: usize) -> EncodedTombstone {
        (vec![0u8; key_len], 7, vec![1u8; 16])
    }

    /// The planner must still honor the entry cap when the payload target is generous.
    #[test]
    fn take_delta_chunk_prefix_respects_entry_limit() {
        let regs = vec![
            encoded_register(8, 16),
            encoded_register(8, 16),
            encoded_register(8, 16),
        ];

        let (regs_len, tombs_len, approx_payload_bytes) =
            take_delta_chunk_prefix(&regs, &[], 2, 1024);
        assert_eq!(regs_len, 2);
        assert_eq!(tombs_len, 0);
        assert_eq!(approx_payload_bytes, 48);
    }

    /// The planner should stop after the first entry once the approximate payload target is hit.
    #[test]
    fn take_delta_chunk_prefix_respects_payload_target() {
        let regs = vec![encoded_register(8, 40), encoded_register(8, 40)];

        let (regs_len, tombs_len, approx_payload_bytes) =
            take_delta_chunk_prefix(&regs, &[], 8, 64);
        assert_eq!(regs_len, 1);
        assert_eq!(tombs_len, 0);
        assert_eq!(approx_payload_bytes, 48);
    }

    /// The planner must always make progress even when one entry exceeds the target by itself.
    #[test]
    fn take_delta_chunk_prefix_always_keeps_one_large_entry() {
        let regs = vec![encoded_register(8, 512)];

        let (regs_len, tombs_len, approx_payload_bytes) =
            take_delta_chunk_prefix(&regs, &[], 8, 64);
        assert_eq!(regs_len, 1);
        assert_eq!(tombs_len, 0);
        assert_eq!(approx_payload_bytes, 520);
    }

    /// Tombstones should fill the remaining room after registers while preserving stream order.
    #[test]
    fn take_delta_chunk_prefix_adds_tombstones_after_registers() {
        let regs = vec![encoded_register(8, 16)];
        let tombs = vec![encoded_tombstone(8), encoded_tombstone(8)];

        let (regs_len, tombs_len, approx_payload_bytes) =
            take_delta_chunk_prefix(&regs, &tombs, 3, 1024);
        assert_eq!(regs_len, 1);
        assert_eq!(tombs_len, 2);
        assert_eq!(approx_payload_bytes, 88);
    }
}
