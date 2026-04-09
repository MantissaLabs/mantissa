//! Client side of Mantissa's anti-entropy protocol.
//!
//! This module drives the roots -> ranges -> delta handshake against a remote `Sync`
//! capability and exposes `DeltaSinkImpl`, the local sink used by the remote peer to
//! stream missing CRDT fragments back into the local stores.

use super::SyncStores;
use crate::agents::types::AgentRecordValue;
use crate::cluster::ClusterViewId;
use crate::jobs::types::JobSpecValue;
use crate::network::types::{NetworkAttachmentValue, NetworkPeerStateValue, NetworkSpecValue};
use crate::scheduler::digest::SchedulerDigestValue;
use crate::secrets::types::SecretValue;
use crate::services::types::ServiceSpecValue;
use crate::store::agent_store::AgentStore;
use crate::store::cluster_view_store::{ClusterViewDomainStore, ClusterViewMetadataRecord};
use crate::store::job_store::JobStore;
use crate::store::network_store::{NetworkAttachmentStore, NetworkPeerStore, NetworkSpecStore};
use crate::store::peer_store::PeersStore;
use crate::store::scheduler_digest_store::SchedulerDigestStore;
use crate::store::secret_store::SecretStore;
use crate::store::service_store::ServiceStore;
use crate::store::volume_store::{VolumeNodeStore, VolumeSpecStore};
use crate::store::workload_store::WorkloadStore;
use crate::sync::ranges::{capnp_fill_ranges, page_ranges_from_capnp};
use crate::topology::peers::PeerValue;
use crate::volumes::types::{VolumeNodeStateValue, VolumeSpecValue};
use crate::workload::model::WorkloadValue;
use async_trait::async_trait;
use bincode;
use capnp_rpc::new_client;
use crdt_store::{PageDigestRange, compute_want_from_have, uuid_key::UuidKey};
use crdts::MVReg;
use protocol::sync::{self, Domain, delta_chunk, delta_sink};
use std::io;
use std::rc::Rc;
use tracing::{debug, warn};

type RegisterDelta<V> = Vec<(UuidKey, MVReg<V, uuid::Uuid>)>;
type TombstoneDelta = Vec<(UuidKey, u64)>;

/// Same domain ordering as the server, used when a caller wants a full peer reconciliation.
const ALL_SYNC_DOMAINS: [Domain; 13] = [
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

/// Carries one peer-scoped context for anti-entropy diagnostics.
#[derive(Clone, Debug)]
pub struct SyncTraceContext {
    pub peer_id: uuid::Uuid,
    pub peer_addr: String,
    pub reason: &'static str,
}

impl SyncTraceContext {
    /// Builds one peer-scoped trace context used by sync diagnostics.
    pub fn peer(peer_id: uuid::Uuid, peer_addr: impl Into<String>, reason: &'static str) -> Self {
        Self {
            peer_id,
            peer_addr: peer_addr.into(),
            reason,
        }
    }
}

#[derive(Clone)]
/// Client-side anti-entropy runner that owns the local replicated domain stores.
///
/// Topology depends on this runner rather than rebuilding ad hoc sync store bundles
/// every time it opens a delta exchange against a remote peer.
pub struct SyncRunner {
    stores: SyncStores,
}

impl SyncRunner {
    /// Builds one anti-entropy runner over the provided local replicated stores.
    pub fn new(stores: SyncStores) -> Self {
        Self { stores }
    }

    /// Runs anti-entropy for every replicated domain against one peer.
    pub async fn sync_all_domains(
        &self,
        sync_cap: sync::Client,
        cluster_view: ClusterViewId,
        trace: Option<SyncTraceContext>,
    ) {
        self.sync_selected_domains(sync_cap, cluster_view, &ALL_SYNC_DOMAINS, trace)
            .await;
    }

    /// Runs anti-entropy for one caller-selected domain subset against one peer view.
    ///
    /// This is used by the global metadata loop to sync only `cluster_views` across split
    /// boundaries while keeping heavy domains view-scoped.
    pub async fn sync_selected_domains(
        &self,
        sync_cap: sync::Client,
        cluster_view: ClusterViewId,
        domains: &[Domain],
        trace: Option<SyncTraceContext>,
    ) {
        sync_selected_domains_with_stores(&self.stores, sync_cap, cluster_view, domains, trace)
            .await;
    }
}

impl SyncStores {
    /// Returns the local MST root digest for one domain so the roots phase can skip matches.
    async fn root_digest(&self, domain: Domain) -> [u8; 16] {
        match domain {
            Domain::Peers => self.peers.root_digest().await,
            Domain::Workloads => self.workloads.root_digest().await,
            Domain::Services => self.services.root_digest().await,
            Domain::Jobs => self.jobs.root_digest().await,
            Domain::Agents => self.agents.root_digest().await,
            Domain::Secrets => self.secrets.root_digest().await,
            Domain::Networks => self.networks.root_digest().await,
            Domain::NetworkPeers => self.network_peers.root_digest().await,
            Domain::NetworkAttachments => self.network_attachments.root_digest().await,
            Domain::ClusterViews => self.cluster_views.root_digest().await,
            Domain::Volumes => self.volumes.root_digest().await,
            Domain::VolumeNodes => self.volume_nodes.root_digest().await,
            Domain::SchedulerDigests => self.scheduler_digests.root_digest().await,
        }
    }

    /// Returns the local digest summary for one domain used to compute missing pages.
    async fn page_range_summary(&self, domain: Domain) -> crdt_store::Result<Vec<PageDigestRange>> {
        match domain {
            Domain::Peers => self.peers.page_range_summary().await,
            Domain::Workloads => self.workloads.page_range_summary().await,
            Domain::Services => self.services.page_range_summary().await,
            Domain::Jobs => self.jobs.page_range_summary().await,
            Domain::Agents => self.agents.page_range_summary().await,
            Domain::Secrets => self.secrets.page_range_summary().await,
            Domain::Networks => self.networks.page_range_summary().await,
            Domain::NetworkPeers => self.network_peers.page_range_summary().await,
            Domain::NetworkAttachments => self.network_attachments.page_range_summary().await,
            Domain::ClusterViews => self.cluster_views.page_range_summary().await,
            Domain::Volumes => self.volumes.page_range_summary().await,
            Domain::VolumeNodes => self.volume_nodes.page_range_summary().await,
            Domain::SchedulerDigests => self.scheduler_digests.page_range_summary().await,
        }
    }
}

/// Local sink implementation passed to a remote peer during `open_delta_for_view`.
///
/// The remote peer pushes typed delta chunks into this sink, which decodes them and applies
/// them directly into the appropriate replicated store.
pub struct DeltaSinkImpl {
    stores: SyncStores,
    expected_view: ClusterViewId,
}

impl DeltaSinkImpl {
    /// Builds a sink bound to the local stores and the cluster view negotiated for this sync.
    pub fn new(stores: SyncStores, expected_view: ClusterViewId) -> Self {
        Self {
            stores,
            expected_view,
        }
    }
}

impl delta_sink::Server for DeltaSinkImpl {
    async fn push_chunk(
        self: Rc<Self>,
        params: delta_sink::PushChunkParams,
    ) -> Result<(), capnp::Error> {
        let chunk = params.get()?.get_chunk()?;
        let domain = chunk
            .get_domain()
            .map_err(|_| capnp::Error::failed("unknown sync domain".into()))?;
        let chunk_view =
            ClusterViewId::from_capnp(chunk.get_view()?).map_err(capnp::Error::failed)?;
        if chunk_view != self.expected_view {
            return Err(capnp::Error::failed(format!(
                "delta chunk view mismatch: expected {}, got {}",
                self.expected_view, chunk_view
            )));
        }
        debug!(
            target: "delta",
            cluster_view = %chunk_view,
            ?domain,
            "received delta chunk"
        );

        // Each domain uses the same transport format but deserializes into a different value type.
        match domain {
            Domain::Peers => {
                apply_chunk(
                    self.stores.peers.clone(),
                    &chunk,
                    decode_register::<PeerValue>,
                )
                .await?
            }
            Domain::Workloads => {
                apply_chunk(
                    self.stores.workloads.clone(),
                    &chunk,
                    decode_register::<WorkloadValue>,
                )
                .await?
            }
            Domain::Jobs => {
                apply_chunk(
                    self.stores.jobs.clone(),
                    &chunk,
                    decode_register::<JobSpecValue>,
                )
                .await?
            }
            Domain::Agents => {
                apply_chunk(
                    self.stores.agents.clone(),
                    &chunk,
                    decode_register::<AgentRecordValue>,
                )
                .await?
            }
            Domain::Services => {
                apply_chunk(
                    self.stores.services.clone(),
                    &chunk,
                    decode_register::<ServiceSpecValue>,
                )
                .await?
            }
            Domain::Secrets => {
                apply_chunk(
                    self.stores.secrets.clone(),
                    &chunk,
                    decode_register::<SecretValue>,
                )
                .await?
            }
            Domain::Networks => {
                apply_chunk(
                    self.stores.networks.clone(),
                    &chunk,
                    decode_register::<NetworkSpecValue>,
                )
                .await?
            }
            Domain::NetworkPeers => {
                apply_chunk(
                    self.stores.network_peers.clone(),
                    &chunk,
                    decode_register::<NetworkPeerStateValue>,
                )
                .await?
            }
            Domain::NetworkAttachments => {
                apply_chunk(
                    self.stores.network_attachments.clone(),
                    &chunk,
                    decode_register::<NetworkAttachmentValue>,
                )
                .await?
            }
            Domain::ClusterViews => {
                apply_chunk(
                    self.stores.cluster_views.clone(),
                    &chunk,
                    decode_register::<ClusterViewMetadataRecord>,
                )
                .await?
            }
            Domain::Volumes => {
                apply_chunk(
                    self.stores.volumes.clone(),
                    &chunk,
                    decode_register::<VolumeSpecValue>,
                )
                .await?
            }
            Domain::VolumeNodes => {
                apply_chunk(
                    self.stores.volume_nodes.clone(),
                    &chunk,
                    decode_register::<VolumeNodeStateValue>,
                )
                .await?
            }
            Domain::SchedulerDigests => {
                apply_chunk(
                    self.stores.scheduler_digests.clone(),
                    &chunk,
                    decode_register::<SchedulerDigestValue>,
                )
                .await?
            }
        }

        Ok(())
    }

    async fn end(
        self: Rc<Self>,
        _params: delta_sink::EndParams,
        _results: delta_sink::EndResults,
    ) -> Result<(), capnp::Error> {
        debug!(target: "delta", "delta stream end");
        Ok(())
    }
}

/// Decodes one streamed chunk and merges it into the destination store.
async fn apply_chunk<V, F>(
    store: impl DeltaStore<V>,
    chunk: &delta_chunk::Reader<'_>,
    decode: F,
) -> Result<(), capnp::Error>
where
    V: Clone + Send + Sync + 'static,
    F: Fn(&delta_chunk::Reader<'_>) -> Result<RegisterDelta<V>, capnp::Error>,
{
    let regs = decode(chunk)?;
    let tombs = collect_tombstones(chunk)?;

    store.apply_delta(regs, tombs).await.map_err(to_capnp)
}

/// Extracts tombstone rows from a wire chunk.
fn collect_tombstones(chunk: &delta_chunk::Reader<'_>) -> Result<TombstoneDelta, capnp::Error> {
    let mut tombs = Vec::new();
    for entry in chunk.get_tombs()?.iter() {
        let key =
            UuidKey::try_from(entry.get_key()?).map_err(|e| capnp::Error::failed(e.to_string()))?;
        tombs.push((key, entry.get_ts()));
    }
    Ok(tombs)
}

/// Deserializes MVReg payloads from one wire chunk for the selected domain value type.
fn decode_register<V>(chunk: &delta_chunk::Reader<'_>) -> Result<RegisterDelta<V>, capnp::Error>
where
    V: for<'de> serde::Deserialize<'de>,
{
    let mut regs = Vec::new();
    for entry in chunk.get_regs()?.iter() {
        let key =
            UuidKey::try_from(entry.get_key()?).map_err(|e| capnp::Error::failed(e.to_string()))?;
        let register: MVReg<V, uuid::Uuid> = bincode::deserialize(entry.get_reg()?)
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        regs.push((key, register));
    }
    Ok(regs)
}

/// Normalizes storage/runtime errors into Cap'n Proto failures for RPC propagation.
fn to_capnp<E: std::fmt::Display>(e: E) -> capnp::Error {
    capnp::Error::failed(e.to_string())
}

/// Small abstraction over replicated stores that can consume streamed anti-entropy chunks.
#[async_trait]
trait DeltaStore<V>: Clone + Send + Sync + 'static {
    async fn apply_delta(self, regs: RegisterDelta<V>, tombs: TombstoneDelta) -> io::Result<()>;
}

#[async_trait]
impl DeltaStore<PeerValue> for PeersStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<PeerValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<WorkloadValue> for WorkloadStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<WorkloadValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<ServiceSpecValue> for ServiceStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<ServiceSpecValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<JobSpecValue> for JobStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<JobSpecValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<AgentRecordValue> for AgentStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<AgentRecordValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<SecretValue> for SecretStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<SecretValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<NetworkSpecValue> for NetworkSpecStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<NetworkSpecValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<NetworkPeerStateValue> for NetworkPeerStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<NetworkPeerStateValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<NetworkAttachmentValue> for NetworkAttachmentStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<NetworkAttachmentValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<ClusterViewMetadataRecord> for ClusterViewDomainStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<ClusterViewMetadataRecord, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<VolumeSpecValue> for VolumeSpecStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<VolumeSpecValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<VolumeNodeStateValue> for VolumeNodeStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<VolumeNodeStateValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

#[async_trait]
impl DeltaStore<SchedulerDigestValue> for SchedulerDigestStore {
    async fn apply_delta(
        self,
        regs: Vec<(UuidKey, MVReg<SchedulerDigestValue, uuid::Uuid>)>,
        tombs: Vec<(UuidKey, u64)>,
    ) -> io::Result<()> {
        self.apply_delta_chunk_update_mst(regs, tombs).await
    }
}

/// Runs anti-entropy for one caller-selected domain subset against one peer view.
///
/// The runner keeps ownership of the local store handles; this helper only borrows them so
/// topology does not have to rebuild one store bundle for every sync attempt.
async fn sync_selected_domains_with_stores(
    stores: &SyncStores,
    sync_cap: sync::Client,
    cluster_view: ClusterViewId,
    domains: &[Domain],
    trace: Option<SyncTraceContext>,
) {
    if domains.is_empty() {
        return;
    }

    let requested_domains = domains.to_vec();
    let res: Result<(), capnp::Error> = async {
        let mut roots_req = sync_cap.get_roots_for_view_request();
        {
            let mut req = roots_req.get().init_req();
            cluster_view.write_capnp(req.reborrow().init_view());
        }
        let roots_resp = roots_req.send().promise.await?;
        let roots_reader = roots_resp.get()?.get_roots()?;

        let mut remote_roots = Vec::with_capacity(roots_reader.len() as usize);
        for idx in 0..roots_reader.len() {
            let entry = roots_reader.get(idx);
            let root_view =
                ClusterViewId::from_capnp(entry.get_view()?).map_err(capnp::Error::failed)?;
            if root_view != cluster_view {
                return Err(capnp::Error::failed(format!(
                    "sync roots view mismatch: expected {cluster_view}, got {root_view}"
                )));
            }
            let domain = entry
                .get_domain()
                .map_err(|_| capnp::Error::failed("unknown domain".into()))?;
            let digest = read_root_digest(entry.get_root_digest()?)?;
            remote_roots.push((domain, digest));
        }

        let mut domains_to_sync = Vec::new();
        // Root equality lets us skip the more expensive page-summary walk for matched domains.
        for domain in &requested_domains {
            let local_root = stores.root_digest(*domain).await;
            let remote_root = remote_roots
                .iter()
                .find(|(candidate, _)| candidate == domain)
                .map(|(_, digest)| *digest)
                .unwrap_or_default();
            if remote_root != local_root {
                domains_to_sync.push(*domain);
            }
        }

        if domains_to_sync.is_empty() {
            return Ok(());
        }

        let mut ranges_req = sync_cap.get_ranges_for_view_request();
        {
            let mut req = ranges_req.get().init_req();
            cluster_view.write_capnp(req.reborrow().init_view());
            let mut list = req.reborrow().init_domains(domains_to_sync.len() as u32);
            for (idx, domain) in domains_to_sync.iter().enumerate() {
                list.set(idx as u32, *domain);
            }
        }
        let ranges_resp = ranges_req.send().promise.await?;
        let ranges_reader = ranges_resp.get()?.get_ranges()?;

        let mut domains_wants = Vec::new();
        for idx in 0..ranges_reader.len() {
            let summary = ranges_reader.get(idx);
            let summary_view =
                ClusterViewId::from_capnp(summary.get_view()?).map_err(capnp::Error::failed)?;
            if summary_view != cluster_view {
                return Err(capnp::Error::failed(format!(
                    "sync ranges view mismatch: expected {cluster_view}, got {summary_view}"
                )));
            }
            let domain = summary
                .get_domain()
                .map_err(|_| capnp::Error::failed("unknown domain".into()))?;
            let remote_summary = summary.get_summary()?;
            let remote_ranges = page_ranges_from_capnp(remote_summary)?;
            let local_ranges = stores.page_range_summary(domain).await.map_err(to_capnp)?;
            // Ask the peer only for pages present in its summary but missing locally.
            let want = compute_want_from_have(&remote_ranges, &local_ranges);
            if !want.is_empty() {
                domains_wants.push((domain, want));
            }
        }

        if domains_wants.is_empty() {
            return Ok(());
        }

        let sink_client = new_client(DeltaSinkImpl::new(stores.clone(), cluster_view));

        let mut od = sync_cap.open_delta_for_view_request();
        {
            let mut req = od.get().init_req();
            cluster_view.write_capnp(req.reborrow().init_view());
            let mut wants_builder = req.reborrow().init_wants(domains_wants.len() as u32);
            for (idx, (domain, want_ranges)) in domains_wants.iter().enumerate() {
                let mut entry = wants_builder.reborrow().get(idx as u32);
                entry.set_domain(*domain);
                let summary_builder = entry.reborrow().init_want();
                capnp_fill_ranges(want_ranges, summary_builder)?;
                cluster_view.write_capnp(entry.reborrow().init_view());
            }
            req.set_sink(sink_client);
        }

        debug!(
            target: "sync",
            cluster_view = %cluster_view,
            domains_requested = requested_domains.len(),
            domains_with_delta = domains_wants.len(),
            "opening selective delta stream"
        );
        od.send().promise.await?;
        Ok(())
    }
    .await;

    if let Err(e) = res {
        warn!(
            target: "sync",
            cluster_view = %cluster_view,
            domains_requested = requested_domains.len(),
            "sync_selected_domains error: {e}"
        );
        if let Some(ctx) = trace.as_ref() {
            warn!(
                target: "diag.sync.peer",
                cluster_view = %cluster_view,
                peer = %ctx.peer_id,
                addr = %ctx.peer_addr,
                reason = %ctx.reason,
                disconnected = is_disconnected_capnp(&e),
                error = %e,
                "peer-scoped sync_selected_domains failure"
            );
        }
    }
}

/// Returns true when one Cap'n Proto error corresponds to a disconnected transport path.
fn is_disconnected_capnp(error: &capnp::Error) -> bool {
    let text = error.to_string();
    text.contains("Disconnected") || text.contains("disconnected")
}

/// Decodes one fixed-width XXHash128 root digest from the sync wire format.
fn read_root_digest(bytes: &[u8]) -> Result<[u8; 16], capnp::Error> {
    bytes.try_into().map_err(|_| {
        capnp::Error::failed(format!(
            "invalid sync root digest length: expected 16, got {}",
            bytes.len()
        ))
    })
}
