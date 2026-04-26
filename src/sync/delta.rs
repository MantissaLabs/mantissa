//! Client side of Mantissa's anti-entropy protocol.
//!
//! This module drives the roots -> ranges -> delta handshake against a remote `Sync`
//! capability and exposes `DeltaSinkImpl`, the local sink used by the remote peer to
//! stream missing CRDT fragments back into the local stores.

use super::{ALL_DOMAINS, SyncStores};
use crate::cluster::{ClusterViewId, RootSchemaState};
use crate::sync::ranges::{capnp_fill_ranges, page_ranges_from_capnp};
use capnp_rpc::new_client;
use crdt_store::adapter::RegAdapter;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::{Entry, PageDigestRange, TableSet, compute_want_from_have, uuid_key::UuidKey};
use merkle_search_tree::digest::Hasher as MstHasher;
use protocol::sync::{self, Domain, delta_chunk, delta_sink};
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::{debug, warn};

type RegisterDelta<C> = Vec<(UuidKey, <C as RegAdapter>::Reg)>;
type TombstoneDelta = Vec<(UuidKey, u64)>;

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
    root_schema: RootSchemaState,
    attachment_sync_notify: Option<Arc<Notify>>,
}

impl SyncRunner {
    /// Builds one anti-entropy runner over the provided local replicated stores.
    pub fn new(
        stores: SyncStores,
        root_schema: RootSchemaState,
        attachment_sync_notify: Option<Arc<Notify>>,
    ) -> Self {
        Self {
            stores,
            root_schema,
            attachment_sync_notify,
        }
    }

    /// Runs anti-entropy for every replicated domain against one peer.
    pub async fn sync_all_domains(
        &self,
        sync_cap: sync::Client,
        cluster_view: ClusterViewId,
        root_schema_version: u32,
        trace: Option<SyncTraceContext>,
    ) {
        self.sync_selected_domains(
            sync_cap,
            cluster_view,
            root_schema_version,
            &ALL_DOMAINS,
            trace,
        )
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
        root_schema_version: u32,
        domains: &[Domain],
        trace: Option<SyncTraceContext>,
    ) {
        sync_selected_domains_with_stores(
            &self.stores,
            sync_cap,
            cluster_view,
            root_schema_version,
            domains,
            trace,
            self.attachment_sync_notify.clone(),
        )
        .await;
    }

    /// Rebuilds the local in-memory MSTs for one selected semantic root schema.
    pub async fn rebuild_msts_for_root_schema_version(
        &self,
        root_schema_version: u32,
    ) -> crdt_store::Result<()> {
        if !self.root_schema.supports(root_schema_version) {
            return Err(Box::new(crdt_store::error::Error::Other(format!(
                "unsupported root schema version {root_schema_version}"
            ))));
        }
        self.stores
            .rebuild_msts_for_root_schema_version(root_schema_version)
            .await
    }
}

impl SyncStores {
    /// Returns the local MST root digest for one domain so the roots phase can skip matches.
    async fn root_digest(
        &self,
        domain: Domain,
        root_schema_version: u32,
    ) -> crdt_store::Result<[u8; 16]> {
        match domain {
            Domain::Peers => self.peers.root_digest_at_version(root_schema_version).await,
            Domain::Workloads => {
                self.workloads
                    .root_digest_at_version(root_schema_version)
                    .await
            }
            Domain::Services => {
                self.services
                    .root_digest_at_version(root_schema_version)
                    .await
            }
            Domain::Jobs => self.jobs.root_digest_at_version(root_schema_version).await,
            Domain::Agents => {
                self.agents
                    .root_digest_at_version(root_schema_version)
                    .await
            }
            Domain::Secrets => {
                self.secrets
                    .root_digest_at_version(root_schema_version)
                    .await
            }
            Domain::Networks => {
                self.networks
                    .root_digest_at_version(root_schema_version)
                    .await
            }
            Domain::NetworkPeers => {
                self.network_peers
                    .root_digest_at_version(root_schema_version)
                    .await
            }
            Domain::NetworkAttachments => {
                self.network_attachments
                    .root_digest_at_version(root_schema_version)
                    .await
            }
            Domain::ClusterViews => {
                self.cluster_views
                    .root_digest_at_version(root_schema_version)
                    .await
            }
            Domain::Volumes => {
                self.volumes
                    .root_digest_at_version(root_schema_version)
                    .await
            }
            Domain::VolumeNodes => {
                self.volume_nodes
                    .root_digest_at_version(root_schema_version)
                    .await
            }
            Domain::SchedulerDigests => {
                self.scheduler_digests
                    .root_digest_at_version(root_schema_version)
                    .await
            }
        }
    }

    /// Returns the local digest summary for one domain used to compute missing pages.
    async fn page_range_summary(
        &self,
        domain: Domain,
        root_schema_version: u32,
    ) -> crdt_store::Result<Vec<PageDigestRange>> {
        match domain {
            Domain::Peers => {
                self.peers
                    .page_range_summary_at_version(root_schema_version)
                    .await
            }
            Domain::Workloads => {
                self.workloads
                    .page_range_summary_at_version(root_schema_version)
                    .await
            }
            Domain::Services => {
                self.services
                    .page_range_summary_at_version(root_schema_version)
                    .await
            }
            Domain::Jobs => {
                self.jobs
                    .page_range_summary_at_version(root_schema_version)
                    .await
            }
            Domain::Agents => {
                self.agents
                    .page_range_summary_at_version(root_schema_version)
                    .await
            }
            Domain::Secrets => {
                self.secrets
                    .page_range_summary_at_version(root_schema_version)
                    .await
            }
            Domain::Networks => {
                self.networks
                    .page_range_summary_at_version(root_schema_version)
                    .await
            }
            Domain::NetworkPeers => {
                self.network_peers
                    .page_range_summary_at_version(root_schema_version)
                    .await
            }
            Domain::NetworkAttachments => {
                self.network_attachments
                    .page_range_summary_at_version(root_schema_version)
                    .await
            }
            Domain::ClusterViews => {
                self.cluster_views
                    .page_range_summary_at_version(root_schema_version)
                    .await
            }
            Domain::Volumes => {
                self.volumes
                    .page_range_summary_at_version(root_schema_version)
                    .await
            }
            Domain::VolumeNodes => {
                self.volume_nodes
                    .page_range_summary_at_version(root_schema_version)
                    .await
            }
            Domain::SchedulerDigests => {
                self.scheduler_digests
                    .page_range_summary_at_version(root_schema_version)
                    .await
            }
        }
    }

    /// Rebuilds every in-memory domain MST using one selected semantic root schema.
    pub async fn rebuild_msts_for_root_schema_version(
        &self,
        root_schema_version: u32,
    ) -> crdt_store::Result<()> {
        self.peers
            .rebuild_mst_from_disk_at_version(root_schema_version)
            .await?;
        self.workloads
            .rebuild_mst_from_disk_at_version(root_schema_version)
            .await?;
        self.jobs
            .rebuild_mst_from_disk_at_version(root_schema_version)
            .await?;
        self.agents
            .rebuild_mst_from_disk_at_version(root_schema_version)
            .await?;
        self.services
            .rebuild_mst_from_disk_at_version(root_schema_version)
            .await?;
        self.secrets
            .rebuild_mst_from_disk_at_version(root_schema_version)
            .await?;
        self.networks
            .rebuild_mst_from_disk_at_version(root_schema_version)
            .await?;
        self.network_peers
            .rebuild_mst_from_disk_at_version(root_schema_version)
            .await?;
        self.network_attachments
            .rebuild_mst_from_disk_at_version(root_schema_version)
            .await?;
        self.cluster_views
            .rebuild_mst_from_disk_at_version(root_schema_version)
            .await?;
        self.volumes
            .rebuild_mst_from_disk_at_version(root_schema_version)
            .await?;
        self.volume_nodes
            .rebuild_mst_from_disk_at_version(root_schema_version)
            .await?;
        self.scheduler_digests
            .rebuild_mst_from_disk_at_version(root_schema_version)
            .await?;
        Ok(())
    }
}

/// Local sink implementation passed to a remote peer during `open_delta_for_view`.
///
/// The remote peer pushes typed delta chunks into this sink, which decodes them and applies
/// them directly into the appropriate replicated store.
pub struct DeltaSinkImpl {
    stores: SyncStores,
    expected_view: ClusterViewId,
    expected_root_schema_version: u32,
}

impl DeltaSinkImpl {
    /// Builds a sink bound to the local stores and the cluster view negotiated for this sync.
    pub fn new(
        stores: SyncStores,
        expected_view: ClusterViewId,
        expected_root_schema_version: u32,
    ) -> Self {
        Self {
            stores,
            expected_view,
            expected_root_schema_version,
        }
    }
}

// Expands one explicit `Domain -> SyncStores field` mapping into the async dispatch that
// feeds the incoming chunk into the correct replicated store. Keeping the mapping local
// preserves a readable domain switch without repeating the same `apply_chunk(...).await?`
// boilerplate for every sync domain.
macro_rules! apply_domain_chunk {
    ($stores:expr, $chunk:expr, $domain:expr, {
        $($variant:ident => $field:ident),+ $(,)?
    }) => {
        match $domain {
            $(Domain::$variant => apply_chunk($stores.$field.clone(), $chunk).await?,)+
        }
    };
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
        if chunk.get_root_schema_version() != self.expected_root_schema_version {
            return Err(capnp::Error::failed(format!(
                "delta chunk root schema mismatch: expected {}, got {}",
                self.expected_root_schema_version,
                chunk.get_root_schema_version()
            )));
        }
        debug!(
            target: "delta",
            cluster_view = %chunk_view,
            ?domain,
            root_schema_version = self.expected_root_schema_version,
            "received delta chunk"
        );

        // Domain dispatch stays explicit, but the store itself now carries the decoded value type.
        apply_domain_chunk!(self.stores, &chunk, domain, {
            Peers => peers,
            Workloads => workloads,
            Jobs => jobs,
            Agents => agents,
            Services => services,
            Secrets => secrets,
            Networks => networks,
            NetworkPeers => network_peers,
            NetworkAttachments => network_attachments,
            ClusterViews => cluster_views,
            Volumes => volumes,
            VolumeNodes => volume_nodes,
            SchedulerDigests => scheduler_digests,
        });

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

/// Decodes one streamed chunk and merges it into one typed MST-backed replicated store.
///
/// All replicated sync domains in Mantissa are backed by `Arc<CrdtMstStore<...>>` with the
/// same delta-apply entrypoint, but with different value types and table sets. This helper
/// targets that shared storage abstraction directly so the delta sink can reuse one generic
/// path for every domain instead of carrying one wrapper trait impl per store alias.
///
/// In the larger sync flow, `push_chunk()` has already validated the chunk's cluster view and
/// selected the destination store for the reported domain. `apply_chunk()` is the narrow step
/// that turns the wire payload into typed register/tombstone batches and hands them to the
/// store's incremental MST update path.
async fn apply_chunk<C, H, T>(
    store: Arc<CrdtMstStore<C, H, T>>,
    chunk: &delta_chunk::Reader<'_>,
) -> Result<(), capnp::Error>
where
    C: RegAdapter<Key = UuidKey, Actor = uuid::Uuid>,
    H: MstHasher<16, C::Key>
        + MstHasher<16, Entry<C::Snapshot>>
        + Default
        + Clone
        + Send
        + Sync
        + 'static,
    T: TableSet,
{
    // Registers and tombstones share the same chunk envelope, but the register payload must be
    // deserialized by the destination store's CRDT adapter.
    let regs = decode_register::<C>(chunk)?;
    let tombs = collect_tombstones(chunk)?;

    store
        .apply_delta_chunk_update_mst(regs, tombs)
        .await
        .map_err(to_capnp)
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

/// Deserializes register payloads from one wire chunk with the selected domain adapter.
fn decode_register<C>(chunk: &delta_chunk::Reader<'_>) -> Result<RegisterDelta<C>, capnp::Error>
where
    C: RegAdapter<Key = UuidKey>,
{
    let mut regs = Vec::new();
    for entry in chunk.get_regs()?.iter() {
        let key =
            UuidKey::try_from(entry.get_key()?).map_err(|e| capnp::Error::failed(e.to_string()))?;
        let register =
            C::decode_reg(entry.get_reg()?).map_err(|e| capnp::Error::failed(e.to_string()))?;
        regs.push((key, register));
    }
    Ok(regs)
}

/// Normalizes storage/runtime errors into Cap'n Proto failures for RPC propagation.
fn to_capnp<E: std::fmt::Display>(e: E) -> capnp::Error {
    capnp::Error::failed(e.to_string())
}

/// Runs anti-entropy for one caller-selected domain subset against one peer view.
///
/// The runner keeps ownership of the local store handles; this helper only borrows them so
/// topology does not have to rebuild one store bundle for every sync attempt.
async fn sync_selected_domains_with_stores(
    stores: &SyncStores,
    sync_cap: sync::Client,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    domains: &[Domain],
    trace: Option<SyncTraceContext>,
    attachment_sync_notify: Option<Arc<Notify>>,
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
            req.set_root_schema_version(root_schema_version);
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
            if entry.get_root_schema_version() != root_schema_version {
                return Err(capnp::Error::failed(format!(
                    "sync roots root schema mismatch: expected {root_schema_version}, got {}",
                    entry.get_root_schema_version()
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
            let local_root = stores
                .root_digest(*domain, root_schema_version)
                .await
                .map_err(to_capnp)?;
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
            req.set_root_schema_version(root_schema_version);
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
            if summary.get_root_schema_version() != root_schema_version {
                return Err(capnp::Error::failed(format!(
                    "sync ranges root schema mismatch: expected {root_schema_version}, got {}",
                    summary.get_root_schema_version()
                )));
            }
            let domain = summary
                .get_domain()
                .map_err(|_| capnp::Error::failed("unknown domain".into()))?;
            let remote_summary = summary.get_summary()?;
            let remote_ranges = page_ranges_from_capnp(remote_summary)?;
            let local_ranges = stores
                .page_range_summary(domain, root_schema_version)
                .await
                .map_err(to_capnp)?;
            // Ask the peer only for pages present in its summary but missing locally.
            let want = compute_want_from_have(&remote_ranges, &local_ranges);
            if !want.is_empty() {
                domains_wants.push((domain, want));
            }
        }

        if domains_wants.is_empty() {
            return Ok(());
        }

        let sink_client = new_client(DeltaSinkImpl::new(
            stores.clone(),
            cluster_view,
            root_schema_version,
        ));

        let mut od = sync_cap.open_delta_for_view_request();
        {
            let mut req = od.get().init_req();
            cluster_view.write_capnp(req.reborrow().init_view());
            req.set_root_schema_version(root_schema_version);
            let mut wants_builder = req.reborrow().init_wants(domains_wants.len() as u32);
            for (idx, (domain, want_ranges)) in domains_wants.iter().enumerate() {
                let mut entry = wants_builder.reborrow().get(idx as u32);
                entry.set_domain(*domain);
                let summary_builder = entry.reborrow().init_want();
                capnp_fill_ranges(want_ranges, summary_builder)?;
                cluster_view.write_capnp(entry.reborrow().init_view());
                entry.set_root_schema_version(root_schema_version);
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
        if should_notify_network_attachment_sync(&domains_wants)
            && let Some(notify) = attachment_sync_notify.as_ref()
        {
            // Remote nodes otherwise only notice replicated attachment changes on the slow
            // attachment-refresh poll. Wake the network controller immediately so forwarding
            // catches up as soon as anti-entropy applies the attachment delta locally.
            notify.notify_one();
        }
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

/// Returns true when a completed delta stream included replicated attachment changes.
///
/// Attachment deltas require a follow-up forwarding refresh on the receiving node so the local
/// VXLAN FDB catches up before clients try to send traffic to newly replicated remote backends.
fn should_notify_network_attachment_sync(domains_wants: &[(Domain, Vec<PageDigestRange>)]) -> bool {
    domains_wants
        .iter()
        .any(|(domain, _)| *domain == Domain::NetworkAttachments)
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

#[cfg(test)]
mod tests {
    use super::should_notify_network_attachment_sync;
    use crdt_store::PageDigestRange;
    use protocol::sync::Domain;

    /// Attachment deltas should wake the network controller on the receiving node immediately.
    #[test]
    fn attachment_domain_requests_forwarding_refresh_notification() {
        let wants = vec![(Domain::NetworkAttachments, Vec::<PageDigestRange>::new())];

        assert!(should_notify_network_attachment_sync(&wants));
    }

    /// Non-attachment deltas must not trigger unnecessary forwarding refresh work.
    #[test]
    fn non_attachment_domains_skip_forwarding_refresh_notification() {
        let wants = vec![
            (Domain::Workloads, Vec::<PageDigestRange>::new()),
            (Domain::Services, Vec::<PageDigestRange>::new()),
        ];

        assert!(!should_notify_network_attachment_sync(&wants));
    }
}
