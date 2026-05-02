//! Client side of Mantissa's anti-entropy protocol.
//!
//! This module drives the roots -> ranges -> delta handshake against a remote `Sync`
//! capability and exposes `DeltaSinkImpl`, the local sink used by the remote peer to
//! stream missing CRDT fragments back into the local stores.

use super::{ALL_DOMAINS, SyncStores};
use crate::cluster::{ClusterViewId, RootSchemaState};
use crate::store::registry::{EncodedRegisters, EncodedTombstones};
use crate::sync::gc_progress::SyncGcProgress;
use crate::sync::ranges::{capnp_fill_ranges, page_ranges_from_capnp};
use capnp_rpc::new_client;
use mantissa_protocol::sync::{self, Domain, delta_chunk, delta_sink};
use mantissa_store::PageDigestRange;
use mantissa_store::compute_want_from_have;
use mantissa_store::mst_store::TombstonePruneFrontiers;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::{debug, warn};

/// Root-phase payload for one remote domain, including GC prune-frontier metadata.
struct RemoteDomainRoot {
    domain: Domain,
    digest: [u8; 16],
    prune_frontiers: TombstonePruneFrontiers,
}

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

/// Extra client-side state shared across one selected-domain sync pass.
struct SyncClientContext {
    trace: Option<SyncTraceContext>,
    gc_progress: SyncGcProgress,
    attachment_sync_notify: Option<Arc<Notify>>,
}

#[derive(Clone)]
/// Client-side anti-entropy runner that owns the local replicated domain stores.
///
/// Topology depends on this runner rather than rebuilding ad hoc sync store bundles
/// every time it opens a delta exchange against a remote peer.
pub struct SyncRunner {
    stores: SyncStores,
    root_schema: RootSchemaState,
    gc_progress: SyncGcProgress,
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
            gc_progress: SyncGcProgress::new(),
            attachment_sync_notify,
        }
    }

    /// Returns the sync-derived root equality tracker used by store GC.
    pub fn gc_progress(&self) -> SyncGcProgress {
        self.gc_progress.clone()
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
            SyncClientContext {
                trace,
                gc_progress: self.gc_progress.clone(),
                attachment_sync_notify: self.attachment_sync_notify.clone(),
            },
        )
        .await;
    }

    /// Rebuilds the local in-memory MSTs for one selected semantic root schema.
    pub async fn rebuild_msts_for_root_schema_version(
        &self,
        root_schema_version: u32,
    ) -> mantissa_store::Result<()> {
        if !self.root_schema.supports(root_schema_version) {
            return Err(Box::new(mantissa_store::error::Error::Other(format!(
                "unsupported root schema version {root_schema_version}"
            ))));
        }
        self.stores
            .rebuild_msts_for_root_schema_version(root_schema_version)
            .await
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

        let registers = collect_registers(&chunk)?;
        let tombstones = collect_tombstones(&chunk)?;
        self.stores
            .require(domain)
            .map_err(to_capnp)?
            .store
            .apply_delta_encoded(registers, tombstones)
            .await
            .map_err(to_capnp)?;

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

/// Extracts opaque tombstone rows from a wire chunk.
fn collect_tombstones(chunk: &delta_chunk::Reader<'_>) -> Result<EncodedTombstones, capnp::Error> {
    let mut tombs = Vec::new();
    for entry in chunk.get_tombs()?.iter() {
        tombs.push((
            entry.get_key()?.to_vec(),
            entry.get_ts(),
            entry.get_origin_actor()?.to_vec(),
        ));
    }
    Ok(tombs)
}

/// Extracts opaque register payloads from one wire chunk.
fn collect_registers(chunk: &delta_chunk::Reader<'_>) -> Result<EncodedRegisters, capnp::Error> {
    let mut regs = Vec::new();
    for entry in chunk.get_regs()?.iter() {
        regs.push((entry.get_key()?.to_vec(), entry.get_reg()?.to_vec()));
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
    context: SyncClientContext,
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
            let frontiers_reader = entry.get_tombstone_prune_frontiers()?;
            let mut prune_frontiers = Vec::with_capacity(frontiers_reader.len() as usize);
            for frontier in frontiers_reader.iter() {
                let origin_actor = frontier.get_origin_actor()?.to_vec();
                let sequence = frontier.get_sequence();
                if sequence > 0 {
                    prune_frontiers.push((origin_actor, sequence));
                }
            }
            remote_roots.push(RemoteDomainRoot {
                domain,
                digest,
                prune_frontiers,
            });
        }

        let mut domains_to_sync = Vec::new();
        // Root equality lets us skip the more expensive page-summary walk for matched domains.
        for domain in &requested_domains {
            let remote_root = remote_roots
                .iter()
                .find(|candidate| candidate.domain == *domain);
            if let Some(remote_root) = remote_root
                && !remote_root.prune_frontiers.is_empty()
            {
                stores
                    .apply_tombstone_prune_frontiers(*domain, remote_root.prune_frontiers.clone())
                    .await
                    .map_err(to_capnp)?;
            }

            let local_root = stores
                .root_digest_at_version(*domain, root_schema_version)
                .await
                .map_err(to_capnp)?;
            let remote_root = remote_root.map(|root| root.digest);
            match remote_root {
                Some(remote_root) if remote_root == local_root => {
                    if let Some(ctx) = context.trace.as_ref() {
                        context.gc_progress.record_equal_root_now(
                            ctx.peer_id,
                            *domain,
                            cluster_view,
                            root_schema_version,
                        );
                    }
                }
                Some(_) | None => domains_to_sync.push(*domain),
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
                .page_range_summary_at_version(domain, root_schema_version)
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
            && let Some(notify) = context.attachment_sync_notify.as_ref()
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
        if let Some(ctx) = context.trace.as_ref() {
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
    use mantissa_protocol::sync::Domain;
    use mantissa_store::PageDigestRange;

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
