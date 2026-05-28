//! Client side of Mantissa's anti-entropy protocol.
//!
//! This module is the caller side of sync. It asks a remote peer for roots, asks for range
//! summaries when roots differ, and then opens a local sink so the remote peer can stream back the
//! CRDT rows this node is missing.

use super::encoding::{self, DomainDeltaRequest, RemoteDomainRangeSummary, RemoteDomainRoot};
use super::{ALL_DOMAINS, SyncStores};
use crate::cluster::{ClusterViewId, RootSchemaState};
use crate::store::replicated::registry::{EncodedRegisters, EncodedTombstones};
use crate::sync::gc_progress::SyncGcProgress;
use capnp_rpc::new_client;
use mantissa_protocol::sync::{self, Domain, delta_sink};
use mantissa_store::compute_want_from_have;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::{debug, warn};

/// View and root schema used for one client-side sync attempt.
///
/// The client sends this scope on every request and expects the remote peer to echo it back in
/// every response and streamed chunk. That protects the local stores from applying data from a
/// different cluster view or MST schema.
#[derive(Clone, Copy, Debug)]
struct SyncAttemptScope {
    cluster_view: ClusterViewId,
    root_schema_version: u32,
}

impl SyncAttemptScope {
    /// Builds the request scope shared by all three sync phases.
    fn new(cluster_view: ClusterViewId, root_schema_version: u32) -> Self {
        Self {
            cluster_view,
            root_schema_version,
        }
    }
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

/// Local side effects and diagnostics shared across one selected-domain sync pass.
struct SyncClientContext {
    trace: Option<SyncTraceContext>,
    gc_progress: SyncGcProgress,
    attachment_sync_notify: Option<Arc<Notify>>,
    master_key_replication_notify: Option<Arc<Notify>>,
}

/// Client-side anti-entropy runner that owns the local replicated domain stores.
///
/// Topology owns peer selection and transport. This runner owns the store-facing sync work for one
/// selected peer once topology hands it a remote `Sync` capability.
#[derive(Clone)]
pub struct SyncRunner {
    stores: SyncStores,
    root_schema: RootSchemaState,
    gc_progress: SyncGcProgress,
    attachment_sync_notify: Option<Arc<Notify>>,
    master_key_replication_notify: Option<Arc<Notify>>,
}

impl SyncRunner {
    /// Builds one anti-entropy runner over the provided local replicated stores.
    pub fn new(
        stores: SyncStores,
        root_schema: RootSchemaState,
        attachment_sync_notify: Option<Arc<Notify>>,
        master_key_replication_notify: Option<Arc<Notify>>,
    ) -> Self {
        Self {
            stores,
            root_schema,
            gc_progress: SyncGcProgress::new(),
            attachment_sync_notify,
            master_key_replication_notify,
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
    ) -> bool {
        self.sync_selected_domains(
            sync_cap,
            cluster_view,
            root_schema_version,
            &ALL_DOMAINS,
            trace,
        )
        .await
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
    ) -> bool {
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
                master_key_replication_notify: self.master_key_replication_notify.clone(),
            },
        )
        .await
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
    expected_scope: SyncAttemptScope,
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
            expected_scope: SyncAttemptScope::new(expected_view, expected_root_schema_version),
        }
    }

    /// Applies one decoded delta chunk to the store selected by the chunk domain.
    async fn apply_decoded_chunk(
        &self,
        domain: Domain,
        registers: EncodedRegisters,
        tombstones: EncodedTombstones,
    ) -> Result<(), capnp::Error> {
        self.stores
            .require(domain)
            .map_err(to_capnp)?
            .store
            .apply_delta_encoded(registers, tombstones)
            .await
            .map_err(to_capnp)
    }
}

impl delta_sink::Server for DeltaSinkImpl {
    /// Accepts one streamed delta chunk from the remote peer.
    ///
    /// The header is checked before decoding and applying rows, so a peer cannot mix chunks from
    /// another view or root schema into this local sync attempt.
    async fn push_chunk(
        self: Rc<Self>,
        params: delta_sink::PushChunkParams,
    ) -> Result<(), capnp::Error> {
        let chunk = params.get()?.get_chunk()?;
        let chunk_header = encoding::decode_delta_chunk_header(
            &chunk,
            self.expected_scope.cluster_view,
            self.expected_scope.root_schema_version,
        )?;
        debug!(
            target: "delta",
            cluster_view = %chunk_header.cluster_view,
            domain = ?chunk_header.domain,
            root_schema_version = self.expected_scope.root_schema_version,
            "received delta chunk"
        );

        let registers = encoding::collect_registers(&chunk)?;
        let tombstones = encoding::collect_tombstones(&chunk)?;
        self.apply_decoded_chunk(chunk_header.domain, registers, tombstones)
            .await
    }

    /// Marks the end of one remote delta stream.
    async fn end(
        self: Rc<Self>,
        _params: delta_sink::EndParams,
        _results: delta_sink::EndResults,
    ) -> Result<(), capnp::Error> {
        debug!(target: "delta", "delta stream end");
        Ok(())
    }
}

/// Normalizes storage/runtime errors into Cap'n Proto failures for RPC propagation.
fn to_capnp<E: std::fmt::Display>(e: E) -> capnp::Error {
    capnp::Error::failed(e.to_string())
}

/// Runs anti-entropy for one caller-selected domain subset against one peer view.
///
/// The public runner owns the local store handles. This helper borrows them so tests and topology
/// can drive the same sync path without rebuilding a registry for every peer attempt.
async fn sync_selected_domains_with_stores(
    stores: &SyncStores,
    sync_cap: sync::Client,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    domains: &[Domain],
    context: SyncClientContext,
) -> bool {
    if domains.is_empty() {
        return true;
    }

    let requested_domains = domains.to_vec();
    let res = run_selected_domain_sync(
        stores,
        &sync_cap,
        cluster_view,
        root_schema_version,
        &requested_domains,
        &context,
    )
    .await;

    if let Err(e) = res {
        log_sync_failure(
            &e,
            cluster_view,
            requested_domains.len(),
            context.trace.as_ref(),
        );
        false
    } else {
        true
    }
}

/// Logs the generic sync failure and, when available, the peer-scoped diagnostic record.
fn log_sync_failure(
    error: &capnp::Error,
    cluster_view: ClusterViewId,
    requested_domain_count: usize,
    trace: Option<&SyncTraceContext>,
) {
    warn!(
        target: "sync",
        cluster_view = %cluster_view,
        domains_requested = requested_domain_count,
        "sync_selected_domains error: {error}"
    );
    if let Some(ctx) = trace {
        warn!(
            target: "diag.sync.peer",
            cluster_view = %cluster_view,
            peer = %ctx.peer_id,
            addr = %ctx.peer_addr,
            reason = %ctx.reason,
            disconnected = is_disconnected_capnp(error),
            error = %error,
            "peer-scoped sync_selected_domains failure"
        );
    }
}

/// Runs the root, range-summary, and delta phases for one peer-scoped sync attempt.
async fn run_selected_domain_sync(
    stores: &SyncStores,
    sync_cap: &sync::Client,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    requested_domains: &[Domain],
    context: &SyncClientContext,
) -> Result<(), capnp::Error> {
    let scope = SyncAttemptScope::new(cluster_view, root_schema_version);

    // Phase 1: compare roots. Matching roots are already converged for this peer and schema.
    // Mismatches move to the range phase.
    let remote_roots = request_remote_domain_roots(sync_cap, scope).await?;
    let domains_with_different_roots =
        find_domains_with_different_roots(stores, requested_domains, &remote_roots, scope, context)
            .await?;
    if domains_with_different_roots.is_empty() {
        return Ok(());
    }

    // Phase 2: ask only for range summaries where roots differed. The response lets this node
    // compute the exact pages it is missing.
    let delta_requests = request_ranges_and_compute_delta_wants(
        stores,
        sync_cap,
        scope,
        &domains_with_different_roots,
    )
    .await?;
    if delta_requests.is_empty() {
        return Ok(());
    }

    // Phase 3: open a local sink, let the remote peer stream missing rows into it, then wake
    // reconcilers that depend on newly applied domains.
    open_remote_delta_stream(
        stores,
        sync_cap,
        scope,
        requested_domains.len(),
        &delta_requests,
    )
    .await?;
    notify_delta_side_effects(&delta_requests, context);
    Ok(())
}

/// Fetches and decodes the peer's per-domain root digests for the requested view.
async fn request_remote_domain_roots(
    sync_cap: &sync::Client,
    scope: SyncAttemptScope,
) -> Result<Vec<RemoteDomainRoot>, capnp::Error> {
    let mut roots_request = sync_cap.get_roots_for_view_request();
    encoding::encode_view_request(
        roots_request.get().init_req(),
        scope.cluster_view,
        scope.root_schema_version,
    );

    let roots_response = roots_request.send().promise.await?;
    encoding::decode_remote_domain_roots(
        roots_response.get()?.get_roots()?,
        scope.cluster_view,
        scope.root_schema_version,
    )
}

/// Compares remote roots with local roots and returns domains that still need range summaries.
///
/// This is the only root-phase step with local side effects. Remote tombstone prune frontiers are
/// applied before comparing roots, and equal roots are recorded for distributed store GC.
async fn find_domains_with_different_roots(
    stores: &SyncStores,
    requested_domains: &[Domain],
    remote_roots: &[RemoteDomainRoot],
    scope: SyncAttemptScope,
    context: &SyncClientContext,
) -> Result<Vec<Domain>, capnp::Error> {
    let mut domains_requiring_ranges = Vec::new();

    for domain in requested_domains {
        let remote_root = remote_roots
            .iter()
            .find(|candidate| candidate.domain == *domain);

        // Prune-frontier propagation is attached to roots because equal roots are the moment when
        // peers can safely learn each other's tombstone GC progress.
        if let Some(remote_root) = remote_root
            && !remote_root.prune_frontiers.is_empty()
        {
            stores
                .apply_tombstone_prune_frontiers(*domain, remote_root.prune_frontiers.clone())
                .await
                .map_err(to_capnp)?;
        }

        let local_root_digest = stores
            .root_digest_at_version(*domain, scope.root_schema_version)
            .await
            .map_err(to_capnp)?;

        match remote_root.map(|root| root.digest) {
            Some(remote_root_digest) if remote_root_digest == local_root_digest => {
                record_equal_domain_root(*domain, scope, local_root_digest, context);
            }
            Some(_) | None => domains_requiring_ranges.push(*domain),
        }
    }

    Ok(domains_requiring_ranges)
}

/// Records one equal-root observation for GC when peer trace context is available.
fn record_equal_domain_root(
    domain: Domain,
    scope: SyncAttemptScope,
    root_digest: [u8; 16],
    context: &SyncClientContext,
) {
    if let Some(trace) = context.trace.as_ref() {
        context.gc_progress.record_equal_root_now(
            trace.peer_id,
            domain,
            scope.cluster_view,
            scope.root_schema_version,
            root_digest,
        );
    }
}

/// Fetches remote range summaries and computes the exact local wants.
///
/// The result is the list of per-domain page ranges this node wants the remote peer to stream back
/// through `DeltaSinkImpl`.
async fn request_ranges_and_compute_delta_wants(
    stores: &SyncStores,
    sync_cap: &sync::Client,
    scope: SyncAttemptScope,
    domains_requiring_ranges: &[Domain],
) -> Result<Vec<DomainDeltaRequest>, capnp::Error> {
    let mut ranges_request = sync_cap.get_ranges_for_view_request();
    encoding::encode_view_ranges_request(
        ranges_request.get().init_req(),
        scope.cluster_view,
        scope.root_schema_version,
        domains_requiring_ranges,
    );

    let ranges_response = ranges_request.send().promise.await?;
    compute_delta_wants_from_range_response(stores, ranges_response.get()?.get_ranges()?, scope)
        .await
}

/// Decodes each range summary and computes missing pages in the original response order.
async fn compute_delta_wants_from_range_response(
    stores: &SyncStores,
    ranges_reader: capnp::struct_list::Reader<'_, sync::domain_range_summary::Owned>,
    scope: SyncAttemptScope,
) -> Result<Vec<DomainDeltaRequest>, capnp::Error> {
    let mut delta_requests = Vec::new();
    for index in 0..ranges_reader.len() {
        let remote_summary = encoding::decode_remote_domain_range_summary(
            ranges_reader.get(index),
            scope.cluster_view,
            scope.root_schema_version,
        )?;
        if let Some(delta_request) =
            compute_delta_want_for_remote_summary(stores, scope, remote_summary).await?
        {
            delta_requests.push(delta_request);
        }
    }
    Ok(delta_requests)
}

/// Computes the missing local pages for one decoded remote range summary.
async fn compute_delta_want_for_remote_summary(
    stores: &SyncStores,
    scope: SyncAttemptScope,
    remote_summary: RemoteDomainRangeSummary,
) -> Result<Option<DomainDeltaRequest>, capnp::Error> {
    let local_ranges = stores
        .page_range_summary_at_version(remote_summary.domain, scope.root_schema_version)
        .await
        .map_err(to_capnp)?;
    let want_ranges = compute_want_from_have(&remote_summary.ranges, &local_ranges);
    if want_ranges.is_empty() {
        return Ok(None);
    }

    Ok(Some(DomainDeltaRequest {
        domain: remote_summary.domain,
        want_ranges,
    }))
}

/// Opens the selective delta stream and feeds remote chunks into a local sink.
async fn open_remote_delta_stream(
    stores: &SyncStores,
    sync_cap: &sync::Client,
    scope: SyncAttemptScope,
    requested_domain_count: usize,
    delta_requests: &[DomainDeltaRequest],
) -> Result<(), capnp::Error> {
    let sink_client = new_client(DeltaSinkImpl::new(
        stores.clone(),
        scope.cluster_view,
        scope.root_schema_version,
    ));

    let mut open_delta_request = sync_cap.open_delta_for_view_request();
    encoding::encode_open_delta_request(
        open_delta_request.get().init_req(),
        scope.cluster_view,
        scope.root_schema_version,
        delta_requests,
        sink_client,
    )?;

    debug!(
        target: "sync",
        cluster_view = %scope.cluster_view,
        domains_requested = requested_domain_count,
        domains_with_delta = delta_requests.len(),
        "opening selective delta stream"
    );
    open_delta_request.send().promise.await?;
    Ok(())
}

/// Wakes local reconcilers for domains whose deltas have just been applied.
fn notify_delta_side_effects(delta_requests: &[DomainDeltaRequest], context: &SyncClientContext) {
    if should_notify_network_attachment_sync(delta_requests)
        && let Some(notify) = context.attachment_sync_notify.as_ref()
    {
        // Remote nodes otherwise only notice replicated attachment changes on the slow
        // attachment-refresh poll. Wake the network controller immediately so forwarding
        // catches up as soon as anti-entropy applies the attachment delta locally.
        notify.notify_one();
    }
    if should_notify_master_key_replication(delta_requests)
        && let Some(notify) = context.master_key_replication_notify.as_ref()
    {
        // Master-key grants are correctness-critical. Anti-entropy is the
        // repair path for missed gossip, so wake the reconciler as soon as
        // this domain receives deltas instead of waiting for another tick.
        notify.notify_one();
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
fn should_notify_network_attachment_sync(delta_requests: &[DomainDeltaRequest]) -> bool {
    delta_requests
        .iter()
        .any(|request| request.domain == Domain::NetworkAttachments)
}

/// Returns true when a completed delta stream included secret master-key grants.
fn should_notify_master_key_replication(delta_requests: &[DomainDeltaRequest]) -> bool {
    delta_requests
        .iter()
        .any(|request| request.domain == Domain::SecretMasterKeys)
}

#[cfg(test)]
mod tests {
    use super::{
        DomainDeltaRequest, should_notify_master_key_replication,
        should_notify_network_attachment_sync,
    };
    use mantissa_protocol::sync::Domain;
    use mantissa_store::PageDigestRange;

    /// Builds a test delta request for notification predicate coverage.
    fn delta_request_for(domain: Domain) -> DomainDeltaRequest {
        DomainDeltaRequest {
            domain,
            want_ranges: Vec::<PageDigestRange>::new(),
        }
    }

    /// Attachment deltas should wake the network controller on the receiving node immediately.
    #[test]
    fn attachment_domain_requests_forwarding_refresh_notification() {
        let wants = vec![delta_request_for(Domain::NetworkAttachments)];

        assert!(should_notify_network_attachment_sync(&wants));
    }

    /// Non-attachment deltas must not trigger unnecessary forwarding refresh work.
    #[test]
    fn non_attachment_domains_skip_forwarding_refresh_notification() {
        let wants = vec![
            delta_request_for(Domain::Workloads),
            delta_request_for(Domain::Services),
        ];

        assert!(!should_notify_network_attachment_sync(&wants));
    }

    /// Master-key deltas should wake the grant reconciler immediately.
    #[test]
    fn master_key_domain_requests_reconciler_notification() {
        let wants = vec![delta_request_for(Domain::SecretMasterKeys)];

        assert!(should_notify_master_key_replication(&wants));
    }

    /// Non-master-key deltas must not trigger unnecessary reconciler work.
    #[test]
    fn non_master_key_domains_skip_reconciler_notification() {
        let wants = vec![delta_request_for(Domain::Secrets)];

        assert!(!should_notify_master_key_replication(&wants));
    }
}
