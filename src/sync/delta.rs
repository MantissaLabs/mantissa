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
use mantissa_store::{PageDigestRange, RowDigest, compute_want_from_have};
use std::ops::Range;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::{debug, warn};

/// Maximum semantic row digests carried by one phase-three request.
///
/// Replicated row keys and digests are both 16 bytes. This keeps the digest payload well below the
/// Cap'n Proto message limit while matching the existing maximum delta rows per response chunk.
const DELTA_REQUEST_MAX_ROW_DIGESTS: usize = 8192;
/// Maximum per-domain wants carried by one phase-three request.
const DELTA_REQUEST_MAX_WANTS: usize = 256;

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
    gc_progress_view: ClusterViewId,
    attachment_sync_notify: Option<Arc<Notify>>,
    network_demand_sync_notify: Option<Arc<Notify>>,
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
    network_demand_sync_notify: Option<Arc<Notify>>,
    master_key_replication_notify: Option<Arc<Notify>>,
}

impl SyncRunner {
    /// Builds one anti-entropy runner over the provided local replicated stores.
    pub fn new(
        stores: SyncStores,
        root_schema: RootSchemaState,
        attachment_sync_notify: Option<Arc<Notify>>,
        network_demand_sync_notify: Option<Arc<Notify>>,
        master_key_replication_notify: Option<Arc<Notify>>,
    ) -> Self {
        Self {
            stores,
            root_schema,
            gc_progress: SyncGcProgress::new(),
            attachment_sync_notify,
            network_demand_sync_notify,
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
    /// This is used by the global metadata loop to sync lightweight cluster metadata across split
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
                gc_progress_view: cluster_view,
                attachment_sync_notify: self.attachment_sync_notify.clone(),
                network_demand_sync_notify: self.network_demand_sync_notify.clone(),
                master_key_replication_notify: self.master_key_replication_notify.clone(),
            },
        )
        .await
    }

    /// Runs cluster-wide anti-entropy while validating requests against the peer's active view.
    ///
    /// The wire view and local GC-progress view are deliberately distinct. A split peer must
    /// receive requests for its own view, while the caller records convergence against its local
    /// view so one cluster-wide GC frontier can include peers from every split partition.
    pub async fn sync_cluster_wide_domains(
        &self,
        sync_cap: sync::Client,
        peer_view: ClusterViewId,
        local_view: ClusterViewId,
        root_schema_version: u32,
        trace: Option<SyncTraceContext>,
    ) -> bool {
        sync_selected_domains_with_stores(
            &self.stores,
            sync_cap,
            peer_view,
            root_schema_version,
            &super::CLUSTER_WIDE_DOMAINS,
            SyncClientContext {
                trace,
                gc_progress: self.gc_progress.clone(),
                gc_progress_view: local_view,
                attachment_sync_notify: self.attachment_sync_notify.clone(),
                network_demand_sync_notify: self.network_demand_sync_notify.clone(),
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

    // Phase 3: open one local sink per bounded request, let the remote peer stream missing rows
    // into it, then wake reconcilers that depend on newly applied domains.
    open_remote_delta_streams(
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
            context.gc_progress_view,
            scope.root_schema_version,
            root_digest,
        );
    }
}

/// Fetches remote page summaries and computes the exact local key ranges to repair.
///
/// The result is the list of per-domain key ranges this node wants the remote peer to stream back
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

/// Decodes each range summary and computes differing key ranges in the original response order.
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
        delta_requests
            .extend(compute_delta_wants_for_remote_summary(stores, scope, remote_summary).await?);
    }
    Ok(delta_requests)
}

/// Computes bounded remote key-range requests for one differing local domain.
async fn compute_delta_wants_for_remote_summary(
    stores: &SyncStores,
    scope: SyncAttemptScope,
    remote_summary: RemoteDomainRangeSummary,
) -> Result<Vec<DomainDeltaRequest>, capnp::Error> {
    let local_ranges = stores
        .page_range_summary_at_version(remote_summary.domain, scope.root_schema_version)
        .await
        .map_err(to_capnp)?;
    let want_ranges =
        compute_want_from_have(&remote_summary.ranges, &local_ranges).map_err(to_capnp)?;
    if want_ranges.is_empty() {
        return Ok(Vec::new());
    }
    let have_rows = stores
        .row_digests_for_ranges(
            remote_summary.domain,
            &want_ranges,
            scope.root_schema_version,
        )
        .await
        .map_err(to_capnp)?;

    bounded_domain_delta_requests(
        remote_summary.domain,
        want_ranges,
        have_rows,
        DELTA_REQUEST_MAX_ROW_DIGESTS,
    )
}

/// Splits one domain want into requests whose row digests fit the phase-three count limit.
///
/// Every replicated domain uses fixed-width UUID row keys. Splitting immediately after the last
/// advertised key therefore creates disjoint inclusive ranges without losing remote-only keys
/// between two local rows.
fn bounded_domain_delta_requests(
    domain: Domain,
    mut want_ranges: Vec<PageDigestRange>,
    mut have_rows: Vec<RowDigest>,
    max_row_digests: usize,
) -> Result<Vec<DomainDeltaRequest>, capnp::Error> {
    if max_row_digests == 0 {
        return Err(capnp::Error::failed(
            "delta request row-digest limit must be greater than zero".to_string(),
        ));
    }

    // `diff()` returns non-overlapping ranges. Sorting both inputs lets one linear pass move each
    // local digest into its requested interval without cloning it.
    want_ranges.sort_by(|left, right| left.start.cmp(&right.start).then(left.end.cmp(&right.end)));
    have_rows.sort_by(|left, right| left.key.cmp(&right.key));
    have_rows.dedup_by(|left, right| left.key == right.key);

    let mut rows = have_rows.into_iter().peekable();
    let mut requests = Vec::new();
    for mut range in want_ranges {
        if range.start > range.end {
            return Err(capnp::Error::failed(
                "invalid delta request range: start exceeds end".to_string(),
            ));
        }
        range.hash.clear();
        while rows.peek().is_some_and(|row| row.key < range.start) {
            rows.next();
        }

        let mut range_rows = Vec::new();
        while rows.peek().is_some_and(|row| row.key <= range.end) {
            if let Some(row) = rows.next() {
                range_rows.push(row);
            }
        }
        split_range_by_digest_limit(domain, range, range_rows, max_row_digests, &mut requests)?;
    }
    Ok(requests)
}

/// Divides one inclusive key range without exceeding the row-digest count limit.
fn split_range_by_digest_limit(
    domain: Domain,
    range: PageDigestRange,
    rows: Vec<RowDigest>,
    max_row_digests: usize,
    requests: &mut Vec<DomainDeltaRequest>,
) -> Result<(), capnp::Error> {
    if rows.is_empty() {
        requests.push(DomainDeltaRequest {
            domain,
            want_ranges: vec![range],
            have_rows: Vec::new(),
        });
        return Ok(());
    }

    let range_end = range.end;
    let mut fragment_start = range.start;
    let mut rows = rows.into_iter().peekable();
    while rows.peek().is_some() {
        let fragment_rows = rows.by_ref().take(max_row_digests).collect::<Vec<_>>();

        let fragment_end = if rows.peek().is_some() {
            fragment_rows
                .last()
                .map(|row| row.key.clone())
                .ok_or_else(|| capnp::Error::failed("empty delta request fragment".to_string()))?
        } else {
            range_end.clone()
        };
        requests.push(DomainDeltaRequest {
            domain,
            want_ranges: vec![PageDigestRange {
                start: fragment_start.clone(),
                end: fragment_end.clone(),
                hash: Vec::new(),
            }],
            have_rows: fragment_rows,
        });

        if rows.peek().is_some() {
            fragment_start = next_fixed_width_key(&fragment_end).ok_or_else(|| {
                capnp::Error::failed("delta request key range cannot advance".to_string())
            })?;
        }
    }
    Ok(())
}

/// Returns the next key in the fixed-width big-endian ordering used by replicated UUID rows.
fn next_fixed_width_key(key: &[u8]) -> Option<Vec<u8>> {
    let mut next = key.to_vec();
    for index in (0..next.len()).rev() {
        if next[index] == u8::MAX {
            next[index] = 0;
            continue;
        }
        next[index] = next[index].saturating_add(1);
        return Some(next);
    }
    None
}

/// Groups bounded domain wants into phase-three RPC slices under the count limits.
fn delta_request_batches(
    requests: &[DomainDeltaRequest],
    max_row_digests: usize,
    max_wants: usize,
) -> Vec<Range<usize>> {
    let mut batches = Vec::new();
    let mut start = 0usize;
    while start < requests.len() {
        let mut end = start;
        let mut row_digests = 0usize;
        while end < requests.len() {
            let next_row_digests = requests[end].have_rows.len();
            if end > start
                && (end.saturating_sub(start) >= max_wants
                    || row_digests.saturating_add(next_row_digests) > max_row_digests)
            {
                break;
            }
            row_digests = row_digests.saturating_add(next_row_digests);
            end = end.saturating_add(1);
        }
        batches.push(start..end);
        start = end;
    }
    batches
}

/// Opens the selective delta streams and feeds remote chunks into independent local sinks.
async fn open_remote_delta_streams(
    stores: &SyncStores,
    sync_cap: &sync::Client,
    scope: SyncAttemptScope,
    requested_domain_count: usize,
    delta_requests: &[DomainDeltaRequest],
) -> Result<(), capnp::Error> {
    run_delta_request_batches(
        sync_cap,
        scope,
        requested_domain_count,
        delta_requests,
        || {
            new_client(DeltaSinkImpl::new(
                stores.clone(),
                scope.cluster_view,
                scope.root_schema_version,
            ))
        },
    )
    .await
}

/// Sends bounded phase-three requests with an independent sink for every request batch.
///
/// `DeltaSink::end()` closes one stream, so a sink cannot be reused by a later
/// `openDeltaForView` call. The factory keeps that lifecycle rule explicit and gives tests a way
/// to verify it without constructing every replicated store.
async fn run_delta_request_batches<F>(
    sync_cap: &sync::Client,
    scope: SyncAttemptScope,
    requested_domain_count: usize,
    delta_requests: &[DomainDeltaRequest],
    create_sink: F,
) -> Result<(), capnp::Error>
where
    F: Fn() -> delta_sink::Client,
{
    let batches = delta_request_batches(
        delta_requests,
        DELTA_REQUEST_MAX_ROW_DIGESTS,
        DELTA_REQUEST_MAX_WANTS,
    );
    debug!(
        target: "sync",
        cluster_view = %scope.cluster_view,
        domains_requested = requested_domain_count,
        delta_wants = delta_requests.len(),
        request_batches = batches.len(),
        "opening selective delta streams"
    );

    for batch in batches {
        // Each RPC is one complete stream. Its server calls `end()` before the request resolves,
        // so the next batch must receive a newly created sink capability.
        let sink_client = create_sink();
        let mut open_delta_request = sync_cap.open_delta_for_view_request();
        encoding::encode_open_delta_request(
            open_delta_request.get().init_req(),
            scope.cluster_view,
            scope.root_schema_version,
            &delta_requests[batch],
            sink_client,
        )?;
        open_delta_request.send().promise.await?;
    }
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
    if should_notify_network_demand_sync(delta_requests)
        && let Some(notify) = context.network_demand_sync_notify.as_ref()
    {
        // These domains feed ingress-pool selection or service public-ingress demand. Wake the
        // network controller immediately so on-demand realization does not wait for a slow drift
        // pass after anti-entropy applies the rows locally.
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

/// Returns true when a completed delta stream changed derived on-demand network demand.
///
/// Service rows define which public-ingress networks exist, ingress-pool rows define which nodes
/// may proxy them, and peer rows carry the labels/readiness used by pool placement.
fn should_notify_network_demand_sync(delta_requests: &[DomainDeltaRequest]) -> bool {
    delta_requests.iter().any(|request| {
        matches!(
            request.domain,
            Domain::Services | Domain::IngressPools | Domain::Peers
        )
    })
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
        DELTA_REQUEST_MAX_WANTS, DomainDeltaRequest, SyncAttemptScope, SyncClientContext,
        bounded_domain_delta_requests, delta_request_batches, record_equal_domain_root,
        run_delta_request_batches, should_notify_master_key_replication,
        should_notify_network_attachment_sync, should_notify_network_demand_sync,
    };
    use crate::cluster::{ClusterId, ClusterViewId};
    use crate::sync::{SyncGcProgress, SyncTraceContext};
    use capnp_rpc::new_client;
    use mantissa_protocol::sync::{Domain, delta_sink, sync};
    use mantissa_store::{PageDigestRange, RowDigest};
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    /// Test sink that records completion of one phase-three stream.
    struct TrackedDeltaSink {
        push_count: Rc<Cell<usize>>,
        end_count: Rc<Cell<usize>>,
    }

    impl delta_sink::Server for TrackedDeltaSink {
        /// Records delivery of the synthetic empty chunk used by the lifecycle test.
        async fn push_chunk(
            self: Rc<Self>,
            _params: delta_sink::PushChunkParams,
        ) -> Result<(), capnp::Error> {
            self.push_count.set(self.push_count.get().saturating_add(1));
            Ok(())
        }

        /// Records that the server closed this sink's stream.
        async fn end(
            self: Rc<Self>,
            _params: delta_sink::EndParams,
            _results: delta_sink::EndResults,
        ) -> Result<(), capnp::Error> {
            self.end_count.set(self.end_count.get().saturating_add(1));
            Ok(())
        }
    }

    /// Test sync server that records the sink capability received by each phase-three request.
    struct RecordingSyncService {
        sinks: Rc<RefCell<Vec<delta_sink::Client>>>,
        want_counts: Rc<RefCell<Vec<u32>>>,
    }

    impl sync::Server for RecordingSyncService {
        /// Closes each received stream after recording its capability and request size.
        async fn open_delta_for_view(
            self: Rc<Self>,
            params: sync::OpenDeltaForViewParams,
            _results: sync::OpenDeltaForViewResults,
        ) -> Result<(), capnp::Error> {
            let (sink, want_count) = {
                let request = params.get()?.get_req()?;
                (request.get_sink()?, request.get_wants()?.len())
            };
            // Retain the capabilities until the test compares them. Otherwise the allocator may
            // reuse the first sink's address after its request completes.
            self.sinks.borrow_mut().push(sink.clone());
            self.want_counts.borrow_mut().push(want_count);
            let mut push_request = sink.push_chunk_request();
            push_request.get().init_chunk();
            push_request.send().await?;
            sink.end_request().send().promise.await?;
            Ok(())
        }
    }

    /// Builds a test delta request for notification predicate coverage.
    fn delta_request_for(domain: Domain) -> DomainDeltaRequest {
        DomainDeltaRequest {
            domain,
            want_ranges: Vec::<PageDigestRange>::new(),
            have_rows: Vec::new(),
        }
    }

    /// Large row-digest lists should become bounded, disjoint requests with complete key coverage.
    #[test]
    fn row_digest_requests_are_split_without_range_gaps() {
        let max_row_digests = 3;
        let range = PageDigestRange {
            start: vec![0],
            end: vec![u8::MAX],
            hash: Vec::new(),
        };
        let rows = (1..=20u8)
            .map(|key| RowDigest {
                key: vec![key],
                digest: [key; 16],
            })
            .collect();

        let requests =
            bounded_domain_delta_requests(Domain::Workloads, vec![range], rows, max_row_digests)
                .expect("bounded delta requests");

        assert!(requests.len() > 1);
        let mut previous_end = None;
        let mut advertised_keys = Vec::new();
        for request in &requests {
            assert_eq!(request.want_ranges.len(), 1);
            assert!(request.have_rows.len() <= max_row_digests);
            let range = &request.want_ranges[0];
            if let Some(previous_end) = previous_end {
                assert_eq!(range.start, vec![previous_end + 1]);
            } else {
                assert_eq!(range.start, vec![0]);
            }
            previous_end = range.end.first().copied();
            advertised_keys.extend(request.have_rows.iter().map(|row| row.key.clone()));
        }
        assert_eq!(previous_end, Some(u8::MAX));
        assert_eq!(
            advertised_keys,
            (1..=20u8).map(|key| vec![key]).collect::<Vec<_>>()
        );

        for batch in delta_request_batches(&requests, max_row_digests, 256) {
            let digest_count = requests[batch]
                .iter()
                .map(|request| request.have_rows.len())
                .sum::<usize>();
            assert!(digest_count <= max_row_digests);
        }
    }

    /// Range splitting should carry into the previous byte without leaving a key gap.
    #[test]
    fn fixed_width_key_increment_carries() {
        assert_eq!(super::next_fixed_width_key(&[1, u8::MAX]), Some(vec![2, 0]));
        assert_eq!(super::next_fixed_width_key(&[u8::MAX, u8::MAX]), None);
    }

    /// An empty requester should keep the complete differing range and send no row digests.
    #[test]
    fn empty_requester_keeps_complete_delta_range() {
        let range = PageDigestRange {
            start: vec![1],
            end: vec![200],
            hash: Vec::new(),
        };

        let requests =
            bounded_domain_delta_requests(Domain::Services, vec![range.clone()], Vec::new(), 3)
                .expect("empty requester delta request");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].want_ranges, vec![range]);
        assert!(requests[0].have_rows.is_empty());
    }

    /// Small per-domain wants should continue sharing one phase-three RPC.
    #[test]
    fn small_delta_wants_share_one_request_batch() {
        let requests = vec![
            delta_request_for(Domain::Workloads),
            delta_request_for(Domain::Services),
            delta_request_for(Domain::Peers),
        ];

        let batches = delta_request_batches(&requests, 8192, 256);

        assert_eq!(batches, vec![0..3]);
    }

    /// Every phase-three RPC should own a distinct sink that the server closes exactly once.
    #[tokio::test(flavor = "current_thread")]
    async fn multiple_delta_request_batches_use_independent_sinks() {
        let requests = (0..=DELTA_REQUEST_MAX_WANTS)
            .map(|_| delta_request_for(Domain::Workloads))
            .collect::<Vec<_>>();
        let sinks = Rc::new(RefCell::new(Vec::new()));
        let want_counts = Rc::new(RefCell::new(Vec::new()));
        let push_count = Rc::new(Cell::new(0usize));
        let end_count = Rc::new(Cell::new(0usize));
        let created_sink_count = Rc::new(Cell::new(0usize));
        let sync_client: sync::Client = new_client(RecordingSyncService {
            sinks: sinks.clone(),
            want_counts: want_counts.clone(),
        });

        run_delta_request_batches(
            &sync_client,
            SyncAttemptScope::new(ClusterViewId::legacy_default(), 1),
            1,
            &requests,
            || {
                created_sink_count.set(created_sink_count.get().saturating_add(1));
                new_client(TrackedDeltaSink {
                    push_count: push_count.clone(),
                    end_count: end_count.clone(),
                })
            },
        )
        .await
        .expect("send multiple delta request batches");

        assert_eq!(want_counts.borrow().as_slice(), &[256, 1]);
        assert_eq!(created_sink_count.get(), 2);
        assert_eq!(push_count.get(), 2);
        assert_eq!(end_count.get(), 2);
        let sinks = sinks.borrow();
        assert_eq!(sinks.len(), 2);
        assert_ne!(
            sinks[0].client.hook.get_ptr(),
            sinks[1].client.hook.get_ptr()
        );
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

    /// Ingress-pool demand domains should wake on-demand network realization.
    #[test]
    fn ingress_demand_domains_request_network_demand_notification() {
        for domain in [Domain::Services, Domain::IngressPools, Domain::Peers] {
            let wants = vec![delta_request_for(domain)];

            assert!(should_notify_network_demand_sync(&wants));
        }
    }

    /// Unrelated domains must not trigger ingress-pool demand recomputation.
    #[test]
    fn unrelated_domains_skip_network_demand_notification() {
        let wants = vec![
            delta_request_for(Domain::Workloads),
            delta_request_for(Domain::NetworkAttachments),
        ];

        assert!(!should_notify_network_demand_sync(&wants));
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

    /// Cross-view wire validation must record global convergence under the caller's local view.
    #[test]
    fn equal_global_root_uses_local_gc_progress_view() {
        let peer_id = uuid::Uuid::from_u128(1);
        let peer_view = ClusterViewId::new(ClusterId::from_uuid(uuid::Uuid::from_u128(2)), 3);
        let local_view = ClusterViewId::new(ClusterId::from_uuid(uuid::Uuid::from_u128(4)), 5);
        let progress = SyncGcProgress::new();
        let context = SyncClientContext {
            trace: Some(SyncTraceContext::peer(peer_id, "inproc", "test")),
            gc_progress: progress.clone(),
            gc_progress_view: local_view,
            attachment_sync_notify: None,
            network_demand_sync_notify: None,
            master_key_replication_notify: None,
        };

        record_equal_domain_root(
            Domain::SecretMasterKeys,
            SyncAttemptScope::new(peer_view, 1),
            [7; 16],
            &context,
        );

        assert!(
            progress
                .last_equal_at(peer_id, Domain::SecretMasterKeys, local_view, 1)
                .is_some()
        );
        assert_eq!(
            progress.last_equal_at(peer_id, Domain::SecretMasterKeys, peer_view, 1),
            None
        );
    }
}
