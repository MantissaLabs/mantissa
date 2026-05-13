//! Client side of Mantissa's anti-entropy protocol.
//!
//! This module drives the roots -> ranges -> delta handshake against a remote `Sync`
//! capability and exposes `DeltaSinkImpl`, the local sink used by the remote peer to
//! stream missing CRDT fragments back into the local stores.

use super::{ALL_DOMAINS, SyncStores};
use crate::cluster::{ClusterViewId, RootSchemaState};
use crate::store::replicated::registry::{EncodedRegisters, EncodedTombstones};
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

/// Range-summary payload decoded from the peer before local want computation.
struct RemoteDomainRangeSummary {
    domain: Domain,
    ranges: Vec<PageDigestRange>,
}

/// Per-domain page ranges this node wants the peer to stream through `DeltaSink`.
struct DomainDeltaRequest {
    domain: Domain,
    want_ranges: Vec<PageDigestRange>,
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
    master_key_replication_notify: Option<Arc<Notify>>,
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
        false
    } else {
        true
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
    // Step 1: compare remote roots against local roots and skip converged domains.
    let remote_roots =
        request_remote_domain_roots(sync_cap, cluster_view, root_schema_version).await?;
    let domains_requiring_ranges = select_domains_requiring_range_sync(
        stores,
        requested_domains,
        &remote_roots,
        cluster_view,
        root_schema_version,
        context,
    )
    .await?;
    if domains_requiring_ranges.is_empty() {
        return Ok(());
    }

    // Step 2: fetch range summaries only for domains whose root digest differs.
    let delta_requests = request_missing_delta_ranges(
        stores,
        sync_cap,
        cluster_view,
        root_schema_version,
        &domains_requiring_ranges,
    )
    .await?;
    if delta_requests.is_empty() {
        return Ok(());
    }

    // Step 3: stream the missing pages into a local sink and wake dependent reconcilers.
    open_remote_delta_stream(
        stores,
        sync_cap,
        cluster_view,
        root_schema_version,
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
    cluster_view: ClusterViewId,
    root_schema_version: u32,
) -> Result<Vec<RemoteDomainRoot>, capnp::Error> {
    let mut roots_request = sync_cap.get_roots_for_view_request();
    encode_view_request(
        roots_request.get().init_req(),
        cluster_view,
        root_schema_version,
    );

    let roots_response = roots_request.send().promise.await?;
    decode_remote_domain_roots(
        roots_response.get()?.get_roots()?,
        cluster_view,
        root_schema_version,
    )
}

/// Encodes the shared view/root-schema selector used by root requests.
fn encode_view_request(
    mut request: sync::view_request::Builder<'_>,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
) {
    cluster_view.write_capnp(request.reborrow().init_view());
    request.set_root_schema_version(root_schema_version);
}

/// Decodes and validates the root response returned by `getRootsForView`.
fn decode_remote_domain_roots(
    roots_reader: capnp::struct_list::Reader<'_, sync::domain_root::Owned>,
    expected_view: ClusterViewId,
    expected_root_schema_version: u32,
) -> Result<Vec<RemoteDomainRoot>, capnp::Error> {
    let mut roots = Vec::with_capacity(roots_reader.len() as usize);
    for index in 0..roots_reader.len() {
        roots.push(decode_remote_domain_root(
            roots_reader.get(index),
            expected_view,
            expected_root_schema_version,
        )?);
    }
    Ok(roots)
}

/// Decodes one root entry and rejects responses scoped to a different view/schema.
fn decode_remote_domain_root(
    root_reader: sync::domain_root::Reader<'_>,
    expected_view: ClusterViewId,
    expected_root_schema_version: u32,
) -> Result<RemoteDomainRoot, capnp::Error> {
    let actual_view =
        ClusterViewId::from_capnp(root_reader.get_view()?).map_err(capnp::Error::failed)?;
    if actual_view != expected_view {
        return Err(capnp::Error::failed(format!(
            "sync roots view mismatch: expected {expected_view}, got {actual_view}"
        )));
    }

    let actual_root_schema_version = root_reader.get_root_schema_version();
    if actual_root_schema_version != expected_root_schema_version {
        return Err(capnp::Error::failed(format!(
            "sync roots root schema mismatch: expected {expected_root_schema_version}, got {actual_root_schema_version}"
        )));
    }

    let domain = root_reader
        .get_domain()
        .map_err(|_| capnp::Error::failed("unknown domain".into()))?;
    let digest = read_root_digest(root_reader.get_root_digest()?)?;
    let prune_frontiers =
        decode_tombstone_prune_frontiers(root_reader.get_tombstone_prune_frontiers()?)?;
    Ok(RemoteDomainRoot {
        domain,
        digest,
        prune_frontiers,
    })
}

/// Decodes peer tombstone prune-frontiers, ignoring the wire default sequence.
fn decode_tombstone_prune_frontiers(
    frontiers_reader: capnp::struct_list::Reader<'_, sync::tombstone_prune_frontier::Owned>,
) -> Result<TombstonePruneFrontiers, capnp::Error> {
    let mut prune_frontiers = Vec::with_capacity(frontiers_reader.len() as usize);
    for frontier in frontiers_reader.iter() {
        let origin_actor = frontier.get_origin_actor()?.to_vec();
        let sequence = frontier.get_sequence();
        if sequence > 0 {
            prune_frontiers.push((origin_actor, sequence));
        }
    }
    Ok(prune_frontiers)
}

/// Applies root-phase side effects and returns only domains that need range summaries.
async fn select_domains_requiring_range_sync(
    stores: &SyncStores,
    requested_domains: &[Domain],
    remote_roots: &[RemoteDomainRoot],
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    context: &SyncClientContext,
) -> Result<Vec<Domain>, capnp::Error> {
    let mut domains_requiring_ranges = Vec::new();

    for domain in requested_domains {
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

        let local_root_digest = stores
            .root_digest_at_version(*domain, root_schema_version)
            .await
            .map_err(to_capnp)?;

        match remote_root.map(|root| root.digest) {
            Some(remote_root_digest) if remote_root_digest == local_root_digest => {
                record_equal_domain_root(*domain, cluster_view, root_schema_version, context);
            }
            Some(_) | None => domains_requiring_ranges.push(*domain),
        }
    }

    Ok(domains_requiring_ranges)
}

/// Records one equal-root observation for GC when peer trace context is available.
fn record_equal_domain_root(
    domain: Domain,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    context: &SyncClientContext,
) {
    if let Some(trace) = context.trace.as_ref() {
        context.gc_progress.record_equal_root_now(
            trace.peer_id,
            domain,
            cluster_view,
            root_schema_version,
        );
    }
}

/// Fetches remote range summaries and computes the exact page ranges missing locally.
async fn request_missing_delta_ranges(
    stores: &SyncStores,
    sync_cap: &sync::Client,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    domains_requiring_ranges: &[Domain],
) -> Result<Vec<DomainDeltaRequest>, capnp::Error> {
    let mut ranges_request = sync_cap.get_ranges_for_view_request();
    encode_view_ranges_request(
        ranges_request.get().init_req(),
        cluster_view,
        root_schema_version,
        domains_requiring_ranges,
    );

    let ranges_response = ranges_request.send().promise.await?;
    compute_missing_delta_ranges_from_response(
        stores,
        ranges_response.get()?.get_ranges()?,
        cluster_view,
        root_schema_version,
    )
    .await
}

/// Encodes the view/root-schema selector plus the explicit domains to summarize.
fn encode_view_ranges_request(
    mut request: sync::view_ranges_request::Builder<'_>,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    domains: &[Domain],
) {
    cluster_view.write_capnp(request.reborrow().init_view());
    request.set_root_schema_version(root_schema_version);
    encode_domain_list(
        request.reborrow().init_domains(domains.len() as u32),
        domains,
    );
}

/// Encodes one domain enum list into an already initialized Cap'n Proto list.
fn encode_domain_list(mut list: capnp::enum_list::Builder<'_, Domain>, domains: &[Domain]) {
    for (index, domain) in domains.iter().enumerate() {
        list.set(index as u32, *domain);
    }
}

/// Decodes each range summary and computes missing pages in the original response order.
async fn compute_missing_delta_ranges_from_response(
    stores: &SyncStores,
    ranges_reader: capnp::struct_list::Reader<'_, sync::domain_range_summary::Owned>,
    expected_view: ClusterViewId,
    expected_root_schema_version: u32,
) -> Result<Vec<DomainDeltaRequest>, capnp::Error> {
    let mut delta_requests = Vec::new();
    for index in 0..ranges_reader.len() {
        let remote_summary = decode_remote_domain_range_summary(
            ranges_reader.get(index),
            expected_view,
            expected_root_schema_version,
        )?;
        if let Some(delta_request) =
            compute_missing_delta_range(stores, expected_root_schema_version, remote_summary)
                .await?
        {
            delta_requests.push(delta_request);
        }
    }
    Ok(delta_requests)
}

/// Decodes one range summary and rejects responses scoped to a different view/schema.
fn decode_remote_domain_range_summary(
    summary_reader: sync::domain_range_summary::Reader<'_>,
    expected_view: ClusterViewId,
    expected_root_schema_version: u32,
) -> Result<RemoteDomainRangeSummary, capnp::Error> {
    let actual_view =
        ClusterViewId::from_capnp(summary_reader.get_view()?).map_err(capnp::Error::failed)?;
    if actual_view != expected_view {
        return Err(capnp::Error::failed(format!(
            "sync ranges view mismatch: expected {expected_view}, got {actual_view}"
        )));
    }

    let actual_root_schema_version = summary_reader.get_root_schema_version();
    if actual_root_schema_version != expected_root_schema_version {
        return Err(capnp::Error::failed(format!(
            "sync ranges root schema mismatch: expected {expected_root_schema_version}, got {actual_root_schema_version}"
        )));
    }

    let domain = summary_reader
        .get_domain()
        .map_err(|_| capnp::Error::failed("unknown domain".into()))?;
    let ranges = page_ranges_from_capnp(summary_reader.get_summary()?)?;
    Ok(RemoteDomainRangeSummary { domain, ranges })
}

/// Computes the local want for one decoded remote range summary.
async fn compute_missing_delta_range(
    stores: &SyncStores,
    root_schema_version: u32,
    remote_summary: RemoteDomainRangeSummary,
) -> Result<Option<DomainDeltaRequest>, capnp::Error> {
    let local_ranges = stores
        .page_range_summary_at_version(remote_summary.domain, root_schema_version)
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
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    requested_domain_count: usize,
    delta_requests: &[DomainDeltaRequest],
) -> Result<(), capnp::Error> {
    let sink_client = new_client(DeltaSinkImpl::new(
        stores.clone(),
        cluster_view,
        root_schema_version,
    ));

    let mut open_delta_request = sync_cap.open_delta_for_view_request();
    encode_open_delta_request(
        open_delta_request.get().init_req(),
        cluster_view,
        root_schema_version,
        delta_requests,
        sink_client,
    )?;

    debug!(
        target: "sync",
        cluster_view = %cluster_view,
        domains_requested = requested_domain_count,
        domains_with_delta = delta_requests.len(),
        "opening selective delta stream"
    );
    open_delta_request.send().promise.await?;
    Ok(())
}

/// Encodes an open-delta request, including the caller-owned sink capability.
fn encode_open_delta_request(
    mut request: sync::view_open_delta_request::Builder<'_>,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    delta_requests: &[DomainDeltaRequest],
    sink_client: sync::DeltaSinkClient,
) -> Result<(), capnp::Error> {
    cluster_view.write_capnp(request.reborrow().init_view());
    request.set_root_schema_version(root_schema_version);
    encode_domain_wants(
        request.reborrow().init_wants(delta_requests.len() as u32),
        cluster_view,
        root_schema_version,
        delta_requests,
    )?;
    request.set_sink(sink_client);
    Ok(())
}

/// Encodes all per-domain want entries for one open-delta request.
fn encode_domain_wants(
    mut wants_builder: capnp::struct_list::Builder<'_, sync::domain_want::Owned>,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    delta_requests: &[DomainDeltaRequest],
) -> Result<(), capnp::Error> {
    for (index, delta_request) in delta_requests.iter().enumerate() {
        encode_domain_want(
            wants_builder.reborrow().get(index as u32),
            cluster_view,
            root_schema_version,
            delta_request,
        )?;
    }
    Ok(())
}

/// Encodes one domain want entry with its page-summary request ranges.
fn encode_domain_want(
    mut want_builder: sync::domain_want::Builder<'_>,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    delta_request: &DomainDeltaRequest,
) -> Result<(), capnp::Error> {
    want_builder.set_domain(delta_request.domain);
    capnp_fill_ranges(
        &delta_request.want_ranges,
        want_builder.reborrow().init_want(),
    )?;
    cluster_view.write_capnp(want_builder.reborrow().init_view());
    want_builder.set_root_schema_version(root_schema_version);
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
