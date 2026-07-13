//! Sync Cap'n Proto service implementation.
//!
//! This module owns the server-side RPC behavior for the view-scoped anti-entropy protocol.
//! Raw wire builders and readers live in `encoding`, while this file keeps the store-facing
//! service flow visible.

use super::{
    ALL_DOMAINS, SyncStores, VIEW_SCOPED_DOMAIN_COUNT, delta_chunk_max, delta_chunk_target_bytes,
    encoding, to_capnp,
};
use crate::cluster::{ClusterViewId, ClusterViewState, RootSchemaState};
use crate::store::replicated::registry::{
    EncodedRegister, EncodedRegisters, EncodedTombstone, EncodedTombstones, ReplicatedStoreEntry,
};
use mantissa_protocol::sync::{Domain, delta_sink, sync};
use std::rc::Rc;
use tracing::{debug, trace};

/// Server side of the sync protocol.
///
/// A peer calls this service in three phases: roots, ranges, then delta chunks. Every phase is
/// tied to a cluster view and a root schema version, so peers do not mix data from different
/// control-plane lineages or different MST layouts.
#[derive(Clone)]
pub struct SyncService {
    cluster_view: ClusterViewState,
    root_schema: RootSchemaState,
    stores: SyncStores,
}

/// View and schema information accepted for one sync RPC.
///
/// `requested_view` is kept for logging. `active_view` is the view this node writes into replies.
/// They are equal after validation, but both names make the service flow easier to read.
#[derive(Clone, Copy)]
struct ValidatedSyncScope {
    requested_view: ClusterViewId,
    active_view: ClusterViewId,
    root_schema_version: u32,
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

    /// Accepts one request scope or rejects it before any store work starts.
    ///
    /// Sync replies only make sense inside this node's active cluster view. The root schema
    /// version must also be supported locally because it selects the MST shape used for roots and
    /// range summaries.
    fn validate_request_scope(
        &self,
        scope: encoding::ViewScope,
    ) -> Result<ValidatedSyncScope, capnp::Error> {
        let requested_view = scope.cluster_view;
        let active_view = self.require_active_view(requested_view)?;
        let root_schema_version =
            self.require_supported_root_schema_version(scope.root_schema_version)?;
        Ok(ValidatedSyncScope {
            requested_view,
            active_view,
            root_schema_version,
        })
    }

    /// Streams one validated non-empty domain want into the caller's delta sink.
    ///
    /// The caller already checked the request and want scopes. This method resolves the store,
    /// captures optional debug state, exports the requested MST ranges, and sends the encoded rows
    /// in bounded chunks.
    async fn stream_delta_for_want(
        &self,
        domain_want: &encoding::DomainDeltaWant,
        scope: ValidatedSyncScope,
        sink: &delta_sink::Client,
    ) -> Result<bool, capnp::Error> {
        let store = self.stores.require(domain_want.domain).map_err(to_capnp)?;
        debug!(
            target: "delta",
            "open_delta_for_view: received ({})",
            store.label
        );
        debug_dump_domain_state(store, "server.before.open_delta").await;

        let (encoded_registers, encoded_tombstones) = store
            .store
            .export_delta_encoded(
                &domain_want.want_ranges,
                &domain_want.have_rows,
                scope.root_schema_version,
            )
            .await
            .map_err(to_capnp)?;
        send_chunks(
            store.domain,
            encoded_registers,
            encoded_tombstones,
            scope.active_view,
            scope.root_schema_version,
            sink,
        )
        .await
    }
}

impl sync::Server for SyncService {
    /// Handles phase 1 of sync: the cheap root comparison.
    ///
    /// The response contains one MST root digest per replicated domain. It also includes
    /// tombstone prune frontiers, because equal roots are where peers can learn how far tombstone
    /// garbage collection has advanced elsewhere.
    async fn get_roots_for_view(
        self: Rc<Self>,
        params: sync::GetRootsForViewParams,
        mut results: sync::GetRootsForViewResults,
    ) -> Result<(), capnp::Error> {
        // Reject stale views and unsupported MST schemas before reading any store state.
        let request_reader = params.get()?.get_req()?;
        let request_scope = encoding::decode_view_request_scope(request_reader)?;
        let scope = self.validate_request_scope(request_scope)?;
        trace!(
            target: "sync",
            requested_view = %scope.requested_view,
            active_view = %scope.active_view,
            root_schema_version = scope.root_schema_version,
            "get_roots_for_view request received"
        );

        // Keep registry order stable. The client still matches entries by domain, but stable
        // ordering makes traces and tests easier to reason about.
        let mut root_entries = results.get().init_roots(VIEW_SCOPED_DOMAIN_COUNT as u32);
        for (index, store) in self.stores.entries().iter().enumerate() {
            let root_digest = store
                .store
                .root_digest_at_version(scope.root_schema_version)
                .await
                .map_err(to_capnp)?;
            let prune_frontiers = store
                .store
                .load_tombstone_prune_frontiers()
                .map_err(to_capnp)?;
            encoding::encode_domain_root(
                root_entries.reborrow().get(index as u32),
                store.domain,
                &root_digest,
                scope.active_view,
                scope.root_schema_version,
                &prune_frontiers,
            );
        }

        Ok(())
    }

    /// Handles phase 2 of sync: narrowing a root mismatch down to page ranges.
    ///
    /// The caller sends the domains whose root digests differed. This service returns page
    /// summaries for those domains, and the caller uses them to compute the exact pages it wants.
    async fn get_ranges_for_view(
        self: Rc<Self>,
        params: sync::GetRangesForViewParams,
        mut results: sync::GetRangesForViewResults,
    ) -> Result<(), capnp::Error> {
        // Ranges from another view or schema would make the caller compare against the wrong MST.
        let request_reader = params.get()?.get_req()?;
        let request_scope = encoding::decode_ranges_request_scope(request_reader)?;
        let scope = self.validate_request_scope(request_scope)?;
        trace!(
            target: "sync",
            requested_view = %scope.requested_view,
            active_view = %scope.active_view,
            root_schema_version = scope.root_schema_version,
            "get_ranges_for_view request received"
        );

        // An empty domain list means "all domains". Non-empty lists are answered in caller order.
        let domains_to_summarize =
            encoding::decode_requested_domains(request_reader, &ALL_DOMAINS)?;
        let mut range_entries = results.get().init_ranges(domains_to_summarize.len() as u32);

        // Dump before reading the summary so a debug trace shows the local state that produced
        // the response.
        for (index, domain) in domains_to_summarize.iter().copied().enumerate() {
            let store = self.stores.require(domain).map_err(to_capnp)?;
            debug!("getRangesForView: received ({})", store.label);
            debug_dump_domain_state(store, "server.before.get_ranges").await;
            let page_ranges = store
                .store
                .page_range_summary_at_version(scope.root_schema_version)
                .await
                .map_err(to_capnp)?;
            encoding::encode_domain_range_summary(
                range_entries.reborrow().get(index as u32),
                store.domain,
                &page_ranges,
                scope.active_view,
                scope.root_schema_version,
            )?;
        }

        Ok(())
    }

    /// Handles phase 3 of sync: streaming the rows the caller is missing.
    ///
    /// The caller already compared roots and ranges. It now sends page ranges, semantic digests
    /// for rows it has, and a sink capability. This service pushes back only missing or different
    /// CRDT rows in chunks.
    async fn open_delta_for_view(
        self: Rc<Self>,
        params: sync::OpenDeltaForViewParams,
        _results: sync::OpenDeltaForViewResults,
    ) -> Result<(), capnp::Error> {
        // Validate before using the sink capability. A mismatched view/schema should receive no
        // chunks.
        let request_reader = params.get()?.get_req()?;
        let request_scope = encoding::decode_open_delta_request_scope(request_reader)?;
        let scope = self.validate_request_scope(request_scope)?;
        debug!(
            target: "delta",
            requested_view = %scope.requested_view,
            active_view = %scope.active_view,
            root_schema_version = scope.root_schema_version,
            "open_delta_for_view request received"
        );

        // Empty wants are a normal successful path: the caller found no missing pages after the
        // previous phases. Close the sink so the caller can finish waiting on the stream.
        let delta_stream = encoding::decode_open_delta_stream_request(request_reader)?;
        if delta_stream.wants.is_empty() {
            delta_stream.sink.end_request().send().promise.await?;
            return Ok(());
        }

        let mut emitted_any_chunk = false;
        for index in 0..delta_stream.wants.len() {
            // Each embedded want repeats the view/schema. Check it too; otherwise a mixed request
            // could smuggle ranges from a different tree into an otherwise valid envelope.
            let domain_want = encoding::decode_domain_want(
                delta_stream.wants.get(index),
                scope.active_view,
                scope.root_schema_version,
            )?;
            if domain_want.want_ranges.is_empty() {
                continue;
            }

            if self
                .stream_delta_for_want(&domain_want, scope, &delta_stream.sink)
                .await?
            {
                emitted_any_chunk = true;
            }
        }

        if !emitted_any_chunk {
            debug!(target: "delta", "open_delta_for_view: no chunks emitted");
        }

        // Signal the end of the stream after every accepted non-empty want has been handled.
        delta_stream.sink.end_request().send().promise.await?;
        Ok(())
    }
}

/// Writes the same debug snapshot before range summaries and delta exports.
///
/// These calls are cheap no-ops unless store debug dumping is enabled. Keeping them in one helper
/// also keeps both sync phases using the same label format and dump order.
async fn debug_dump_domain_state(store: &ReplicatedStoreEntry, phase: &str) {
    let dump_label = store.dump_label(phase);
    store.store.debug_dump_root(&dump_label).await;
    store.store.debug_dump_ranges(&dump_label, 5).await;
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

/// Number of encoded rows to send in one `pushChunk` call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DeltaChunkPlan {
    register_count: usize,
    tombstone_count: usize,
    approx_payload_bytes: usize,
}

impl DeltaChunkPlan {
    /// Builds an empty chunk plan.
    fn empty() -> Self {
        Self {
            register_count: 0,
            tombstone_count: 0,
            approx_payload_bytes: 0,
        }
    }

    /// Returns how many encoded rows this plan will send.
    fn entry_count(self) -> usize {
        self.register_count + self.tombstone_count
    }

    /// Returns true when this plan would send no rows.
    fn is_empty(self) -> bool {
        self.entry_count() == 0
    }
}

/// Plans the next delta chunk without copying row data.
///
/// The result says how many rows to take from the front of the remaining register and tombstone
/// slices. A chunk stops at the entry limit or once adding another row would pass the payload
/// target. The first row is always allowed so one large row cannot block sync progress.
fn plan_delta_chunk_prefix(
    registers: &[EncodedRegister],
    tombstones: &[EncodedTombstone],
    max_entries_per_chunk: usize,
    target_payload_bytes: usize,
) -> DeltaChunkPlan {
    let mut plan = DeltaChunkPlan::empty();

    // Keep registers before tombstones. This preserves the stream shape expected by existing
    // receivers while still allowing both lists to share one chunk budget.
    while plan.entry_count() < max_entries_per_chunk && plan.register_count < registers.len() {
        let entry_bytes = encoded_register_payload_bytes(&registers[plan.register_count]);
        if payload_target_would_be_exceeded(
            plan.approx_payload_bytes,
            entry_bytes,
            target_payload_bytes,
        ) {
            break;
        }
        plan.approx_payload_bytes = plan.approx_payload_bytes.saturating_add(entry_bytes);
        plan.register_count += 1;
    }

    // Tombstones use the remaining room after registers.
    while plan.entry_count() < max_entries_per_chunk && plan.tombstone_count < tombstones.len() {
        let entry_bytes = encoded_tombstone_payload_bytes(&tombstones[plan.tombstone_count]);
        if payload_target_would_be_exceeded(
            plan.approx_payload_bytes,
            entry_bytes,
            target_payload_bytes,
        ) {
            break;
        }
        plan.approx_payload_bytes = plan.approx_payload_bytes.saturating_add(entry_bytes);
        plan.tombstone_count += 1;
    }

    if !plan.is_empty() {
        return plan;
    }

    // The byte target is soft. If the next row is larger than the target, send it alone.
    if let Some(first_register) = registers.first() {
        return DeltaChunkPlan {
            register_count: 1,
            tombstone_count: 0,
            approx_payload_bytes: encoded_register_payload_bytes(first_register),
        };
    }
    if let Some(first_tombstone) = tombstones.first() {
        return DeltaChunkPlan {
            register_count: 0,
            tombstone_count: 1,
            approx_payload_bytes: encoded_tombstone_payload_bytes(first_tombstone),
        };
    }

    plan
}

/// Returns true when another row would make a non-empty chunk too large.
///
/// Empty chunks are allowed to accept their first row regardless of size. That is the progress
/// guarantee for unusually large values.
fn payload_target_would_be_exceeded(
    current_payload_bytes: usize,
    next_entry_bytes: usize,
    target_payload_bytes: usize,
) -> bool {
    current_payload_bytes > 0
        && current_payload_bytes.saturating_add(next_entry_bytes) > target_payload_bytes
}

/// Streams one domain delta to the caller in bounded chunks.
///
/// `export_delta_encoded` already produced the encoded rows. This function only slices those rows
/// into several `pushChunk` calls so a large delta does not become one very large RPC.
async fn send_chunks(
    domain: Domain,
    encoded_registers: EncodedRegisters,
    encoded_tombstones: EncodedTombstones,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    sink: &delta_sink::Client,
) -> Result<bool, capnp::Error> {
    // Read the knobs once per domain. That keeps all chunks from one export using the same limits.
    let chunk_max = delta_chunk_max();
    let chunk_target_bytes = delta_chunk_target_bytes();

    if encoded_registers.is_empty() && encoded_tombstones.is_empty() {
        return Ok(false);
    }

    let mut remaining_registers = encoded_registers.as_slice();
    let mut remaining_tombstones = encoded_tombstones.as_slice();

    while !remaining_registers.is_empty() || !remaining_tombstones.is_empty() {
        let plan = plan_delta_chunk_prefix(
            remaining_registers,
            remaining_tombstones,
            chunk_max,
            chunk_target_bytes,
        );
        let (register_chunk, next_registers) = remaining_registers.split_at(plan.register_count);
        let (tombstone_chunk, next_tombstones) =
            remaining_tombstones.split_at(plan.tombstone_count);
        debug!(
            target: "delta",
            ?domain,
            regs = register_chunk.len(),
            tombs = tombstone_chunk.len(),
            chunk_max,
            chunk_target_bytes,
            approx_payload_bytes = plan.approx_payload_bytes,
            "sending delta chunk"
        );

        // `pushChunk` is a streaming call. Awaiting send only checks that the RPC write was
        // accepted by the transport, not that the remote has applied the data.
        let mut request = sink.push_chunk_request();
        encoding::encode_delta_chunk(
            request.get().init_chunk(),
            domain,
            register_chunk,
            tombstone_chunk,
            cluster_view,
            root_schema_version,
        );
        request.send().await?;

        remaining_registers = next_registers;
        remaining_tombstones = next_tombstones;
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::{EncodedRegister, EncodedTombstone, plan_delta_chunk_prefix};

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
    fn plan_delta_chunk_prefix_respects_entry_limit() {
        let regs = vec![
            encoded_register(8, 16),
            encoded_register(8, 16),
            encoded_register(8, 16),
        ];

        let plan = plan_delta_chunk_prefix(&regs, &[], 2, 1024);
        assert_eq!(plan.register_count, 2);
        assert_eq!(plan.tombstone_count, 0);
        assert_eq!(plan.approx_payload_bytes, 48);
    }

    /// The planner should stop after the first entry once the approximate payload target is hit.
    #[test]
    fn plan_delta_chunk_prefix_respects_payload_target() {
        let regs = vec![encoded_register(8, 40), encoded_register(8, 40)];

        let plan = plan_delta_chunk_prefix(&regs, &[], 8, 64);
        assert_eq!(plan.register_count, 1);
        assert_eq!(plan.tombstone_count, 0);
        assert_eq!(plan.approx_payload_bytes, 48);
    }

    /// The planner must always make progress even when one entry exceeds the target by itself.
    #[test]
    fn plan_delta_chunk_prefix_always_keeps_one_large_entry() {
        let regs = vec![encoded_register(8, 512)];

        let plan = plan_delta_chunk_prefix(&regs, &[], 8, 64);
        assert_eq!(plan.register_count, 1);
        assert_eq!(plan.tombstone_count, 0);
        assert_eq!(plan.approx_payload_bytes, 520);
    }

    /// Tombstones should fill the remaining room after registers while preserving stream order.
    #[test]
    fn plan_delta_chunk_prefix_adds_tombstones_after_registers() {
        let regs = vec![encoded_register(8, 16)];
        let tombs = vec![encoded_tombstone(8), encoded_tombstone(8)];

        let plan = plan_delta_chunk_prefix(&regs, &tombs, 3, 1024);
        assert_eq!(plan.register_count, 1);
        assert_eq!(plan.tombstone_count, 2);
        assert_eq!(plan.approx_payload_bytes, 88);
    }
}
