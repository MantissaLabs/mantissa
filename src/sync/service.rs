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
    EncodedRegister, EncodedRegisters, EncodedTombstone, EncodedTombstones,
};
use mantissa_protocol::sync::{Domain, delta_sink, sync};
use std::rc::Rc;
use tracing::{debug, trace};

/// Cap'n Proto server that exposes all replicated stores through one sync interface.
#[derive(Clone)]
pub struct SyncService {
    cluster_view: ClusterViewState,
    root_schema: RootSchemaState,
    stores: SyncStores,
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

    /// Resolves and validates the view/root-schema scope shared by all sync RPCs.
    fn validate_scope(
        &self,
        scope: encoding::ViewScope,
    ) -> Result<(ClusterViewId, u32), capnp::Error> {
        let active_view = self.require_active_view(scope.cluster_view)?;
        let root_schema_version =
            self.require_supported_root_schema_version(scope.root_schema_version)?;
        Ok((active_view, root_schema_version))
    }
}

impl sync::Server for SyncService {
    /// Returns domain roots scoped to the caller-provided cluster view.
    async fn get_roots_for_view(
        self: Rc<Self>,
        params: sync::GetRootsForViewParams,
        mut results: sync::GetRootsForViewResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_req()?;
        let request_scope = encoding::decode_view_request_scope(request)?;
        let requested_view = request_scope.cluster_view;
        let (active_view, root_schema_version) = self.validate_scope(request_scope)?;
        trace!(
            target: "sync",
            requested_view = %requested_view,
            active_view = %active_view,
            root_schema_version,
            "get_roots_for_view request received"
        );

        let mut roots_builder = results.get().init_roots(VIEW_SCOPED_DOMAIN_COUNT as u32);
        for (index, store) in self.stores.entries().iter().enumerate() {
            let root_digest = store
                .store
                .root_digest_at_version(root_schema_version)
                .await
                .map_err(to_capnp)?;
            let frontiers = store
                .store
                .load_tombstone_prune_frontiers()
                .map_err(to_capnp)?;
            encoding::encode_domain_root(
                roots_builder.reborrow().get(index as u32),
                store.domain,
                &root_digest,
                active_view,
                root_schema_version,
                &frontiers,
            );
        }

        Ok(())
    }

    /// Returns range summaries scoped to the caller-provided cluster view.
    async fn get_ranges_for_view(
        self: Rc<Self>,
        params: sync::GetRangesForViewParams,
        mut results: sync::GetRangesForViewResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_req()?;
        let request_scope = encoding::decode_ranges_request_scope(request)?;
        let requested_view = request_scope.cluster_view;
        let (active_view, root_schema_version) = self.validate_scope(request_scope)?;
        trace!(
            target: "sync",
            requested_view = %requested_view,
            active_view = %active_view,
            root_schema_version,
            "get_ranges_for_view request received"
        );

        let requested_domains = encoding::decode_requested_domains(request, &ALL_DOMAINS)?;
        let mut ranges_builder = results.get().init_ranges(requested_domains.len() as u32);
        for (index, domain) in requested_domains.iter().copied().enumerate() {
            let store = self.stores.require(domain).map_err(to_capnp)?;
            debug!("getRangesForView: received ({})", store.label);
            let dump_label = store.dump_label("server.before.get_ranges");
            store.store.debug_dump_root(&dump_label).await;
            store.store.debug_dump_ranges(&dump_label, 5).await;
            let ranges = store
                .store
                .page_range_summary_at_version(root_schema_version)
                .await
                .map_err(to_capnp)?;
            encoding::encode_domain_range_summary(
                ranges_builder.reborrow().get(index as u32),
                store.domain,
                &ranges,
                active_view,
                root_schema_version,
            )?;
        }

        Ok(())
    }

    /// Streams delta chunks scoped to the caller-provided cluster view.
    async fn open_delta_for_view(
        self: Rc<Self>,
        params: sync::OpenDeltaForViewParams,
        _results: sync::OpenDeltaForViewResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_req()?;
        let request_scope = encoding::decode_open_delta_request_scope(request)?;
        let requested_view = request_scope.cluster_view;
        let (active_view, root_schema_version) = self.validate_scope(request_scope)?;
        debug!(
            target: "delta",
            requested_view = %requested_view,
            active_view = %active_view,
            root_schema_version,
            "open_delta_for_view request received"
        );

        let stream_request = encoding::decode_open_delta_stream_request(request)?;
        if stream_request.wants.is_empty() {
            stream_request.sink.end_request().send().promise.await?;
            return Ok(());
        }

        let mut sent_chunks = false;
        for index in 0..stream_request.wants.len() {
            let want = encoding::decode_domain_want(
                stream_request.wants.get(index),
                active_view,
                root_schema_version,
            )?;
            if want.want_ranges.is_empty() {
                continue;
            }

            let store = self.stores.require(want.domain).map_err(to_capnp)?;
            debug!(
                target: "delta",
                "open_delta_for_view: received ({})",
                store.label
            );
            let dump_label = store.dump_label("server.before.open_delta");
            store.store.debug_dump_root(&dump_label).await;
            store.store.debug_dump_ranges(&dump_label, 5).await;
            let (regs, tombs) = store
                .store
                .export_delta_encoded(&want.want_ranges)
                .map_err(to_capnp)?;
            if send_chunks(
                store.domain,
                regs,
                tombs,
                active_view,
                root_schema_version,
                &stream_request.sink,
            )
            .await?
            {
                sent_chunks = true;
            }
        }

        if !sent_chunks {
            debug!(target: "delta", "open_delta_for_view: no chunks emitted");
        }

        stream_request.sink.end_request().send().promise.await?;
        Ok(())
    }
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

        let mut request = sink.push_chunk_request();
        encoding::encode_delta_chunk(
            request.get().init_chunk(),
            domain,
            regs_chunk,
            tombs_chunk,
            cluster_view,
            root_schema_version,
        );
        request.send().await?;

        regs_slice = rest_regs;
        tombs_slice = rest_tombs;
    }

    Ok(true)
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
