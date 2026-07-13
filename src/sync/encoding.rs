//! Cap'n Proto encoding and decoding helpers for Mantissa sync RPCs.
//!
//! The sync service and client both keep protocol decisions in their own modules and call these
//! helpers for raw builder/reader work, so wire-format details stay isolated from sync flow.

use crate::cluster::ClusterViewId;
use crate::store::replicated::registry::{
    EncodedRegister, EncodedRegisters, EncodedTombstone, EncodedTombstones,
};
use mantissa_protocol::sync::{self, Domain, delta_chunk, delta_sink};
use mantissa_store::mst_store::TombstonePruneFrontiers;
use mantissa_store::{PageDigestRange, RowDigest};

/// View and root-schema selector decoded from one sync request.
pub(crate) struct ViewScope {
    pub(crate) cluster_view: ClusterViewId,
    pub(crate) root_schema_version: u32,
}

/// Root-phase payload for one remote domain, including GC prune-frontier metadata.
pub(crate) struct RemoteDomainRoot {
    pub(crate) domain: Domain,
    pub(crate) digest: [u8; 16],
    pub(crate) prune_frontiers: TombstonePruneFrontiers,
}

/// Range-summary payload decoded from the peer before local want computation.
pub(crate) struct RemoteDomainRangeSummary {
    pub(crate) domain: Domain,
    pub(crate) ranges: Vec<PageDigestRange>,
}

/// Per-domain page ranges and local row digests used to request a filtered delta.
pub(crate) struct DomainDeltaRequest {
    pub(crate) domain: Domain,
    pub(crate) want_ranges: Vec<PageDigestRange>,
    pub(crate) have_rows: Vec<RowDigest>,
}

/// Per-domain page ranges and row digests received through `openDeltaForView`.
pub(crate) struct DomainDeltaWant {
    pub(crate) domain: Domain,
    pub(crate) want_ranges: Vec<PageDigestRange>,
    pub(crate) have_rows: Vec<RowDigest>,
}

/// Open-delta stream payload decoded after the request scope has been validated.
pub(crate) struct OpenDeltaStreamRequest<'a> {
    pub(crate) wants: capnp::struct_list::Reader<'a, sync::domain_want::Owned>,
    pub(crate) sink: delta_sink::Client,
}

/// Validated metadata for one inbound delta chunk.
pub(crate) struct DeltaChunkHeader {
    pub(crate) domain: Domain,
    pub(crate) cluster_view: ClusterViewId,
}

/// Encodes one page-summary slice into the Cap'n Proto representation used by sync RPCs.
pub(crate) fn capnp_fill_ranges(
    ranges: &[PageDigestRange],
    mut out: sync::page_range_summary::Builder<'_>,
) -> Result<(), capnp::Error> {
    let mut lst = out.reborrow().init_ranges(ranges.len() as u32);
    for (i, r) in ranges.iter().enumerate() {
        let mut it = lst.reborrow().get(i as u32);
        it.set_start(&r.start);
        it.set_end(&r.end);
        it.set_hash(&r.hash);
    }
    Ok(())
}

/// Decodes a Cap'n Proto page summary back into the store-facing range type.
pub(crate) fn page_ranges_from_capnp(
    reader: sync::page_range_summary::Reader<'_>,
) -> Result<Vec<PageDigestRange>, capnp::Error> {
    let ranges = reader.get_ranges()?;
    let mut out = Vec::with_capacity(ranges.len() as usize);
    for i in 0..ranges.len() {
        let r = ranges.get(i);
        out.push(PageDigestRange {
            start: r.get_start()?.to_vec(),
            end: r.get_end()?.to_vec(),
            hash: r.get_hash()?.to_vec(),
        });
    }
    Ok(out)
}

/// Encodes semantic row digests into one delta request.
fn capnp_fill_row_digests(
    rows: &[RowDigest],
    mut out: capnp::struct_list::Builder<'_, sync::row_digest::Owned>,
) {
    for (index, row) in rows.iter().enumerate() {
        let mut item = out.reborrow().get(index as u32);
        item.set_key(&row.key);
        item.set_digest(&row.digest);
    }
}

/// Decodes and validates semantic row digests from one delta request.
fn row_digests_from_capnp(
    rows: capnp::struct_list::Reader<'_, sync::row_digest::Owned>,
) -> Result<Vec<RowDigest>, capnp::Error> {
    let mut out = Vec::with_capacity(rows.len() as usize);
    for row in rows.iter() {
        out.push(RowDigest {
            key: row.get_key()?.to_vec(),
            digest: read_row_digest(row.get_digest()?)?,
        });
    }
    Ok(out)
}

/// Decodes the scope of a root request.
pub(crate) fn decode_view_request_scope(
    request: sync::view_request::Reader<'_>,
) -> Result<ViewScope, capnp::Error> {
    Ok(ViewScope {
        cluster_view: ClusterViewId::from_capnp(request.get_view()?)
            .map_err(capnp::Error::failed)?,
        root_schema_version: request.get_root_schema_version(),
    })
}

/// Decodes the scope of a range-summary request.
pub(crate) fn decode_ranges_request_scope(
    request: sync::view_ranges_request::Reader<'_>,
) -> Result<ViewScope, capnp::Error> {
    Ok(ViewScope {
        cluster_view: ClusterViewId::from_capnp(request.get_view()?)
            .map_err(capnp::Error::failed)?,
        root_schema_version: request.get_root_schema_version(),
    })
}

/// Decodes the explicit domain selector from a range-summary request.
pub(crate) fn decode_requested_domains(
    request: sync::view_ranges_request::Reader<'_>,
    default_domains: &[Domain],
) -> Result<Vec<Domain>, capnp::Error> {
    let domains_reader = request.get_domains()?;
    if domains_reader.is_empty() {
        return Ok(default_domains.to_vec());
    }

    let mut domains = Vec::with_capacity(domains_reader.len() as usize);
    for domain in domains_reader.iter() {
        domains.push(domain?);
    }
    Ok(domains)
}

/// Decodes the scope of an open-delta request.
pub(crate) fn decode_open_delta_request_scope(
    request: sync::view_open_delta_request::Reader<'_>,
) -> Result<ViewScope, capnp::Error> {
    Ok(ViewScope {
        cluster_view: ClusterViewId::from_capnp(request.get_view()?)
            .map_err(capnp::Error::failed)?,
        root_schema_version: request.get_root_schema_version(),
    })
}

/// Decodes open-delta wants and the caller-provided sink capability.
pub(crate) fn decode_open_delta_stream_request<'a>(
    request: sync::view_open_delta_request::Reader<'a>,
) -> Result<OpenDeltaStreamRequest<'a>, capnp::Error> {
    Ok(OpenDeltaStreamRequest {
        wants: request.get_wants()?,
        sink: request.get_sink()?,
    })
}

/// Encodes one server-side root response entry.
pub(crate) fn encode_domain_root(
    mut root_builder: sync::domain_root::Builder<'_>,
    domain: Domain,
    root_digest: &[u8; 16],
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    frontiers: &TombstonePruneFrontiers,
) {
    root_builder.set_domain(domain);
    root_builder.set_root_digest(root_digest);
    cluster_view.write_capnp(root_builder.reborrow().init_view());
    root_builder.set_root_schema_version(root_schema_version);

    let mut frontier_list = root_builder
        .reborrow()
        .init_tombstone_prune_frontiers(frontiers.len() as u32);
    for (frontier_idx, (origin_actor, sequence)) in frontiers.iter().enumerate() {
        let mut frontier = frontier_list.reborrow().get(frontier_idx as u32);
        frontier.set_origin_actor(origin_actor);
        frontier.set_sequence(*sequence);
    }
}

/// Encodes one server-side range-summary response entry.
pub(crate) fn encode_domain_range_summary(
    mut summary_builder: sync::domain_range_summary::Builder<'_>,
    domain: Domain,
    ranges: &[PageDigestRange],
    cluster_view: ClusterViewId,
    root_schema_version: u32,
) -> Result<(), capnp::Error> {
    summary_builder.set_domain(domain);
    capnp_fill_ranges(ranges, summary_builder.reborrow().init_summary())?;
    cluster_view.write_capnp(summary_builder.reborrow().init_view());
    summary_builder.set_root_schema_version(root_schema_version);
    Ok(())
}

/// Decodes one open-delta want and validates its view/root-schema scope.
pub(crate) fn decode_domain_want(
    want_reader: sync::domain_want::Reader<'_>,
    expected_view: ClusterViewId,
    expected_root_schema_version: u32,
) -> Result<DomainDeltaWant, capnp::Error> {
    let want_view =
        ClusterViewId::from_capnp(want_reader.get_view()?).map_err(capnp::Error::failed)?;
    if want_view != expected_view {
        return Err(capnp::Error::failed(format!(
            "domain want view mismatch: expected {expected_view}, got {want_view}"
        )));
    }
    if want_reader.get_root_schema_version() != expected_root_schema_version {
        return Err(capnp::Error::failed(format!(
            "domain want root schema mismatch: expected {expected_root_schema_version}, got {}",
            want_reader.get_root_schema_version()
        )));
    }

    let domain = want_reader
        .get_domain()
        .map_err(|_| capnp::Error::failed("unknown sync domain".into()))?;
    let want_ranges = page_ranges_from_capnp(want_reader.get_want()?)?;
    let have_rows = row_digests_from_capnp(want_reader.get_have()?)?;
    Ok(DomainDeltaWant {
        domain,
        want_ranges,
        have_rows,
    })
}

/// Encodes one outbound delta chunk for a remote sink.
pub(crate) fn encode_delta_chunk(
    mut chunk_builder: delta_chunk::Builder<'_>,
    domain: Domain,
    regs_chunk: &[EncodedRegister],
    tombs_chunk: &[EncodedTombstone],
    cluster_view: ClusterViewId,
    root_schema_version: u32,
) {
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

/// Encodes the shared view/root-schema selector used by client root requests.
pub(crate) fn encode_view_request(
    mut request: sync::view_request::Builder<'_>,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
) {
    cluster_view.write_capnp(request.reborrow().init_view());
    request.set_root_schema_version(root_schema_version);
}

/// Decodes and validates the root response returned by `getRootsForView`.
pub(crate) fn decode_remote_domain_roots(
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

/// Encodes the view/root-schema selector plus the explicit domains to summarize.
pub(crate) fn encode_view_ranges_request(
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

/// Decodes one range summary and rejects responses scoped to a different view/schema.
pub(crate) fn decode_remote_domain_range_summary(
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

/// Encodes an open-delta request, including the caller-owned sink capability.
pub(crate) fn encode_open_delta_request(
    mut request: sync::view_open_delta_request::Builder<'_>,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
    delta_requests: &[DomainDeltaRequest],
    sink_client: delta_sink::Client,
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
    capnp_fill_row_digests(
        &delta_request.have_rows,
        want_builder
            .reborrow()
            .init_have(delta_request.have_rows.len() as u32),
    );
    cluster_view.write_capnp(want_builder.reborrow().init_view());
    want_builder.set_root_schema_version(root_schema_version);
    Ok(())
}

/// Decodes and validates one inbound delta chunk header.
pub(crate) fn decode_delta_chunk_header(
    chunk_reader: &delta_chunk::Reader<'_>,
    expected_view: ClusterViewId,
    expected_root_schema_version: u32,
) -> Result<DeltaChunkHeader, capnp::Error> {
    let domain = chunk_reader
        .get_domain()
        .map_err(|_| capnp::Error::failed("unknown sync domain".into()))?;
    let chunk_view =
        ClusterViewId::from_capnp(chunk_reader.get_view()?).map_err(capnp::Error::failed)?;
    if chunk_view != expected_view {
        return Err(capnp::Error::failed(format!(
            "delta chunk view mismatch: expected {}, got {}",
            expected_view, chunk_view
        )));
    }
    if chunk_reader.get_root_schema_version() != expected_root_schema_version {
        return Err(capnp::Error::failed(format!(
            "delta chunk root schema mismatch: expected {}, got {}",
            expected_root_schema_version,
            chunk_reader.get_root_schema_version()
        )));
    }

    Ok(DeltaChunkHeader {
        domain,
        cluster_view: chunk_view,
    })
}

/// Extracts opaque tombstone rows from a wire chunk.
pub(crate) fn collect_tombstones(
    chunk: &delta_chunk::Reader<'_>,
) -> Result<EncodedTombstones, capnp::Error> {
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
pub(crate) fn collect_registers(
    chunk: &delta_chunk::Reader<'_>,
) -> Result<EncodedRegisters, capnp::Error> {
    let mut regs = Vec::new();
    for entry in chunk.get_regs()?.iter() {
        regs.push((entry.get_key()?.to_vec(), entry.get_reg()?.to_vec()));
    }
    Ok(regs)
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

/// Decodes one fixed-width semantic row digest from the sync wire format.
fn read_row_digest(bytes: &[u8]) -> Result<[u8; 16], capnp::Error> {
    bytes.try_into().map_err(|_| {
        capnp::Error::failed(format!(
            "invalid sync row digest length: expected 16, got {}",
            bytes.len()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use capnp::message::Builder;

    /// Domain wants should preserve every row digest used by filtered delta export.
    #[test]
    fn domain_want_roundtrips_have_rows() {
        let view = ClusterViewId::legacy_default();
        let request = DomainDeltaRequest {
            domain: Domain::Workloads,
            want_ranges: vec![PageDigestRange {
                start: vec![1],
                end: vec![9],
                hash: vec![7; 16],
            }],
            have_rows: vec![RowDigest {
                key: vec![3; 16],
                digest: [5; 16],
            }],
        };
        let mut message = Builder::new_default();
        encode_domain_want(
            message.init_root::<sync::domain_want::Builder<'_>>(),
            view,
            2,
            &request,
        )
        .unwrap();

        let reader = message
            .get_root_as_reader::<sync::domain_want::Reader<'_>>()
            .unwrap();
        let decoded = decode_domain_want(reader, view, 2).unwrap();

        assert_eq!(decoded.domain, request.domain);
        assert_eq!(decoded.want_ranges, request.want_ranges);
        assert_eq!(decoded.have_rows, request.have_rows);
    }

    /// Invalid digest widths must fail before an untrusted row filter reaches storage.
    #[test]
    fn domain_want_rejects_invalid_row_digest_width() {
        let view = ClusterViewId::legacy_default();
        let mut message = Builder::new_default();
        {
            let mut want = message.init_root::<sync::domain_want::Builder<'_>>();
            want.set_domain(Domain::Workloads);
            want.reborrow().init_want();
            view.write_capnp(want.reborrow().init_view());
            want.set_root_schema_version(1);
            let mut rows = want.reborrow().init_have(1);
            let mut row = rows.reborrow().get(0);
            row.set_key(&[1; 16]);
            row.set_digest(&[2; 8]);
        }

        let reader = message
            .get_root_as_reader::<sync::domain_want::Reader<'_>>()
            .unwrap();
        let error = match decode_domain_want(reader, view, 1) {
            Ok(_) => panic!("invalid row digest should be rejected"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("expected 16"));
    }
}
