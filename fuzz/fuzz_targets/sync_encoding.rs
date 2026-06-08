#![no_main]

use std::io::Cursor;

use arbitrary::{Arbitrary, Unstructured};
use capnp::message::ReaderOptions;
use libfuzzer_sys::fuzz_target;
use mantissa_protocol::{sync_capnp, topology_capnp};

const MAX_RAW_BYTES: usize = 64 * 1024;
const MAX_ITEMS: usize = 32;
const MAX_BYTES: usize = 256;
const MAX_TRAVERSAL_WORDS: usize = 64 * 1024;
const MAX_NESTING_DEPTH: i32 = 32;

#[derive(Arbitrary, Debug)]
struct SyncInput {
    domain_tag: u8,
    cluster_id: Vec<u8>,
    epoch: u64,
    root_schema_version: u32,
    ranges: Vec<PageRangeInput>,
    regs: Vec<RegInput>,
    tombs: Vec<TombInput>,
    frontiers: Vec<FrontierInput>,
    root_digest: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
struct PageRangeInput {
    start: Vec<u8>,
    end: Vec<u8>,
    hash: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
struct RegInput {
    key: Vec<u8>,
    reg: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
struct TombInput {
    key: Vec<u8>,
    ts: u64,
    origin_actor: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
struct FrontierInput {
    origin_actor: Vec<u8>,
    sequence: u64,
}

fuzz_target!(|data: &[u8]| {
    assert_raw_sync_decoders_do_not_panic(data);

    let mut unstructured = Unstructured::new(data);
    let Ok(input) = SyncInput::arbitrary(&mut unstructured) else {
        return;
    };

    assert_page_range_summary_roundtrips(&input);
    assert_delta_chunk_roundtrips(&input);
    assert_domain_root_roundtrips(&input);
    assert_domain_range_summary_roundtrips(&input);
    assert_domain_want_roundtrips(&input);
    assert_view_request_roundtrips(&input);
    assert_view_ranges_request_roundtrips(&input);
});

/// Exercises sync schema root readers with arbitrary bytes and bounded traversal.
fn assert_raw_sync_decoders_do_not_panic(data: &[u8]) {
    let raw = bounded_raw_bytes(data);
    let options = ReaderOptions {
        traversal_limit_in_words: Some(MAX_TRAVERSAL_WORDS),
        nesting_limit: MAX_NESTING_DEPTH,
    };
    let Ok(message) = capnp::serialize::read_message(&mut Cursor::new(raw), options) else {
        return;
    };

    let _ = message.get_root::<sync_capnp::page_range_summary::Reader<'_>>();
    let _ = message.get_root::<sync_capnp::delta_chunk::Reader<'_>>();
    let _ = message.get_root::<sync_capnp::domain_root::Reader<'_>>();
    let _ = message.get_root::<sync_capnp::domain_range_summary::Reader<'_>>();
    let _ = message.get_root::<sync_capnp::domain_want::Reader<'_>>();
    let _ = message.get_root::<sync_capnp::view_request::Reader<'_>>();
    let _ = message.get_root::<sync_capnp::view_ranges_request::Reader<'_>>();
}

/// Verifies page range summaries preserve range bytes and ordering.
fn assert_page_range_summary_roundtrips(input: &SyncInput) {
    let mut message = capnp::message::Builder::new_default();
    write_page_range_summary(
        message.init_root::<sync_capnp::page_range_summary::Builder<'_>>(),
        &input.ranges,
    );

    with_root::<sync_capnp::page_range_summary::Owned, _>(&message, |decoded| {
        assert_page_ranges_equal(
            decoded.get_ranges().expect("ranges should decode"),
            &input.ranges,
        );
    });
}

/// Verifies delta chunks preserve domain, scope, registers, and tombstones.
fn assert_delta_chunk_roundtrips(input: &SyncInput) {
    let mut message = capnp::message::Builder::new_default();
    {
        let mut builder = message.init_root::<sync_capnp::delta_chunk::Builder<'_>>();
        builder.set_domain(domain(input.domain_tag));
        builder.set_root_schema_version(input.root_schema_version);
        write_view(builder.reborrow().init_view(), input);
        write_regs(
            builder.reborrow().init_regs(bounded_len(input.regs.len())),
            &input.regs,
        );
        write_tombs(
            builder
                .reborrow()
                .init_tombs(bounded_len(input.tombs.len())),
            &input.tombs,
        );
    }

    with_root::<sync_capnp::delta_chunk::Owned, _>(&message, |decoded| {
        assert_eq!(
            decoded.get_domain().expect("domain should decode"),
            domain(input.domain_tag)
        );
        assert_eq!(decoded.get_root_schema_version(), input.root_schema_version);
        assert_view_equal(decoded.get_view().expect("view should decode"), input);
        assert_regs_equal(decoded.get_regs().expect("regs should decode"), &input.regs);
        assert_tombs_equal(
            decoded.get_tombs().expect("tombs should decode"),
            &input.tombs,
        );
    });
}

/// Verifies domain roots preserve root digest, view, schema, and prune frontiers.
fn assert_domain_root_roundtrips(input: &SyncInput) {
    let mut message = capnp::message::Builder::new_default();
    {
        let mut builder = message.init_root::<sync_capnp::domain_root::Builder<'_>>();
        builder.set_domain(domain(input.domain_tag));
        builder.set_root_digest(&bounded_bytes(&input.root_digest));
        builder.set_root_schema_version(input.root_schema_version);
        write_view(builder.reborrow().init_view(), input);
        write_frontiers(
            builder
                .reborrow()
                .init_tombstone_prune_frontiers(bounded_len(input.frontiers.len())),
            &input.frontiers,
        );
    }

    with_root::<sync_capnp::domain_root::Owned, _>(&message, |decoded| {
        assert_eq!(
            decoded.get_domain().expect("domain should decode"),
            domain(input.domain_tag)
        );
        assert_eq!(
            decoded
                .get_root_digest()
                .expect("root digest should decode"),
            bounded_bytes(&input.root_digest)
        );
        assert_eq!(decoded.get_root_schema_version(), input.root_schema_version);
        assert_view_equal(decoded.get_view().expect("view should decode"), input);
        assert_frontiers_equal(
            decoded
                .get_tombstone_prune_frontiers()
                .expect("frontiers should decode"),
            &input.frontiers,
        );
    });
}

/// Verifies domain range summaries preserve nested range summaries.
fn assert_domain_range_summary_roundtrips(input: &SyncInput) {
    let mut message = capnp::message::Builder::new_default();
    {
        let mut builder = message.init_root::<sync_capnp::domain_range_summary::Builder<'_>>();
        builder.set_domain(domain(input.domain_tag));
        builder.set_root_schema_version(input.root_schema_version);
        write_view(builder.reborrow().init_view(), input);
        write_page_range_summary(builder.reborrow().init_summary(), &input.ranges);
    }

    with_root::<sync_capnp::domain_range_summary::Owned, _>(&message, |decoded| {
        assert_eq!(
            decoded.get_domain().expect("domain should decode"),
            domain(input.domain_tag)
        );
        assert_eq!(decoded.get_root_schema_version(), input.root_schema_version);
        assert_view_equal(decoded.get_view().expect("view should decode"), input);
        assert_page_ranges_equal(
            decoded
                .get_summary()
                .expect("summary should decode")
                .get_ranges()
                .expect("ranges should decode"),
            &input.ranges,
        );
    });
}

/// Verifies domain wants preserve nested wanted ranges.
fn assert_domain_want_roundtrips(input: &SyncInput) {
    let mut message = capnp::message::Builder::new_default();
    {
        let mut builder = message.init_root::<sync_capnp::domain_want::Builder<'_>>();
        builder.set_domain(domain(input.domain_tag));
        builder.set_root_schema_version(input.root_schema_version);
        write_view(builder.reborrow().init_view(), input);
        write_page_range_summary(builder.reborrow().init_want(), &input.ranges);
    }

    with_root::<sync_capnp::domain_want::Owned, _>(&message, |decoded| {
        assert_eq!(
            decoded.get_domain().expect("domain should decode"),
            domain(input.domain_tag)
        );
        assert_eq!(decoded.get_root_schema_version(), input.root_schema_version);
        assert_view_equal(decoded.get_view().expect("view should decode"), input);
        assert_page_ranges_equal(
            decoded
                .get_want()
                .expect("want should decode")
                .get_ranges()
                .expect("ranges should decode"),
            &input.ranges,
        );
    });
}

/// Verifies view requests preserve the requested view and schema version.
fn assert_view_request_roundtrips(input: &SyncInput) {
    let mut message = capnp::message::Builder::new_default();
    {
        let mut builder = message.init_root::<sync_capnp::view_request::Builder<'_>>();
        builder.set_root_schema_version(input.root_schema_version);
        write_view(builder.reborrow().init_view(), input);
    }

    with_root::<sync_capnp::view_request::Owned, _>(&message, |decoded| {
        assert_eq!(decoded.get_root_schema_version(), input.root_schema_version);
        assert_view_equal(decoded.get_view().expect("view should decode"), input);
    });
}

/// Verifies range requests preserve the requested domain list.
fn assert_view_ranges_request_roundtrips(input: &SyncInput) {
    let mut message = capnp::message::Builder::new_default();
    {
        let mut builder = message.init_root::<sync_capnp::view_ranges_request::Builder<'_>>();
        builder.set_root_schema_version(input.root_schema_version);
        write_view(builder.reborrow().init_view(), input);

        let domains = [
            domain(input.domain_tag),
            domain(input.domain_tag.wrapping_add(1)),
        ];
        let mut domain_builder = builder.reborrow().init_domains(domains.len() as u32);
        for (index, domain) in domains.iter().copied().enumerate() {
            domain_builder.set(index as u32, domain);
        }
    }

    with_root::<sync_capnp::view_ranges_request::Owned, _>(&message, |decoded| {
        assert_eq!(decoded.get_root_schema_version(), input.root_schema_version);
        assert_view_equal(decoded.get_view().expect("view should decode"), input);
        let domains = decoded.get_domains().expect("domains should decode");
        assert_eq!(domains.len(), 2);
        assert_eq!(
            domains.get(0).expect("first domain should decode"),
            domain(input.domain_tag)
        );
        assert_eq!(
            domains.get(1).expect("second domain should decode"),
            domain(input.domain_tag.wrapping_add(1))
        );
    });
}

/// Reads the root from a just-built message while keeping the owner in scope.
fn with_root<T, F>(message: &capnp::message::Builder<capnp::message::HeapAllocator>, assert: F)
where
    T: capnp::traits::Owned,
    F: for<'a> FnOnce(T::Reader<'a>),
{
    let bytes = capnp::serialize::write_message_to_words(message);
    let reader = capnp::serialize::read_message(
        &mut Cursor::new(bytes),
        capnp::message::ReaderOptions::new(),
    )
    .expect("encoded sync message should decode");
    let root = reader
        .get_root::<T::Reader<'_>>()
        .expect("encoded sync root should decode");
    assert(root);
}

/// Writes the generated cluster view scope into one builder.
fn write_view(mut builder: topology_capnp::cluster_view_id::Builder<'_>, input: &SyncInput) {
    builder
        .reborrow()
        .init_cluster_id()
        .set_value(&bounded_bytes(&input.cluster_id));
    builder.set_epoch(input.epoch);
}

/// Verifies the generated cluster view scope was preserved.
fn assert_view_equal(reader: topology_capnp::cluster_view_id::Reader<'_>, input: &SyncInput) {
    assert_eq!(
        reader
            .get_cluster_id()
            .expect("cluster id should decode")
            .get_value()
            .expect("cluster id value should decode"),
        bounded_bytes(&input.cluster_id)
    );
    assert_eq!(reader.get_epoch(), input.epoch);
}

/// Writes a page range summary.
fn write_page_range_summary(
    mut builder: sync_capnp::page_range_summary::Builder<'_>,
    ranges: &[PageRangeInput],
) {
    let mut list = builder.reborrow().init_ranges(bounded_len(ranges.len()));
    for (index, range) in ranges.iter().take(MAX_ITEMS).enumerate() {
        let mut item = list.reborrow().get(index as u32);
        item.set_start(&bounded_bytes(&range.start));
        item.set_end(&bounded_bytes(&range.end));
        item.set_hash(&bounded_bytes(&range.hash));
    }
}

/// Writes register entries into a delta chunk.
fn write_regs(
    mut builder: capnp::struct_list::Builder<'_, sync_capnp::reg_item::Owned>,
    regs: &[RegInput],
) {
    for (index, reg) in regs.iter().take(MAX_ITEMS).enumerate() {
        let mut item = builder.reborrow().get(index as u32);
        item.set_key(&bounded_bytes(&reg.key));
        item.set_reg(&bounded_bytes(&reg.reg));
    }
}

/// Writes tombstone entries into a delta chunk.
fn write_tombs(
    mut builder: capnp::struct_list::Builder<'_, sync_capnp::tomb_item::Owned>,
    tombs: &[TombInput],
) {
    for (index, tomb) in tombs.iter().take(MAX_ITEMS).enumerate() {
        let mut item = builder.reborrow().get(index as u32);
        item.set_key(&bounded_bytes(&tomb.key));
        item.set_ts(tomb.ts);
        item.set_origin_actor(&bounded_bytes(&tomb.origin_actor));
    }
}

/// Writes tombstone prune frontiers into a domain root.
fn write_frontiers(
    mut builder: capnp::struct_list::Builder<'_, sync_capnp::tombstone_prune_frontier::Owned>,
    frontiers: &[FrontierInput],
) {
    for (index, frontier) in frontiers.iter().take(MAX_ITEMS).enumerate() {
        let mut item = builder.reborrow().get(index as u32);
        item.set_origin_actor(&bounded_bytes(&frontier.origin_actor));
        item.set_sequence(frontier.sequence);
    }
}

/// Verifies page range list contents.
fn assert_page_ranges_equal(
    ranges: capnp::struct_list::Reader<'_, sync_capnp::page_range::Owned>,
    expected: &[PageRangeInput],
) {
    assert_eq!(ranges.len(), bounded_len(expected.len()));
    for (index, expected) in expected.iter().take(MAX_ITEMS).enumerate() {
        let range = ranges.get(index as u32);
        assert_eq!(
            range.get_start().expect("start should decode"),
            bounded_bytes(&expected.start)
        );
        assert_eq!(
            range.get_end().expect("end should decode"),
            bounded_bytes(&expected.end)
        );
        assert_eq!(
            range.get_hash().expect("hash should decode"),
            bounded_bytes(&expected.hash)
        );
    }
}

/// Verifies register list contents.
fn assert_regs_equal(
    regs: capnp::struct_list::Reader<'_, sync_capnp::reg_item::Owned>,
    expected: &[RegInput],
) {
    assert_eq!(regs.len(), bounded_len(expected.len()));
    for (index, expected) in expected.iter().take(MAX_ITEMS).enumerate() {
        let reg = regs.get(index as u32);
        assert_eq!(
            reg.get_key().expect("key should decode"),
            bounded_bytes(&expected.key)
        );
        assert_eq!(
            reg.get_reg().expect("reg should decode"),
            bounded_bytes(&expected.reg)
        );
    }
}

/// Verifies tombstone list contents.
fn assert_tombs_equal(
    tombs: capnp::struct_list::Reader<'_, sync_capnp::tomb_item::Owned>,
    expected: &[TombInput],
) {
    assert_eq!(tombs.len(), bounded_len(expected.len()));
    for (index, expected) in expected.iter().take(MAX_ITEMS).enumerate() {
        let tomb = tombs.get(index as u32);
        assert_eq!(
            tomb.get_key().expect("key should decode"),
            bounded_bytes(&expected.key)
        );
        assert_eq!(tomb.get_ts(), expected.ts);
        assert_eq!(
            tomb.get_origin_actor().expect("origin actor should decode"),
            bounded_bytes(&expected.origin_actor)
        );
    }
}

/// Verifies tombstone frontier list contents.
fn assert_frontiers_equal(
    frontiers: capnp::struct_list::Reader<'_, sync_capnp::tombstone_prune_frontier::Owned>,
    expected: &[FrontierInput],
) {
    assert_eq!(frontiers.len(), bounded_len(expected.len()));
    for (index, expected) in expected.iter().take(MAX_ITEMS).enumerate() {
        let frontier = frontiers.get(index as u32);
        assert_eq!(
            frontier
                .get_origin_actor()
                .expect("origin actor should decode"),
            bounded_bytes(&expected.origin_actor)
        );
        assert_eq!(frontier.get_sequence(), expected.sequence);
    }
}

/// Maps arbitrary domain tags onto known sync domains.
fn domain(tag: u8) -> sync_capnp::Domain {
    match tag % 14 {
        0 => sync_capnp::Domain::Peers,
        1 => sync_capnp::Domain::Workloads,
        2 => sync_capnp::Domain::Services,
        3 => sync_capnp::Domain::Secrets,
        4 => sync_capnp::Domain::Networks,
        5 => sync_capnp::Domain::NetworkPeers,
        6 => sync_capnp::Domain::NetworkAttachments,
        7 => sync_capnp::Domain::ClusterViews,
        8 => sync_capnp::Domain::Volumes,
        9 => sync_capnp::Domain::VolumeNodes,
        10 => sync_capnp::Domain::SchedulerDigests,
        11 => sync_capnp::Domain::Jobs,
        12 => sync_capnp::Domain::Agents,
        _ => sync_capnp::Domain::SecretMasterKeys,
    }
}

/// Bounds generated list sizes to keep each fuzz iteration cheap.
fn bounded_len(len: usize) -> u32 {
    len.min(MAX_ITEMS) as u32
}

/// Bounds generated byte fields to keep each fuzz iteration cheap.
fn bounded_bytes(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().copied().take(MAX_BYTES).collect()
}

/// Bounds raw message bytes independently from generated field bytes.
fn bounded_raw_bytes(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().copied().take(MAX_RAW_BYTES).collect()
}
