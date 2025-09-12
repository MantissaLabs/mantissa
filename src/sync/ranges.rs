use crdt_store::PageDigestRange;

/// Fill a capnp builder from a slice of page digest ranges.
pub fn capnp_fill_ranges(
    ranges: &[PageDigestRange],
    mut out: protocol::sync::page_range_summary::Builder,
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

/// Parse page digest ranges from a capnp reader.
pub fn page_ranges_from_capnp(
    reader: protocol::sync::page_range_summary::Reader,
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
