#![no_main]

use std::io::Cursor;

use capnp::message::ReaderOptions;
use libfuzzer_sys::fuzz_target;
use mantissa_protocol::{
    agents_capnp, gossip_capnp, info_capnp, jobs_capnp, network_capnp, node_capnp,
    scheduling_capnp, secrets_capnp, server_capnp, services_capnp, sync_capnp, task_capnp,
    topology_capnp, volumes_capnp, workload_capnp,
};

const MAX_INPUT_BYTES: usize = 64 * 1024;
const MAX_TRAVERSAL_WORDS: usize = 64 * 1024;
const MAX_NESTING_DEPTH: i32 = 32;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    let options = ReaderOptions {
        traversal_limit_in_words: Some(MAX_TRAVERSAL_WORDS),
        nesting_limit: MAX_NESTING_DEPTH,
    };
    let Ok(message) = capnp::serialize::read_message(&mut Cursor::new(data), options) else {
        return;
    };

    try_schema_roots(&message);
});

/// Attempts to view one message as representative root structs from every schema.
fn try_schema_roots(message: &capnp::message::Reader<capnp::serialize::OwnedSegments>) {
    macro_rules! try_root {
        ($root:ty) => {
            let _ = message.get_root::<$root>();
        };
    }

    try_root!(agents_capnp::agent_session_spec::Reader<'_>);
    try_root!(agents_capnp::agent_run_spec::Reader<'_>);
    try_root!(agents_capnp::agent_event::Reader<'_>);

    try_root!(gossip_capnp::message_list::Reader<'_>);
    try_root!(gossip_capnp::gossip_message::Reader<'_>);

    try_root!(info_capnp::info::Reader<'_>);
    try_root!(info_capnp::node_port_info::Reader<'_>);
    try_root!(info_capnp::load_balancer_info::Reader<'_>);

    try_root!(jobs_capnp::job_submit_spec::Reader<'_>);
    try_root!(jobs_capnp::job_record::Reader<'_>);
    try_root!(jobs_capnp::job_event::Reader<'_>);

    try_root!(network_capnp::network_create_spec::Reader<'_>);
    try_root!(network_capnp::network_spec::Reader<'_>);
    try_root!(network_capnp::network_event::Reader<'_>);

    try_root!(node_capnp::node_id::Reader<'_>);

    try_root!(scheduling_capnp::summary::Reader<'_>);
    try_root!(scheduling_capnp::scheduler_digest_event::Reader<'_>);
    try_root!(scheduling_capnp::scheduler_store_snapshot::Reader<'_>);

    try_root!(secrets_capnp::secret_spec::Reader<'_>);
    try_root!(secrets_capnp::secret_record::Reader<'_>);
    try_root!(secrets_capnp::secret_event::Reader<'_>);
    try_root!(secrets_capnp::secret_master_key_sync_record::Reader<'_>);

    try_root!(server_capnp::register_node_response::Reader<'_>);
    try_root!(server_capnp::cluster_credential::Reader<'_>);
    try_root!(server_capnp::session_ticket_record::Reader<'_>);
    try_root!(server_capnp::capabilities::Reader<'_>);

    try_root!(services_capnp::service_spec::Reader<'_>);
    try_root!(services_capnp::service_deploy_spec::Reader<'_>);
    try_root!(services_capnp::service_event::Reader<'_>);

    try_root!(sync_capnp::page_range_summary::Reader<'_>);
    try_root!(sync_capnp::delta_chunk::Reader<'_>);
    try_root!(sync_capnp::domain_root::Reader<'_>);
    try_root!(sync_capnp::domain_range_summary::Reader<'_>);
    try_root!(sync_capnp::view_request::Reader<'_>);
    try_root!(sync_capnp::view_ranges_request::Reader<'_>);
    try_root!(sync_capnp::view_open_delta_request::Reader<'_>);

    try_root!(task_capnp::task_logs_request::Reader<'_>);
    try_root!(task_capnp::task_spec::Reader<'_>);
    try_root!(task_capnp::task_start_request::Reader<'_>);
    try_root!(task_capnp::task_log_frame::Reader<'_>);

    try_root!(topology_capnp::topology_event::Reader<'_>);
    try_root!(topology_capnp::join_request::Reader<'_>);
    try_root!(topology_capnp::peer::Reader<'_>);
    try_root!(topology_capnp::cluster_view_id::Reader<'_>);
    try_root!(topology_capnp::cluster_operation::Reader<'_>);

    try_root!(volumes_capnp::volume_spec::Reader<'_>);
    try_root!(volumes_capnp::volume_node_status::Reader<'_>);
    try_root!(volumes_capnp::volume_event::Reader<'_>);

    try_root!(workload_capnp::workload_spec::Reader<'_>);
    try_root!(workload_capnp::workload_status::Reader<'_>);
    try_root!(workload_capnp::workload_event::Reader<'_>);
    try_root!(workload_capnp::workload_start_request::Reader<'_>);
}
