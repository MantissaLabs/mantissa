#[macro_use]
mod common;

use common::convergence::wait_until;
use common::testkit::TestNode;
use crdt_store::uuid_key::UuidKey;
use mantissa::cluster::{ClusterViewId, RootSchemaState};
use mantissa::runtime::set::RuntimeSet;
use mantissa::runtime::testing::IN_MEMORY_RUNTIME_BACKEND_KIND;
use mantissa::runtime::testing::new_in_memory_runtime_backend;
use mantissa::runtime::types::RuntimeSupportProfile;
use mantissa::server::headless::{HeadlessConfig, HeadlessKeys, HeadlessNode, HeadlessTransport};
use mantissa::topology::peers::{PeerSchedulingState, PeerValue};
use mantissa::workload::model::{ExecutionPlatform, IsolationMode};
use net::noise::NoiseKeys;
use protocol::sync::{Domain, delta_sink};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use uuid::Uuid;

/// Formats raw digest bytes as lowercase hex for root-comparison assertions.
fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

struct NoopDeltaSink;

impl delta_sink::Server for NoopDeltaSink {
    async fn push_chunk(
        self: Rc<Self>,
        _params: delta_sink::PushChunkParams,
    ) -> Result<(), capnp::Error> {
        Ok(())
    }

    async fn end(
        self: Rc<Self>,
        _params: delta_sink::EndParams,
        _results: delta_sink::EndResults,
    ) -> Result<(), capnp::Error> {
        Ok(())
    }
}

/// Builds one in-process headless config that advertises the requested root schema support range.
fn headless_config_with_root_schema(
    minimum_supported_version: u32,
    supported_version: u32,
) -> HeadlessConfig {
    HeadlessConfig {
        transport: HeadlessTransport::Inproc,
        root_schema_override: Some(
            RootSchemaState::new(minimum_supported_version, supported_version)
                .expect("valid root schema override"),
        ),
        sync_tick: Some(Duration::from_millis(100)),
        global_metadata_sync_tick: Some(Duration::from_millis(100)),
        gossip_tick: Some(Duration::from_millis(100)),
        runtime_set: Some(default_test_runtime_set()),
        ..HeadlessConfig::default()
    }
}

/// Starts one test node advertising the requested root schema support range.
async fn new_node_with_root_schema(
    minimum_supported_version: u32,
    supported_version: u32,
) -> TestNode {
    TestNode {
        node: Box::new(
            HeadlessNode::new_with_config(headless_config_with_root_schema(
                minimum_supported_version,
                supported_version,
            ))
            .await
            .expect("headless node with root schema"),
        ),
    }
}

/// Starts one restartable node backed by caller-provided durable state and root schema range.
async fn create_restartable_node_with_root_schema(
    db: Arc<redb::Database>,
    self_id: Uuid,
    keys: HeadlessKeys,
    local_volume_root: PathBuf,
    minimum_supported_version: u32,
    supported_version: u32,
) -> HeadlessNode {
    HeadlessNode::new_with(
        db,
        self_id,
        keys,
        HeadlessConfig {
            listen_addr: "127.0.0.1:0".to_string(),
            transport: HeadlessTransport::Inproc,
            root_schema_override: Some(
                RootSchemaState::new(minimum_supported_version, supported_version)
                    .expect("valid root schema override"),
            ),
            sync_tick: Some(Duration::from_millis(100)),
            sync_fanout: None,
            global_metadata_sync_tick: Some(Duration::from_millis(100)),
            global_metadata_sync_fanout: None,
            gossip_tick: Some(Duration::from_millis(100)),
            gossip_fanout: None,
            gossip_channel_capacity: None,
            task_runtime: None,
            runtime_set: Some(default_test_runtime_set()),
            local_volume_root: Some(local_volume_root),
        },
    )
    .await
    .expect("restartable root schema node")
}

/// Builds the default runtime set used by headless root-schema tests.
fn default_test_runtime_set() -> RuntimeSet {
    RuntimeSet::singleton(
        IN_MEMORY_RUNTIME_BACKEND_KIND,
        new_in_memory_runtime_backend(),
    )
}

/// Reads the best-known root schema support range for one peer from the local peer store.
fn stored_peer_root_schema(node: &TestNode, peer_id: Uuid) -> (u32, u32) {
    let reg = node
        .node
        .peers
        .get_reg(&UuidKey::from(peer_id))
        .expect("read peer register")
        .expect("peer register present");
    let value = PeerValue::select_reg(&reg).expect("resolved peer value");
    (
        value.root_schema.minimum_supported_version,
        value.root_schema.supported_version,
    )
}

/// Reads the converged scheduling state for one peer row from the local peer store.
fn stored_peer_scheduling(node: &TestNode, peer_id: Uuid) -> Option<PeerSchedulingState> {
    let reg = node.node.peers.get_reg(&UuidKey::from(peer_id)).ok()??;
    let value = PeerValue::select_reg(&reg)?;
    Some(value.scheduling)
}

/// Reads the converged runtime support profile for one peer row from the local peer store.
fn stored_peer_runtime_support(node: &TestNode, peer_id: Uuid) -> Option<RuntimeSupportProfile> {
    let reg = node.node.peers.get_reg(&UuidKey::from(peer_id)).ok()??;
    let value = PeerValue::select_reg(&reg)?;
    Some(value.runtime_support)
}

/// Mutates one local peer row directly so sync can exercise a runtime-support-only root change.
async fn update_peer_runtime_support(
    node: &TestNode,
    target: Uuid,
    runtime_support: RuntimeSupportProfile,
) {
    let key = UuidKey::from(target);
    let reg = node
        .node
        .peers
        .get_reg(&key)
        .expect("read peer register")
        .expect("peer register present");
    let mut value = PeerValue::select_reg(&reg).expect("resolved peer value");
    value.runtime_support = runtime_support;
    node.node
        .peers
        .upsert(&key, value)
        .await
        .expect("update peer runtime support");
}

/// Reads the peer-domain root digest served by one node for the requested schema version.
async fn peers_root_hex_at_version(
    node: &TestNode,
    root_schema_version: u32,
) -> Result<String, capnp::Error> {
    let cluster_view = active_cluster_view(node).await?;

    let mut request = node.node.sync_client.get_roots_for_view_request();
    {
        let mut req = request.get().init_req();
        cluster_view.write_capnp(req.reborrow().init_view());
        req.set_root_schema_version(root_schema_version);
    }

    let response = request.send().promise.await?;
    let roots = response.get()?.get_roots()?;
    for idx in 0..roots.len() {
        let entry = roots.get(idx);
        if matches!(entry.get_domain(), Ok(Domain::Peers)) {
            return Ok(bytes_to_hex(entry.get_root_digest()?));
        }
    }

    Err(capnp::Error::failed(
        "missing peers root in sync roots response".to_string(),
    ))
}

/// Reads the active cluster view from the topology service.
async fn active_cluster_view(node: &TestNode) -> Result<ClusterViewId, capnp::Error> {
    let view_response = node
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await?;
    let cluster_view = ClusterViewId::from_capnp(view_response.get()?.get_view()?)
        .map_err(capnp::Error::failed)?;
    Ok(cluster_view)
}

/// Requests peer-domain range summaries for one explicit root schema version.
async fn request_peers_ranges_at_version(
    node: &TestNode,
    root_schema_version: u32,
) -> Result<(), capnp::Error> {
    let cluster_view = active_cluster_view(node).await?;

    let mut request = node.node.sync_client.get_ranges_for_view_request();
    {
        let mut req = request.get().init_req();
        cluster_view.write_capnp(req.reborrow().init_view());
        req.set_root_schema_version(root_schema_version);
        let mut domains = req.reborrow().init_domains(1);
        domains.set(0, Domain::Peers);
    }

    request.send().promise.await?;
    Ok(())
}

/// Opens an empty delta stream for one explicit root schema version.
async fn open_empty_delta_at_version(
    node: &TestNode,
    root_schema_version: u32,
) -> Result<(), capnp::Error> {
    let cluster_view = active_cluster_view(node).await?;

    let mut request = node.node.sync_client.open_delta_for_view_request();
    {
        let mut req = request.get().init_req();
        cluster_view.write_capnp(req.reborrow().init_view());
        req.set_root_schema_version(root_schema_version);
        req.reborrow().init_wants(0);
        req.set_sink(capnp_rpc::new_client(NoopDeltaSink));
    }

    request.send().promise.await?;
    Ok(())
}

/// Opens a delta stream whose top-level request and domain want disagree on root schema.
async fn open_delta_with_mismatched_want_version(
    node: &TestNode,
    request_root_schema_version: u32,
    want_root_schema_version: u32,
) -> Result<(), capnp::Error> {
    let cluster_view = active_cluster_view(node).await?;

    let mut request = node.node.sync_client.open_delta_for_view_request();
    {
        let mut req = request.get().init_req();
        cluster_view.write_capnp(req.reborrow().init_view());
        req.set_root_schema_version(request_root_schema_version);
        let mut wants = req.reborrow().init_wants(1);
        let mut want = wants.reborrow().get(0);
        want.set_domain(Domain::Peers);
        cluster_view.write_capnp(want.reborrow().init_view());
        want.set_root_schema_version(want_root_schema_version);
        want.reborrow().init_want();
        req.set_sink(capnp_rpc::new_client(NoopDeltaSink));
    }

    request.send().promise.await?;
    Ok(())
}

/// Marks one node drained through the real topology RPC with an optional stop-timeout override.
async fn drain_node_with_timeout(
    node: &TestNode,
    target: Uuid,
    reason: &str,
    task_stop_timeout_secs: Option<u32>,
) {
    let mut request = node.topology().drain_node_request();
    {
        let mut params = request.get();
        params
            .reborrow()
            .init_node_id()
            .set_bytes(target.as_bytes());
        params.set_reason(reason);
        params.set_task_stop_timeout_secs(task_stop_timeout_secs.unwrap_or(0));
    }
    request.send().promise.await.expect("drainNode send");
}

/// Marks one node drained through the real topology RPC so sync exercises a root-visible field.
async fn drain_node(node: &TestNode, target: Uuid, reason: &str) {
    drain_node_with_timeout(node, target, reason, None).await;
}

// Validates that peers with no root schema overlap cannot join the same cluster.
local_test!(root_schema_join_rejects_without_overlap, {
    let anchor = new_node_with_root_schema(1, 1).await;
    let joiner = new_node_with_root_schema(2, 2).await;

    let error = joiner
        .join(&anchor)
        .await
        .expect_err("join must fail without root schema overlap");
    assert!(
        error.to_string().contains("root schema mismatch")
            || error
                .to_string()
                .contains("compatible root schema version overlap"),
        "unexpected join error: {error}"
    );
});

// Validates every sync entry point rejects root schema versions outside local support.
local_test!(
    root_schema_rejects_unsupported_version_on_all_sync_entrypoints,
    {
        let node = new_node_with_root_schema(1, 1).await;

        let roots_error = peers_root_hex_at_version(&node, 2)
            .await
            .expect_err("unsupported roots request should fail");
        assert!(
            roots_error
                .to_string()
                .contains("root schema version 2 is unsupported"),
            "unexpected roots error: {roots_error}"
        );

        let ranges_error = request_peers_ranges_at_version(&node, 2)
            .await
            .expect_err("unsupported ranges request should fail");
        assert!(
            ranges_error
                .to_string()
                .contains("root schema version 2 is unsupported"),
            "unexpected ranges error: {ranges_error}"
        );

        let delta_error = open_empty_delta_at_version(&node, 2)
            .await
            .expect_err("unsupported delta request should fail");
        assert!(
            delta_error
                .to_string()
                .contains("root schema version 2 is unsupported"),
            "unexpected delta error: {delta_error}"
        );
    }
);

// Validates delta requests reject wants whose root schema disagrees with the request.
local_test!(root_schema_delta_rejects_mismatched_want_version, {
    let node = new_node_with_root_schema(1, 2).await;

    let error = open_delta_with_mismatched_want_version(&node, 1, 2)
        .await
        .expect_err("mismatched want root schema should fail");

    assert!(
        error
            .to_string()
            .contains("domain want root schema mismatch: expected 1, got 2"),
        "unexpected mismatched want error: {error}"
    );
});

// Validates a rolling upgrade keeps mixed-version peers compatible and recovers missed updates.
local_test!(root_schema_upgrade_overlap_recovers_missed_updates, {
    let anchor = new_node_with_root_schema(1, 1).await;
    let mut upgraded = new_node_with_root_schema(1, 2).await;

    upgraded.join(&anchor).await.expect("join upgraded peer");
    anchor
        .assert_cluster_size(2, "anchor should see upgraded peer")
        .await;
    upgraded
        .assert_cluster_size(2, "upgraded peer should see anchor")
        .await;

    assert_eq!(stored_peer_root_schema(&anchor, upgraded.id()), (1, 2));
    assert_eq!(stored_peer_root_schema(&upgraded, anchor.id()), (1, 1));

    assert!(
        peers_root_hex_at_version(&anchor, 2).await.is_err(),
        "old peer must reject unsupported v2 root requests"
    );
    let upgraded_root_v2 = peers_root_hex_at_version(&upgraded, 2)
        .await
        .expect("upgraded peer serves v2 roots");
    assert!(!upgraded_root_v2.is_empty());

    upgraded.node.stop_cluster_background_tasks();
    upgraded.stop().await.expect("stop upgraded peer");

    drain_node(&anchor, anchor.id(), "upgrade-root-schema").await;

    upgraded
        .start()
        .await
        .expect("restart upgraded peer listener");
    upgraded.node.ensure_cluster_background_tasks();
    anchor.node.sync_once_now();
    upgraded.node.sync_once_now();

    let converged = wait_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async {
            stored_peer_scheduling(&upgraded, anchor.id())
                .map(|state| {
                    !state.schedulable
                        && state.drain_requested
                        && state.reason.as_deref() == Some("upgrade-root-schema")
                })
                .unwrap_or(false)
        },
    )
    .await;
    assert!(
        converged,
        "upgraded peer did not recover missed scheduling update through mixed-version sync"
    );
});

// Validates a downgraded peer can still interoperate with newer peers through the overlap range.
local_test!(root_schema_downgrade_overlap_recovers_missed_updates, {
    let anchor = new_node_with_root_schema(1, 2).await;
    let mut downgraded = new_node_with_root_schema(1, 1).await;

    downgraded
        .join(&anchor)
        .await
        .expect("join downgraded peer");
    anchor
        .assert_cluster_size(2, "new anchor should see downgraded peer")
        .await;
    downgraded
        .assert_cluster_size(2, "downgraded peer should see anchor")
        .await;

    assert_eq!(stored_peer_root_schema(&anchor, downgraded.id()), (1, 1));
    assert_eq!(stored_peer_root_schema(&downgraded, anchor.id()), (1, 2));

    let anchor_root_v2 = peers_root_hex_at_version(&anchor, 2)
        .await
        .expect("new anchor serves v2 roots");
    assert!(!anchor_root_v2.is_empty());
    assert!(
        peers_root_hex_at_version(&downgraded, 2).await.is_err(),
        "downgraded peer must reject unsupported v2 root requests"
    );

    downgraded.node.stop_cluster_background_tasks();
    downgraded.stop().await.expect("stop downgraded peer");

    drain_node(&anchor, anchor.id(), "downgrade-root-schema").await;

    downgraded
        .start()
        .await
        .expect("restart downgraded peer listener");
    downgraded.node.ensure_cluster_background_tasks();
    anchor.node.sync_once_now();
    downgraded.node.sync_once_now();

    let converged = wait_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async {
            stored_peer_scheduling(&downgraded, anchor.id())
                .map(|state| {
                    !state.schedulable
                        && state.drain_requested
                        && state.reason.as_deref() == Some("downgrade-root-schema")
                })
                .unwrap_or(false)
        },
    )
    .await;
    assert!(
        converged,
        "downgraded peer did not recover missed scheduling update through overlap-version sync"
    );

    let roots_equal_at_v1 = wait_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async {
            let anchor_root = peers_root_hex_at_version(&anchor, 1).await.ok();
            let downgraded_root = peers_root_hex_at_version(&downgraded, 1).await.ok();
            match (anchor_root, downgraded_root) {
                (Some(left), Some(right)) => !left.is_empty() && left == right,
                _ => false,
            }
        },
    )
    .await;
    assert!(
        roots_equal_at_v1,
        "downgrade recovery should converge through the shared v1 root projection"
    );
});

// Validates that once every peer supports the newer projection, both sides can serve it.
local_test!(root_schema_all_upgraded_peers_serve_latest_projection, {
    let anchor = new_node_with_root_schema(1, 2).await;
    let upgraded = new_node_with_root_schema(1, 2).await;

    upgraded.join(&anchor).await.expect("join upgraded peer");
    anchor
        .assert_cluster_size(2, "anchor should see upgraded peer")
        .await;
    upgraded
        .assert_cluster_size(2, "upgraded peer should see anchor")
        .await;

    assert_eq!(stored_peer_root_schema(&anchor, upgraded.id()), (1, 2));
    assert_eq!(stored_peer_root_schema(&upgraded, anchor.id()), (1, 2));

    let roots_equal = wait_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async {
            let anchor_root = peers_root_hex_at_version(&anchor, 2).await.ok();
            let upgraded_root = peers_root_hex_at_version(&upgraded, 2).await.ok();
            match (anchor_root, upgraded_root) {
                (Some(left), Some(right)) => !left.is_empty() && left == right,
                _ => false,
            }
        },
    )
    .await;
    assert!(
        roots_equal,
        "fully upgraded peers did not converge on a shared v2 root projection"
    );
});

// Validates a real production peer-domain subfield stays root-neutral in v1 and root-visible in v2.
local_test!(
    root_schema_peer_runtime_support_only_affects_v2_projection,
    {
        let anchor = new_node_with_root_schema(1, 2).await;

        let anchor_root_v1_before = peers_root_hex_at_version(&anchor, 1)
            .await
            .expect("anchor serves v1 roots");
        let anchor_root_v2_before = peers_root_hex_at_version(&anchor, 2)
            .await
            .expect("anchor serves v2 roots");

        anchor.node.stop_cluster_background_tasks();
        update_peer_runtime_support(
            &anchor,
            anchor.id(),
            RuntimeSupportProfile::new(
                [ExecutionPlatform::Oci],
                [IsolationMode::Standard, IsolationMode::Sandboxed],
                ["default", "oci-default"],
                [
                    "exec",
                    "interactive_exec",
                    "logs",
                    "runtime.feature.root-schema-v2",
                ],
            ),
        )
        .await;

        let updated_support = stored_peer_runtime_support(&anchor, anchor.id())
            .expect("updated local runtime support");
        assert!(
            updated_support
                .feature_flags
                .iter()
                .any(|flag| flag == "runtime.feature.root-schema-v2"),
            "local runtime support update should persist before root comparison"
        );

        let anchor_root_v1_after = peers_root_hex_at_version(&anchor, 1)
            .await
            .expect("anchor serves v1 roots after runtime support update");
        let anchor_root_v2_after = peers_root_hex_at_version(&anchor, 2)
            .await
            .expect("anchor serves v2 roots after runtime support update");

        assert_eq!(
            anchor_root_v1_before, anchor_root_v1_after,
            "v1 peer roots should ignore runtime-support-only updates"
        );
        assert_ne!(
            anchor_root_v2_before, anchor_root_v2_after,
            "v2 peer roots should include runtime support updates"
        );
        assert_ne!(
            anchor_root_v1_after, anchor_root_v2_after,
            "v1 and v2 roots should diverge once runtime support becomes root-visible"
        );
    }
);

// Validates a same-identity restart can change the advertised root schema range cleanly.
local_test!(
    root_schema_restart_updates_advertised_range_for_same_peer,
    {
        let anchor = new_node_with_root_schema(1, 2).await;

        let state_dir = tempdir().expect("root schema restart state dir");
        let db_path = state_dir.path().join("state.redb");
        let db = Arc::new(redb::Database::create(db_path).expect("create redb"));
        let self_id = Uuid::new_v4();
        let keys = HeadlessKeys::new(
            Arc::new(NoiseKeys::from_private_bytes([0x41; 32])),
            ed25519_dalek::SigningKey::from_bytes(&[0x51; 32]),
        );
        let local_volume_root = state_dir.path().join("volumes");

        let mut peer = TestNode {
            node: Box::new(
                create_restartable_node_with_root_schema(
                    db.clone(),
                    self_id,
                    keys.clone(),
                    local_volume_root.clone(),
                    1,
                    2,
                )
                .await,
            ),
        };

        peer.join(&anchor).await.expect("join restartable peer");
        anchor
            .assert_cluster_size(2, "anchor should see restartable peer")
            .await;
        assert_eq!(stored_peer_root_schema(&anchor, self_id), (1, 2));

        peer.stop().await.expect("stop restartable peer");
        drop(peer);

        let restarted = TestNode {
            node: Box::new(
                create_restartable_node_with_root_schema(
                    db,
                    self_id,
                    keys,
                    local_volume_root,
                    1,
                    1,
                )
                .await,
            ),
        };

        anchor.node.sync_once_now();
        restarted.node.sync_once_now();

        let updated = wait_until(
            Duration::from_secs(10),
            Duration::from_millis(50),
            || async { stored_peer_root_schema(&anchor, self_id) == (1, 1) },
        )
        .await;
        assert!(
            updated,
            "anchor did not learn the downgraded root schema range after same-identity restart"
        );
    }
);
