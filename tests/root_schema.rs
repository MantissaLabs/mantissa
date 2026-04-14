#[macro_use]
mod common;

use common::convergence::wait_until;
use common::testkit::TestNode;
use crdt_store::uuid_key::UuidKey;
use mantissa::cluster::{ClusterViewId, RootSchemaState};
use mantissa::runtime::set::RuntimeSet;
use mantissa::runtime::testing::IN_MEMORY_RUNTIME_BACKEND_KIND;
use mantissa::runtime::testing::new_in_memory_runtime_backend;
use mantissa::server::headless::{HeadlessConfig, HeadlessKeys, HeadlessNode, HeadlessTransport};
use mantissa::topology::peers::PeerValue;
use net::noise::NoiseKeys;
use protocol::sync::Domain;
use std::path::PathBuf;
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
        runtime_set: Some(RuntimeSet::singleton(
            IN_MEMORY_RUNTIME_BACKEND_KIND,
            new_in_memory_runtime_backend(),
        )),
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
            runtime_set: Some(RuntimeSet::singleton(
                IN_MEMORY_RUNTIME_BACKEND_KIND,
                new_in_memory_runtime_backend(),
            )),
            local_volume_root: Some(local_volume_root),
        },
    )
    .await
    .expect("restartable root schema node")
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

/// Reads the peer-domain root digest served by one node for the requested schema version.
async fn peers_root_hex_at_version(
    node: &TestNode,
    root_schema_version: u32,
) -> Result<String, capnp::Error> {
    let view_response = node
        .topology()
        .get_cluster_view_request()
        .send()
        .promise
        .await?;
    let cluster_view = ClusterViewId::from_capnp(view_response.get()?.get_view()?)
        .map_err(capnp::Error::failed)?;

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

/// Applies one label set to the selected node through the real topology RPC.
async fn set_node_labels(node: &TestNode, target: Uuid, labels: &[&str]) {
    let mut request = node.topology().set_node_labels_request();
    {
        let mut params = request.get();
        params
            .reborrow()
            .init_node_id()
            .set_bytes(target.as_bytes());
        let mut entries = params.reborrow().init_labels(labels.len() as u32);
        for (idx, label) in labels.iter().enumerate() {
            entries.set(idx as u32, label);
        }
        params.reborrow().init_remove_keys(0);
        params.set_replace(true);
    }
    request.send().promise.await.expect("setNodeLabels send");
}

/// Reads the labels exposed by `Topology.list` for one node id.
async fn listed_node_labels(node: &TestNode, target: Uuid) -> Option<Vec<String>> {
    let response = node.topology().list_request().send().promise.await.ok()?;
    let rows = response.get().ok()?.get_nodes().ok()?.get_nodes().ok()?;
    for row in rows.iter() {
        let listed_id = Uuid::from_slice(row.get_id().ok()?.get_bytes().ok()?).ok()?;
        if listed_id != target {
            continue;
        }

        let labels = row.get_labels().ok()?;
        let mut out = Vec::with_capacity(labels.len() as usize);
        for label in labels.iter() {
            let text = label.ok()?.to_str().ok()?.trim().to_string();
            if !text.is_empty() {
                out.push(text);
            }
        }
        out.sort();
        return Some(out);
    }

    None
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

    set_node_labels(&anchor, anchor.id(), &["upgrade=ready"]).await;

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
            listed_node_labels(&upgraded, anchor.id()).await
                == Some(vec!["upgrade=ready".to_string()])
        },
    )
    .await;
    assert!(
        converged,
        "upgraded peer did not recover missed label update through mixed-version sync"
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

    set_node_labels(&anchor, anchor.id(), &["downgrade=ready"]).await;

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
            listed_node_labels(&downgraded, anchor.id()).await
                == Some(vec!["downgrade=ready".to_string()])
        },
    )
    .await;
    assert!(
        converged,
        "downgraded peer did not recover missed label update through overlap-version sync"
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
