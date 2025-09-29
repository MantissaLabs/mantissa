#[macro_use]
mod common;

use std::time::{Duration, Instant};

use common::testkit::{ClusterConfig, TestNode};
use mantissa::services::ServiceManager;
use mantissa::services::types::compute_service_id;
use protocol::services::services;
use tokio::time::sleep;
use uuid::Uuid;

local_test!(services_gossip_propagates_across_peers, {
    const SERVICE_NAME: &str = "demo-service";
    const MANIFEST_NAME: &str = "demo-manifest";

    let cluster = TestNode::new_cluster_inproc_with_config(2, ClusterConfig::default())
        .await
        .expect("cluster should start");
    TestNode::assert_cluster_size_all(&cluster, 2, "cluster should stabilise to two nodes").await;

    let anchor = &cluster[0];
    let peer = &cluster[1];

    let manifest_id = Uuid::new_v4();
    let service_id = compute_service_id(SERVICE_NAME);

    register_service_via_rpc(
        &anchor.node.services_client,
        manifest_id,
        MANIFEST_NAME,
        SERVICE_NAME,
    )
    .await;

    assert!(
        wait_for_service_state(&anchor.node.service_manager, service_id, true).await,
        "anchor should observe newly registered service"
    );
    assert!(
        wait_for_service_state(&peer.node.service_manager, service_id, true).await,
        "peer should receive service via gossip"
    );

    let peer_ids = list_service_ids(&peer.node.services_client).await;
    assert!(
        peer_ids.contains(&service_id),
        "peer Services.list should report gossiped service"
    );

    remove_service_via_rpc(&anchor.node.services_client, service_id).await;

    assert!(
        wait_for_service_state(&anchor.node.service_manager, service_id, false).await,
        "anchor should remove service after delete"
    );
    assert!(
        wait_for_service_state(&peer.node.service_manager, service_id, false).await,
        "peer should drop service after gossip remove"
    );

    let peer_ids = list_service_ids(&peer.node.services_client).await;
    assert!(
        peer_ids.is_empty(),
        "peer service listing should be empty after removal"
    );
});

async fn register_service_via_rpc(
    client: &services::Client,
    manifest_id: Uuid,
    manifest_name: &str,
    service_name: &str,
) {
    let mut upsert = client.upsert_request();
    {
        let mut specs = upsert.get().init_specs(1);
        let mut spec = specs.reborrow().get(0);
        spec.set_manifest_id(manifest_id.as_bytes());
        spec.set_manifest_name(manifest_name);
        spec.set_service_name(service_name);

        let mut tasks = spec.reborrow().init_tasks(1);
        let mut task = tasks.reborrow().get(0);
        task.set_name("web");
        task.set_image("ghcr.io/mantissa/demo:web");
        task.set_replicas(1);
        let mut command = task.reborrow().init_command(1);
        command.set(0, "--serve");

        spec.reborrow().init_workload_ids(0);
    }

    upsert
        .send()
        .promise
        .await
        .expect("service upsert should succeed");
}

async fn remove_service_via_rpc(client: &services::Client, service_id: Uuid) {
    let mut delete = client.delete_request();
    {
        let mut ids = delete.get().init_ids(1);
        ids.set(0, service_id.as_bytes());
    }
    delete
        .send()
        .promise
        .await
        .expect("service delete should succeed");
}

async fn wait_for_service_state(
    manager: &ServiceManager,
    service_id: Uuid,
    expect_present: bool,
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let specs = manager
            .list_services()
            .expect("service list should succeed during wait");
        let present = specs.iter().any(|spec| spec.id == service_id);
        if present == expect_present {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn list_service_ids(client: &services::Client) -> Vec<Uuid> {
    let response = client
        .list_request()
        .send()
        .promise
        .await
        .expect("Services.list call should succeed");
    let reader = response
        .get()
        .expect("Services.list should yield result message");
    let specs = reader
        .get_services()
        .expect("Services.list should include services list");

    let mut ids = Vec::with_capacity(specs.len() as usize);
    for spec in specs.iter() {
        let data = spec.get_id().expect("service id data").to_owned();
        if data.len() != 16 {
            continue;
        }
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&data);
        ids.push(Uuid::from_bytes(bytes));
    }

    ids
}
