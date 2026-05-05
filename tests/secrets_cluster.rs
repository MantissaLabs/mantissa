#[macro_use]
mod common;

use common::convergence::wait_until;
use common::testkit::{RuntimeBackendOverrideGuard, TestNode};
use mantissa_protocol::secrets::secrets;
use std::time::Duration;

/// Creates a secret through the public RPC so encryption uses the node's live keyring.
async fn create_secret(
    client: &secrets::Client,
    name: &str,
    plaintext: &[u8],
) -> Result<(), capnp::Error> {
    let mut request = client.create_request();
    {
        let mut inner = request.get().init_request();
        inner.set_name(name);
        inner.set_plaintext(plaintext);
        inner.set_description("");
        inner.init_metadata(0);
    }
    request.send().promise.await?.get()?.get_secret()?;
    Ok(())
}

/// Fetches secret plaintext through the public RPC, proving local decryption works.
async fn fetch_secret_plaintext(
    client: &secrets::Client,
    name: &str,
) -> Result<Vec<u8>, capnp::Error> {
    let mut request = client.get_request();
    {
        let mut params = request.get();
        params.set_name(name);
        params.set_version_id(&[]);
    }
    let response = request.send().promise.await?;
    let plaintext = response.get()?.get_version()?.get_plaintext()?.to_vec();
    Ok(plaintext)
}

/// Waits until a replicated secret is present and decryptable by the target node.
async fn wait_for_plaintext(
    client: &secrets::Client,
    name: &str,
    expected: &[u8],
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(50), || async {
        matches!(
            fetch_secret_plaintext(client, name).await,
            Ok(plaintext) if plaintext == expected
        )
    })
    .await
}

/// Rotates the cluster master key through the public secrets RPC.
async fn rotate_master_key(client: &secrets::Client) -> Result<(), capnp::Error> {
    client
        .rotate_master_key_request()
        .send()
        .promise
        .await?
        .get()?;
    Ok(())
}

local_test!(master_key_exchange_supports_three_node_secret_decryption, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let anchor = TestNode::new_with_tick_ms(100).await;
    let first_secret = b"created-before-joins";
    create_secret(
        &anchor.node.secrets_client,
        "pre-join-cluster-secret",
        first_secret,
    )
    .await
    .expect("create pre-join secret on anchor");

    let second = TestNode::new_with_tick_ms(100).await;
    second.join(&anchor).await.expect("second joins anchor");
    anchor
        .assert_cluster_size(2, "anchor sees second after first join")
        .await;
    second
        .assert_cluster_size(2, "second sees anchor after first join")
        .await;

    assert!(
        wait_for_plaintext(
            &second.node.secrets_client,
            "pre-join-cluster-secret",
            first_secret,
            Duration::from_secs(10),
        )
        .await,
        "second node should decrypt the anchor secret after master-key transfer"
    );

    let third = TestNode::new_with_tick_ms(100).await;
    third
        .join(&second)
        .await
        .expect("third joins through second");
    let cluster = [anchor, second, third];
    TestNode::assert_cluster_size_all(&cluster, 3, "three-node cluster after chained join").await;

    for node in &cluster {
        assert!(
            wait_for_plaintext(
                &node.node.secrets_client,
                "pre-join-cluster-secret",
                first_secret,
                Duration::from_secs(10),
            )
            .await,
            "node {} should decrypt the anchor-created secret",
            node.id()
        );
    }

    let third_secret = b"created-after-third-join";
    create_secret(
        &cluster[2].node.secrets_client,
        "post-join-cluster-secret",
        third_secret,
    )
    .await
    .expect("create post-join secret on third node");

    for node in &cluster {
        assert!(
            wait_for_plaintext(
                &node.node.secrets_client,
                "post-join-cluster-secret",
                third_secret,
                Duration::from_secs(10),
            )
            .await,
            "node {} should decrypt the third-created secret",
            node.id()
        );
    }
});

local_test!(master_key_rotation_replicates_through_sync_domain, {
    let _guard = RuntimeBackendOverrideGuard::install_default();

    let anchor = TestNode::new_with_tick_ms(100).await;
    let second = TestNode::new_with_tick_ms(100).await;
    second.join(&anchor).await.expect("second joins anchor");
    let cluster = [anchor, second];
    TestNode::assert_cluster_size_all(&cluster, 2, "two-node cluster before rotation").await;

    let secret = b"rotate-through-sync";
    create_secret(&cluster[0].node.secrets_client, "rotated-secret", secret)
        .await
        .expect("create secret before rotation");
    assert!(
        wait_for_plaintext(
            &cluster[1].node.secrets_client,
            "rotated-secret",
            secret,
            Duration::from_secs(10),
        )
        .await,
        "second node should decrypt the pre-rotation secret"
    );

    rotate_master_key(&cluster[0].node.secrets_client)
        .await
        .expect("rotate master key on anchor");

    for node in &cluster {
        assert!(
            wait_for_plaintext(
                &node.node.secrets_client,
                "rotated-secret",
                secret,
                Duration::from_secs(10),
            )
            .await,
            "node {} should decrypt the rewrapped secret after replicated key sync",
            node.id()
        );
    }
});
