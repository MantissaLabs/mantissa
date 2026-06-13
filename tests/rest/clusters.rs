use axum::http::{Method, StatusCode};

use crate::common;
use crate::harness::RestTestHarness;

local_test!(rest_clusters_use_real_local_session, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(Method::GET, "/v1/clusters", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let clusters = value.as_array().expect("clusters response is array");
    assert_eq!(clusters.len(), 1);
    assert_eq!(clusters[0]["node_count"], 1);
    assert_eq!(clusters[0]["local_active"], true);
    let cluster_id = clusters[0]["cluster_id"]
        .as_str()
        .expect("cluster summary id")
        .to_string();

    let (status, value) = harness
        .json_request(Method::GET, "/v1/clusters/views", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("cluster views response is array")
            .iter()
            .any(|view| view["view"]["cluster_id"] == cluster_id && view["node_count"] == 1)
    );

    let (status, value) = harness
        .json_request(Method::GET, "/v1/clusters/current", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["cluster_id"], cluster_id);

    let (status, value) = harness
        .json_request(Method::GET, "/v1/clusters/split-candidates", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let source_cluster_id = value["source_view"]["cluster_id"]
        .as_str()
        .expect("split candidates include source cluster")
        .to_string();
    assert_eq!(
        value["candidates"]
            .as_array()
            .expect("split candidates are array")
            .len(),
        1
    );

    let (status, value) = harness
        .json_request(
            Method::GET,
            &format!("/v1/clusters/{source_cluster_id}/split-candidates"),
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["source_view"]["cluster_id"], source_cluster_id);
});
