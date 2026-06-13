use axum::http::{Method, StatusCode};

use crate::common;
use crate::harness::RestTestHarness;

local_test!(rest_clusters_use_real_local_session, {
    let harness = RestTestHarness::new().await;

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
