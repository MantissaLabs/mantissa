use axum::http::{Method, StatusCode};
use mantissa::{
    ingress::types::{IngressPoolSpecDraft, IngressPoolSpecValue},
    scheduler::placement::PlacementPolicy,
    services::types::{
        PublicIngressPolicy, ServicePortProtocol, ServiceSpecValue, TaskTemplateNetworkRequirement,
        TaskTemplateSpecValue,
    },
    workload::types::ExecutionSpec,
};
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

use crate::common;
use crate::common::convergence::wait_until;
use crate::harness::RestTestHarness;

const REST_INGRESS_SERVICE_NAME: &str = "rest-ingress-endpoints";
const REST_INGRESS_TEMPLATE_NAME: &str = "api";
const REST_INGRESS_POOL_NAME: &str = "public-web";
const REST_INGRESS_PUBLIC_PORT: u16 = 8080;
const REST_INGRESS_NOT_REPORTED_DETAIL: &str = "endpoint has not been reported by the source node";

local_test!(rest_ingress_pool_crud_and_empty_endpoints, {
    let harness = RestTestHarness::new().await;

    let request = json!({
        "name": "public-web",
        "min_nodes": 1,
        "max_nodes": 2,
        "placement": {
            "constraints": [],
            "strategy": "spread"
        }
    });
    let (status, value) = harness
        .json_request(Method::PUT, "/v1/ingress", true, Some(request))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["name"], "public-web");
    assert_eq!(value["min_nodes"], 1);
    assert_eq!(value["max_nodes"], 2);
    assert_eq!(value["placement_strategy"], "spread");

    let (status, value) = harness
        .json_request(Method::GET, "/v1/ingress", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("ingress list response is array")
            .iter()
            .any(|pool| pool["name"] == "public-web")
    );

    let (status, value) = harness
        .json_request(Method::GET, "/v1/ingress/public-web", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["name"], "public-web");

    let (status, value) = harness
        .json_request(
            Method::GET,
            "/v1/ingress/endpoints?pool=public-web&ready=true",
            true,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        value
            .as_array()
            .expect("endpoint response is array")
            .is_empty()
    );

    let (status, value) = harness
        .json_request(Method::DELETE, "/v1/ingress/public-web", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["deleted"], 1);

    let (status, value) = harness
        .json_request(Method::GET, "/v1/ingress/public-web", true, None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");
});

local_test!(rest_ingress_endpoints_filters_expected_rows, {
    let harness = RestTestHarness::new().await;
    let (service_id, network_id) = seed_rest_ingress_endpoint_fixture(&harness).await;

    let uri = format!(
        "/v1/ingress/endpoints?service={REST_INGRESS_SERVICE_NAME}&template={REST_INGRESS_TEMPLATE_NAME}&pool={REST_INGRESS_POOL_NAME}&port={REST_INGRESS_PUBLIC_PORT}"
    );
    let converged = wait_until(Duration::from_secs(5), Duration::from_millis(50), || {
        let harness = &harness;
        let uri = uri.clone();
        async move {
            let (status, value) = harness
                .json_request(Method::GET, uri.as_str(), true, None)
                .await;
            status == StatusCode::OK
                && value
                    .as_array()
                    .map(|rows| rows.len() == 1)
                    .unwrap_or(false)
        }
    })
    .await;
    assert!(
        converged,
        "seeded ingress endpoint row should become visible through REST"
    );

    let (status, value) = harness
        .json_request(Method::GET, uri.as_str(), true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let rows = value.as_array().expect("endpoint response is array");
    assert_eq!(rows.len(), 1);
    let endpoint = &rows[0];
    assert_eq!(endpoint["service_id"], service_id.to_string());
    assert_eq!(endpoint["service_name"], REST_INGRESS_SERVICE_NAME);
    assert_eq!(endpoint["template_name"], REST_INGRESS_TEMPLATE_NAME);
    assert_eq!(endpoint["network_id"], network_id.to_string());
    assert_eq!(endpoint["node_id"], harness.node_id.to_string());
    assert_eq!(endpoint["node_ip"], serde_json::Value::Null);
    assert_eq!(endpoint["public_port"], REST_INGRESS_PUBLIC_PORT);
    assert_eq!(endpoint["protocol"], "tcp");
    assert_eq!(endpoint["ingress_mode"], "ingress_pool");
    assert_eq!(endpoint["ingress_pool"], REST_INGRESS_POOL_NAME);
    assert_eq!(endpoint["ready"], false);
    assert_eq!(endpoint["generation"], 0);
    assert_eq!(endpoint["detail"], REST_INGRESS_NOT_REPORTED_DETAIL);

    let by_id_uri = format!("/v1/ingress/endpoints?service={service_id}");
    assert_endpoint_count(&harness, by_id_uri.as_str(), 1).await;
    assert_endpoint_count(&harness, "/v1/ingress/endpoints?template=worker", 0).await;
    assert_endpoint_count(&harness, "/v1/ingress/endpoints?pool=private", 0).await;
    assert_endpoint_count(&harness, "/v1/ingress/endpoints?port=9090", 0).await;
    assert_endpoint_count(
        &harness,
        format!("/v1/ingress/endpoints?service={REST_INGRESS_SERVICE_NAME}&ready=true").as_str(),
        0,
    )
    .await;
});

/// Seeds one ingress pool and one running service intent for REST endpoint filtering tests.
async fn seed_rest_ingress_endpoint_fixture(harness: &RestTestHarness) -> (Uuid, Uuid) {
    let network_id = Uuid::new_v4();
    let pool = IngressPoolSpecValue::from_draft(IngressPoolSpecDraft {
        name: REST_INGRESS_POOL_NAME.to_string(),
        min_nodes: 1,
        max_nodes: Some(1),
        placement: PlacementPolicy::default(),
        spread_by: None,
    })
    .expect("build ingress pool spec");
    harness
        .node()
        .node
        .ingress_pool_registry
        .upsert(pool)
        .await
        .expect("seed ingress pool");

    let service = rest_ingress_endpoint_service(network_id);
    let service_id = service.id;
    harness
        .node()
        .node
        .service_controller
        .registry()
        .upsert(service)
        .await
        .expect("seed service spec");
    (service_id, network_id)
}

/// Builds one service intent whose public endpoint target is derived from an ingress pool.
fn rest_ingress_endpoint_service(network_id: Uuid) -> ServiceSpecValue {
    let template = TaskTemplateSpecValue {
        name: REST_INGRESS_TEMPLATE_NAME.to_string(),
        execution: ExecutionSpec {
            image: "ghcr.io/mantissa/rest-ingress-test:latest".to_string(),
            command: Vec::new(),
            tty: false,
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: vec![TaskTemplateNetworkRequirement::new("default", network_id)],
            ports: Vec::new(),
            placement: PlacementPolicy::default(),
        },
        depends_on: Vec::new(),
        replicas: 1,
        readiness: None,
        public_port: Some(REST_INGRESS_PUBLIC_PORT),
        public_protocol: Some(ServicePortProtocol::Tcp),
        public_ingress: PublicIngressPolicy::IngressPool {
            pool: REST_INGRESS_POOL_NAME.to_string(),
        },
        placement_preferences: Vec::new(),
        autoscale: None,
    };

    ServiceSpecValue::new(
        Uuid::new_v4(),
        REST_INGRESS_SERVICE_NAME,
        REST_INGRESS_SERVICE_NAME,
        vec![template],
        vec![Uuid::new_v4()],
    )
}

/// Calls the endpoint route and asserts the number of returned target rows.
async fn assert_endpoint_count(harness: &RestTestHarness, uri: &str, expected: usize) {
    let (status, value) = harness.json_request(Method::GET, uri, true, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        value.as_array().expect("endpoint response is array").len(),
        expected,
        "unexpected endpoint row count for {uri}; response={value}"
    );
}
