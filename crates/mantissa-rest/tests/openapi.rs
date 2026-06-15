use mantissa_rest::{openapi, server};
use serde_json::Value;
use std::collections::BTreeSet;

/// Ensures the checked-in OpenAPI JSON matches the route-generated document.
#[test]
fn checked_in_openapi_spec_is_current() {
    let document = server::openapi();
    let generated = openapi::json_string(&document);
    let checked_in = include_str!("../openapi/mantissa-rest.openapi.json");

    assert_eq!(
        generated, checked_in,
        "regenerate with `cargo run -p mantissa-rest --bin generate-openapi`"
    );
}

/// Ensures the generated OpenAPI document covers every REST method/path pair.
#[test]
fn openapi_spec_covers_all_rest_endpoints() {
    let value = openapi::json_value(&server::openapi());
    let actual = documented_operations(&value);
    let expected = expected_operations();

    assert_eq!(actual, expected);
}

/// Ensures the generated OpenAPI document describes the REST security contract.
#[test]
fn openapi_spec_documents_security_contract() {
    let value = openapi::json_value(&server::openapi());

    assert_eq!(value["openapi"], openapi::OPENAPI_VERSION);
    assert!(value["components"]["securitySchemes"]["bearerAuth"].is_object());
    assert!(value["components"]["securitySchemes"]["mtls"].is_object());
    assert_eq!(
        value["paths"]["/healthz"]["get"]["security"],
        Value::Array(vec![])
    );
    assert!(value["paths"]["/v1/health"]["get"]["responses"]["401"].is_object());
}

/// Collects method/path pairs from the generated OpenAPI JSON.
fn documented_operations(value: &Value) -> BTreeSet<(String, String)> {
    let paths = value["paths"]
        .as_object()
        .expect("OpenAPI paths should be an object");
    let mut operations = BTreeSet::new();
    for (path, item) in paths {
        let methods = item.as_object().expect("path item should be an object");
        for method in methods.keys() {
            if is_http_method(method) {
                operations.insert((method.to_ascii_uppercase(), path.clone()));
            }
        }
    }
    operations
}

/// Returns the expected REST operation set exposed by the embedded API.
fn expected_operations() -> BTreeSet<(String, String)> {
    [
        ("DELETE", "/v1/agents/sessions/{session_id}"),
        ("DELETE", "/v1/jobs/{job_id}"),
        ("DELETE", "/v1/networks/{network_id}"),
        ("DELETE", "/v1/nodes/{node_id}"),
        ("DELETE", "/v1/secrets/{name}"),
        ("DELETE", "/v1/services/{selector}"),
        ("DELETE", "/v1/volumes/{selector}"),
        ("GET", "/healthz"),
        ("GET", "/v1/agents/sessions"),
        ("GET", "/v1/agents/sessions/{session_id}"),
        ("GET", "/v1/agents/sessions/{session_id}/runs"),
        ("GET", "/v1/clusters"),
        ("GET", "/v1/clusters/current"),
        ("GET", "/v1/clusters/operations/{operation_id}"),
        ("GET", "/v1/clusters/split-candidates"),
        ("GET", "/v1/clusters/views"),
        ("GET", "/v1/clusters/{cluster_id}/split-candidates"),
        ("GET", "/v1/health"),
        ("GET", "/v1/jobs"),
        ("GET", "/v1/jobs/{job_id}"),
        ("GET", "/v1/networks"),
        ("GET", "/v1/networks/{network_id}"),
        ("GET", "/v1/networks/{network_id}/attachments"),
        ("GET", "/v1/networks/{network_id}/peers"),
        ("GET", "/v1/nodes"),
        ("GET", "/v1/nodes/{node_id}"),
        ("GET", "/v1/nodes/{node_id}/drain"),
        ("GET", "/v1/scheduler/summary"),
        ("GET", "/v1/secrets"),
        ("GET", "/v1/secrets/{name}"),
        ("GET", "/v1/secrets/{name}/versions/{version_id}"),
        ("GET", "/v1/services"),
        ("GET", "/v1/services/{selector}"),
        ("GET", "/v1/services/{selector}/status"),
        ("GET", "/v1/tasks"),
        ("GET", "/v1/tasks/{selector}"),
        ("GET", "/v1/tasks/{selector}/attach"),
        ("GET", "/v1/tasks/{selector}/exec"),
        ("GET", "/v1/tasks/{selector}/logs"),
        ("GET", "/v1/volumes"),
        ("GET", "/v1/volumes/{selector}"),
        ("GET", "/v1/volumes/{selector}/status"),
        ("POST", "/v1/agents/sessions"),
        ("POST", "/v1/agents/sessions/{session_id}/cancel"),
        ("POST", "/v1/agents/sessions/{session_id}/close"),
        ("POST", "/v1/agents/sessions/{session_id}/input"),
        ("POST", "/v1/jobs"),
        ("POST", "/v1/jobs/{job_id}/cancel"),
        ("POST", "/v1/networks"),
        ("POST", "/v1/nodes/{node_id}/drain"),
        ("POST", "/v1/nodes/{node_id}/resume"),
        ("POST", "/v1/secrets"),
        ("POST", "/v1/services"),
        ("POST", "/v1/tasks"),
        ("POST", "/v1/tasks/{selector}/stop"),
        ("POST", "/v1/volumes"),
        ("POST", "/v1/volumes/import"),
        ("PUT", "/v1/nodes/{node_id}/labels"),
        ("PUT", "/v1/secrets/{name}"),
    ]
    .into_iter()
    .map(|(method, path)| (method.to_string(), path.to_string()))
    .collect()
}

/// Returns whether one serialized path item key is an HTTP method.
fn is_http_method(value: &str) -> bool {
    matches!(
        value,
        "get" | "put" | "post" | "delete" | "options" | "head" | "patch" | "trace"
    )
}
