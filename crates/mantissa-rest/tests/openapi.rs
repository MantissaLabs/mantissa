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

/// Ensures workload submission schemas keep CPU and memory resources mandatory.
#[test]
fn openapi_spec_requires_workload_resource_bounds() {
    let value = openapi::json_value(&server::openapi());
    let schemas = &value["components"]["schemas"];

    assert_required(
        &schemas["TaskStartRequest"],
        &["name", "image", "cpu_millis", "memory_bytes"],
    );
    assert_field_minimum(&schemas["TaskStartRequest"], "cpu_millis", 1);
    assert_field_minimum(&schemas["TaskStartRequest"], "memory_bytes", 1);
    assert_required(
        &schemas["TaskTemplateSpec"],
        &["name", "image", "resources"],
    );
    assert_required(
        &schemas["TaskTemplateResources"],
        &["cpu_millis", "memory_mb"],
    );
    assert_field_minimum(&schemas["TaskTemplateResources"], "cpu_millis", 1);
    assert_field_minimum(&schemas["TaskTemplateResources"], "memory_mb", 1);
    assert_required(&schemas["JobExecutionSpec"], &["image", "resources"]);
    assert_required(
        &schemas["JobExecutionResources"],
        &["cpu_millis", "memory_mb"],
    );
    assert_field_minimum(&schemas["JobExecutionResources"], "cpu_millis", 1);
    assert_field_minimum(&schemas["JobExecutionResources"], "memory_mb", 1);
    assert_required(&schemas["AgentExecutionSpec"], &["image", "resources"]);
    assert_required(
        &schemas["AgentExecutionResources"],
        &["cpu_millis", "memory_mb"],
    );
    assert_field_minimum(&schemas["AgentExecutionResources"], "cpu_millis", 1);
    assert_field_minimum(&schemas["AgentExecutionResources"], "memory_mb", 1);
}

/// Ensures docs renderers get short titles and separate descriptive text.
#[test]
fn openapi_spec_separates_operation_titles_from_descriptions() {
    let value = openapi::json_value(&server::openapi());

    assert_eq!(value["paths"]["/v1/nodes"]["get"]["summary"], "List nodes");
    assert_eq!(
        value["paths"]["/v1/nodes"]["get"]["description"],
        "Lists cluster nodes visible to the local daemon."
    );
    assert_eq!(
        value["paths"]["/v1/nodes/{node_id}"]["get"]["summary"],
        "Node status"
    );
    assert_eq!(
        value["paths"]["/v1/nodes/{node_id}"]["delete"]["summary"],
        "Evict node"
    );
    assert_eq!(
        value["paths"]["/v1/nodes/{node_id}/drain"]["get"]["summary"],
        "Drain status"
    );
    assert_eq!(
        value["paths"]["/v1/nodes/{node_id}/drain"]["post"]["summary"],
        "Drain node"
    );
    assert_eq!(
        value["paths"]["/v1/nodes/{node_id}/labels"]["put"]["summary"],
        "Label node"
    );
}

/// Ensures generated sidebar labels stay compact as new REST routes are added.
#[test]
fn openapi_operation_summaries_stay_short() {
    let value = openapi::json_value(&server::openapi());
    let paths = value["paths"]
        .as_object()
        .expect("OpenAPI paths should be an object");

    for (path, item) in paths {
        let methods = item.as_object().expect("path item should be an object");
        for (method, operation) in methods {
            if !is_http_method(method) {
                continue;
            }

            let summary = operation["summary"]
                .as_str()
                .expect("operation should have a summary");
            let description = operation["description"]
                .as_str()
                .expect("operation should have a description");
            assert!(
                summary.split_whitespace().count() <= 4,
                "{method} {path} summary is too long: {summary}"
            );
            assert!(
                !summary.ends_with('.'),
                "{method} {path} summary should be a title: {summary}"
            );
            assert_ne!(
                summary, description,
                "{method} {path} summary should not duplicate the description"
            );
        }
    }
}

/// Asserts that one schema contains every required field listed by name.
fn assert_required(schema: &Value, expected: &[&str]) {
    let actual = schema["required"]
        .as_array()
        .expect("schema should have required fields")
        .iter()
        .map(|field| field.as_str().expect("required field should be a string"))
        .collect::<BTreeSet<_>>();

    for field in expected {
        assert!(
            actual.contains(field),
            "schema is missing required field '{field}': {actual:?}"
        );
    }
}

/// Asserts that one schema field advertises the expected inclusive minimum.
fn assert_field_minimum(schema: &Value, field: &str, expected: u64) {
    assert_eq!(
        schema["properties"][field]["minimum"].as_u64(),
        Some(expected),
        "schema field '{field}' should have minimum {expected}: {schema}"
    );
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
        ("DELETE", "/v1/ingress/{name}"),
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
        ("GET", "/v1/ingress"),
        ("GET", "/v1/ingress/endpoints"),
        ("GET", "/v1/ingress/{name}"),
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
        ("PUT", "/v1/ingress"),
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
