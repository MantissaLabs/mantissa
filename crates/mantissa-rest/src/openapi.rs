//! OpenAPI document assembly for the embedded REST facade.

use crate::{error::RestErrorBody, stream::task_exec::TaskInteractiveClientMessage};
use serde_json::{Value, json};
use std::{fs, io, path::PathBuf};
use utoipa::{
    OpenApi,
    openapi::{
        Components, OpenApi as OpenApiDocument,
        path::Operation,
        security::{HttpAuthScheme, HttpBuilder, SecurityRequirement, SecurityScheme},
        tag::Tag,
    },
};

/// OpenAPI version published for generated REST specifications.
pub const OPENAPI_VERSION: &str = "3.2.0";

/// Checked-in OpenAPI file name generated from the REST route declarations.
pub const OPENAPI_FILE_NAME: &str = "mantissa-rest.openapi.json";

#[derive(OpenApi)]
#[openapi(components(schemas(RestErrorBody, TaskInteractiveClientMessage)))]
struct BaseApi;

/// Builds the base document that route registration extends with path items.
pub fn base_document() -> OpenApiDocument {
    let mut document = BaseApi::openapi();
    document.info.title = "Mantissa REST API".to_string();
    document.info.version = env!("CARGO_PKG_VERSION").to_string();
    document.info.description = Some(
        "Local HTTP facade over the Mantissa Cap'n Proto admin session. \
         All /v1 endpoints require an Authorization: Bearer token. \
         Non-loopback listeners must also be served with TLS and client \
         certificate verification enabled."
            .to_string(),
    );
    document.tags = Some(tags());
    let components = document.components.get_or_insert_with(Components::new);
    components.add_security_scheme(
        "bearerAuth",
        SecurityScheme::Http(
            HttpBuilder::new()
                .scheme(HttpAuthScheme::Bearer)
                .bearer_format("Mantissa REST token")
                .description(Some(
                    "Daemon-generated REST token presented as a Bearer token.",
                ))
                .build(),
        ),
    );
    components.add_security_scheme(
        "mtls",
        SecurityScheme::MutualTls {
            description: Some(
                "Required when the REST listener is bound to a non-loopback interface.".to_string(),
            ),
            extensions: None,
        },
    );
    document.security = Some(vec![SecurityRequirement::new(
        "bearerAuth",
        Vec::<String>::new(),
    )]);
    document
}

/// Finalizes route-generated metadata before serialization.
pub fn finalize_document(mut document: OpenApiDocument) -> OpenApiDocument {
    apply_operation_docs(&mut document);
    if let Some(path) = document.paths.paths.get_mut("/healthz")
        && let Some(operation) = path.get.as_mut()
    {
        operation.security = Some(Vec::new());
    }
    document
}

/// Applies human-curated titles and descriptions used by rendered API docs.
fn apply_operation_docs(document: &mut OpenApiDocument) {
    for &(method, path, summary, description) in OPERATION_DOCS {
        if let Some(operation) = operation_mut(document, method, path) {
            operation.summary = Some(summary.to_string());
            operation.description = Some(description.to_string());
        }
    }
}

/// Returns one mutable OpenAPI operation by method and path.
fn operation_mut<'a>(
    document: &'a mut OpenApiDocument,
    method: OperationMethod,
    path: &str,
) -> Option<&'a mut Operation> {
    let path_item = document.paths.paths.get_mut(path)?;
    match method {
        OperationMethod::Get => path_item.get.as_mut(),
        OperationMethod::Put => path_item.put.as_mut(),
        OperationMethod::Post => path_item.post.as_mut(),
        OperationMethod::Delete => path_item.delete.as_mut(),
    }
}

/// Converts the typed OpenAPI document into the checked-in JSON representation.
pub fn json_value(document: &OpenApiDocument) -> Value {
    let mut value =
        serde_json::to_value(document).expect("OpenAPI document should serialize to JSON");
    value["openapi"] = Value::String(OPENAPI_VERSION.to_string());
    inject_common_error_responses(&mut value);
    sort_json_objects(value)
}

/// Serializes the OpenAPI document as stable, pretty-printed JSON.
pub fn json_string(document: &OpenApiDocument) -> String {
    let value = json_value(document);
    let mut rendered =
        serde_json::to_string_pretty(&value).expect("OpenAPI JSON should pretty-print");
    rendered.push('\n');
    rendered
}

/// Returns the crate-local path where the generated OpenAPI file is checked in.
pub fn spec_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("openapi")
        .join(OPENAPI_FILE_NAME)
}

/// Writes the generated OpenAPI document to the crate-local openapi directory.
pub fn write_spec_file(document: &OpenApiDocument) -> io::Result<PathBuf> {
    let path = spec_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, json_string(document))?;
    Ok(path)
}

/// Adds common REST error responses to every protected operation.
fn inject_common_error_responses(value: &mut Value) {
    let Some(paths) = value.get_mut("paths").and_then(Value::as_object_mut) else {
        return;
    };
    for (path, item) in paths {
        let Some(methods) = item.as_object_mut() else {
            continue;
        };
        for (method, operation) in methods {
            if !is_http_method(method) || path == "/healthz" {
                continue;
            }
            let Some(responses) = operation
                .get_mut("responses")
                .and_then(Value::as_object_mut)
            else {
                continue;
            };
            for (status, description) in COMMON_ERROR_RESPONSES {
                responses
                    .entry(status)
                    .or_insert_with(|| error_response(description));
            }
        }
    }
}

/// Returns one JSON OpenAPI error response using the shared REST error body.
fn error_response(description: &'static str) -> Value {
    json!({
        "description": description,
        "content": {
            "application/json": {
                "schema": {
                    "$ref": "#/components/schemas/RestErrorBody"
                }
            }
        }
    })
}

/// Returns JSON with every object key sorted for feature-independent output.
fn sort_json_objects(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sort_json_objects).collect()),
        Value::Object(map) => {
            let mut entries: Vec<_> = map
                .into_iter()
                .map(|(key, value)| (key, sort_json_objects(value)))
                .collect();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));

            let mut sorted = serde_json::Map::new();
            for (key, value) in entries {
                sorted.insert(key, value);
            }
            Value::Object(sorted)
        }
        other => other,
    }
}

/// Returns whether one serialized path item key is an HTTP method.
fn is_http_method(value: &str) -> bool {
    matches!(
        value,
        "get" | "put" | "post" | "delete" | "options" | "head" | "patch" | "trace"
    )
}

/// Builds the top-level tag descriptions used by rendered API documentation.
fn tags() -> Vec<Tag> {
    [
        ("health", "REST listener and local daemon health checks."),
        ("nodes", "Cluster node inspection and maintenance actions."),
        ("agents", "Durable agent session submission and control."),
        (
            "jobs",
            "Finite controller-owned job submission and lifecycle.",
        ),
        ("services", "Service deployment, inspection, and deletion."),
        (
            "networks",
            "Overlay network creation, inspection, and deletion.",
        ),
        (
            "ingress",
            "Public ingress pool configuration and endpoint target discovery.",
        ),
        (
            "volumes",
            "Local volume creation, import, inspection, and deletion.",
        ),
        (
            "tasks",
            "Standalone task lifecycle, logs, attach, and exec.",
        ),
        (
            "secrets",
            "Secret metadata and plaintext version management.",
        ),
        ("scheduler", "Local scheduler capacity inspection."),
        (
            "clusters",
            "Cluster lineage, view, operation, and split inspection.",
        ),
    ]
    .into_iter()
    .map(|(name, description)| {
        let mut tag = Tag::new(name);
        tag.description = Some(description.to_string());
        tag
    })
    .collect()
}

const COMMON_ERROR_RESPONSES: [(&str, &str); 6] = [
    (
        "400",
        "Invalid request syntax, query parameters, or resource selector.",
    ),
    ("401", "Missing or invalid REST bearer token."),
    ("404", "Requested resource was not found."),
    ("409", "Request conflicts with the current resource state."),
    ("500", "Unexpected REST facade or local client failure."),
    ("503", "Local daemon or REST worker is unavailable."),
];

#[derive(Clone, Copy)]
enum OperationMethod {
    Get,
    Put,
    Post,
    Delete,
}

type OperationDoc = (OperationMethod, &'static str, &'static str, &'static str);

const OPERATION_DOCS: &[OperationDoc] = &[
    (
        OperationMethod::Get,
        "/healthz",
        "Liveness",
        "Reports whether the REST gateway process itself is alive.",
    ),
    (
        OperationMethod::Get,
        "/v1/health",
        "Health check",
        "Reports whether the REST gateway can authenticate and ping the daemon.",
    ),
    (
        OperationMethod::Get,
        "/v1/nodes",
        "List nodes",
        "Lists cluster nodes visible to the local daemon.",
    ),
    (
        OperationMethod::Get,
        "/v1/nodes/{node_id}",
        "Node status",
        "Fetches one cluster node by UUID string.",
    ),
    (
        OperationMethod::Delete,
        "/v1/nodes/{node_id}",
        "Evict node",
        "Evicts one stale node identity by UUID string.",
    ),
    (
        OperationMethod::Get,
        "/v1/nodes/{node_id}/drain",
        "Drain status",
        "Fetches the current drain-status snapshot for one node.",
    ),
    (
        OperationMethod::Post,
        "/v1/nodes/{node_id}/drain",
        "Drain node",
        "Requests drain for one node by UUID string.",
    ),
    (
        OperationMethod::Put,
        "/v1/nodes/{node_id}/labels",
        "Label node",
        "Applies one node label update by UUID string.",
    ),
    (
        OperationMethod::Post,
        "/v1/nodes/{node_id}/resume",
        "Resume node",
        "Resumes scheduling for one drained node by UUID string.",
    ),
    (
        OperationMethod::Get,
        "/v1/agents/sessions",
        "List sessions",
        "Lists durable agent sessions visible to the local daemon.",
    ),
    (
        OperationMethod::Post,
        "/v1/agents/sessions",
        "Submit session",
        "Submits one durable agent session manifest to the local daemon.",
    ),
    (
        OperationMethod::Get,
        "/v1/agents/sessions/{session_id}",
        "Get session",
        "Fetches one durable agent session and its retained run history.",
    ),
    (
        OperationMethod::Delete,
        "/v1/agents/sessions/{session_id}",
        "Delete session",
        "Deletes one closed durable agent session and its retained run history.",
    ),
    (
        OperationMethod::Get,
        "/v1/agents/sessions/{session_id}/runs",
        "List runs",
        "Lists durable runs for one agent session.",
    ),
    (
        OperationMethod::Post,
        "/v1/agents/sessions/{session_id}/input",
        "Submit input",
        "Queues structured input on one idle agent session.",
    ),
    (
        OperationMethod::Post,
        "/v1/agents/sessions/{session_id}/cancel",
        "Cancel session",
        "Requests cancellation for one active or queued agent session run.",
    ),
    (
        OperationMethod::Post,
        "/v1/agents/sessions/{session_id}/close",
        "Close session",
        "Closes one durable agent session and rejects future input.",
    ),
    (
        OperationMethod::Get,
        "/v1/jobs",
        "List jobs",
        "Lists first-class jobs visible to the local daemon.",
    ),
    (
        OperationMethod::Post,
        "/v1/jobs",
        "Submit job",
        "Submits one first-class job manifest to the local daemon.",
    ),
    (
        OperationMethod::Get,
        "/v1/jobs/{job_id}",
        "Get job",
        "Fetches one first-class job by UUID string.",
    ),
    (
        OperationMethod::Post,
        "/v1/jobs/{job_id}/cancel",
        "Cancel job",
        "Cancels one first-class job by UUID string.",
    ),
    (
        OperationMethod::Delete,
        "/v1/jobs/{job_id}",
        "Delete job",
        "Deletes one terminal first-class job by UUID string.",
    ),
    (
        OperationMethod::Get,
        "/v1/services",
        "List services",
        "Lists services visible to the local daemon.",
    ),
    (
        OperationMethod::Post,
        "/v1/services",
        "Deploy service",
        "Deploys or updates one service manifest through the local daemon.",
    ),
    (
        OperationMethod::Get,
        "/v1/services/{selector}",
        "Get service",
        "Fetches one service by UUID text or exact service name.",
    ),
    (
        OperationMethod::Delete,
        "/v1/services/{selector}",
        "Delete service",
        "Deletes one service by UUID text or exact service name.",
    ),
    (
        OperationMethod::Get,
        "/v1/services/{selector}/status",
        "Service status",
        "Fetches one service status snapshot by UUID text or exact service name.",
    ),
    (
        OperationMethod::Get,
        "/v1/networks",
        "List networks",
        "Lists overlay networks visible to the local daemon.",
    ),
    (
        OperationMethod::Post,
        "/v1/networks",
        "Create network",
        "Creates one overlay network through the local daemon.",
    ),
    (
        OperationMethod::Get,
        "/v1/networks/{network_id}",
        "Get network",
        "Fetches one overlay network inspection by UUID string.",
    ),
    (
        OperationMethod::Delete,
        "/v1/networks/{network_id}",
        "Delete network",
        "Deletes one overlay network by UUID string.",
    ),
    (
        OperationMethod::Get,
        "/v1/networks/{network_id}/peers",
        "Network peers",
        "Lists per-peer convergence rows for one overlay network.",
    ),
    (
        OperationMethod::Get,
        "/v1/networks/{network_id}/attachments",
        "Network attachments",
        "Lists workload attachment rows for one overlay network.",
    ),
    (
        OperationMethod::Get,
        "/v1/ingress",
        "List ingress pools",
        "Lists ingress pools visible to the local daemon.",
    ),
    (
        OperationMethod::Put,
        "/v1/ingress",
        "Apply ingress pool",
        "Creates or replaces one ingress pool through the local daemon.",
    ),
    (
        OperationMethod::Get,
        "/v1/ingress/endpoints",
        "Ingress endpoints",
        "Lists public endpoint target rows visible through ingress.",
    ),
    (
        OperationMethod::Get,
        "/v1/ingress/{selector}",
        "Get ingress pool",
        "Fetches one ingress pool by UUID string or exact name.",
    ),
    (
        OperationMethod::Delete,
        "/v1/ingress/{selector}",
        "Delete ingress pool",
        "Deletes one ingress pool by UUID string or exact name.",
    ),
    (
        OperationMethod::Get,
        "/v1/volumes",
        "List volumes",
        "Lists volumes visible to the local daemon.",
    ),
    (
        OperationMethod::Post,
        "/v1/volumes",
        "Create volume",
        "Creates one managed local volume through the local daemon.",
    ),
    (
        OperationMethod::Post,
        "/v1/volumes/import",
        "Import volume",
        "Imports one existing local path as a volume through the local daemon.",
    ),
    (
        OperationMethod::Get,
        "/v1/volumes/{selector}",
        "Get volume",
        "Fetches one volume by UUID text or exact volume name.",
    ),
    (
        OperationMethod::Delete,
        "/v1/volumes/{selector}",
        "Delete volume",
        "Deletes one volume by UUID text or exact volume name.",
    ),
    (
        OperationMethod::Get,
        "/v1/volumes/{selector}/status",
        "Volume status",
        "Fetches one volume status by UUID text or exact volume name.",
    ),
    (
        OperationMethod::Get,
        "/v1/tasks",
        "List tasks",
        "Lists standalone tasks visible to the local daemon.",
    ),
    (
        OperationMethod::Post,
        "/v1/tasks",
        "Start task",
        "Starts one standalone task through the local daemon.",
    ),
    (
        OperationMethod::Get,
        "/v1/tasks/{selector}",
        "Get task",
        "Fetches one standalone task by UUID text or exact task name.",
    ),
    (
        OperationMethod::Get,
        "/v1/tasks/{selector}/logs",
        "Task logs",
        "Streams standalone task logs as newline-delimited JSON frames.",
    ),
    (
        OperationMethod::Get,
        "/v1/tasks/{selector}/attach",
        "Attach task",
        "Opens a WebSocket bridge to one running task's stdio streams.",
    ),
    (
        OperationMethod::Get,
        "/v1/tasks/{selector}/exec",
        "Exec task",
        "Opens a WebSocket bridge to one command exec session inside a running task.",
    ),
    (
        OperationMethod::Post,
        "/v1/tasks/{selector}/stop",
        "Stop task",
        "Stops one standalone task by UUID text or accepted selector.",
    ),
    (
        OperationMethod::Get,
        "/v1/secrets",
        "List secrets",
        "Lists secret summaries visible to the local daemon.",
    ),
    (
        OperationMethod::Post,
        "/v1/secrets",
        "Create secret",
        "Creates one secret with base64-encoded plaintext.",
    ),
    (
        OperationMethod::Get,
        "/v1/secrets/{name}",
        "Get secret",
        "Fetches the current plaintext version for one secret.",
    ),
    (
        OperationMethod::Put,
        "/v1/secrets/{name}",
        "Update secret",
        "Updates one secret with a new base64-encoded plaintext version.",
    ),
    (
        OperationMethod::Delete,
        "/v1/secrets/{name}",
        "Delete secret",
        "Deletes one secret by name.",
    ),
    (
        OperationMethod::Get,
        "/v1/secrets/{name}/versions/{version_id}",
        "Secret version",
        "Fetches one explicit plaintext secret version by UUID string.",
    ),
    (
        OperationMethod::Get,
        "/v1/scheduler/summary",
        "Capacity summary",
        "Fetches scheduler capacity summary from the local scheduler capability.",
    ),
    (
        OperationMethod::Get,
        "/v1/clusters",
        "List clusters",
        "Lists cluster lineage summaries known to the local daemon.",
    ),
    (
        OperationMethod::Get,
        "/v1/clusters/views",
        "List views",
        "Lists raw cluster view summaries known to the local daemon.",
    ),
    (
        OperationMethod::Get,
        "/v1/clusters/current",
        "Current cluster",
        "Returns the active cluster view associated with the local session.",
    ),
    (
        OperationMethod::Get,
        "/v1/clusters/operations/{operation_id}",
        "Get operation",
        "Fetches the latest locally known cluster operation state by UUID string.",
    ),
    (
        OperationMethod::Get,
        "/v1/clusters/split-candidates",
        "Split candidates",
        "Lists split candidates for the local active cluster view.",
    ),
    (
        OperationMethod::Get,
        "/v1/clusters/{cluster_id}/split-candidates",
        "Cluster split candidates",
        "Lists split candidates for one explicit cluster lineage id.",
    ),
];
