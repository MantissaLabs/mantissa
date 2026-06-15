//! OpenAPI document assembly for the embedded REST facade.

use crate::{error::RestErrorBody, stream::task_exec::TaskInteractiveClientMessage};
use serde_json::{Value, json};
use std::{fs, io, path::PathBuf};
use utoipa::{
    OpenApi,
    openapi::{
        Components, OpenApi as OpenApiDocument,
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
    if let Some(path) = document.paths.paths.get_mut("/healthz")
        && let Some(operation) = path.get.as_mut()
    {
        operation.security = Some(Vec::new());
    }
    document
}

/// Converts the typed OpenAPI document into the checked-in JSON representation.
pub fn json_value(document: &OpenApiDocument) -> Value {
    let mut value =
        serde_json::to_value(document).expect("OpenAPI document should serialize to JSON");
    value["openapi"] = Value::String(OPENAPI_VERSION.to_string());
    inject_common_error_responses(&mut value);
    value
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
