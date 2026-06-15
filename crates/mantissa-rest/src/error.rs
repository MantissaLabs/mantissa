use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use utoipa::ToSchema;

/// Stable JSON error body returned by REST handlers.
#[derive(Debug, Serialize, ToSchema)]
pub struct RestErrorBody {
    pub code: &'static str,
    pub message: String,
}

/// HTTP error with a compact machine-readable code and human message.
#[derive(Debug)]
pub struct RestError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl RestError {
    /// Creates a new REST error with an explicit status and stable code.
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    /// Creates an HTTP 401 error.
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    /// Creates an HTTP 400 error.
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    /// Creates an HTTP 404 error.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    /// Creates an HTTP 409 error for conflicting resource state.
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    /// Creates an HTTP 503 error for local daemon or worker unavailability.
    pub fn service_unavailable(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            message,
        )
    }

    /// Creates an HTTP 500 error for unexpected gateway or client failures.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
    }

    /// Returns the HTTP status attached to this error.
    pub fn status(&self) -> StatusCode {
        self.status
    }
}

impl IntoResponse for RestError {
    /// Converts the error into a JSON HTTP response.
    fn into_response(self) -> Response {
        let body = RestErrorBody {
            code: self.code,
            message: self.message,
        };
        (self.status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body;

    #[tokio::test]
    async fn into_response_writes_status_and_json_body() {
        let response = RestError::service_unavailable("daemon unavailable").into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["code"], "service_unavailable");
        assert_eq!(value["message"], "daemon unavailable");
    }
}
