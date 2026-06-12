use crate::{
    auth::RestAuth,
    client_worker::ClientWorkerError,
    error::RestError,
    state::AppState,
    types::health::{HealthResponse, LivenessResponse},
};
use axum::{Json, extract::State};

/// Reports whether the REST gateway process itself is alive.
pub async fn liveness() -> Json<LivenessResponse> {
    Json(LivenessResponse::ok())
}

/// Reports whether the REST gateway can authenticate and ping the daemon.
pub async fn health(
    State(state): State<AppState>,
    _auth: RestAuth,
) -> Result<Json<HealthResponse>, RestError> {
    let health = state
        .client()
        .health()
        .await
        .map_err(client_worker_error_to_rest)?;
    if health.daemon_reachable {
        Ok(Json(HealthResponse::daemon_reachable()))
    } else {
        Err(RestError::service_unavailable("daemon is not reachable"))
    }
}

/// Maps client worker failures to HTTP service-unavailable errors.
fn client_worker_error_to_rest(error: ClientWorkerError) -> RestError {
    RestError::service_unavailable(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        auth::RestAuthConfig,
        client_worker::{ClientHealth, ClientWorkerHandle},
        server,
    };
    use axum::{
        body::{self, Body},
        http::{Request, StatusCode, header::AUTHORIZATION},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn liveness_route_returns_ok_without_auth() {
        let state = AppState::new(
            RestAuthConfig::Bearer {
                token: Some("secret".to_string()),
            },
            ClientWorkerHandle::fixed_health_for_tests(Ok(ClientHealth {
                daemon_reachable: true,
            })),
        );
        let response = server::router(state)
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["status"], "ok");
    }

    #[tokio::test]
    async fn health_route_requires_auth() {
        let state = AppState::new(
            RestAuthConfig::Bearer {
                token: Some("secret".to_string()),
            },
            ClientWorkerHandle::fixed_health_for_tests(Ok(ClientHealth {
                daemon_reachable: true,
            })),
        );
        let response = server::router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn health_route_returns_daemon_status() {
        let state = AppState::new(
            RestAuthConfig::Bearer {
                token: Some("secret".to_string()),
            },
            ClientWorkerHandle::fixed_health_for_tests(Ok(ClientHealth {
                daemon_reachable: true,
            })),
        );
        let response = server::router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .header(AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["status"], "ok");
        assert_eq!(value["daemon"]["reachable"], true);
    }

    #[tokio::test]
    async fn health_route_maps_worker_failure_to_unavailable() {
        let state = AppState::new(
            RestAuthConfig::Bearer {
                token: Some("secret".to_string()),
            },
            ClientWorkerHandle::fixed_health_for_tests(Err(ClientWorkerError::DaemonUnavailable(
                "daemon down".to_string(),
            ))),
        );
        let response = server::router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .header(AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["code"], "service_unavailable");
        assert_eq!(value["message"], "daemon down");
    }
}
