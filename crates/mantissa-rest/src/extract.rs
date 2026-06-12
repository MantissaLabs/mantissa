//! REST-specific request extractors.

use crate::error::RestError;
use axum::{
    Json,
    extract::{FromRequest, Request, rejection::JsonRejection},
};
use serde::de::DeserializeOwned;

/// JSON request extractor that keeps malformed bodies in the REST error shape.
#[derive(Debug)]
pub struct RestJson<T>(pub T);

impl<S, T> FromRequest<S> for RestJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = RestError;

    /// Reads one JSON request body and maps Axum rejections into `RestError`.
    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        Json::<T>::from_request(req, state)
            .await
            .map(|Json(value)| Self(value))
            .map_err(json_rejection_to_rest)
    }
}

/// Converts Axum JSON rejections into the facade's stable JSON error envelope.
fn json_rejection_to_rest(rejection: JsonRejection) -> RestError {
    RestError::bad_request(rejection.body_text())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{self, Body},
        http::{Request as HttpRequest, StatusCode, header::CONTENT_TYPE},
        response::IntoResponse,
    };
    use serde::Deserialize;

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct TestRequest {
        name: String,
    }

    #[tokio::test]
    async fn rest_json_maps_invalid_json_to_rest_error_body() {
        let request = HttpRequest::builder()
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from("{"))
            .unwrap();

        let error = RestJson::<TestRequest>::from_request(request, &())
            .await
            .unwrap_err();
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["code"], "bad_request");
        assert!(!value["message"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn rest_json_rejects_unknown_fields_with_rest_error_body() {
        let request = HttpRequest::builder()
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"name":"demo","extra":true}"#))
            .unwrap();

        let error = RestJson::<TestRequest>::from_request(request, &())
            .await
            .unwrap_err();
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["code"], "bad_request");
        assert!(value["message"].as_str().unwrap().contains("unknown field"));
    }
}
