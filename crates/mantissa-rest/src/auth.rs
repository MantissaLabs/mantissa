use crate::{error::RestError, state::AppState};
use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, header::AUTHORIZATION, request::Parts},
};

/// Marker extractor for routes that require REST authorization.
pub struct RestAuth;

impl FromRequestParts<AppState> for RestAuth {
    type Rejection = RestError;

    /// Authenticates one protected HTTP request before the route handler runs.
    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let token = bearer_token(&parts.headers)?;
        let valid = state
            .client()
            .validate_rest_token(token.to_string())
            .await
            .map_err(|error| RestError::service_unavailable(error.to_string()))?;
        if !valid {
            return Err(RestAuthError::InvalidBearer.into());
        }
        Ok(Self)
    }
}

/// Extracts the bearer token from one request header map.
fn bearer_token(headers: &HeaderMap) -> Result<&str, RestAuthError> {
    let value = headers
        .get(AUTHORIZATION)
        .ok_or(RestAuthError::MissingBearer)?;
    let value = value.to_str().map_err(|_| RestAuthError::InvalidBearer)?;
    let Some(token) = value.strip_prefix("Bearer ") else {
        return Err(RestAuthError::InvalidBearer);
    };
    if token.is_empty() {
        return Err(RestAuthError::InvalidBearer);
    }
    Ok(token)
}

/// Authentication failures surfaced through stable REST error bodies.
#[derive(Debug)]
pub enum RestAuthError {
    MissingBearer,
    InvalidBearer,
}

impl From<RestAuthError> for RestError {
    /// Converts authentication failures into HTTP unauthorized responses.
    fn from(error: RestAuthError) -> Self {
        match error {
            RestAuthError::MissingBearer => RestError::unauthorized("missing bearer token"),
            RestAuthError::InvalidBearer => RestError::unauthorized("invalid bearer token"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn bearer_token_accepts_valid_header() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));

        assert_eq!(bearer_token(&headers).unwrap(), "secret");
    }

    #[test]
    fn bearer_token_rejects_missing_header() {
        assert!(matches!(
            bearer_token(&HeaderMap::new()),
            Err(RestAuthError::MissingBearer)
        ));
    }

    #[test]
    fn bearer_token_rejects_malformed_header() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("secret"));

        assert!(matches!(
            bearer_token(&headers),
            Err(RestAuthError::InvalidBearer)
        ));
    }
}
