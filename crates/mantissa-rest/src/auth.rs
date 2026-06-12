use crate::{error::RestError, state::AppState};
use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, header::AUTHORIZATION, request::Parts},
};

/// Authentication policy for REST control routes.
#[derive(Clone, Debug)]
pub enum RestAuthConfig {
    Bearer { token: Option<String> },
    Disabled,
}

impl RestAuthConfig {
    /// Returns whether this policy has an active bearer token configured.
    pub fn has_bearer_token(&self) -> bool {
        matches!(self, Self::Bearer { token: Some(_) })
    }

    /// Validates one request header map against this authentication policy.
    pub fn authorize_headers(&self, headers: &HeaderMap) -> Result<(), RestAuthError> {
        match self {
            Self::Disabled => Ok(()),
            Self::Bearer {
                token: Some(expected),
            } => {
                let value = headers
                    .get(AUTHORIZATION)
                    .ok_or(RestAuthError::MissingBearer)?;
                let value = value.to_str().map_err(|_| RestAuthError::InvalidBearer)?;
                let Some(token) = value.strip_prefix("Bearer ") else {
                    return Err(RestAuthError::InvalidBearer);
                };
                if token == expected {
                    Ok(())
                } else {
                    Err(RestAuthError::InvalidBearer)
                }
            }
            Self::Bearer { token: None } => Err(RestAuthError::TokenNotConfigured),
        }
    }
}

/// Marker extractor for routes that require REST authorization.
pub struct RestAuth;

impl FromRequestParts<AppState> for RestAuth {
    type Rejection = RestError;

    /// Authenticates one protected HTTP request before the route handler runs.
    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        state.auth().authorize_headers(&parts.headers)?;
        Ok(Self)
    }
}

/// Authentication failures surfaced through stable REST error bodies.
#[derive(Debug)]
pub enum RestAuthError {
    MissingBearer,
    InvalidBearer,
    TokenNotConfigured,
}

impl From<RestAuthError> for RestError {
    /// Converts authentication failures into HTTP unauthorized responses.
    fn from(error: RestAuthError) -> Self {
        match error {
            RestAuthError::MissingBearer => RestError::unauthorized("missing bearer token"),
            RestAuthError::InvalidBearer => RestError::unauthorized("invalid bearer token"),
            RestAuthError::TokenNotConfigured => {
                RestError::unauthorized("REST bearer token is not configured")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn authorize_headers_accepts_matching_bearer_token() {
        let auth = RestAuthConfig::Bearer {
            token: Some("secret".to_string()),
        };
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));

        auth.authorize_headers(&headers).unwrap();
    }

    #[test]
    fn authorize_headers_rejects_missing_bearer_token() {
        let auth = RestAuthConfig::Bearer {
            token: Some("secret".to_string()),
        };

        assert!(matches!(
            auth.authorize_headers(&HeaderMap::new()),
            Err(RestAuthError::MissingBearer)
        ));
    }

    #[test]
    fn authorize_headers_rejects_unconfigured_token() {
        let auth = RestAuthConfig::Bearer { token: None };

        assert!(matches!(
            auth.authorize_headers(&HeaderMap::new()),
            Err(RestAuthError::TokenNotConfigured)
        ));
    }
}
