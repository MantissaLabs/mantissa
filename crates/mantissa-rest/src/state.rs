use crate::{auth::RestAuthConfig, client_worker::ClientWorkerHandle};
use std::sync::Arc;

/// Shared Axum application state for REST routes.
#[derive(Clone)]
pub struct AppState {
    auth: Arc<RestAuthConfig>,
    client: ClientWorkerHandle,
}

impl AppState {
    /// Creates REST application state from auth policy and client worker handle.
    pub fn new(auth: RestAuthConfig, client: ClientWorkerHandle) -> Self {
        Self {
            auth: Arc::new(auth),
            client,
        }
    }

    /// Returns the REST authentication policy.
    pub fn auth(&self) -> &RestAuthConfig {
        &self.auth
    }

    /// Returns the local Mantissa client worker handle.
    pub fn client(&self) -> &ClientWorkerHandle {
        &self.client
    }
}
