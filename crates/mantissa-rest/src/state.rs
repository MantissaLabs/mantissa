use crate::client_worker::ClientWorkerHandle;

/// Shared Axum application state for REST routes.
#[derive(Clone)]
pub struct AppState {
    client: ClientWorkerHandle,
}

impl AppState {
    /// Creates REST application state from a local client worker handle.
    pub fn new(client: ClientWorkerHandle) -> Self {
        Self { client }
    }

    /// Returns the local Mantissa client worker handle.
    pub fn client(&self) -> &ClientWorkerHandle {
        &self.client
    }
}
