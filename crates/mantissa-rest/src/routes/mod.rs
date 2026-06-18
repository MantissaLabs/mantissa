//! Axum route handlers for the local REST facade.

use crate::{client_worker::ClientWorkerError, error::RestError};

pub mod agents;
pub mod clusters;
pub mod health;
pub mod ingress;
pub mod jobs;
pub mod networks;
pub mod nodes;
pub mod scheduler;
pub mod secrets;
pub mod services;
pub mod tasks;
pub mod volumes;

/// Maps local client worker errors into REST HTTP errors.
pub(crate) fn worker_error_to_rest(error: ClientWorkerError) -> RestError {
    match error {
        ClientWorkerError::DaemonUnavailable(message) => RestError::service_unavailable(message),
        ClientWorkerError::InvalidRequest(message) => RestError::bad_request(message),
        ClientWorkerError::NotFound(message) => RestError::not_found(message),
        ClientWorkerError::Conflict(message) => RestError::conflict(message),
        ClientWorkerError::OperationFailed(message) => RestError::internal(message),
        ClientWorkerError::RequestChannelClosed
        | ClientWorkerError::ResponseChannelClosed
        | ClientWorkerError::Runtime(_) => RestError::service_unavailable(error.to_string()),
    }
}
