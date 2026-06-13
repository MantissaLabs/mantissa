use crate::{config::ClientConfig, connection};
use anyhow::Result;

/// Fetches the current local REST bearer token from the daemon.
pub async fn show_token(cfg: &ClientConfig) -> Result<String> {
    let session = connection::get_local_session(cfg).await?;

    let request = session.get_rest_admin_request();
    let rest_admin = request.send().pipeline.get_rest_admin();
    let request = rest_admin.show_token_request();

    let response = request.send().promise.await?;
    Ok(response.get()?.get_token()?.to_string()?)
}

/// Rotates the local REST bearer token and returns the newly issued token.
pub async fn rotate_token(cfg: &ClientConfig) -> Result<String> {
    let session = connection::get_local_session(cfg).await?;

    let request = session.get_rest_admin_request();
    let rest_admin = request.send().pipeline.get_rest_admin();
    let request = rest_admin.rotate_token_request();

    let response = request.send().promise.await?;
    Ok(response.get()?.get_token()?.to_string()?)
}

/// Validates a local REST bearer token through the daemon-owned token store.
pub async fn validate_token(cfg: &ClientConfig, token: &str) -> Result<bool> {
    let session = connection::get_local_session(cfg).await?;

    let request = session.get_rest_admin_request();
    let rest_admin = request.send().pipeline.get_rest_admin();
    let mut request = rest_admin.validate_token_request();
    request.get().set_token(token);

    let response = request.send().promise.await?;
    Ok(response.get()?.get_valid())
}
