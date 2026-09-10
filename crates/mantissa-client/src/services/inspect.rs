use crate::error::{ClientError, ClientErrorKind};
use crate::{config::ClientConfig, connection, tasks::uuid_from_data};
use anyhow::{Context, Result, ensure};
use capnp::capability::Response;
use mantissa_protocol::services::{service_status_snapshot, services};
use uuid::Uuid;

/// Keeps the full response available without copying every template into a second model.
pub struct ServiceInspection {
    response: Response<services::status_results::Owned>,
}

impl ServiceInspection {
    /// Borrows configuration and progress from the same response to keep them consistent.
    pub fn snapshot(&self) -> Result<service_status_snapshot::Reader<'_>> {
        Ok(self.response.get()?.get_snapshot()?)
    }
}

/// Resolves a service name when necessary, then fetches its configuration and progress by ID.
pub async fn inspect(cfg: &ClientConfig, selector: &str) -> Result<ServiceInspection> {
    let selector = selector.trim();
    ensure!(
        !selector.is_empty(),
        ClientError::new(
            ClientErrorKind::InvalidRequest,
            "service name or ID must not be empty"
        )
    );

    let session = connection::get_local_session(cfg).await?;
    let services = session
        .get_services_request()
        .send()
        .pipeline
        .get_services();

    let id = match Uuid::parse_str(selector) {
        Ok(id) => id,
        Err(_) => {
            let mut request = services.inspect_request();
            request.get().set_selector(selector);

            let response = request
                .send()
                .promise
                .await
                .map_err(|error| {
                    ClientError::from_capnp_domain_error(ClientErrorKind::NotFound, error)
                })
                .with_context(|| format!("could not find service '{selector}'"))?;

            uuid_from_data(response.get()?.get_service()?.get_id()?)?
        }
    };

    let mut request = services.status_request();
    request.get().set_service_id(id.as_bytes());

    let response = request
        .send()
        .promise
        .await
        .map_err(|error| ClientError::from_capnp_domain_error(ClientErrorKind::NotFound, error))
        .with_context(|| format!("could not inspect service '{selector}'"))?;

    Ok(ServiceInspection { response })
}
