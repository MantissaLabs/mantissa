//! Docker-to-runtime translation helpers.
//!
//! These helpers isolate the Bollard-specific wire shapes from the generic
//! runtime metadata used by the rest of the scheduler.

use bollard::container::LogOutput;
use bollard::errors::Error as BollardError;
use bollard::service::ContainerInspectResponse;

use crate::runtime::types::{
    RuntimeAttachmentTarget, RuntimeConfigInfo, RuntimeError, RuntimeInfo, RuntimeLogFrame,
    RuntimeLogStream, RuntimeNetworkEndpoint, RuntimeStateInfo,
};

/// Converts one Docker attach or log frame into the runtime-neutral output
/// stream used by higher layers.
pub(super) fn runtime_log_frame_from_output(output: LogOutput) -> RuntimeLogFrame {
    match output {
        LogOutput::StdErr { message } => RuntimeLogFrame {
            stream: RuntimeLogStream::StdErr,
            message: message.to_vec(),
        },
        LogOutput::StdOut { message } => RuntimeLogFrame {
            stream: RuntimeLogStream::StdOut,
            message: message.to_vec(),
        },
        LogOutput::StdIn { message } | LogOutput::Console { message } => RuntimeLogFrame {
            stream: RuntimeLogStream::Console,
            message: message.to_vec(),
        },
    }
}

/// Normalizes low-level Docker API errors into stable runtime error variants.
pub(super) fn classify_runtime_error(runtime_id: &str, err: BollardError) -> RuntimeError {
    match &err {
        BollardError::DockerResponseServerError { status_code, .. } if *status_code == 404 => {
            RuntimeError::NotFound(runtime_id.to_string())
        }
        BollardError::DockerResponseServerError {
            status_code,
            message,
        } => RuntimeError::backend(Some(*status_code), format!("docker api error: {message}")),
        _ => RuntimeError::backend(None, format!("docker api error: {err}")),
    }
}

/// Converts one inspect response into the generic runtime info shape used
/// outside the backend.
pub(super) fn runtime_info_from_inspect(inspect: ContainerInspectResponse) -> RuntimeInfo {
    let image = inspect
        .config
        .as_ref()
        .and_then(|config| config.image.clone())
        .unwrap_or_default();
    let tty = inspect.config.as_ref().and_then(|config| config.tty);
    let labels = inspect
        .config
        .as_ref()
        .and_then(|config| config.labels.clone())
        .unwrap_or_default();
    let raw_status = inspect
        .state
        .as_ref()
        .and_then(|state| state.status.as_ref())
        .map(|status| status.to_string());
    let running = inspect.state.as_ref().and_then(|state| state.running);
    let pid = inspect.state.as_ref().and_then(|state| state.pid);
    let exit_code = inspect
        .state
        .as_ref()
        .and_then(|state| state.exit_code)
        .and_then(|value| i32::try_from(value).ok());
    let error = inspect.state.as_ref().and_then(|state| state.error.clone());
    let attachment_target = inspect.state.as_ref().and_then(|state| {
        if !state.running.unwrap_or(false) {
            return None;
        }
        state
            .pid
            .filter(|pid| *pid > 0)
            .and_then(|pid| i32::try_from(pid).ok())
            .map(RuntimeAttachmentTarget::NetworkNamespacePid)
    });
    let network_endpoints = inspect
        .network_settings
        .as_ref()
        .and_then(|settings| settings.networks.as_ref())
        .map(|networks| {
            networks
                .iter()
                .map(|(name, endpoint)| RuntimeNetworkEndpoint {
                    name: name.clone(),
                    ip_address: endpoint.ip_address.clone(),
                })
                .collect()
        })
        .unwrap_or_default();

    RuntimeInfo {
        id: inspect.id.unwrap_or_default(),
        name: inspect
            .name
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string(),
        image,
        labels,
        status: raw_status.clone().unwrap_or_default(),
        state: RuntimeStateInfo {
            raw_status,
            running,
            pid,
            exit_code,
            error,
        },
        // Docker inspect exposes an RFC3339 timestamp string here, while the
        // generic runtime metadata keeps the sortable creation field in the
        // list and inventory path only.
        created: 0,
        config: RuntimeConfigInfo { tty },
        attachment_target,
        network_endpoints,
    }
}

/// Converts one Docker list response entry into the generic runtime info
/// shape.
pub(super) fn runtime_info_from_list_entry(
    entry: bollard::models::ContainerSummary,
) -> RuntimeInfo {
    let id = entry.id.unwrap_or_default();
    let name = entry
        .names
        .unwrap_or_default()
        .first()
        .cloned()
        .unwrap_or_default()
        .trim_start_matches('/')
        .to_string();
    let image = entry.image.unwrap_or_default();
    let labels = entry.labels.unwrap_or_default();
    let status = entry.status.unwrap_or_default();
    let raw_status = entry.state.map(|value| value.to_string());
    let running = raw_status.as_deref().map(|state| state == "running");
    let created = entry.created.unwrap_or_default();

    RuntimeInfo {
        id,
        name,
        image,
        labels,
        status,
        state: RuntimeStateInfo {
            raw_status,
            running,
            ..Default::default()
        },
        created,
        ..Default::default()
    }
}
