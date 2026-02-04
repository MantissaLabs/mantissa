use std::path::Path;

use tracing::warn;

const GPU_OVERRIDE_ENV: &str = "MANTISSA_GPU_DEVICE_OVERRIDES";

/// # Description:
///
/// Identifies a GPU device using stable inventory attributes so overrides can
/// target a specific physical accelerator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GpuDeviceSelector {
    Uuid(String),
    PciBusId(String),
    Index(u32),
}

/// # Description:
///
/// Represents the override behavior applied to a matching GPU device.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GpuDeviceOverrideAction {
    Disable,
    OverrideId(String),
}

/// # Description:
///
/// Binds a selector to an override action to control GPU inventory reporting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GpuDeviceOverride {
    pub selector: GpuDeviceSelector,
    pub action: GpuDeviceOverrideAction,
}

/// # Description:
///
/// Reads GPU device overrides from the environment so operators can pin or
/// disable specific devices without code changes.
pub fn read_gpu_device_overrides() -> Vec<GpuDeviceOverride> {
    match std::env::var(GPU_OVERRIDE_ENV) {
        Ok(raw) => parse_gpu_device_overrides(&raw),
        Err(std::env::VarError::NotPresent) => Vec::new(),
        Err(err) => {
            warn!(
                target: "gpu",
                "failed to read {GPU_OVERRIDE_ENV} overrides: {err}"
            );
            Vec::new()
        }
    }
}

/// # Description:
///
/// Returns the first override that matches the supplied GPU identity fields.
pub fn gpu_device_override_for<'a>(
    uuid: Option<&str>,
    pci_bus_id: Option<&str>,
    index: u32,
    overrides: &'a [GpuDeviceOverride],
) -> Option<&'a GpuDeviceOverride> {
    overrides.iter().find(|entry| {
        selector_matches_device(&entry.selector, uuid, pci_bus_id, index)
    })
}

/// # Description:
///
/// Parses the override configuration string into structured overrides.
fn parse_gpu_device_overrides(raw: &str) -> Vec<GpuDeviceOverride> {
    let mut overrides = Vec::new();

    for entry in raw.split(';') {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }

        match parse_gpu_device_override(trimmed) {
            Some(override_entry) => overrides.push(override_entry),
            None => warn!(target: "gpu", "ignoring invalid gpu override '{trimmed}'"),
        }
    }

    overrides
}

/// # Description:
///
/// Parses a single override entry in `selector=action` form.
fn parse_gpu_device_override(entry: &str) -> Option<GpuDeviceOverride> {
    let (selector_raw, action_raw) = entry.split_once('=')?;
    let selector = parse_override_selector(selector_raw.trim())?;
    let action = parse_override_action(action_raw.trim())?;
    Some(GpuDeviceOverride { selector, action })
}

/// # Description:
///
/// Parses the selector portion of an override entry.
fn parse_override_selector(raw: &str) -> Option<GpuDeviceSelector> {
    let (kind_raw, value_raw) = raw.split_once(':')?;
    let kind = kind_raw.trim().to_ascii_lowercase();
    let value = value_raw.trim();
    if value.is_empty() {
        return None;
    }

    match kind.as_str() {
        "uuid" => Some(GpuDeviceSelector::Uuid(value.to_string())),
        "pci" | "pcibus" | "pcibusid" => Some(GpuDeviceSelector::PciBusId(value.to_string())),
        "index" => value.parse::<u32>().ok().map(GpuDeviceSelector::Index),
        _ => None,
    }
}

/// # Description:
///
/// Parses the action portion of an override entry.
fn parse_override_action(raw: &str) -> Option<GpuDeviceOverrideAction> {
    let action = raw.trim();
    if action.eq_ignore_ascii_case("disable") || action.eq_ignore_ascii_case("disabled") {
        return Some(GpuDeviceOverrideAction::Disable);
    }

    let Some(rest) = action.strip_prefix("id:") else {
        return None;
    };

    let trimmed = rest.trim();
    if trimmed.is_empty() {
        return None;
    }

    Some(GpuDeviceOverrideAction::OverrideId(trimmed.to_string()))
}

/// # Description:
///
/// Checks whether a selector matches the provided GPU identity fields.
fn selector_matches_device(
    selector: &GpuDeviceSelector,
    uuid: Option<&str>,
    pci_bus_id: Option<&str>,
    index: u32,
) -> bool {
    match selector {
        GpuDeviceSelector::Uuid(value) => uuid
            .map(|candidate| candidate.eq_ignore_ascii_case(value))
            .unwrap_or(false),
        GpuDeviceSelector::PciBusId(value) => pci_bus_id
            .map(|candidate| candidate.eq_ignore_ascii_case(value))
            .unwrap_or(false),
        GpuDeviceSelector::Index(value) => *value == index,
    }
}

/// # Description:
///
/// Reports whether the host runtime is prepared to run NVIDIA GPU workloads so
/// the scheduler can avoid placing GPU tasks on misconfigured nodes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GpuRuntimeStatus {
    Ready,
    NotReady(&'static str),
}

impl GpuRuntimeStatus {
    /// # Description:
    ///
    /// Returns true when the runtime has the prerequisites to launch GPU-bound
    /// containers so callers can gate scheduling decisions.
    pub fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }

    /// # Description:
    ///
    /// Provides a short explanation when GPU runtime prerequisites are missing
    /// to surface actionable diagnostics.
    pub fn reason(self) -> Option<&'static str> {
        match self {
            Self::Ready => None,
            Self::NotReady(reason) => Some(reason),
        }
    }
}

/// # Description:
///
/// Checks the host operating system and NVIDIA tooling so GPU scheduling can
/// short-circuit when the runtime prerequisites are unavailable.
pub fn gpu_runtime_status() -> GpuRuntimeStatus {
    #[cfg(target_os = "linux")]
    {
        if nvidia_toolkit_present() {
            GpuRuntimeStatus::Ready
        } else {
            GpuRuntimeStatus::NotReady(
                "nvidia container toolkit not detected; install drivers and toolkit (see docs/gpu-setup.md)",
            )
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        GpuRuntimeStatus::NotReady(
            "gpu scheduling requires a Linux host with NVIDIA drivers and toolkit",
        )
    }
}

/// # Description:
///
/// Detects whether NVIDIA container runtime tooling is available on the host to
/// support GPU-enabled Docker workloads.
pub fn nvidia_toolkit_present() -> bool {
    let absolute_candidates = [
        "/usr/bin/nvidia-container-runtime",
        "/usr/bin/nvidia-container-cli",
        "/usr/bin/nvidia-container-toolkit",
        "/usr/local/bin/nvidia-container-runtime",
        "/usr/local/bin/nvidia-container-cli",
        "/usr/local/bin/nvidia-container-toolkit",
    ];

    if absolute_candidates
        .iter()
        .any(|path| Path::new(path).exists())
    {
        return true;
    }

    let Ok(path) = std::env::var("PATH") else {
        return false;
    };

    for dir in path.split(':') {
        for candidate in [
            "nvidia-container-runtime",
            "nvidia-container-cli",
            "nvidia-container-toolkit",
        ] {
            if Path::new(dir).join(candidate).exists() {
                return true;
            }
        }
    }

    false
}
