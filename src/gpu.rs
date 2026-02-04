use std::path::Path;

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
