//! Runtime permissions shared by launches and later exec commands inside the container.

use std::path::Path;

use nono::{AccessMode, CapabilitySet};

use crate::{RuntimeSandboxPathKind, SandboxInitError};

/// Limits automatic directory access to programs, libraries, certificates, and time zones.
const READ_DIRECTORIES: &[&str] = &[
    "/bin",
    "/sbin",
    "/lib",
    "/lib64",
    "/usr/bin",
    "/usr/sbin",
    "/usr/lib",
    "/usr/lib64",
    "/usr/libexec",
    "/usr/local/bin",
    "/usr/local/lib",
    "/usr/local/lib64",
    "/etc/ssl/certs",
    "/etc/pki/tls/certs",
    "/etc/pki/ca-trust/extracted",
    "/usr/share/zoneinfo",
];

/// These individual files provide loading, name resolution, time, and random data.
const READ_FILES: &[&str] = &[
    "/etc/ld.so.cache",
    "/etc/hosts",
    "/etc/resolv.conf",
    "/etc/nsswitch.conf",
    "/etc/host.conf",
    "/etc/gai.conf",
    "/etc/passwd",
    "/etc/group",
    "/etc/localtime",
    "/dev/urandom",
    "/dev/random",
    "/dev/zero",
];

/// Grants runtime files without exposing the image workdir or an executable's siblings.
pub(super) fn allow_runtime_files(
    mut capabilities: CapabilitySet,
    executable: &Path,
) -> Result<CapabilitySet, SandboxInitError> {
    for path in READ_DIRECTORIES {
        capabilities = allow_optional_path(
            capabilities,
            path,
            AccessMode::Read,
            RuntimeSandboxPathKind::Directory,
        )?;
    }
    for path in READ_FILES {
        capabilities = allow_optional_path(
            capabilities,
            path,
            AccessMode::Read,
            RuntimeSandboxPathKind::File,
        )?;
    }

    // Output redirection needs a writable null device, without access to other devices.
    capabilities = allow_optional_path(
        capabilities,
        "/dev/null",
        AccessMode::ReadWrite,
        RuntimeSandboxPathKind::File,
    )?;
    Ok(capabilities.allow_file(executable, AccessMode::Read)?)
}

/// Skips runtime files absent from an image, but preserves permission and type errors.
fn allow_optional_path(
    capabilities: CapabilitySet,
    path: &str,
    access: AccessMode,
    kind: RuntimeSandboxPathKind,
) -> Result<CapabilitySet, SandboxInitError> {
    match std::fs::metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(capabilities),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }

    match kind {
        RuntimeSandboxPathKind::Directory => Ok(capabilities.allow_path(path, access)?),
        RuntimeSandboxPathKind::File => Ok(capabilities.allow_file(path, access)?),
    }
}
