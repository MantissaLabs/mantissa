use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use mantissa::runtime::oci::docker::{
    MANTISSA_SANDBOX_CHECK_ARGUMENT, MANTISSA_SANDBOX_POLICY_ENV_VAR,
};
use mantissa::runtime::types::{
    RuntimeSandboxAccessMode, RuntimeSandboxNetworkMode, RuntimeSandboxPathKind,
    RuntimeSandboxPathRule, RuntimeSandboxPolicy, RuntimeSandboxPolicyCodecError,
};
#[cfg(target_os = "linux")]
use nono::sandbox::{SeccompNetFallback, SeccompOpts};
use nono::{AccessMode, CapabilitySet, Sandbox};
use thiserror::Error;

mod bootstrap;

/// Errors returned while the sandbox helper translates one Mantissa policy into active isolation.
#[derive(Debug, Error)]
pub enum SandboxInitError {
    #[error("sandbox policy environment variable {env_var} is missing")]
    MissingPolicyEnv { env_var: &'static str },

    #[error("sandbox policy environment variable {env_var} is not valid unicode")]
    InvalidPolicyEnv { env_var: &'static str },

    #[error("sandbox policy transport is invalid: {0}")]
    PolicyCodec(#[from] RuntimeSandboxPolicyCodecError),

    #[error("sandboxed command must contain at least one argument")]
    MissingCommand,

    #[error("sandbox command was not found or is not executable: {0}")]
    CommandNotExecutable(String),

    #[error("sandbox working directory must resolve to a directory: {0}")]
    WorkingDirectoryNotDirectory(String),

    #[error("sandbox working directory is not readable under the declared policy: {0}")]
    WorkingDirectoryNotAllowed(String),

    #[error("sandbox initialization failed: {0}")]
    Sandbox(#[from] nono::NonoError),

    #[error("sandbox helper io failed: {0}")]
    Io(#[from] std::io::Error),

    #[cfg(target_os = "linux")]
    #[error("proxy-only network fallback is not supported by the mantissa sandbox helper")]
    UnsupportedProxyFallback,

    #[cfg(target_os = "linux")]
    #[error(
        "sandbox requires Landlock ABI V3 or newer to protect read-only files from truncation, detected {0}"
    )]
    UnsupportedLandlockVersion(&'static str),
}

/// Restricts the workload before exec, or checks the same restrictions without running it.
///
/// Docker uses `--check` with the workload command to verify its image and policy
/// in a disposable container before creating the actual workload container.
pub fn run_sandbox_init<I>(args: I) -> Result<(), SandboxInitError>
where
    I: IntoIterator<Item = OsString>,
{
    let policy = load_runtime_sandbox_policy_from_env()?;
    let mut args = args.into_iter().peekable();
    let check_only = args
        .next_if(|arg| arg == MANTISSA_SANDBOX_CHECK_ARGUMENT)
        .is_some();
    let command = resolve_target_command_from_args(args)?;

    if let Some(working_directory) = policy.working_directory.as_ref() {
        env::set_current_dir(working_directory)?;
    }

    let executable = resolve_executable(&command[0])?;
    apply_runtime_sandbox_policy(&policy, &executable)?;

    if check_only {
        Ok(())
    } else {
        exec_target_command(&executable, &command)
    }
}

/// Loads one serialized runtime sandbox policy from the helper environment.
fn load_runtime_sandbox_policy_from_env() -> Result<RuntimeSandboxPolicy, SandboxInitError> {
    let encoded = match env::var(MANTISSA_SANDBOX_POLICY_ENV_VAR) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => {
            return Err(SandboxInitError::MissingPolicyEnv {
                env_var: MANTISSA_SANDBOX_POLICY_ENV_VAR,
            });
        }
        Err(env::VarError::NotUnicode(_)) => {
            return Err(SandboxInitError::InvalidPolicyEnv {
                env_var: MANTISSA_SANDBOX_POLICY_ENV_VAR,
            });
        }
    };

    RuntimeSandboxPolicy::decode_env_value(&encoded).map_err(SandboxInitError::from)
}

/// Resolves the workload command that the helper must `exec` after sandboxing.
fn resolve_target_command_from_args<I>(args: I) -> Result<Vec<OsString>, SandboxInitError>
where
    I: IntoIterator<Item = OsString>,
{
    let command = args.into_iter().collect::<Vec<_>>();
    if command.is_empty() {
        return Err(SandboxInitError::MissingCommand);
    }
    Ok(command)
}

/// Applies filesystem and network restrictions before starting the workload.
///
/// Linux needs the static seccomp filter even when Landlock can restrict TCP,
/// because Landlock does not block UDP. Proxy-dependent fallbacks are rejected.
fn apply_runtime_sandbox_policy(
    policy: &RuntimeSandboxPolicy,
    executable: &Path,
) -> Result<(), SandboxInitError> {
    let capabilities = build_capability_set(policy, executable)?;

    #[cfg(target_os = "linux")]
    {
        let abi = Sandbox::detect_abi()?;
        // Read-only permissions must prevent truncation as well as ordinary writes.
        if !abi.has_truncate() {
            return Err(SandboxInitError::UnsupportedLandlockVersion(
                abi.version_string(),
            ));
        }

        match Sandbox::apply_seccomp_with_abi(&capabilities, &abi, SeccompOpts::network_baseline())?
        {
            SeccompNetFallback::None | SeccompNetFallback::BlockAll => Ok(()),
            SeccompNetFallback::ProxyOnly { .. } => Err(SandboxInitError::UnsupportedProxyFallback),
        }
    }

    #[cfg(target_os = "macos")]
    {
        Sandbox::apply_auto(&capabilities)?;
        Ok(())
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    Err(nono::NonoError::UnsupportedPlatform(std::env::consts::OS.to_string()).into())
}

/// Adds declared permissions to the runtime files needed to launch the selected executable.
fn build_capability_set(
    policy: &RuntimeSandboxPolicy,
    executable: &Path,
) -> Result<CapabilitySet, SandboxInitError> {
    let mut capabilities = bootstrap::allow_runtime_files(CapabilitySet::new(), executable)?;
    for rule in &policy.filesystem {
        capabilities = add_path_rule(capabilities, rule)?;
    }

    capabilities = match policy.network {
        RuntimeSandboxNetworkMode::AllowAll => capabilities,
        RuntimeSandboxNetworkMode::Blocked => capabilities.block_network(),
    };

    validate_working_directory(policy, &capabilities)?;
    Ok(capabilities)
}

/// Adds one declared filesystem rule to the accumulating sandbox capability set.
fn add_path_rule(
    capabilities: CapabilitySet,
    rule: &RuntimeSandboxPathRule,
) -> Result<CapabilitySet, SandboxInitError> {
    let access = access_mode(rule.access);
    match rule.kind {
        RuntimeSandboxPathKind::Directory => capabilities
            .allow_path(&rule.path, access)
            .map_err(Into::into),
        RuntimeSandboxPathKind::File => capabilities
            .allow_file(&rule.path, access)
            .map_err(Into::into),
    }
}

/// Resolves PATH before restricting access so exec uses the same file that received permission.
fn resolve_executable(command: &std::ffi::OsStr) -> Result<PathBuf, SandboxInitError> {
    let path = Path::new(command);
    let candidates = if path.components().count() > 1 || path.is_absolute() {
        vec![path.to_path_buf()]
    } else {
        env::split_paths(&env::var_os("PATH").unwrap_or_else(|| "/bin:/usr/bin".into()))
            .map(|directory| directory.join(path))
            .collect()
    };

    for candidate in candidates {
        let Ok(metadata) = candidate.metadata() else {
            continue;
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            if metadata.permissions().mode() & 0o111 == 0 {
                continue;
            }
        }

        if metadata.is_file() {
            return Ok(if candidate.is_absolute() {
                candidate
            } else {
                env::current_dir()?.join(candidate)
            });
        }
    }

    Err(SandboxInitError::CommandNotExecutable(
        command.to_string_lossy().into_owned(),
    ))
}
/// Maps one Mantissa sandbox access mode into the current helper backend model.
fn access_mode(mode: RuntimeSandboxAccessMode) -> AccessMode {
    match mode {
        RuntimeSandboxAccessMode::Read => AccessMode::Read,
        RuntimeSandboxAccessMode::Write => AccessMode::Write,
        RuntimeSandboxAccessMode::ReadWrite => AccessMode::ReadWrite,
    }
}

/// Verifies that the declared working directory remains readable after the sandbox applies.
fn validate_working_directory(
    policy: &RuntimeSandboxPolicy,
    capabilities: &CapabilitySet,
) -> Result<(), SandboxInitError> {
    let Some(working_directory) = policy.working_directory.as_ref() else {
        return Ok(());
    };

    let resolved = working_directory.canonicalize()?;
    if !resolved.is_dir() {
        return Err(SandboxInitError::WorkingDirectoryNotDirectory(
            resolved.display().to_string(),
        ));
    }

    if capabilities.path_covered_with_access(Path::new(&resolved), AccessMode::Read) {
        return Ok(());
    }

    Err(SandboxInitError::WorkingDirectoryNotAllowed(
        resolved.display().to_string(),
    ))
}

/// Replaces the helper process with the real workload command after sandboxing.
fn exec_target_command(executable: &Path, command: &[OsString]) -> Result<(), SandboxInitError> {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        let mut process = std::process::Command::new(executable);
        // Preserve names used by programs such as BusyBox to select their behavior.
        process.arg0(&command[0]);
        process.args(&command[1..]);
        Err(SandboxInitError::Io(process.exec()))
    }

    #[cfg(not(unix))]
    {
        let status = std::process::Command::new(executable)
            .args(&command[1..])
            .status()?;
        if status.success() {
            Ok(())
        } else {
            Err(SandboxInitError::Io(std::io::Error::other(format!(
                "sandboxed command exited with status {status}"
            ))))
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn resolve_target_command_from_args_rejects_empty_input() {
        let error = resolve_target_command_from_args(Vec::<OsString>::new())
            .expect_err("helper should reject empty commands");
        assert!(matches!(error, SandboxInitError::MissingCommand));
    }

    #[test]
    fn build_capability_set_accepts_readable_working_directory() {
        let temp_dir = tempdir().expect("create temp dir");
        let workspace = temp_dir.path().join("workspace");
        std::fs::create_dir(&workspace).expect("create workspace");

        let policy = RuntimeSandboxPolicy {
            working_directory: Some(workspace.clone()),
            filesystem: vec![RuntimeSandboxPathRule::directory(
                workspace,
                RuntimeSandboxAccessMode::ReadWrite,
            )],
            network: RuntimeSandboxNetworkMode::Blocked,
        };

        build_capability_set(&policy, &env::current_exe().expect("test executable"))
            .expect("working directory should be covered");
    }

    #[test]
    fn build_capability_set_rejects_working_directory_outside_policy() {
        let temp_dir = tempdir().expect("create temp dir");
        let workspace = temp_dir.path().join("workspace");
        std::fs::create_dir(&workspace).expect("create workspace");
        let denied = temp_dir.path().join("denied");
        std::fs::create_dir(&denied).expect("create denied dir");

        let policy = RuntimeSandboxPolicy {
            working_directory: Some(denied.clone()),
            filesystem: vec![RuntimeSandboxPathRule::directory(
                workspace,
                RuntimeSandboxAccessMode::ReadWrite,
            )],
            network: RuntimeSandboxNetworkMode::Blocked,
        };

        let error = build_capability_set(&policy, &env::current_exe().expect("test executable"))
            .expect_err("helper should reject denied workdir");
        assert!(matches!(
            error,
            SandboxInitError::WorkingDirectoryNotAllowed(path)
            if path == denied.display().to_string()
        ));
    }
}
