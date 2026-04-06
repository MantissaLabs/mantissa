use std::env;
use std::ffi::OsString;
use std::path::Path;
use std::process::ExitCode;

use mantissa::runtime::oci::docker::{MANTISSA_NONO_POLICY_ENV_VAR, NONO_EXEC_READONLY_DIRS};
use mantissa::runtime::types::{
    RuntimeSandboxAccessMode, RuntimeSandboxNetworkMode, RuntimeSandboxPathKind,
    RuntimeSandboxPathRule, RuntimeSandboxPolicy, RuntimeSandboxPolicyCodecError,
};
#[cfg(target_os = "linux")]
use nono::sandbox::SeccompNetFallback;
use nono::{AccessMode, CapabilitySet, Sandbox};
use thiserror::Error;

/// Enters the `nono` sandbox and then replaces itself with the target workload command.
fn main() -> ExitCode {
    match run_nono_helper(std::env::args_os().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("mantissa-nono-init: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Errors returned by the helper while it translates one Mantissa policy into `nono`.
#[derive(Debug, Error)]
enum NonoHelperError {
    #[error("sandbox policy environment variable {env_var} is missing")]
    MissingPolicyEnv { env_var: &'static str },

    #[error("sandbox policy environment variable {env_var} is not valid unicode")]
    InvalidPolicyEnv { env_var: &'static str },

    #[error("sandbox policy transport is invalid: {0}")]
    PolicyCodec(#[from] RuntimeSandboxPolicyCodecError),

    #[error("sandboxed command must contain at least one argument")]
    MissingCommand,

    #[error("sandbox working directory must resolve to a directory: {0}")]
    WorkingDirectoryNotDirectory(String),

    #[error("sandbox working directory is not readable under the declared policy: {0}")]
    WorkingDirectoryNotAllowed(String),

    #[error("sandbox initialization failed: {0}")]
    Sandbox(#[from] nono::NonoError),

    #[error("sandbox helper io failed: {0}")]
    Io(#[from] std::io::Error),

    #[cfg(target_os = "linux")]
    #[error("proxy-only network fallback is not supported by mantissa's nono helper")]
    UnsupportedProxyFallback,
}

/// Runs the full helper flow: decode policy, apply `nono`, and `exec` the target command.
fn run_nono_helper<I>(args: I) -> Result<(), NonoHelperError>
where
    I: IntoIterator<Item = OsString>,
{
    let policy = load_runtime_sandbox_policy_from_env()?;
    let command = resolve_target_command_from_args(args)?;

    if let Some(working_directory) = policy.working_directory.as_ref() {
        env::set_current_dir(working_directory)?;
    }

    apply_runtime_sandbox_policy(&policy)?;
    exec_target_command(&command)
}

/// Loads one serialized runtime sandbox policy from the helper environment.
fn load_runtime_sandbox_policy_from_env() -> Result<RuntimeSandboxPolicy, NonoHelperError> {
    let encoded = match env::var(MANTISSA_NONO_POLICY_ENV_VAR) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => {
            return Err(NonoHelperError::MissingPolicyEnv {
                env_var: MANTISSA_NONO_POLICY_ENV_VAR,
            });
        }
        Err(env::VarError::NotUnicode(_)) => {
            return Err(NonoHelperError::InvalidPolicyEnv {
                env_var: MANTISSA_NONO_POLICY_ENV_VAR,
            });
        }
    };

    RuntimeSandboxPolicy::decode_env_value(&encoded).map_err(NonoHelperError::from)
}

/// Resolves the workload command that the helper must `exec` after sandboxing.
fn resolve_target_command_from_args<I>(args: I) -> Result<Vec<OsString>, NonoHelperError>
where
    I: IntoIterator<Item = OsString>,
{
    let command = args.into_iter().collect::<Vec<_>>();
    if command.is_empty() {
        return Err(NonoHelperError::MissingCommand);
    }
    Ok(command)
}

/// Applies one structured runtime sandbox policy through `nono`.
fn apply_runtime_sandbox_policy(policy: &RuntimeSandboxPolicy) -> Result<(), NonoHelperError> {
    let capabilities = build_capability_set(policy)?;

    #[cfg(target_os = "linux")]
    match Sandbox::apply(&capabilities)? {
        SeccompNetFallback::None | SeccompNetFallback::BlockAll => Ok(()),
        SeccompNetFallback::ProxyOnly { .. } => Err(NonoHelperError::UnsupportedProxyFallback),
    }

    #[cfg(not(target_os = "linux"))]
    {
        Sandbox::apply(&capabilities)?;
        Ok(())
    }
}

/// Builds one `nono` capability set from one runtime sandbox policy.
fn build_capability_set(policy: &RuntimeSandboxPolicy) -> Result<CapabilitySet, NonoHelperError> {
    let mut capabilities = CapabilitySet::new();
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

/// Adds one declared filesystem rule to the accumulating `nono` capability set.
fn add_path_rule(
    capabilities: CapabilitySet,
    rule: &RuntimeSandboxPathRule,
) -> Result<CapabilitySet, NonoHelperError> {
    if !rule.path.exists() && is_optional_bootstrap_rule(rule) {
        return Ok(capabilities);
    }

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

/// Returns whether one filesystem rule is an optional bootstrap directory that may be absent.
fn is_optional_bootstrap_rule(rule: &RuntimeSandboxPathRule) -> bool {
    rule.kind == RuntimeSandboxPathKind::Directory
        && rule.access == RuntimeSandboxAccessMode::Read
        && NONO_EXEC_READONLY_DIRS
            .iter()
            .any(|candidate| rule.path == Path::new(candidate))
}

/// Maps one Mantissa sandbox access mode into the `nono` capability model.
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
) -> Result<(), NonoHelperError> {
    let Some(working_directory) = policy.working_directory.as_ref() else {
        return Ok(());
    };

    let resolved = working_directory.canonicalize()?;
    if !resolved.is_dir() {
        return Err(NonoHelperError::WorkingDirectoryNotDirectory(
            resolved.display().to_string(),
        ));
    }

    if capabilities.path_covered_with_access(Path::new(&resolved), AccessMode::Read) {
        return Ok(());
    }

    Err(NonoHelperError::WorkingDirectoryNotAllowed(
        resolved.display().to_string(),
    ))
}

/// Replaces the helper process with the real workload command after sandboxing.
fn exec_target_command(command: &[OsString]) -> Result<(), NonoHelperError> {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        let mut process = std::process::Command::new(&command[0]);
        process.args(&command[1..]);
        Err(NonoHelperError::Io(process.exec()))
    }

    #[cfg(not(unix))]
    {
        let status = std::process::Command::new(&command[0])
            .args(&command[1..])
            .status()?;
        if status.success() {
            Ok(())
        } else {
            Err(NonoHelperError::Io(std::io::Error::other(format!(
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
        assert!(matches!(error, NonoHelperError::MissingCommand));
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

        build_capability_set(&policy).expect("working directory should be covered");
    }

    #[test]
    fn build_capability_set_rejects_working_directory_outside_policy() {
        let temp_dir = tempdir().expect("create temp dir");
        let workspace = temp_dir.path().join("workspace");
        let other_dir = temp_dir.path().join("other");
        std::fs::create_dir(&workspace).expect("create workspace");
        std::fs::create_dir(&other_dir).expect("create other dir");

        let policy = RuntimeSandboxPolicy {
            working_directory: Some(workspace.clone()),
            filesystem: vec![RuntimeSandboxPathRule::directory(
                other_dir,
                RuntimeSandboxAccessMode::ReadWrite,
            )],
            network: RuntimeSandboxNetworkMode::AllowAll,
        };

        let error = build_capability_set(&policy)
            .expect_err("helper should reject a workdir outside the policy");
        assert!(matches!(
            error,
            NonoHelperError::WorkingDirectoryNotAllowed(_)
        ));
    }

    #[test]
    fn optional_bootstrap_rule_matches_readonly_dir_entries() {
        let rule = RuntimeSandboxPathRule::directory("/lib64", RuntimeSandboxAccessMode::Read);

        assert!(is_optional_bootstrap_rule(&rule));
    }

    #[test]
    fn optional_bootstrap_rule_rejects_non_bootstrap_paths() {
        let rule = RuntimeSandboxPathRule::directory("/workspace", RuntimeSandboxAccessMode::Read);

        assert!(!is_optional_bootstrap_rule(&rule));
    }
}
