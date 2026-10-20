#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

use mantissa::runtime::oci::docker::{DockerRuntimeBackend, MANTISSA_SANDBOX_POLICY_ENV_VAR};
use mantissa::runtime::types::{
    RuntimeBackend, RuntimeCreateRequest, RuntimeLogsOptions, RuntimeSandboxAccessMode,
    RuntimeSandboxNetworkMode, RuntimeSandboxPathRule, RuntimeSandboxPolicy,
};
use mantissa::workload::model::IsolationMode;

const HELPER: &str = env!("CARGO_BIN_EXE_mantissa-sandbox-init");

/// Owns executable and data fixtures so tests can distinguish policy denials from Unix permissions.
struct Fixture {
    directory: tempfile::TempDir,
    root: PathBuf,
    image: Option<String>,
    containers: Vec<String>,
}

impl Fixture {
    /// Builds a small standalone workload that tests actual file and socket operations.
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("rootfs");
        for path in ["app", "workspace", "readonly", "private"] {
            std::fs::create_dir_all(root.join(path)).unwrap();
        }
        std::fs::write(root.join("readonly/input"), "readable").unwrap();
        std::fs::write(root.join("private/secret"), "private").unwrap();
        std::fs::write(root.join("app/neighbor"), "private").unwrap();
        std::os::unix::fs::symlink(root.join("private/secret"), root.join("workspace/link"))
            .unwrap();

        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/probe.rs");
        let output = Command::new("rustc")
            .args(["--edition=2024", "-o"])
            .arg(root.join("app/program"))
            .arg(source)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::os::unix::fs::symlink("program", root.join("app/probe")).unwrap();

        Self {
            directory,
            root,
            image: None,
            containers: Vec::new(),
        }
    }

    /// Grants data explicitly while keeping executable siblings and private files outside the policy.
    fn policy(root: &Path, network: RuntimeSandboxNetworkMode) -> RuntimeSandboxPolicy {
        RuntimeSandboxPolicy {
            working_directory: Some(root.join("workspace")),
            filesystem: vec![
                RuntimeSandboxPathRule::directory(
                    root.join("workspace"),
                    RuntimeSandboxAccessMode::ReadWrite,
                ),
                RuntimeSandboxPathRule::directory(
                    root.join("readonly"),
                    RuntimeSandboxAccessMode::Read,
                ),
            ],
            network,
        }
    }

    /// Runs the production helper directly, including its check-only entry point.
    fn run_helper(
        &self,
        policy: &RuntimeSandboxPolicy,
        network: &str,
        check: bool,
        mode: &str,
    ) -> Output {
        let mut command = Command::new(HELPER);
        if check {
            command.arg("--check");
        }
        command
            .arg(self.root.join("app/probe"))
            .arg(&self.root)
            .args([network, mode])
            .env(
                MANTISSA_SANDBOX_POLICY_ENV_VAR,
                policy.encode_env_value().unwrap(),
            )
            .output()
            .unwrap()
    }

    /// Packages host-compatible loader files into a disposable image without weakening Docker security.
    fn build_image(&mut self) {
        docker(&["image", "inspect", "busybox:1.37.0"]);
        for binary in [Path::new(HELPER), &self.root.join("app/program")] {
            let output = Command::new("ldd").arg(binary).output().unwrap();
            assert!(output.status.success(), "ldd failed");
            for path in String::from_utf8_lossy(&output.stdout)
                .split_whitespace()
                .filter(|word| word.starts_with('/'))
            {
                let destination = self.root.join(path.trim_start_matches('/'));
                std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
                std::fs::copy(path, destination).unwrap();
            }
        }

        for path in ["etc/mantissa-test-secret", "usr/share/mantissa-test-secret"] {
            let destination = self.root.join(path);
            std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
            std::fs::write(destination, "private").unwrap();
        }
        std::fs::remove_file(self.root.join("workspace/link")).unwrap();
        std::os::unix::fs::symlink("/private/secret", self.root.join("workspace/link")).unwrap();
        std::fs::write(
            self.directory.path().join("Dockerfile"),
            "FROM busybox:1.37.0\nCOPY rootfs/ /\nWORKDIR /\n",
        )
        .unwrap();

        let image = format!(
            "mantissa-sandbox-test:{}",
            self.directory
                .path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .trim_start_matches('.')
        );
        self.image = Some(image.clone());
        docker(&[
            "build",
            "--pull=false",
            "--tag",
            &image,
            self.directory.path().to_str().unwrap(),
        ]);
    }
}

impl Drop for Fixture {
    /// Removes only this test's containers and image, including after an assertion fails.
    fn drop(&mut self) {
        for container in &self.containers {
            let _ = Command::new("docker")
                .args(["rm", "--force", container])
                .output();
        }
        if let Some(image) = &self.image {
            let _ = Command::new("docker")
                .args(["image", "rm", "--force", image])
                .output();
        }
    }
}

/// Keeps Docker command failures attached to the assertion that created the fixture.
fn docker(args: &[&str]) -> Output {
    let output = Command::new("docker").args(args).output().unwrap();
    assert!(
        output.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

/// Checks the real kernel boundary, including child processes, symlinks, and UDP restrictions.
#[test]
fn helper_enforces_filesystem_and_network_policy() {
    let support = nono::Sandbox::support_info();
    if !support.is_supported {
        eprintln!("skipping kernel enforcement test: {}", support.details);
        return;
    }

    let fixture = Fixture::new();
    if !nono::Sandbox::detect_abi().unwrap().has_truncate() {
        let policy = Fixture::policy(&fixture.root, RuntimeSandboxNetworkMode::Blocked);
        let rejected = fixture.run_helper(&policy, "blocked", true, "mark");
        assert!(!rejected.status.success());
        assert!(String::from_utf8_lossy(&rejected.stderr).contains("requires Landlock ABI V3"));
        assert!(!fixture.root.join("workspace/check-executed").exists());
        return;
    }

    for (network, name) in [
        (RuntimeSandboxNetworkMode::AllowAll, "allow_all"),
        (RuntimeSandboxNetworkMode::Blocked, "blocked"),
    ] {
        let policy = Fixture::policy(&fixture.root, network);
        let checked = fixture.run_helper(&policy, name, true, "mark");
        assert!(
            checked.status.success(),
            "{}",
            String::from_utf8_lossy(&checked.stderr)
        );
        assert!(!fixture.root.join("workspace/check-executed").exists());

        let output = fixture.run_helper(&policy, name, false, "");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("SANDBOX_OK"));
    }

    let policy = Fixture::policy(&fixture.root, RuntimeSandboxNetworkMode::Blocked);
    let by_name = Command::new(HELPER)
        .args(["probe", fixture.root.to_str().unwrap(), "blocked"])
        .env("PATH", fixture.root.join("app"))
        .env("EXPECTED_ARG0", "probe")
        .env(
            MANTISSA_SANDBOX_POLICY_ENV_VAR,
            policy.encode_env_value().unwrap(),
        )
        .output()
        .unwrap();
    assert!(
        by_name.status.success(),
        "{}",
        String::from_utf8_lossy(&by_name.stderr)
    );

    let mut policy = Fixture::policy(&fixture.root, RuntimeSandboxNetworkMode::Blocked);
    policy.working_directory = Some(fixture.root.join("private"));
    let rejected = fixture.run_helper(&policy, "blocked", true, "mark");
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("working directory is not readable")
    );
    assert!(!fixture.root.join("workspace/check-executed").exists());
}

/// Verifies create, later exec, and a fresh backend against persisted container policy.
#[tokio::test]
#[ignore = "requires Docker, busybox:1.37.0, and Linux Landlock"]
async fn docker_enforces_policy_after_backend_restart() {
    let mut fixture = Fixture::new();
    fixture.build_image();
    let image = fixture.image.clone().unwrap();

    for (network, name) in [
        (RuntimeSandboxNetworkMode::AllowAll, "allow_all"),
        (RuntimeSandboxNetworkMode::Blocked, "blocked"),
    ] {
        let backend = DockerRuntimeBackend::new().await.unwrap();
        let request = RuntimeCreateRequest {
            name: format!(
                "mantissa-enforcement-{name}-{}",
                fixture
                    .directory
                    .path()
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
            ),
            image: image.clone(),
            isolation_mode: IsolationMode::Sandboxed,
            sandbox_policy: Some(Fixture::policy(Path::new("/"), network)),
            command: Some(vec![
                "/app/probe".to_string(),
                "/".to_string(),
                name.to_string(),
                "hold".to_string(),
            ]),
            volumes: Some(vec![
                format!("{}:/workspace:rw", fixture.root.join("workspace").display()),
                format!("{}:/readonly:rw", fixture.root.join("readonly").display()),
            ]),
            ..Default::default()
        };
        let id = backend.create_instance(request).await.unwrap();
        fixture.containers.push(id.clone());
        backend.start_instance(&id).await.unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let options = RuntimeLogsOptions {
            follow: true,
            ..Default::default()
        };
        let mut logs = Box::pin(backend.stream_instance_logs(&id, &options, tx));
        tokio::time::timeout(Duration::from_secs(10), async {
            // Wait for the workload's assertions before testing later exec commands.
            loop {
                tokio::select! {
                    biased;
                    frame = rx.recv() => {
                        let frame = frame.expect("probe log stream closed before success");
                        if String::from_utf8_lossy(&frame.message).contains("SANDBOX_OK") {
                            break;
                        }
                    }
                    result = &mut logs => { result.unwrap(); panic!("probe did not report success"); }
                }
            }
        }).await.unwrap();
        drop(logs);
        drop(backend);

        let backend = DockerRuntimeBackend::new().await.unwrap();
        let command = vec!["/app/probe".to_string(), "/".to_string(), name.to_string()];
        assert_eq!(
            backend
                .exec_instance(&id, &command, Some(Duration::from_secs(10)))
                .await
                .unwrap()
                .exit_code,
            Some(0)
        );
        let shell = vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "if cat /private/secret >/dev/null 2>&1; then exit 1; fi; echo ok >/workspace/shell-output".to_string(),
        ];
        assert_eq!(
            backend
                .exec_instance(&id, &shell, Some(Duration::from_secs(10)))
                .await
                .unwrap()
                .exit_code,
            Some(0)
        );
        backend.remove_instance(&id, true, false).await.unwrap();
    }

    let seccomp = fixture.directory.path().join("deny-landlock.json");
    std::fs::write(&seccomp, r#"{"defaultAction":"SCMP_ACT_ALLOW","syscalls":[{"names":["landlock_create_ruleset"],"action":"SCMP_ACT_ERRNO","errnoRet":1}]}"#).unwrap();
    let helper_mount = format!("{HELPER}:/mantissa-sandbox-init:ro");
    let policy = Fixture::policy(Path::new("/"), RuntimeSandboxNetworkMode::Blocked)
        .encode_env_value()
        .unwrap();
    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--security-opt",
            &format!("seccomp={}", seccomp.display()),
            "--volume",
            &helper_mount,
            "--env",
            &format!("{MANTISSA_SANDBOX_POLICY_ENV_VAR}={policy}"),
            "--entrypoint",
            "/mantissa-sandbox-init",
            &image,
            "--check",
            "/app/probe",
            "/",
            "blocked",
            "mark",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Landlock"));
}
