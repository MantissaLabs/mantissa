#[cfg(target_os = "linux")]
use anyhow::{anyhow, bail, Context, Result};
#[cfg(target_os = "linux")]
use cargo_metadata::{Artifact, CompilerMessage, Message, MetadataCommand, Target};
#[cfg(target_os = "linux")]
use std::{
    borrow::Cow,
    env,
    ffi::OsString,
    fs,
    io::{self, BufRead, BufReader},
    path::PathBuf,
    process::{Command, Stdio},
    thread,
};

#[cfg(not(target_os = "linux"))]
use anyhow::Result;

#[cfg(target_os = "linux")]
/// Normalize the architecture string used for BPF compilation so cache keys
/// remain stable across closely related targets.
fn target_arch_fixup(target_arch: Cow<'_, str>) -> Cow<'_, str> {
    if target_arch.starts_with("riscv64") {
        Cow::from("riscv64")
    } else {
        target_arch
    }
}

#[cfg(target_os = "linux")]
/// Build the workspace eBPF binaries while sending cargo output to stderr so
/// progress lines are not promoted to build-script warnings.
fn build_ebpf_without_warnings<'a>(
    packages: impl IntoIterator<Item = aya_build::Package<'a>>,
    toolchain: aya_build::Toolchain<'a>,
) -> Result<()> {
    let out_dir = env::var_os("OUT_DIR")
        .map(PathBuf::from)
        .context("OUT_DIR not set while preparing eBPF build output")?;

    let endian = env::var("CARGO_CFG_TARGET_ENDIAN")
        .context("CARGO_CFG_TARGET_ENDIAN not set")?;
    let target = match endian.as_str() {
        "big" => "bpfeb",
        "little" => "bpfel",
        _ => bail!("unsupported endian value {endian} for eBPF build"),
    };

    let raw_arch = env::var("CARGO_CFG_TARGET_ARCH")
        .context("CARGO_CFG_TARGET_ARCH not set")?;
    let bpf_target_arch = target_arch_fixup(raw_arch.into()).into_owned();
    let target = format!("{target}-unknown-none");

    if !rustup_target_installed(&target)? {
        eprintln!(
            "Skipping eBPF build: rustup target `{target}` not installed (install with `rustup target add {target}` or set MANTISSA_SKIP_BPF=1)."
        );
        return Ok(());
    }

    for package in packages {
        println!("cargo:rerun-if-changed={}", package.root_dir);

        let toolchain_str = match &toolchain {
            aya_build::Toolchain::Nightly => "nightly",
            aya_build::Toolchain::Custom(spec) => spec,
        };

        let mut cmd = Command::new("rustup");
        cmd.args([
            "run",
            toolchain_str,
            "cargo",
            "build",
            "--package",
            package.name,
            "-Z",
            "build-std=core",
            "--bins",
            "--message-format=json",
            "--release",
            "--target",
            &target,
        ]);

        if package.no_default_features {
            cmd.arg("--no-default-features");
        }
        cmd.args(["--features", &package.features.join(",")]);

        const SEPARATOR: &str = "\x1f";
        let mut rustflags = OsString::new();
        for segment in [
            "--cfg=bpf_target_arch=\"",
            &bpf_target_arch,
            "\"",
            SEPARATOR,
            "-Cdebuginfo=2",
            SEPARATOR,
            "-Clink-arg=--btf",
        ] {
            rustflags.push(segment);
        }
        cmd.env("CARGO_ENCODED_RUSTFLAGS", rustflags);

        for key in ["RUSTC", "RUSTC_WORKSPACE_WRAPPER"] {
            cmd.env_remove(key);
        }

        let target_dir = out_dir.join(package.name);
        cmd.arg("--target-dir").arg(&target_dir);

        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to spawn {cmd:?}"))?;

        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("missing stderr from eBPF build command"))?;
        let stderr_handle = thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines() {
                if let Ok(line) = line {
                    eprintln!("{line}");
                }
            }
        });

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("missing stdout from eBPF build command"))?;
        let stdout = BufReader::new(stdout);
        let mut executables = Vec::new();
        for message in Message::parse_stream(stdout) {
            let message =
                message.context("failed to parse cargo JSON message for eBPF build output")?;
            match message {
                Message::CompilerArtifact(Artifact {
                    executable: Some(executable),
                    target: Target { name, .. },
                    ..
                }) => {
                    executables.push((name, executable.into_std_path_buf()));
                }
                Message::CompilerMessage(CompilerMessage { message, .. }) => {
                    for line in message.rendered.unwrap_or_default().lines() {
                        eprintln!("{line}");
                    }
                }
                Message::TextLine(line) => {
                    eprintln!("{line}");
                }
                _ => {}
            }
        }

        let status = child
            .wait()
            .with_context(|| format!("failed to wait for {cmd:?}"))?;
        if !status.success() {
            bail!("{cmd:?} failed with status {status}");
        }

        stderr_handle
            .join()
            .expect("eBPF stderr forwarding thread panicked");

        for (name, binary) in executables {
            let destination = out_dir.join(name);
            fs::copy(&binary, &destination).with_context(|| {
                format!("failed to copy {binary:?} to {destination:?} after eBPF build")
            })?;
        }
    }

    Ok(())
}

#[cfg(target_os = "linux")]
/// Build the eBPF programs when compiling on Linux, ensuring artifacts are
/// available for runtime networking features.
fn build_bpf() -> Result<()> {
    println!("cargo:rerun-if-env-changed=MANTISSA_SKIP_BPF");

    if env::var_os("MANTISSA_SKIP_BPF").is_some() {
        return Ok(());
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);

    let mut metadata_cmd = MetadataCommand::new();
    metadata_cmd.manifest_path(manifest_dir.join("Cargo.toml"));
    let metadata = metadata_cmd.exec()?;

    let packages: Vec<_> = metadata
        .packages
        .iter()
        .filter(|pkg| pkg.name == "network-ebpf")
        .map(|pkg| {
            let root_dir = pkg
                .manifest_path
                .parent()
                .ok_or_else(|| {
                    io::Error::other("network-ebpf manifest path does not have a parent directory")
                })?
                .as_str();

            Ok(aya_build::Package {
                name: pkg.name.as_str(),
                root_dir,
                no_default_features: false,
                features: &[],
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if packages.is_empty() {
        return Ok(());
    }

    if let Err(err) = ensure_bpf_linker() {
        eprintln!("Skipping eBPF build: {err}");
        return Ok(());
    }

    let toolchain = match env::var("MANTISSA_BPF_TOOLCHAIN") {
        Ok(spec) => {
            let leaked: &'static str = Box::leak(spec.into_boxed_str());
            aya_build::Toolchain::Custom(leaked)
        }
        Err(_) => aya_build::Toolchain::Nightly,
    };

    // Forward child cargo output to stderr to avoid clogging the host build with warnings.
    build_ebpf_without_warnings(packages.into_iter(), toolchain)?;

    let out_dir = PathBuf::from(
        env::var_os("OUT_DIR")
            .ok_or_else(|| anyhow!("OUT_DIR missing while copying compiled eBPF artifacts"))?,
    );
    let dest_dir = manifest_dir.join("target/bpf");
    fs::create_dir_all(&dest_dir)?;

    for program in [
        "vxlan_xdp",
        "bridge_xdp",
        "bridge_tc_ingress",
        "bridge_tc_egress",
    ] {
        let source = out_dir.join(program);
        if source.exists() {
            let destination = dest_dir.join(format!("{program}.bpf.o"));
            fs::copy(&source, &destination)?;
            println!("cargo:rerun-if-changed={}", destination.display());
        } else {
            println!(
                "cargo:warning=compiled eBPF artifact for {program} not found at {}",
                source.display()
            );
        }
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
/// No-op build hook on non-Linux hosts; eBPF programs are not compiled here.
fn build_bpf() -> Result<()> {
    println!("cargo:warning=skipping eBPF compilation on non-Linux host");
    Ok(())
}

/// Build script entry point that attempts eBPF compilation while degrading
/// gracefully to runtime fallbacks when unavailable.
fn main() {
    if let Err(err) = build_bpf() {
        println!(
            "cargo:warning=skipping eBPF build (will fall back to DNS-only VIP behavior): {err:#}"
        );
    }
}

#[cfg(target_os = "linux")]
/// Ensure `bpf-linker` is available before triggering compilation so we can
/// surface a clear error early in the build process.
fn ensure_bpf_linker() -> Result<()> {
    let status = Command::new("bpf-linker").arg("--version").status();
    match status {
        Ok(_) => Ok(()),
        Err(_) => bail!(
            "bpf-linker not found in PATH. Install it with `cargo install --git https://github.com/aya-rs/bpf-linker bpf-linker` \
             or set MANTISSA_SKIP_BPF=1 to bypass eBPF compilation."
        ),
    }
}

#[cfg(target_os = "linux")]
/// Check whether the BPF target triple is installed for the selected rustup toolchain.
fn rustup_target_installed(target: &str) -> Result<bool> {
    let output = Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .context("failed to query rustup for installed targets")?;

    if !output.status.success() {
        eprintln!(
            "Skipping eBPF build: rustup target list failed with status {}",
            output.status
        );
        return Ok(false);
    }

    let installed = String::from_utf8_lossy(&output.stdout)
        .lines()
        .any(|line| line.trim() == target);
    Ok(installed)
}
