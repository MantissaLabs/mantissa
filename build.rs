#[cfg(target_os = "linux")]
use anyhow::{Context, Result, anyhow, bail};
#[cfg(target_os = "linux")]
use cargo_metadata::{Artifact, CompilerMessage, Message, MetadataCommand, Target};
#[cfg(target_os = "linux")]
use std::{
    borrow::Cow,
    env,
    ffi::OsString,
    fs,
    io::{self, BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
};

#[cfg(not(target_os = "linux"))]
use anyhow::Result;

#[cfg(target_os = "linux")]
const BPF_PROGRAMS: &[&str] = &[
    "vxlan_xdp",
    "bridge_xdp",
    "bridge_tc_ingress_v4",
    "bridge_tc_egress_v4",
    "bridge_tc_ingress_v6",
    "bridge_tc_egress_v6",
    "nodeport_tc_ingress",
    "nodeport_tc_egress",
];

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

    let endian = env::var("CARGO_CFG_TARGET_ENDIAN").context("CARGO_CFG_TARGET_ENDIAN not set")?;
    let target = match endian.as_str() {
        "big" => "bpfeb",
        "little" => "bpfel",
        _ => bail!("unsupported endian value {endian} for eBPF build"),
    };

    let raw_arch = env::var("CARGO_CFG_TARGET_ARCH").context("CARGO_CFG_TARGET_ARCH not set")?;
    let bpf_target_arch = target_arch_fixup(raw_arch.into()).into_owned();
    let target = format!("{target}-unknown-none");

    let toolchain_str = match &toolchain {
        aya_build::Toolchain::Nightly => "nightly",
        aya_build::Toolchain::Custom(spec) => spec,
    };

    if !rustc_supports_target(toolchain_str, &target)? {
        eprintln!(
            "Skipping eBPF build: rustc toolchain `{toolchain_str}` does not list `{target}` (set MANTISSA_SKIP_BPF=1 to bypass eBPF compilation)."
        );
        return Ok(());
    }

    for package in packages {
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
            for line in reader.lines().map_while(std::result::Result::ok) {
                eprintln!("{line}");
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
/// Register the eBPF source inputs so Cargo only reruns this build script when they change.
fn track_ebpf_inputs(root_dir: &Path) -> Result<()> {
    let paths = [
        root_dir.join("Cargo.toml"),
        root_dir.join("src"),
        root_dir.join(".cargo"),
    ];

    for path in paths {
        track_ebpf_path(&path)?;
    }

    Ok(())
}

#[cfg(target_os = "linux")]
/// Recursively emit rerun directives for files under an input path, skipping target artifacts.
fn track_ebpf_path(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let metadata = fs::metadata(path)?;
    if metadata.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();
            if entry_path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "target")
            {
                continue;
            }
            track_ebpf_path(&entry_path)?;
        }
    } else {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    Ok(())
}

#[cfg(target_os = "linux")]
/// Write the Rust module that embeds any BPF programs compiled by this build.
fn write_embedded_bpf_module(programs: &[(&str, PathBuf)]) -> Result<()> {
    let out_dir = env::var_os("OUT_DIR")
        .map(PathBuf::from)
        .context("OUT_DIR not set while preparing embedded eBPF module")?;
    let destination = out_dir.join("embedded_bpf.rs");
    let mut source = String::from("const PROGRAMS: &[EmbeddedBpfProgram] = &[\n");

    for (name, path) in programs {
        source.push_str("    EmbeddedBpfProgram {\n");
        source.push_str(&format!("        name: {},\n", rust_string_literal(name)));
        source.push_str(&format!(
            "        bytes: aya::include_bytes_aligned!({}),\n",
            rust_string_literal(path.to_string_lossy().as_ref())
        ));
        source.push_str("    },\n");
    }

    source.push_str("];\n");
    fs::write(destination, source).context("write generated embedded eBPF module")
}

#[cfg(target_os = "linux")]
/// Escape one value as a Rust string literal for generated source.
fn rust_string_literal(value: &str) -> String {
    let mut literal = String::with_capacity(value.len() + 2);
    literal.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => literal.push_str("\\\\"),
            '"' => literal.push_str("\\\""),
            '\n' => literal.push_str("\\n"),
            '\r' => literal.push_str("\\r"),
            '\t' => literal.push_str("\\t"),
            _ => literal.push(ch),
        }
    }
    literal.push('"');
    literal
}

#[cfg(target_os = "linux")]
/// Build the eBPF programs when compiling on Linux, ensuring artifacts are
/// available for runtime networking features.
fn build_bpf() -> Result<()> {
    println!("cargo:rerun-if-env-changed=MANTISSA_SKIP_BPF");
    println!("cargo:rerun-if-env-changed=MANTISSA_BPF_TOOLCHAIN");

    write_embedded_bpf_module(&[])?;

    if env::var_os("MANTISSA_SKIP_BPF").is_some() {
        return Ok(());
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);

    let mut metadata_cmd = MetadataCommand::new();
    metadata_cmd.manifest_path(manifest_dir.join("Cargo.toml"));
    // The eBPF build only needs to locate workspace members. Using `--no-deps` keeps cargo from
    // resolving and downloading the full dependency graph (which may be blocked in restricted
    // environments even when the host build succeeds from cache).
    metadata_cmd.no_deps();
    // Prefer an offline metadata query so the build remains deterministic when internet access is
    // unavailable (e.g. CI or air-gapped nodes). If the workspace has never been built before and
    // the cargo index is missing, this will fail and we'll fall back to an online query.
    metadata_cmd.other_options(vec!["--offline".into()]);
    let metadata = match metadata_cmd.exec() {
        Ok(metadata) => metadata,
        Err(err) => {
            let mut retry = MetadataCommand::new();
            retry.manifest_path(manifest_dir.join("Cargo.toml"));
            retry.no_deps();
            retry
                .exec()
                .with_context(|| format!("cargo metadata failed in offline mode: {err}"))?
        }
    };

    let packages: Vec<_> = metadata
        .packages
        .iter()
        .filter(|pkg| pkg.name == "mantissa-ebpf")
        .map(|pkg| {
            let root_dir = pkg
                .manifest_path
                .parent()
                .ok_or_else(|| {
                    io::Error::other("mantissa-ebpf manifest path does not have a parent directory")
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

    for package in &packages {
        track_ebpf_inputs(&PathBuf::from(package.root_dir))?;
    }

    if let Err(err) = ensure_bpf_linker() {
        eprintln!("building without embedded eBPF programs: {err}");
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
    build_ebpf_without_warnings(packages, toolchain)?;

    let out_dir = PathBuf::from(
        env::var_os("OUT_DIR")
            .ok_or_else(|| anyhow!("OUT_DIR missing while embedding compiled eBPF artifacts"))?,
    );
    let mut embedded = Vec::new();

    for program in BPF_PROGRAMS {
        let source = out_dir.join(program);
        if source.exists() {
            embedded.push((*program, source));
        } else {
            println!(
                "cargo:warning=compiled eBPF artifact for {program} was not embedded because it was not found at {}",
                source.display()
            );
        }
    }

    write_embedded_bpf_module(&embedded)?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
/// No-op build hook on non-Linux hosts; eBPF programs are not compiled here.
fn build_bpf() -> Result<()> {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux")
        && let Some(out_dir) = std::env::var_os("OUT_DIR")
    {
        std::fs::write(
            std::path::PathBuf::from(out_dir).join("embedded_bpf.rs"),
            "const PROGRAMS: &[EmbeddedBpfProgram] = &[];\n",
        )?;
    }
    println!("cargo:warning=skipping eBPF compilation on non-Linux host");
    Ok(())
}

/// Build script entry point that attempts eBPF compilation while degrading
/// gracefully to runtime fallbacks when unavailable.
fn main() {
    if let Err(err) = build_bpf() {
        println!(
            "cargo:warning=building without embedded eBPF programs; networking will use non-BPF fallbacks where available: {err:#}"
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
/// Check whether the selected rust toolchain advertises the BPF target triple.
fn rustc_supports_target(toolchain: &str, target: &str) -> Result<bool> {
    let output = Command::new("rustup")
        .args(["run", toolchain, "rustc", "--print=target-list"])
        .output()
        .context("failed to query rustc for supported targets")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("rustc target list failed: {}", stderr.trim());
    }

    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .any(|line| line.trim() == target))
}
