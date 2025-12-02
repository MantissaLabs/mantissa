#[cfg(target_os = "linux")]
use cargo_metadata::MetadataCommand;
#[cfg(target_os = "linux")]
use std::{env, error::Error, fs, io, path::PathBuf, process::Command};

#[cfg(not(target_os = "linux"))]
use std::error::Error;

#[cfg(target_os = "linux")]
/// Build the eBPF programs when compiling on Linux, ensuring artifacts are
/// available for runtime networking features.
fn build_bpf() -> Result<(), Box<dyn Error>> {
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
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;

    if packages.is_empty() {
        return Ok(());
    }

    ensure_bpf_linker()?;

    let toolchain = match env::var("MANTISSA_BPF_TOOLCHAIN") {
        Ok(spec) => {
            let leaked: &'static str = Box::leak(spec.into_boxed_str());
            aya_build::Toolchain::Custom(leaked)
        }
        Err(_) => aya_build::Toolchain::Nightly,
    };

    aya_build::build_ebpf(packages.into_iter(), toolchain)?;

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").ok_or_else(|| {
        io::Error::other("OUT_DIR missing while copying compiled eBPF artifacts")
    })?);
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
fn build_bpf() -> Result<(), Box<dyn Error>> {
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
fn ensure_bpf_linker() -> Result<(), Box<dyn Error>> {
    let status = Command::new("bpf-linker").arg("--version").status();
    match status {
        Ok(_) => Ok(()),
        Err(_) => Err(Box::new(io::Error::new(
            io::ErrorKind::NotFound,
            "bpf-linker not found in PATH. Install it with `cargo install --git https://github.com/aya-rs/bpf-linker bpf-linker` \
             or set MANTISSA_SKIP_BPF=1 to bypass eBPF compilation.",
        ))),
    }
}
