#[cfg(target_os = "linux")]
use std::{env, error::Error, fs, io, path::PathBuf, process::Command};

#[cfg(not(target_os = "linux"))]
use std::error::Error;

#[cfg(target_os = "linux")]
fn build_bpf() -> Result<(), Box<dyn Error>> {
    if env::var_os("MANTISSA_SKIP_BPF").is_some() {
        println!("cargo:warning=skipping eBPF compilation (MANTISSA_SKIP_BPF set)");
        return Ok(());
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);

    let mut metadata_cmd = aya_build::cargo_metadata::MetadataCommand::new();
    metadata_cmd.manifest_path(manifest_dir.join("Cargo.toml"));
    let metadata = metadata_cmd.exec()?;

    let packages: Vec<_> = metadata
        .packages
        .into_iter()
        .filter(|pkg| pkg.name == "network-ebpf")
        .collect();

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
        io::Error::new(
            io::ErrorKind::Other,
            "OUT_DIR missing while copying compiled eBPF artifacts",
        )
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
fn build_bpf() -> Result<(), Box<dyn Error>> {
    println!("cargo:warning=skipping eBPF compilation on non-Linux host");
    Ok(())
}

fn main() {
    if let Err(err) = build_bpf() {
        eprintln!("Failed to compile eBPF programs: {err:#}");
        std::process::exit(1);
    }
}

#[cfg(target_os = "linux")]
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
