use anyhow::{Result, anyhow};
use std::borrow::Cow;
use std::path::{Path, PathBuf};

struct EmbeddedBpfProgram {
    name: &'static str,
    bytes: &'static [u8],
}

include!(concat!(env!("OUT_DIR"), "/embedded_bpf.rs"));

#[derive(Clone)]
pub(crate) enum BpfObject {
    Embedded {
        name: &'static str,
        bytes: &'static [u8],
    },
    File {
        path: PathBuf,
    },
}

impl BpfObject {
    /// Return a stable human-readable label for logging and error context.
    pub(crate) fn label(&self) -> Cow<'_, str> {
        match self {
            Self::Embedded { name, .. } => Cow::Borrowed(name),
            Self::File { path } => path.to_string_lossy(),
        }
    }

    /// Return the backing path when this object was resolved from a filesystem override.
    #[cfg(test)]
    pub(crate) fn file_path(&self) -> Option<&Path> {
        match self {
            Self::Embedded { .. } => None,
            Self::File { path } => Some(path.as_path()),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BpfObjectResolver {
    artifact_dir: Option<PathBuf>,
}

impl BpfObjectResolver {
    /// Build a resolver that prefers one explicit artifact directory before embedded objects.
    pub(crate) fn new(artifact_dir: Option<PathBuf>) -> Self {
        Self { artifact_dir }
    }

    /// Resolve one BPF program name to either an explicit file override or embedded bytecode.
    pub(crate) fn resolve(&self, name: &str) -> Result<BpfObject> {
        if is_path_like(name) {
            return resolve_path_like(name);
        }

        if let Some(root) = &self.artifact_dir {
            return resolve_from_dir(root, name);
        }

        embedded_object(name).ok_or_else(|| {
            anyhow!(
                "BPF program '{name}' is not embedded in this binary; rebuild with eBPF support or set MANTISSA_BPF_DIR to a directory containing that object"
            )
        })
    }
}

/// Resolve one explicit path-like program value without consulting embedded objects.
fn resolve_path_like(name: &str) -> Result<BpfObject> {
    for path in path_candidates(PathBuf::from(name)) {
        if path.exists() {
            return Ok(BpfObject::File { path });
        }
    }
    Err(anyhow!("BPF artifact path '{name}' does not exist"))
}

/// Resolve one program from an explicit artifact directory.
fn resolve_from_dir(root: &Path, name: &str) -> Result<BpfObject> {
    for path in name_candidates(root, name) {
        if path.exists() {
            return Ok(BpfObject::File { path });
        }
    }
    Err(anyhow!(
        "configured BPF artifact directory {} does not contain '{name}'",
        root.display()
    ))
}

/// Return the embedded bytecode for one built-in BPF program name.
fn embedded_object(name: &str) -> Option<BpfObject> {
    PROGRAMS
        .iter()
        .find(|program| program.name == name)
        .map(|program| BpfObject::Embedded {
            name: program.name,
            bytes: program.bytes,
        })
}

/// Return whether a program value is an absolute or relative filesystem path.
fn is_path_like(name: &str) -> bool {
    Path::new(name).is_absolute() || name.contains(std::path::MAIN_SEPARATOR)
}

/// Enumerate path candidates for a direct program path.
fn path_candidates(path: PathBuf) -> Vec<PathBuf> {
    let mut candidates = vec![path.clone()];
    if path.extension().is_none() {
        candidates.push(path.with_extension("bpf.o"));
        candidates.push(path.with_extension("o"));
    }
    candidates
}

/// Enumerate file candidates for a program name inside an explicit artifact directory.
fn name_candidates(root: &Path, name: &str) -> Vec<PathBuf> {
    vec![
        root.join(name),
        root.join(format!("{name}.bpf.o")),
        root.join(format!("{name}.o")),
    ]
}
