use std::{fmt, io, path::PathBuf};

#[derive(Debug)]
pub enum ClientConnectError {
    LocalSocketNotFound { tried: Vec<PathBuf> },
    LocalSocketPermissionDenied { path: PathBuf },
    LocalSocketRefused { path: PathBuf }, // daemon not accepting yet / stale socket
    LocalSocketNotASocket { path: PathBuf }, // file exists but is not a socket
    LocalSocketOther { path: PathBuf, source: io::Error },
    // TODO: Add other variants here.
}

impl fmt::Display for ClientConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ClientConnectError::*;
        match self {
            LocalSocketNotFound { tried } => {
                writeln!(f, "Mantissa daemon is not reachable locally.")?;
                writeln!(f, "I looked for a Unix socket at:")?;
                for p in tried {
                    writeln!(f, "  - {}", p.display())?;
                }
                writeln!(
                    f,
                    "\nStart the daemon or connect to a remote node with --anchor <ip:port>."
                )?;
                Ok(())
            }
            LocalSocketPermissionDenied { path } => {
                write!(f, "Permission denied opening {}. Try running the daemon as the same user, or check file permissions (expected 0600).", path.display())
            }
            LocalSocketRefused { path } => {
                write!(f, "The local socket exists at {} but the daemon refused the connection (is it starting up or stale?). Try restarting the daemon.", path.display())
            }
            LocalSocketNotASocket { path } => {
                write!(
                    f,
                    "Found {} but it isn’t a Unix socket. Remove it and restart the daemon.",
                    path.display()
                )
            }
            LocalSocketOther { path, source } => {
                write!(f, "Failed to connect to {}: {}", path.display(), source)
            }
        }
    }
}

impl std::error::Error for ClientConnectError {}
