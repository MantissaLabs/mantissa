use std::{fmt, io, path::PathBuf};

#[derive(Debug)]
pub enum ClientSocketError {
    NotFound { tried: Vec<PathBuf> },
    PermissionDenied { path: PathBuf },
    Refused { path: PathBuf },    // daemon not accepting yet / stale socket
    NotASocket { path: PathBuf }, // file exists but is not a socket
    Other { path: PathBuf, source: io::Error },
}

impl fmt::Display for ClientSocketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ClientSocketError::*;
        match self {
            NotFound { tried } => {
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
            PermissionDenied { path } => {
                write!(
                    f,
                    "Permission denied opening {}. The local Mantissa socket is an admin control socket; run the daemon as the same user, use sudo, or join the mantissa group for a root daemon.",
                    path.display()
                )
            }
            Refused { path } => {
                write!(
                    f,
                    "The local socket exists at {} but the daemon refused the connection (is it starting up or stale?). Try restarting the daemon.",
                    path.display()
                )
            }
            NotASocket { path } => {
                write!(
                    f,
                    "Found {} but it isn’t a Unix socket. Remove it and restart the daemon.",
                    path.display()
                )
            }
            Other { path, source } => {
                write!(f, "Failed to connect to {}: {}", path.display(), source)
            }
        }
    }
}

impl std::error::Error for ClientSocketError {}
