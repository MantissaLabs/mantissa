use net::paths::ensure_state_dir;
use std::{io, path::PathBuf};

/// Resolve the default redb state file, adapting to root vs unprivileged execution.
pub fn default_db_path() -> io::Result<PathBuf> {
    let dir = ensure_state_dir()?;
    Ok(dir.join("state.redb"))
}
