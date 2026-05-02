use mantissa_net::paths::ensure_state_dir;
use redb::Database;
use std::{
    fs::{self, OpenOptions},
    io,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

#[cfg(unix)]
const STATE_DB_MODE: u32 = 0o600;

/// Resolve the default redb state file, adapting to root vs unprivileged execution.
pub fn default_db_path() -> io::Result<PathBuf> {
    let dir = ensure_state_dir()?;
    Ok(dir.join("state.redb"))
}

/// Open the default Redb state database after applying state-file permissions.
pub fn open_default_database() -> io::Result<Database> {
    open_state_database(default_db_path()?)
}

/// Open a Redb state database after ensuring the backing file is owner-only.
pub fn open_state_database(path: impl AsRef<Path>) -> io::Result<Database> {
    let path = path.as_ref();
    ensure_state_db_file(path)?;
    let db = Database::create(path).map_err(|err| io::Error::other(err.to_string()))?;
    restrict_state_db_file(path)?;
    Ok(db)
}

/// Create the state database file with restrictive permissions before Redb opens it.
fn ensure_state_db_file(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    #[cfg(unix)]
    {
        if path.exists() {
            restrict_state_db_file(path)?;
        } else {
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(STATE_DB_MODE)
                .open(path)
            {
                Ok(_file) => {}
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                    restrict_state_db_file(path)?;
                }
                Err(err) => Err(err)?,
            }
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        let _file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(())
    }
}

/// Force the state database file back to owner-only permissions on Unix hosts.
fn restrict_state_db_file(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::set_permissions(path, fs::Permissions::from_mode(STATE_DB_MODE))?;
    }

    #[cfg(not(unix))]
    {
        let _ = path;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::open_state_database;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[cfg(unix)]
    #[test]
    fn open_state_database_enforces_owner_only_file_mode() {
        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("state.redb");
        let db = open_state_database(&db_path).expect("open state db");
        drop(db);

        let mode = db_path
            .metadata()
            .expect("state db metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}
