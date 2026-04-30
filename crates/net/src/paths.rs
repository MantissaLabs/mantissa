use std::env;
use std::ffi::CString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const SYSTEM_STATE_DIR: &str = "/var/lib/mantissa";
const USER_STATE_SUBDIR: &str = ".mantissa";
const MANTISSA_GROUP: &str = "mantissa";
pub const STATE_DIR_ENV: &str = "MANTISSA_STATE_DIR";

static STATE_DIR_OVERRIDE: OnceLock<PathBuf> = OnceLock::new();

/// Returns true when the effective uid has root privileges.
pub fn running_as_root() -> bool {
    #[cfg(unix)]
    {
        unsafe { libc::geteuid() == 0 }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

/// Set a process-local state directory override for daemon startup.
///
/// This is used by `mantissa init --state-dir` so every subsystem that resolves
/// local state during startup observes the same directory without mutating the
/// process environment.
pub fn set_state_dir_override(path: PathBuf) -> io::Result<()> {
    match STATE_DIR_OVERRIDE.set(path.clone()) {
        Ok(()) => Ok(()),
        Err(_) if STATE_DIR_OVERRIDE.get() == Some(&path) => Ok(()),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "state directory override was already set to a different path",
        )),
    }
}

/// Return the process-local state directory override, if one has been configured.
pub fn state_dir_override() -> Option<PathBuf> {
    STATE_DIR_OVERRIDE.get().cloned()
}

/// Resolve the persistent state directory path without creating it.
///
/// Root uses `/var/lib/mantissa`; unprivileged users fall back to `~/.mantissa`.
pub fn resolve_state_dir_path() -> io::Result<PathBuf> {
    if let Some(path) = state_dir_override() {
        return Ok(path);
    }

    if let Some(override_dir) = env::var_os(STATE_DIR_ENV).filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(override_dir));
    }

    if running_as_root() {
        return Ok(PathBuf::from(SYSTEM_STATE_DIR));
    }

    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "HOME not set"))?;
    Ok(home.join(USER_STATE_SUBDIR))
}

/// Ensure an explicit persistent state directory exists with useful permissions.
pub fn ensure_state_dir_at(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let path = path.as_ref().to_path_buf();
    fs::create_dir_all(&path)?;

    #[cfg(unix)]
    {
        let desired_mode = if running_as_root() { 0o750 } else { 0o700 };
        let _ = fs::set_permissions(&path, fs::Permissions::from_mode(desired_mode));

        if running_as_root()
            && let Some(gid) = lookup_group_gid(MANTISSA_GROUP)
        {
            let _ = chown_group(&path, gid);
        }
    }

    Ok(path)
}

/// Resolve the persistent state directory and ensure it exists with useful permissions.
pub fn ensure_state_dir() -> io::Result<PathBuf> {
    ensure_state_dir_at(resolve_state_dir_path()?)
}

/// Helper to change a filesystem object's group to `mantissa`.
#[cfg(unix)]
pub fn ensure_mantissa_group(path: &Path) {
    if running_as_root()
        && let Some(gid) = lookup_group_gid(MANTISSA_GROUP)
    {
        let _ = chown_group(path, gid);
    }
}

#[cfg(not(unix))]
/// No-op helper for non-Unix platforms.
pub fn ensure_mantissa_group(_path: &Path) {}

/// Resolve the `mantissa` group gid when present on the system.
#[cfg(unix)]
fn lookup_group_gid(name: &str) -> Option<libc::gid_t> {
    let cname = CString::new(name).ok()?;
    let mut buf_len = 1024usize;

    loop {
        let mut grp: libc::group = unsafe { std::mem::zeroed() };
        let mut result = std::ptr::null_mut();
        let mut buf = vec![0 as libc::c_char; buf_len];

        let ret = unsafe {
            libc::getgrnam_r(
                cname.as_ptr(),
                &mut grp,
                buf.as_mut_ptr(),
                buf_len,
                &mut result,
            )
        };

        if ret == 0 {
            if result.is_null() {
                return None;
            }
            return Some(unsafe { (*result).gr_gid });
        } else if ret == libc::ERANGE {
            buf_len *= 2;
            if buf_len > 1 << 20 {
                return None;
            }
        } else {
            return None;
        }
    }
}

/// Change the group of a path to the provided gid, leaving ownership intact.
#[cfg(unix)]
fn chown_group(path: &Path, gid: libc::gid_t) -> io::Result<()> {
    let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid path for chown: {}", path.display()),
        )
    })?;

    let res = unsafe { libc::chown(c_path.as_ptr(), libc::uid_t::MAX, gid) };
    if res == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
