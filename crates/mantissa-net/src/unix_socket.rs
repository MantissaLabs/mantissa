use crate::paths::{
    STATE_DIR_ENV, SYSTEM_STATE_DIR, ensure_mantissa_group, running_as_root, state_dir_override,
};
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use mantissa_protocol::server::cluster_session;
use std::os::unix::fs::PermissionsExt;
use std::{
    env, fs, io,
    path::{Path, PathBuf},
};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::info;

/// List potential Unix socket locations ordered by preference.
pub fn candidate_unix_socket_paths() -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Some(path) = explicit_state_socket_path() {
        push_socket_candidate(&mut v, path);
    }
    push_socket_candidate(&mut v, PathBuf::from("/var/run/mantissa.sock")); // default
    push_socket_candidate(&mut v, PathBuf::from("/run/mantissa.sock"));
    push_socket_candidate(&mut v, system_state_socket_path());
    if let Ok(dir) = env::var("XDG_RUNTIME_DIR") {
        push_socket_candidate(
            &mut v,
            Path::new(&dir).join("mantissa").join("mantissa.sock"),
        );
    }
    if explicit_state_socket_path().is_none()
        && let Some(path) = user_state_socket_path()
    {
        push_socket_candidate(&mut v, path);
    }
    if !running_as_root() {
        push_socket_candidate(&mut v, private_tmp_socket_path());
    }
    v
}

/// Adds one candidate path while preserving discovery order and uniqueness.
fn push_socket_candidate(paths: &mut Vec<PathBuf>, path: PathBuf) {
    if !paths.iter().any(|existing| existing == &path) {
        paths.push(path);
    }
}

/// Return the socket path inside the root daemon's persistent state directory.
pub fn system_state_socket_path() -> PathBuf {
    Path::new(SYSTEM_STATE_DIR).join("mantissa.sock")
}

/// Return the socket path for an explicitly configured state directory.
fn explicit_state_socket_path() -> Option<PathBuf> {
    if let Some(dir) = state_dir_override() {
        return Some(dir.join("mantissa.sock"));
    }

    env::var_os(STATE_DIR_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|dir| dir.join("mantissa.sock"))
}

/// Return the state-directory socket path when a state directory can be resolved.
fn user_state_socket_path() -> Option<PathBuf> {
    if let Some(dir) = state_dir_override() {
        return Some(dir.join("mantissa.sock"));
    }

    if let Some(dir) = env::var_os(STATE_DIR_ENV).filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(dir).join("mantissa.sock"));
    }

    if running_as_root() {
        return None;
    }

    env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|home| home.join(".mantissa").join("mantissa.sock"))
}

/// Return the private temp-directory fallback used when no runtime/state path exists.
fn private_tmp_socket_path() -> PathBuf {
    env::temp_dir()
        .join(format!("mantissa-{}", effective_uid_string()))
        .join("mantissa.sock")
}

/// Return the effective uid string for per-user runtime socket paths.
fn effective_uid_string() -> String {
    #[cfg(unix)]
    {
        unsafe { libc::geteuid() }.to_string()
    }
    #[cfg(not(unix))]
    {
        "0".to_string()
    }
}

/// Return true for shared system directories that Mantissa must not chmod.
fn is_shared_socket_parent(parent: &Path) -> bool {
    parent == Path::new("/var/run") || parent == Path::new("/run") || parent == env::temp_dir()
}

/// Remove lingering socket files and pre-create parent directories with sane permissions.
fn prepare_socket_file(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent()
        && !is_shared_socket_parent(parent)
    {
        fs::create_dir_all(parent)?;
        let mode = if running_as_root() { 0o770 } else { 0o700 };
        fs::set_permissions(parent, fs::Permissions::from_mode(mode))?;
        if running_as_root() {
            ensure_mantissa_group(parent);
        }
    }
    if path.exists() {
        let _ = fs::remove_file(path);
    }
    Ok(())
}

/// This method starts a Unix socket server using the provided server handle.
/// One important thing to note is while the TCP server serves a Server handle,
/// here we serve a ClusterSession handle to avoid the gating on token/session
/// tickets. Access to this socket is therefore cluster-admin equivalent.
pub async fn start_unix_socket_server_auto(
    server_handle: cluster_session::Client,
) -> io::Result<PathBuf> {
    let mut last_err: Option<io::Error> = None;

    for path in candidate_unix_socket_paths() {
        if let Err(e) = prepare_socket_file(&path) {
            last_err = Some(e);
            continue;
        }
        match UnixListener::bind(&path) {
            Ok(listener) => {
                if running_as_root() {
                    let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o660));
                    ensure_mantissa_group(&path);
                } else {
                    let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o600));
                }
                info!(
                    target: "server",
                    "Local admin UnixSocket listening at {}; access grants cluster control",
                    path.display()
                );
                tokio::task::spawn_local(accept_loop(listener, server_handle.clone()));
                return Ok(path);
            }
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        }
    }

    Err(last_err.unwrap_or_else(|| io::Error::other("no usable UnixSocket path")))
}

async fn accept_loop(listener: UnixListener, server_handle: cluster_session::Client) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::task::spawn_local(handle_unix_conn(stream, server_handle.clone()));
            }
            Err(e) => eprintln!("UnixSocket accept error: {e}"),
        }
    }
}

async fn handle_unix_conn(stream: UnixStream, server_handle: cluster_session::Client) {
    let (reader, writer) = stream.compat().split();

    let network = twoparty::VatNetwork::new(
        futures::io::BufReader::new(reader),
        futures::io::BufWriter::new(writer),
        rpc_twoparty_capnp::Side::Server,
        Default::default(),
    );

    let rpc_system = RpcSystem::new(Box::new(network), Some(server_handle.client));

    if let Err(e) = rpc_system.await {
        eprintln!("UnixSocket RPC error: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_paths_do_not_use_public_tmp_socket() {
        let public_tmp_socket = PathBuf::from("/tmp/mantissa.sock");

        assert!(
            !candidate_unix_socket_paths()
                .iter()
                .any(|path| path == &public_tmp_socket),
            "local control socket must not fall back to a shared /tmp path"
        );
    }

    #[test]
    fn candidate_paths_include_system_state_socket() {
        assert!(
            candidate_unix_socket_paths()
                .iter()
                .any(|path| path == &system_state_socket_path()),
            "clients should discover root daemons listening under the system state directory"
        );
    }

    #[test]
    fn private_tmp_socket_path_is_uid_scoped() {
        let path = private_tmp_socket_path();

        assert_eq!(
            path.file_name().and_then(|name| name.to_str()),
            Some("mantissa.sock")
        );
        assert!(
            path.parent()
                .and_then(|parent| parent.file_name())
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("mantissa-")),
            "private temp fallback should live below a per-user directory"
        );
    }

    #[cfg(unix)]
    #[test]
    fn prepare_socket_file_sets_private_parent_permissions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let parent = dir.path().join("mantissa-runtime");
        let socket = parent.join("mantissa.sock");

        prepare_socket_file(&socket).expect("prepare socket path");

        let mode = fs::metadata(&parent)
            .expect("parent metadata")
            .permissions()
            .mode()
            & 0o777;
        let expected = if running_as_root() { 0o770 } else { 0o700 };
        assert_eq!(mode, expected);
    }

    #[test]
    fn shared_socket_parents_are_recognized() {
        assert!(is_shared_socket_parent(Path::new("/var/run")));
        assert!(is_shared_socket_parent(Path::new("/run")));
        assert!(is_shared_socket_parent(&env::temp_dir()));
    }
}
