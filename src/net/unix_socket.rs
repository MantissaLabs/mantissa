use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, TryFutureExt};
use std::os::unix::fs::PermissionsExt;
use std::{
    env, fs, io,
    path::{Path, PathBuf},
};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::compat::TokioAsyncReadCompatExt;

pub fn candidate_unix_socket_paths() -> Vec<PathBuf> {
    let mut v = Vec::new();
    v.push(PathBuf::from("/var/run/mantissa.sock")); // default
    v.push(PathBuf::from("/run/mantissa.sock"));
    if let Ok(dir) = env::var("XDG_RUNTIME_DIR") {
        v.push(Path::new(&dir).join("mantissa.sock"));
    }
    v.push(PathBuf::from("/tmp/mantissa.sock")); // last resort
    v
}

fn prepare_socket_file(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        if parent != Path::new("/var/run") && parent != Path::new("/run") {
            let _ = fs::set_permissions(parent, fs::Permissions::from_mode(0o700));
        }
    }
    if path.exists() {
        let _ = fs::remove_file(path);
    }
    Ok(())
}

pub async fn start_unix_socket_server_auto(
    server_handle: crate::server_capnp::server::Client,
) -> io::Result<PathBuf> {
    let mut last_err: Option<io::Error> = None;

    for path in candidate_unix_socket_paths() {
        if let Err(e) = prepare_socket_file(&path) {
            last_err = Some(e);
            continue;
        }
        match UnixListener::bind(&path) {
            Ok(listener) => {
                let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o600));
                eprintln!("Local UnixSocket listening at {}", path.display());
                tokio::task::spawn_local(accept_loop(listener, server_handle.clone()));
                return Ok(path);
            }
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        }
    }

    Err(last_err
        .unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "no usable UnixSocket path")))
}

async fn accept_loop(listener: UnixListener, server_handle: crate::server_capnp::server::Client) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::task::spawn_local(handle_unix_conn(stream, server_handle.clone()));
            }
            Err(e) => eprintln!("UnixSocket accept error: {e}"),
        }
    }
}

async fn handle_unix_conn(stream: UnixStream, server_handle: crate::server_capnp::server::Client) {
    let (reader, writer) = stream.compat().split();

    let network = twoparty::VatNetwork::new(
        futures::io::BufReader::new(reader),
        futures::io::BufWriter::new(writer),
        rpc_twoparty_capnp::Side::Server,
        Default::default(),
    );

    let rpc_system = RpcSystem::new(Box::new(network), Some(server_handle.client));
    rpc_system
        .map_err(|e| eprintln!("UnixSocket RPC error: {e}"))
        .await;
}
