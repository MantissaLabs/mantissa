use crate::paths::{ensure_mantissa_group, ensure_state_dir, running_as_root};
use getrandom::getrandom;
use std::path::{Path, PathBuf};
use std::{fs, io};
use uuid::Uuid;
use x25519_dalek::{PublicKey, StaticSecret};

/// Default UDP listen port for the Mantissa-managed WireGuard underlay.
pub const DEFAULT_WIREGUARD_LISTEN_PORT: u16 = 51820;

/// Stable WireGuard key material stored on disk and reused across restarts.
///
/// Mantissa uses this keypair to build an encrypted underlay between nodes. The public key is
/// gossiped through the Peers CRDT so every node can configure kernel WireGuard peers without
/// any external `wg` tooling.
pub struct WireGuardKeys {
    pub private: StaticSecret,
    pub public: PublicKey,
}

impl WireGuardKeys {
    /// Construct a WireGuard keypair from raw private key bytes and derive the public key.
    pub fn from_private_bytes(secret: [u8; 32]) -> Self {
        let private = StaticSecret::from(secret);
        let public = PublicKey::from(&private);
        Self { private, public }
    }

    /// Return the private key bytes for persistence.
    pub fn to_private_bytes(&self) -> [u8; 32] {
        self.private.to_bytes()
    }

    /// Return the public key bytes for advertisement to peers.
    pub fn public_bytes(&self) -> [u8; 32] {
        self.public.to_bytes()
    }
}

/// Prefer `/var/lib/mantissa` when privileged, otherwise fallback to `~/.mantissa`.
pub fn resolve_wireguard_key_path() -> io::Result<PathBuf> {
    let dir = ensure_state_dir()?;
    Ok(dir.join("wireguard.key"))
}

/// Prefer `/var/lib/mantissa` when privileged, otherwise fallback to `~/.mantissa`.
pub fn resolve_wireguard_port_path() -> io::Result<PathBuf> {
    let dir = ensure_state_dir()?;
    Ok(dir.join("wireguard.port"))
}

/// Prefer `/var/lib/mantissa` when privileged, otherwise fallback to `~/.mantissa`.
pub fn resolve_wireguard_underlay_preference_path() -> io::Result<PathBuf> {
    let dir = ensure_state_dir()?;
    Ok(dir.join("wireguard.underlay"))
}

/// Load the persisted decision for using WireGuard as a VXLAN underlay.
///
/// Mantissa keeps this boolean on disk so nodes that already switched the VXLAN underlay to
/// WireGuard remain stable across restarts, even while the cluster membership is changing.
pub fn load_wireguard_underlay_preference() -> io::Result<bool> {
    let path = resolve_wireguard_underlay_preference_path()?;
    if !path.exists() {
        return Ok(false);
    }

    let contents = fs::read_to_string(&path)?;
    let value = match contents.trim() {
        "" => true,
        "0" | "false" | "no" => false,
        _ => true,
    };
    Ok(value)
}

/// Persist the decision for using WireGuard as a VXLAN underlay.
///
/// When `enabled` is true we write a small marker file. When false we remove the marker file.
pub fn persist_wireguard_underlay_preference(enabled: bool) -> io::Result<()> {
    let path = resolve_wireguard_underlay_preference_path()?;
    if !enabled {
        match fs::remove_file(&path) {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        }
    }

    fs::write(&path, "1\n")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if running_as_root() { 0o640 } else { 0o600 };
        let _ = fs::set_permissions(&path, fs::Permissions::from_mode(mode));
        if running_as_root() {
            ensure_mantissa_group(&path);
        }
    }
    Ok(())
}

/// Resolve a stable UDP listen port for the Mantissa-managed WireGuard underlay.
///
/// Precedence:
/// 1) Explicit override port supplied by the caller.
/// 2) `wireguard.port` persisted in the state dir (survives restarts).
/// 3) Optional preferred port supplied by the caller (typically the node advertise port).
/// 4) `DEFAULT_WIREGUARD_LISTEN_PORT`.
pub fn load_or_choose_wireguard_listen_port() -> io::Result<u16> {
    load_or_choose_wireguard_listen_port_with_preferred_and_override(None, None)
}

/// Resolve a stable UDP listen port for the Mantissa-managed WireGuard underlay, optionally
/// preferring a specific port.
///
/// This is used to keep the deployment "zero-config" by selecting a port that is already known
/// to be reachable between nodes (for example the existing control-plane advertise port).
///
/// The returned port is persisted so restarts keep a stable endpoint unless explicitly overridden.
pub fn load_or_choose_wireguard_listen_port_with_preferred(
    preferred_port: Option<u16>,
) -> io::Result<u16> {
    load_or_choose_wireguard_listen_port_with_preferred_and_override(preferred_port, None)
}

/// Resolve a stable UDP listen port for the Mantissa-managed WireGuard underlay, optionally
/// preferring a specific port and honoring an explicit override.
///
/// This is used to keep the deployment "zero-config" by selecting a port that is already known
/// to be reachable between nodes (for example the existing control-plane advertise port).
///
/// The returned port is persisted so restarts keep a stable endpoint unless explicitly overridden.
pub fn load_or_choose_wireguard_listen_port_with_preferred_and_override(
    preferred_port: Option<u16>,
    override_port: Option<u16>,
) -> io::Result<u16> {
    if let Some(port) = override_port {
        if port == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "wireguard port must be non-zero",
            ));
        }
        return Ok(port);
    }

    let path = resolve_wireguard_port_path()?;
    if path.exists() {
        let contents = fs::read_to_string(&path)?;
        let port = contents.trim().parse::<u16>().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid wireguard port file")
        })?;
        if port == 0 {
            let recovered_port = preferred_port.unwrap_or(DEFAULT_WIREGUARD_LISTEN_PORT);
            persist_wireguard_listen_port(&path, recovered_port)?;
            return Ok(recovered_port);
        }

        // Migrate older deployments that defaulted to 51820 when we can safely infer a more
        // reachable port (for example the Mantissa RPC advertise port). This avoids clusters
        // getting stuck on a blocked/closed UDP port after upgrading.
        if port == DEFAULT_WIREGUARD_LISTEN_PORT
            && let Some(preferred_port) = preferred_port
            && preferred_port != port
        {
            persist_wireguard_listen_port(&path, preferred_port)?;
            return Ok(preferred_port);
        }

        return Ok(port);
    }

    let port = preferred_port.unwrap_or(DEFAULT_WIREGUARD_LISTEN_PORT);
    if port == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "wireguard port must be non-zero",
        ));
    }
    persist_wireguard_listen_port(&path, port)?;
    Ok(port)
}

/// Persist the WireGuard listen port with the same permissions used for freshly generated state.
fn persist_wireguard_listen_port(path: &Path, port: u16) -> io::Result<()> {
    fs::write(path, format!("{port}\n"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if running_as_root() { 0o640 } else { 0o600 };
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
        if running_as_root() {
            ensure_mantissa_group(path);
        }
    }
    Ok(())
}

/// Load a 32-byte WireGuard private key from `path`, or generate and persist a new one.
///
/// The key file is intentionally raw 32-byte material so we do not depend on external tools
/// or config formats at runtime.
pub fn load_or_generate_wireguard_keys(path: impl AsRef<Path>) -> io::Result<WireGuardKeys> {
    let path = path.as_ref();
    let private_bytes = if path.exists() {
        let bytes = fs::read(path)?;
        let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "wireguard private key must be 32 bytes",
            )
        })?;
        #[cfg(unix)]
        {
            if running_as_root() {
                use std::os::unix::fs::PermissionsExt;
                let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o640));
                ensure_mantissa_group(path);
            }
        }
        arr
    } else {
        let mut sk = [0u8; 32];
        getrandom(&mut sk)?;
        fs::write(path, sk)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = if running_as_root() { 0o640 } else { 0o600 };
            let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
            if running_as_root() {
                ensure_mantissa_group(path);
            }
        }
        sk
    };

    Ok(WireGuardKeys::from_private_bytes(private_bytes))
}

/// Fixed ULA /64 used for the WireGuard tunnel addresses between Mantissa nodes.
///
/// We intentionally keep the prefix constant so every node can compute the peer tunnel IP
/// purely from the peer UUID. This avoids distributing per-node tunnel addresses in the CRDT
/// and eliminates allocation/collision logic.
///
/// Prefix: `fd42:6d61:6e74:6973::/64`  (the middle words are ASCII-ish for "mantis")
pub const WIREGUARD_TUNNEL_PREFIX: [u16; 4] = [0xfd42, 0x6d61, 0x6e74, 0x6973];

/// Return the `/64` tunnel prefix used by Mantissa for WireGuard underlay addresses.
pub fn wireguard_tunnel_prefix() -> (std::net::Ipv6Addr, u8) {
    let prefix = std::net::Ipv6Addr::new(
        WIREGUARD_TUNNEL_PREFIX[0],
        WIREGUARD_TUNNEL_PREFIX[1],
        WIREGUARD_TUNNEL_PREFIX[2],
        WIREGUARD_TUNNEL_PREFIX[3],
        0,
        0,
        0,
        0,
    );
    (prefix, 64)
}

/// Derive a deterministic WireGuard tunnel IPv6 address for a given node id.
///
/// This mapping is stable and consistent across the cluster, so nodes do not need any
/// coordination to compute peer tunnel addresses.
pub fn wireguard_tunnel_ipv6(node_id: Uuid) -> std::net::Ipv6Addr {
    let bytes = node_id.as_bytes();
    let host = &bytes[8..];
    std::net::Ipv6Addr::new(
        WIREGUARD_TUNNEL_PREFIX[0],
        WIREGUARD_TUNNEL_PREFIX[1],
        WIREGUARD_TUNNEL_PREFIX[2],
        WIREGUARD_TUNNEL_PREFIX[3],
        u16::from_be_bytes([host[0], host[1]]),
        u16::from_be_bytes([host[2], host[3]]),
        u16::from_be_bytes([host[4], host[5]]),
        u16::from_be_bytes([host[6], host[7]]),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::STATE_DIR_ENV;
    use parking_lot::{Mutex, MutexGuard};
    use std::ffi::OsString;
    use std::sync::OnceLock;
    use tempfile::TempDir;

    /// Serialize state-dir overrides because these tests mutate one process-global environment.
    fn state_dir_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    /// Restore one temporary environment override after the scoped test mutation ends.
    struct EnvOverrideGuard {
        previous: Option<OsString>,
        _lock: MutexGuard<'static, ()>,
    }

    impl EnvOverrideGuard {
        /// Apply one temporary state-dir override for a unit test and hold the serialization lock.
        fn state_dir(path: &Path) -> Self {
            let lock = state_dir_lock().lock();
            let previous = std::env::var_os(STATE_DIR_ENV);
            unsafe {
                std::env::set_var(STATE_DIR_ENV, path.as_os_str());
            }
            Self {
                previous,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvOverrideGuard {
        /// Restore the previous state-dir override after the test completes.
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => unsafe {
                    std::env::set_var(STATE_DIR_ENV, value);
                },
                None => unsafe {
                    std::env::remove_var(STATE_DIR_ENV);
                },
            }
        }
    }

    #[test]
    fn zero_port_file_is_repaired_to_preferred_port() {
        let dir = TempDir::new().expect("create temp wireguard state dir");
        let _guard = EnvOverrideGuard::state_dir(dir.path());
        let port_path = resolve_wireguard_port_path().expect("resolve wireguard port path");
        fs::write(&port_path, "0\n").expect("seed invalid wireguard port file");

        let port =
            load_or_choose_wireguard_listen_port_with_preferred_and_override(Some(6578), None)
                .expect("repair stale zero-valued wireguard port");

        assert_eq!(port, 6578);
        assert_eq!(
            fs::read_to_string(&port_path).expect("read repaired wireguard port"),
            "6578\n"
        );
    }

    #[test]
    fn invalid_underlay_preference_marker_returns_error() {
        let dir = TempDir::new().expect("create temp wireguard state dir");
        let _guard = EnvOverrideGuard::state_dir(dir.path());
        let preference_path = resolve_wireguard_underlay_preference_path()
            .expect("resolve wireguard underlay preference path");
        fs::write(&preference_path, [0xff, 0xfe]).expect("seed invalid preference file");

        let err = load_wireguard_underlay_preference().expect_err("invalid utf8 should surface");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
