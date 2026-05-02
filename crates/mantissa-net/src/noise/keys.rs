use crate::paths::{ensure_mantissa_group, ensure_state_dir, running_as_root};
use getrandom::getrandom;
use std::path::{Path, PathBuf};
use std::{fs, io};
use x25519_dalek::{PublicKey, StaticSecret};

/// Static X25519 keypair used to identify one Mantissa node in Noise sessions.
pub struct NoiseKeys {
    /// Private key used to build initiator and responder handshake states.
    pub private: StaticSecret,
    /// Public key advertised to peers and stored in cluster identity records.
    pub public: PublicKey,
}

impl NoiseKeys {
    /// Construct one keypair from an existing 32-byte private key.
    ///
    /// This is used by tests and persisted key loading so the public key is
    /// always derived from the canonical private bytes.
    pub fn from_private_bytes(secret: [u8; 32]) -> Self {
        let priv_key = StaticSecret::from(secret);
        let pub_key = PublicKey::from(&priv_key);
        Self {
            private: priv_key,
            public: pub_key,
        }
    }

    /// Return the private key as raw bytes for handshake builder setup.
    pub fn to_private_bytes(&self) -> [u8; 32] {
        self.private.to_bytes()
    }

    /// Return the public key as an owned 32-byte array.
    pub fn public_bytes(&self) -> [u8; 32] {
        self.public.to_bytes()
    }

    /// Return the public key as a borrowed 32-byte array.
    pub fn public_slice(&self) -> &[u8; 32] {
        self.public.as_bytes()
    }
}

/// Resolve the on-disk location used for the node's persisted Noise keypair.
///
/// Mantissa prefers `/var/lib/mantissa` when it is running with privileges and
/// falls back to the user's state directory otherwise.
pub fn resolve_noise_key_path() -> io::Result<PathBuf> {
    let dir = ensure_state_dir()?;
    Ok(dir.join("noise.key"))
}

/// Load the node's private key from `path` or generate and persist a new one.
///
/// This keeps key ownership in one place so every caller gets the same file
/// permissions and root-group handling across development and production.
pub fn load_or_generate_noise_keys(path: impl AsRef<Path>) -> io::Result<NoiseKeys> {
    let path = path.as_ref();
    let private_bytes = if path.exists() {
        let bytes = fs::read(path)?;
        let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "noise private key must be 32 bytes",
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

    Ok(NoiseKeys::from_private_bytes(private_bytes))
}
