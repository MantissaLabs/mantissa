use ed25519_dalek::{SECRET_KEY_LENGTH, SigningKey};
use getrandom::getrandom;
use net::paths::{ensure_mantissa_group, ensure_state_dir, running_as_root};
use std::{fs, io, path::Path};

pub struct SignKeys {
    // Our signing key for credentials, inner contains the
    // pub verifying key to send to other peers.
    pub sk: SigningKey,
}

/// Resolve the signing key path, honouring root-aware system defaults.
pub fn resolve_signing_key_path() -> io::Result<std::path::PathBuf> {
    let dir = ensure_state_dir()?;
    Ok(dir.join("ed25519.key"))
}

/// Load or generate the Ed25519 signing keys for API authentication.
pub fn load_or_generate_sign_keys(path: impl AsRef<Path>) -> io::Result<SignKeys> {
    let path = path.as_ref();
    let sk_bytes = if path.exists() {
        let b = fs::read(path)?;
        if b.len() != SECRET_KEY_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad ed25519 key",
            ));
        }
        let mut arr = [0u8; SECRET_KEY_LENGTH];
        arr.copy_from_slice(&b);
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
        let mut arr = [0u8; SECRET_KEY_LENGTH];
        getrandom(&mut arr)?;
        fs::write(path, arr)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = if running_as_root() { 0o640 } else { 0o600 };
            let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
            if running_as_root() {
                ensure_mantissa_group(path);
            }
        }
        arr
    };
    let sk = SigningKey::from_bytes(&sk_bytes);
    Ok(SignKeys { sk })
}
