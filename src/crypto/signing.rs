use ed25519_dalek::{SigningKey, SECRET_KEY_LENGTH};
use getrandom::getrandom;
use std::{fs, io, path::Path};

pub struct SignKeys {
    // Our signing key for credentials, inner contains the
    // pub verifying key to send to other peers.
    pub sk: SigningKey,
}

pub fn resolve_signing_key_path() -> io::Result<std::path::PathBuf> {
    // e.g. ~/.mantissa/ed25519.key (mirror your noise path layout)
    let home = std::env::var_os("HOME")
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "HOME not set"))?;
    let mut p = std::path::PathBuf::from(home);
    p.push(".mantissa");
    fs::create_dir_all(&p)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&p, fs::Permissions::from_mode(0o700));
    }
    p.push("ed25519.key");
    Ok(p)
}

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
        arr
    } else {
        let mut arr = [0u8; SECRET_KEY_LENGTH];
        getrandom(&mut arr)?;
        fs::write(path, arr)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
        }
        arr
    };
    let sk = SigningKey::from_bytes(&sk_bytes);
    Ok(SignKeys { sk })
}
