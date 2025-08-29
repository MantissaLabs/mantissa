// server/cluster_keys.rs
use ed25519_dalek::{SigningKey, VerifyingKey};
use getrandom::getrandom;
use std::path::PathBuf;
use std::{fs, io, path::Path};

pub struct ClusterKeys {
    pub signing: SigningKey,
    pub verifying: VerifyingKey,
}

pub fn resolve_cluster_key_path() -> io::Result<PathBuf> {
    // prefer /var/lib/mantissa/cluster.ed25519; fallback to ~/.mantissa/cluster.ed25519
    let primary = PathBuf::from("/var/lib/mantissa/cluster.ed25519");

    if let Some(parent) = primary.parent() {
        match fs::create_dir_all(parent) {
            Ok(_) => return Ok(primary),
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => { /* fall back */ }
            Err(_e) => { /* fall back */ }
        }
    }

    let home = std::env::var_os("HOME")
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "HOME not set"))?;
    let mut p = PathBuf::from(home);
    p.push(".mantissa");
    fs::create_dir_all(&p)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&p, fs::Permissions::from_mode(0o700));
    }
    p.push("cluster.ed25519");
    Ok(p)
}

pub fn load_or_generate_cluster_keys(path: impl AsRef<Path>) -> io::Result<ClusterKeys> {
    use ed25519_dalek::Signer;

    let path = path.as_ref();
    let sk_bytes: [u8; 32] = if path.exists() {
        let bytes = fs::read(path)?;
        bytes[..32].try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "cluster signing key must be 32 bytes",
            )
        })?
    } else {
        let mut tmp = [0u8; 32];
        getrandom(&mut tmp)?;
        fs::write(path, &tmp)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
        }
        tmp
    };

    let signing = SigningKey::from_bytes(&sk_bytes);
    let verifying = signing.verifying_key();

    Ok(ClusterKeys { signing, verifying })
}
