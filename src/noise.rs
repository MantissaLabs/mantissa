use futures::lock::Mutex;
use getrandom::getrandom;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs, io, path::Path};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use x25519_dalek::{PublicKey, StaticSecret};

use crate::token::TokenStore;

pub struct NoiseKeys {
    pub private: StaticSecret,
    pub public: PublicKey,
}

impl NoiseKeys {
    pub fn from_private_bytes(secret: [u8; 32]) -> Self {
        let priv_key = StaticSecret::from(secret);
        let pub_key = PublicKey::from(&priv_key);
        Self {
            private: priv_key,
            public: pub_key,
        }
    }

    pub fn to_private_bytes(&self) -> [u8; 32] {
        self.private.to_bytes()
    }

    pub fn public_bytes(&self) -> [u8; 32] {
        self.public.to_bytes()
    }

    pub fn public_slice(&self) -> &[u8; 32] {
        self.public.as_bytes()
    }
}

const NOISE_PARAMS: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";
const MAX_FRAME: usize = 64 * 1024;

// prologue no longer contains the token — just a static tag
fn prologue() -> &'static [u8] {
    b"MANTISSA|v1"
}

pub async fn client_handshake(
    tcp: tokio::net::TcpStream,
    token: &str,
    keys: &NoiseKeys,
) -> std::io::Result<tokio::io::DuplexStream> {
    use std::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let pk_bytes = keys.private.to_bytes();

    let builder = snow::Builder::new(NOISE_PARAMS.parse().unwrap())
        .prologue(prologue())
        .local_private_key(&pk_bytes);

    let mut hs = builder
        .build_initiator()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    let (mut rd, mut wr) = tcp.into_split();

    // -> e
    let mut out = vec![0u8; 65535];
    let n = hs.write_message(&[], &mut out).unwrap();
    wr.write_all(&out[..n]).await?;

    // <- e, ee, s, es
    let mut inb = vec![0u8; 65535];
    let nread = rd.read(&mut inb).await?;
    hs.read_message(&inb[..nread], &mut out).map_err(|e| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("handshake failed: {e}"),
        )
    })?;

    // -> s, se, payload=token (encrypted)
    let n = hs.write_message(token.as_bytes(), &mut out).unwrap();
    wr.write_all(&out[..n]).await?;

    let transport = hs.into_transport_mode().unwrap();
    Ok(spawn_noise_pump(rd, wr, transport)) // your existing pump
}

pub async fn server_handshake(
    tcp: tokio::net::TcpStream,
    tokens: TokenStore,
    keys: &NoiseKeys,
) -> std::io::Result<tokio::io::DuplexStream> {
    use std::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // FIXME: Fix this and use local socket instead.
    // peer_is_loopback is problematic to test locally on different ports.
    let peer_is_loopback = tcp
        .peer_addr()
        .ok()
        .map(|sa| sa.ip().is_loopback())
        .unwrap_or(false);

    let pk_bytes = keys.private.to_bytes();

    let builder = snow::Builder::new(NOISE_PARAMS.parse().unwrap())
        .prologue(prologue())
        .local_private_key(&pk_bytes);

    let mut hs = builder
        .build_responder()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    let (mut rd, mut wr) = tcp.into_split();

    // <- e
    let mut inb = vec![0u8; 65535];
    let nread = rd.read(&mut inb).await?;
    let mut out = vec![0u8; 65535];
    hs.read_message(&inb[..nread], &mut out).map_err(|e| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("handshake failed: {e}"),
        )
    })?;

    // -> e, ee, s, es
    let n = hs.write_message(&[], &mut out).unwrap();
    wr.write_all(&out[..n]).await?;

    // <- s, se, payload=token
    let nread = rd.read(&mut inb).await?;
    let payload_len = hs.read_message(&inb[..nread], &mut out).map_err(|e| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("handshake failed: {e}"),
        )
    })?;
    let token_bytes = &out[..payload_len];
    let token_str = std::str::from_utf8(token_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::PermissionDenied, "invalid token bytes"))?;

    // TODO: Use Admission Trait to check whether the member is already registered
    // or not, and if not, register them using their public key.

    // Auth decision:
    // - If loopback and flag is set -> allow without token
    // - Else -> require token match
    if !peer_is_loopback {
        if !tokens.matches(token_str).await {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid join token",
            ));
        }
    }

    let transport = hs.into_transport_mode().unwrap();
    Ok(spawn_noise_pump(rd, wr, transport))
}

fn spawn_noise_pump(
    mut tcp_rd: tokio::net::tcp::OwnedReadHalf,
    mut tcp_wr: tokio::net::tcp::OwnedWriteHalf,
    transport: snow::TransportState,
) -> tokio::io::DuplexStream {
    // app_side <-> pump_side
    let (app_side, pump_side) = tokio::io::duplex(MAX_FRAME * 2);

    // Split the pump end so each task owns exactly one half
    let (mut pump_r, mut pump_w) = tokio::io::split(pump_side);

    // Share the Noise transport safely between tasks
    let transport = Arc::new(Mutex::new(transport));
    let t_read = transport.clone();
    let t_write = transport.clone();

    // Task 1: TCP -> decrypt -> app  (writes to pump_w)
    tokio::spawn(async move {
        let mut len_buf = [0u8; 2];
        let mut cipher = vec![0u8; MAX_FRAME + 1024];
        let mut plain = vec![0u8; MAX_FRAME + 1024];

        loop {
            if tcp_rd.read_exact(&mut len_buf).await.is_err() {
                let _ = pump_w.shutdown().await;
                break;
            }
            let clen = u16::from_be_bytes(len_buf) as usize;
            if clen > cipher.len() {
                cipher.resize(clen, 0);
            }
            if tcp_rd.read_exact(&mut cipher[..clen]).await.is_err() {
                let _ = pump_w.shutdown().await;
                break;
            }

            if plain.len() < clen {
                plain.resize(clen, 0);
            }
            let n = {
                let mut tr = t_read.lock().await;
                match tr.read_message(&cipher[..clen], &mut plain) {
                    Ok(n) => n,
                    Err(_) => {
                        let _ = pump_w.shutdown().await;
                        break;
                    }
                }
            };

            if pump_w.write_all(&plain[..n]).await.is_err() {
                let _ = pump_w.shutdown().await;
                break;
            }
        }
    });

    // Task 2: app -> encrypt -> TCP (reads from pump_r)
    tokio::spawn(async move {
        let mut buf = vec![0u8; MAX_FRAME];
        let mut cipher = vec![0u8; MAX_FRAME + 16];

        loop {
            let n = match pump_r.read(&mut buf).await {
                Ok(0) | Err(_) => {
                    let _ = tcp_wr.shutdown().await;
                    break;
                }
                Ok(n) => n,
            };
            let p = &buf[..n];

            if cipher.len() < p.len() + 16 {
                cipher.resize(p.len() + 16, 0);
            }
            let clen = {
                let mut tr = t_write.lock().await;
                match tr.write_message(p, &mut cipher) {
                    Ok(n) => n,
                    Err(_) => {
                        let _ = tcp_wr.shutdown().await;
                        break;
                    }
                }
            };

            let len_bytes = (clen as u16).to_be_bytes();
            if tcp_wr.write_all(&len_bytes).await.is_err() {
                break;
            }
            if tcp_wr.write_all(&cipher[..clen]).await.is_err() {
                break;
            }
            if tcp_wr.flush().await.is_err() {
                break;
            }
        }
    });

    app_side
}

/// Prefer `/var/lib/mantissa/noise.key`; fallback to `~/.mantissa/noise.key`.
pub fn resolve_noise_key_path() -> io::Result<PathBuf> {
    let primary = PathBuf::from("/var/lib/mantissa/noise.key");

    // Try to ensure the system dir exists; if we can create it, we likely can write the key there.
    if let Some(parent) = primary.parent() {
        match fs::create_dir_all(parent) {
            Ok(_) => return Ok(primary),
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => { /* fall back */ }
            Err(e) => {
                // If it failed for another reason (e.g., read-only FS), also fall back.
                eprintln!(
                    "warn: cannot use {} ({e}); falling back to HOME",
                    parent.display()
                );
            }
        }
    }

    // Fallback: ~/.mantissa/noise.key
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
    p.push("noise.key");
    Ok(p)
}

/// Load a 32-byte private key from `path`, or generate and persist a new one.
/// Derives the public key every time.
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
        arr
    } else {
        let mut sk = [0u8; 32];
        getrandom(&mut sk)?;
        fs::write(path, &sk)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
        }
        sk
    };

    Ok(NoiseKeys::from_private_bytes(private_bytes))
}
