use futures::lock::Mutex;
use getrandom::getrandom;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs, io, path::Path};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use x25519_dalek::{PublicKey, StaticSecret};

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

    // -> s, se    (no app payload here)
    let n = hs.write_message(&[], &mut out).unwrap();
    wr.write_all(&out[..n]).await?;

    let transport = hs.into_transport_mode().unwrap();
    Ok(spawn_noise_io_bridge(rd, wr, transport))
}

pub async fn server_handshake(
    tcp: tokio::net::TcpStream,
    keys: &NoiseKeys,
) -> std::io::Result<tokio::io::DuplexStream> {
    use std::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

    // <- s, se   (no app payload expected)
    let nread = rd.read(&mut inb).await?;
    hs.read_message(&inb[..nread], &mut out).map_err(|e| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("handshake failed: {e}"),
        )
    })?;

    let transport = hs.into_transport_mode().unwrap();
    Ok(spawn_noise_io_bridge(rd, wr, transport))
}

/// Spawn a Noise-protected I/O bridge between the TCP socket and an in-process
/// duplex stream returned to the caller (the "application end").
///
/// Layout:
///   - app_end  (returned)  <==== plaintext ====>  noise_end (internal)
///   - Task A: TCP -> [len|ciphertext] -> decrypt -> write plaintext to app_end
///   - Task B: read plaintext from app_end -> encrypt -> TCP [len|ciphertext]
///
/// Framing:
///   Each encrypted frame on the wire is sent as:
///     [u16 big-endian length][ciphertext bytes...]
///
/// Concurrency:
///   `snow::TransportState` is not `Sync`, and its read/write operations mutate
///   internal counters/nonces. We wrap it in `Arc<Mutex<_>>` and *share the same
///   transport* across both directions to keep the Noise state coherent.
fn spawn_noise_io_bridge(
    mut tcp_reader: tokio::net::tcp::OwnedReadHalf,
    mut tcp_writer: tokio::net::tcp::OwnedWriteHalf,
    transport: snow::TransportState,
) -> tokio::io::DuplexStream {
    // A bidirectional in-process pipe: one end for the application (`app_end`),
    // the other end (`noise_end`) is used internally by the bridge tasks.
    let (app_end, noise_end) = tokio::io::duplex(MAX_FRAME * 2);

    // Split the internal end so each task owns exactly one half.
    // - `noise_writer`: Task A writes plaintext to the app (app will read it from `app_end`)
    // - `noise_reader`: Task B reads plaintext from the app (app writes to `app_end`)
    let (mut noise_reader, mut noise_writer) = tokio::io::split(noise_end);

    // Share the Noise transport safely between the two tasks.
    let transport = Arc::new(Mutex::new(transport));
    let transport_for_read = transport.clone(); // used by Task A (decrypt)
    let transport_for_write = transport.clone(); // used by Task B (encrypt)

    // Task A: TCP -> decrypt -> app
    //
    // Reads length-prefixed ciphertext from the TCP socket, decrypts it with
    // Noise, and forwards the resulting plaintext into `noise_writer` so the
    // application can read it from `app_end`.
    tokio::spawn(async move {
        let mut len_prefix = [0u8; 2];
        let mut cipher_buf = vec![0u8; MAX_FRAME + 1024]; // headroom
        let mut plain_buf = vec![0u8; MAX_FRAME + 1024];

        loop {
            // Read the 2-byte length prefix.
            if tcp_reader.read_exact(&mut len_prefix).await.is_err() {
                let _ = noise_writer.shutdown().await;
                break;
            }
            let clen = u16::from_be_bytes(len_prefix) as usize;

            // Read encrypted payload.
            if cipher_buf.len() < clen {
                cipher_buf.resize(clen, 0);
            }
            if tcp_reader
                .read_exact(&mut cipher_buf[..clen])
                .await
                .is_err()
            {
                let _ = noise_writer.shutdown().await;
                break;
            }

            // Decrypt into plaintext.
            if plain_buf.len() < clen {
                plain_buf.resize(clen, 0);
            }
            let n_plain = {
                let mut t = transport_for_read.lock().await;
                match t.read_message(&cipher_buf[..clen], &mut plain_buf) {
                    Ok(n) => n,
                    Err(_) => {
                        let _ = noise_writer.shutdown().await;
                        break;
                    }
                }
            };

            // Forward plaintext to the application side.
            if noise_writer.write_all(&plain_buf[..n_plain]).await.is_err() {
                let _ = noise_writer.shutdown().await;
                break;
            }
        }
    });

    // Task B: app -> encrypt -> TCP
    //
    // Reads plaintext from `noise_reader` (what the application writes to `app_end`),
    // encrypts it with Noise, and writes length-prefixed ciphertext to the TCP socket.
    tokio::spawn(async move {
        let mut plain_buf = vec![0u8; MAX_FRAME];
        let mut cipher_buf = vec![0u8; MAX_FRAME + 16]; // AEAD overhead

        loop {
            // Read plaintext from the application side.
            let n_plain = match noise_reader.read(&mut plain_buf).await {
                Ok(0) | Err(_) => {
                    let _ = tcp_writer.shutdown().await;
                    break;
                }
                Ok(n) => n,
            };
            let plain = &plain_buf[..n_plain];

            // Encrypt with Noise.
            if cipher_buf.len() < plain.len() + 16 {
                cipher_buf.resize(plain.len() + 16, 0);
            }
            let clen = {
                let mut t = transport_for_write.lock().await;
                match t.write_message(plain, &mut cipher_buf) {
                    Ok(n) => n,
                    Err(_) => {
                        let _ = tcp_writer.shutdown().await;
                        break;
                    }
                }
            };

            // Write length prefix + ciphertext to the wire.
            let len_bytes = (clen as u16).to_be_bytes();
            if tcp_writer.write_all(&len_bytes).await.is_err() {
                break;
            }
            if tcp_writer.write_all(&cipher_buf[..clen]).await.is_err() {
                break;
            }
            if tcp_writer.flush().await.is_err() {
                break;
            }
        }
    });

    // The application uses this end (plaintext in both directions).
    app_end
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
