use crate::paths::{ensure_mantissa_group, ensure_state_dir, running_as_root};
use async_trait::async_trait;
use futures::lock::Mutex;
use getrandom::getrandom;
use hkdf::Hkdf;
use sha2::Sha256;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{Duration, timeout};
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

const NOISE_PARAMS_JOIN: &str = "Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s";
const NOISE_PARAMS_PEER: &str = "Noise_IK_25519_ChaChaPoly_BLAKE2s";
const MAX_FRAME: usize = 64 * 1024;
const TOKEN_PSK_SALT: &[u8] = b"mantissa/noise-psk-salt/v1";
const TOKEN_PSK_INFO: &[u8] = b"mantissa/noise-psk-info/v1";
const TOKEN_PSK_LOCATION: u8 = 3;
const JOIN_PROBE_REQ: &[u8; 8] = b"MNTJNP01";
const JOIN_PROBE_RESP: &[u8; 8] = b"MNTJNP02";
const JOIN_PROBE_HELLO: &[u8; 8] = b"MNTJNH01";
const JOIN_PROBE_ACK: &[u8; 8] = b"MNTJNA01";
const JOIN_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Indicates whether a Noise handshake authenticated a peer or a joiner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandshakeKind {
    Join,
    Peer,
}

/// Result of a server-side Noise handshake, including the authenticated kind.
pub struct ServerHandshake {
    pub stream: tokio::io::DuplexStream,
    pub kind: HandshakeKind,
    pub join_probe: bool,
}

/// Client-side join handshake result, including whether the server supports probes.
pub struct ClientJoinHandshake {
    pub stream: tokio::io::DuplexStream,
    pub probe_enabled: bool,
}

struct ServerJoinHandshake {
    stream: tokio::io::DuplexStream,
    probe_required: bool,
}

/// Parse and validate the configured Noise parameters.
/// This centralizes handshake configuration for both client and server.
fn parsed_noise_params(params: &str) -> io::Result<snow::params::NoiseParams> {
    params
        .parse()
        .map_err(|e| io::Error::other(format!("invalid noise params: {e}")))
}

/// Provide a Noise PSK derived from the current join token.
/// This authenticates the connection at the transport layer for cluster peers.
#[async_trait(?Send)]
pub trait NoisePskProvider {
    /// Return the current Noise PSK used to authenticate transport connections.
    async fn psk(&self) -> io::Result<[u8; 32]>;
}

/// Verify whether a remote static public key is allowed for peer connections.
#[async_trait(?Send)]
pub trait NoisePeerVerifier {
    /// Return true if `remote_static` is an authorized peer static key.
    async fn is_allowed(&self, remote_static: &[u8]) -> io::Result<bool>;
}

#[derive(Debug)]
pub enum PeerHandshakeError {
    PatternMismatch,
    UnknownPeer,
    Io(io::Error),
}

impl From<io::Error> for PeerHandshakeError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// Derive a 32-byte PSK from the join token using HKDF-SHA256.
/// This turns the human-pasted token into a Noise-compatible shared secret.
pub fn derive_psk_from_token(token: &str) -> io::Result<[u8; 32]> {
    if token.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "join token is empty",
        ));
    }

    let hk = Hkdf::<Sha256>::new(Some(TOKEN_PSK_SALT), token.as_bytes());
    let mut out = [0u8; 32];
    hk.expand(TOKEN_PSK_INFO, &mut out).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "failed to derive Noise PSK from token",
        )
    })?;
    Ok(out)
}

// prologue no longer contains the token — just a static tag
fn prologue() -> &'static [u8] {
    b"MANTISSA|v1"
}

/// Client side join handshake with probe negotiation.
/// Handshake (Noise_XXpsk3) with length-prefixed frames and a PSK for authentication.
/// The client sends a small payload indicating probe support and reads the server's ack.
pub async fn client_handshake_join_with_probe(
    tcp: tokio::net::TcpStream,
    keys: &crate::noise::NoiseKeys,
    psk: &[u8; 32],
) -> io::Result<ClientJoinHandshake> {
    let pk_bytes = keys.private.to_bytes();

    let builder = snow::Builder::new(parsed_noise_params(NOISE_PARAMS_JOIN)?)
        .prologue(prologue())
        .local_private_key(&pk_bytes)
        .psk(TOKEN_PSK_LOCATION, psk);

    let mut hs = builder
        .build_initiator()
        .map_err(|e| io::Error::other(e.to_string()))?;

    let (mut rd, mut wr) = tcp.into_split();

    // -> e (send probe hello payload)
    let mut out = vec![0u8; 65535];
    let n = hs
        .write_message(JOIN_PROBE_HELLO, &mut out)
        .map_err(|e| io::Error::other(e.to_string()))?;
    write_framed(&mut wr, &out[..n]).await?;

    // <- e, ee, s, es
    let mut inb = vec![0u8; 65535];
    let nread = read_framed_len(&mut rd, &mut inb).await?;
    let n = hs.read_message(&inb[..nread], &mut out).map_err(|e| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("handshake failed: {e}"),
        )
    })?;
    let probe_enabled = &out[..n] == JOIN_PROBE_ACK;

    // -> s, se (no payload)
    let n = hs
        .write_message(&[], &mut out)
        .map_err(|e| io::Error::other(e.to_string()))?;
    write_framed(&mut wr, &out[..n]).await?;

    // Done: switch to transport and spawn the IO bridge
    let transport = hs
        .into_transport_mode()
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(ClientJoinHandshake {
        stream: crate::noise::spawn_noise_io_bridge(rd, wr, transport),
        probe_enabled,
    })
}

/// Client side handshake (legacy-compatible).
/// Handshake (Noise_XXpsk3) with length-prefixed frames and a PSK for authentication.
/// Returns a duplex stream bridged through the Noise transport.
pub async fn client_handshake_join(
    tcp: tokio::net::TcpStream,
    keys: &crate::noise::NoiseKeys,
    psk: &[u8; 32],
) -> io::Result<tokio::io::DuplexStream> {
    Ok(client_handshake_join_with_probe(tcp, keys, psk)
        .await?
        .stream)
}

/// Confirm the join PSK on the client side by round-tripping a short probe.
/// This ensures an invalid join token fails before Cap'n Proto setup.
pub async fn join_probe_client(stream: &mut tokio::io::DuplexStream) -> io::Result<()> {
    let fut = async {
        stream.write_all(JOIN_PROBE_REQ).await?;
        stream.flush().await?;
        let mut buf = [0u8; JOIN_PROBE_RESP.len()];
        stream.read_exact(&mut buf).await?;
        if buf != *JOIN_PROBE_RESP {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "join probe response mismatch",
            ));
        }
        Ok(())
    };

    timeout(JOIN_PROBE_TIMEOUT, fut)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "join probe timed out"))?
}

/// Confirm the join PSK on the server side by validating a short probe and responding.
/// This rejects invalid tokens before Cap'n Proto setup.
pub async fn join_probe_server(stream: &mut tokio::io::DuplexStream) -> io::Result<()> {
    let fut = async {
        let mut buf = [0u8; JOIN_PROBE_REQ.len()];
        stream.read_exact(&mut buf).await?;
        if buf != *JOIN_PROBE_REQ {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "join probe request mismatch",
            ));
        }
        stream.write_all(JOIN_PROBE_RESP).await?;
        stream.flush().await?;
        Ok(())
    };

    timeout(JOIN_PROBE_TIMEOUT, fut)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "join probe timed out"))?
}

/// Client side handshake for authenticated peer-to-peer connections.
/// Uses Noise IK to authenticate the responder's static key and reveal our static key.
pub async fn client_handshake_peer(
    tcp: tokio::net::TcpStream,
    keys: &crate::noise::NoiseKeys,
    responder_static: &[u8; 32],
) -> io::Result<tokio::io::DuplexStream> {
    let pk_bytes = keys.private.to_bytes();

    let builder = snow::Builder::new(parsed_noise_params(NOISE_PARAMS_PEER)?)
        .prologue(prologue())
        .local_private_key(&pk_bytes)
        .remote_public_key(responder_static);

    let mut hs = builder
        .build_initiator()
        .map_err(|e| io::Error::other(e.to_string()))?;

    let (mut rd, mut wr) = tcp.into_split();

    // -> e, es, s, ss
    let mut out = vec![0u8; 65535];
    let n = hs
        .write_message(&[], &mut out)
        .map_err(|e| io::Error::other(e.to_string()))?;
    write_framed(&mut wr, &out[..n]).await?;

    // <- e, ee, se
    let mut inb = vec![0u8; 65535];
    let nread = read_framed_len(&mut rd, &mut inb).await?;
    hs.read_message(&inb[..nread], &mut out).map_err(|e| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("handshake failed: {e}"),
        )
    })?;

    let transport = hs
        .into_transport_mode()
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(crate::noise::spawn_noise_io_bridge(rd, wr, transport))
}

/// Server side handshake.
/// Handshake (Noise_XXpsk3) with length-prefixed frames and a PSK for authentication.
/// Returns a duplex stream bridged through the Noise transport.
pub async fn server_handshake_join(
    tcp: tokio::net::TcpStream,
    keys: &crate::noise::NoiseKeys,
    psk: &[u8; 32],
) -> io::Result<tokio::io::DuplexStream> {
    let (mut rd, wr) = tcp.into_split();
    let mut first = vec![0u8; 65535];
    let nread = read_framed_len(&mut rd, &mut first).await?;
    server_handshake_join_with_first_frame(rd, wr, keys, psk, &first[..nread]).await
}

/// Server side join handshake (Noise_XXpsk3) using a pre-read first frame.
pub async fn server_handshake_join_with_first_frame(
    rd: tokio::net::tcp::OwnedReadHalf,
    wr: tokio::net::tcp::OwnedWriteHalf,
    keys: &crate::noise::NoiseKeys,
    psk: &[u8; 32],
    first_frame: &[u8],
) -> io::Result<tokio::io::DuplexStream> {
    Ok(
        server_handshake_join_with_first_frame_probe(rd, wr, keys, psk, first_frame)
            .await?
            .stream,
    )
}

/// Server side join handshake (Noise_XXpsk3) using a pre-read first frame.
/// Returns the Noise stream plus whether the client requested the join probe.
async fn server_handshake_join_with_first_frame_probe(
    mut rd: tokio::net::tcp::OwnedReadHalf,
    mut wr: tokio::net::tcp::OwnedWriteHalf,
    keys: &crate::noise::NoiseKeys,
    psk: &[u8; 32],
    first_frame: &[u8],
) -> io::Result<ServerJoinHandshake> {
    fn map_join_error(err: snow::Error) -> io::Error {
        let msg = err.to_string();
        if msg.contains("decrypt") || msg.contains("psk") {
            io::Error::new(io::ErrorKind::PermissionDenied, "invalid join token")
        } else {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("handshake failed: {msg}"),
            )
        }
    }

    let pk_bytes = keys.private.to_bytes();

    let builder = snow::Builder::new(parsed_noise_params(NOISE_PARAMS_JOIN)?)
        .prologue(prologue())
        .local_private_key(&pk_bytes)
        .psk(TOKEN_PSK_LOCATION, psk);

    let mut hs = builder
        .build_responder()
        .map_err(|e| io::Error::other(e.to_string()))?;

    let mut out = vec![0u8; 65535];
    let n = hs
        .read_message(first_frame, &mut out)
        .map_err(map_join_error)?;
    let probe_required = &out[..n] == JOIN_PROBE_HELLO;

    // -> e, ee, s, es
    let payload = if probe_required {
        JOIN_PROBE_ACK.as_ref()
    } else {
        &[][..]
    };
    let n = hs
        .write_message(payload, &mut out)
        .map_err(|e| io::Error::other(e.to_string()))?;
    write_framed(&mut wr, &out[..n]).await?;

    // <- s, se
    let mut inb = vec![0u8; 65535];
    let nread = read_framed_len(&mut rd, &mut inb).await?;
    hs.read_message(&inb[..nread], &mut out)
        .map_err(map_join_error)?;

    let transport = hs
        .into_transport_mode()
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(ServerJoinHandshake {
        stream: crate::noise::spawn_noise_io_bridge(rd, wr, transport),
        probe_required,
    })
}

/// Server side peer handshake (Noise IK) using a pre-read first frame.
pub async fn server_handshake_peer_with_first_frame(
    rd: tokio::net::tcp::OwnedReadHalf,
    mut wr: tokio::net::tcp::OwnedWriteHalf,
    keys: &crate::noise::NoiseKeys,
    first_frame: &[u8],
    verifier: Arc<dyn NoisePeerVerifier>,
) -> Result<tokio::io::DuplexStream, PeerHandshakeError> {
    let pk_bytes = keys.private.to_bytes();

    let builder = snow::Builder::new(parsed_noise_params(NOISE_PARAMS_PEER)?)
        .prologue(prologue())
        .local_private_key(&pk_bytes);

    let mut hs = builder
        .build_responder()
        .map_err(|e| io::Error::other(e.to_string()))?;

    let mut out = vec![0u8; 65535];
    hs.read_message(first_frame, &mut out)
        .map_err(|_| PeerHandshakeError::PatternMismatch)?;

    let remote = hs
        .get_remote_static()
        .ok_or(PeerHandshakeError::PatternMismatch)?;

    if !verifier.is_allowed(remote).await? {
        return Err(PeerHandshakeError::UnknownPeer);
    }

    let n = hs
        .write_message(&[], &mut out)
        .map_err(|e| io::Error::other(e.to_string()))?;
    write_framed(&mut wr, &out[..n]).await?;

    let transport = hs
        .into_transport_mode()
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(crate::noise::spawn_noise_io_bridge(rd, wr, transport))
}

#[derive(Debug)]
pub enum ServerHandshakeError {
    UnknownPeer,
    Io(io::Error),
}

impl From<io::Error> for ServerHandshakeError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// Select a server-side handshake based on the first frame:
/// - Try peer IK first (requires authorized static key).
/// - Fallback to join XXpsk3 when the peer pattern doesn't match.
pub async fn server_handshake_select(
    rd: tokio::net::tcp::OwnedReadHalf,
    wr: tokio::net::tcp::OwnedWriteHalf,
    keys: &crate::noise::NoiseKeys,
    psk: &[u8; 32],
    first_frame: &[u8],
    verifier: Arc<dyn NoisePeerVerifier>,
) -> Result<ServerHandshake, ServerHandshakeError> {
    let pk_bytes = keys.private.to_bytes();

    let builder = snow::Builder::new(parsed_noise_params(NOISE_PARAMS_PEER)?)
        .prologue(prologue())
        .local_private_key(&pk_bytes);

    let mut hs = builder
        .build_responder()
        .map_err(|e| io::Error::other(e.to_string()))?;

    let mut out = vec![0u8; 65535];
    match hs.read_message(first_frame, &mut out) {
        Ok(_) => {
            let remote = hs
                .get_remote_static()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing peer static"))?;

            if !verifier.is_allowed(remote).await? {
                return Err(ServerHandshakeError::UnknownPeer);
            }

            let mut wr = wr;
            let n = hs
                .write_message(&[], &mut out)
                .map_err(|e| io::Error::other(e.to_string()))?;
            write_framed(&mut wr, &out[..n]).await?;

            let transport = hs
                .into_transport_mode()
                .map_err(|e| io::Error::other(e.to_string()))?;

            return Ok(ServerHandshake {
                stream: crate::noise::spawn_noise_io_bridge(rd, wr, transport),
                kind: HandshakeKind::Peer,
                join_probe: false,
            });
        }
        Err(_) => {
            let join = server_handshake_join_with_first_frame_probe(rd, wr, keys, psk, first_frame)
                .await?;
            return Ok(ServerHandshake {
                stream: join.stream,
                kind: HandshakeKind::Join,
                join_probe: join.probe_required,
            });
        }
    }
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

/// Prefer `/var/lib/mantissa` when privileged, otherwise fallback to `~/.mantissa`.
pub fn resolve_noise_key_path() -> io::Result<PathBuf> {
    let dir = ensure_state_dir()?;
    Ok(dir.join("noise.key"))
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

pub async fn read_framed_len<R>(rd: &mut R, buf: &mut Vec<u8>) -> io::Result<usize>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut len = [0u8; 2];
    rd.read_exact(&mut len).await?;
    let n = u16::from_be_bytes(len) as usize;

    if n > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "handshake frame too large",
        ));
    }

    if buf.len() < n {
        buf.resize(n, 0);
    }
    rd.read_exact(&mut buf[..n]).await?;
    Ok(n)
}

pub async fn write_framed<W>(wr: &mut W, data: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if data.len() > u16::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let len = (data.len() as u16).to_be_bytes();
    wr.write_all(&len).await?;
    wr.write_all(data).await?;
    wr.flush().await
}
