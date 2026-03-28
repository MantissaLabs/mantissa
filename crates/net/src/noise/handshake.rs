use async_trait::async_trait;
use hkdf::Hkdf;
use sha2::Sha256;
use std::io;
use std::rc::Rc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::timeout;

use super::framing::{read_framed_len, write_framed};
use super::keys::NoiseKeys;
use super::transport::NoiseStream;
use super::{
    JOIN_PROBE_ACK, JOIN_PROBE_HELLO, JOIN_PROBE_REQ, JOIN_PROBE_RESP, JOIN_PROBE_TIMEOUT,
    NOISE_PARAMS_JOIN, NOISE_PARAMS_PEER, TOKEN_PSK_INFO, TOKEN_PSK_LOCATION, TOKEN_PSK_SALT,
    parsed_noise_params, prologue,
};

/// Indicates whether a server-side Noise handshake authenticated a peer or a
/// joining node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandshakeKind {
    /// Handshake authenticated a join token and admitted a joining node.
    Join,
    /// Handshake authenticated a known peer static key with the IK pattern.
    Peer,
}

/// Result of a server-side Noise handshake, including the authenticated kind.
pub struct ServerHandshake {
    /// Established encrypted transport stream for the authenticated session.
    pub stream: NoiseStream,
    /// Handshake kind the server selected after inspecting the first frame.
    pub kind: HandshakeKind,
    /// Whether the join path negotiated the post-handshake probe capability.
    pub join_probe: bool,
}

/// Client-side join handshake result, including probe support negotiation.
pub struct ClientJoinHandshake {
    /// Established encrypted transport stream for the join session.
    pub stream: NoiseStream,
    /// Whether the server acknowledged support for the join probe round-trip.
    pub probe_enabled: bool,
}

/// Internal server-side join handshake state before the public result is built.
struct ServerJoinHandshake {
    stream: NoiseStream,
    probe_required: bool,
}

/// Provide the current Noise PSK derived from the active join token.
///
/// The server uses this abstraction so token storage and rotation stay outside
/// the transport crate.
#[async_trait(?Send)]
pub trait NoisePskProvider {
    /// Return the current Noise PSK used to authenticate transport connections.
    async fn psk(&self) -> io::Result<[u8; 32]>;
}

/// Verify whether a remote static public key is allowed for peer handshakes.
///
/// This decouples cluster membership policy from the transport handshake logic.
#[async_trait(?Send)]
pub trait NoisePeerVerifier {
    /// Return true if `remote_static` is an authorized peer static key.
    async fn is_allowed(&self, remote_static: &[u8]) -> io::Result<bool>;
}

/// Errors returned when the server tries to interpret the first frame as a
/// peer IK handshake.
#[derive(Debug)]
pub enum PeerHandshakeError {
    /// The first frame does not match the IK pattern and should be retried as
    /// another handshake type.
    PatternMismatch,
    /// The peer pattern matched, but the remote static key is not authorized.
    UnknownPeer,
    /// Any lower-level I/O or transport setup failure.
    Io(io::Error),
}

impl From<io::Error> for PeerHandshakeError {
    /// Wrap a transport or framing error so peer handshake callers can use one
    /// error type throughout the selection path.
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// Errors returned by the server-side handshake selector.
#[derive(Debug)]
pub enum ServerHandshakeError {
    /// The first frame matched the peer pattern, but the node is not allowed.
    UnknownPeer,
    /// Any I/O, handshake, or framing failure that is not an authorization miss.
    Io(io::Error),
}

impl From<io::Error> for ServerHandshakeError {
    /// Wrap a transport or framing error from the selected handshake path.
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// Derive a 32-byte PSK from a join token using HKDF-SHA256.
///
/// This converts the human-pasted cluster token into the fixed-size secret
/// required by the `XXpsk3` join handshake.
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

/// Run the client side of the join handshake and negotiate probe support.
///
/// The client sends a probe-capability marker in the first payload so the
/// server can acknowledge support before both sides switch to transport mode.
pub async fn client_handshake_join_with_probe(
    tcp: tokio::net::TcpStream,
    keys: &NoiseKeys,
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

    // Send the first join message with an embedded probe-capability marker.
    let mut out = vec![0u8; 65535];
    let n = hs
        .write_message(JOIN_PROBE_HELLO, &mut out)
        .map_err(|e| io::Error::other(e.to_string()))?;
    write_framed(&mut wr, &out[..n]).await?;

    // Read the responder payload and check whether it echoed probe support.
    let mut inb = vec![0u8; 65535];
    let nread = read_framed_len(&mut rd, &mut inb).await?;
    let n = hs.read_message(&inb[..nread], &mut out).map_err(|e| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("handshake failed: {e}"),
        )
    })?;
    let probe_enabled = &out[..n] == JOIN_PROBE_ACK;

    // Finish the XX handshake and switch into the streaming transport.
    let n = hs
        .write_message(&[], &mut out)
        .map_err(|e| io::Error::other(e.to_string()))?;
    write_framed(&mut wr, &out[..n]).await?;

    let transport = hs
        .into_stateless_transport_mode()
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(ClientJoinHandshake {
        stream: NoiseStream::new(rd, wr, transport),
        probe_enabled,
    })
}

/// Run the client side of the join handshake and return only the stream.
///
/// This keeps older call sites simple while the probe-capable variant remains
/// the canonical entry point.
pub async fn client_handshake_join(
    tcp: tokio::net::TcpStream,
    keys: &NoiseKeys,
    psk: &[u8; 32],
) -> io::Result<NoiseStream> {
    Ok(client_handshake_join_with_probe(tcp, keys, psk)
        .await?
        .stream)
}

/// Round-trip one short probe over an established join stream on the client.
///
/// This lets the caller validate the join token before Cap'n Proto setup.
pub async fn join_probe_client<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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

/// Validate one short probe on the server side of an established join stream.
///
/// This rejects invalid join tokens before the higher-level RPC system starts.
pub async fn join_probe_server<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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

/// Run the client side of the authenticated peer-to-peer IK handshake.
///
/// The initiator pins the responder static key and reveals its own static key
/// so both sides can switch directly into the steady-state transport.
pub async fn client_handshake_peer(
    tcp: tokio::net::TcpStream,
    keys: &NoiseKeys,
    responder_static: &[u8; 32],
) -> io::Result<NoiseStream> {
    let pk_bytes = keys.private.to_bytes();

    let builder = snow::Builder::new(parsed_noise_params(NOISE_PARAMS_PEER)?)
        .prologue(prologue())
        .local_private_key(&pk_bytes)
        .remote_public_key(responder_static);

    let mut hs = builder
        .build_initiator()
        .map_err(|e| io::Error::other(e.to_string()))?;

    let (mut rd, mut wr) = tcp.into_split();

    let mut out = vec![0u8; 65535];
    let n = hs
        .write_message(&[], &mut out)
        .map_err(|e| io::Error::other(e.to_string()))?;
    write_framed(&mut wr, &out[..n]).await?;

    let mut inb = vec![0u8; 65535];
    let nread = read_framed_len(&mut rd, &mut inb).await?;
    hs.read_message(&inb[..nread], &mut out).map_err(|e| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("handshake failed: {e}"),
        )
    })?;

    let transport = hs
        .into_stateless_transport_mode()
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(NoiseStream::new(rd, wr, transport))
}

/// Run the server side of the join handshake using a fresh TCP stream.
///
/// The server reads the first framed message itself, then delegates to the
/// pre-read variant used by the handshake selector.
pub async fn server_handshake_join(
    tcp: tokio::net::TcpStream,
    keys: &NoiseKeys,
    psk: &[u8; 32],
) -> io::Result<NoiseStream> {
    let (mut rd, wr) = tcp.into_split();
    let mut first = vec![0u8; 65535];
    let nread = read_framed_len(&mut rd, &mut first).await?;
    server_handshake_join_with_first_frame(rd, wr, keys, psk, &first[..nread]).await
}

/// Run the server side of the join handshake with an already-consumed first frame.
///
/// This is the public helper used after a listener has peeked at the first
/// message to decide which handshake path should take ownership.
pub async fn server_handshake_join_with_first_frame(
    rd: OwnedReadHalf,
    wr: OwnedWriteHalf,
    keys: &NoiseKeys,
    psk: &[u8; 32],
    first_frame: &[u8],
) -> io::Result<NoiseStream> {
    Ok(
        server_handshake_join_with_first_frame_probe(rd, wr, keys, psk, first_frame)
            .await?
            .stream,
    )
}

/// Run the full server join handshake and preserve whether the client asked
/// for probe support.
async fn server_handshake_join_with_first_frame_probe(
    mut rd: OwnedReadHalf,
    mut wr: OwnedWriteHalf,
    keys: &NoiseKeys,
    psk: &[u8; 32],
    first_frame: &[u8],
) -> io::Result<ServerJoinHandshake> {
    /// Map Noise join handshake failures into stable I/O errors for callers.
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

    let payload = if probe_required {
        JOIN_PROBE_ACK.as_ref()
    } else {
        &[][..]
    };
    let n = hs
        .write_message(payload, &mut out)
        .map_err(|e| io::Error::other(e.to_string()))?;
    write_framed(&mut wr, &out[..n]).await?;

    let mut inb = vec![0u8; 65535];
    let nread = read_framed_len(&mut rd, &mut inb).await?;
    hs.read_message(&inb[..nread], &mut out)
        .map_err(map_join_error)?;

    let transport = hs
        .into_stateless_transport_mode()
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(ServerJoinHandshake {
        stream: NoiseStream::new(rd, wr, transport),
        probe_required,
    })
}

/// Run the server side of the authenticated peer IK handshake.
///
/// The first frame is already available because the listener needed it to
/// choose between the peer and join handshake patterns.
pub async fn server_handshake_peer_with_first_frame(
    rd: OwnedReadHalf,
    mut wr: OwnedWriteHalf,
    keys: &NoiseKeys,
    first_frame: &[u8],
    verifier: Rc<dyn NoisePeerVerifier>,
) -> Result<NoiseStream, PeerHandshakeError> {
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
        .into_stateless_transport_mode()
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(NoiseStream::new(rd, wr, transport))
}

/// Select the correct server-side handshake by inspecting the first frame.
///
/// The listener tries the peer IK pattern first because it is the steady-state
/// path for existing members. When that does not match, it falls back to the
/// token-authenticated join handshake.
pub async fn server_handshake_select(
    rd: OwnedReadHalf,
    wr: OwnedWriteHalf,
    keys: &NoiseKeys,
    psk: &[u8; 32],
    first_frame: &[u8],
    verifier: Rc<dyn NoisePeerVerifier>,
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
                .into_stateless_transport_mode()
                .map_err(|e| io::Error::other(e.to_string()))?;

            Ok(ServerHandshake {
                stream: NoiseStream::new(rd, wr, transport),
                kind: HandshakeKind::Peer,
                join_probe: false,
            })
        }
        Err(_) => {
            let join = server_handshake_join_with_first_frame_probe(rd, wr, keys, psk, first_frame)
                .await?;
            Ok(ServerHandshake {
                stream: join.stream,
                kind: HandshakeKind::Join,
                join_probe: join.probe_required,
            })
        }
    }
}
