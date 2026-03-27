use crate::paths::{ensure_mantissa_group, ensure_state_dir, running_as_root};
use async_trait::async_trait;
use getrandom::getrandom;
use hkdf::Hkdf;
use sha2::Sha256;
use std::cmp::min;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fs, io};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::{Duration, timeout};
use tracing::{debug, warn};
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
const MAX_WIRE_FRAME: usize = u16::MAX as usize;
const NOISE_TRANSPORT_OVERHEAD: usize = 16;
const MAX_TRANSPORT_PLAINTEXT_FRAME: usize = MAX_WIRE_FRAME - NOISE_TRANSPORT_OVERHEAD;
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
    pub stream: NoiseStream,
    pub kind: HandshakeKind,
    pub join_probe: bool,
}

/// Client-side join handshake result, including whether the server supports probes.
pub struct ClientJoinHandshake {
    pub stream: NoiseStream,
    pub probe_enabled: bool,
}

struct ServerJoinHandshake {
    stream: NoiseStream,
    probe_required: bool,
}

/// Direct Noise transport stream backed by the TCP socket halves.
///
/// This keeps application bytes on the caller's I/O path instead of bouncing
/// them through an in-process duplex bridge and background tasks.
pub struct NoiseStream {
    reader: NoiseReadHalf,
    writer: NoiseWriteHalf,
}

/// Read half of one Noise transport session.
///
/// It owns the TCP read half, frame parsing state, and the inbound nonce.
pub struct NoiseReadHalf {
    tcp_reader: OwnedReadHalf,
    transport: Arc<snow::StatelessTransportState>,
    next_nonce: u64,
    len_prefix: [u8; 2],
    len_prefix_filled: usize,
    cipher_buf: Vec<u8>,
    cipher_len: usize,
    cipher_read: usize,
    plain_buf: Vec<u8>,
    plain_len: usize,
    plain_offset: usize,
}

/// Write half of one Noise transport session.
///
/// It owns the TCP write half, the outbound nonce, one staged plaintext frame,
/// and one encrypted frame waiting to hit the socket.
pub struct NoiseWriteHalf {
    tcp_writer: OwnedWriteHalf,
    transport: Arc<snow::StatelessTransportState>,
    next_nonce: u64,
    staged_plain: Vec<u8>,
    staged_plain_len: usize,
    wire_buf: Vec<u8>,
    wire_len: usize,
    wire_written: usize,
}

impl NoiseStream {
    /// Build one direct Noise stream from the TCP halves and the finished
    /// stateless transport state derived from the handshake.
    fn new(
        tcp_reader: OwnedReadHalf,
        tcp_writer: OwnedWriteHalf,
        transport: snow::StatelessTransportState,
    ) -> Self {
        let transport = Arc::new(transport);
        Self {
            reader: NoiseReadHalf {
                tcp_reader,
                transport: transport.clone(),
                next_nonce: 0,
                len_prefix: [0u8; 2],
                len_prefix_filled: 0,
                cipher_buf: vec![0u8; MAX_WIRE_FRAME],
                cipher_len: 0,
                cipher_read: 0,
                plain_buf: vec![0u8; MAX_TRANSPORT_PLAINTEXT_FRAME],
                plain_len: 0,
                plain_offset: 0,
            },
            writer: NoiseWriteHalf {
                tcp_writer,
                transport,
                next_nonce: 0,
                staged_plain: vec![0u8; MAX_TRANSPORT_PLAINTEXT_FRAME],
                staged_plain_len: 0,
                wire_buf: vec![0u8; MAX_WIRE_FRAME + 2],
                wire_len: 0,
                wire_written: 0,
            },
        }
    }

    /// Split the direct Noise stream into independent read and write halves.
    ///
    /// The RPC layer uses this so Cap'n Proto can drive both directions without
    /// an extra Tokio `split()` lock around the whole transport.
    pub fn into_split(self) -> (NoiseReadHalf, NoiseWriteHalf) {
        (self.reader, self.writer)
    }
}

impl AsyncRead for NoiseStream {
    /// Read decrypted plaintext directly from the underlying TCP socket.
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for NoiseStream {
    /// Stage plaintext for encryption and flush encrypted frames directly to
    /// the underlying TCP socket as capacity requires.
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    /// Flush any staged plaintext and encrypted bytes to the TCP socket.
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    /// Flush pending bytes and shut down the TCP write half.
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}

impl AsyncRead for NoiseReadHalf {
    /// Read decrypted bytes into the caller-provided buffer.
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        loop {
            if this.plain_offset < this.plain_len {
                let available = this.plain_len - this.plain_offset;
                let to_copy = min(available, buf.remaining());
                buf.put_slice(&this.plain_buf[this.plain_offset..this.plain_offset + to_copy]);
                this.plain_offset += to_copy;
                if this.plain_offset == this.plain_len {
                    this.plain_offset = 0;
                    this.plain_len = 0;
                }
                return Poll::Ready(Ok(()));
            }

            match poll_fill_reader(
                Pin::new(&mut this.tcp_reader),
                cx,
                &mut this.len_prefix,
                &mut this.len_prefix_filled,
                "stream.read.len_prefix",
                true,
            ) {
                Poll::Ready(Ok(false)) => return Poll::Ready(Ok(())),
                Poll::Ready(Ok(true)) => {}
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            }

            this.cipher_len = u16::from_be_bytes(this.len_prefix) as usize;
            if this.cipher_len > MAX_WIRE_FRAME {
                warn!(
                    target: "diag.transport",
                    direction = "read",
                    stage = "stream.read.frame_too_large",
                    frame_len = this.cipher_len,
                    max_wire_frame = MAX_WIRE_FRAME,
                    "noise ciphertext exceeds wire frame limit"
                );
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "noise frame too large",
                )));
            }

            match poll_fill_reader(
                Pin::new(&mut this.tcp_reader),
                cx,
                &mut this.cipher_buf[..this.cipher_len],
                &mut this.cipher_read,
                "stream.read.frame",
                false,
            ) {
                Poll::Ready(Ok(true)) => {}
                Poll::Ready(Ok(false)) => unreachable!("frame reads do not allow clean EOF"),
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            }

            let nonce = next_nonce(&mut this.next_nonce)?;
            let n_plain = match this.transport.read_message(
                nonce,
                &this.cipher_buf[..this.cipher_len],
                &mut this.plain_buf,
            ) {
                Ok(n) => n,
                Err(err) => {
                    warn!(
                        target: "diag.transport",
                        direction = "read",
                        stage = "stream.decrypt",
                        frame_len = this.cipher_len,
                        error = %err,
                        "noise decrypt failed"
                    );
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("noise decrypt failed: {err}"),
                    )));
                }
            };

            this.len_prefix_filled = 0;
            this.cipher_len = 0;
            this.cipher_read = 0;
            this.plain_len = n_plain;
            this.plain_offset = 0;

            if this.plain_len == 0 {
                continue;
            }
        }
    }
}

impl NoiseWriteHalf {
    /// Returns true when one encrypted frame is still being written to the TCP
    /// socket.
    fn has_pending_wire(&self) -> bool {
        self.wire_written < self.wire_len
    }

    /// Encrypt one caller-provided plaintext slice into the pending wire
    /// buffer.
    fn prepare_pending_frame_from_slice(&mut self, plaintext: &[u8]) -> io::Result<()> {
        debug_assert!(!self.has_pending_wire());

        let nonce = next_nonce(&mut self.next_nonce)?;
        let cipher_len =
            match self
                .transport
                .write_message(nonce, plaintext, &mut self.wire_buf[2..])
            {
                Ok(n) => n,
                Err(err) => {
                    warn!(
                        target: "diag.transport",
                        direction = "write",
                        stage = "stream.encrypt",
                        plain_len = plaintext.len(),
                        error = %err,
                        "noise encrypt failed"
                    );
                    return Err(io::Error::other(format!("noise encrypt failed: {err}")));
                }
            };

        if cipher_len > MAX_WIRE_FRAME {
            warn!(
                target: "diag.transport",
                direction = "write",
                stage = "stream.encrypt.frame_too_large",
                plain_len = plaintext.len(),
                cipher_len,
                max_wire_frame = MAX_WIRE_FRAME,
                "noise ciphertext exceeds wire frame limit"
            );
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "noise ciphertext exceeds wire frame limit",
            ));
        }

        self.wire_buf[..2].copy_from_slice(&(cipher_len as u16).to_be_bytes());
        self.wire_len = cipher_len + 2;
        self.wire_written = 0;
        Ok(())
    }

    /// Encrypt the staged plaintext buffer into the pending wire buffer.
    fn prepare_pending_frame_from_staged(&mut self) -> io::Result<()> {
        if self.staged_plain_len == 0 {
            return Ok(());
        }

        let plain_len = self.staged_plain_len;
        let nonce = next_nonce(&mut self.next_nonce)?;
        let cipher_len = match self.transport.write_message(
            nonce,
            &self.staged_plain[..plain_len],
            &mut self.wire_buf[2..],
        ) {
            Ok(n) => n,
            Err(err) => {
                warn!(
                    target: "diag.transport",
                    direction = "write",
                    stage = "stream.encrypt",
                    plain_len,
                    error = %err,
                    "noise encrypt failed"
                );
                return Err(io::Error::other(format!("noise encrypt failed: {err}")));
            }
        };

        if cipher_len > MAX_WIRE_FRAME {
            warn!(
                target: "diag.transport",
                direction = "write",
                stage = "stream.encrypt.frame_too_large",
                plain_len,
                cipher_len,
                max_wire_frame = MAX_WIRE_FRAME,
                "noise ciphertext exceeds wire frame limit"
            );
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "noise ciphertext exceeds wire frame limit",
            ));
        }

        self.wire_buf[..2].copy_from_slice(&(cipher_len as u16).to_be_bytes());
        self.wire_len = cipher_len + 2;
        self.wire_written = 0;
        self.staged_plain_len = 0;
        Ok(())
    }

    /// Push the pending encrypted frame to the TCP socket.
    fn poll_drain_pending_wire(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.has_pending_wire() {
            match Pin::new(&mut self.tcp_writer)
                .poll_write(cx, &self.wire_buf[self.wire_written..self.wire_len])
            {
                Poll::Ready(Ok(0)) => {
                    let err = io::Error::new(
                        io::ErrorKind::WriteZero,
                        "noise transport write returned 0",
                    );
                    log_transport_io("stream.write.frame", "write", &err);
                    return Poll::Ready(Err(err));
                }
                Poll::Ready(Ok(written)) => {
                    self.wire_written += written;
                }
                Poll::Ready(Err(err)) => {
                    log_transport_io("stream.write.frame", "write", &err);
                    return Poll::Ready(Err(err));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        self.wire_len = 0;
        self.wire_written = 0;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for NoiseWriteHalf {
    /// Stage plaintext and only encrypt immediately when a full frame is ready
    /// or the caller provided a full frame directly.
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        // Bound the buffered state to one encrypted frame plus one staged
        // plaintext frame. If both are full we must make forward progress on
        // the socket before accepting more bytes.
        if this.staged_plain_len == this.staged_plain.len() {
            if this.has_pending_wire() {
                match this.poll_drain_pending_wire(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => return Poll::Pending,
                }
            }
            this.prepare_pending_frame_from_staged()?;
        }

        // Large writes can skip the staging buffer entirely once the current
        // pending frame has drained.
        if this.staged_plain_len == 0
            && !this.has_pending_wire()
            && buf.len() >= MAX_TRANSPORT_PLAINTEXT_FRAME
        {
            this.prepare_pending_frame_from_slice(&buf[..MAX_TRANSPORT_PLAINTEXT_FRAME])?;
            let _ = this.poll_drain_pending_wire(cx);
            return Poll::Ready(Ok(MAX_TRANSPORT_PLAINTEXT_FRAME));
        }

        let capacity = this.staged_plain.len() - this.staged_plain_len;
        if capacity == 0 {
            return Poll::Pending;
        }

        let to_copy = min(capacity, buf.len());
        this.staged_plain[this.staged_plain_len..this.staged_plain_len + to_copy]
            .copy_from_slice(&buf[..to_copy]);
        this.staged_plain_len += to_copy;

        if this.staged_plain_len == this.staged_plain.len() && !this.has_pending_wire() {
            this.prepare_pending_frame_from_staged()?;
            let _ = this.poll_drain_pending_wire(cx);
        }

        Poll::Ready(Ok(to_copy))
    }

    /// Encrypt any staged plaintext and flush all bytes to the socket.
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        loop {
            if this.has_pending_wire() {
                match this.poll_drain_pending_wire(cx) {
                    Poll::Ready(Ok(())) => continue,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => return Poll::Pending,
                }
            }

            if this.staged_plain_len > 0 {
                this.prepare_pending_frame_from_staged()?;
                continue;
            }

            match Pin::new(&mut this.tcp_writer).poll_flush(cx) {
                Poll::Ready(Ok(())) => return Poll::Ready(Ok(())),
                Poll::Ready(Err(err)) => {
                    log_transport_io("stream.flush", "write", &err);
                    return Poll::Ready(Err(err));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Flush pending bytes before shutting down the TCP write half.
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.as_mut().poll_flush(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        }

        let this = self.as_mut().get_mut();
        match Pin::new(&mut this.tcp_writer).poll_shutdown(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => {
                log_transport_io("stream.shutdown", "write", &err);
                Poll::Ready(Err(err))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Advance one monotonically increasing Noise nonce and fail once the session
/// exhausts the transport counter space.
fn next_nonce(next: &mut u64) -> io::Result<u64> {
    let nonce = *next;
    *next = next
        .checked_add(1)
        .ok_or_else(|| io::Error::other("noise nonce exhausted"))?;
    Ok(nonce)
}

/// Fill the caller-provided slice directly from the TCP reader.
///
/// When `allow_clean_eof` is true, reaching EOF before any bytes of the next
/// frame have arrived cleanly ends the stream instead of surfacing an error.
fn poll_fill_reader<R>(
    mut reader: Pin<&mut R>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
    filled: &mut usize,
    stage: &'static str,
    allow_clean_eof: bool,
) -> Poll<io::Result<bool>>
where
    R: AsyncRead,
{
    while *filled < buf.len() {
        let mut read_buf = ReadBuf::new(&mut buf[*filled..]);
        match reader.as_mut().poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                let read = read_buf.filled().len();
                if read == 0 {
                    if *filled == 0 && allow_clean_eof {
                        return Poll::Ready(Ok(false));
                    }
                    let err = io::Error::new(io::ErrorKind::UnexpectedEof, "noise frame truncated");
                    log_transport_io(stage, "read", &err);
                    return Poll::Ready(Err(err));
                }
                *filled += read;
            }
            Poll::Ready(Err(err)) => {
                log_transport_io(stage, "read", &err);
                return Poll::Ready(Err(err));
            }
            Poll::Pending => return Poll::Pending,
        }
    }

    Poll::Ready(Ok(true))
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

    // Done: switch to transport and return a direct encrypted stream.
    let transport = hs
        .into_stateless_transport_mode()
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(ClientJoinHandshake {
        stream: NoiseStream::new(rd, wr, transport),
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
) -> io::Result<NoiseStream> {
    Ok(client_handshake_join_with_probe(tcp, keys, psk)
        .await?
        .stream)
}

/// Confirm the join PSK on the client side by round-tripping a short probe.
/// This ensures an invalid join token fails before Cap'n Proto setup.
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

/// Confirm the join PSK on the server side by validating a short probe and responding.
/// This rejects invalid tokens before Cap'n Proto setup.
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

/// Client side handshake for authenticated peer-to-peer connections.
/// Uses Noise IK to authenticate the responder's static key and reveal our static key.
pub async fn client_handshake_peer(
    tcp: tokio::net::TcpStream,
    keys: &crate::noise::NoiseKeys,
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
        .into_stateless_transport_mode()
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(NoiseStream::new(rd, wr, transport))
}

/// Server side handshake.
/// Handshake (Noise_XXpsk3) with length-prefixed frames and a PSK for authentication.
/// Returns a duplex stream bridged through the Noise transport.
pub async fn server_handshake_join(
    tcp: tokio::net::TcpStream,
    keys: &crate::noise::NoiseKeys,
    psk: &[u8; 32],
) -> io::Result<NoiseStream> {
    let (mut rd, wr) = tcp.into_split();
    let mut first = vec![0u8; 65535];
    let nread = read_framed_len(&mut rd, &mut first).await?;
    server_handshake_join_with_first_frame(rd, wr, keys, psk, &first[..nread]).await
}

/// Server side join handshake (Noise_XXpsk3) using a pre-read first frame.
pub async fn server_handshake_join_with_first_frame(
    rd: OwnedReadHalf,
    wr: OwnedWriteHalf,
    keys: &crate::noise::NoiseKeys,
    psk: &[u8; 32],
    first_frame: &[u8],
) -> io::Result<NoiseStream> {
    Ok(
        server_handshake_join_with_first_frame_probe(rd, wr, keys, psk, first_frame)
            .await?
            .stream,
    )
}

/// Server side join handshake (Noise_XXpsk3) using a pre-read first frame.
/// Returns the Noise stream plus whether the client requested the join probe.
async fn server_handshake_join_with_first_frame_probe(
    mut rd: OwnedReadHalf,
    mut wr: OwnedWriteHalf,
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
        .into_stateless_transport_mode()
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(ServerJoinHandshake {
        stream: NoiseStream::new(rd, wr, transport),
        probe_required,
    })
}

/// Server side peer handshake (Noise IK) using a pre-read first frame.
pub async fn server_handshake_peer_with_first_frame(
    rd: OwnedReadHalf,
    mut wr: OwnedWriteHalf,
    keys: &crate::noise::NoiseKeys,
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
    rd: OwnedReadHalf,
    wr: OwnedWriteHalf,
    keys: &crate::noise::NoiseKeys,
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

/// # Description:
///
/// Returns true when one I/O error kind maps to an expected disconnect condition.
fn is_expected_disconnect(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
    )
}

/// # Description:
///
/// Emits one transport diagnostic log with consistent fields so disconnect patterns can be
/// extracted from large cluster logs with target-based filters.
fn log_transport_io(stage: &'static str, direction: &'static str, err: &io::Error) {
    if is_expected_disconnect(err.kind()) {
        debug!(
            target: "diag.transport",
            direction = direction,
            stage = stage,
            error_kind = ?err.kind(),
            error = %err,
            "noise transport disconnected"
        );
    } else {
        warn!(
            target: "diag.transport",
            direction = direction,
            stage = stage,
            error_kind = ?err.kind(),
            error = %err,
            "noise transport I/O error"
        );
    }
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
    if let Err(e) = rd.read_exact(&mut len).await {
        log_transport_io("handshake.read.len_prefix", "read", &e);
        return Err(e);
    }
    let n = u16::from_be_bytes(len) as usize;

    if n > MAX_FRAME {
        warn!(
            target: "diag.transport",
            direction = "read",
            stage = "handshake.read.frame_too_large",
            frame_len = n,
            max_frame = MAX_FRAME,
            "noise framed payload exceeded MAX_FRAME"
        );
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "handshake frame too large",
        ));
    }

    if buf.len() < n {
        buf.resize(n, 0);
    }
    if let Err(e) = rd.read_exact(&mut buf[..n]).await {
        log_transport_io("handshake.read.frame", "read", &e);
        return Err(e);
    }
    Ok(n)
}

pub async fn write_framed<W>(wr: &mut W, data: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if data.len() > u16::MAX as usize {
        warn!(
            target: "diag.transport",
            direction = "write",
            stage = "handshake.write.frame_too_large",
            frame_len = data.len(),
            max_frame = u16::MAX as usize,
            "noise framed payload exceeded u16::MAX"
        );
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let len = (data.len() as u16).to_be_bytes();
    if let Err(e) = wr.write_all(&len).await {
        log_transport_io("handshake.write.len_prefix", "write", &e);
        return Err(e);
    }
    if let Err(e) = wr.write_all(data).await {
        log_transport_io("handshake.write.frame", "write", &e);
        return Err(e);
    }
    if let Err(e) = wr.flush().await {
        log_transport_io("handshake.flush", "write", &e);
        return Err(e);
    }
    Ok(())
}
