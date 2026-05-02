use std::cmp::min;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tracing::warn;

use super::diagnostics::log_transport_io;
use super::{MAX_TRANSPORT_PLAINTEXT_FRAME, MAX_WIRE_FRAME};

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
    pub(crate) fn new(
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

/// Advance one monotonically increasing Noise nonce for the transport state.
///
/// This fails once the session exhausts the transport counter space instead of
/// silently wrapping and reusing a nonce.
fn next_nonce(next: &mut u64) -> io::Result<u64> {
    let nonce = *next;
    *next = next
        .checked_add(1)
        .ok_or_else(|| io::Error::other("noise nonce exhausted"))?;
    Ok(nonce)
}

/// Fill `buf` directly from the TCP reader until it is complete.
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
