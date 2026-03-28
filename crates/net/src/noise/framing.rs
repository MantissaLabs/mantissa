use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::warn;

use super::MAX_FRAME;
use super::diagnostics::log_transport_io;

/// Read one length-prefixed handshake frame into `buf`.
///
/// The frame format is shared by the join and peer handshakes before both
/// sides switch into the streaming Noise transport.
pub async fn read_framed_len<R>(rd: &mut R, buf: &mut Vec<u8>) -> io::Result<usize>
where
    R: AsyncRead + Unpin,
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

/// Write one length-prefixed handshake frame and flush it to the socket.
///
/// Handshake messages stay framed and flush immediately so both sides can
/// drive the finite-state protocol without transport-level buffering surprises.
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
