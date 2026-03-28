//! Noise handshake and transport support for Mantissa's TCP control plane.
//!
//! The public API stays flat at `net::noise::*`, while the implementation is
//! split by responsibility so the transport, handshake, framing, and key
//! persistence paths can evolve independently.

mod diagnostics;
mod framing;
mod handshake;
mod keys;
mod transport;

pub use framing::{read_framed_len, write_framed};
pub use handshake::{
    ClientJoinHandshake, HandshakeKind, NoisePeerVerifier, NoisePskProvider, PeerHandshakeError,
    ServerHandshake, ServerHandshakeError, client_handshake_join, client_handshake_join_with_probe,
    client_handshake_peer, derive_psk_from_token, join_probe_client, join_probe_server,
    server_handshake_join, server_handshake_join_with_first_frame,
    server_handshake_peer_with_first_frame, server_handshake_select,
};
pub use keys::{NoiseKeys, load_or_generate_noise_keys, resolve_noise_key_path};
pub use transport::{NoiseReadHalf, NoiseStream, NoiseWriteHalf};

use std::io;
use tokio::time::Duration;

/// Noise pattern used when a node joins the cluster with a token-derived PSK.
pub(crate) const NOISE_PARAMS_JOIN: &str = "Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s";

/// Noise pattern used for steady-state authenticated peer-to-peer traffic.
pub(crate) const NOISE_PARAMS_PEER: &str = "Noise_IK_25519_ChaChaPoly_BLAKE2s";

/// Maximum handshake frame size accepted by the length-prefixed helpers.
pub(crate) const MAX_FRAME: usize = 64 * 1024;

/// Maximum encrypted transport frame size after the two-byte wire prefix.
pub(crate) const MAX_WIRE_FRAME: usize = u16::MAX as usize;

/// Authenticated-encryption overhead added by the transport cipher.
pub(crate) const NOISE_TRANSPORT_OVERHEAD: usize = 16;

/// Largest plaintext payload that still fits in one encrypted wire frame.
pub(crate) const MAX_TRANSPORT_PLAINTEXT_FRAME: usize = MAX_WIRE_FRAME - NOISE_TRANSPORT_OVERHEAD;

/// HKDF salt used when converting a join token into a Noise PSK.
pub(crate) const TOKEN_PSK_SALT: &[u8] = b"mantissa/noise-psk-salt/v1";

/// HKDF info string used when converting a join token into a Noise PSK.
pub(crate) const TOKEN_PSK_INFO: &[u8] = b"mantissa/noise-psk-info/v1";

/// PSK slot mandated by the `XXpsk3` Noise pattern.
pub(crate) const TOKEN_PSK_LOCATION: u8 = 3;

/// Client probe request sent over an established join stream.
pub(crate) const JOIN_PROBE_REQ: &[u8; 8] = b"MNTJNP01";

/// Server probe response sent over an established join stream.
pub(crate) const JOIN_PROBE_RESP: &[u8; 8] = b"MNTJNP02";

/// Probe-capable client payload attached to the first join handshake message.
pub(crate) const JOIN_PROBE_HELLO: &[u8; 8] = b"MNTJNH01";

/// Probe-capable server payload attached to the join handshake response.
pub(crate) const JOIN_PROBE_ACK: &[u8; 8] = b"MNTJNA01";

/// Upper bound for join-probe round trips during capability negotiation.
pub(crate) const JOIN_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Parse and validate one configured Noise parameter string.
///
/// This keeps the handshake builders on both client and server paths aligned.
pub(crate) fn parsed_noise_params(params: &str) -> io::Result<snow::params::NoiseParams> {
    params
        .parse()
        .map_err(|e| io::Error::other(format!("invalid noise params: {e}")))
}

/// Return the fixed Mantissa handshake prologue shared by every Noise pattern.
///
/// The join token is intentionally not part of the prologue because it is
/// already incorporated into the dedicated PSK slot.
pub(crate) fn prologue() -> &'static [u8] {
    b"MANTISSA|v1"
}
