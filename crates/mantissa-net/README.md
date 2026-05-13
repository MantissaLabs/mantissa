# mantissa-net

Networking utilities for Mantissa control-plane transports.

This crate contains the lower-level pieces used by Mantissa clients and servers:
Noise handshakes, Cap'n Proto-compatible secure streams, Unix socket helpers,
in-process test transport, and WireGuard path utilities.

## Modules

- `noise`: Noise key management, PSK derivation, join handshakes, peer
  handshakes, framing, and encrypted stream types.
- `tcp_secure`: secure TCP listener setup for Mantissa node RPC.
- `unix_socket`: local admin socket path discovery and Unix listener helpers.
- `inproc`: in-process client registry used by tests and headless nodes.
- `wireguard`: helpers for WireGuard overlay configuration.
- `paths`: shared state-directory and socket path resolution.

## Example

Derive the join PSK used by the join transport:

```rust
use mantissa_net::noise::derive_psk_from_token;

fn main() -> std::io::Result<()> {
    let psk = derive_psk_from_token("join-token-value")?;
    assert_eq!(psk.len(), 32);
    Ok(())
}
```

Load or create the local Noise identity:

```rust,no_run
use mantissa_net::noise::{load_or_generate_noise_keys, resolve_noise_key_path};

fn main() -> std::io::Result<()> {
    let path = resolve_noise_key_path()?;
    let keys = load_or_generate_noise_keys(path)?;
    println!("local static key: {:?}", keys.public_bytes());
    Ok(())
}
```

## Consumer Guidance

Most application code should use `mantissa-client` or the main Mantissa server
runtime instead of calling this crate directly. Use `mantissa-net` directly when
building transport integrations, tests, or low-level node communication code.
