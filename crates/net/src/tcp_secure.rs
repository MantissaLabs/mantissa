use crate::noise::{HandshakeKind, NoisePeerVerifier, NoisePskProvider};
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tracing::{error, info, warn};

// Global cap on concurrent handshakes to keep CPU usage predictable during floods.
const MAX_HANDSHAKES: usize = 64;
// Per-IP token bucket settings for unauthenticated join/unknown attempts.
const RATE_LIMIT_BURST: f32 = 20.0;
const RATE_LIMIT_REFILL_PER_SEC: f32 = 5.0;
const RATE_LIMIT_MAX_ENTRIES: usize = 10_000;
const RATE_LIMIT_TTL: Duration = Duration::from_secs(60);
const RATE_LIMIT_PRUNE_INTERVAL: Duration = Duration::from_secs(10);

// Stricter limiter for repeated invalid join tokens (aggressive throttling).
const INVALID_TOKEN_BURST: f32 = 10.0;
const INVALID_TOKEN_REFILL_PER_SEC: f32 = 0.0;
const INVALID_TOKEN_TTL: Duration = Duration::from_secs(60);

struct RateState {
    tokens: f32,
    last_refill: Instant,
    last_seen: Instant,
}

/// Bounded, in-memory token bucket limiter for unauthenticated handshakes.
/// This keeps CPU and memory usage predictable during join floods.
struct RateLimiter {
    entries: HashMap<IpAddr, RateState>,
    max_entries: usize,
    burst: f32,
    refill_per_sec: f32,
    ttl: Duration,
    prune_interval: Duration,
    last_prune: Instant,
}

impl RateLimiter {
    /// Create a bounded token bucket limiter tuned for unauthenticated joins.
    /// This provides cheap DoS resistance without persistence or disk IO.
    fn new(
        max_entries: usize,
        burst: f32,
        refill_per_sec: f32,
        ttl: Duration,
        prune_interval: Duration,
    ) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries,
            burst,
            refill_per_sec,
            ttl,
            prune_interval,
            last_prune: Instant::now(),
        }
    }

    /// Decide whether to allow a new unauthenticated attempt from `ip`.
    /// This is used to throttle join/unknown peers while leaving authenticated traffic untouched.
    fn allow(&mut self, ip: IpAddr, now: Instant) -> bool {
        self.prune(now);

        if !self.entries.contains_key(&ip) && self.entries.len() >= self.max_entries {
            self.evict_oldest();
        }

        let entry = self.entries.entry(ip).or_insert_with(|| RateState {
            tokens: self.burst,
            last_refill: now,
            last_seen: now,
        });

        let elapsed = now.duration_since(entry.last_refill).as_secs_f32();
        entry.tokens = (entry.tokens + elapsed * self.refill_per_sec).min(self.burst);
        entry.last_refill = now;
        entry.last_seen = now;

        if entry.tokens < 1.0 {
            return false;
        }

        entry.tokens -= 1.0;
        true
    }

    /// Trim stale entries to keep memory usage bounded and predictable.
    fn prune(&mut self, now: Instant) {
        if now.duration_since(self.last_prune) < self.prune_interval {
            return;
        }
        self.entries
            .retain(|_, state| now.duration_since(state.last_seen) <= self.ttl);
        self.last_prune = now;
    }

    /// Evict the least recently seen entry when at capacity.
    fn evict_oldest(&mut self) {
        if let Some((oldest, _)) = self.entries.iter().min_by_key(|(_, state)| state.last_seen) {
            let key = *oldest;
            self.entries.remove(&key);
        }
    }
}

/// Accept-loop used by both blocking and non-blocking variants.
async fn accept_loop(
    listener: TcpListener,
    server_handle: protocol::server::server::Client,
    noise_keys: Arc<crate::noise::NoiseKeys>,
    psk_provider: Arc<dyn NoisePskProvider>,
    peer_verifier: Arc<dyn NoisePeerVerifier>,
) {
    let handshake_semaphore = Arc::new(Semaphore::new(MAX_HANDSHAKES));
    let rate_limiter = Arc::new(AsyncMutex::new(RateLimiter::new(
        RATE_LIMIT_MAX_ENTRIES,
        RATE_LIMIT_BURST,
        RATE_LIMIT_REFILL_PER_SEC,
        RATE_LIMIT_TTL,
        RATE_LIMIT_PRUNE_INTERVAL,
    )));
    let invalid_token_limiter = Arc::new(AsyncMutex::new(RateLimiter::new(
        RATE_LIMIT_MAX_ENTRIES,
        INVALID_TOKEN_BURST,
        INVALID_TOKEN_REFILL_PER_SEC,
        INVALID_TOKEN_TTL,
        RATE_LIMIT_PRUNE_INTERVAL,
    )));

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                error!(target: "server", "TCP accept error: {e}");
                continue;
            }
        };
        let peer_ip = peer.ip();

        let permit = match handshake_semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                warn!(
                    target: "server",
                    "dropping connection from {peer_ip} due to handshake limit"
                );
                continue;
            }
        };

        if let Err(e) = stream.set_nodelay(true) {
            warn!(target: "server", "set_nodelay failed: {e}");
        }

        let server_handle_clone = server_handle.clone();
        let keys = noise_keys.clone();
        let psk_provider = psk_provider.clone();
        let peer_verifier = peer_verifier.clone();
        let rate_limiter = rate_limiter.clone();
        let invalid_token_limiter = invalid_token_limiter.clone();

        tokio::task::spawn_local(async move {
            let _permit = permit;
            let psk = match psk_provider.psk().await {
                Ok(psk) => psk,
                Err(e) => {
                    error!(target: "server", "Noise PSK derivation failed: {e}");
                    return;
                }
            };

            let (mut rd, wr) = stream.into_split();
            let mut first = vec![0u8; 65535];
            let nread = match crate::noise::read_framed_len(&mut rd, &mut first).await {
                Ok(n) => n,
                Err(e) => {
                    error!(target: "server", "Noise handshake read failed: {e}");
                    return;
                }
            };

            match crate::noise::server_handshake_select(
                rd,
                wr,
                &keys,
                &psk,
                &first[..nread],
                peer_verifier,
            )
            .await
            {
                Ok(mut handshake) => {
                    if matches!(handshake.kind, HandshakeKind::Join) {
                        let allowed = rate_limiter.lock().await.allow(peer_ip, Instant::now());
                        if !allowed {
                            warn!(
                                target: "server",
                                "rate-limited join attempt from {peer_ip}"
                            );
                            return;
                        }
                    }

                    if matches!(handshake.kind, HandshakeKind::Join) && handshake.join_probe {
                        if let Err(e) = crate::noise::join_probe_server(&mut handshake.stream).await
                        {
                            warn!(target: "server", "Noise join probe failed: {e}");
                            return;
                        }
                    }

                    let (reader, writer) =
                        tokio_util::compat::TokioAsyncReadCompatExt::compat(handshake.stream)
                            .split();

                    let network = twoparty::VatNetwork::new(
                        futures::io::BufReader::new(reader),
                        futures::io::BufWriter::new(writer),
                        rpc_twoparty_capnp::Side::Server,
                        Default::default(),
                    );

                    let rpc_system =
                        RpcSystem::new(Box::new(network), Some(server_handle_clone.client));

                    if let Err(e) = rpc_system.await {
                        error!(target: "server", "TCP secure RPC error: {e}");
                    }
                }
                Err(crate::noise::ServerHandshakeError::UnknownPeer) => {
                    let allowed = rate_limiter.lock().await.allow(peer_ip, Instant::now());
                    if !allowed {
                        warn!(
                            target: "server",
                            "rate-limited unknown peer from {peer_ip}"
                        );
                        return;
                    }
                    warn!(target: "server", "Noise peer rejected: unknown static key");
                }
                Err(crate::noise::ServerHandshakeError::Io(e)) => {
                    if e.to_string() == "invalid join token" {
                        let allowed = invalid_token_limiter
                            .lock()
                            .await
                            .allow(peer_ip, Instant::now());
                        if !allowed {
                            warn!(
                                target: "server",
                                "rate-limited invalid join token from {peer_ip}"
                            );
                            return;
                        }
                        warn!(target: "server", "Noise join handshake failed: invalid join token");
                    } else {
                        error!(target: "server", "Noise handshake failed: {e}");
                    }
                }
            }
        });
    }
}

/// **Blocking**: runs the accept loop on the current task until error/abort.
/// (compat: unchanged signature/behavior)
pub async fn start_tcp_secure_listener(
    listen_addr: String,
    server_handle: protocol::server::server::Client,
    noise_keys: Arc<crate::noise::NoiseKeys>,
    psk_provider: Arc<dyn NoisePskProvider>,
    peer_verifier: Arc<dyn NoisePeerVerifier>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(&listen_addr).await?;
    let bound = listener.local_addr()?;
    info!(target: "server", "Server listening (secure) on {}", bound);
    accept_loop(
        listener,
        server_handle,
        noise_keys,
        psk_provider,
        peer_verifier,
    )
    .await;
    Ok(())
}

/// **Non-Blocking**: spawns the accept loop and returns:
///  - JoinHandle<()> for the loop
///  - oneshot::Receiver<()> that fires once the socket is bound (readiness)
///  - the actual bound SocketAddr (helpful if you passed "127.0.0.1:0")
pub async fn start_tcp_secure_listener_nonblocking_with_ready(
    listen_addr: String,
    server_handle: protocol::server::server::Client,
    noise_keys: Arc<crate::noise::NoiseKeys>,
    psk_provider: Arc<dyn NoisePskProvider>,
    peer_verifier: Arc<dyn NoisePeerVerifier>,
) -> Result<
    (
        tokio::task::JoinHandle<()>,
        tokio::sync::oneshot::Receiver<()>,
        std::net::SocketAddr,
    ),
    Box<dyn std::error::Error>,
> {
    let listener = TcpListener::bind(&listen_addr).await?;
    let bound = listener.local_addr()?;
    info!(target: "server", "Server listening (secure) on {}", bound);

    let (tx, rx) = tokio::sync::oneshot::channel();

    // Move everything into the local task (Cap’n Proto requires !Send)
    let handle = tokio::task::spawn_local(async move {
        // Signal readiness immediately after successful bind.
        let _ = tx.send(());
        accept_loop(
            listener,
            server_handle,
            noise_keys,
            psk_provider,
            peer_verifier,
        )
        .await;
    });

    Ok((handle, rx, bound))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    /// Verifies the token bucket enforces the burst limit without refill.
    #[test]
    fn rate_limiter_enforces_burst_limit() {
        let mut limiter = RateLimiter::new(
            10,
            2.0,
            0.0,
            Duration::from_secs(60),
            Duration::from_secs(0),
        );
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let now = Instant::now();

        assert!(limiter.allow(ip, now));
        assert!(limiter.allow(ip, now));
        assert!(!limiter.allow(ip, now));
    }

    /// Verifies tokens are refilled over time for the same IP.
    #[test]
    fn rate_limiter_refills_tokens() {
        let mut limiter = RateLimiter::new(
            10,
            1.0,
            1.0,
            Duration::from_secs(60),
            Duration::from_secs(0),
        );
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let now = Instant::now();

        assert!(limiter.allow(ip, now));
        assert!(!limiter.allow(ip, now));
        assert!(limiter.allow(ip, now + Duration::from_secs(1)));
    }

    /// Verifies the limiter evicts the oldest entry when at capacity.
    #[test]
    fn rate_limiter_evicts_oldest() {
        let mut limiter =
            RateLimiter::new(2, 1.0, 0.0, Duration::from_secs(60), Duration::from_secs(0));
        let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3));
        let ip2 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4));
        let ip3 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5));

        let base = Instant::now();
        assert!(limiter.allow(ip1, base));
        assert!(limiter.allow(ip2, base + Duration::from_secs(1)));

        // Insert a third IP, forcing eviction.
        assert!(limiter.allow(ip3, base + Duration::from_secs(2)));
        assert_eq!(limiter.entries.len(), 2);
        assert!(!limiter.entries.contains_key(&ip1));
    }

    /// Verifies idle entries are pruned after TTL expiration.
    #[test]
    fn rate_limiter_prunes_expired_entries() {
        let mut limiter =
            RateLimiter::new(10, 1.0, 0.0, Duration::from_secs(1), Duration::from_secs(0));
        let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 6));
        let ip2 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7));

        let base = Instant::now();
        assert!(limiter.allow(ip1, base));
        assert_eq!(limiter.entries.len(), 1);

        // Advance time beyond TTL and trigger prune.
        assert!(limiter.allow(ip2, base + Duration::from_secs(2)));
        assert_eq!(limiter.entries.len(), 1);
        assert!(!limiter.entries.contains_key(&ip1));
    }
}
