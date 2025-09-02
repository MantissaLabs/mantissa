// Noise is self-contained.
#[path = "noise.rs"]
pub mod noise;

// LocalSessionStore is self-contained (depends only on `noise` + external crates).
#[path = "store/local_session_store.rs"]
pub mod local_session_store;

// Credentials live under server/, but we can export that file directly
// as a top-level `credential` module to avoid building all of `server/`.
#[path = "server/credential.rs"]
pub mod credential;

// New: allow AuthStore tests to compile (needs crypto::rand).
#[path = "crypto/mod.rs"]
pub mod crypto;

#[path = "server/auth.rs"]
pub mod auth;
