use data_encoding::Specification;
use getrandom::getrandom;
use std::sync::Arc;
use tokio::sync::RwLock;

const TOKEN_PREFIX: &str = "MNTSSA-1-";

#[derive(Clone, Default)]
pub struct TokenStore {
    inner: Arc<RwLock<String>>,
}

impl TokenStore {
    /// Initialize with an optional existing token. `None`/empty means no valid token yet.
    pub fn new(initial: Option<String>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(initial.unwrap_or_default())),
        }
    }

    /// Generate a brand-new token `MNTSSA-1-<base32lower_nopad>` and store it.
    pub async fn generate(&self) -> String {
        let token = generate_token();
        *self.inner.write().await = token.clone();
        token
    }

    /// Rotate to a new token (alias of `generate`).
    pub async fn rotate(&self) -> String {
        self.generate().await
    }

    /// Current token if set.
    pub async fn current(&self) -> Option<String> {
        let s = self.inner.read().await.clone();
        if s.is_empty() {
            None
        } else {
            Some(s)
        }
    }

    /// Validate a candidate token against the current one.
    pub async fn matches(&self, candidate: &str) -> bool {
        let cur = self.inner.read().await.clone();
        !cur.is_empty() && cur == candidate
    }
}

/// Produce `MNTSSA-1-<longtoken>` where `<longtoken>` is base32 (lowercase, no padding).
fn generate_token() -> String {
    let mut bytes = [0u8; 48];
    getrandom(&mut bytes).expect("CSPRNG failed");

    let mut spec = Specification::new();
    spec.symbols.push_str("abcdefghijklmnopqrstuvwxyz234567");
    spec.padding = None;
    let enc = spec.encoding().expect("valid base32 spec");

    let encoded = enc.encode(&bytes);
    format!("{TOKEN_PREFIX}{encoded}")
}

/// Optional format check (lowercase base32, no padding).
pub fn is_valid_format(token: &str) -> bool {
    if !token.starts_with(TOKEN_PREFIX) {
        return false;
    }
    let rest = &token[TOKEN_PREFIX.len()..];
    !rest.is_empty()
        && rest
            .bytes()
            .all(|b| (b'a'..=b'z').contains(&b) || (b'2'..=b'7').contains(&b))
}
