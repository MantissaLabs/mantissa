use std::sync::Arc;
use tokio::sync::RwLock;

/// Simple, rotateable join-token store.
/// - `new(token)` to initialize
/// - `set(token)` to rotate (invalidates previous immediately)
/// - `get()` to read current token
/// - `matches(candidate)` to validate a client-provided token
#[derive(Clone, Default)]
pub struct TokenStore {
    inner: Arc<RwLock<String>>,
}

impl TokenStore {
    /// Initialize with a token (can be empty if you want to reject all joins until set)
    pub fn new(initial: String) -> Self {
        Self {
            inner: Arc::new(RwLock::new(initial)),
        }
    }

    /// Get the current token (clone)
    pub async fn get(&self) -> String {
        self.inner.read().await.clone()
    }

    /// Set/rotate the token; immediately invalidates the previous token for *new connections*.
    pub async fn set(&self, new_token: String) {
        *self.inner.write().await = new_token;
    }

    /// Validate a candidate token.
    pub async fn matches(&self, candidate: &str) -> bool {
        *self.inner.read().await == candidate
    }
}
