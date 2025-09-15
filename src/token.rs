use data_encoding::Specification;
use getrandom::getrandom;
use redb::Database;
use std::{io, sync::Arc};
use tokio::sync::RwLock;

use crate::store::local_token_store::LocalTokenStore;

const TOKEN_PREFIX: &str = "MNTISA-1-";

#[derive(Clone, Default)]
pub struct TokenStoreInMemory {
    inner: Arc<RwLock<String>>,
}

impl TokenStoreInMemory {
    /// Initialize with an optional existing token. `None`/empty means no valid token yet.
    pub fn new(initial: Option<String>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(initial.unwrap_or_default())),
        }
    }

    /// Generate a brand-new token `MNTISA-1-<base32lower_nopad>` and store it.
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
        if s.is_empty() { None } else { Some(s) }
    }

    /// Validate a candidate token against the current one.
    pub async fn matches(&self, candidate: &str) -> bool {
        let cur = self.inner.read().await.clone();
        !cur.is_empty() && cur == candidate
    }
}

/// Produce `MNTISA-1-<longtoken>` where `<longtoken>` is base32 (lowercase, no padding).
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
            .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
}

#[derive(Clone)]
pub struct TokenStore {
    local_store: LocalTokenStore,
    in_memory: TokenStoreInMemory,
}

impl TokenStore {
    /// Load from `redb`. If empty or invalid, generate a fresh token and persist it.
    pub fn load(database: Arc<Database>) -> io::Result<Self> {
        let local_store = LocalTokenStore::new(database)?;
        let token = match local_store.read()? {
            Some(saved) if is_valid_format(&saved) => saved,
            _ => {
                // We are inside token.rs so we can call the private generator directly.
                let fresh = generate_token();
                local_store.write(&fresh)?;
                fresh
            }
        };
        let in_memory = TokenStoreInMemory::new(Some(token));
        Ok(Self {
            local_store,
            in_memory,
        })
    }

    /// Give a `TokenStore` handle to components that already expect the in-memory store
    /// (e.g., `ServerImpl::with_token_store`).
    pub fn in_memory_handle(&self) -> TokenStoreInMemory {
        self.in_memory.clone()
    }

    /// The current token string (always present after `load`).
    pub async fn current_token(&self) -> String {
        // In practice this is always Some(...) after load()
        self.in_memory.current().await.unwrap_or_default()
    }

    /// Rotate, persist, and return the new token.
    pub async fn rotate_and_persist(&self) -> io::Result<String> {
        let new_token = self.in_memory.rotate().await;
        self.local_store.write(&new_token)?;
        Ok(new_token)
    }

    /// Convenience for server join checks.
    pub async fn matches(&self, presented: &str) -> bool {
        self.in_memory.matches(presented).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{store::local_token_store::LocalTokenStore, token::TokenStore};
    use redb::Database;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn local_token_store_roundtrip() {
        let temp_directory = tempdir().unwrap();
        let db_path = temp_directory.path().join("state.redb");
        let database = Arc::new(Database::create(db_path).unwrap());
        let store = LocalTokenStore::new(database).expect("open");
        assert!(store.read().unwrap().is_none());
        store.write("MNTISA-1-abc234").unwrap();
        assert_eq!(store.read().unwrap().as_deref(), Some("MNTISA-1-abc234"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn token_store_persistent_survives_restart_and_rotates() {
        let temp_directory = tempdir().unwrap();
        let db_path = temp_directory.path().join("state.redb");

        let token_a;
        let token_rotated;

        // First boot.
        {
            let database = Arc::new(Database::create(&db_path).unwrap());
            let persistent_a = TokenStore::load(database.clone()).unwrap();
            token_a = persistent_a.current_token().await;
        }

        // "Restart"
        {
            let database_reopen = Arc::new(Database::create(&db_path).unwrap());

            let persistent_b = TokenStore::load(database_reopen).unwrap();
            let token_b = persistent_b.current_token().await;

            // Assert tokens are equal.
            assert_eq!(token_a, token_b, "token must be stable across restart");

            // rotate
            token_rotated = persistent_b.rotate_and_persist().await.unwrap();
        }

        // restart again → must see rotated value
        let database_third = Arc::new(Database::create(&db_path).unwrap());
        let persistent_c = TokenStore::load(database_third).unwrap();
        let token_c = persistent_c.current_token().await;
        assert_eq!(token_c, token_rotated);
    }
}
