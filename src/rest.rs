use data_encoding::Specification;
use getrandom::getrandom;
use mantissa_protocol::rest::rest_admin;
use redb::Database;
use std::{io, rc::Rc, sync::Arc};
use tokio::sync::RwLock;

use crate::store::local::LocalRestTokenStore;

const REST_TOKEN_PREFIX: &str = "MNTISA-REST-1-";

/// In-memory view of the local REST bearer token.
#[derive(Clone, Default)]
pub struct RestTokenStoreInMemory {
    inner: Arc<RwLock<String>>,
}

impl RestTokenStoreInMemory {
    /// Initializes the in-memory store with an optional persisted token.
    pub fn new(initial: Option<String>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(initial.unwrap_or_default())),
        }
    }

    /// Generates and installs a fresh REST bearer token.
    pub async fn generate(&self) -> io::Result<String> {
        let token = generate_rest_token()?;
        *self.inner.write().await = token.clone();
        Ok(token)
    }

    /// Rotates the REST bearer token to a fresh value.
    pub async fn rotate(&self) -> io::Result<String> {
        self.generate().await
    }

    /// Returns the current REST bearer token if one is installed.
    pub async fn current(&self) -> Option<String> {
        let token = self.inner.read().await.clone();
        if token.is_empty() { None } else { Some(token) }
    }

    /// Validates a candidate token with a constant-time comparison.
    pub async fn matches(&self, candidate: &str) -> bool {
        let current = self.inner.read().await;
        !current.is_empty() && constant_time_eq(current.as_bytes(), candidate.as_bytes())
    }
}

/// Persistent REST token store loaded during daemon bootstrap.
#[derive(Clone)]
pub struct RestTokenStore {
    local_store: LocalRestTokenStore,
    in_memory: RestTokenStoreInMemory,
}

impl RestTokenStore {
    /// Loads the node-local REST bearer token, creating it when requested.
    pub fn load(database: Arc<Database>, create_missing: bool) -> io::Result<Self> {
        let local_store = LocalRestTokenStore::new(database)?;
        let token = match local_store.read()? {
            Some(saved) if is_valid_rest_token_format(&saved) => Some(saved),
            _ if create_missing => {
                let fresh = generate_rest_token()?;
                local_store.write(&fresh)?;
                Some(fresh)
            }
            _ => None,
        };
        let in_memory = RestTokenStoreInMemory::new(token);
        Ok(Self {
            local_store,
            in_memory,
        })
    }

    /// Returns the current REST bearer token, creating one when needed.
    pub async fn current_token(&self) -> io::Result<String> {
        if let Some(token) = self.in_memory.current().await {
            return Ok(token);
        }
        self.rotate_and_persist().await
    }

    /// Rotates, persists, and returns the new REST bearer token.
    pub async fn rotate_and_persist(&self) -> io::Result<String> {
        let new_token = self.in_memory.rotate().await?;
        self.local_store.write(&new_token)?;
        Ok(new_token)
    }

    /// Validates a presented REST bearer token.
    pub async fn matches(&self, presented: &str) -> bool {
        self.in_memory.matches(presented).await
    }
}

/// Cap'n Proto service for node-local REST token administration.
#[derive(Clone)]
pub struct RestAdmin {
    token_store: RestTokenStore,
}

impl RestAdmin {
    /// Constructs a REST admin service backed by the local token store.
    pub fn new(token_store: RestTokenStore) -> Self {
        Self { token_store }
    }
}

impl rest_admin::Server for RestAdmin {
    /// Returns the current local REST bearer token.
    async fn show_token(
        self: Rc<Self>,
        _params: rest_admin::ShowTokenParams,
        mut results: rest_admin::ShowTokenResults,
    ) -> Result<(), capnp::Error> {
        let token = self.token_store.current_token().await?;
        results.get().set_token(&token);
        Ok(())
    }

    /// Rotates the local REST bearer token.
    async fn rotate_token(
        self: Rc<Self>,
        _params: rest_admin::RotateTokenParams,
        mut results: rest_admin::RotateTokenResults,
    ) -> Result<(), capnp::Error> {
        let token = self.token_store.rotate_and_persist().await?;
        results.get().set_token(&token);
        Ok(())
    }

    /// Validates one presented REST bearer token.
    async fn validate_token(
        self: Rc<Self>,
        params: rest_admin::ValidateTokenParams,
        mut results: rest_admin::ValidateTokenResults,
    ) -> Result<(), capnp::Error> {
        let token = params.get()?.get_token()?.to_str()?;
        let valid = self.token_store.matches(token).await;
        results.get().set_valid(valid);
        Ok(())
    }
}

/// Produces a `MNTISA-REST-1-<base32lower_nopad>` token.
fn generate_rest_token() -> io::Result<String> {
    let mut bytes = [0u8; 48];
    getrandom(&mut bytes)?;

    let mut spec = Specification::new();
    spec.symbols.push_str("abcdefghijklmnopqrstuvwxyz234567");
    spec.padding = None;
    let encoding = spec
        .encoding()
        .map_err(|error| io::Error::other(format!("invalid token encoding: {error}")))?;

    Ok(format!("{REST_TOKEN_PREFIX}{}", encoding.encode(&bytes)))
}

/// Validates the stable textual REST token format.
pub fn is_valid_rest_token_format(token: &str) -> bool {
    let Some(encoded) = token.strip_prefix(REST_TOKEN_PREFIX) else {
        return false;
    };
    !encoded.is_empty()
        && encoded
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || (b'2'..=b'7').contains(&byte))
}

/// Compares two byte slices without data-dependent early returns.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    let max_len = left.len().max(right.len());
    for index in 0..max_len {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        diff |= usize::from(left_byte ^ right_byte);
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn rest_token_format_requires_rest_prefix() {
        assert!(is_valid_rest_token_format("MNTISA-REST-1-abc234"));
        assert!(!is_valid_rest_token_format("MNTISA-1-abc234"));
        assert!(!is_valid_rest_token_format("MNTISA-REST-1-ABC234"));
        assert!(!is_valid_rest_token_format("MNTISA-REST-1-"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rest_token_store_survives_restart_and_rotates() {
        let temp_directory = tempdir().unwrap();
        let db_path = temp_directory.path().join("state.redb");

        let first_token;
        let rotated_token;

        {
            let database = Arc::new(Database::create(&db_path).unwrap());
            let store = RestTokenStore::load(database, true).unwrap();
            first_token = store.current_token().await.unwrap();
            assert!(is_valid_rest_token_format(&first_token));
        }

        {
            let database = Arc::new(Database::create(&db_path).unwrap());
            let store = RestTokenStore::load(database, true).unwrap();
            assert_eq!(store.current_token().await.unwrap(), first_token);
            rotated_token = store.rotate_and_persist().await.unwrap();
            assert_ne!(rotated_token, first_token);
        }

        {
            let database = Arc::new(Database::create(&db_path).unwrap());
            let store = RestTokenStore::load(database, true).unwrap();
            assert_eq!(store.current_token().await.unwrap(), rotated_token);
            assert!(store.matches(&rotated_token).await);
            assert!(!store.matches(&first_token).await);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rest_token_store_can_create_on_first_show() {
        let temp_directory = tempdir().unwrap();
        let db_path = temp_directory.path().join("state.redb");
        let database = Arc::new(Database::create(&db_path).unwrap());
        let local_store = LocalRestTokenStore::new(database.clone()).unwrap();

        let store = RestTokenStore::load(database, false).unwrap();
        assert!(local_store.read().unwrap().is_none());
        assert!(!store.matches("").await);

        let token = store.current_token().await.unwrap();
        assert!(is_valid_rest_token_format(&token));
        assert_eq!(local_store.read().unwrap().as_deref(), Some(token.as_str()));
    }
}
