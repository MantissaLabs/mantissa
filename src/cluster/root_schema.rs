use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// First root schema version understood by Mantissa clusters.
pub const LEGACY_ROOT_SCHEMA_VERSION: u32 = 1;
/// Lowest root schema version this binary keeps compatible during rolling upgrades.
pub const MIN_SUPPORTED_ROOT_SCHEMA_VERSION: u32 = LEGACY_ROOT_SCHEMA_VERSION;
/// Highest root schema version this binary knows how to serve.
pub const SUPPORTED_ROOT_SCHEMA_VERSION: u32 = LEGACY_ROOT_SCHEMA_VERSION;

/// Cluster-visible root-schema metadata advertised by each peer.
///
/// The range `[minimum_supported_version, supported_version]` defines the
/// semantic root projections this node can serve to peers during sync.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct RootSchemaInfo {
    #[serde(default = "legacy_root_schema_version")]
    pub minimum_supported_version: u32,
    #[serde(default = "legacy_root_schema_version")]
    pub supported_version: u32,
    #[serde(default)]
    pub updated_at_unix_ms: u64,
}

impl RootSchemaInfo {
    /// Builds one validated root-schema info snapshot.
    pub fn new(
        minimum_supported_version: u32,
        supported_version: u32,
        updated_at_unix_ms: u64,
    ) -> Result<Self, String> {
        if minimum_supported_version < LEGACY_ROOT_SCHEMA_VERSION {
            return Err(format!(
                "minimum supported root schema version must be >= {LEGACY_ROOT_SCHEMA_VERSION}, got {minimum_supported_version}"
            ));
        }
        if supported_version < LEGACY_ROOT_SCHEMA_VERSION {
            return Err(format!(
                "supported root schema version must be >= {LEGACY_ROOT_SCHEMA_VERSION}, got {supported_version}"
            ));
        }
        if minimum_supported_version > supported_version {
            return Err(format!(
                "minimum supported root schema version {minimum_supported_version} exceeds supported version {supported_version}"
            ));
        }

        Ok(Self {
            minimum_supported_version,
            supported_version,
            updated_at_unix_ms,
        })
    }

    /// Returns the support-range snapshot exported by this binary at startup.
    pub fn local_initial() -> Self {
        Self {
            minimum_supported_version: MIN_SUPPORTED_ROOT_SCHEMA_VERSION,
            supported_version: SUPPORTED_ROOT_SCHEMA_VERSION,
            updated_at_unix_ms: now_unix_ms(),
        }
    }

    /// Selects the newer peer-advertised root-schema snapshot.
    ///
    /// Root-schema metadata must support rollback, so precedence is based on the
    /// owner node's latest publication time rather than a monotonic max.
    pub fn merge(left: Self, right: Self) -> Self {
        if right.precedence_key() >= left.precedence_key() {
            right
        } else {
            left
        }
    }

    /// Returns the deterministic last-writer precedence key for one snapshot.
    fn precedence_key(&self) -> (u64, u32, u32) {
        (
            self.updated_at_unix_ms,
            self.supported_version,
            self.minimum_supported_version,
        )
    }

    /// Returns whether this peer can serve the requested semantic root schema version.
    pub fn supports(&self, version: u32) -> bool {
        version >= self.minimum_supported_version && version <= self.supported_version
    }

    /// Returns the highest common semantic root schema version shared by two peers.
    pub fn highest_common_version(left: Self, right: Self) -> Option<u32> {
        let minimum = left
            .minimum_supported_version
            .max(right.minimum_supported_version);
        let maximum = left.supported_version.min(right.supported_version);
        (minimum <= maximum).then_some(maximum)
    }
}

impl Default for RootSchemaInfo {
    fn default() -> Self {
        Self {
            minimum_supported_version: LEGACY_ROOT_SCHEMA_VERSION,
            supported_version: LEGACY_ROOT_SCHEMA_VERSION,
            updated_at_unix_ms: 0,
        }
    }
}

impl fmt::Display for RootSchemaInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "minimum_supported={}, supported={}, updated_at_unix_ms={}",
            self.minimum_supported_version, self.supported_version, self.updated_at_unix_ms
        )
    }
}

/// Process-local holder for the semantic root schema range this binary can serve.
#[derive(Clone, Copy, Debug)]
pub struct RootSchemaState {
    minimum_supported_version: u32,
    supported_version: u32,
    updated_at_unix_ms: u64,
}

impl RootSchemaState {
    /// Creates one validated local root-schema state holder.
    pub fn new(minimum_supported_version: u32, supported_version: u32) -> Result<Self, String> {
        let updated_at_unix_ms = now_unix_ms();
        RootSchemaInfo::new(
            minimum_supported_version,
            supported_version,
            updated_at_unix_ms,
        )?;
        Ok(Self {
            minimum_supported_version,
            supported_version,
            updated_at_unix_ms,
        })
    }

    /// Creates one state holder using the binary's configured support range.
    pub fn local_initial() -> Self {
        Self::new(
            MIN_SUPPORTED_ROOT_SCHEMA_VERSION,
            SUPPORTED_ROOT_SCHEMA_VERSION,
        )
        .expect("local root schema state must be valid")
    }

    /// Returns the lowest root schema version this binary still serves.
    pub fn minimum_supported_version(&self) -> u32 {
        self.minimum_supported_version
    }

    /// Returns the highest root schema version this binary supports.
    pub fn supported_version(&self) -> u32 {
        self.supported_version
    }

    /// Returns one cluster-visible info snapshot for gossip and join payloads.
    pub fn info(&self) -> RootSchemaInfo {
        RootSchemaInfo {
            minimum_supported_version: self.minimum_supported_version,
            supported_version: self.supported_version,
            updated_at_unix_ms: self.updated_at_unix_ms,
        }
    }

    /// Returns whether the provided version can be served by this binary.
    pub fn supports(&self, version: u32) -> bool {
        version >= self.minimum_supported_version && version <= self.supported_version
    }
}

impl Default for RootSchemaState {
    fn default() -> Self {
        Self::local_initial()
    }
}

/// Returns the default root schema version used by serde field defaults.
fn legacy_root_schema_version() -> u32 {
    LEGACY_ROOT_SCHEMA_VERSION
}

/// Returns the current wall-clock time in Unix milliseconds.
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{
        LEGACY_ROOT_SCHEMA_VERSION, RootSchemaInfo, RootSchemaState, SUPPORTED_ROOT_SCHEMA_VERSION,
    };

    /// Merge must prefer the newest root-schema publication so rollback remains possible.
    #[test]
    fn merge_prefers_newer_publication() {
        let merged = RootSchemaInfo::merge(
            RootSchemaInfo::new(1, 3, 10).expect("left info"),
            RootSchemaInfo::new(1, 2, 20).expect("right info"),
        );

        assert_eq!(merged.minimum_supported_version, 1);
        assert_eq!(merged.supported_version, 2);
        assert_eq!(merged.updated_at_unix_ms, 20);
    }

    /// Negotiation must pick the highest version available to both peers.
    #[test]
    fn highest_common_version_prefers_latest_overlap() {
        let left = RootSchemaInfo::new(1, 3, 10).expect("left info");
        let right = RootSchemaInfo::new(2, 4, 20).expect("right info");

        assert_eq!(RootSchemaInfo::highest_common_version(left, right), Some(3));
    }

    /// The support range must reject versions outside its declared bounds.
    #[test]
    fn state_supports_only_declared_versions() {
        let state = RootSchemaState::new(LEGACY_ROOT_SCHEMA_VERSION, SUPPORTED_ROOT_SCHEMA_VERSION)
            .expect("state");

        assert!(state.supports(LEGACY_ROOT_SCHEMA_VERSION));
        assert!(!state.supports(0));
    }
}
