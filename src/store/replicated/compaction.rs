use chrono::{DateTime, Utc};
use std::cmp::Ordering;

/// Timestamp rank that compares valid RFC3339 values by instant and otherwise by raw text.
///
/// Some existing registry selectors use this exact ordering so malformed legacy timestamps remain
/// deterministic without being silently promoted over valid timestamps. Register compaction uses it
/// to preserve those selectors while turning their comparison logic into a reusable rank key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParsedOrRawTimestampRank(String);

impl ParsedOrRawTimestampRank {
    /// Builds one timestamp rank from the replicated timestamp text stored in a CRDT value.
    pub(crate) fn new(raw: &str) -> Self {
        Self(raw.to_string())
    }

    /// Parses the stored timestamp as a UTC instant when it is well-formed RFC3339.
    fn parsed(&self) -> Option<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(&self.0)
            .map(|timestamp| timestamp.with_timezone(&Utc))
            .ok()
    }
}

impl Ord for ParsedOrRawTimestampRank {
    /// Compares parsed timestamps when both sides are valid and otherwise falls back to raw text.
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.parsed(), other.parsed()) {
            (Some(left), Some(right)) => left.cmp(&right),
            _ => self.0.cmp(&other.0),
        }
    }
}

impl PartialOrd for ParsedOrRawTimestampRank {
    /// Delegates partial ordering to the total deterministic timestamp ordering.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
