use capnp::message::ReaderOptions;
use thiserror::Error;

const MAX_VOTER_SETS: u32 = 2;

/// Values the caller chooses for Raft message safety limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtocolLimitSettings {
    /// Largest encoded vote or append message.
    pub max_message_bytes: usize,

    /// Largest encoded log entry.
    pub max_entry_bytes: usize,

    /// Largest entry list accepted in one append request.
    pub max_append_entries: u32,

    /// Largest number of voters and non-voters in one group.
    pub max_membership_nodes: u32,

    /// Cap'n Proto traversal budget for one message.
    pub max_traversal_bytes: usize,

    /// Cap'n Proto nesting budget for one message.
    pub max_nesting_levels: u32,
}

/// Validated limits used while reading and writing Raft messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtocolLimits {
    max_message_bytes: usize,
    max_entry_bytes: usize,
    max_append_entries: u32,
    max_membership_nodes: u32,
    max_traversal_bytes: usize,
    max_nesting_levels: i32,
}

impl ProtocolLimits {
    /// Validates every caller-selected limit. This function has no defaults.
    pub fn new(settings: ProtocolLimitSettings) -> Result<Self, InvalidProtocolLimits> {
        let non_zero_settings = [
            ("maximum message bytes", settings.max_message_bytes),
            ("maximum entry bytes", settings.max_entry_bytes),
            (
                "maximum append entries",
                settings.max_append_entries as usize,
            ),
            (
                "maximum membership nodes",
                settings.max_membership_nodes as usize,
            ),
            ("maximum traversal bytes", settings.max_traversal_bytes),
            (
                "maximum nesting levels",
                settings.max_nesting_levels as usize,
            ),
        ];
        if let Some((name, _)) = non_zero_settings.into_iter().find(|(_, value)| *value == 0) {
            return Err(InvalidProtocolLimits::Zero { name });
        }
        if settings.max_entry_bytes > settings.max_message_bytes {
            return Err(InvalidProtocolLimits::EntryExceedsMessage {
                entry_bytes: settings.max_entry_bytes,
                message_bytes: settings.max_message_bytes,
            });
        }
        if settings.max_traversal_bytes < settings.max_message_bytes {
            return Err(InvalidProtocolLimits::TraversalBelowMessage {
                traversal_bytes: settings.max_traversal_bytes,
                message_bytes: settings.max_message_bytes,
            });
        }
        if !settings.max_traversal_bytes.is_multiple_of(8) {
            return Err(InvalidProtocolLimits::TraversalNotWordAligned {
                traversal_bytes: settings.max_traversal_bytes,
            });
        }
        let max_nesting_levels = i32::try_from(settings.max_nesting_levels).map_err(|_| {
            InvalidProtocolLimits::NestingTooLarge {
                nesting_levels: settings.max_nesting_levels,
            }
        })?;

        Ok(Self {
            max_message_bytes: settings.max_message_bytes,
            max_entry_bytes: settings.max_entry_bytes,
            max_append_entries: settings.max_append_entries,
            max_membership_nodes: settings.max_membership_nodes,
            max_traversal_bytes: settings.max_traversal_bytes,
            max_nesting_levels,
        })
    }

    /// Returns the largest encoded vote or append message.
    #[must_use]
    pub const fn max_message_bytes(self) -> usize {
        self.max_message_bytes
    }

    /// Returns the largest encoded log entry.
    #[must_use]
    pub const fn max_entry_bytes(self) -> usize {
        self.max_entry_bytes
    }

    /// Returns the largest entry list accepted in one append request.
    #[must_use]
    pub const fn max_append_entries(self) -> u32 {
        self.max_append_entries
    }

    /// Returns the largest number of voting sets in one membership.
    #[must_use]
    pub const fn max_voter_sets(self) -> u32 {
        MAX_VOTER_SETS
    }

    /// Returns the caller-selected limit for voters and non-voters.
    #[must_use]
    pub const fn max_membership_nodes(self) -> u32 {
        self.max_membership_nodes
    }

    /// Returns Cap'n Proto reader limits for one Raft message.
    pub fn reader_options(self) -> ReaderOptions {
        let mut options = ReaderOptions::new();
        options
            .traversal_limit_in_words(Some(self.max_traversal_bytes / 8))
            .nesting_limit(self.max_nesting_levels);
        options
    }
}

/// Explains why a set of Raft protocol limits cannot be used.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InvalidProtocolLimits {
    /// Every byte and count limit must allow at least one value.
    #[error("{name} must be greater than zero")]
    Zero {
        /// Name of the zero limit.
        name: &'static str,
    },

    /// An entry cannot be larger than the message carrying it.
    #[error(
        "maximum entry size {entry_bytes} exceeds maximum message size \
         {message_bytes}"
    )]
    EntryExceedsMessage {
        /// Caller-selected entry limit.
        entry_bytes: usize,

        /// Caller-selected message limit.
        message_bytes: usize,
    },

    /// The reader must be able to traverse every accepted message.
    #[error(
        "Cap'n Proto traversal limit {traversal_bytes} is below maximum \
         message size {message_bytes}"
    )]
    TraversalBelowMessage {
        /// Caller-selected traversal limit.
        traversal_bytes: usize,

        /// Caller-selected message limit.
        message_bytes: usize,
    },

    /// Cap'n Proto measures traversal in complete eight-byte words.
    #[error("Cap'n Proto traversal limit {traversal_bytes} is not divisible by 8")]
    TraversalNotWordAligned {
        /// Caller-selected traversal limit.
        traversal_bytes: usize,
    },

    /// Cap'n Proto represents its nesting limit as a signed integer.
    #[error("Cap'n Proto nesting limit {nesting_levels} exceeds i32::MAX")]
    NestingTooLarge {
        /// Caller-selected nesting limit.
        nesting_levels: u32,
    },
}
