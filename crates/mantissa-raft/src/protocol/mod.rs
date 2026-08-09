//! Converts typed Raft values to and from Cap'n Proto messages.

mod adapter;
mod append;
pub(crate) mod catalog_record;
mod entry;
mod error;
mod group_id;
mod limits;
mod log_id;
pub(crate) mod membership;
mod message;
pub(crate) mod rpc;
mod vote;

pub use adapter::{ApplicationCommandAdapter, NodeIdAdapter};
pub use append::{
    decode_append_entries_request, decode_append_entries_response, encode_append_entries_request,
    encode_append_entries_response,
};
pub use entry::encode_log_entry;
pub use error::ProtocolError;
pub use limits::{InvalidProtocolLimits, ProtocolLimitSettings, ProtocolLimits};
pub use vote::{
    decode_vote_request, decode_vote_response, encode_vote_request, encode_vote_response,
};

pub(crate) use entry::decode_log_entry;
pub(crate) use group_id::{encode_group_id, read_group_id, write_group_id};
pub(crate) use log_id::{read_log_id, write_log_id};
pub(crate) use message::{check_output_size, read_message};
