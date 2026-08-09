/// Distinguishes application results from responses to internal Raft entries.
///
/// OpenRaft requires one response for every applied entry. Blank and membership
/// entries have no application result, so the adapter represents them
/// explicitly instead of requiring applications to invent a default response.
#[derive(Debug, Eq, PartialEq)]
pub enum EntryResponse<R> {
    /// Result produced by an application command.
    Application(R),

    /// Marker produced by an internal blank or membership entry.
    Internal,
}
