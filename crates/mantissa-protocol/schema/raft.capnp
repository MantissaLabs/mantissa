@0xa744c8a7a4dcf650;

using Volumes = import "volumes.capnp";

# Entry point for Raft and application RPCs on one authenticated connection.
interface RaftTransport {
  getRaft @0 () -> (service :Raft);
  # Return the generic Raft service for this authenticated connection.

  getApplication @1 () -> (service :AnyPointer);
  # Return the application service registered by the crate using Raft.
}

# Raft RPCs addressed to one application-defined group.
interface Raft {
  requestVote @0 (groupId :RaftGroupId, request :VoteRequest)
      -> (response :VoteResponse);
  # Ask one member of a Raft group to vote in an election.

  appendEntries @1 (groupId :RaftGroupId, request :AppendEntriesRequest)
      -> (response :AppendEntriesResponse);
  # Replicate log entries or send a heartbeat when entries is empty.

  installSnapshot @2 (
      groupId :RaftGroupId,
      request :InternalSnapshotRequest
  ) -> (response :InternalSnapshotResponse);
  # Send one bounded part of an internal Raft snapshot.

  startElection @3 (groupId :RaftGroupId) -> ();
  # Ask one caught-up voter to start an election during a planned leader move.

  requestLeadership @4 (groupId :RaftGroupId) -> ();
  # Ask the current leader to move leadership to the authenticated caller.

  startGroup @5 (groupId :RaftGroupId) -> ();
  # Start one saved group before the authenticated caller begins using it.
}

# One Mantissa command stored in a Raft log entry.
struct RaftApplicationCommand {
  union {
    invalid @0 :Void;
    # Default value. Decoders always reject it.

    volumeControlCommand @1 :Volumes.VolumeControlCommand;
    # Bounded replicated-volume control state command.
  }
}

# Durable identity and startup state for one local Raft group.
struct RaftGroupRecord {
  formatVersion @0 :UInt16;
  # Exact catalog record format understood by this build.

  groupId @1 :RaftGroupId;
  # Application-defined identity of this Raft group.

  activation @2 :RaftGroupActivation;
  # Whether startup should leave this group idle or start it.

  vote @3 :Vote;
  # Latest vote saved by this local group, or none before its first election.

  membership @4 :StoredMembership;
  # Latest stored membership and its log position, when one exists.

  appliedLogId @5 :LogId;
  # Newest entry applied by the local state machine, or null before apply.
}

# Request sent by a candidate asking another member for its vote.
struct VoteRequest {
  vote @0 :Vote;
  # Candidate and term requesting the vote.

  lastLogId @1 :LogId;
  # Last log entry stored by the candidate, or none when its log is empty.
}

# Reply sent after a member handles a vote request.
struct VoteResponse {
  vote @0 :Vote;
  # Latest vote known by the member sending the reply.

  granted @1 :Bool;
  # True when the requested vote was accepted and stored.

  lastLogId @2 :LogId;
  # Last log entry stored by the member sending the reply.
}

# Request sent by a leader to replicate entries or send a heartbeat.
struct AppendEntriesRequest {
  vote @0 :Vote;
  # Current committed leader and term.

  previousLogId @1 :LogId;
  # Entry immediately before the first entry in this request.

  entries @2 :List(LogEntry);
  # Ordered entries to append. Empty means this request is a heartbeat.

  leaderCommit @3 :LogId;
  # Highest log entry the leader knows is committed.
}

# Reply sent after a member handles an append request.
struct AppendEntriesResponse {
  union {
    success @0 :Void;
    # Every entry in the request was accepted.

    partialSuccessWithoutLog @1 :Void;
    # Partial success with no matching log entry.

    partialSuccess @2 :LogId;
    # Entries were accepted through the returned log ID.

    conflict @3 :Void;
    # The previous log ID did not match the member's log.

    higherVote @4 :Vote;
    # The member knows a newer vote than the leader.
  }
}

# One bounded part of an internal snapshot sent by the current leader.
struct InternalSnapshotRequest {
  vote @0 :Vote;
  # Current committed leader and term.

  snapshotId @1 :Text;
  # Stable snapshot name chosen by OpenRaft.

  lastLogId @2 :LogId;
  # Newest log entry included in the snapshot, or none for an empty log.

  lastMembership @3 :StoredMembership;
  # Latest membership included in the snapshot, or none before initialization.

  chunkNumber @4 :UInt64;
  # Increasing part number starting at zero.

  chunkOffset @5 :UInt64;
  # Byte position at which this part begins.

  finished @6 :Bool;
  # True only for the final part.

  data @7 :Data;
  # Application snapshot bytes bounded by the transport settings.
}

# Reply after receiving one internal snapshot part.
struct InternalSnapshotResponse {
  vote @0 :Vote;
  # Latest vote known by the receiving member.
}

# Candidate or leader chosen for one Raft term.
struct Vote {
  term @0 :UInt64;
  # Raft election term.

  memberId @1 :NodeId;
  # Member chosen as the candidate or leader in this term.

  committed @2 :Bool;
  # True when a quorum has accepted this leader.
}

# One application-defined Raft member ID.
struct NodeId {
  value @0 :Data;
  # Exact bytes written and checked by the application node-ID adapter.
}

# Leader and index that identify one Raft log entry.
struct LogId {
  leaderTerm @0 :UInt64;
  # Term of the committed leader that created the entry.

  index @1 :UInt64;
  # Increasing position of the entry in the Raft log.

  leaderMemberId @2 :NodeId;
  # Member that led the term in which this entry was created. This is absent
  # only for OpenRaft's first membership entry at term and index zero.
}

# One entry stored and replicated by Raft.
struct LogEntry {
  id @0 :LogId;
  # Leader term, leader member, and index of this entry.

  union {
    blank @1 :Void;
    # Internal entry written when a leader is elected.

    application @2 :RaftApplicationCommand;
    # Command passed to the application after the entry commits.

    membership @3 :Membership;
    # Change to the members of this Raft group.
  }
}

# Voting sets and all known members of one Raft group.
struct Membership {
  voterSets @0 :List(VoterSet);
  # One normal voting set or two sets during a membership change.

  memberIds @1 :List(NodeId);
  # All voters and non-voting members known to the group.
}

# Member IDs that form one voting set.
struct VoterSet {
  memberIds @0 :List(NodeId);
  # Members that must provide a majority for this voting set.
}

# One application-defined Raft group ID.
struct RaftGroupId {
  value @0 :Data;
  # Exact bytes written and checked by the application group-ID adapter.
}

# Whether a discovered Raft group should run.
enum RaftGroupActivation {
  inactive @0;
  # Keep the durable group idle after discovery.

  active @1;
  # Start the durable group after discovery and before serving requests.
}

# Last-known membership and the log entry that established it.
struct StoredMembership {
  logId @0 :LogId;
  # Log entry containing this membership, or none before initialization.

  membership @1 :Membership;
  # Voters and non-voting members in the group.
}

# Identity stored at the beginning of one encrypted Raft log segment.
struct RaftLogSegmentHeader {
  formatVersion @0 :UInt16;
  # Exact segment format understood by this build.

  groupId @1 :RaftGroupId;
  # Group that owns the segment.

  segmentId @2 :Data;
  # Random 16-byte identity that is never reused.
}

# Authenticated metadata for one encrypted Raft log frame.
struct RaftLogFrameHeader {
  formatVersion @0 :UInt16;
  # Exact frame format understood by this build.

  segmentId @1 :Data;
  # Segment containing the frame.

  frameNumber @2 :UInt64;
  # Increasing frame number used once within this segment.

  logId @3 :LogId;
  # Raft log entry stored in the encrypted frame.

  plaintextBytes @4 :UInt32;
  # Encoded log-entry bytes before encryption.

  ciphertextBytes @5 :UInt32;
  # Encrypted bytes including the authentication tag.
}

# Durable location of one encrypted Raft log frame.
struct RaftLogLocation {
  formatVersion @0 :UInt16;
  # Exact location format understood by this build.

  groupId @1 :RaftGroupId;
  # Group that owns the log entry.

  logId @2 :LogId;
  # Raft log entry stored at this location.

  segmentId @3 :Data;
  # Segment file containing the frame.

  frameOffset @4 :UInt64;
  # Byte offset of the frame prefix in the segment.

  frameBytes @5 :UInt32;
  # Complete frame size including its prefix.

  frameNumber @6 :UInt64;
  # Frame number used to make this frame's nonce unique.
}

# Saved commit and removal positions for one Raft log.
struct RaftLogState {
  formatVersion @0 :UInt16;
  # Exact state format understood by this build.

  groupId @1 :RaftGroupId;
  # Group that owns this log.

  lastRemovedLogId @2 :LogId;
  # Newest entry removed from the start, or none when nothing was removed.

  committedLogId @3 :LogId;
  # Newest entry known to be committed, or none before the first commit.
}
