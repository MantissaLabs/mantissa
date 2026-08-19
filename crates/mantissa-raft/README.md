# mantissa-raft

Generic typed Raft support for Mantissa applications.

`mantissa-raft` provides the consensus machinery shared by applications that
need a small, strongly consistent control state. Applications define their own
commands, responses, snapshots, and state machine. This crate stores and moves
those values without interpreting their meaning.

## Application Boundary

An application supplies the types selected by `RaftApplication` and implements
the state-machine traits:

- `ApplicationCommand` and `ApplicationResponse` mark owned values that may
  cross the Raft boundary.
- `ApplicationStateMachine` applies committed commands in log order.
- `DurableApplicationStateMachine` builds and installs bounded snapshots.
- `ApplicationSnapshot` streams snapshot data without requiring the complete
  snapshot to remain in memory.
- `ApplyContext` identifies the committed term and log index being applied.

Expected application-level rejection belongs in the response type. Storage,
transport, and state-machine failures remain Raft errors.

## Components

- `catalog`: stores the identity, members, and activation state of every local
  Raft group in Redb.
- `durable_log`: stores encrypted log entries in per-group segment files, with
  their locations and retention state indexed in Redb.
- `protocol`: converts typed Raft values to and from bounded Cap'n Proto
  messages.
- `runtime`: starts cataloged groups on demand, suspends idle groups, and limits
  concurrent startup and background work.
- `transport`: shares one authenticated, bounded TCP transport across local
  groups.
- `memory`: provides an in-process, non-durable Raft implementation for tests.

## Runtime Model

A catalog record does not start a task, timer, transport, or state machine.
`GroupRuntime` activates a group only when local or incoming work needs it and
returns the existing handle when the group is already running. Idle groups may
be stopped without removing the durable record needed for later recovery.

Transport and runtime limits are checked before use. They bound message sizes,
active groups, concurrent group starts, background work, connections, and
snapshot transfers so one group cannot consume unbounded node resources.

## Durable State

The group catalog and log index live in Redb. Raft log payloads live in
encrypted append-only segment files. The application owns its durable state and
snapshot format; structured application commands and snapshots use Cap'n Proto.

This separation lets an application keep its state compact while Raft handles
leader election, membership, ordered control changes, log recovery, and
snapshot transfer.

## Consumer Guidance

Use this crate when implementing a Mantissa subsystem that needs consensus.
Define application rules in the application crate and keep them out of
`mantissa-raft`. Most daemon code should use the subsystem's higher-level
runtime instead of constructing Raft groups directly.

The in-memory module is intended for tests and protocol development. Durable
applications should use the catalog, encrypted log, TCP transport, and their
own durable state-machine storage together.
