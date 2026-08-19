# mantissa-volume

Replicated block-volume application and local data path for Mantissa.

`mantissa-volume` defines the rules, durable local records, block storage, and
Linux device support used by replicated volumes. It uses `mantissa-raft` for
the small set of control decisions that require quorum. Volume blocks travel
through a separate data path and are never stored in the Raft log.

## Safety Model

Each volume generation has one bounded Raft control state. It records the
volume descriptor, active copies, current fence, writer grant, disposition,
and any recovery or replacement grant. It deliberately does not record local
devices, mounts, copy progress, or cleanup jobs.

The applied fence and writer session decide whether a block request may run.
Old sessions are rejected after a new fence is committed. Foreground durable
operations complete only after the active copy set has acknowledged them.
Recovery and replica replacement copy data outside Raft, but Raft must first
grant the operation and later commit the resulting copy set.

## Local Data Path

On Linux, a mounted replicated volume uses these layers:

1. Device-mapper provides the stable block-device path mounted by the
   filesystem.
2. ublk sends kernel block requests to a `BlockHandler` in userspace.
3. The fixed replica path checks the current fence and applies each accepted
   operation to the active replica files.
4. The filesystem manager formats, mounts, checks, and expands ext4 or XFS.

Device-mapper keeps the filesystem path stable while the underlying ublk
device changes during online expansion or local driver replacement.

The crate compiles on non-Linux hosts so the rest of the workspace can be
checked there, but ublk, device-mapper, and replicated-volume filesystem
operations are available only on Linux.

## Components

- `control_state`: evaluates the commands that initialize, expand, retain,
  restore, fence, recover, and replace a volume generation.
- `state_machine`: applies committed commands to durable bounded state and
  builds or installs Raft snapshots.
- `catalog`: stores local replicas, attachments, retirements, and storage-pool
  reservations in Redb.
- `storage`: stores control state and fixed-offset replica files, and provides
  the peer data protocol, I/O admission, recovery, and repair workers.
- `driver`: exposes checked block I/O through ublk and a stable device-mapper
  mapping.
- `fs`: manages ext4 and XFS filesystems and reports their capacity.
- `protocol`: encodes volume commands, responses, state, and identities with
  Cap'n Proto.
- `storage_format`: calculates the complete local space reservation for a
  replica, including headers and recovery metadata.

## Descriptor Example

The public descriptor types validate identity, generation, capacity, and block
alignment before storage is opened:

```rust,no_run
use mantissa_volume::{
    VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId,
};
use uuid::Uuid;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor = VolumeDescriptor::new(
        VolumeId::new(Uuid::new_v4())?,
        VolumeGeneration::new(1)?,
        10 * 1024 * 1024 * 1024,
        VolumeBlockSizes::supported(),
    )?;

    println!("volume capacity: {} bytes", descriptor.capacity().bytes());
    Ok(())
}
```

## Consumer Guidance

This is an internal Mantissa application crate, not the user-facing volume
API. The main daemon owns placement, reconciliation, scheduling, and public
status. The CLI and REST API reach that daemon through `mantissa-client`.

Keep cluster-wide lifecycle decisions in the main replicated-volume runtime.
Keep consensus-only rules in `control_state`, local durable facts in `catalog`,
and block movement in `storage`. Raft must not become part of the foreground
block data path.
