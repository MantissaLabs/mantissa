# Backup and Disaster Recovery

Mantissa state is durable, but a state directory is not a portable node image by
default. It contains both replicated cluster data and node-local identity:

- `state.redb`: replicated CRDT rows, local node id, session tickets, join token,
  wrapped secret master-key envelopes, and other durable control-plane state.
- `noise.key`: static Noise identity used for peer transport authentication.
- `ed25519.key`: signing identity used for renewable cluster credentials.
- `wireguard.key` and `wireguard.port`: local WireGuard underlay identity and
  port selection when WireGuard is enabled.

Copying `/var/lib/mantissa` directly onto another machine and starting it
unchanged creates an identity collision. The restored daemon would claim the
source node id and cryptographic identities, while the source node may still be
known by the cluster or may later come back.

## Backup Procedure

Take backups from a stopped daemon or from a filesystem snapshot that gives a
consistent view of the state directory. Avoid copying `state.redb` while the
daemon is actively writing unless the copy is provided by an atomic snapshot.

For a root daemon, the default state directory is:

```bash
/var/lib/mantissa
```

For an unprivileged daemon, the default state directory is:

```bash
~/.mantissa
```

Backups contain encrypted cluster secrets, wrapped master-key envelopes, and
node private keys. Store them as sensitive material. Anyone who can read a full
backup and obtain or guess the master-key passphrase can potentially recover
workload secrets, and anyone with the identity keys can impersonate the backed-up
node unless the restore workflow below regenerates identity before startup.

## Restore Modes

### Same-Node Restore

Use this when restoring the same node identity after disk loss or host
replacement, and only when the old copy of that node will never run again.

1. Stop Mantissa on the target host.
2. Restore the full state directory, including `state.redb`, `noise.key`,
   `ed25519.key`, and WireGuard files.
3. Start `mantissa init` normally and provide the same master-key passphrase
   used for the restored local envelope.
4. Verify the node converges with peers.

This preserves the node id and keys, so peers recognize it as the same member.
It is unsafe if the original node can still reappear.

### Clone as a New Node

Use this when copying another node's state directory as a seed for a new node.
This preserves replicated cluster data and wrapped secret master-key envelopes,
but removes local identity so the daemon starts as a distinct node.

1. Stop Mantissa on the target host.
2. Copy the source state directory to the target state directory.
3. Start the daemon with an identity reset:

```bash
sudo mantissa init --reset-identity --state-dir /var/lib/mantissa
```

For an unprivileged daemon:

```bash
mantissa init --reset-identity --state-dir ~/.mantissa
```

The reset removes local key files, clears local session-ticket caches, clears
server-issued session-ticket tables, removes the stored node id, and locally
purges the copied node's old peer row without writing a replicated tombstone. It
keeps other replicated CRDT rows, cluster view metadata, join token state, and
wrapped secret master-key envelopes. Passing `--reset-identity` is the
confirmation; there is no interactive prompt so this flow is usable from
provisioning and recovery automation. The next `mantissa init` still needs the
passphrase for the preserved local envelope.

4. Join an active cluster through the normal join path:

```bash
mantissa join --anchor <peer-addr>:6578 --join-token <token>
```

The copied peer rows may still include the source node. If that source node is
gone permanently, retire it from any active cluster member once the restored
node is healthy:

```bash
mantissa nodes evict <old-node-id>
```

The identity reset intentionally does not rewrite or tombstone peer rows
because it cannot know whether the source node is still valid elsewhere.

### Whole-Cluster Loss

When all nodes are lost, restore one node from backup as a same-node restore.
After it starts, rotate admission and secret material if the backup may have
been exposed:

```bash
mantissa token rotate
mantissa secrets rotate-master-key
```

Bring additional replacement nodes back with the clone-as-new-node workflow and
join them to the restored seed node. Do not start multiple copies of the same
backup unchanged.

## Stale Removed Nodes

Nodes that stayed active but offline remain inside the tombstone-GC barrier and
can restart normally. Nodes that intentionally left or were removed are outside
that barrier; after the tombstone retention window, old data directories must
not publish stale replicated rows directly.

For reintroducing an old removed node, either restore it through a fresh join or
start it with `mantissa init --reset-identity`.

Use `mantissa nodes evict <node-id>` only for stopped, replaced, or otherwise
stale identities. If a process is still running with that identity, stop or
reset it before eviction so it does not keep trying to participate with retired
credentials.
