# mantissa-health

Small SWIM-style health state machine used by Mantissa topology.

`mantissa-health` keeps peer liveness state separate from networking and
storage. It records active probe results, passive observations, and incoming
membership rumors, then returns actions for the caller to gossip or apply to
transport state.

## Concepts

- `HealthMonitor`: thread-safe monitor for one local node.
- `Status`: local view of a peer (`Alive`, `Suspect`, `Down`, and related states).
- `SwimEvent`: compact liveness rumor with peer id and incarnation.
- `Action`: side effect requested by the monitor, such as gossiping an event or
  invalidating a peer connection.

The monitor does not send network traffic itself. Callers own probe transport,
timers, and gossip delivery.

## Example

```rust
use std::time::Duration;

use mantissa_health::{Action, HealthMonitor};
use uuid::Uuid;

fn main() {
    let local_id = Uuid::new_v4();
    let peer_id = Uuid::new_v4();
    let health = HealthMonitor::new(local_id);

    health.record_join(peer_id, 1);

    let actions = health.record_probe_failure(
        peer_id,
        Duration::from_millis(100),
        Duration::from_secs(1),
    );

    for action in actions {
        match action {
            Action::Gossip(event) => {
                // Enqueue the event on the caller's gossip transport.
                println!("gossip {event:?}");
            }
            Action::InvalidatePeer(peer) => {
                // Drop cached transport state for the down peer.
                println!("invalidate {peer}");
            }
        }
    }
}
```

## Design Notes

Incarnations are monotonic per node. A local node refutes stale suspicion by
advancing its incarnation and returning an `Alive` event for the caller to gossip.
Remote updates are applied by incarnation order and status rank.
