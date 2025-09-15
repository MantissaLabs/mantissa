@0x8ddfac9670d583d3;

# Lightweight health RPC for liveness/diagnostics.
interface Health {
  ping @0 () -> (ok :Bool, now :UInt64, rootDigest :Data);
  # Returns a quick heartbeat with current time and the 16-byte MST root digest (Peers domain).
}

enum NodeStatus {
  unknown @0;
  # default before any heartbeat observed

  alive @1;
  # heartbeats within expected window

  suspect @2;
  # consecutive misses over threshold (e.g., >= 3), not yet declared down

  down @3;
  # unreachable beyond failure timeout / reconnection budget

  degraded @4;
  # reachable but persistent digest/root mismatch beyond grace window
  # (useful to signal "needs anti-entropy" without marking it down)
}
