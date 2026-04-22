# eBPF / NodePort Production Readiness Plan

This document is a working implementation checklist for making Mantissa's
eBPF-backed VIP and NodePort dataplane production ready without disabling it.
It is intentionally implementation-oriented: each phase names the code to
change, the behavior to verify, and the test checkpoints to run before moving
on.

## Scope and release position

Goal:

- Keep the eBPF / NodePort path enabled and first-class.
- Make the current public ingress + VIP dataplane correct under production
  traffic, backend churn, restarts, and operator mistakes.
- Preserve Mantissa's "service discovery + working load balancer" experience.

Release assumptions:

- Single trust domain cluster.
- No multi-tenant or partially trusted node model for this first production
  release.
- "Production ready" here means correct, observable, capacity-aware,
  configurable, and validated in CI on real privileged Linux runners.

Not a release blocker for this scope:

- Full Kubernetes-style network policy parity.
- Cloud load balancer automation.
- Advanced L7 ingress features.

Still worth tracking after the blocker work:

- Network policy enforcement in the dataplane.
- Richer XDP hardening beyond frame sanity checks.

## Current state summary

The current dataplane already has strong fundamentals:

- Deterministic VIP selection and backend rings are in place.
- Overlay VIP DNAT/SNAT works through `bridge_tc_ingress*` /
  `bridge_tc_egress*`.
- Public ingress works through `nodeport_tc_ingress` /
  `nodeport_tc_egress`.
- Node-local diagnostics exist through `mantissa info`.
- Privileged Linux tests exist for eBPF overlay, NodePort, and WireGuard.

The main remaining blockers are:

1. Conntrack hardening: the NAT path is a tuple cache, not a real connection
   tracker.
2. Flow capacity and eviction hardening: current map sizes are small enough to
   become a correctness problem under real traffic.
3. Source-IP contract: the dataplane currently rewrites external traffic into
   the host-access IP and does not preserve client IP.
4. Fragment / ICMP / PMTU handling: the production contract is incomplete.
5. Operator-facing configuration hardening: best-effort NodePort identity
   autodetection still exists and is too risky for production defaults.
6. Dataplane observability: current counters are useful, but not enough to
   debug flow eviction, reverse misses, or state churn.

## Code map

Primary control-plane and userspace programming code:

- `src/network/discovery.rs`
- `src/network/lb.rs`
- `src/network/nodeport.rs`
- `src/network/bpf/mod.rs`
- `src/network/controller.rs`
- `src/config.rs`
- `src/node/mod.rs`
- `crates/client/src/node/info.rs`
- `docs/networking-ebpf.md`
- `docs/configuration.md`

Primary dataplane code:

- `crates/network-ebpf/src/lib.rs`
- `crates/network-ebpf/src/bin/bridge_tc_ingress.rs`
- `crates/network-ebpf/src/bin/bridge_tc_egress.rs`
- `crates/network-ebpf/src/bin/bridge_tc_ingress_v6.rs`
- `crates/network-ebpf/src/bin/bridge_tc_egress_v6.rs`
- `crates/network-ebpf/src/bin/nodeport_tc_ingress.rs`
- `crates/network-ebpf/src/bin/nodeport_tc_egress.rs`
- `crates/network-ebpf/src/bin/vxlan_xdp.rs`
- `crates/network-ebpf/src/bin/bridge_xdp.rs`

Primary test and CI code:

- `tests/nodeport.rs`
- `tests/ebpf_overlay.rs`
- `tests/wireguard.rs`
- `tests/common/privileged_networking.rs`
- `tests/stress_large_cluster.rs`
- `.github/workflows/pr.yml`
- `.github/workflows/merge.yml`

## Definition of done

Call the eBPF / NodePort path production ready only when all of the following
are true:

- TCP and UDP public ingress use explicit, well-defined conntrack semantics.
- Reverse-path rewriting remains correct under sustained concurrency and backend
  churn.
- Flow capacity is tunable, observable, and tested under pressure.
- Public ingress has a clear source-IP contract and the product docs match the
  actual behavior.
- Fragment / ICMP / PMTU behavior is explicit, implemented, and tested.
- Production configuration does not rely on ambiguous interface autodetection.
- `mantissa info` and exported metrics can explain why a public endpoint is
  degraded.

## Phase 1: Freeze the production contract

Objective:

- Lock down what "supported" means before deep code changes start.

Implementation tasks:

- Decide and document the source-IP contract for the first production release.
  Options:
  - Option A: preserve source IP.
  - Option B: explicit SNAT to host-access IP, with operator-visible warnings.
  - Option C: configurable mode with one clear default.
- Decide whether fragmented IPv4 must be supported in the first production
  release or whether the release contract is "no fragmented ingress, but
  correct PMTU / ICMP behavior".
- Decide whether NodePort will require explicit `network.nodeport.iface` in
  production mode, or whether best-effort autodetect remains available only as
  a development fallback.
- Align the docs with the actual implementation status.
  - `docs/networking-ebpf.md`
  - `docs/configuration.md`
- Fix the current documentation inconsistency around IPv6 NodePort support.
  The code and tests already have IPv6-specific NodePort paths, while
  `docs/configuration.md` still says public traffic is IPv4-only.

Code touchpoints:

- `docs/networking-ebpf.md`
- `docs/configuration.md`
- `src/config.rs`
- `src/network/nodeport.rs`

Exit criteria:

- One explicit supported-scope section exists in the docs.
- One explicit operator contract exists for:
  - source IP,
  - public address selection,
  - fragment / PMTU behavior,
  - required host privileges and firewall responsibilities.

## Phase 2: Replace tuple-cache NAT with real conntrack semantics

Objective:

- Make the NAT path stateful enough that replies are only rewritten for valid
  flows and can be cleaned up correctly.

Current limitation:

- `Flow4` / `Flow6` are only 5-tuples, and the cached NAT values only carry the
  fields required for address rewriting.
- The TC programs do not inspect TCP flags.
- Any reverse packet with a matching reverse tuple gets rewritten.
- Flow creation is not restricted to valid first packets.

Relevant code today:

- Shared flow / NAT structs:
  - `crates/network-ebpf/src/lib.rs`
- Overlay dataplane:
  - `crates/network-ebpf/src/bin/bridge_tc_ingress.rs`
  - `crates/network-ebpf/src/bin/bridge_tc_egress.rs`
  - IPv6 variants
- Public NodePort dataplane:
  - `crates/network-ebpf/src/bin/nodeport_tc_ingress.rs`
  - `crates/network-ebpf/src/bin/nodeport_tc_egress.rs`

Implementation tasks:

- Add TCP header parsing helpers to `crates/network-ebpf/src/lib.rs`.
  - Introduce a minimal `TcpHeader`.
  - Add helpers for:
    - flags,
    - data offset,
    - SYN / ACK / FIN / RST detection.
- Extend NAT values in `crates/network-ebpf/src/lib.rs` so they can carry
  conntrack metadata.
  Minimum fields:
  - protocol,
  - flow direction metadata if needed,
  - TCP state,
  - last-seen timestamp or age bucket,
  - explicit teardown marker.
- Use `bpf_ktime_get_ns()` or equivalent helper to record activity timestamps in
  the dataplane state.
- Restrict TCP flow creation to valid first packets.
  Minimum rule:
  - create new TCP state only from SYN without ACK,
  - do not create state from stray ACK / FIN / RST packets.
- Restrict reverse-path rewrite to valid conntrack state transitions.
  Minimum rule:
  - reverse rewrite only if state exists and is in an allowed reverse state.
- Add FIN / RST handling so flows do not survive longer than needed.
- Use shorter aging for UDP than for TCP.
- Keep the overlay VIP path and NodePort path behavior aligned.
  Do not harden one and leave the other as a weaker tuple cache.

Suggested implementation order:

1. Add shared TCP parsing types and helpers in
   `crates/network-ebpf/src/lib.rs`.
2. Harden overlay VIP flow handling first.
   Files:
   - `bridge_tc_ingress.rs`
   - `bridge_tc_egress.rs`
   - `bridge_tc_ingress_v6.rs`
   - `bridge_tc_egress_v6.rs`
3. Port the same conntrack rules to the public NodePort path.
   Files:
   - `nodeport_tc_ingress.rs`
   - `nodeport_tc_egress.rs`
4. Expose enough state and counters to diagnose conntrack behavior.

Important design rule:

- Keep the data structure and state machine as small as possible. Do not build a
  generic Linux-conntrack clone. The release only needs enough state to make
  SNAT / DNAT correct for Mantissa's public ingress and VIP flows.

Exit criteria:

- Stray reverse packets do not get rewritten unless a valid tracked flow exists.
- TCP flow creation is gated by SYN.
- TCP teardown removes or ages out state predictably.
- UDP state uses bounded aging.
- Overlay VIP and NodePort use the same semantic contract.

## Phase 3: Add explicit flow cleanup and churn handling

Objective:

- Ensure backend changes, service removal, and public-port remaps cannot leave
  stale flow state behind.

Current limitation:

- Overlay VIP flow cleanup exists in `src/network/lb.rs` through
  `clear_vip_flows_v4` / `clear_vip_flows_v6`.
- NodePort currently has no equivalent public-flow cleanup path for
  `NODEPORT_FWD` / `NODEPORT_REV`.

Implementation tasks:

- Add NodePort flow-clear helpers to `src/network/nodeport.rs`.
  Mirror the patterns already used in `src/network/lb.rs`.
- Clear NodePort flows when:
  - a public service is removed,
  - the backing VIP changes,
  - the published node IP changes,
  - the public port changes,
  - the protocol set changes,
  - the host-access attachment for a network is removed,
  - the NodePort manager reattaches with a different interface identity.
- Audit service discovery refresh behavior in `src/network/discovery.rs` so
  public endpoint reconciliation and flow cleanup stay in sync.
- Add explicit regression tests for:
  - service delete,
  - port remap,
  - backend churn,
  - daemon restart with persisted services.

Code touchpoints:

- `src/network/nodeport.rs`
- `src/network/lb.rs`
- `src/network/discovery.rs`
- `tests/nodeport.rs`
- `tests/ebpf_overlay.rs`

Exit criteria:

- Public flow state is cleaned up when the owning service or mapping changes.
- Backend ring churn does not leave stale reverse-flow behavior behind.

## Phase 4: Capacity sizing, tunables, and eviction observability

Objective:

- Make flow-map pressure visible and controllable.

Current limitation:

- Overlay flow maps are hard-coded to 1024 entries.
- NodePort flow maps are hard-coded to 2048 entries.
- Capacity breaches today are mostly silent until behavior degrades.
- Current runtime status shows capacities, but not occupancy, misses, or
  evictions.

Implementation tasks:

- Make the following capacities configurable:
  - overlay forward flow map size,
  - overlay reverse flow map size,
  - NodePort forward flow map size,
  - NodePort reverse flow map size,
  - NodePort VIP map size if needed,
  - NodePort host map size if needed.
- Decide whether capacities are:
  - build-time constants,
  - loader-time overrides,
  - or config-file driven loader-time overrides.
- If using loader-time overrides:
  - update BPF loading paths in:
    - `src/network/bpf/mod.rs`
    - `src/network/nodeport.rs`
- Extend diagnostics to report:
  - current configured capacities,
  - current estimated occupancy,
  - eviction count,
  - reverse-flow miss count,
  - insert failure count,
  - flow-clear count.
- Keep `mantissa info` useful, but also add exported metrics so operators do
  not need interactive access to debug production issues.

Suggested config additions:

- `network.bpf.overlay_flow_capacity`
- `network.nodeport.flow_capacity`
- `network.nodeport.vip_capacity`
- `network.nodeport.host_capacity`

Suggested code touchpoints:

- `src/config.rs`
- `src/network/bpf/mod.rs`
- `src/network/nodeport.rs`
- `src/node/mod.rs`
- `crates/client/src/node/info.rs`
- `crates/network-ebpf/src/bin/bridge_tc_ingress.rs`
- `crates/network-ebpf/src/bin/bridge_tc_egress.rs`
- `crates/network-ebpf/src/bin/nodeport_tc_ingress.rs`
- `crates/network-ebpf/src/bin/nodeport_tc_egress.rs`

Test work:

- Add a privileged map-pressure test that intentionally exceeds the current
  flow-map capacity and verifies:
  - the eviction / miss counters move,
  - the service degrades in a visible way,
  - recovery occurs once the pressure disappears.

Exit criteria:

- Flow capacities are configurable.
- Evictions and reverse misses are visible through diagnostics and metrics.
- Pressure testing exists for both overlay VIP and public NodePort paths.

## Phase 5: Decide and implement the source-IP contract

Objective:

- Stop treating source-IP behavior as an implicit side effect of routing.

Current limitation:

- `nodeport_tc_ingress.rs` rewrites external traffic into the overlay's
  host-access IP so replies are routable.
- This makes the current behavior "SNAT to host-access", but it is not framed
  as a first-class product contract.

Implementation options:

- Option A: keep SNAT as the first production contract.
  This is the lowest-risk option if correctness and operator clarity are the
  immediate priority.
- Option B: preserve client IP.
  This is more desirable from an operator point of view, but requires careful
  return-path guarantees across the overlay.
- Option C: make source mode configurable.

Recommended first production path:

- Option A or C.
- Do not attempt client-IP preservation unless return-path correctness is
  guaranteed for both local and remote backends.

Implementation tasks:

- Introduce an explicit config field for source-IP mode if configurable.
  Candidate name:
  - `network.nodeport.source_mode = snat_host_access | preserve_client`
- Surface the selected mode in:
  - `mantissa info`
  - configuration docs
  - service public-endpoint detail when degraded
- If preserving client IP:
  - add multi-node tests where the selected backend is remote,
  - verify the backend sees the original client IP,
  - verify replies still leave through the correct node path.
- If keeping SNAT:
  - document it unambiguously,
  - add tests that assert the backend sees the host-access source and not the
    original client IP.

Code touchpoints:

- `src/config.rs`
- `src/network/nodeport.rs`
- `crates/network-ebpf/src/bin/nodeport_tc_ingress.rs`
- `crates/network-ebpf/src/bin/nodeport_tc_egress.rs`
- `docs/networking-ebpf.md`
- `docs/configuration.md`
- `tests/nodeport.rs`

Exit criteria:

- Source-IP behavior is explicit, configurable or intentionally fixed, and
  tested.

## Phase 6: Fragment, ICMP, and PMTU hardening

Objective:

- Make the ingress contract correct for non-happy-path networking.

Current limitation:

- The overlay and NodePort NAT paths currently ignore fragmented IPv4 and do not
  implement a complete ICMP / PMTU story.

Implementation tasks:

- Decide the minimum production contract:
  - Full fragmented IPv4 support, or
  - explicit rejection plus PMTU-safe operation.
- Add explicit drop reasons for:
  - fragmented IPv4,
  - unsupported transport,
  - reverse-flow miss if treated as drop,
  - invalid TCP state transition if conntrack hardening is added.
- Add ICMP / PMTU handling appropriate for the chosen contract.
  Minimum acceptable behavior:
  - no silent blackholing for oversized TCP paths,
  - clear diagnostics when unsupported traffic is dropped.
- Consider MSS clamping if needed for NodePort and overlay public traffic.
- Validate the effect of WireGuard / VXLAN MTU on public flows, especially when
  the selected backend is remote.

Code touchpoints:

- `crates/network-ebpf/src/lib.rs`
- `crates/network-ebpf/src/bin/nodeport_tc_ingress.rs`
- `crates/network-ebpf/src/bin/nodeport_tc_egress.rs`
- `crates/network-ebpf/src/bin/bridge_tc_ingress.rs`
- `crates/network-ebpf/src/bin/bridge_tc_egress.rs`
- IPv6 bridge ingress for neighbor handling:
  - `crates/network-ebpf/src/bin/bridge_tc_ingress_v6.rs`
- userspace status plumbing:
  - `src/network/nodeport.rs`
  - `src/node/mod.rs`
  - `crates/client/src/node/info.rs`

Test work:

- Add explicit fragment rejection tests if fragments stay unsupported.
- Add PMTU / large-payload tests.
- Add remote-backend tests over WireGuard-enabled underlay once the privileged
  lane is running in CI.

Exit criteria:

- Fragment and PMTU behavior is explicit, observable, and tested.

## Phase 7: Harden production identity selection and config validation

Objective:

- Remove ambiguous publication identity selection from the production path.

Current limitation:

- `src/network/nodeport.rs` still supports best-effort autodetection by picking
  the first up non-loopback interface with a usable address.
- The docs already say production operators should set
  `network.nodeport.iface` and usually `network.nodeport.ip`.

Implementation tasks:

- Add stricter validation in `src/config.rs` for production NodePort.
  Candidate rules:
  - if NodePort is enabled and the node publishes public services, require
    `network.nodeport.iface`,
  - require either `network.nodeport.ip` or `network.advertise_addr`,
  - reject ambiguous family mismatches early,
  - reject loopback interface except in explicit test mode.
- Keep best-effort autodetect only for:
  - local development,
  - tests,
  - or the built-in best-effort autodetect fallback.
- Surface the resolved production identity and the reason it was chosen in
  diagnostics.

Code touchpoints:

- `src/config.rs`
- `src/network/nodeport.rs`
- `docs/configuration.md`
- `docs/networking-ebpf.md`
- `tests/nodeport.rs`

Exit criteria:

- Production nodes do not silently pick the wrong public interface.
- Ambiguous NodePort identity selection becomes a startup validation failure or
  explicit degraded state.

## Phase 8: Expand diagnostics and metrics

Objective:

- Make NodePort and VIP failures diagnosable from operator telemetry.

Implementation tasks:

- Extend `NodePortStatus` with:
  - source mode,
  - flow occupancy if available,
  - eviction count,
  - reverse miss count,
  - invalid-state transition count,
  - fragment drop count,
  - flow-clear count,
  - selected-public-identity source:
    - explicit `nodeport.ip`,
    - derived from `advertise_addr`,
    - autodetected.
- Add equivalent overlay VIP dataplane counters where needed.
- Wire the new counters through:
  - `src/network/nodeport.rs`
  - `src/node/mod.rs`
  - `crates/client/src/node/info.rs`
- Add exported metrics endpoint coverage for the same high-signal counters.
  Even if the broader repository metrics work lands separately, this dataplane
  should not rely only on `mantissa info`.

Suggested metrics:

- `mantissa_nodeport_flow_evictions_total`
- `mantissa_nodeport_reverse_misses_total`
- `mantissa_nodeport_nat_insert_failures_total`
- `mantissa_nodeport_fragment_drops_total`
- `mantissa_nodeport_conntrack_invalid_transitions_total`
- `mantissa_nodeport_program_attach_failures_total`
- `mantissa_lb_flow_evictions_total`
- `mantissa_lb_reverse_misses_total`

Exit criteria:

- Operators can distinguish configuration mistakes, attach failures, flow
  pressure, reverse misses, and protocol unsupported cases.

## Future work: privileged CI and release validation

Objective:

- Capture the eventual CI shape for the privileged dataplane without making it a
  blocker for the current production-hardening work.

Current limitation:

- `tests/common/privileged_networking.rs` gates privileged tests behind
  `MANTISSA_RUN_NETWORKING_TESTS`.
- CI workflows install the BPF toolchain but do not enable the privileged lane.

Deferred plan:

- Add a privileged Linux CI job on self-hosted GitHub Actions runners that
  runs:
  - `tests/nodeport.rs`
  - `tests/ebpf_overlay.rs`
  - `tests/wireguard.rs`
- Set `MANTISSA_RUN_NETWORKING_TESTS=1` in that job.
- Ensure the runner executes as root or under a setup that satisfies the
  current privileged test assumptions.
- Keep the existing general `cargo test` lane, but add an explicit
  privileged-dataplane lane rather than burying it inside the generic suite.
- Add longer-running soak coverage for:
  - sustained TCP connections,
  - sustained UDP flows,
  - backend churn during active traffic,
  - service delete during active traffic,
  - daemon restart while public traffic is active,
  - map-pressure / eviction behavior,
  - remote-backend NodePort over WireGuard.

Code touchpoints:

- `.github/workflows/pr.yml`
- `.github/workflows/merge.yml`
- `tests/common/privileged_networking.rs`
- `tests/nodeport.rs`
- `tests/ebpf_overlay.rs`
- `tests/wireguard.rs`

## Phase 10: Decide what to do about network policy

Objective:

- Explicitly mark this as deferred or implement a minimal first step.

Recommendation for this release:

- Do not block release on full network policy enforcement.
- Document clearly that this first production release is for single-trust-domain
  clusters.
- Track policy work separately after conntrack hardening is complete.

If policy work is started later, likely touchpoints are:

- `src/network/types.rs`
- protocol schemas under `crates/protocol/schema/network.capnp` if policy
  becomes part of replicated network specs
- `src/network/discovery.rs`
- TC / XDP programs under `crates/network-ebpf/src/bin/*`

## Test plan by phase

### Local correctness checkpoints

After each phase, run:

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

### Existing privileged dataplane suite

Use a privileged Linux environment:

```bash
MANTISSA_RUN_NETWORKING_TESTS=1 cargo test --test nodeport --features testkit -- --nocapture
MANTISSA_RUN_NETWORKING_TESTS=1 cargo test --test ebpf_overlay --features testkit -- --nocapture
MANTISSA_RUN_NETWORKING_TESTS=1 cargo test --test wireguard --features testkit -- --nocapture
```

### New tests to add

Add to `tests/nodeport.rs`:

- long-lived TCP connection survives normal traffic churn
- UDP state ages out correctly
- reverse-flow miss is observable
- map-pressure / eviction behavior
- source-IP contract assertion
- restart with active public service restores NodePort state correctly
- remote-backend NodePort over multi-node cluster

Add to `tests/ebpf_overlay.rs`:

- overlay LB flow eviction pressure
- backend-ring change while traffic is active
- large-payload / PMTU case

Add to `tests/wireguard.rs` or a new privileged networking test:

- remote NodePort through WireGuard underlay
- restart during encrypted public traffic

### Release checkpoint

Do not call the dataplane production ready until these all pass:

```bash
cargo test --workspace --lib --bins --tests --examples --features testkit
MANTISSA_RUN_NETWORKING_TESTS=1 cargo test --test nodeport --features testkit -- --nocapture
MANTISSA_RUN_NETWORKING_TESTS=1 cargo test --test ebpf_overlay --features testkit -- --nocapture
MANTISSA_RUN_NETWORKING_TESTS=1 cargo test --test wireguard --features testkit -- --nocapture
```

## Suggested implementation order

This is the recommended sequence to minimize thrash:

1. Freeze the release contract and clean up the docs.
2. Add shared TCP parsing and conntrack data structures.
3. Harden overlay VIP flow tracking.
4. Harden NodePort flow tracking.
5. Add explicit NodePort flow cleanup on churn and removal.
6. Add configurable flow capacities and observability.
7. Finalize source-IP behavior and tests.
8. Finalize fragment / ICMP / PMTU behavior and tests.
9. Tighten config validation for production public identity.
10. Revisit privileged CI later, once self-hosted GitHub Actions runners are
    available.

## Commit-sized work breakdown

These are good atomic steps to implement and validate separately:

1. docs/config: freeze NodePort production contract
2. ebpf: add shared TCP parsing and conntrack state types
3. network-lb: harden overlay VIP flow tracking
4. nodeport: harden public-flow tracking
5. nodeport: add flow cleanup on mapping churn
6. network: add flow capacity config and diagnostics
7. nodeport: make source-IP mode explicit
8. network: implement PMTU / fragment handling contract
9. tests: add privileged flow-pressure and restart coverage
10. ci: add privileged dataplane lane on self-hosted runners later

## Open design questions to resolve before coding

- Should the first production contract preserve client IP or explicitly SNAT?
- Must fragmented IPv4 be supported now, or is explicit reject + PMTU handling
  enough for the first release?
- Are flow capacities runtime-configurable, or do we intentionally freeze them
  at loader time for simplicity?
- Does production NodePort require explicit `network.nodeport.iface`, or do we
  keep autodetect behind an unsafe development flag?
- Is a metrics endpoint added as part of this work, or does the dataplane only
  expose richer status while the broader metrics work lands separately?

## Final release checklist

- [ ] Source-IP contract chosen and documented
- [ ] Conntrack semantics implemented for overlay VIP path
- [ ] Conntrack semantics implemented for NodePort path
- [ ] Flow cleanup exists for public-port churn and service removal
- [ ] Flow capacities are tunable and observable
- [ ] Reverse misses and evictions are visible
- [ ] Fragment / PMTU behavior is explicit and tested
- [ ] Production config rejects ambiguous public identity
- [ ] `mantissa info` surfaces the final dataplane state clearly
- [ ] Docs match the real supported scope
