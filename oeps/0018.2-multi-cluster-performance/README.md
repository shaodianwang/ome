# OEP-0018.2: Multi-cluster performance hardening + endpoint publisher

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Terminology](#terminology)
  - [The shape of the fix: wire the layer that's already designed](#the-shape-of-the-fix-wire-the-layer-thats-already-designed)
- [Design Details](#design-details)
  - [Connection-layer performance](#connection-layer-performance)
  - [Placement reconciler scale](#placement-reconciler-scale)
  - [Event-driven status (the watch-funnel)](#event-driven-status-the-watch-funnel)
  - [Fan-out breadth](#fan-out-breadth)
  - [Endpoint publisher](#endpoint-publisher)
  - [Multi-node gang](#multi-node-gang)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** OEP-0018 shipped a faithful, defensively-coded alpha of Kueue MultiKueue's external-reconciler model — the placement/fan-out *control logic* is correct, in places stricter than the OEP. What it did not ship is the **performance layer**: every cross-cluster read is a **direct, uncached apiserver round-trip on a 30s poll**, remote clients run at **client-go defaults (5 QPS / 10 burst)**, the placement reconciler runs at **concurrency 1**, and the endpoint publisher is **status-only** (no DNS/Gateway), leaving OEP-0018's own Alpha graduation bar unmet.

This OEP hardens OME multi-cluster to MultiKueue-grade performance and closes the endpoint gap. The headline finding from studying Kueue MultiKueue's connection layer is that **most of the fix was already written in-tree and simply not wired** — the selectively-caching remote client existed (Pod-banned, tested) but `buildRemoteClient` hardcoded `NewNeverCachingClient`. So the highest-leverage changes are low-effort: connect what's built, then add the event-driven and breadth layers.

**No CRD or `InferenceService` API change.** The work is in the connection layer (`pkg/controller/v1beta1/workloadcluster`), the placement reconciler (`pkg/controller/v1beta1/placement`), and a new endpoint publisher (`pkg/controller/v1beta1/placement/endpoint`).

## Motivation

Three named failure modes at fleet scale:

1. **Uncached poll amplification.** `buildRemoteClient` returned `NewNeverCachingClient`, so every per-poll `getDerived` / `findWinner` per-cluster GET / loser pre-GET is a direct remote apiserver round-trip, repeated every 30s per ISVC. With N candidate clusters that is O(N) remote GETs per ISVC per poll, and status convergence is bounded below by the 30s poll, not by events.
2. **Throttled and serialized.** Remote clients inherit client-go's 5 QPS / 10 burst, so a cache cold-sync, the GC's paged List, or a reconcile burst throttles client-side; and the placement reconciler runs at concurrency 1 (no `MaxConcurrentReconciles`), so all ISVC placement serializes behind one worker — multiplying the uncached latency above.
3. **Not a product end-to-end.** The endpoint publisher only writes `status.placement.endpoint`; nothing programs a global DNS record or Gateway route to the winner. OEP-0018's Alpha criterion ("endpoint publication for one concrete global-traffic backend") is therefore unmet, and "OME owns the global endpoint" is silently inverted into "an external LB reads status".

### Goals

- **MultiKueue-grade connection layer:** a selectively-caching remote client (cache the low-cardinality derived `InferenceService`, never Pods), tuned QPS/Burst + per-call timeouts, and a two-backoff reconnect scheme.
- **Event-driven status:** a remote watch-funnel so winner/loss/admission converge on events, not a 30s poll.
- **A scalable placement reconciler:** configurable concurrency, an indexed cluster-change re-enqueue (no fleet-wide List), per-cluster fault isolation, and a winner-lost grace window before re-placement.
- **A real endpoint publisher** for one concrete backend (Gateway API HTTPRoute), behind a pluggable interface.
- **No `InferenceService`/CRD API change**; every new value config-driven with no in-code magic default (the repository's no-magic-values rule).

### Non-Goals

- **Global *pooled* quota / cross-cluster borrowing.** Stays per-cluster (OEP-0018 Non-Goal); unchanged here.
- **A new control-plane API surface.** No new CRD; the winner-lost grace clock is intentionally in-memory rather than a new `PlacementStatus` timestamp field.
- **Multi-cluster *load-balanced* serving.** Still single-active; this OEP makes single-active fast + complete, not multi-active.

## Proposal

### Terminology

- **Derived ISVC:** the clone the control plane creates on a candidate cluster (OEP-0018), tagged with the `ome.io/placement-origin` label for ownership/GC.
- **Selectively-caching client:** a remote client that runs an informer cache for a small allow-list of kinds (here: the derived `InferenceService`) and reads everything else live — banning a Pod informer by construction. (`workloadcluster/remoteclient.go`)
- **Watch-funnel:** a remote watch whose events are resolved to the LOCAL source ISVC key and pushed onto a small buffered channel consumed via `source.Channel` — the event-driven alternative to polling (MultiKueue `multikueuecluster.go`).
- **Two-backoff:** an *establish* backoff (1m→10m) bounding a single hung `Watch()` call, distinct from a *retry* backoff (0→5m20s) pacing reconnect attempts.

### The shape of the fix: wire the layer that's already designed

The load-bearing observation:

> The OEP-0018 architecture is right; the performance code is mostly written but unwired. The first and biggest win is to **connect the selectively-caching client to the live path** — turning every per-poll cross-cluster GET from a remote round-trip into a local cache read — not to re-architect.

## Design Details

Status legend: **[done]** implemented; **[planned]** second wave (depends on [done] work).

### Connection-layer performance
**[done]** `buildRemoteClient` now builds the selectively-caching client when cached kinds are configured (cached kind = derived `InferenceService` only; the Pod-ban and cache-synced gating are preserved), with a cancel wired into `Disconnect` for teardown. The cached kinds + the `ome.io/placement-origin` selector are *injected* via `SetCacheOptions` rather than imported, because `workloadcluster` must not import `placement` (which already imports it) — also exactly the no-magic-values pattern. A new `ClientTuning` (QPS/Burst/PerCallTimeout) is applied to the `rest.Config`; a two-backoff (establish 1m→10m racing a `Watch` timeout, retry 0→5m20s) now spaces reconnect requeues. Timing knobs (health interval, connection grace, events-batch period, reconnect backoff) are functional options on `SetupWithManager`.

A subtlety to settle at wiring time: once the derived `InferenceService` is a cached kind, the *same* per-cluster client is shared with the GC's remote derived-list — served from the cache, which is safe (origin-scoped + the empty-live-set guard) but can transiently under-report on a cold cache (it would just skip a delete that passes next cycle). The control-plane live-UID list stays uncached (it uses the GC's `APIReader`).

### Placement reconciler scale
**[done]** Adds `MaxConcurrentReconciles`; replaces `isvcsForClusterChange`'s unscoped List-of-all-ISVCs with a field-indexed lookup (`ome.io/placement-eligible`) so a WorkloadCluster change re-enqueues only ISVCs whose candidate set includes (or just lost) the changed cluster; snapshots the connected set and bounds each `placeOn` with a per-cluster timeout so one wedged remote can't block healthy peers; and adds a configurable winner-lost grace window — distinguishing a *disconnected* winner (hold, don't arm the clock) from a derived *absent on a connected* winner (re-place only after grace). The grace clock is in-memory (keyed by source UID), strictly conservative (only ever delays a re-race).

### Event-driven status (the watch-funnel)
**[planned]** Register a remote event handler on the cached derived-ISVC informer, resolve each event to the LOCAL source ISVC key, push onto a small buffered channel consumed via `source.Channel` with a ~1s `AddAfter` batch delay (mirror MultiKueue `wlUpdateCh`). Then drop the 30s poll for status convergence (keep a long safety requeue). Depends on the cache being wired (done); `Manager.EstablishWatch` is already exported for this consumer.

### Fan-out breadth
**[planned]** Split nomination from execution: a `status.placement` nominated-cluster list written by a nomination step, with the existing clone/delete executor acting on it; add an `Incremental` dispatcher (batches of N, one round-timeout) alongside `AllAtOnce` to bound remote churn at high cluster counts (mirror Kueue `incrementaldispatcher.go`).

### Endpoint publisher
**[done]** New package `placement/endpoint` with an `EndpointPublisher` interface + a Gateway API HTTPRoute backend: it programs the global host to resolve to the current winner's ingress (an `ExternalName` Service aliasing the off-cluster winner host + an HTTPRoute on the configured global gateway), repoints on re-placement, and cleans up via a finalizer. All values config-driven; an unconfigured global gateway degrades to a no-op. Caveats (ExternalName backend support, cross-namespace `ReferenceGrant`, the winner URL being externally resolvable) are documented at the call site and remain the OEP-0018 "largest integration unknown".

### Multi-node gang
**[planned / open]** OME's scheduler-plugins `PodGroup` and Kueue's pod-group both gate a Size>1 Instance's pods; their cooperation is untested and is OEP-0018's Alpha-blocking open question. No code yet; must be proven before Alpha for PD-disaggregated/multi-node.

## Test Plan

- **Multi-cluster envtest (done):** 1 control + 2 member envtest apiservers in one process; a source ISVC fans out to both members, winner selection + loser deletion + status flowback, re-placement when the winner's derived is deleted, and negative kubeconfig paths — all hermetic, no KIND/Docker. This is the local bed for every change above. (Framework details in OEP-0020.)
- **Performance harness (done):** the local envtest + fake-executor + `rangespec.yaml` checker (OEP-0020) gives a regression gate for reconcile throughput as the cache/concurrency changes land.
- **KIND e2e (planned, authoritative):** the ≥2-cluster suite against real Kueue (race, preemption, gated-wait, winner-loss re-placement, multi-node gang) — the paths that only reproduce against real Kueue.

## Graduation Criteria

- **Alpha:** the [done] connection-layer + placement-scale + endpoint publisher landed and wired into the control-plane manager; the multi-cluster envtest suite green and gating; one concrete endpoint backend working.
- **Beta:** the watch-funnel replaces the status poll; Incremental dispatcher available; the KIND e2e green and gating; multi-node gang proven.
- **Stable:** soak at fleet scale with the perf-harness regression gate calibrated on CI hardware; a recorded decision on pooled quota (defer to the MultiKueue Cohort path or not).

## Drawbacks

- **Cache memory per cluster.** An informer over the derived `InferenceService` per worker cluster costs memory ∝ (#clusters × #derived); bounded because only that one low-cardinality kind is cached (Pods are banned) and scoped by the origin label.
- **Behavior change: winner-lost is no longer instant.** Re-placement now waits the grace window on a connected-but-absent winner (set grace=0 to restore the old behavior); two existing placement tests were updated to the new contract.
- **Endpoint backend portability.** The HTTPRoute + ExternalName approach is vendor-neutral but some Gateway implementations restrict ExternalName backends or need a `ReferenceGrant`; the interface lets a DNS/GSLB backend replace it.

## Alternatives

1. **Leave it polling + uncached (status quo).** Rejected: O(N) remote GETs per ISVC per 30s at 5 QPS, concurrency 1 — does not scale past a small fleet.
2. **Adopt MultiKueue wholesale** instead of porting its practices. Rejected in OEP-0018 (cedes the brain + ISVC surgery); this OEP keeps OME's control logic and only borrows the connection/perf patterns.
3. **A new `PlacementStatus` timestamp for durable winner-lost grace** (vs the in-memory clock). Deferred: the in-memory clock is strictly conservative and avoids an API change; revisit if durable grace across control-plane restarts is required.

## Open Questions

- **Watch-funnel cutover.** When the funnel lands, how long a safety requeue do we keep behind it, and do we feature-gate the poll→event switch? Owner: TBD.
- **GC vs cache.** Should the GC's remote derived-list bypass the cache (a dedicated uncached client) or accept the cold-cache under-report (safe, self-correcting)? Decide at wiring. Owner: TBD.
- **Endpoint backend.** Is HTTPRoute+ExternalName sufficient for the target environments, or is a DNS/GSLB backend needed first? This is OEP-0018's largest integration unknown. Owner: TBD.
- **Multi-node gang.** Do `PodGroup` and Kueue's pod-group cooperate for a Size>1 Instance, or deadlock/double-count? Must be proven before Alpha. Owner: TBD.

See also: OEP-0018 (the alpha this hardens), OEP-0018.1 (the sibling fractional-admission scale-out follow-up — a complementary axis), and OEP-0020 (the multi-cluster + perf test beds).
