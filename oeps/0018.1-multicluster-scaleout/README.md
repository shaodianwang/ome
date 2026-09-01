# OEP-0018.1: Multi-Cluster Horizontal Scale-Out via Fractional Admission

> **Status: largely implemented.** This is a follow-up to OEP-0018. The base it
> depends on is fixed and validated, and fractional placement itself has since
> shipped as `spec.placement.mode: Split` under OEP-0018.3. Read this document
> as the design rationale for that behavior; see
> [Current state](#current-state) for what remains.

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Terminology](#terminology)
  - [The model: still one ISVC; the control plane now fans it out fractionally](#the-model-still-one-isvc-the-control-plane-now-fans-it-out-fractionally)
  - [Why fractional admission (not a central planner)](#why-fractional-admission-not-a-central-planner)
  - [The gang-boundary invariant](#the-gang-boundary-invariant)
  - [What is new vs OEP-0018 — and what is not](#what-is-new-vs-oep-0018--and-what-is-not)
  - [User stories](#user-stories)
- [Design Details](#design-details)
  - [Objects / API](#objects--api)
  - [Components on the control-plane cluster](#components-on-the-control-plane-cluster)
  - [The fractional fan-out algorithm](#the-fractional-fan-out-algorithm)
  - [Global replica accounting + apportioned autoscaling](#global-replica-accounting--apportioned-autoscaling)
  - [The weighted global load-balanced endpoint](#the-weighted-global-load-balanced-endpoint)
  - [Re-sharding on cluster loss / capacity shift](#re-sharding-on-cluster-loss--capacity-shift)
  - [Cross-cluster transport](#cross-cluster-transport)
  - [End-to-end flow](#end-to-end-flow)
  - [Failure modes and re-sharding](#failure-modes-and-re-sharding)
  - [Dependencies](#dependencies)
- [Current state](#current-state)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** OEP-0018 places an ISVC on **one** cluster — the first whose Kueue
admits the whole thing wins, the losers are deleted. That works only while the
desired replica count fits in a single cluster. It does not for a service that
needs **more replicas than any one cluster can hold**: e.g. 1000 pods ≈ 125
replicas of an 8-pod tensor-parallel gang, which no single GPU cluster in our
fleet has room for. This OEP generalizes 0018's fan-out + race from **binary to
fractional**: clone the ISVC with a replica count to every candidate cluster;
each cluster's Kueue admits **as many replicas as fit**; **keep every cluster
that admitted ≥1** (do not delete the partial winners); sum admitted replicas
across clusters until the desired N is met; and load-balance one global endpoint
across all admitting homes, **weighted by ready replicas**.

This lifts OEP-0018's explicit non-goal — "multi-cluster *load-balanced
serving*" — and nothing else. It stays faithful to 0018's load-bearing
principle: **Kueue admission *is* the capacity signal; there is no central
capacity reader and no central planner.** Fractional admission is the same
self-correcting race, run per-replica instead of per-service.

One hard invariant draws the boundary of "fractional": **we split only at the
replica boundary, never within a gang.** A tensor- or pipeline-parallel
Instance's collectives (NVLink / ICI / InfiniBand) cannot cross a cluster, so a
single Instance must land whole on one cluster — see
[the gang-boundary invariant](#the-gang-boundary-invariant). "Fractional" means
fractions of N **replicas**, never fractions of one gang.

The ISVC stays the single user-authored unit. The user does **not** author N
ISVCs or pick clusters; the control plane derives per-cluster sub-deployments
with apportioned replica counts internally. The model is still "the ISVC is the
unit, the control plane fans it out" — now to several homes at once instead of
one.

## Motivation

OEP-0018 solved *where* a service runs when it fits somewhere. It deliberately
punted on *what if it fits nowhere whole*. Three failure modes follow from
single-active once a service outgrows a cluster:

1. **A large service is unschedulable even though the fleet has room.** Consider
   an 8-pod-gang model that needs 125 replicas (1000 pods). No single cluster
   has 1000 free GPUs of the right flavor, but three clusters with ~400 free
   each collectively do. Under 0018, every candidate's Kueue gates the whole
   125-replica clone forever (it never fits), so **the service never starts** —
   despite 1200 GPUs of headroom across the fleet. Single-active turns "enough
   capacity, wrong shape" into a hard outage.
2. **No way to absorb a regional capacity spike.** When one region's GPUs free
   up faster than another's, single-active can't lean into it: a service is
   pinned to its one home and can't burst the marginal replicas elsewhere. The
   fleet's aggregate elasticity is invisible to a single-home service.
3. **No serving-HA, even incidentally.** 0018 says so explicitly ("single-active
   has no in-flight resilience"): preemption or cluster loss means a re-fan-out,
   i.e. a serving gap with no second home already serving. Fractional placement
   gives partial resilience for free — losing one of three homes degrades
   capacity, it does not zero it.

The platform constraint from 0018 is unchanged and still binding: **OME owns the
multi-cluster control plane; OME does not reinvent GPU quota (Kueue does that).**
This OEP keeps that line exactly. It adds spread across homes; it adds no central
capacity engine.

### Goals

- **Fractional placement across ≥2 homes.** Apportion an ISVC's desired N
  replicas across all candidate clusters that admit ≥1, summing admitted
  replicas until N is met. A service that fits in no single cluster but fits
  across the fleet runs.
- **Produce the weighted-home routing contract.** The control plane emits, and
  keeps fresh, each home's per-ingress backend plus a weight tracking its *ready*
  replicas — the exact data a global load balancer needs to split traffic across
  homes in proportion to ready capacity (not round-robin, not single-home-resolve).
  Actually balancing traffic from that contract is done by a global-traffic
  *consumer* — most likely the **existing front-tier** (an API-proxy / router
  fleet) pointed at `homes[]`, or client-side LB. OME natively programming a
  GSLB/Gateway/mesh is an **optional** follow-up OEP, not something this OEP builds
  or blocks on. This OEP owns the contract + a pluggable publisher seam (default
  no-op), not the backend.
- **Kueue stays the only capacity signal.** Each cluster's Kueue still decides
  how many replicas fit, by admitting that many pods. OME reads no capacity, runs
  no scoring, no quota math. Fractional = per-replica race; same self-correcting
  property as 0018.
- **The ISVC stays the single user-authored unit.** Additive API only: an opt-in
  toggle plus per-cluster min/max knobs in spec, and `status.placement` becomes a
  list of homes. OME never rewrites the user's ISVC into multiple ISVCs.
- **The gang-boundary invariant is enforced, not assumed.** Splitting happens at
  the replica boundary only; an Instance (gang) is never split across clusters.

### Non-Goals

- **Splitting a gang across clusters.** A tensor/pipeline-parallel Instance's
  collectives can't cross a cluster fabric. Hard out — and a correctness
  invariant, not a deferral (see [the gang-boundary invariant](#the-gang-boundary-invariant)).
- **Global *pooled* quota / cross-cluster borrowing / global fair-share.**
  Unchanged from 0018: quota is per-cluster (each Kueue `ClusterQueue`
  independently). Fractional placement consumes per-cluster quota; it does not
  pool it. The MultiKueue Cohort path remains the escape hatch if pooling
  becomes mandatory (see [Alternatives](#alternatives)).
- **A central global scheduler / bin-packer.** We do **not** compute an optimal
  N-across-K assignment centrally. Apportionment is emergent from per-cluster
  Kueue admission, exactly as 0018's single winner is.
- **Cross-cluster KV-cache / session affinity / stateful routing.** v1 weighted
  load-balancing is stateless request distribution. Sticky sessions, prefix-cache
  locality across homes, and PD-disaggregated cross-cluster KV transfer are out.
- **Replacing single-active.** 0018's single-active is the degenerate
  one-home case of this design and stays the default. Scale-out is opt-in.
- **Building / natively programming the global-traffic backend.** This OEP produces
  the weighted-home routing *contract* and a pluggable publisher seam; the contract
  is consumed by the existing front-tier (or client-side LB). Having OME *natively*
  program a GSLB / multi-cluster Gateway / mesh is an **optional** follow-up OEP,
  not required for 0018.1. See
  [the weighted endpoint](#the-weighted-global-load-balanced-endpoint).
- **Automatic global autoscaling.** Computing/driving desired N from an aggregated
  cross-home load signal, and home consolidation on load decline. Desired N is set
  externally (spec / operator / schedule); the apportionment loop reacts to N
  changes and per-home scalers run within their clamped share. The one strong use
  case (diurnal consolidation) is largely covered by a scheduled N change, and
  cross-cluster scale-up is too slow to chase bursts — so the signal aggregation is
  deferred to a follow-up (see [Global replica accounting](#global-replica-accounting--apportioned-autoscaling)).
- **Model staging / weight replication at placement time.** Unchanged from 0018
  and OEP-0009: weights are pre-staged on all candidates; a cluster without the
  model is not a candidate.

## Proposal

### Terminology

Inherits all of OEP-0018's terms (control-plane cluster, workload cluster,
candidate, derived ISVC, admission gate). New or redefined here:

- **Home:** a candidate cluster that admitted **≥1 replica** of a derived ISVC
  and is kept (i.e. not deleted as a "loser"). Under 0018 there is exactly one
  home; under this OEP there are one or more. `status.placement` enumerates the
  homes.
- **Apportioned replica count:** the number of replicas the fan-out controller
  *requests* on a given candidate (the derived ISVC's `replicas` there).
  Distinct from **admitted replicas** — how many of those the cluster's Kueue
  actually let run — and from **ready replicas** — how many are serving traffic.
  The endpoint weight is driven by *ready*, the global desired by *admitted*,
  the request by *apportioned*. Confusing the three is the central bookkeeping
  hazard of this OEP.
- **Fractional admission:** the generalization of 0018's race from "first
  cluster to admit the *whole* ISVC wins" to "each cluster admits *as many
  replicas as fit*; keep every cluster that admitted >0." The unit of the race
  is one replica, not one service.
- **Gang / Instance:** an atomic multi-pod group (an OMENative Instance, Size≥1)
  whose pods run collectives over a low-latency fabric and must start, stop, and
  migrate together. The replica boundary is **between** gangs; never inside one.
- **Re-shard:** re-running the apportionment when a home is lost or quota shifts
  — a partial re-fan-out that tops up the deficit on remaining/new candidates,
  not a full teardown.

### The model: still one ISVC; the control plane now fans it out fractionally

The decision that kept 0018 small — "a multi-cluster ISVC is not a new kind; it
is the same `InferenceService`, distinguished structurally by cluster role" —
holds here verbatim. What changes is only what the control-plane fan-out
controller *does* with the derived ISVCs:

- **0018 (single-active):** clone to all candidates → first whole-ISVC admission
  wins → **delete every other clone** → one home, one endpoint target.
- **0018.1 (fractional):** clone to all candidates with apportioned replica
  counts → each cluster admits as many as fit → **keep every clone that admitted
  ≥1** → N homes → one endpoint **load-balanced across all of them**.

The user still authors one ISVC and submits it to the control-plane cluster. The
derived ISVCs are internal; they are not user objects and are never surfaced as
"go edit these." `status.placement` reports the homes; the user's intent
(`spec.replicas`, the model, the runtime) is authored once. **We do not
auto-rewrite the user's ISVC into multiple ISVCs** — that would fork the unit of
ownership, double the surface a user must reason about, and break the "one ISVC
is the contract" model that 0018 fought to preserve.

### Why fractional admission (not a central planner)

The reader's first objection is the right one to answer head-on:

> "If you want N replicas spread across K clusters by free capacity, why not just
> read each cluster's free GPUs, solve the bin-packing centrally, and write the
> exact per-cluster replica counts? That's one clone per home and no churn."

Because that is the central-capacity-reader OEP-0018 already rejected, and the
reasons get *stronger* at fractional scale, not weaker:

1. **Capacity reads are eventually consistent; a fractional plan multiplies the
   races.** A central planner reading "cluster A has 400 free, B has 400, C has
   200" and writing 50/50/25 replicas is racing every other placement decision
   in the fleet across *three* writes instead of one. Each can be stale on
   arrival; the planner must then reconcile partial failures across homes. The
   fractional race has no such window: each cluster's Kueue admits exactly what
   fits *at admission time*, and "what fits" is the truth by construction.
2. **Over-subscription stays structurally impossible.** Under the race, a cluster
   without room for the k-th replica simply never ungates that replica's pods —
   it cannot over-admit. A central planner that mis-reads capacity *will*
   over-assign and must detect-and-retreat.
3. **No new machinery.** Fractional admission reuses 0018's transport, GC, and
   gating wholesale; it adds bookkeeping (sum admitted across homes) and a
   weighted endpoint, not a scheduler. A central planner adds a capacity reader,
   a solver, and a multi-write reconciliation loop — the exact cost 0018 priced
   and refused.

The cost we accept in exchange (same shape as 0018, larger in degree): each
placement transiently asks **every** candidate for its share, and the apportioned
request may overshoot what any single cluster admits — see
[Drawbacks](#drawbacks) and the over-request strategy in
[the algorithm](#the-fractional-fan-out-algorithm).

### The gang-boundary invariant

**Invariant (correctness, not preference): a single Instance — one
tensor/pipeline-parallel gang — must be admitted whole on exactly one cluster.
The fan-out splits the replica count across clusters; it never splits a gang.**

Rationale: an Instance of Size>1 runs collective operations (all-reduce,
all-gather, the KV exchange in PD) over a low-latency, high-bandwidth fabric —
NVLink/NVSwitch within a node, ICI within a TPU slice, InfiniBand/RoCE within a
cluster's rail-optimized network. Those collectives assume single-digit-µs RTT
and a shared fabric. A cross-cluster (cross-region, cross-provider) link is
WAN-class: milliseconds of RTT, no shared fabric, no RDMA. Place two ranks of one
gang on two clusters and the collective doesn't just slow down — the NCCL/ICI
bring-up fails outright, exactly the multi-host-slice split failure mode OME
already hit and fixed for TPU gangs (per-gang slice affinity). So the boundary
is physical: **the replica is the smallest unit we may
relocate; the gang is indivisible.**

Consequence for apportionment: the unit of all replica math is **whole gangs**.
If a service's desired N=125 8-pod replicas and cluster A admits 60, A holds 480
pods in 60 complete gangs — never 59.5. Kueue's pod-group admission is
all-or-nothing per gang, which is what makes this enforceable: a half-admitted
gang is not a state Kueue produces. We rely on that.

### What is new vs OEP-0018 — and what is not

New in this OEP:
- **Fractional fan-out:** keep all clones that admitted ≥1; sum admitted across
  homes toward the desired N (vs delete-all-but-first).
- **`status.placement` becomes a list of homes** (`{cluster, apportionedReplicas,
  admittedReplicas, readyReplicas, weight, endpoint}`), not one cluster.
- **Spec opt-in + per-cluster knobs:** `spec.placement.scaleOut`,
  `minReplicasPerCluster`, `maxReplicasPerCluster`, `spread`.
- **The weighted-home routing contract + a pluggable `EndpointPublisher` seam** —
  per-home backend + ready-replica weight in `status.placement`. The balancing is
  done by a consumer (the existing front-tier, or client-side LB); OME natively
  programming a backend is an optional follow-up OEP, not built or blocked-on here.
- **Global replica accounting + apportioned (clamped) autoscaling:** one *external*
  desired N (spec / operator / schedule) fanned to per-cluster targets; per-cluster
  HPA/KEDA (OEP-0013) clamped to their apportioned share against per-cluster quota.
  Auto-driving N from an aggregated global signal is a non-goal (see Non-Goals).
- **Re-shard:** partial re-fan-out on home loss / quota shift, with grace
  windows.

Explicitly **not** new (inherited unchanged, so the surface stays small):
- The `WorkloadCluster` CRD and registry (0018).
- The connection manager / cross-cluster transport (0018).
- Kueue mandatory-but-invisible; no Kueue vocabulary in the user API (0018).
- No ISVC spec surgery, no MultiKueue adoption, no `spec.managedBy` (0018).
- The derived-ISVC mechanism and origin-annotation GC (0018).

### User stories

1. *As a service owner with a 125-replica model that fits in no single cluster*,
   I set `spec.placement.scaleOut.enabled: true` and apply one ISVC to the
   control plane. OME spreads the 125 replicas across the three `gb300` clusters
   that have room, gives me one endpoint that load-balances across all three by
   ready replicas, and I never pick a cluster or author a second ISVC.
2. *As that same owner*, when one of my three homes loses capacity to preemption,
   my endpoint keeps serving on the other two at reduced capacity while OME
   re-shards the lost replicas onto a fourth candidate — no full outage, no
   manual action.
3. *As a platform operator*, I cap any one service at `maxReplicasPerCluster: 50`
   so no single service can monopolize a cluster, and set
   `minReplicasPerCluster: 4` so I never get 1-replica slivers that waste a home's
   scheduling overhead.
4. *As a single-cluster or single-active user*, nothing changes: without
   `scaleOut.enabled`, behavior is exactly OEP-0018 (the N=1-home case).

## Design Details

### Objects / API

**`WorkloadCluster`** — unchanged from OEP-0018 (cluster-scoped registry,
connection source, capability labels, conditions-only status). Fractional
placement adds no field to it; "how many replicas fit" is still answered by that
cluster's Kueue at admission, never stored on the registry.

**ISVC spec — additive opt-in + per-cluster bounds:**

```yaml
spec:
  # ... unchanged ISVC spec (model, runtime, engine/decoder, replicas: 125) ...
  placement:
    scaleOut:
      enabled: true              # opt-in; absent/false => OEP-0018 single-active (default)
    minReplicasPerCluster: 4     # don't keep a home that can only take fewer than this
                                 # (avoids 1-replica slivers; 0 = keep any home with >=1)
    maxReplicasPerCluster: 50    # never apportion/admit more than this per home
                                 # (operator anti-monopoly; unset = no cap)
    spread: Balanced             # Balanced | Packed
                                 # Balanced: prefer even spread across homes
                                 # Packed: prefer filling fewer homes first (less LB fan-out)
```

All knobs are optional; an ISVC with only `scaleOut.enabled: true` gets
unbounded, balanced fractional placement. `spec.replicas` (the desired N) is
authored exactly as today — it is a **global** target, not per-cluster.

**ISVC status — `placement` generalized from one cluster to a list of homes:**

```yaml
status:
  placement:
    desiredReplicas: 125         # echo of the global target the controller is driving toward
    admittedReplicas: 110        # sum of admittedReplicas across homes (the live global count)
    phase: FanningOut | Admitted | Running | Degraded | ReSharding
    endpoint: https://my-svc.global.example   # the single global, load-balanced endpoint
    homes:
      - cluster: cluster-a
        apportionedReplicas: 50
        admittedReplicas: 50
        readyReplicas: 50
        weight: 50               # share of endpoint traffic; tracks readyReplicas
        endpoint: https://my-svc.cluster-a.ingress   # per-home backend (LB target)
        conditions: [...]        # mirrored from the derived ISVC on this home
      - cluster: cluster-b
        apportionedReplicas: 50
        admittedReplicas: 40     # this cluster only had room for 40
        readyReplicas: 38
        weight: 38
        endpoint: https://my-svc.cluster-b.ingress
      - cluster: cluster-c
        apportionedReplicas: 25
        admittedReplicas: 20
        readyReplicas: 20
        weight: 20
        endpoint: https://my-svc.cluster-c.ingress
```

Note the example sums to 110 admitted against 125 desired: the controller is
mid-flight, still trying to place the remaining 15 (see the algorithm). `phase`
is `Degraded` while `admittedReplicas < desiredReplicas` and no candidate can
take more; `Running` once the sum meets desired.

### Components on the control-plane cluster

Reuses 0018's four components; two are extended:

1. **Connection manager** — unchanged from 0018 (per-cluster cached client +
   label-scoped watch; reachability conditions). Still the hard subsystem.
2. **Fan-out controller** — *extended*. No longer "first wins, delete the rest."
   Now: compute apportionment, create derived ISVCs with replica counts on all
   candidates, observe per-home admitted counts, keep all homes with ≥1,
   accumulate toward desired N, and drive re-shard. The algorithm below.
3. **Status mirror** — *extended*. Watches the derived ISVC on **every** home and
   writes the `homes[]` list (admitted/ready/weight per home), not one cluster.
4. **Endpoint publisher** — *a pluggable seam, not a backend*. Publishes the
   `homes[]` contract (per-home ingress + ready-replica weight) through an
   `EndpointPublisher` interface whose default is a no-op / manual publisher
   (writes status, programs nothing). A global-traffic consumer (the existing
   front-tier programmed from `homes[]`, or client-side LB) does the balancing;
   OME natively programming a backend is an optional follow-up OEP (see
   [the weighted endpoint](#the-weighted-global-load-balanced-endpoint)).

### The fractional fan-out algorithm

```
Inputs: desired N (spec.replicas); candidate set C (reachable WorkloadClusters
        whose labels satisfy the ISVC's accelerator requirements — the SAME
        matching 0018 does); per-cluster min/max; spread mode.

1. APPORTION (initial request, not a capacity read):
   target_per_cluster = ceil(N / |C|)   [Balanced]   or   N filling fewer  [Packed]
   clamp each to maxReplicasPerCluster.
   Over-request on purpose: ask each candidate for its target; Kueue will admit
   only what fits. We never read free GPUs to size the request — over-request +
   admit-what-fits IS the fractional race. (Bounded by maxReplicasPerCluster so
   we don't request absurd counts.)

2. CREATE/UPDATE the DERIVED ISVC on EVERY candidate with replicas=target.
   Pods carry the Kueue queue-name label (per 0018). Each cluster's Kueue admits
   as many WHOLE gangs (replicas) as fit; the rest stay gated. Gangs are
   indivisible (the invariant) — Kueue's pod-group admission is per-gang.

3. OBSERVE per home: admittedReplicas[home] = count of admitted (ungated,
   running) replicas. Keep every home with admittedReplicas >= max(1, minPerCluster);
   for a home that admitted >0 but < minPerCluster, scale its derived ISVC DOWN
   to 0 and drop it (avoid slivers) — return those replicas to the deficit.

4. ACCOUNT: placed = sum(admittedReplicas[kept homes]).
   - If placed >= N: trim over-admission. We over-requested, so some homes may
     have admitted more than their fair share; scale the most-over homes down to
     bring the sum to N (respecting min/max), preferring to keep homes already
     ready (avoid churning serving replicas). Done -> phase=Running.
   - If placed < N: deficit = N - placed. RE-APPORTION the deficit across
     candidates that are NOT yet at their admitted ceiling (i.e. still have a
     gated remainder that might admit later, or newly-reachable candidates),
     bump their derived replicas, and GOTO 3. If no candidate can take more
     (all gated remainders persist), phase=Degraded and wait — Kueue queueing
     governs, NOT an OME timeout (the same gated-deadline pause 0018 uses).

5. NEVER delete a home that admitted >0 (this is the core delta from 0018's
   step 4 "delete all losers"). Deletion happens only via: drop-sliver (step 3),
   trim-overshoot scale-down (step 4), or re-shard (cluster loss).

6. PUBLISH the weighted endpoint across all kept homes; weight[home] tracks
   readyReplicas[home].
```

Two subtleties the reader should not miss:

- **Over-request is deliberate, and it is the only honest option without a
  capacity read.** We cannot know how many fit without asking; asking *is*
  creating the derived ISVC and watching Kueue admit. The cost is transient
  over-admission (step 4 trims it) and gated remainders on some homes. The
  alternative — read free GPUs first — is the central planner we rejected.
- **Trimming over-admission must prefer not-yet-ready replicas.** Scaling down a
  home that is already serving sheds live capacity and flaps the endpoint weight;
  scaling down gated/just-admitted replicas costs nothing. The trim policy is
  "shed the youngest, gated-first" — `TBD` exact ordering.

### Global replica accounting + apportioned autoscaling

This is the subsystem with the most cross-OEP coupling. The question it answers:
**who owns the desired N, and how do per-cluster autoscalers reconcile to it?**

**Scope decision: desired N is set *externally* — spec, operator, or a schedule —
not computed from an aggregated cross-home signal.** Automatically driving N from
fleet-wide load (a "global autoscaler") is a **non-goal** (see
[Non-Goals](#non-goals)). Rationale: the only strong use case — diurnal
consolidation of a service whose elastic range crosses the single-cluster
boundary — is largely served by an operator/scheduled change to N, which the
apportionment loop already reacts to; and cross-cluster scale-up is too slow (cold
load + gang admission) to chase fast bursts anyway. Revisit in a follow-up if a
concrete need appears.

The failure mode this contract prevents: per-cluster HPA/KEDA (OEP-0013) each
independently chasing the *full* N on their own home — three homes each scaling
toward 125 → 375 replicas, a 3× over-provision and a quota fight.

The contract:

1. **The control-plane fan-out controller owns the desired N.** N comes from the
   user's ISVC (`spec.replicas`) or an operator/scheduled change to it — never
   from a single home, and (v1) never from an auto-aggregated global signal. The
   controller is the single writer of each home's *target* replica count (the
   derived ISVC's `spec.replicas`).
2. **Per-cluster autoscalers (OEP-0013) do NOT own N. They are clamped.** Each
   home's HPA/KEDA `maxReplicas` is pinned by the controller to that home's
   apportioned share (≤ `maxReplicasPerCluster`, ≤ what the home's quota admits).
   A home scales *within* its apportioned band on local signal, but never chases
   the global N — its ceiling is its share. This is the load-bearing rule.
3. **A rise in desired N beyond current homes' headroom triggers a re-shard, not a
   local overshoot.** When N is raised (user/operator/schedule) past what the
   existing homes can admit, the controller re-apportions / adds candidates (a
   re-fan-out) — the only way to get more replicas is more Kueue admissions, i.e.
   the race, not a local autoscaler decision.
4. **Per-cluster Kueue quota is the hard ceiling under everything.** An apportioned
   target above what the home's `ClusterQueue` admits just produces gated pods
   (counted as deficit, step 4). Autoscaling cannot conjure quota.

Firmly out of v1: aggregating a cross-home load signal to *set* N, and home
consolidation on load decline — both need cross-cluster metric federation that
interacts with OEP-0013's per-Component scaler model. Deferred (see Non-Goals).

### The weighted global load-balanced endpoint

Tl;dr: fractional placement is useless without a multi-home load balancer —
capacity you can't address is wasted. But the concrete global-traffic backend is a
**foundational problem that single-active 0018 also has**. So this OEP deliberately
does **not** design the backend. It defines a precise **contract** and a
**pluggable publisher seam**.

Since this was written, one concrete backend has shipped: `placement/endpoint`
programs a Gateway API HTTPRoute over per-home ExternalName Services, weighted by
each home's ready replicas. The seam remains, so a DNS/GSLB backend can be added
without touching the reconciler.

**What this OEP produces — the contract.** The control plane writes and keeps fresh
the per-home routing intent in `status.placement.homes[]`:
`{cluster, endpoint (per-home ingress / LB target), weight}` where **weight tracks
that home's *ready* replicas**, alongside the single stable global endpoint name.
That data — "here are the N home backends and their ready-replica weights" — is the
entire interface a publisher consumes.

**What this OEP requires of any publisher — the invariants.** The global-endpoint
OEP may choose any backend, but it MUST satisfy:

1. **One stable global endpoint** for the whole ISVC, invariant across re-shard and
   independent of how many homes it spans. Clients never see the homes.
2. **Split by *ready* replicas** — never by apportioned or admitted. A home whose
   replicas are admitted-but-not-yet-ready takes proportionally less until they
   warm; weighting by anything but *ready* is the classic "added a backend before
   it was healthy" outage.
3. **Reweight on convergence and on re-shard, with hysteresis** — track replica
   moves (autoscale within band, re-shard) without flapping on every transient
   readiness blip. This is the same lesson as OME's existing intra-cluster traffic
   deadband, lifted to the cross-cluster layer.
4. **Health-drain a lost or re-homed home fast** — pull it from rotation before its
   replicas are formally accounted gone.

**The pluggable seam.** OME exposes an `EndpointPublisher` consuming the `homes[]`
contract; the **default is a no-op / manual publisher** (it writes the contract to
status and programs nothing), so this OEP is implementable and testable as a
control-plane layer entirely on its own. Concrete publishers — a multi-cluster
Gateway with weighted `backendRefs`, GSLB / weighted DNS, a service mesh, or an
existing front-tier router fleet — are the global-endpoint OEP's deliverables, each
weighed against invariants 1–4.

**Who consumes the contract — and why it's not a blocker.** `homes[]` is data; some
global-traffic layer reads it and balances. That consumer is **most likely the
front-tier we already run** (an API-proxy / router fleet) — point its
backend pool at the per-home ingresses with the published weights. Client-side LB
(clients read `homes[]`) is an alternative. Neither is a new OEP — they're
integrations against the contract. So **this OEP does not block on building
anything new**: it ships the contract + a no-op publisher, and you wire it to the
traffic layer you have.

**The optional follow-up.** If you later want OME to *natively* program a global
backend (a multi-cluster Gateway / GSLB / mesh) instead of leaning on the
front-tier, that's a separate, **optional** OEP — worth doing eventually (it also
gives single-active 0018 a stable cluster-independent name), but not required for
0018.1 to function. The backend choice is gated on what the fleet's global-traffic
system exposes across its heterogeneous providers (there's no Gateway API object
spanning them), which is why it's its own proposal rather than a subsection here.

**Consequence, stated plainly.** 0018.1 is fully functional with the existing
front-tier consuming `homes[]`. The native-LB OEP is an enhancement, not a gate.

### Re-sharding on cluster loss / capacity shift

Tl;dr: a home can vanish (unreachable) or shrink (preempted). Re-shard tops up
the deficit on the surviving/new homes — a partial re-fan-out, not a teardown —
and it must not flap.

Triggers:
- **Home unreachable:** connection manager flips it unhealthy (0018). Its
  replicas are presumed lost after a grace window.
- **Home loses admission (preemption):** that home's admittedReplicas drops.
  The base detects this: the Single-mode sticky path re-checks admission and
  re-races after a grace window.
- **Global scale-up beyond current headroom:** desired N rises past what homes
  can admit (see autoscaling, point 3).

Mechanism: on any trigger, recompute `deficit = desired − sum(admitted on still-
healthy homes)` and re-enter the algorithm at step 4 (re-apportion the deficit
across candidates with headroom). Crucially:

- **Re-shard is additive-first.** Top up the deficit on other homes *before*
  removing the lost home's `status` entry, so the endpoint keeps serving on the
  survivors throughout. Never zero an ISVC during a re-shard.
- **Grace windows prevent flapping.** A home that blips unreachable for <
  `reshardGraceWindow` (default `TBD`, order of 30–60s) is **not** re-sharded —
  re-homing a cluster that is actually still serving causes an unnecessary
  capacity move and an endpoint reweight churn. This is the same grace-window
  open question 0018 raised for single-active, now load-bearing because partial
  loss is the common case.
- **Weight rebalances continuously.** As survivors absorb the deficit and new
  homes warm, the endpoint weights track ready replicas (the publisher's job).

### Cross-cluster transport

Unchanged from OEP-0018: one kubeconfig Secret per workload cluster, direct
apiserver calls, origin-annotation (`ome.io/placement-origin` +
`...-origin-uid`) GC, static-credentials-by-default with opt-in allowlisted
`exec` plugins. Fractional placement changes only the *fan-out controller's*
behavior, not the wire. Two transport-adjacent base bugs were the most dangerous
at multi-home scale — the cross-tenant `deleteLosers` data-loss and the
under-scoped GC — and both are fixed (see [Current state](#current-state)).

### End-to-end flow

```
1. User applies ISVC (replicas: 125, placement.scaleOut.enabled: true) to the
   CONTROL-PLANE cluster.
2. Fan-out controller computes candidates by accelerator match (gb300 + reachable)
   -> {cluster-a, cluster-b, cluster-c}. Apportions ceil(125/3)=42 each (Balanced),
   clamped to maxPerCluster=50.
3. Creates the DERIVED ISVC on all three with replicas=42; pods labeled
   queue-name=<lq>. Each cluster's Kueue admits as many WHOLE 8-pod gangs as fit.
4. cluster-a admits 42; cluster-b admits 42; cluster-c has room for only 20 (gates
   22). placed = 104, deficit = 21.
5. Re-apportion the deficit: cluster-a and cluster-b still under maxPerCluster=50,
   bump each toward 50 (+8 each = 16) and look for headroom; suppose both reach 50
   admitted. placed = 120, deficit = 5; cluster-c's 22 gated remainder admits 1 more
   as a gang frees -> placed = 121... loop until 125 or all gated -> Degraded@121.
6. Status mirror writes status.placement.homes (3 entries, admitted/ready/weight).
7. The control plane publishes the weighted-home contract (3 backends + ready
   weights); the global endpoint load-balances across all three home ingresses —
   the concrete backend programming is the global weighted endpoint OEP's job.
8. Later: cluster-b preempted, admitted drops 50->30. After graceWindow, re-shard:
   bump the deficit onto cluster-a (headroom) / a 4th candidate; endpoint keeps
   serving on the survivors the whole time; weights rebalance.
```

### Failure modes and re-sharding

| What breaks | What happens | Mitigation (not always a full fix) |
|---|---|---|
| No candidate can fit the whole N, but the fleet collectively can | Under 0018 the service never starts; under this OEP it spreads | The core fix this OEP delivers: fractional admission sums across homes |
| Aggregate fleet capacity < N | `placed < N` permanently | phase=Degraded, serve at `placed` replicas; keep gated remainders queued (Kueue governs, no OME timeout); surface the shortfall in status |
| One home admits >0 but < minReplicasPerCluster | A 1–3 replica sliver wastes a home's overhead | Drop the sliver (scale derived to 0), return its replicas to the deficit (algorithm step 3) |
| Over-request causes transient over-admission (sum > N) | Briefly more replicas than desired (wasted GPUs) | Trim youngest/gated-first down to N (step 4); prefer not shedding ready replicas |
| A home goes unreachable | Its replicas presumed lost after graceWindow | Re-shard the deficit onto survivors/new candidates **additively**; endpoint keeps serving on survivors; never zero the ISVC |
| A home is preempted (admitted drops) | Partial capacity loss, NOT total | Re-shard the lost replicas; the base re-checks admission and re-races after a grace window (Single mode) |
| Brief unreachable blip < graceWindow | Risk of needless re-home + weight flap | Grace window suppresses re-shard; hysteresis on endpoint reweight |
| Endpoint weights chase transient readiness | Traffic flaps to warming/cold replicas | Weight by **ready** only, with hysteresis; never weight by apportioned/admitted |
| Gang would be split across clusters | Physically broken collectives (gang bring-up fails) | **Invariant**: split only at replica boundary; Kueue per-gang admission enforces indivisibility |
| Two control-plane controllers (HA) double-fan-out | Conflicting deriveds across many homes | Leader election; deterministic derived names; the GC is control-plane-scoped, so one control plane never reaps another's deriveds |

### Dependencies

- **All of OEP-0018's dependencies**, unchanged: Kueue on every workload cluster
  (`ResourceFlavor`/`ClusterQueue`/`LocalQueue`); the `InstanceReadyTimeout`
  gated-deadline pause (so gated remainders don't time out); the connection
  manager.
- **A global-traffic consumer for `homes[]`** — NOT a hard dependency on new OME
  code. This OEP produces the weighted-home contract; something must consume it to
  balance traffic. The expected consumer is the **existing front-tier**
  (an API-proxy / router fleet) programmed from `homes[]`, or client-side LB — an
  integration, not a new subsystem. OME natively programming a GSLB/Gateway/mesh is
  an **optional** future OEP (also benefits single-active 0018's stable naming), not
  a gate on 0018.1.
- **OEP-0013 (per-cluster autoscaling)**, now load-bearing: per-home HPA/KEDA must
  be clampable to an apportioned ceiling the control plane writes (autoscaling
  section, point 2). If 0013's scalers can't be externally bounded, fractional
  autoscaling over-provisions.
- **OEP-0009 (model-weight locality):** weights pre-staged on *every* home, not
  just one. Fractional placement spreads to more clusters, so the pre-staging
  assumption now spans the whole candidate set — a cluster missing the model is
  simply not a candidate.
- **A weighted multi-cluster traffic backend** (Gateway API weighted routes /
  GSLB / mesh). The single largest integration unknown; without it there is no
  multi-home serving (the weighted-endpoint section).

## Current state

**The base (OEP-0018) is fixed and validated, and fractional placement itself
has shipped.** A bug-hunt over the base implementation found 38
defects, eight of which this OEP called blockers. All eight are resolved, as are
the notable mediums; the base now has both an envtest and a real-Kueue
end-to-end suite. Fractional placement landed as `spec.placement.mode: Split`
under OEP-0018.3, so the sections above describe shipped behavior rather than a
proposal.

The race admission signal flows as designed: `inferencereplica/status.go` sets
`Admitted` on the OMENative instance status, the irprojector copies it onto
`isvc.Status.Components[].Lifecycle.InstanceStatuses`, and `placement/admission.go`
reads it.

### Resolved blockers

| # | Blocker | Resolution |
|---|---|---|
| 1 | `deleteLosers` deleted by identity alone (cross-tenant data loss) | `deleteDerivedOn` reads first, checks the origin label/UID, and deletes under a UID precondition; `applyDerived` carries the same guard so a foreign object is never overwritten either |
| 2 | Candidate matching ignored accelerator requirements | `MatchCandidates` ANDs the requirements selector with the optional cluster-selector; an ISVC declaring no requirement matches NO cluster instead of fanning out fleet-wide, extended to the typed `spec.placement` selectors |
| 3 | Partial admission misread as full placement | `AllComponentsAdmitted` requires every declared component to have an admitted instance; `AnyInstanceAdmitted` is retained only as a low-level predicate |
| 4 | GC deleted live deriveds on a transient/partial List | A list error aborts the sweep, and an empty live-UID set is treated as suspect and skipped rather than reaping everything |
| 5 | GC had no control-plane identity scoping | `GCReconciler.ControlPlaneID` filters both the remote List and the orphan classification, so one control plane never reaps another's deriveds |
| 6 | Sticky winner never re-checked admission | The sticky path re-reads per-component IR status and, on lost admission, rides a grace window then re-races off the healthy candidates |
| 7 | Endpoint publisher was a stub | `placement/endpoint` programs a Gateway API HTTPRoute over per-home ExternalName Services, weighted by ready replicas; the nil-flap is gone (sticky last-known URL) |
| 8 | Cross-cluster RBAC was wrong and over-broad | The token ClusterRole now grants read-only `pods` and `scheduling.x-k8s.io/podgroups`, and ClusterServingRuntime access was narrowed to get/list/watch |

### Resolved mediums

- Reconnect/backoff: a previously-reachable cluster tolerates transient probe
  failures for a grace period instead of disconnecting on one failed `/version`,
  and reconnects on a two-backoff schedule.
- `placeOn` no longer clobbers worker-side metadata — control-plane-owned keys
  are merged over what the worker added, with conflict-aware writes and the
  foreign-object guard.
- `deleteLosers` tolerates per-cluster failures instead of aborting on the
  first, and `reconcileDelete` holds the finalizer when a *connected* cluster
  still has the derived.
- Candidate set vs `Manager.Connected()` divergence no longer flaps placement:
  fan-out snapshots connectivity and skips unreachable candidates rather than
  treating them as lost.
- `KueueServingAnnotation` (never a real Kueue key) is gone; only the
  queue-name label, which is what actually gates, remains.

### Validation

- `tests/integration/multicluster/` — 1 control plane + 2 member envtest
  apiservers in one process, covering the produce→project→read path the base
  originally lacked, plus the winner-lost grace window, lost-admission re-race,
  terminal failure, origin guard, GC scoping, and the Split apportionment modes.
- The same suite under `OME_MC_REALKUEUE=1` runs full member managers with
  Kueue's real admission stack — S1–S7 cover the happy race, PD all-components
  admission, gated-deadline, no-quota-anywhere, re-placement, and GC scoping.
  **Not wired into CI**; it is opt-in and local-only today.

### What is still open

- **A KIND multi-cluster e2e.** All existing coverage is envtest, which has no
  kubelet, so nothing it places ever runs. Real scheduling, gang formation, and
  cross-home traffic remain unproven.
- **No `Degraded` phase.** A placement reports `Placed` whenever at least one
  home is admitted, so a permanent shortfall — 3 replicas admitted against a
  desired 125 — looks healthy at the phase level. The shortfall is visible only
  in the per-candidate counts. Step 4 of the algorithm above calls for
  `Degraded`; it was never implemented.
- **No re-shard damping.** Split re-apportions from scratch every poll with no
  grace window and no weight hysteresis, so capacity flux can churn targets and
  endpoint weights. Single mode has an equivalent guard; Split does not.
- **Trim ordering is implicit.** The algorithm leaves the exact order `TBD` and
  asks for gated-first. The plan's keep-selection prefers Ready instances, which
  sheds gated ones first in practice — but an admitted-but-still-starting
  instance sits in the same tier as a gated one, so between those two the
  tiebreak is index, not admission.
- **Terminal failure is not surfaced in Split.** `IsTerminallyFailed` is only
  consulted on the Single-mode sticky path; a Split home whose instances fail
  terminally is invisible.
- **The gang-boundary invariant is structural, not asserted.** Apportionment is
  in whole replicas and a replica is a whole gang, so it holds by construction —
  but no test pins it.
- **Apportioned autoscaling and HA** remain Beta items. Per-home replica bands
  are pinned on the derived, but nothing clamps a per-home scaler dynamically.

## Test Plan

The base validation this OEP originally had to bootstrap now exists: the
envtest multi-cluster suite covers produce→project→read plus a regression for
each resolved blocker, and the real-Kueue path covers admission for real. What
remains is the fractional/multi-home/re-shard surface and the kubelet-level
coverage envtest cannot give.

- **Base validation (done):** `tests/integration/multicluster/` asserts a derived
  is *produced* on each candidate, the admission signal *projects* to
  `status.placement`, and the controller *reads* it to elect; with
  `OME_MC_REALKUEUE=1` the same suite drives real Kueue admission. Remaining gap:
  the real-Kueue path is opt-in and not gated in CI.
- **Unit:** apportionment (`ceil(N/|C|)`, min/max clamping, Packed vs Balanced);
  sliver-drop; over-admission trim (gated-first, prefer-keep-ready);
  deficit re-apportionment loop; per-home admitted/ready/weight accounting;
  weight = ready (never admitted/apportioned).
- **Integration (envtest, control-plane side):** fan-out creates apportioned
  deriveds across N fake remote clients; one admits fully, one partially, one
  none → homes list reflects admitted; deficit re-apportions; a home flips
  unreachable → re-shard tops up survivors additively (no zeroing).
- **Multi-cluster e2e (KIND, ≥3 clusters), authoritative:** real Kueue on each;
  submit an ISVC whose N exceeds any single cluster's quota → verify it spreads,
  `sum(admitted)` converges to N (or Degraded with the right shortfall), the
  weighted endpoint serves across all homes proportional to ready, preempt one
  home → re-shard onto the others with no full outage, weights rebalance.
  **Gang-boundary:** a Size>1 (PD / multi-node) ISVC must place whole gangs only
  — assert no gang is split across clusters under any apportionment.
- **Soak:** churn (repeated re-shard under induced capacity flux) to validate
  grace windows and weight hysteresis don't flap the endpoint.

## Graduation Criteria

- **Alpha:** OEP-0018's blockers are fixed and the base e2e is green; fractional
  placement ships as `spec.placement.mode: Split` with per-home
  admitted/ready accounting and a ready-weighted endpoint. Outstanding for
  Alpha: `sum(admitted)` convergence proven against real Kueue across ≥2 homes,
  a `Degraded` phase for a shortfall that never closes, and the gang-boundary
  invariant asserted by a test rather than held by construction.
- **Beta:** re-shard hardened (grace windows, additive-first, weight hysteresis);
  apportioned autoscaling wired to OEP-0013 (per-home scalers clamped to share);
  HA control plane (leader election) with control-plane-scoped GC; over-admission
  trim policy finalized; per-home health surfaced in status.
- **Stable:** soak at fleet scale across ≥3 regions/providers; a recorded
  decision on the weighted-endpoint backend; a recorded decision on whether
  per-cluster quota is still sufficient or the MultiKueue Cohort path is needed;
  documented operator guidance for min/max-per-cluster and spread.

## Drawbacks

- **Multiplied fan-out churn.** 0018's churn (a gated clone on every candidate)
  is now *sustained*: every home keeps a derived ISVC, the deficit loop
  re-`CreateOrUpdate`s apportioned counts, and re-shard re-fans-out. At 20
  candidates with continuous re-apportionment, that is real and ongoing
  create/update traffic — worse than 0018's one-shot race. The `Incremental`
  fan-out idea from 0018 (widen only if short) is more important here, not less.
- **Over-request wastes transient capacity.** Asking each candidate for its
  apportioned share and trimming the overshoot means briefly running more
  replicas than desired. Bounded by `maxReplicasPerCluster`, trimmed gated-first,
  but non-zero GPU waste during convergence.
- **Multi-home value requires a traffic consumer.** Fractional placement that
  nothing balances across is stranded capacity — so the `homes[]` contract must be
  consumed (existing front-tier, or client-side LB). That's an integration, not a
  new subsystem this OEP builds; but it *is* a wiring step without which spreading
  buys nothing.
- **Operational complexity jumps.** `status.placement` is now a list, debugging
  spans N homes, and "why is my service degraded" can mean any of N clusters'
  Kueue declining. Single-active was one cluster to look at; this is N.
- **No cross-home statefulness.** Stateless weighted LB ignores KV-cache /
  prefix-cache / session affinity; a request can hit any home, losing cache
  locality benefits a single home would have. Acceptable for v1; a real limit for
  cache-sensitive workloads.
- **Per-cluster quota fights are more frequent.** Spreading to more clusters
  means contending for more `ClusterQueue`s; a busy fleet may leave a service
  chronically `Degraded` below N with no pooled-quota recourse (per-cluster only).

## Alternatives

Presented as a menu with each option's cost, then the pick.

1. **Adopt MultiKueue Cohort (pooled quota + dispatch).** Make the control plane a
   MultiKueue manager, the ISVC a Kueue job, and use a Cohort so quota is
   borrowable across clusters; let MultiKueue's dispatcher spread the work.
   *Gives:* real pooled quota (the one thing per-cluster can't do), transport and
   GC for free. *Costs:* exactly what OEP-0018 rejected — OME cedes the brain to
   MultiKueue, deep surgery on the ISVC type (`spec.managedBy`, PodSets, a
   MultiKueueAdapter), and a serving-aware re-shard would fight MultiKueue's
   batch-oriented dispatcher. **Rejected** for the same reasons as 0018; it stays
   the graduation path *iff* pooled quota becomes a hard requirement. Fractional
   admission beats it for our use case because it needs no surgery and keeps OME
   the brain — at the cost of per-cluster (not pooled) quota.
2. **Central capacity planner (read free GPUs, solve N-across-K, write exact
   counts).** *Gives:* one clone per home, no over-request churn, an "optimal"
   spread. *Costs:* a central capacity reader over eventually-consistent data, a
   bin-packing solver, and a multi-write reconciliation loop racing every other
   placement in the fleet — and it *can* over-assign and must detect-and-retreat.
   **Rejected:** fractional admission gets the same spread with no capacity read
   and no solver, and over-subscription is structurally impossible (a cluster
   without room never ungates). The planner's "optimality" isn't worth a
   capacity-reading subsystem that 0018 already priced and refused.
3. **Auto-rewrite the user's ISVC into N per-cluster ISVCs.** Have OME split one
   ISVC into several real user-visible ISVCs, one per home. *Gives:* no derived-
   object indirection; each home is a normal ISVC. *Costs:* forks the unit of
   ownership (the user now has N objects to reason about, edit, and keep in
   sync), breaks "one ISVC is the contract," and makes global accounting a
   cross-object aggregation problem. **Rejected:** the whole point of 0018 was
   "the ISVC is the unit, the control plane fans it out" — rewriting the user's
   object discards that.
4. **Static manual sharding (user authors one ISVC per cluster by hand).** What
   operators do today. *Gives:* zero new machinery. *Costs:* manual, error-prone,
   no global accounting, no re-shard, no weighted endpoint — exactly the toil this
   OEP removes. **Rejected** as the status quo.

## Open Questions

- **Weighted-endpoint consumer — integration, not a blocker.** v1 expects the
  existing front-tier (an API-proxy / router fleet) to consume `homes[]` and
  balance; this OEP owns only the contract, invariants 1–4, and the pluggable
  publisher seam (default no-op). Whether to *also* have OME natively program a global backend
  (Gateway weighted routes vs GSLB vs mesh) — and its reweight latency — is an
  **optional** follow-up OEP, not a gate on 0018.1. Owner of any follow-up: TBD.
- **Automatic global autoscaling — deferred (now a Non-Goal), not an open question
  for v1.** v1 takes desired N externally (spec/operator/schedule); the in-scope
  contract (controller owns N, locals clamped to share) is firm. The follow-up
  question, *if* a concrete need (e.g. diurnal consolidation beyond a scheduled N)
  appears: how to aggregate a cross-home load signal to drive N and mesh it with
  OEP-0013's per-Component scaler model. Owner of any follow-up: TBD.
- **Over-admission trim ordering.** Exact policy for which over-admitted replicas
  to shed (youngest? gated-first? lowest-utilization home?) without flapping the
  endpoint. TBD.
- **Re-shard grace window + weight hysteresis values.** Default `reshardGraceWindow`
  (order 30–60s?) and the reweight hysteresis that stops flapping on transient
  readiness — both need real-fleet tuning. TBD.
- **Min/max-per-cluster defaults.** What `minReplicasPerCluster` avoids slivers
  without over-rejecting small homes; whether `maxReplicasPerCluster` should
  default to unlimited or a fraction of cluster quota. TBD.
- **`Packed` vs `Balanced` default.** Balanced spreads (more LB fan-out, better
  resilience); Packed fills fewer homes (less fan-out, less resilience). Which is
  the safer default? Probably Balanced, but TBD.
- **Interaction with per-cluster preemption priorities.** When two scaled-out
  services contend for the same homes' quota, whose replicas does Kueue preempt,
  and does that cause oscillating re-shards between them? TBD.

See also: OEP-0018 (the single-active base this extends), OEP-0018.3 (the
placement-mode set this shipped under), OEP-0013 (per-cluster autoscaling, now apportioned to a global target),
OEP-0009 (model-weight locality, the pre-staging assumption that must now span
every home).
</content>
