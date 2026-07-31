# OEP-0018.3: Placement Modes — Single, All, and Split

**Status:** provisional. `Single` ships today (OEP-0018); this OEP adds `All`
(new) and `Split` (= OEP-0018.1). Open questions marked inline.

Tl;dr: OME can place an InferenceService on exactly **one** workload cluster —
the race winner — and that cardinality is baked into the reconciler. Users need
two more shapes: **run it on every capable cluster** (redundancy, or soaking up
spare capacity), and **spread its replicas across clusters** when it doesn't fit
in one. This OEP makes cardinality a choice: `spec.placement.mode ∈ {Single,
All, Split}`. The three modes share candidate selection and fan-out; they differ
in exactly two places — **what happens to the non-winners** and **how the
endpoint is published**.

## Motivation

The placement reconciler (`pkg/controller/v1beta1/placement/controller.go`) has
one shape: fan out to every matched candidate, let each cluster's Kueue gate the
pods, keep the first fully-admitted cluster, `deleteLosers` on the rest, publish
the winner's single URL. Cardinality is **hard-coded** — it always keeps exactly
one cluster. That cannot express two real needs:

1. **Serve everywhere / soak spare capacity.** Run in every cluster that can host
   it, each scaling to local demand — not one cluster with the rest idle.
2. **Scale past one cluster.** A model needing more replicas than any single
   cluster admits (OEP-0018.1: 125 replicas × an 8-pod gang = 1000 pods) has
   nowhere to put the surplus.

One `mode` field, not three controllers: the pipeline is 90% shared (resolve
candidates → clone derived ISVC → Kueue admits); only the tail differs. Forking
would triple the fan-out/derive/finalizer/GC surface for a tail difference.

### Goals

1. Add `spec.placement.mode ∈ {Single, All, Split}` — typed, defaulted, validated.
2. Implement **`All`** end to end (keep every admitted home; each autoscales locally).
3. Land the shared **per-home status + endpoint model** `All` and `Split` both need.
4. Bind **`Split`** to the OEP-0018.1 algorithm (reference, don't re-specify).
5. Change nothing for existing users: no `spec.placement`, or `Single`, == today.

### Non-goals

- Tiered / GPU-first preference-ordering — dropped; `mode` is cardinality only.
- Re-specifying Split's fractional algorithm — that's OEP-0018.1.
- Global replica coordination — each home autoscales independently (OEP-0013).
- The global load balancer itself — OME publishes the weighted routing contract;
  the external gateway/DNS does the traffic split.
- Per-mode spec variants (different replica policy per cluster) — later.

## Terminology

- **Placement mode** — the cardinality of a placement (how many clusters serve):
  - **Single** — one cluster serves (the winner); the rest are swept. Today.
  - **All** — every candidate that admits serves; none swept; each autoscales
    locally against its own Kueue quota. N equal-weight endpoints.
  - **Split** — the guaranteed floor (`minReplicas`) is distributed across
    clusters; keep every cluster that admitted ≥1; sum until the floor is met
    (OEP-0018.1). N endpoints weighted by ready replicas.
- **Candidate** — a Ready `WorkloadCluster` whose labels satisfy the ISVC's
  requirement selector. Identical across modes — selection is orthogonal to mode.
- **Home** — a candidate actually serving (admitted). `Single` has one; `All`/`Split` have ≥1.

## What must be true

1. **`Single` is unchanged** — byte-for-byte OEP-0018 for existing ISVCs.
2. **`All` keeps every admitted home** — a home dropping out (lost admission,
   disconnect) shrinks the served set but never re-races or tears down the others.
3. **The endpoint describes every home** — `All`/`Split` expose one addressable
   endpoint per home (+ weight in `Split`) and publish a backend per home.
4. **Mode is validated, not inferred** — an unsupported/not-yet-built mode is
   rejected at admission, never silently downgraded to `Single`.

## Approach

`spec.placement = {mode, requirements, clusterSelector}`, with
`requirements`/`clusterSelector` subsuming the legacy annotations (still honored
when `spec.placement` is nil). The reconcile is shared through candidate
resolution and fan-out, and branches on `mode` only here:

**1. Non-winner handling** (after the race resolves per-candidate admission):

| Mode | Admitted candidates | Non-admitted candidates |
|---|---|---|
| `Single` | keep the one winner (lexical tie-break) | sweep all (`deleteLosers`) |
| `All` | keep **all** | leave in place; keep trying (capacity may free up) |
| `Split` | keep all that admitted ≥1 | per OEP-0018.1 |

**2. Endpoint publication** — `Single` = one URL; `All` = one backend per home,
equal weight; `Split` = one backend per home, weighted by ready replicas.

The sticky-winner / re-home-on-lost-admission (PR #863) / terminal-fail logic is
**`Single`-only** — it protects the *one-home* invariant. `All`/`Split` are
per-home independent: a home that loses admission is just not-currently-serving;
the others are unaffected and the endpoint set shrinks. `All` is best-effort —
serve on whoever admitted, keep retrying the rest — because failing the whole
placement when one cluster is full would defeat the redundancy goal.

### Example: `All` for low-priority batch inference

The motivating `All` case: an opportunistic batch-inference service that should
soak up spare capacity fleet-wide. Give it a **low `WorkloadPriorityClass`** and
a **small `minReplicas`** and set `mode: All`:

```yaml
spec:
  placement:
    mode: All
    requirements: accelerator=tpu7x
  engine:
    minReplicas: 1        # small floor per home
    # low-priority workload class so Kueue admits it only where quota is spare
    # and preempts it first when a higher-priority service needs the room
```

Every candidate with spare quota admits its small floor and runs a home; each
home autoscales locally as demand and spare capacity allow; where a cluster is
full of higher-priority work, `All`'s best-effort semantics simply skip it (no
home there) until capacity frees up. Net: the batch job spreads across every
cluster that has room, at the priority tier where it belongs, without the
operator hand-picking clusters — and it yields cleanly to production traffic via
Kueue preemption. Contrast `Single`, which would pin the whole batch job to one
cluster and leave the rest of the fleet's spare capacity untouched.

### Status & endpoint model (shared by All and Split)

`PlacementStatus` today is structurally one-home (single `Cluster` + `Endpoint`).
The additive, back-compatible change: extend **`CandidatePlacement`** with an
optional `Endpoint *apis.URL` (and, for `Split`, `ReadyReplicas`/`Weight`). In
`Single` these stay empty and the top-level fields remain authoritative; in
`All`/`Split`, `candidates[]` is the source of truth and top-level `status.url`
becomes the aggregated global host. The **endpoint publisher** (`.../placement/
endpoint`) already emits a Gateway API `HTTPRoute` with `backendRefs` + `weight`,
so "one backend per home" is a publisher change, not a new mechanism. Extending
`CandidatePlacement` (already cluster-keyed, already populated during the race)
avoids a second, drift-prone cluster-keyed list.

`Split` binds to the OEP-0018.1 fractional algorithm (distribute `minReplicas`
across candidates; each Kueue admits what fits; keep every cluster with ≥1; sum
admitted until the floor is met; weight the endpoint by ready replicas; enforce
the gang-boundary invariant). It splits the guaranteed floor, not `maxReplicas` —
see open questions. This OEP contributes only the `mode` binding and the shared
status/endpoint model.

## Anticipated objections

Q. *"Isn't `All` just `Split` at full replicas everywhere?"*
A. No — who sets the per-cluster replica count differs. `All` gives every home
the ISVC's full replica policy and lets each autoscale to local demand (N
independent services sharing a spec); `Split` gives the fleet **one** desired
count and distributes it (one service spread thin). `All` = redundancy / spare-
capacity soak; `Split` = scale-past-one-cluster.

Q. *"`All` with no re-race — doesn't that lose `Single`'s resilience?"*
A. It trades it for redundancy. `Single` re-homes because it has one home; `All`
already has others, so a failed home is a capacity dip, not an outage — re-racing
would churn healthy homes for no gain.

## Open questions / next steps

- **`Split` replica-count** — resolved: Split distributes **`minReplicas`** (the
  guaranteed floor), never `maxReplicas`. `maxReplicas` is an autoscaling ceiling
  reached only under load, so splitting it would reserve capacity everywhere that
  may never be used and could not guarantee each home hits its share. In Split,
  `maxReplicas` stays a per-home *local* autoscaling ceiling — a home scales up
  from its assigned share on local demand; a fixed count is `minReplicas ==
  maxReplicas`. (Reconcile the wording with OEP-0018.1.)
- **`CandidatePlacement.Weight` type** — raw ready-replica count vs normalized
  0–100; leaning raw in status, normalize only when writing `backendRefs[].weight`. TBD.
- **Webhook capability signal** — how the validating webhook learns which modes
  this build supports (build tag / capability ConfigMap / static list). TBD before `All` merges.

Next: (1) API `{mode, requirements, clusterSelector}`; (2) selector bridge + mode
switch with `Single` wired; (3) per-home status/endpoint + multi-backend
publisher; (4) `All`; (5) `Split` bound to OEP-0018.1; (6) config + dev-env demo.
Scope: control-plane placement path only; single-cluster ISVCs are unaffected.
