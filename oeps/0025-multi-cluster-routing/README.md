# OEP-0025: Multi-Cluster Routing — capacity-aware traffic maps

**Tl;dr:** Placement decides *where* an InferenceService's replicas run and quota
decides *how much* each cluster may run, but the global router weights clusters
only by live ready-replica count — reactive, with no notion of intended capacity
and no stable contract for a gateway to consume. This OEP adds **`TrafficMap`**:
a per-ISVC, control-plane-generated, **capacity-planned, health-gated** routing
table that a gateway-neutral publisher programs onto a real data plane — the
routing *projection* of placement + quota, as OEP-0024's distributor projects
budgets onto Kueue. A weight is a function of five variables: three computed by
the control plane, and two observed from the home — reported capacity and an
end-to-end probe — that are **individually optional and off by default**. Opt-in
via Helm; SMG is one optional backend, not a hard dependency.

> Status: provisional / alpha. Visible `TBD`s mark still-open decisions; dates
> are absolute so staleness shows.

## Summary

OME already places an ISVC across workload clusters (OEP-0018:
`Single`/`All`/`Split`) and budgets accelerator capacity per cluster (OEP-0024:
`AcceleratorQuota`). Placement facts land in
`InferenceService.status.placement.candidates[]` — per home: `endpoint`,
`admittedReplicas`, `readyReplicas` — and the existing Gateway API publisher
(`pkg/controller/v1beta1/placement/endpoint/gatewayapi.go`) already turns
`readyReplicas` into HTTPRoute backend weights.

That is enough for *reactive* routing and nothing more. Four gaps, each detailed
under *Motivation*: no capacity intent in the weights; no consolidated contract
for a gateway to consume; implicit cutoff; and control-plane readiness that is
not serving readiness.

`TrafficMap` is one namespaced object per ISVC, owned by it, written by a new
control-plane routing controller, consumed by a pluggable publisher. Weight =
capacity allocation (Split apportionment ∩ quota-admitted count), gated by health.

## Motivation

### The failure modes we are fixing

- **Weight drift under autoscaling (the reactive-weight anti-pattern).** A home
  scaling 2→8 silently quadruples its share mid-flight though the plan never said
  it should carry more; a home mid-cold-start is under-weighted exactly when it
  was provisioned to take load. The router chases runtime state instead of
  enforcing intent.
- **Gateway coupling.** Re-deriving the split from every ISVC couples the data
  plane to the ISVC schema and smears the routing table across every status,
  making per-namespace RBAC for "who may see the routing table" awkward.
- **Slow cutoff.** Losing a whole home — a cluster disconnecting, or a large
  failure domain going down — drains traffic only as `readyReplicas` decays.
- **The unreachable-but-healthy home (the green-status black hole).** Everything
  between the client and the pods — the cluster's ingress gateway, its DNS record,
  its TLS certificate, the ExternalName Service the publisher created — is
  **unobserved**. A home can report 7/7 ready while nothing reaches it, and
  `TrafficMap` will confidently weight it 70%. Worse: the health gate is the very
  mechanism relied on for cutoff, so the one failure class it structurally cannot
  see is the class where cutoff matters most.
- **Replica counts overstate prefill/decode capacity mid-rollout.**
  `splitAdmittedReplicas` and `splitReadyReplicas` both take the **MIN** across
  the scaled components. MIN is an *upper bound* on usable prefill/decode pairs,
  **not** a count of them — two ready prefills and two ready decodes are two pairs
  only if actually paired (same failure domain, interconnect-reachable, bound to
  each other). The components recover at different rates, so the skew is widest
  exactly when the number is most load-bearing; where a replica takes tens of
  minutes to become servable that window is hours, not seconds, and the binary
  gate hides it completely — 1 of 50 decoders ready still passes `ready > 0` and
  holds the full planned share.

### Why now

Placement gives the homes and their endpoints, `Split` the per-home
apportionment, `AcceleratorQuota` the admitted denominator. Before OEP-0018 and
OEP-0024 there was nothing to compute a capacity-aware weight *from*; the missing
piece is the projection and the contract.

### Goals

1. **A `TrafficMap` CRD** (`ome.io/v1beta1`), one per ISVC, namespaced,
   owner-referenced, generated from `status.placement` + quota allocation.
   *Rationale:* a stable single-object contract a gateway can watch without
   knowing the ISVC schema.
2. **Capacity-planned, health-gated weights** — track the intended allocation,
   drop to 0 only when a home has no ready replicas. *Rationale:* enforce the
   plan, but never black-hole traffic onto a dead home.
3. **A gateway-neutral publisher.** Contract independent of any one data plane;
   retarget the existing Gateway API publisher; SMG optional. *Rationale:*
   upstreamable contract, pluggable data plane — matching the `endpoint` package,
   which already anticipates "backends added later".
4. **Fast, explicit cutoff** — a home losing all ready replicas reflects as
   weight 0 within one reconcile and the publisher reprograms.
5. **Opt-in** via Helm; disabled means exactly today's behavior.
6. **Observed signals may refine either input — never mandatorily.** Each is
   **independently** toggled and off unless configured. *Rationale:* the control
   plane holds the *plan* and the endpoint holds the *truth*, so trusting only one
   is wrong in one of the failure modes above — but *requiring* both makes every
   install pay for a data-plane dependency it may not need.

### Non-Goals

- **Redefining placement or quota.** `TrafficMap` reads their outputs; it does not
  decide where replicas go or how much capacity a home has.
- **Intra-cluster routing.** Balancing *inside* a home is the local Service /
  in-cluster gateway's job. `TrafficMap` weights *homes*, not pods.
- **L7 traffic policy.** Headers, retries, timeouts, hash keys, and per-revision
  canary weights belong to OEP-0011 and the rollout OEPs. `TrafficMap` carries
  per-home endpoints + weights + health, and composes *under* an L7 policy.
- **Hand-authoring routing.** Generated, not operator-written (see Alternatives).
- **Request-outcome ejection at the edge.** Ejecting a home because *live client
  requests* to it fail is the gateway's job (outlier detection): different
  timescale, and it sees the client's actual network path. The active probe below
  does **not** make it redundant.
- **Specifying a gateway data plane.** SMG's image and deployment topology are
  Helm/GitOps concerns; its `TrafficMap` watcher lives in the SMG repo.

## Proposal

### Terms

- **Home.** A workload cluster serving a share of an ISVC — a `Candidate` that
  reached `Placed`/`Admitted` and reported an addressable `endpoint`. One home =
  one routing target.
- **Capacity allocation (`a_h`).** Replicas a home is *intended* to run: in
  `Split`, `admittedReplicas` bounded by `spec.placement.split` and intersected
  with what quota admits; uniform in `Single`/`All`. The weight's numerator.
- **Health gate.** Per home: `readyReplicas > 0`, optionally ANDed with a probe
  result. Failing it contributes weight 0 regardless of allocation — the mechanism
  that cuts traffic to a down home.
- **Capacity source.** Where `a_h` comes from: `ControlPlane` (default) reads
  `status.placement`; `Endpoint` asks the home and uses the answer as a
  **ceiling** on the plan.
- **Reachability probe.** An active request the control plane issues *to a home's
  `endpoint`* — the URL a client uses — so the result reflects the whole path,
  unlike `readyReplicas`, which is observed from inside.
- **TrafficMap.** The per-ISVC CRD holding the table: per home, endpoint + weight
  + the provenance of that weight.
- **Publisher.** Programs exactly one concrete data plane from a `TrafficMap`.
  Pluggable; one active per install.

### The one API type: `TrafficMap`

The control plane owns and writes it; humans read it but never edit it —
deliberately analogous to OEP-0024's distributor projecting a quota tree onto
native Kueue objects.

```yaml
apiVersion: ome.io/v1beta1
kind: TrafficMap
metadata:
  name: llama-3-70b            # same name as the ISVC
  namespace: team-a
  ownerReferences:             # owned by the ISVC; GC'd with it
    - {apiVersion: ome.io/v1beta1, kind: InferenceService, name: llama-3-70b, controller: true}
spec:
  service: llama-3-70b
  mode: Split                  # mirrored from spec.placement.mode
  entries:                     # the routing table, controller-generated
    - cluster: cluster-a
      endpoint: https://llama-3-70b.team-a.cluster-a.example.com
      weight: 70               # capacity-planned, health-gated
      healthy: true
      capacity:                # provenance: why the weight is what it is
        allocated: 7           # intended replicas on this home
        ready: 7               # live ready replicas
        factor: "1.0"          # per-replica relative capacity c_h
        source: ControlPlane   # which input produced `allocated`
    - cluster: cluster-b       # second home, 3 replicas → weight 30
      endpoint: https://llama-3-70b.team-a.cluster-b.example.com
      weight: 30
      healthy: true
      capacity: {allocated: 3, ready: 3, factor: "1.0", source: ControlPlane}
  observedISVCGeneration: 42   # the ISVC generation this table came from
status:                        # written by the publisher, not the generator
  programmed: true
  gatewayRef: {group: gateway.networking.k8s.io, kind: HTTPRoute, name: llama-3-70b}
  observedTrafficMapGeneration: 5
  conditions: [{type: Programmed, status: "True", reason: HTTPRouteReconciled}]
```

Two writers, one object:

- The **routing controller** owns `spec` — recomputes `entries` whenever
  `status.placement` or the home's quota allocation changes.
- The **publisher** owns `status` — programs the data plane and reports
  `Programmed`.

*Anticipated objection — "a machine-written `spec` is unusual; spec is user
intent."* True, and we considered a status-only shape (Alternatives). The
computed table stays in `spec` because it is a *declarative artifact* the
publisher consumes like any other desired state — the same generation
(control plane) / realization (publisher) split as ISVC→derived-ISVC. The object
is never hand-edited; a validating webhook may reject non-controller writes to
make that explicit.

### How a weight is computed: the five variables

```
raw_h    = a_h × c_h        # planned serving capacity
raw_h    = 0                # unless healthy_h
weight_h = normalize(raw_h) # relative ints, per Gateway API
```

Five variables feed that. Three are computed by the control plane and always
present; two are observed from the home and are **individually optional and off
by default** — enabling one never enables the other, and with both off the
control plane makes no outbound call at all.

1. **`a_h` — capacity allocation** *(always; the weight's numerator)*. In
   `Split`, the apportioned `admittedReplicas` intersected with what quota
   admits, so traffic follows the *plan* (7:3 above) rather than live readiness —
   a home scaling 7→9 under a spike does **not** steal a larger share. In `All`,
   uniform (`a_h = 1` per healthy home). In `Single`, one weighted entry; the map
   still exists so cutover on a failed winner is expressible.

2. **`c_h` — per-replica capacity factor** *(always present; defaults to `1.0`)*.
   Replicas are not fungible across accelerators: 100 replicas on each of two
   types is not 50/50 if one serves 2× the tokens/sec. Precedence: an explicit
   per-home factor on the ISVC — the accurate source, since the ratio is
   workload-specific (model × runtime × precision), not a hardware constant —
   then an opt-in `AcceleratorClass` default, then `1.0`. **Never auto-derived
   from TFLOPS**: raw FLOPS is not a reliable throughput proxy across vendors.
   Affects *weighting only*, not placement.

3. **`r_h` — ready replicas** *(always; the health gate)*. Binary in v1:
   `r_h == 0 → weight 0`. *Failure mode avoided:* a proportional gate
   (`weight ∝ ready/allocated`) swings on every transient readiness dip,
   reintroducing the drift we are removing. *Cost accepted:* a home at 1/7 ready
   keeps its full 70% share and may briefly overload.

4. **Reported capacity — a ceiling on `a_h`** *(optional, off by default)*.
   Polled from a capacity endpoint the home exposes: `a_h = min(plan_h, reported_h)`.
   - **Why:** only the router knows its *realized* prefill/decode pairing.
     `MIN(ready_prefill, ready_decode)` is an upper bound on usable pairs, not a
     count of them.
   - **A ceiling, never a raise** — otherwise reactive drift returns through the
     back door, and a router that is hot, buggy, or stale could claim more traffic
     than the plan granted.
   - **Contract:** an absolute count in servable units — not a utilization ratio,
     and not a flat sum of per-worker slots, which counts prefill slots with no
     decode partner and so *overstates* capacity during exactly the skew this
     exists for — plus a staleness stamp, a stated cross-replica aggregation rule
     (`TBD`, Q9), and **fail-open** to `plan_h` with a condition and a metric.
   - **Not shippable yet:** no pair-aware capacity route exists on the data plane.
     Sequenced last.

5. **Probe result — ANDed into the health gate** *(optional, off by default)*. A
   periodic request to the entry's `endpoint`, the same URL a client uses.
   - **Why:** a router's report about itself cannot include anything *above* it —
     ingress gateway, DNS, certificate, route object — which is the entire
     unreachable-but-healthy class. Bound: the control plane is not the client, so
     a green probe proves the path is good *from here*; edge ejection stays
     complementary (Non-Goals).
   - **Depth decides the value:** a liveness endpoint proves a process is up; only
     an inference-path endpoint proves the home can serve. Not free — where a group
     serves roughly one request at a time, a synthetic request occupies real
     serving capacity.
   - **Status policy:** `429` (overloaded) and `401`/`403` (the prober's own
     credentials) must **not** gate; `5xx` and transport errors do; "probe could
     not run" gates **nothing**, since that condition is uniform across homes and
     would zero the fleet at once.
   - **Hysteresis is mandatory:** N consecutive failures to gate, M to recover. The
     prober is fleet-correlated, so the all-zero guard — equal weights rather than
     black-holing — is load-bearing here and must not be removed.

Both optional variables are read by **one** shared per-home observer rather than
two clients (same surface, same credentials, same failure modes), but they stay
separately toggled: sharing a client is not bundling the features.
### End-to-end flow

1. **Placement + quota run (existing).** `status.placement.candidates[]` carries
   each home's `endpoint`, `admittedReplicas`, `readyReplicas`.
2. **Routing controller generates `TrafficMap`.** Watches the ISVC and the
   relevant quota allocation, computes the weights, writes `spec`. One per ISVC,
   owner-referenced for GC.
3. **Observation refines the inputs (optional, off by default).** Where
   configured: poll capacity and clamp `a_h` to the reported ceiling; probe each
   `endpoint` and AND the result into the gate. Either input failing to answer
   falls back to the control-plane value and records why.
4. **Publisher programs the data plane.** Reads `entries` (rather than
   re-deriving from `status.placement`) and reconciles one HTTPRoute with a
   weighted backendRef per home; writes `status.programmed`.
5. **Cutoff.** A home losing all ready replicas — or, where probing is enabled,
   failing N consecutive probes — flips `healthy:false`/`weight:0` in the next
   reconcile; the publisher reprograms and the gateway stops sending to it.

### Why a CRD and not just ISVC status

The per-ISVC routing facts *do* already exist in `status.placement`; the question
is whether to consolidate them.

| Dimension | Reuse `status.placement` only | New `TrafficMap` CRD (derived) |
|---|---|---|
| Gateway coupling | Watches **all** ISVCs, re-derives split | Watches **one** object kind per service |
| Capacity-aware weights | No — ready-replicas only | Yes — allocation ∩ quota, health-gated |
| Stable contract / RBAC | Smeared across ISVC status | One object, own RBAC, own webhook |
| Extra machinery | None | +1 CRD, +1 controller writing a machine `spec` |
| Where routing "why" lives | Nowhere consolidated | `entries[].capacity` provenance |

**Pick: `TrafficMap`, derived from `status.placement`.** `status.placement` stays
the source of truth for *placement facts*; `TrafficMap` is the *routing
projection* of those facts plus quota — the same layering as OEP-0024 (budgets in
the CRD, enforcement projected onto Kueue). Not duplicated state: the projection
is what makes capacity-aware weights and a gateway-neutral contract possible.

## Design Details

### `TrafficMap` schema (v1)

- `spec.service` (string, required): the routed ISVC name (== `metadata.name` in
  v1; a distinct field leaves room for a future many-ISVC-to-one-service map).
- `spec.mode` (enum `Single|All|Split`): mirrored so a consumer need not read the
  ISVC.
- `spec.entries[]` (`listMapKey: cluster`): one per serving home —
  - `cluster` (string, required), `endpoint` (URL, required),
    `weight` (int32 ≥0), `healthy` (bool).
  - `capacity.allocated` / `.ready` (int32), `.factor` (quantity, default `1.0`):
    weight provenance — `weight ∝ allocated × factor`, health-gated.
  - `capacity.source` (enum `ControlPlane|Endpoint`, default `ControlPlane`):
    which input produced `allocated`. *Rationale:* without it an operator cannot
    distinguish "the home reported less capacity" from "the override failed open"
    — same number, opposite remediations.
  - `probe` (optional, present only when probing is configured): last result,
    observation time, consecutive-failure count. *Rationale:* `healthy:false`
    alone does not say whether readiness or reachability failed, and those point
    at different teams.
- `spec.observedISVCGeneration` (int64) for staleness detection.
- `status.programmed` (bool), `status.gatewayRef`,
  `status.observedTrafficMapGeneration` (int64), `status.conditions[]`.

### Ownership & lifecycle

- **One `TrafficMap` per ISVC**, `metadata.name == isvc.name`, namespaced to it.
  *Rationale:* mirrors HTTPRoute-per-service; trivial ownership, GC and RBAC;
  scales linearly with ISVC count, no single hot object.
- **Owner-referenced** (`controller: true`) → GC'd with the ISVC. The publisher
  removes programmed data-plane objects via a finalizer, so a route never
  outlives its map.
- **Created only for multi-cluster ISVCs** (those with `spec.placement`) and only
  when routing is enabled. A `Single`-mode ISVC gets a one-entry map so
  winner-cutover is expressible; a non-placement ISVC gets none.
- **The map exists for the whole life of the ISVC, even with zero entries.**
  Emptiness is a *state of the map*, never its absence. *Rationale:* absence is
  unattributable — feature off, ISVC not placed, no addressable home, and a wedged
  controller all look identical. Worse, deleting on degradation removes the object
  exactly when it is most wanted: a map serving real traffic would vanish the
  moment its last home stopped being addressable, taking `capacity.allocated`,
  `capacity.ready` and the factors with it — the very fields that say *what* went
  away. Kubernetes makes the same call: a selector `Service` keeps its
  `EndpointSlice` when no pod is ready.
- **An empty map must say why**, or we trade "absent and ambiguous" for "present
  and ambiguous". The router owns a `Routable` condition — `True` when at least
  one entry is addressable, else `False` with reason `NotPlaced`,
  `NoAddressableHome`, or `AllHomesUnready`. `status.conditions` is a
  `listType=map` keyed on `type`, so `Routable` and the publisher's `Programmed`
  coexist under server-side apply — but they are two writers on one subresource
  and each must patch only its own condition.

### Publisher contract (pluggable, gateway-neutral)

A `Publisher` reads a `TrafficMap`, reconciles exactly one data-plane
realization, and reports `status.programmed`. Exactly one is active per install,
selected by config.

- **Gateway API HTTPRoute** (retargeted from today's status-reading path): one
  HTTPRoute, one `backendRef` per home (an ExternalName Service aliasing the home
  endpoint, as today), `weight` per entry. When all weights are 0 it must **not**
  black-hole — it falls back to equal weight, matching today's guard in
  `gatewayapi.go`.
- **SMG** (optional): SMG consumes `TrafficMap` **natively** — a watcher inside
  SMG populates its worker registry and splits by entry weight. Because we own
  that codebase this is a native reflector mirroring its existing pod
  service-discovery, not an out-of-process adapter or xDS push. Here SMG is the
  consumer, so the "publisher" is a no-op beyond marking `status.programmed`; the
  contract is identical.

### Enabling routing, and the no-magic-values rule

Per the repo's hard constraint (no behavioral value baked into the binary):

- A Helm toggle gates the routing controller and publisher; **default off**, and
  off means today's behavior.
- The active publisher (`gatewayapi` | `smg`), the gateway ref, the backend port
  and any host template come from the operator ConfigMap / Helm values — **no
  in-code fallbacks**. Absent config degrades gracefully (routing simply not
  programmed) rather than injecting a default gateway name or host.
- The two observed inputs are config-driven end to end and **default to absent**:
  the capacity source, and the probe's path, method, accepted status codes,
  period, timeout and thresholds. There is **no in-code probe path and no in-code
  status list** — an unconfigured probe does not run, rather than silently
  probing a guessed `/health`. *Rationale:* a guessed path is the
  hardcoded-magic-value failure mode in a health-check costume, and its specific
  harm is that it appears to work — a shallow probe against a wrong-but-live path
  returns 200 forever.

### Where it runs

The routing controller runs in the **control plane** (OEP-0019) alongside the
placement fan-out controller — it needs the aggregated `status.placement`, which
only the control plane sees. The publisher runs there too. Neither runs on
workload clusters.

### Interplay with adjacent OEPs

- **OEP-0018 placement.** Downstream of it; consumes
  `status.placement.candidates[]` and changes nothing about the race, GC, or
  apportionment. Placement is cluster-granular, so entries are too — one per home,
  never per pod.
- **OEP-0024 quota.** `AcceleratorQuota` is the capacity denominator: `allocated`
  is bounded by what the home's ClusterQueue admitted. With quota disabled,
  allocation falls back to the `Split` apportionment alone (Open Question #2).
- **OEP-0011 traffic policy & rollout OEPs.** They own *L7* behavior and
  *per-revision* weights *within* a home; `TrafficMap` owns *cross-home* weights.
  They compose — canary at the home, cross-home at the gateway. Conflating them
  would put revision rollout state into a cross-cluster object.
- **OEP-0023 status URLs.** `entries[].endpoint` reuses the per-home addressable
  URLs OEP-0023 standardized; no new URL scheme here.
- **Large failure domains.** `TrafficMap` cuts a *home* fast. Cutting a
  *sub-cluster* failure domain is a level below and out of scope.

## Test Plan

- **Unit (weight math).** Table-driven: `Split` 7:3 both healthy → 70/30; one home
  `ready:0` → 100/0; all `ready:0` → equal-weight fallback (no black-hole); `All`
  → uniform minus unhealthy; `Single` → one entry.
- **Unit + controller (default-off parity).** With no capacity source and no probe
  configured, `entries[]` match the pre-existing control-plane path for the same
  inputs and the controller issues **zero** outbound requests; each toggle is also
  exercised alone, so enabling one never activates the other. *Rationale:*
  "optional and off by default" is a claim that rots silently — this test is what
  fails when someone wires a probe into the default path.
- **Unit (capacity ceiling).** `reported > plan` → plan wins; `reported < plan` →
  reported wins; absent or stale → plan wins, `capacity.source` stays
  `ControlPlane`, fallback condition set. A report on one home must not change
  another home's `allocated`, only its normalized share.
- **Unit (probe status policy).** `429`/`401`/`403` do not gate; `5xx` and
  transport errors do; "probe could not run" gates nothing even when it affects
  every home; the gate flips exactly at the Nth failure and recovers exactly at
  the Mth success.
- **Controller (envtest).** `status.placement` transitions → `spec` converges;
  ISVC deletion → GC'd; quota change → weights recomputed;
  `observedISVCGeneration` tracks the source. A home failing N probes →
  `healthy:false`, weight 0, survivors renormalize; **every** home failing →
  equal-weight fallback, never all-zero (the correlated-prober case).
- **Publisher (envtest).** `spec` → one HTTPRoute with weighted backendRefs; entry
  removed → backendRef dropped and stale ExternalName Service deleted;
  `status.programmed` set; finalizer removes the route on map deletion.
- **Integration (multi-cluster harness, OEP-0020).** fan-out → admit → map
  generated → gateway programmed → kill a home → weight→0 within one reconcile.
- **Leak-free fixtures** (`example.com`, generic cluster names).

## Graduation Criteria

- **Alpha.** CRD + generator + Gateway API publisher behind the Helm toggle
  (default off). `Split`/`All`/`Single` weight math + binary gate. Unit + envtest.
- **Beta.** SMG publisher; multi-cluster integration suite gating cutoff and
  capacity-aware weights; documented consumption contract; scale test at TBD ISVC
  count. The active-probe **capability** available behind its own toggle (default
  off) with the full status policy and hysteresis, plus a measured per-probe cost
  on a disaggregated home. Beta requires the capability to exist and be correct,
  **not** that any install enables it.
- **Stable.** Default-on decision made (or opt-in kept, with rationale);
  proportional-gate question resolved; a second gateway backend proving the
  contract is genuinely neutral. Endpoint-reported capacity needs a pair-aware
  data-plane surface first, so it graduates on that timeline, not this one.

## Drawbacks

- **A new CRD + controller**, with a machine-written `spec` (justified above). If
  most installs never enable multi-cluster routing this is cost carried for a
  subset — mitigated by the default-off toggle.
- **Two objects can disagree transiently.** The map can lag placement by a
  reconcile; `observedISVCGeneration` makes the lag observable, not absent.
- **The binary gate can overload a barely-ready home** (accepted; Open Question #1).
- **The observed inputs add a control-plane → data-plane dependency in a direction
  that did not previously exist.** Outbound calls to every home on a timer: new
  credentials, new egress, new timeouts, and — if the probe is deep — real serving
  capacity consumed on the homes being checked. It also makes the prober
  fleet-correlated. Fail-open, hysteresis and the all-zero guard are mitigations,
  not a full fix: a prober returning confidently wrong answers defeats all three.

## Alternatives

1. **Reuse `status.placement` only (no CRD).** Simplest, partly built. Rejected as
   the *primary* design: cannot deliver capacity-aware weights or a decoupled
   contract. We still keep `status.placement` as the source it derives from.
2. **Fleet-wide single `TrafficMap`.** One object to watch, but a write-contention
   hotspot with unbounded growth and awkward per-namespace RBAC. Rejected.
3. **Hand-authored `TrafficMap`.** Maximum control, but discards the
   auto-derivation that is the whole point and reintroduces plan/routing drift.
   Rejected; a future *override* field could layer operator intent on top.
4. **Reuse `EndpointSlice`.** Models endpoint + readiness but has no
   weight/capacity semantics and is cluster-local by design; bending it would
   abuse a well-known type. Rejected.
5. **DNS-weight / external global LB only.** Viable as a later *publisher*
   backend, but DNS TTL cutoff is too slow for the fast-cutoff goal. Not v1.

## Open Questions

0. ~~Should a `TrafficMap` exist when nothing is routable?~~ **Resolved: yes,
   always, with a `Routable=False` reason.** Deleting on degradation made the
   object vanish at peak diagnostic value and made a never-created map
   indistinguishable from a broken controller.
1. **Proportional vs. binary health gate.** Should v2 scale by a bounded
   `min(ready/allocated, 1)`, or does that reintroduce reactive drift?
   **Partially answered by endpoint-reported capacity:** where it is enabled, the
   barely-ready case is handled by lowering `allocated` rather than scaling the
   gate — the benefit without per-reconcile drift, since the ceiling moves on the
   home's settled view. Open for homes on the control-plane default; leaning
   binary there.
2. **Allocation denominator when quota is disabled.** Is the `Split` apportionment
   alone the right `allocated`, or should it fall back to ready-replicas?
3. ~~How SMG consumes `TrafficMap`.~~ **Resolved: native CRD watch inside SMG**,
   reusing its existing reflector + worker registry rather than an xDS push. Its
   one real gap is a weighted-split routing policy.
4. **Static vs. measured capacity factor `c_h`.** Should a later version derive it
   from observed per-home tokens/sec so heterogeneous weights self-tune? Leaning
   static for v1 — measured needs a trusted cross-home throughput metric.
5. **Per-ISVC vs. per-endpoint granularity.** If one ISVC fronts multiple routable
   model endpoints, is one map per ISVC still the right unit? `spec.service`
   leaves room; v1 assumes 1:1.
6. **Composition with per-revision rollout weights.** Confirm the "canary at the
   home, cross-home at the gateway" layering holds for every rollout progression.
7. **Probe depth versus probe cost.** Deep probing catches what shallow cannot but
   consumes serving capacity. Deep-at-low-frequency, shallow-with-deep-for-a-
   subset, or a dedicated cheap deep path from the runtime? Needs a measured
   per-probe cost — `TBD`. Blocks the probe's default guidance, not the mechanism.
8. **Where the prober runs.** One control-plane prober, or must the probe
   originate where client traffic actually enters? The second is more faithful and
   materially more machinery; I am not yet convinced the fidelity pays for itself.
9. **Capacity aggregation across a home's router replicas.** `min`, `max`, or
   any-responder? `min` is the conservative lean but lets one lagging replica
   dominate. Must be settled with the data-plane owners **before** the endpoint
   input is built, since it changes what the endpoint computes.
10. **Probe granularity and fan-out.** One probe per entry means (ISVCs × homes)
    probes per period, each potentially a real inference request. Does the shared
    path warrant a per-home probe amortized across ISVCs? `TBD` — depends on
    ISVC-per-home counts we have not measured.

## See also

- OEP-0018 (placement), OEP-0018.3 (placement modes), OEP-0024 (quota),
  OEP-0011 (traffic policy), OEP-0023 (status URLs), OEP-0019 (control-plane split).
- Code this builds on: `pkg/controller/v1beta1/placement/endpoint/` (publisher +
  Gateway API HTTPRoute), `pkg/apis/ome/v1beta1/inference_service_status.go`
  (`PlacementStatus` / `CandidatePlacement`).

**Next action:** the two observed inputs are independently shippable and should be
sequenced by dependency, not by value. Active health probing is
control-plane-only and can land once its status policy and thresholds are settled
(Open Questions #7, #8). Endpoint-reported capacity is **blocked** on a pair-aware
capacity surface existing on the data plane, so it follows — and Open Question #9
must be agreed with the data-plane owners before that work starts, since it
determines what the endpoint has to compute. Open Question #2 still gates the
generator's weight math. Owner: @fanyang-real.
