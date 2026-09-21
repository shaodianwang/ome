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
end-to-end probe — that are **individually optional and off by default**. The
installation supplies a global gate, defaults, and one shared publisher;
`spec.routing`, a sibling of `spec.placement`, lets one ISVC replace its probe or
capacity policy inline and override only publisher options declared safe for
tenant use. No per-ISVC profile object, publisher process, or polling goroutine
is introduced. Opt-in via Helm; deployment-specific publishers and SMG are
optional backends, not hard dependencies.

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
control-plane routing controller, consumed by a pluggable publisher. Each weight
is `min(admitted, ready, optional reported capacity) * factor`, with probe policy
applied separately.
Installation configuration enables the feature, chooses the publisher, and
provides transport credentials. An optional inline `spec.routing` changes one
ISVC's behavior without changing that ConfigMap or constructing another
publisher.

## Motivation

### The failure modes we are fixing

- **Weights lag runtime serving capacity.** An admitted allocation can remain
  unchanged while pods become unready during rollout or graceful termination.
  Continuing to send the planned share to that home overloads the remaining
  ready pods and delays traffic removal from terminating replicas.
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
  minutes to become servable that window is hours, not seconds. Capping each
  home's weight by its ready count prevents 1 of 50 ready decoders from retaining
  the full planned share, while the count remains an upper bound until the
  runtime reports stronger pairing evidence.

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
2. **Runtime-ready, capacity-bounded weights** — use the minimum of admitted,
   ready, and optional reported capacity, then apply the configured factor and
   probe gate. *Rationale:* never route above the plan or to capacity that cannot
   currently serve.
3. **A gateway-neutral publisher.** Contract independent of any one data plane;
   retarget the existing Gateway API publisher; deployment-specific backends and
   SMG are optional. *Rationale:* upstreamable contract, pluggable data plane —
   matching the `endpoint` package, which already anticipates "backends added
   later".
4. **Fast, explicit cutoff** — a home losing all ready replicas reflects as
   weight 0 within one reconcile and the publisher reprograms.
5. **Opt-in** via Helm; disabled means exactly today's behavior.
6. **Observed signals may refine either input — never mandatorily.** Each is
   **independently** toggled and off unless configured. *Rationale:* the control
   plane holds the *plan* and the endpoint holds the *truth*, so trusting only one
   is wrong in one of the failure modes above — but *requiring* both makes every
   install pay for a data-plane dependency it may not need.
7. **Inline, per-ISVC routing behavior.** `spec.routing` is a top-level sibling
   of `spec.placement`; a model owner can change probe, reported-capacity, and
   safe publisher behavior with the ISVC itself. *Rationale:* model paths and
   capacity contracts vary by runtime, and coupling those changes to an
   installation ConfigMap creates an unnecessary second rollout.
8. **One installation-selected publisher, shared by every ISVC.** The publisher
   name, network endpoint, TLS material, and credentials remain operator-owned;
   only options that the selected publisher explicitly marks per-ISVC-safe may
   vary. *Rationale:* one ISVC must not redirect OME's privileged identity, and
   per-ISVC publisher instances would multiply connection pools and cleanup
   owners for no routing benefit.

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
- **Selecting a different publisher implementation per ISVC.** One installation
  has one publisher lifecycle and one credential boundary. A workload may change
  safe mapping behavior, not choose another backend or another network target.
- **One background loop or goroutine per ISVC.** Per-ISVC periods are data in
  one leader-owned controller workqueue; they do not create per-object
  background loops.

## Proposal

### Terms

- **Home.** A workload cluster serving a share of an ISVC — a `Candidate` that
  reached `Placed`/`Admitted` and reported an addressable `endpoint`. One home =
  one routing target.
- **Capacity allocation (`a_h`).** The home's quota-admitted replica count,
  optionally lowered by endpoint-reported capacity. It is an upper bound on the
  weight's numerator.
- **Routable capacity.** `min(a_h, readyReplicas)`. An unready home is always
  zero. A failed probe normally gates this value to zero; `PreserveTraffic` may
  ignore only probe gates after every home reports a conclusive probe failure.
- **Capacity source.** Where `a_h` comes from: `ControlPlane` (default) reads
  `status.placement`; `Endpoint` asks the home and uses the answer as a
  **ceiling** on the plan.
- **Reachability probe.** An active request the control plane issues *to a home's
  `endpoint`* — the URL a client uses — so the result reflects the whole path,
  unlike `readyReplicas`, which is observed from inside.
- **TrafficMap.** The per-ISVC CRD holding the table: per home, endpoint + weight
  + the provenance of that weight.
- **Installation default.** The operator-owned probe, capacity, or safe publisher
  behavior used when the corresponding block is absent from `spec.routing`.
- **Inline override.** A complete per-ISVC probe or capacity block that replaces
  its installation default. It is not field-merged with the ConfigMap.
- **Publisher.** The single implementation selected for an installation, with
  one shared instance per manager process rather than one per ISVC. Leader
  election leaves one active mutator. Its transport and credentials are global;
  each publish call receives immutable effective options for that ISVC.

### Routing policy lives beside placement

Placement answers *where the workload runs*. Routing answers *how traffic is
measured and published after placement*. Nesting routing under placement would
make a data-plane policy look like a placement algorithm input, and copying the
source object to a workload cluster could accidentally start another routing
loop. Therefore, the source ISVC carries the two as siblings:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-70b
spec:
  placement:
    mode: Split
    split:
      replicas: 10
  routing:
    enabled: true                    # optional; false opts this ISVC out
    capacityFactors:
      cluster-a: "2"
      cluster-b: "1"
    probe:                           # complete block: replaces the global probe
      path: /v1/models
      method: GET
      acceptStatuses: [200]
      gateStatuses: [404, 500, 502, 503, 504]
      period: 10s
      timeout: 3s
      failureThreshold: 3
      successThreshold: 2
      allFailedPolicy: PreserveTraffic
    capacity:
      disabled: true                 # explicitly ignore a configured global poller
    publisher:
      options:                     # key-wise overlay of safe options only
        routingClass: premium      # illustrative; the publisher defines its keys
```

`spec.routing` is control-plane-only. Fan-out removes it from derived workload
ISVCs, as it removes other directives whose owner is the control plane; a member
cluster must never probe homes or publish a second global route.

#### Resolution rules

The resolution rule is intentionally asymmetric: probe and capacity are atomic
policies, while publisher options are a safe overlay on one shared client.

| Source shape | Effective behavior |
|---|---|
| Global `routing.enabled: false` | Routing is off for every ISVC. Inline fields cannot bypass the installation gate. |
| `spec.routing` absent | Inherit all installation defaults. The runtime-ready weight formula still applies; no per-ISVC observer or publisher override is introduced. |
| `spec.routing.enabled` absent | Inherit the installation's enabled behavior. The API uses a nullable boolean so absent and false remain distinct. |
| `spec.routing.enabled: false` | Opt this ISVC out. Stop observations and delete its `TrafficMap`; the map's publisher finalizer keeps it terminating until every claimed target is unpublished. Other inline fields may remain staged but are inactive. |
| `spec.routing.enabled: true` | Opt in when the installation gate is on; it cannot override global `routing.enabled: false`. |
| `spec.routing: {}` or one nested block absent | Inherit the installation default for each absent block. |
| `probe: {disabled: true}` | Turn probing off for this ISVC, even when the global default enables it. No other probe field may coexist with `disabled: true`. |
| Complete inline `probe` block | Replace the global probe **as a whole**. No field-by-field merge. |
| `capacity: {disabled: true}` | Turn endpoint capacity polling off for this ISVC. No other capacity field may coexist with `disabled: true`. |
| Complete inline `capacity` block | Replace the global capacity policy **as a whole**. No field-by-field merge. |
| `publisher.options` absent | Inherit all global publisher behavior options. |
| `publisher.options` present | Overlay named, publisher-declared safe keys; omitted safe keys inherit. Publisher name and protected transport/authentication options remain global. |

An empty present probe or capacity block is invalid, not shorthand for disabled.
A partial block is also invalid. This makes an inline policy self-contained: once
an ISVC supplies a probe, a later ConfigMap edit cannot silently change that
probe's method, threshold, or cadence. The API rejects `disabled: true` combined
with operational fields.

Publisher options differ because the implementation-specific surface is an
extensible string map. Each registered publisher classifies its keys as either:

1. **Global-only:** implementation name, remote endpoint or URL template,
   request transport settings, TLS material, identity, and credentials. These
   keys are not legal in an ISVC.
2. **Per-ISVC-safe:** routing target or tier mapping and other behavior the
   publisher can scope to one ISVC. Deployment-specific documentation defines
   the concrete keys; its network endpoint, resync cadence, and client identity
   remain global.

The per-ISVC map overlays only category (2). An unknown or global-only key fails
closed for that ISVC before any external request; it never becomes an arbitrary
option passed to a privileged client. The selected publisher name is deliberately
absent from the ISVC API. If a previously programmed ISVC is updated with an
invalid override, the publisher retains its last known-good applied state and
reports `Programmed=False`; it does not derive destructive cleanup from invalid
input.

`spec.placement.capacityFactors` predates this separation but controls routing,
not placement. `spec.routing.capacityFactors` is the primary field. During the
compatibility window, the controller honors the placement field only when the
new field is absent; admission rejects an object that sets both, rather than
silently choosing one. Migration copies the map to `spec.routing` and removes
the deprecated alias.

#### `RoutingSpec` schema (v1)

- `enabled` (nullable bool): absent inherits installation enablement; false opts
  this ISVC out; true still requires the global gate.
- `capacityFactors` (`map[string]Quantity`): per-home relative serving factors,
  keyed by WorkloadCluster name.
- `probe` (optional object): `disabled`, or the complete path, method, accepted
  and gating statuses, period, timeout, failure threshold, success threshold,
  and all-failed policy. Inline methods are limited to `GET`, `HEAD`, and `POST`;
  an ISVC cannot make the controller issue destructive or tunneling methods with
  its network identity.
- `capacity` (optional object): `disabled`, or the complete path, method, format,
  format options, sample window, quorum, period, timeout, and maximum report age.
  Its inline method has the same safe-method restriction.
- `publisher.options` (`map[string]string`): a key-wise overlay restricted to
  safe keys declared by the globally selected publisher. There is no per-ISVC
  publisher name, endpoint, TLS, or credential field.

### The routing projection resource: `TrafficMap`

`RoutingSpec` extends the existing ISVC resource; `TrafficMap` is the one new
resource kind. The control plane owns and writes it; humans read it but never
edit it — deliberately analogous to OEP-0024's distributor projecting a quota
tree onto native Kueue objects.

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
      weight: 7                # normalized capacity ratio
      healthy: true
      capacity:                # provenance: why the weight is what it is
        allocated: 7           # intended replicas on this home
        ready: 7               # live ready replicas
        factor: "1.0"          # per-replica relative capacity c_h
        source: ControlPlane   # which input produced `allocated`
    - cluster: cluster-b       # second home, 3 replicas → weight 3
      endpoint: https://llama-3-70b.team-a.cluster-b.example.com
      weight: 3
      healthy: true
      capacity: {allocated: 3, ready: 3, factor: "1.0", source: ControlPlane}
  observedISVCGeneration: 42   # the ISVC generation this table came from
status:                        # independently written by routing and publisher
  sourceUID: 4424bd9b-9986-4de0-b17b-b3cdbd920f34
  programmed: true
  gatewayRef: {group: gateway.networking.k8s.io, kind: HTTPRoute, name: llama-3-70b}
  observedTrafficMapGeneration: 5
  conditions: [{type: Programmed, status: "True", reason: HTTPRouteReconciled}]
```

Two writers, one object:

- The **routing controller** owns `spec` — recomputes `entries` whenever
  `status.placement` or the home's quota allocation changes — plus
  `status.sourceUID` and the TrafficMap's `Routable` condition.
- The **publisher** patches the TrafficMap's `Programmed`, `gatewayRef`, and
  observed-generation fields plus its publisher journal. It does not replace
  sibling status, `sourceUID`, or the routing controller's `Routable`
  condition.

*Anticipated objection — "a machine-written `spec` is unusual; spec is user
intent."* True, and we considered a status-only shape (Alternatives). The
computed table stays in `spec` because it is a *declarative artifact* the
publisher consumes like any other desired state — the same generation
(control plane) / realization (publisher) split as ISVC→derived-ISVC. The object
is never hand-edited; a validating webhook may reject non-controller writes to
make that explicit.

### How a weight is computed: the five variables

```
base_h   = min(a_h, r_h, optional reported_h)
raw_h    = base_h × c_h
raw_h    = 0                # when the probe gate is closed
weight_h = normalize(raw_h) # relative ints, per Gateway API
```

Five variables feed that. Three are computed by the control plane and always
present; two are observed from the home and are **individually optional and off
by default** — enabling one never enables the other, and with both off the
control plane makes no outbound call at all.

1. **`a_h` — admitted capacity** *(always; an upper bound on the numerator)*.
   Placement and quota determine how many replicas a home may serve. Readiness
   and an optional endpoint report can lower this number but never raise it. In
   `Single`, normalization still yields one weighted entry while capacity is
   positive; the map exists so cutoff on a failed winner is expressible.

2. **`c_h` — per-replica capacity factor** *(always available; an omitted field
   has the semantic identity value `1.0`)*.
   Replicas are not fungible across accelerators: 100 replicas on each of two
   types is not 50/50 if one serves 2× the tokens/sec. Precedence: an explicit
   `spec.routing.capacityFactors[home]` value — the accurate source, since the
   ratio is workload-specific (model × runtime × precision), not a hardware
   constant — then an opt-in `AcceleratorClass` default, then `1.0`. The
   deprecated `spec.placement.capacityFactors` alias is read only when the new
   field is absent. The controller applies the identity when no factor is set;
   the CRD does not server-default the generated TrafficMap field because doing
   so would make the API server and controller repeatedly add and remove it.
   **Never auto-derived from TFLOPS**: raw FLOPS is not a reliable throughput
   proxy across vendors. Affects *weighting only*, not placement.

3. **`r_h` — ready replicas** *(always; part of routable capacity)*. The base
   count uses `min(a_h, r_h)`, so partial readiness reduces the share and
   `r_h == 0` always produces weight 0. Admitted capacity remains a ceiling, so
   readiness can remove traffic but cannot grant more than placement allowed.

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
   - **Still not pair-aware anywhere.** An existing router surface does report a
     capacity figure, so the *plumbing* is buildable — but it counts prefill-side
     accelerators and does not observe decode, so it reports full capacity in
     exactly the skew window this input was justified by. Treat it as coverage
     for unhealthy or ejected accelerators, not for skew. A genuinely pair-aware
     endpoint remains a data-plane prerequisite.
   - **A reading may need converting.** A figure in weighted accelerators is not
     the same quantity as a replica count, and one that already folds in
     hardware weighting must not then be multiplied by `c_h` — that applies the
     weighting twice. Conversion is operator-supplied config, and pairing an
     already-weighted source with a per-home factor is rejected rather than
     silently double-counted.
   - **The response shape is pluggable, and only one shape is ours.** The
     control plane defines a single format — an absolute servable count plus a
     producer-set timestamp — and registers it like any other; a deployment
     compiles in more for producers it does not control. *Rationale:* a format
     is not merely a parse target. Where a number lives is the easy part; what
     it means decides whether a ceiling is safe, so a format owns its decoder,
     its own configuration, and whether the producer timestamps its reading at
     all — which is what determines whether the staleness bound can fire.
     Naming a format the binary does not carry is a startup error listing what
     it does, never a silent fallback to the plan: a dropped setting and a
     satisfied one are indistinguishable from the resulting weight.

5. **Probe result — ANDed into `healthy`** *(optional, off by default)*. A
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
     prober is fleet-correlated, so the default `PreserveTraffic` all-failed
     policy ignores only the probe gates and recomputes from current ready
     capacity. An operator may instead select `Drain` for an installation where
     fresh, conclusive failures from every home must produce an intentional
     all-zero table. Neither policy revives an unready or zero-capacity home.

The two logical observers share one transport and bounded request executor (same
surface, same credentials, same failure modes), but they stay separately toggled:
sharing transport is not bundling the features.

### End-to-end flow

1. **Placement + quota run (existing).** `status.placement.candidates[]` carries
   each home's `endpoint`, `admittedReplicas`, `readyReplicas`.
2. **The controller resolves routing policy.** The installation gate wins
   first; absent ISVC blocks inherit defaults, complete probe/capacity blocks
   replace them, and safe publisher keys overlay their defaults. Resolution is a
   pure per-reconcile value, not mutable publisher state.
3. **Routing controller generates `TrafficMap`.** Watches the source ISVC and the
   relevant quota allocation, computes the weights, writes `spec`. One per ISVC,
   owner-referenced for GC.
4. **Observation refines the inputs (optional, off by default).** The shared
   routing controller workqueue polls capacity and probes each endpoint according
   to that ISVC's resolved periods. Either input failing to answer falls back to
   the control-plane value and records why.
5. **The shared publisher programs the data plane.** It reads `entries` rather
   than re-deriving from `status.placement`, receives the ISVC's resolved safe
   options for that call, and writes `status.programmed`.
6. **Cutoff.** A home losing all ready replicas flips `healthy:false`/`weight:0`
   in the next reconcile. A home failing N consecutive probes does the same while
   another home still passes; a fleet-wide failure follows `allFailedPolicy`.

### Why a CRD and not just ISVC status

The per-ISVC routing facts *do* already exist in `status.placement`; the question
is whether to consolidate them.

| Dimension | Reuse `status.placement` only | New `TrafficMap` CRD (derived) |
|---|---|---|
| Gateway coupling | Watches **all** ISVCs, re-derives split | Watches **one** object kind per service |
| Capacity-aware weights | No — ready-replicas only | Yes — admitted ∩ ready ∩ reported capacity, probe-gated |
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
    `weight` (int32, 0–1,000,000), `healthy` (bool). The upper bound matches the
    Gateway API backend weight contract; larger exact ratios are proportionally
    compressed without turning a positive arm into zero.
  - `capacity.allocated` / `.ready` (int32), `.factor` (quantity, semantic
    default `1.0` when omitted): weight provenance —
    `weight ∝ min(allocated, ready) × factor`, then probe-gated.
  - `capacity.source` (enum `ControlPlane|Endpoint`, default `ControlPlane`):
    which input produced `allocated`. *Rationale:* without it an operator cannot
    distinguish "the home reported less capacity" from "the override failed open"
    — same number, opposite remediations.
  - `capacity.fallbackReason` (string, optional): why a configured endpoint
    source did not produce a usable ceiling and the control-plane allocation was
    retained. Empty means polling is disabled or its latest result was usable.
  - `probe` (optional, present only when probing is configured): latest attempt
    result, current hysteresis gate, attempt time, consecutive-failure count, and
    a `sha256:<64 lowercase hex>` digest of the complete effective probe policy.
    `result: Unknown` leaves the prior `gated` value unchanged. *Rationale:*
    `healthy:false` alone does not say whether readiness or reachability failed,
    and those point at different teams. The digest makes persisted hysteresis
    attributable to the policy that produced it without duplicating that policy
    into every entry.
- `spec.observedISVCGeneration` (int64) for staleness detection.
- `status.sourceUID` (the source ISVC UID), `status.programmed` (bool), `status.gatewayRef`,
  `status.observedTrafficMapGeneration` (int64), `status.publisher`, and
  `status.conditions[]`.

The `TrafficMap` carries the state needed to reverse publisher side effects:

```yaml
status:
  sourceUID: 4424bd9b-9986-4de0-b17b-b3cdbd920f34
  publisher:
    publisherName: ExamplePublisher
    claimedTargets: [team-a/model-service]
    observedOptionsDigest: sha256:0000000000000000000000000000000000000000000000000000000000000000
```

`publisherName` identifies the implementation whose cleanup semantics apply; an
implementation upgraded under the same name must remain able to clean canonical
identifiers written by its prior versions.
`claimedTargets` is a conservative write-ahead ownership set: the publisher
atomically claims the complete canonical desired set before any external call,
retains targets it has drained, and removes a claim only after idempotent
unpublish succeeds. `observedOptionsDigest` identifies the effective safe
behavior from the last successful programming pass; it may be absent while a
first attempt has claims but has not completed. The normal `programmed` fields
report successful realization separately from this cleanup authority.

The status schema caps publisher names at 128 characters and the claim set at
128 opaque identifiers of at most 512 characters. Before any external mutation,
a publisher validates and canonicalizes the complete desired set and verifies
that `existing claims ∪ desired targets` fits those bounds. If it cannot fit, the
publisher leaves external state unchanged and reports the limit; the operator
can disable routing, wait for cleanup, correct the mapping, and re-enable it. No
publisher-defined request payload is stored in this shared API: retries converge
from the latest TrafficMap through an idempotent publisher contract, while the
conservative claim set makes every possibly touched target discoverable for
drain and cleanup. Claim identifiers are status-visible and never contain
credentials or secret material.

The routing controller owns `status.sourceUID` through its isolated status
field manager. A publisher must observe that value matching the live controller
owner before planning or mutating new external state. Publisher finalization is
different: the durable claim journal remains its sole cleanup authority, so
unpublish does not require the source or its owner reference to remain readable.

### Ownership & lifecycle

- **One `TrafficMap` per ISVC**, `metadata.name == isvc.name`, namespaced to it.
  *Rationale:* mirrors HTTPRoute-per-service; trivial ownership, GC and RBAC;
  scales linearly with ISVC count, no single hot object.
- **Owner-referenced** (`controller: true`) → GC requests its deletion with the
  ISVC. Before any external mutation, a stateful publisher adds its finalizer to
  the TrafficMap and persists the complete claim set. Direct deletion or owner
  GC then leaves the map terminating, with its status journal intact, until the
  publisher has cleaned every claim and removed the finalizer. The publisher
  reconciles TrafficMaps directly, so cleanup does not require the owner ISVC to
  remain readable. `status.sourceUID` preserves the generated map's identity if
  orphan propagation removes its owner reference. For an older map with an
  exact live owner and no source UID, active eligible routing first normalizes
  its spec and then writes the UID before it may be published. Reaping can rely
  on the exact owner reference to delete that legacy map while the UID remains
  empty, without creating a transient publication authorization. A non-empty
  mismatch is never overwritten or deleted.
- **Publisher claims are controller-owned.** The publisher records exact
  target claims in `TrafficMap.status.publisher`, which survives until the map's
  finalizer cleanup. A normal, user-editable metadata annotation is **not** an
  authority for deletion, zeroing, or ownership. *Rationale:* if a user can edit
  the cleanup journal, the operator's privileged publisher becomes a confused
  deputy against another service's target. ISVC status carries no duplicate
  journal. On ISVC deletion, the routing controller requests deletion when the
  map has either the exact controller owner (including a legacy map with empty
  provenance) or no controller and a matching `status.sourceUID`. A conflicting
  controller is never overridden. The existing placement finalizer retains the
  source and its derived workloads while an exact-owner or matching-provenance
  map remains. It also fails closed for an ownerless legacy map with empty
  provenance when publisher status, its finalizer, or another publisher-owned
  status field shows possible external publication. An exact owner with
  mismatched provenance, or a conflicting controller with matching provenance,
  remains blocked for manual resolution. An ownerless provenance mismatch, or a
  conflicting controller without matching provenance, is foreign and does not
  block teardown. Both checks use authoritative reads; the routing delete
  carries UID and resource-version preconditions. A direct same-key TrafficMap
  watch keeps finalizer-repair and cleanup events live after orphaning. This
  makes TrafficMap publisher cleanup a barrier before backend teardown without
  adding routing state to the ISVC.
- **Created only for placement-eligible ISVCs** (those with a non-empty placement
  requirement or cluster selector) and only when both the installation gate and
  the ISVC's resolved enablement are true. A
  `Single`-mode ISVC gets a one-entry map so winner-cutover is expressible; a
  non-placement ISVC gets none. On per-ISVC or installation-wide disable, the
  routing controller stops observations and deletes the `TrafficMap`. Its
  publisher finalizer turns that delete into the cleanup request and preserves
  the journal until unpublish completes; only then can the map disappear.
- **The map exists for the whole enabled routing lifetime, even with zero
  entries.**
  Emptiness is a *state of the map*, never its absence. *Rationale:* absence is
  unattributable — feature off, ISVC not placed, no addressable home, and a wedged
  controller all look identical. Worse, deleting on degradation removes the object
  exactly when it is most wanted: a map serving real traffic would vanish the
  moment its last home stopped being addressable, taking `capacity.allocated`,
  `capacity.ready` and the factors with it — the very fields that say *what* went
  away. Kubernetes makes the same call: a selector `Service` keeps its
  `EndpointSlice` when no pod is ready.
- **An empty or all-zero map must say why**, or we trade "absent and ambiguous"
  for "present and ambiguous". The router owns a `Routable` condition — `True`
  when at least one entry has positive weight, else `False` with reason
  `NotPlaced`, `NoAddressableHome`, `AllHomesUnready`, `NoRoutableCapacity`, or
  `AllHomesProbeFailed`. A PreserveTraffic fallback is `Routable=True` with
  reason `AllHomesProbeFailed`. `status.conditions` is a
  `listType=map` keyed on `type`, so `Routable` and the publisher's `Programmed`
  coexist under server-side apply — but they are two writers on one subresource
  and each must patch only its own condition.
- **A policy change resets only affected observations.** Changing a probe block
  discards that ISVC's old hysteresis counters and starts inconclusive; changing
  a capacity block discards its sample window and falls open to the plan until
  the new quorum is met. Reusing observations across different paths, formats,
  or thresholds would make the new policy inherit evidence it never collected.
- **Restart hydration requires an exact identity match.** Persisted probe state
  is reusable only when the TrafficMap owner UID, map namespace/name, entry
  cluster and endpoint, and probe policy digest all match the current target and
  resolved policy. Missing or mismatched identity starts inconclusive and
  ungated instead of importing an unrelated gate or hysteresis counter.
- **Publisher mapping changes reconcile old state first.** The controller
  validates the complete canonical desired set, preflights the bounded union,
  and atomically persists that union as the durable claim set **before** any
  external mutation. It drains every claimed target no longer desired and only
  then idempotently applies the latest desired values. Retired targets remain
  claimed while held at zero. A crash after the
  claim write, midway through drain or apply, after a lost response, or before a
  success-status write is safe: the retry sees every target that may have been
  touched, drains stale claims again, and converges to the latest TrafficMap.
- **Legacy applied state is migration input, not authority.** Before upgrading, an
  operator should clean the legacy annotation through the old publisher. If an
  in-place migration is required, global configuration must authorize exact
  `(namespace/name, canonical-target)` pairs that may be copied. A target listed
  for one ISVC cannot be claimed by another. A malformed, mismatched, or unlisted
  annotation value causes no external mutation and reports a migration-blocked
  condition; the controller never treats arbitrary annotation text as a target.

### Publisher contract (pluggable, gateway-neutral)

A `Publisher` reads a `TrafficMap`, reconciles exactly one data-plane
realization, and reports `status.programmed`. Global config selects exactly one
implementation for the installation; each manager process constructs one shared
instance, and leader election permits one active mutator. The instance owns one
transport and connection pool; each call receives the effective, immutable
options for the reconciled ISVC, so concurrent reconciles cannot leak one
service's tier mapping into another.

Before a publisher mutates external state, it must:

1. Require the routing-owned `status.sourceUID` to match the live, exact
   controller owner. Missing or mismatched provenance fails closed before plan,
   claim, drain, or apply. Deletion finalization remains owner-independent and
   follows the durable claim journal.
2. Reject unknown and global-only per-ISVC option keys.
3. Render and canonicalize target names. Every publisher that exposes a
   target-affecting per-ISVC option must enforce an operator-owned target scope
   (for example, an allowed namespace or anchored prefix). A template is not
   permission to name an arbitrary target; missing or malformed scope
   configuration always fails closed before any external request.
4. Reject duplicate targets within one map and collisions with another ISVC's
   controller-owned claims.
5. Add the TrafficMap finalizer, preflight `existing claims ∪ desired targets`,
   and atomically persist that complete canonical set before the first external
   mutation. Every mutation and cleanup call must be idempotent so a retry can
   converge from the latest desired map.

The TrafficMap publisher controller sets `MaxConcurrentReconciles=1` as a
non-tunable correctness invariant. Target claim lookup uses the uncached API
reader, and claim persistence uses API-server conflict detection before any
external call. Lookup, persistence, and external mutation are therefore
serialized on the elected leader; two first-time reconciles cannot both observe
an unclaimed target and publish it, even when the ordinary controller cache lags.
Observation work may remain concurrent because it cannot claim or mutate
publisher targets.

A publisher that relies on this serialized external-target journal declares
itself stateful. Manager startup rejects a stateful publisher when leader election
is disabled; process-local `MaxConcurrentReconciles=1` is not presented as a
cross-process lock. A future mode that intentionally runs multiple active
publisher processes must first replace this invariant with an atomic per-target
claim object.

If the globally selected publisher name differs from
`TrafficMap.status.publisher.publisherName` while `claimedTargets` is non-empty,
the new publisher does **not** guess how to clean another implementation's state
and does not publish on top of it. It reports a publisher-mismatch condition
instructing the operator to restore the old publisher, disable routing and wait
for cleanup, then perform the configured publisher switch.

Stateful backends must also define **drain** and **unpublish** separately. Drain
keeps the target under controller ownership at weight zero; unpublish removes
the override or object. They are not interchangeable: in a runtime-weight
backend, writing zero stops traffic, while deleting the runtime value restores
the statically authored fallback weight and may resume it. Therefore, removing a
target from an active mapping drains it before new targets are published and
retains the zeroed target in its claim set while its consuming route may still
exist. The v1 publisher cannot observe that external route, so it does not
release one retired target independently: retired targets remain owned at zero
until whole-ISVC disable or deletion, after the operator has completed the
documented external-route teardown. Finalizer-driven unpublish may clear them
only as that sequence's last step.

- **Gateway API HTTPRoute** (retargeted from today's status-reading path): one
  HTTPRoute, one `backendRef` per home (an ExternalName Service aliasing the home
  endpoint, as today), `weight` per entry. TrafficMap weights are authoritative
  and are applied verbatim, including all-zero results caused by readiness,
  capacity, or the probe's `Drain` policy. Legacy placement-only publication
  retains its equal-weight fallback. Before mutation, the adapter reads live
  HTTPRoutes and rejects another route with the same exact hostname attached to
  the same effective Gateway.
- **External runtime weights** (optional, deployment-specific): render one
  target per home from the entry and its safe per-ISVC mapping options, then call
  the globally configured automation endpoint. This publisher changes
  **weights only** — it does not create client discovery, deploy proxy tasks, or
  author the consuming weighted route. Those are explicit data-plane
  prerequisites; concrete option names and transport details belong with that
  publisher's implementation documentation.
- **SMG** (optional): SMG consumes `TrafficMap` **natively** — a watcher inside
  SMG populates its worker registry and splits by entry weight. Because we own
  that codebase this is a native reflector mirroring its existing pod
  service-discovery, not an out-of-process adapter or xDS push. Here SMG is the
  consumer, so the "publisher" is a no-op beyond marking `status.programmed`; the
  contract is identical.

Changing the global publisher name is deliberately not a live per-ISVC switch.
The same rule applies to any global endpoint, authority, or namespace setting
that changes where a canonical target must be cleaned. For a stateful publisher
the operator first disables routing, waits until the old publisher's finalizers
and target claims are clean, changes the configuration, and then re-enables
routing. This keeps one cleanup owner throughout the switch.

### Enabling routing, and the no-magic-values rule

Per the repo's hard constraint (no behavioral value baked into the binary):

- A Helm toggle gates the routing controller and publisher; **default off**, and
  off means today's behavior. It is a hard upper bound: an ISVC cannot opt in
  when the installation is off. When it is on, an optional
  `spec.routing.enabled: false` opts one ISVC out without changing other
  services.
- The active publisher (`gatewayapi` or an optional deployment-specific
  backend), gateway or remote publisher endpoint, backend port, request
  transport, TLS material, and credentials come from the operator ConfigMap /
  Helm values — **no in-code fallbacks and no per-ISVC override**. Absent required
  config degrades gracefully (routing is not programmed and reports why) rather
  than guessing a destination.
- Global probe and capacity blocks are defaults, not mandatory profiles. An
  inline block is stored with the ISVC and takes effect from its watch event; it
  requires neither a ConfigMap edit nor a manager restart.
- Control-plane installations do not install the catalog-dependent general ISVC
  validator. Cross-field routing invariants that CEL can express therefore live
  in the CRD schema, and the per-reconcile resolver repeats the complete
  validation before returning effective configuration. Publisher-specific
  semantic validation remains with the selected publisher and runs before side
  effects.
- The two observed inputs default to absent. Whether supplied globally or
  inline, the capacity source and probe path, method, status codes, period,
  timeout, and thresholds remain explicit. There is **no in-code probe path and
  no in-code status list** — an unconfigured probe does not run, rather than
  silently probing a guessed `/health`. *Rationale:* a guessed path is the
  hardcoded-magic-value failure mode in a health-check costume, and its specific
  harm is that it appears to work — a shallow probe against a wrong-but-live path
  returns 200 forever.

### Shared workqueue scheduling and bounded concurrency

Per-ISVC policy changes the data, **not** the controller topology. The elected
leader owns the routing controller and one shared HTTP transport. A reconcile
runs the observations due for that ISVC, persists or caches their per-home due
times, then returns `RequeueAfter` for the earliest next probe or capacity poll.
Controller-runtime's delayed workqueue schedules the object; no ISVC gets a
ticker or long-lived goroutine of its own.

A policy update recalculates that object's deadline, and deletion drops its
observation state. The global operator fields
`routing.observer.maxConcurrentReconciles` and
`routing.observer.maxConcurrentRequests` bound ISVC-level work and the shared
fixed-size request executor, respectively; their Helm values are explicit, and
an enabled observer rejects missing or non-positive values instead of using an
in-code fallback. Operator-owned `routing.observer.minPeriod` and
`routing.observer.maxSamples` bound tenant-supplied cadence and capacity-window
cost; an inline value outside those limits fails closed. The executor pulls due
homes from a job channel. It must not start one goroutine per target and park
excess work behind a semaphore — that still creates O(targets) goroutines.
Per-request context deadlines may differ by ISVC, but both active requests and
worker goroutines remain bounded as ISVCs are added.

Publisher resync follows the same rule: the shared TrafficMap publisher
controller uses its globally configured cadence and delayed workqueue. It does
not start a publisher loop for every map.

### Where it runs

The routing controller runs in the **control plane** (OEP-0019) alongside the
placement fan-out controller — it needs the aggregated `status.placement`, which
only the control plane sees. The publisher runs there too. Neither runs on
workload clusters.

### Interplay with adjacent OEPs

- **OEP-0018 placement.** Downstream of it; consumes
  `status.placement.candidates[]` and changes nothing about the race, GC, or
  apportionment. Placement is cluster-granular, so entries are too — one per home,
  never per pod. Per-home capacity factors move to `spec.routing` because they
  affect only traffic share; the old placement field remains a deprecated read
  alias for one compatibility window.
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

### Implementation sequence

This change should land as sequential, independently reviewed PRs, each branched
from the then-current main branch rather than as a GitHub stack:

1. **API and resolver.** Add top-level `RoutingSpec`, the deprecated
   `placement.capacityFactors` compatibility path, validation/defaulting,
   generated artifacts, and a pure global-plus-inline resolver. Strip routing
   from derived ISVCs. With `spec.routing` absent, this PR must not change
   runtime behavior. This is an API foundation: apart from the capacity-factor
   compatibility read, observer and publisher overrides remain inert until the
   following PRs wire them to side-effecting code.
2. **Shared observation scheduling.** Change probe and capacity lookup from one
   installation-wide policy to per-target effective policy, using the routing
   controller's delayed workqueue, one shared transport, and bounded request
   concurrency. Reset only the observation state whose policy changes.
3. **Shared publisher overrides and lifecycle.** Keep one publisher instance;
   pass effective safe options per reconcile, enforce the option/target scope,
   serialize its reconciler, move target-claim authority into
   `TrafficMap.status.publisher`, and safely migrate or reject legacy annotation
   state. Reconcile TrafficMaps directly, add their publisher finalizer before
   side effects, use field-isolated status patches, and drain old claims before
   publishing new targets. Predicates must observe routing enablement,
   publisher-option changes, deletion, and claim cleanup so finalization cannot
   stall.
4. **Rollout and E2E.** Exercise inheritance first on an existing service, then
   add its inline probe and target mapping. Verify the TrafficMap-to-publisher
   path before testing data-plane weight shifts. Rollback removes the inline
   block to restore global defaults; a global publisher change follows the
   disable-clean-switch-enable sequence above.

## Test Plan

- **API/defaulting.** Round-trip `spec.routing` as a sibling of `placement`;
  verify a derived workload-cluster ISVC contains no routing block. Validate that
  `disabled: true` is exclusive with operational fields and an empty or partial
  inline probe/capacity block is rejected. Verify the selected publisher's
  resolver rejects a publisher name or protected endpoint/TLS/credential option
  before any external mutation; the opaque map cannot express those as typed
  first-class ISVC fields.
- **Resolution matrix.** Table-drive global gate × global default × inline shape:
  global off always wins; absent ISVC enablement preserves global behavior;
  explicit ISVC false opts out and cleans up; absent nested blocks inherit;
  complete probe/capacity blocks replace atomically; explicit disabled turns one
  input off; safe publisher keys overlay while omitted keys inherit. Mutating the
  returned effective options for ISVC A must not alter global defaults or ISVC B.
- **Capacity-factor migration.** New field only works; deprecated placement alias
  only produces identical weights; both fields set is rejected. Generated CRDs,
  clients, deepcopy code, and OpenAPI all expose the new field and deprecate the
  alias without deleting it.
- **Unit (weight math).** Table-driven: admitted `7:3`, ready `4:3` → `4:3`;
  one home `ready:0` → `100/0`; all `ready:0` → all-zero; `All` also follows
  ready counts; `Single` → one entry.
- **Unit + controller (compatibility).** With `spec.routing` absent, observer and
  publisher settings inherit the installation defaults exactly; with no global
  capacity source or probe, the controller issues **zero** outbound requests.
  The runtime-ready weight formula applies independently of inline routing
  configuration. Each observed input is also exercised alone, so enabling one
  never activates the other.
- **Unit (capacity ceiling).** `reported > plan` → plan wins; `reported < plan` →
  reported wins; absent or stale → plan wins, `capacity.source` stays
  `ControlPlane`, `capacity.fallbackReason` is set. Two ISVCs with different
  formats, periods, and quorum settings retain independent sample windows;
  changing one capacity block clears only that ISVC's window.
- **Unit (probe status policy).** `429`/`401`/`403` do not gate; `5xx` and
  transport errors do; "probe could not run" gates nothing even when it affects
  every home; the gate flips exactly at the Nth failure and recovers exactly at
  the Mth success. Two ISVCs with different paths and thresholds do not share
  counters; changing or disabling a probe clears its prior gate.
- **Unit (shared scheduling).** With a fake clock, differently-perioded ISVCs
  return the correct `RequeueAfter`; updates replace deadlines; deleted targets
  lose state; publisher requeues are per object. Assert the controller's
  concurrency and fixed request-worker count match the two global observer
  settings and stay within those bounds as target count grows. Missing or
  non-positive settings fail validation when observation is enabled. The
  implementation must not launch one blocked goroutine per target. Do not assert
  a fragile process-wide goroutine count — assert there is no per-object runnable
  or ticker and the executor creates at most its fixed worker bound.
- **Controller (envtest).** `status.placement` transitions → `spec` converges;
  routing-policy-only changes enqueue; TrafficMap deletion and publisher-claim
  cleanup enqueue the publisher directly; direct TrafficMap deletion remains
  blocked on its finalizer until claims are clean; quota change → weights
  recomputed;
  `observedISVCGeneration` tracks the source.
  Predicate-level tests cover each transition independently. A home failing N
  probes → `healthy:false`, weight 0, survivors renormalize; **every** home
  failing → ready-capacity weights under `PreserveTraffic`, or all-zero under the
  explicit `Drain` policy. Per-ISVC opt-out unpublishes and removes its map while
  leaving other ISVCs active; inline true cannot bypass a disabled global gate.
- **Publisher isolation and safety.** Construct the publisher once, exercise two
  ISVCs with different immutable safe options, and prove each outbound request
  uses only its own effective mapping. Assert the controller configuration keeps
  publisher reconciles at one worker, then race two same-target enqueue events
  and prove the serialized, uncached claim rejects the second even when the
  informer cache is stale. A stateful publisher with leader election disabled is
  rejected at startup. Unknown/protected keys, missing or malformed target scope,
  out-of-scope targets, duplicates, cross-ISVC collisions, and a claim union over
  the API bound produce no external mutation and set a precise
  `Programmed=False` reason.
- **Publisher lifecycle (envtest + fake backend).** `TrafficMap.spec` programs the
  expected targets; a target-mapping update atomically preclaims the full new
  set, drains old claims before publishing new ones, and survives failure or
  restart at every mutation boundary, including a successful external write
  followed by a lost response or failed status patch. Recovery idempotently
  converges to the latest TrafficMap while every possibly touched target remains
  claimed. A retired target remains claimed at zero until whole-ISVC
  disable/deletion. Tampering with ordinary annotations cannot cause a zero or
  delete. Direct TrafficMap deletion and global disable retain the map's
  finalizer until all claims are unpublished. Exercise drain (`weight=0`)
  separately from unpublish (remove override/static fallback).
- **Applied-state migration and status ownership.** Pre-upgrade cleanup leaves no
  legacy state. In-place migration copies only an exact allowlisted
  `(namespace/name, target)` pair; a target authorized for another ISVC, or any
  malformed, injected, or unlisted annotation, causes no external call. A
  publisher-name mismatch with outstanding claims blocks both cleanup and new
  publication. Active eligible routing backfills `status.sourceUID` only after
  normalizing a map with an exact live controller owner; reaping may instead
  delete such a legacy map while provenance is missing. Missing or mismatched
  provenance permits no new publisher effect, and a mismatch also prevents
  routing deletion. Concurrent routing and publisher status updates prove the
  publisher patch changes only `TrafficMap.status.publisher` and its own
  `Programmed` fields—no source UID, sibling status, or condition is lost.
- **Deletion ordering and orphan propagation.** Deleting an ISVC requests a
  UID/resource-version-fenced delete for an exact-owned map or an ownerless map
  with matching durable provenance. Placement keeps every derived backend while
  that map, a conflicting matching-provenance map, or ambiguous legacy
  publication evidence remains, and proceeds only after authoritative absence.
  Confirmed foreign maps remain untouched and do not block teardown; ownership
  conflicts, read failures, and replacement races fail closed.
- **Gateway API publisher (envtest).** `spec` → one HTTPRoute with weighted
  backendRefs; entry removed → backendRef dropped and stale ExternalName Service
  deleted; `status.programmed` set; finalizer removes the route on map deletion.
- **Integration (multi-cluster harness, OEP-0020).** fan-out → admit → map
  generated → gateway programmed → kill a home → weight→0 within one reconcile.
  Run two ISVCs with different inline probe paths and publisher mappings, change
  one object, and prove the other's schedule, targets, and weights do not move.
- **Integration (kind).** With kube-controller-manager garbage collection
  running, delete the owner ISVC and verify the TrafficMap enters deletion while
  its publisher finalizer preserves the journal through cleanup.
- **Upgrade and live E2E.** Deploy the new manager over an installation whose
  ISVC omits `spec.routing`; verify inherited observer/publisher settings and
  confirm weights follow current ready capacity. Then add an inline probe to
  that ISVC, observe the configured path
  (without a doubled slash), verify non-zero healthy weights, and confirm the
  published backend values match. For a stateful weighted backend, separately
  verify `1/1 → 1/0 → 0/1 → 1/1` without restarting its data-plane proxy.
- **Leak-free fixtures** (`example.com`, generic cluster names).

## Graduation Criteria

- **Alpha.** CRD + generator + Gateway API publisher behind the Helm toggle
  (default off); top-level `spec.routing` with deterministic inheritance and
  validation; one shared publisher and workqueue-scheduled observation path;
  `Split`/`All`/`Single` runtime-ready weight math. Unit + envtest.
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
- **Two configuration layers require explicit resolution.** Global defaults plus
  inline behavior are more code than an installation-only ConfigMap. Atomic
  probe/capacity replacement and a published resolution matrix bound the cost;
  field-wise merging would make the effective policy too difficult to audit.
- **Publisher option maps are less self-describing than typed fields.** The
  global-only/per-ISVC-safe registry keeps the core API gateway-neutral, but an
  unsupported key is found by the selected publisher rather than OpenAPI alone.
  A precise condition and fail-before-side-effect validation are required.
- **Readiness changes can move traffic shares during a rollout.** This is
  intentional: the map tracks capacity that can currently serve while admitted
  replicas remain the upper bound.
- **The observed inputs add a control-plane → data-plane dependency in a direction
  that did not previously exist.** Outbound calls to every home on a timer: new
  credentials, new egress, new timeouts, and — if the probe is deep — real serving
  capacity consumed on the homes being checked. It also makes the prober
  fleet-correlated. Fail-open and hysteresis are mitigations, not a full fix: a
  prober returning confidently wrong answers can still drain a home. Selecting
  the all-failed `Drain` policy accepts the additional risk of a fleet-wide
  false-positive in exchange for never routing to a set of confirmed failing
  homes.

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
6. **`profileRef` from each ISVC to a routing ConfigMap.** Centralizes policy but
   makes every model-specific path or threshold change a second-resource rollout
   and lets a shared profile edit change many services at once. Rejected as the
   primary API; installation defaults still cover the deliberate shared case.
7. **Field-wise merge of inline probe/capacity blocks.** Saves repeated fields,
   but an ISVC that overrides only `path` still changes when an operator edits
   the global timeout or failure threshold. Rejected: a present policy is complete
   and reviewable in one object.
8. **One publisher or observer loop per ISVC.** Mechanically simple, but creates
   unbounded tickers, transports, credentials, and shutdown paths as the fleet
   grows. Rejected in favor of one publisher instance and one bounded shared
   controller workqueue.
9. **Allow the ISVC to select the publisher or its remote endpoint.** Flexible,
   but lets a workload redirect OME's privileged identity and leaves cross-backend
   cleanup ambiguous. Rejected; those values remain installation-owned.

## Open Questions

0. ~~Should a `TrafficMap` exist when nothing is routable?~~ **Resolved: yes,
   always, with a `Routable=False` reason.** Deleting on degradation made the
   object vanish at peak diagnostic value and made a never-created map
   indistinguishable from a broken controller.
1. ~~Proportional vs. binary readiness.~~ **Resolved: use runtime-ready capacity.**
   The numerator is `min(admitted, ready, optional reported capacity)`, so a
   readiness drop reduces traffic immediately while the admitted allocation
   prevents readiness from raising a home's share beyond the placement plan.
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
9. ~~Capacity aggregation across a home's router replicas.~~ **Resolved:
   believe the pessimist, but require corroboration — the quorum-th smallest of
   a bounded window, not the smallest.**
   *Why not `max`:* an existing publisher answers the same question with `max`,
   skipping failed members so one lagging replica cannot drag a home down. That
   is a sound answer to a *different* question, because it is choosing a
   **weight** while this value is a **ceiling**. The two fail in opposite
   directions: too high a weight under-uses a home, whereas too high a ceiling
   sends a home more traffic than it said it could serve. Only the second
   reaches an engine.
   *Why not a strict `min` either:* the control plane polls one address per
   home, so successive polls may reach different reporters behind a balancer.
   A strict minimum lets whichever reporter is broken set the ceiling alone —
   one that has lost its watch reports near zero — and hold the home there for
   the whole window. With a round-robin balancer and a window at least as long
   as the replica count, the bad reading is always present and the home never
   recovers. Enlarging the window, the obvious way to smooth, makes that
   strictly *more* likely.
   *The rule:* the ceiling is the **quorum-th smallest** reading in the window.
   A reporter can still always lower it; it just cannot do so alone. This
   tolerates `quorum-1` misbehaving reporters, and it covers the
   partially-synced reporter returning a small non-zero figure — which
   discarding zeros alone would miss. Below the quorum no ceiling applies and
   the plan stands, since a ceiling drawn from fewer readings than the quorum
   is precisely what the quorum prevents.
   *Cost we accept:* a genuine drop is believed only after `quorum` readings,
   and a unanimous zero is still honoured, so a home that truly can serve
   nothing can still say so.
10. **Probe granularity and fan-out.** One probe per entry means (ISVCs × homes)
    probes per period, each potentially a real inference request. Does the shared
    path warrant a per-home probe amortized across ISVCs? `TBD` — depends on
    ISVC-per-home counts we have not measured.
11. ~~Where model-specific routing configuration lives.~~ **Resolved: inline at
    top-level `spec.routing`, with installation defaults.** A reference-only
    profile makes an ISVC edit depend on a second resource; field-wise merge
    makes the effective policy depend on both. Whole-block replacement keeps the
    selected behavior visible on the object.
12. ~~Whether per-ISVC behavior needs per-ISVC publishers or loops.~~ **Resolved:
    no.** The installation selects one publisher implementation and the leader
    uses one controller-runtime workqueue. Per-ISVC configuration is immutable
    input to a reconcile, not controller topology.

## See also

- OEP-0018 (placement), OEP-0018.3 (placement modes), OEP-0024 (quota),
  OEP-0011 (traffic policy), OEP-0023 (status URLs), OEP-0019 (control-plane split).
- Code this builds on: `pkg/controller/v1beta1/placement/endpoint/` (publisher +
  Gateway API HTTPRoute), `pkg/apis/ome/v1beta1/inference_service_status.go`
  (`PlacementStatus` / `CandidatePlacement`).

**Next action:** land the API/resolver compatibility PR first, then the shared
workqueue observation path, then publisher option isolation and the
TrafficMap-owned claim journal.
Only after those three invariants pass their transition tests should an operator
move an existing service from inherited defaults to inline configuration.
Endpoint-reported capacity remains **blocked** on a pair-aware data-plane surface;
Open Questions #2, #7, and #8 still gate its production guidance. Owner:
@fanyang-real.
