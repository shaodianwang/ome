# OEP-0011: Inference Service Traffic Management

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Today's Gaps](#todays-gaps)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Conceptual Overview](#conceptual-overview)
  - [User Stories](#user-stories)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Part 1: Backend Policy](#part-1-backend-policy)
    - [Typed Core](#typed-core)
    - [Annotation Extension](#annotation-extension)
    - [Pass-Through Namespace](#pass-through-namespace)
    - [Engine HTTPRoute Always Emitted](#engine-httproute-always-emitted)
  - [Part 2: Rollout Strategies](#part-2-rollout-strategies)
    - [Three Strategies via One Field](#three-strategies-via-one-field)
    - [API Contract](#api-contract)
    - [Step-Based Rollouts](#step-based-rollouts)
    - [Per-Step Execution Order](#per-step-execution-order)
    - [Lifecycle Phases](#lifecycle-phases)
    - [Per-Revision Service Convention](#per-revision-service-convention)
    - [Generation-Scoped Discovery (PD Hazard)](#generation-scoped-discovery-pd-hazard)
    - [HTTPRoute Weighted BackendRefs](#httproute-weighted-backendrefs)
    - [PD Coupling](#pd-coupling)
    - [Concurrent Spec Changes](#concurrent-spec-changes)
    - [Failure Handling](#failure-handling)
    - [Out of Scope: Workload Orchestration](#out-of-scope-workload-orchestration)
  - [Part 3: Composition](#part-3-composition)
  - [Status Surface](#status-surface)
  - [Webhook Validation](#webhook-validation)
  - [Translator Architecture](#translator-architecture)
  - [Coexistence with Hand-Authored Policies](#coexistence-with-hand-authored-policies)
  - [Field Coverage Matrix](#field-coverage-matrix)
  - [Observability](#observability)
- [Opt-In and Rollout](#opt-in-and-rollout)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

OME's `InferenceService` (ISVC) is the operator's single declaration of
intent for serving a model. Today it covers the workload pods, the
ingress entry point, and (in `Serverless` mode only) a Knative-style
canary traffic split. Every other deployment mode exposes one ingress
knob (request timeout) and offers no canary path — operators reach for
hand-authored sibling resources outside OME's lifecycle.

This OEP promotes traffic management — both **backend policy**
(LB algorithm, hash key, circuit breaker, retries) and **rollout
strategies** (canary, blue-green, and the existing rolling default
via weighted routing between revisions) — to a first-class concern of
the InferenceService. Operators declare intent on the ISVC itself; OME
emits the right native resources, owned by the ISVC, with status
surfaced through existing `InferenceService.Status` fields.

All three rollout strategies use one existing field
(`ComponentExtensionSpec.CanaryTrafficPercent`) with three semantic
points: unset = rolling upgrade (today's default), `0` = blue-green
standby (new revision running, 0% traffic), `1-99` = canary, `100` =
instant cutover / promotion. No new rollout-strategy enum is needed.

For operators who want a declarative multi-step rollout plan (e.g.
"10% with manual approval → 50% auto-advancing after 10m → 100% with
manual approval"), the OEP adds an optional `spec.traffic.rollout`
block with a step list and a `Manual | Auto` promotion policy. When
unset, the simple single-value `canaryTrafficPercent` continues to
work. The step form decouples pod capacity from traffic weight so
operators can express full-capacity validation (run 100% new at 0%
traffic, then atomic cutover) without juggling intermediate states.

The OEP introduces **no new CRDs** and one new spec field:
`InferenceServiceSpec.Traffic`. The canary half reuses the existing
`ComponentExtensionSpec.CanaryTrafficPercent` field
(`pkg/apis/ome/v1beta1/component.go:35`) and the existing revision-
tracking status fields (`inference_service_status.go:38-51`) —
generalizing their semantic from Knative-only to all deployment modes.

Per OEP-0007 precedent: **no Kubernetes feature gate**. Opt-in is
per-ISVC by setting any traffic field, annotation, or
`canaryTrafficPercent`.

OME emits one `BackendTrafficPolicy` per ISVC (targeting every
OME-managed HTTPRoute) and, when canary is active, extends the
HTTPRoute's `backendRefs` to weight traffic between stable and canary
revisions. The same backend policy applies to both pools.

**Scope.** This OEP is about **traffic** — HTTPRoutes, BackendTrafficPolicies,
weighted backends, status. The orchestration question — how each
deployment-mode reconciler creates per-revision pod sets and Services
— is **out of scope**. That is the per-mode reconciler's responsibility,
addressed in OEP-0007 (OMENative) and follow-ups for RawDeployment,
MultiNode, and PDDisaggregated. This OEP defines the traffic contract
those reconcilers populate.

The alpha translator targets Envoy Gateway (MSL's standard). Istio is
a stub that surfaces `NoTranslatorAvailable`. This OEP is **additive**:
nothing changes for ISVCs that don't opt in.

## Motivation

### Today's Gaps

**Backend policy.** OME exposes `ComponentExtensionSpec.TimeoutSeconds`
and nothing else. Everything beyond request timeout (LB algorithm,
hash-based session affinity, circuit breakers, retries, connection
timeouts, backend TLS, rate limiting) is hand-authored as a sibling
gateway-implementation-specific resource. Real example on
`msl-cw-use2-2` today: `i8s-workloads/avocado-tier5-chat2-kyle-...`
carries a `BackendTrafficPolicy` with `loadBalancer: LeastRequest` +
circuit breaker + timeouts, all maintained separately from the ISVC.
Three problems: drift on ISVC delete, gateway-implementation lock-in,
status blindness.

**Canary.** The Knative-style canary infrastructure already exists in
the API (`CanaryTrafficPercent` + revision status fields), and the
Knative reconciler at `ksvc_reconciler.go:99-136` already implements
it. Every other deployment mode silently ignores these fields. A spec
change always triggers rolling upgrade with no way to weight traffic
between current and candidate. Operators either deploy two ISVCs and
wire traffic split by hand, or adopt an external rollout controller
(Argo Rollouts, Flagger) with its own CRD vocabulary.

Both gaps push operators outside OME's lifecycle. Promoting both into
the ISVC collapses to one declaration with OME's existing admission
and lifecycle paths.

### Goals

1. **One declaration per ISVC** for both policy and canary intent.
2. **Implementation-agnostic.** Envoy Gateway, Istio, kgateway behind
   the same operator-facing surface.
3. **InferenceService-level scope.** One BTP per ISVC targeting every
   HTTPRoute. Canary applies whole-ISVC (PD components coupled).
4. **Lifecycle-bound.** Emitted resources owned by the ISVC.
5. **Discoverable.** Common LB knobs typed; long tail annotation;
   impl-specific pass-through.
6. **Rolling, canary, and blue-green across all modes** via the
   existing `CanaryTrafficPercent` field and revision status fields —
   no new spec field, no new strategy enum.
7. **Optional declarative multi-step rollouts** with manual or
   automatic promotion between steps, decoupling pod capacity from
   traffic weight so operators can validate at full capacity before
   cutover.
8. **Operator-visible lifecycle.** An explicit `rolloutPhase` status
   field tells operators which strategy is active and what state
   it's in (Stable, Canarying, BlueGreenStandby, Paused, Failed).
9. **Composition.** Sticky session AND any rollout strategy at the
   same time, with the same backend policy across stable and
   canary/standby pools.

### Non-Goals

1. **Not modeling every Gateway implementation feature.** Pass-through
   covers what OME doesn't type.
2. **Not per-Component policy differentiation in alpha.** Operators
   needing it hand-author a sibling BTP (coexistence rules apply).
3. **Not a Gateway resource manager.** OME doesn't own `Gateway` /
   `GatewayClass`.
4. **Not a rate limiter or auth gate.**
5. **Not a feature gate.** Per OEP-0007.
6. **Not a top-level CRD.** No `InferenceServiceTrafficPolicy` /
   `InferenceServiceCanary` CRDs.
7. **Not workload orchestration.** Per-revision pod sets and per-mode
   reconciler behavior live in OEP-0007 and follow-ups. This OEP
   defines what HTTPRoutes / Services those reconcilers must produce,
   not how they produce them.
8. **Not multiple concurrent canaries** per ISVC, not auto-promotion,
   not header-based canary routing — all follow-ups.

## Proposal

### Conceptual Overview

```
InferenceService
├── spec.traffic                        (typed: LB algorithm + hash + endpointOverride)
├── spec.engine.canaryTrafficPercent    (existing field; semantic extended to all modes)
└── metadata.annotations[ome.io/*]      (annotation extension + impl-specific pass-through)

                  │
                  ▼
   ┌──────────────────────────────────────────────────┐
   │  Traffic translator                              │
   │  ├── reads InferenceService.Status.Components[]  │
   │  │     - per-revision Service names              │
   │  │     - traffic[] weights (existing field)      │
   │  ├── emits weighted HTTPRoute backendRefs        │
   │  └── emits one BackendTrafficPolicy per ISVC     │
   └──────────────────────────────────────────────────┘
                  │
                  ▼
   Owned by the ISVC. Auto-cleanup on delete.
```

Three layers, one ISVC declaration:

| Surface | Holds | Notes |
|---|---|---|
| `spec.traffic` (typed) | LB algorithm, hash key, endpointOverride | New, ISVC-level |
| `metadata.annotations[ome.io/*]` | Circuit breaker, retries, sub-timeouts, pass-through | ISVC-level |
| `spec.engine.canaryTrafficPercent` (existing) | Canary percent | PD-coupled across components |

The per-mode reconciler is responsible for **populating
`InferenceService.Status.Components[<c>].Traffic[]`** with per-revision
Service names and weights. The traffic translator and HTTPRoute builder
in this OEP **consume** that status to emit the right wire format.

### User Stories

#### Story 1 — Add session affinity (the SGLang case)

```yaml
spec:
  traffic:
    algorithm: ConsistentHash
    consistentHash:
      type: Header
      headers: [X-Session-ID]
```

OME emits one `BackendTrafficPolicy` named `<isvc>` targeting every
OME-managed HTTPRoute (top-level + router + decoder + engine) via
`targetRefs`. Status surfaces `loadBalancerType: ConsistentHash` and
`BackendPolicyReady=True` once the gateway controller acknowledges.

#### Story 2 — Canary a runtime upgrade at 10%

```yaml
spec:
  runtime:
    name: srt-llama-v2                  # changed from srt-llama
  engine:
    canaryTrafficPercent: 10            # existing field
```

The per-mode reconciler (OMENative, RawDeployment, etc.) is responsible
for creating the canary revision's pod set and Service (named
`<isvc>-engine-rev-<hash>`) and updating `status.components.engine.traffic[]`
with `[stable@90, canary@10]`. The HTTPRoute builder reads that status
and emits weighted `backendRefs`. The same `BackendTrafficPolicy`
applies to both pools. Operator promotes by setting percent to 100,
rolls back by setting to 0.

Print columns (already wired at `inference_service.go:324-326`):

```
NAME       LATEST   PREVROLLOUTREVISION                 LATESTREADYREVISION
llama-70b  10       llama-70b-engine-rev-abc123         llama-70b-engine-rev-def456
```

#### Story 3 — Sticky session AND canary together

```yaml
spec:
  runtime: { name: srt-llama-v2 }
  traffic:
    algorithm: ConsistentHash
    consistentHash: { type: Header, headers: [X-Session-ID] }
  engine:
    canaryTrafficPercent: 10
```

HTTPRoute has weighted `backendRefs` (90/10). Single BTP applies
`ConsistentHash` within each pool. A request with `X-Session-ID=abc`
lands in either the stable or canary pool (weighted), then sticks to a
specific pod within that pool. About 10% of distinct sessions go to
the canary; sticky within. This is the composition story the unified
OEP exists to deliver.

#### Story 4 — Blue-green deployment (deploy then instant cutover)

Operator wants to spin up v2 alongside v1, validate v2 in isolation
(internal traffic only), then switch 100% atomically — no gradual
shift.

**Step 1: deploy v2 in standby.** Change the runtime and explicitly
set `canaryTrafficPercent: 0`:

```yaml
spec:
  runtime:
    name: srt-llama-v2                # changed from srt-llama
  engine:
    canaryTrafficPercent: 0           # explicit 0 = standby (not unset)
```

OME's per-mode reconciler creates the v2 revision pod set at a small
scale (the per-mode reconciler decides; suggested minimum 1 pod).
The HTTPRoute's `backendRefs` route 100% to the stable v1 revision
and 0% to v2; both revisions are alive.

```
NAME       LATEST  PREVROLLOUTREVISION              LATESTREADYREVISION
llama-70b  0       llama-70b-engine-rev-v1hash      llama-70b-engine-rev-v2hash
```

```yaml
status:
  components:
    engine:
      rolloutPhase: BlueGreenStandby
      latestReadyRevision: llama-70b-engine-rev-v2hash
      latestRolledoutRevision: llama-70b-engine-rev-v1hash
      traffic:
        - latestRevision: true
          revisionName: llama-70b-engine-rev-v2hash
          percent: 0
          tag: latest
        - latestRevision: false
          revisionName: llama-70b-engine-rev-v1hash
          percent: 100
          tag: prev
```

**Step 2: validate v2 directly.** Operator hits the per-revision
Service or per-revision HTTPRoute subdomain without going through the
weighted routing:

```bash
# Via per-revision Service (in-cluster):
kubectl exec -n playground client-pod -- \
  curl http://llama-70b-engine-rev-v2hash.playground.svc.cluster.local/v1/...

# Or via endpointOverride (Story 7) if spec.traffic.endpointOverride is set
```

**Step 3: cutover to v2 atomically.**

```yaml
spec:
  engine:
    canaryTrafficPercent: 100
```

HTTPRoute `backendRefs` collapses to a single backend targeting v2.
v1 pods drain (per-mode reconciler responsibility).
`latestRolledoutRevision` becomes v2; `previousRolledoutRevision`
becomes v1.

```yaml
status:
  components:
    engine:
      rolloutPhase: Stable
      latestRolledoutRevision: llama-70b-engine-rev-v2hash
      previousRolledoutRevision: llama-70b-engine-rev-v1hash   # retained for quick rollback
      traffic:
        - latestRevision: true
          revisionName: llama-70b-engine-rev-v2hash
          percent: 100
```

**Quick rollback within retention window:** set
`canaryTrafficPercent` to 0 again and edit the spec back to v1, OR
spec-revert to v1 → controller recognizes the previous revision hash
and re-promotes it. Operator-visible state: `rolloutPhase`
transitions back through `BlueGreenStandby` to `Stable`.

#### Story 5 — Multi-step rollout with mixed manual/auto promotion

Operator wants a planned 4-step rollout: validate at 10% with manual
approval, advance to 50% and auto-proceed after 10 minutes, validate
at 80% with manual approval, then cutover. The simple single-value
canary requires editing `canaryTrafficPercent` four times manually.
The step-based rollout makes the plan declarative.

**Write:**

```yaml
spec:
  runtime: { name: srt-llama-v2 }
  traffic:
    rollout:
      promotionPolicy: Manual
      scaleDownDelaySeconds: 30
      steps:
        - newCapacity: "10%"
          oldCapacity: "100%"          # capacity surge for safety
          trafficWeight: 10
          pause: {}                    # wait for operator
        - newCapacity: "50%"
          oldCapacity: "50%"
          trafficWeight: 50
          pause: { duration: 10m }     # auto-advance after 10m
        - newCapacity: "80%"
          oldCapacity: "20%"
          trafficWeight: 80
          pause: {}                    # wait for operator
        - newCapacity: "100%"
          oldCapacity: "0%"
          trafficWeight: 100           # final step; no pause
```

**See** at any point in the rollout:

```yaml
status:
  components:
    engine:
      rolloutPhase: Paused             # waiting for manual promote
      currentStep: 0
      latestReadyRevision: llama-70b-engine-rev-v2hash
      latestRolledoutRevision: llama-70b-engine-rev-v1hash
      traffic:
        - revisionName: llama-70b-engine-rev-v2hash
          percent: 10
          tag: latest
        - revisionName: llama-70b-engine-rev-v1hash
          percent: 90
          tag: prev
```

**Advance to the next step** via annotation:

```bash
kubectl annotate isvc llama-70b ome.io/rollout-promote=1 --overwrite
```

Or skip all the way to completion:

```bash
kubectl annotate isvc llama-70b ome.io/rollout-promote=full --overwrite
```

**Roll back from any step:**

```bash
kubectl annotate isvc llama-70b ome.io/rollout-rollback=true --overwrite
```

Rollback is instant: HTTPRoute backendRefs flip to 100/0 (old/new),
new revision drains after the configured `scaleDownDelaySeconds`.

The single-value `canaryTrafficPercent` form (Stories 2 and 4) and
the step-based form (this story) are **mutually exclusive** —
admission rejects setting both on the same Component.

#### Story 6 — Envoy-specific feature via pass-through

```yaml
metadata:
  annotations:
    ome.io/btp.loadBalancer.slowStart.window: 30s
    ome.io/btp.loadBalancer.slowStart.aggression: "1.5"
```

`ome.io/btp.<path>` is stitched verbatim into the emitted
`BackendTrafficPolicy.spec.<path>`. OME does not validate; the gateway
controller does, and rejections surface as
`BackendPolicyReady=False, Reason=GatewayRejected`. Status condition
message lists every pass-through key applied.

#### Story 7 — Migration from a hand-authored BTP

Operator has a hand-authored `BackendTrafficPolicy` named the same as
what OME would emit. Today they add `spec.traffic` to the ISVC.

OME defers: `BackendPolicyReady=False, Reason=ConflictingPolicy` with a
message naming the conflicting resource. The hand-authored policy
remains in effect. Operator deletes the manual policy → OME takes
ownership next reconcile. (Coexistence phases at GA tighten the rules;
see [Coexistence with Hand-Authored Policies](#coexistence-with-hand-authored-policies).)

### Risks and Mitigations

**Per-Component LB differentiation not in alpha.** Operators wanting
it hand-author a sibling BTP targeting only one Component's HTTPRoute
(coexistence — OME defers per-name). Usage metric drives the
promotion decision for a typed per-Component surface in beta+.

**PD canary inconsistency.** Engine canary at 10%, decoder at 20%
would route engine-canary traffic to decoder-stable with mismatched
KV-cache shape. Webhook enforces equal `canaryTrafficPercent` across
engine and decoder when both are present in PD mode.

**Pass-through namespace abuse.** Operators may stay on pass-through
indefinitely, evading typing. Per-field usage metric makes this
visible; documentation calls pass-through unsupported and subject to
change.

**Hand-authored policies break on upgrade.** Three-phase coexistence:
alpha defers, beta warns, GA rejects without an ack annotation.

**Cross-implementation translation is lossy.** Field coverage matrix
bounds the divergence per translator; unsupported fields surface as
status conditions, not silent drops.

**Workload orchestration is out of scope but required.** Canary cannot
function without per-revision pod sets and Services. This OEP defines
the contract those reconcilers must populate; per-mode reconciler
work is tracked in OEP-0007 (OMENative) and follow-ups. Alpha
graduation requires at least one non-Knative mode (OMENative) to
implement the contract, proving feasibility.

## Design Details

### Part 1: Backend Policy

#### Typed Core

One new Go struct, one new field on `InferenceServiceSpec`:

```go
// pkg/apis/ome/v1beta1/traffic_types.go (new file)

// TrafficSpec configures the load-balancing policy applied to all
// HTTPRoutes OME emits for the InferenceService. For less-common
// settings (circuit breaker, retries, sub-second timeouts), see the
// ome.io/* annotation contract.
type TrafficSpec struct {
    // +kubebuilder:validation:Enum=RoundRobin;LeastRequest;Random;ConsistentHash
    // +optional
    Algorithm *LoadBalancerType `json:"algorithm,omitempty"`

    // ConsistentHash configures the hash key. Required when
    // Algorithm=ConsistentHash; rejected otherwise.
    // +optional
    ConsistentHash *ConsistentHashSpec `json:"consistentHash,omitempty"`

    // EndpointOverride routes requests to a specific pod when a header
    // (e.g. "X-Endpoint-HostPort: 10.0.0.5:30000") is present and
    // healthy; falls back to Algorithm otherwise. Envoy Gateway only.
    // +optional
    EndpointOverride *EndpointOverrideSpec `json:"endpointOverride,omitempty"`

    // Rollout configures a step-based progressive rollout plan.
    // When set, the controller advances through the steps under the
    // configured PromotionPolicy and the simple per-Component
    // canaryTrafficPercent field MUST NOT be set on the same
    // Component (admission rejects). When unset, operators continue
    // to drive rollouts by editing canaryTrafficPercent directly.
    // +optional
    Rollout *RolloutSpec `json:"rollout,omitempty"`
}

type ConsistentHashSpec struct {
    // +kubebuilder:validation:Enum=Header;Cookie;SourceIP
    Type HashType `json:"type"`
    // +optional
    // +listType=atomic
    Headers []HashHeader `json:"headers,omitempty"`
    // +optional
    Cookie *HashCookie `json:"cookie,omitempty"`
}

type HashHeader struct { Name string `json:"name"` }
type HashCookie struct {
    Name string `json:"name"`
    // +optional
    TTLSeconds *int64 `json:"ttlSeconds,omitempty"`
}

type EndpointOverrideSpec struct {
    // +kubebuilder:validation:Enum=Header;Metadata
    Type EndpointOverrideType `json:"type"`
    // +optional
    // +listType=atomic
    Headers []HashHeader `json:"headers,omitempty"`
}

// RolloutSpec defines a multi-step progressive rollout plan.
// Step semantics and operator verbs are documented in
// "Step-Based Rollouts" below.
type RolloutSpec struct {
    // Steps is the ordered list of rollout stages. At least one step
    // required. The final step's TrafficWeight must be 100 (otherwise
    // the rollout never completes).
    // +kubebuilder:validation:MinItems=1
    Steps []RolloutStep `json:"steps"`

    // PromotionPolicy controls how the rollout advances between steps.
    // Manual = operator advances via the ome.io/rollout-promote
    // annotation. Auto = controller advances based on pause.duration.
    // Default: Manual.
    // +kubebuilder:validation:Enum=Manual;Auto
    // +optional
    PromotionPolicy *PromotionPolicy `json:"promotionPolicy,omitempty"`

    // ScaleDownDelaySeconds is the wait between shifting traffic away
    // from the old revision and scaling its pods down, to drain
    // in-flight requests. Default: 30.
    // +optional
    // +kubebuilder:default=30
    // +kubebuilder:validation:Minimum=0
    ScaleDownDelaySeconds *int32 `json:"scaleDownDelaySeconds,omitempty"`
}

type PromotionPolicy string

const (
    PromotionPolicyManual PromotionPolicy = "Manual"
    PromotionPolicyAuto   PromotionPolicy = "Auto"
)

// RolloutStep specifies one stage of a progressive rollout.
// NewCapacity and TrafficWeight are independent: the operator can
// run the new revision at full capacity with 0% traffic (validation),
// or shift traffic ahead of capacity (rare, generally unsafe).
type RolloutStep struct {
    // NewCapacity is the fraction of desired replicas to run for the
    // new revision at this step. Percentage ("25%") or absolute
    // ("3").
    // +kubebuilder:validation:XIntOrString
    NewCapacity intstr.IntOrString `json:"newCapacity"`

    // OldCapacity is the fraction of desired replicas to keep for the
    // old revision at this step. Defaults to (100% - NewCapacity).
    // Setting both > 50% allows a temporary capacity surge — useful
    // for keeping full old capacity while the new revision validates
    // at the same scale.
    // +optional
    // +kubebuilder:validation:XIntOrString
    OldCapacity *intstr.IntOrString `json:"oldCapacity,omitempty"`

    // TrafficWeight is the percentage of traffic routed to the new
    // revision at this step. 0-100. The old revision receives
    // (100 - TrafficWeight).
    // +kubebuilder:validation:Minimum=0
    // +kubebuilder:validation:Maximum=100
    TrafficWeight int32 `json:"trafficWeight"`

    // Pause holds the rollout at this step. If absent, the controller
    // advances to the next step immediately after the step's
    // capacity, traffic, and drain operations complete.
    // +optional
    Pause *RolloutPause `json:"pause,omitempty"`
}

type RolloutPause struct {
    // Duration to pause before auto-advancing. If unset and
    // PromotionPolicy is Manual, the controller waits indefinitely
    // for the ome.io/rollout-promote annotation. If PromotionPolicy
    // is Auto and Duration is unset, the controller advances
    // immediately (i.e., pause is a no-op).
    // +optional
    Duration *metav1.Duration `json:"duration,omitempty"`
}
```

Added to `InferenceServiceSpec`:

```go
type InferenceServiceSpec struct {
    // ... existing fields ...

    // +optional
    Traffic *TrafficSpec `json:"traffic,omitempty"`
}
```

Operators get `kubectl explain isvc.spec.traffic` and IDE completion
for free.

#### Annotation Extension

Long-tail backend-policy knobs on the ISVC metadata. Webhook validates
and coerces per key.

| Key | Type | Maps to |
|---|---|---|
| `ome.io/circuit-breaker-max-connections` | int | `circuitBreaker.maxConnections` |
| `ome.io/circuit-breaker-max-parallel-requests` | int | `circuitBreaker.maxParallelRequests` |
| `ome.io/circuit-breaker-max-pending-requests` | int | `circuitBreaker.maxPendingRequests` |
| `ome.io/circuit-breaker-max-parallel-retries` | int | `circuitBreaker.maxParallelRetries` |
| `ome.io/circuit-breaker-per-endpoint-max-connections` | int | `circuitBreaker.perEndpoint.maxConnections` |
| `ome.io/retry-attempts` | int (default 0) | `retry.numRetries` |
| `ome.io/retry-on` | comma-separated (`5xx`, `reset`, `gateway-error`, `connect-failure`, `retriable-status-codes`) | `retry.retryOn` |
| `ome.io/retry-per-try-timeout` | duration | `retry.perTryTimeout` |
| `ome.io/timeout-idle` | duration | `timeout.http.connectionIdleTimeout` |
| `ome.io/timeout-max-connection-duration` | duration | `timeout.http.maxConnectionDuration` |
| `ome.io/timeout-tcp-connect` | duration | `timeout.tcp.connectTimeout` |
| `ome.io/managed-by-conflict-acked` | `"true"` | Acknowledges a same-name hand-authored policy (suppresses GA rejection). |
| `ome.io/rollout-ready-timeout` | duration (default `15m`) | How long the per-mode reconciler waits for new-revision pods to reach Ready before marking the rollout `Failed`. Applies to canary and blue-green standby alike. |
| `ome.io/revision-history-limit` | int (default `3`) | Non-active revisions per Component retained before garbage collection (in addition to `LatestRolledoutRevision` and `PreviousRolledoutRevision`). |
| `ome.io/rollout-promote` | int step index, or `"full"` | Operator verb for step-based rollouts (when `spec.traffic.rollout` is set). Advances the rollout to the named step, or to completion if `"full"`. Controller clears the annotation after consuming it. No-op when `spec.traffic.rollout` is unset. |
| `ome.io/rollout-rollback` | `"true"` | Operator verb for step-based rollouts. Initiates instant rollback: traffic shifts to 100% old, new revision drains after `scaleDownDelaySeconds`. Works alongside the operator-set `canaryTrafficPercent` form too (equivalent to setting percent to 0 and reverting the spec change). Controller clears the annotation after consuming it. |

Request-level timeout stays on the existing per-Component
`ComponentExtensionSpec.TimeoutSeconds` — that's HTTPRoute-level, not
BTP-level.

Conventions:
- Annotations are strings; webhook coerces (`strconv.Atoi`,
  `time.ParseDuration`).
- Unknown `ome.io/*` keys within Levenshtein 2 of a documented key are
  rejected at admission with a Did-you-mean.

#### Pass-Through Namespace

Escape hatch for impl-specific fields OME does not model:

- `ome.io/btp.<dotted-path>` → Envoy Gateway `BackendTrafficPolicy.spec.<path>`
- `ome.io/dr.<dotted-path>` → Istio `DestinationRule.spec.<path>`

Values stitched verbatim into the emitted resource. No OME-side
validation. Per-pass-through-field metric counts usage for promotion
review. Pass-through namespace is per-translator (using `btp.*` with
Istio active surfaces as `UnsupportedField`).

#### Engine HTTPRoute Always Emitted

Today the engine HTTPRoute is omitted when a router is present
(`buildTopLevelHTTPRoute` makes top-level fall back to router). This
OEP changes that: OME always emits `<isvc>-engine`. Two reasons:

1. **Canary needs it.** Per-revision Services need per-Component
   HTTPRoutes to attach to.
2. **Direct engine access** is operationally useful (debugging,
   per-Component policy via hand-authored sibling, mesh integration).

Backward-compatible: emitting a new HTTPRoute does not change existing
traffic behavior because no client was using the engine-subdomain
hostname (it didn't exist).

### Part 2: Rollout Strategies

This OEP defines the **traffic-side** contract for rollouts. Workload
orchestration (how per-revision pod sets are created and torn down)
is the per-mode reconciler's responsibility, **out of scope for this
OEP** — covered in [Out of Scope: Workload Orchestration](#out-of-scope-workload-orchestration)
below.

#### Three Strategies via One Field

All three rollout strategies use the existing
`ComponentExtensionSpec.CanaryTrafficPercent` field
(`pkg/apis/ome/v1beta1/component.go:35`). The value alone selects the
strategy:

| Value | Strategy | Behavior |
|---|---|---|
| **unset** (`nil`) | **Rolling upgrade** (today's default) | Spec change replaces pods in place. No per-revision pod sets, no weighted routing, no HTTPRoute changes. Identical to today's behavior. |
| **`0`** (explicit) | **Blue-green standby** | Per-mode reconciler materializes the new revision pod set (at minimum scale; suggested 1 pod or `MinReplicas × 1%`, reconciler decides). HTTPRoute `backendRefs` route 100% to stable, 0% to canary. Both revisions alive; operator validates the new revision via per-revision Service. |
| **`1`-`99`** | **Canary** | Per-mode reconciler materializes the canary revision at `MinReplicas × percent/100` (rounded up to min 1). HTTPRoute `backendRefs` weighted between stable and canary. |
| **`100`** | **Promotion / instant cutover** | Canary scaled to full `MinReplicas`; stable drained and deleted. From a `0` (blue-green standby) state, this is the atomic cutover. From `1-99` (canary), this is gradual promotion. |

Operators control the strategy by editing the percent value. Common
operational sequences:

- **Pure rolling** (today, default): leave `canaryTrafficPercent` unset across the spec's lifetime.
- **Canary** (progressive): `unset` → `10` → `25` → `50` → `100` over time.
- **Blue-green** (atomic cutover): `unset` → `0` (deploy + validate) → `100` (cutover).
- **Roll back from canary**: `10` → `0` (stop sending traffic, keep canary pods for diagnosis) → `unset` (decommission).
- **Roll back from blue-green**: `0` → `unset` (decommission canary, revert spec separately if needed).

The mode the operator gets is purely a function of what value they
write into the field. No new strategy enum is needed.

#### API Contract

Spec, reused from existing API:

- `ComponentExtensionSpec.CanaryTrafficPercent *int64`
  (`component.go:33-35`). Semantic extended from Knative-only to all
  deployment modes, with `0` and `100` as meaningful endpoints.

Status, reused from existing API
(`inference_service_status.go:38-51`):

- `LatestReadyRevision` — latest revision that has come up Ready
- `LatestCreatedRevision` — latest revision the controller has
  materialized (may not be Ready yet)
- `PreviousRolledoutRevision` — the previous stable revision (retained for quick rollback)
- `LatestRolledoutRevision` — the current stable revision (100% traffic when no rollout in flight)
- `Traffic []knservingv1.TrafficTarget` — the revision/weight pairs

Status content per strategy:

| Strategy | `Traffic[]` content |
|---|---|
| Rolling (unset) | One entry: `LatestRolledoutRevision @ 100%, latestRevision: true`. |
| Blue-green standby (0) | Two entries: latest @ 0%, prev @ 100%. `latestRevision: true` flag on the new one. |
| Canary (1-99) | Two entries: latest @ N%, prev @ (100-N)%. |
| Promoted (100) | One entry: new revision @ 100%; `LatestRolledoutRevision` updated to the new hash; `PreviousRolledoutRevision` retains the old one for the retention window. |

The Knative reconciler at `ksvc_reconciler.go:99-136` already
implements exactly this contract. Other per-mode reconcilers
replicate it.

#### Step-Based Rollouts

The simple form (`canaryTrafficPercent: N`) ties pod capacity to
traffic weight (canary pods scale to `MinReplicas × N/100`, traffic
weighted to N%) and requires operators to edit the value at each
stage. For more sophisticated rollouts — multi-step plans,
auto-advancing pauses, and capacity vs. traffic decoupling — operators
set `spec.traffic.rollout` instead.

**Capacity vs traffic decoupling.** The step form has independent
`newCapacity` and `trafficWeight` fields. This enables patterns the
simple form cannot express:

- **Full-capacity validation then atomic cutover** (true blue-green):
  step 1 with `newCapacity: 100%, oldCapacity: 100%, trafficWeight: 0`
  (full new revision running, zero traffic, validate by hitting
  per-revision Service directly); step 2 with `trafficWeight: 100`
  (atomic switch).
- **Capacity surge for safety**: `newCapacity: 25%, oldCapacity: 100%`
  keeps full old capacity while ramping new. Total cluster GPU usage
  briefly exceeds steady state.
- **Conservative capacity reduction**: `newCapacity: 50%, oldCapacity: 50%`
  matches the simple form's coupled behavior.

**Promotion policy.** `spec.traffic.rollout.promotionPolicy` is
`Manual` (default) or `Auto`:

- **`Manual`**: controller pauses at each step's `pause` block and
  waits for the operator to advance via the `ome.io/rollout-promote`
  annotation. The annotation value is either an integer step index
  (`"2"` advances to step 2) or `"full"` (advance all the way to the
  final step). The controller clears the annotation after consuming
  it.
- **`Auto`**: controller advances based on `pause.duration` at each
  step. If `pause` is absent or `duration` is unset, the controller
  advances immediately.

A step's `pause: {}` (empty pause block) means "pause indefinitely
under Manual; no-op under Auto". `pause: { duration: 10m }` means
"pause for 10 minutes regardless of policy, then under Auto advance
automatically, under Manual still wait for annotation past the
10-minute mark".

**Rollback.** `ome.io/rollout-rollback: "true"` initiates instant
rollback at any step: traffic shifts to 100% old, new revision drains
after `scaleDownDelaySeconds`, controller clears the annotation. The
rollback path is the same for step-based and simple-form rollouts.

**Mutual exclusivity.** A Component cannot have both
`spec.traffic.rollout` (ISVC-level) and a per-Component
`canaryTrafficPercent` set simultaneously. Admission rejects with
`MixedRolloutForms`. Operators pick one form per ISVC.

**Final step.** The last entry in `steps[]` must have
`trafficWeight: 100`. Without it the rollout never completes.
Admission enforces.

#### Per-Step Execution Order

When the controller advances into step N, the per-mode reconciler
follows a strict execution order. The order is the same for the
simple `canaryTrafficPercent` form and for step-based rollouts; the
step form just iterates this sequence for each step.

1. **Scale up new revision** to `newCapacity` (rounded up to min 1
   for non-zero capacity; min 1 always for `BlueGreenStandby` so the
   revision is addressable).
2. **Wait for new-revision pods to reach Ready.** Bounded by
   `ome.io/rollout-ready-timeout` (default `15m`); on timeout, enter
   `Failed` (see [Failure Handling](#failure-handling)).
3. **Update HTTPRoute `backendRefs` weights** to `trafficWeight` for
   the new revision and `(100 - trafficWeight)` for the old. The BTP
   translator does not re-emit; only the HTTPRoute changes.
4. **Wait `scaleDownDelaySeconds`** (default `30s`) for in-flight
   requests to drain on the old revision. The new revision is now
   receiving its share of traffic; the old revision is still serving
   the rest but no new requests should land on the connections being
   drained.
5. **Scale down old revision** to `oldCapacity`. For the final step
   (`oldCapacity: 0%`), this completes the promotion — old pods
   delete, `LatestRolledoutRevision` becomes the new revision,
   `PreviousRolledoutRevision` retains the old one for the retention
   window.
6. **Check pause condition.** If the step has a `pause` block and the
   policy says wait, transition `rolloutPhase` to `Paused` and
   requeue without advancing.

The order is **scale up before traffic shift before scale down**.
Total serving capacity never drops below what the prior step
sustained.

#### Lifecycle Phases

A new top-level field on `ComponentStatusSpec` makes the operator-
visible rollout state explicit:

```go
type ComponentStatusSpec struct {
    // ... existing fields ...

    // RolloutPhase reflects the current rollout state for this
    // Component. Operators read this to understand which strategy is
    // active and whether it has succeeded.
    // +optional
    RolloutPhase string `json:"rolloutPhase,omitempty"`
}
```

Phases:

| Phase | When |
|---|---|
| **`Stable`** | One revision live, 100% traffic. No rollout in flight. |
| **`Canarying`** | Two revisions live, canary at 1-99%, canary pods Ready. |
| **`BlueGreenStandby`** | Two revisions live, canary at 0%, canary pods Ready. |
| **`Pending`** | New revision being materialized; canary pods not yet Ready. Transient. |
| **`Paused`** | Step-based rollout paused at a step boundary. Waiting for `ome.io/rollout-promote` annotation (Manual policy) or for `pause.duration` to elapse (Auto policy). |
| **`Promoting`** | Operator set percent to 100 (or advanced past the final step's pre-cutover state); canary scaling to full, stable draining. Transient. |
| **`RollingBack`** | Operator set percent down (e.g. 50 → 0) or set `ome.io/rollout-rollback: true`; canary scaling down, stable scaling back. Transient. |
| **`Failed`** | Canary pods failed to reach Ready within the timeout; rollout aborted with the canary preserved for diagnosis (operator decides: clear the spec change to recover, or set percent to 0 to keep diagnosing). |

The reconciler computes the phase from observed state (revision counts,
Ready replicas, target replicas, observed Traffic[] content). Operator
events fire on phase transitions:
`PhaseTransition` (Normal) for ordinary moves, `RolloutFailed`
(Warning) when entering `Failed`.

#### Per-Revision Service Convention

Per-mode reconcilers, when materializing a canary or blue-green
standby, create per-revision Services named:

```
<isvc>-<component>-rev-<revision-hash>
```

Each per-revision Service selects only that revision's pods (via a
`ome.io/revision: <hash>` label). The revision hash computation and
pod orchestration are the per-mode reconciler's concern.

The existing per-Component "logical" Service (`<isvc>-<component>`)
remains as the externally-addressable name. Per-revision Services are
HTTPRoute backend targets, AND are addressable in-cluster for
blue-green standby validation (Story 4).

#### Generation-Scoped Discovery (PD Hazard)

In PD-disaggregated workloads, components talk to each other
internally — the router sends requests to engine pods and decoder
pods; engine and decoder exchange KV cache. During a rollout, the
new revision of any component must talk to the new revision of every
other component, **not** to the old. Mixed-version internal calls
risk KV-cache shape incompatibility, protocol mismatches, and
silent data corruption.

Concretely: today the router's pod template typically embeds engine /
decoder Service DNS names (e.g. `<isvc>-engine.<ns>.svc.cluster.local`)
in its config. If the router is rolled out to v2 but the engine
Service still selects v1 pods, the v2 router calls v1 engine. The
externally-visible weighted HTTPRoute is irrelevant to this
internal traffic.

**The per-mode reconciler is responsible for ensuring internal
discovery is generation-scoped.** This means:

- Every internal endpoint a component uses must resolve to peers in
  the same revision.
- The per-revision Services (`<isvc>-<component>-rev-<hash>`) provide
  this naturally: a v2-router can target
  `<isvc>-engine-rev-<v2-hash>.<ns>.svc.cluster.local` instead of
  the logical Service name.
- The discovery mechanism (ConfigMap mount, env var injection, side-
  car) must surface the per-revision Service name to the pod, not
  the logical name, when a rollout is in flight.

**Implementations** (per-mode reconciler choice):

- **Per-revision discovery ConfigMap.** The reconciler emits a
  per-revision ConfigMap (`<isvc>-discovery-rev-<hash>`) containing
  the revision-scoped peer addresses. Pods mount the ConfigMap
  matching their own revision.
- **Per-revision env var injection.** The reconciler injects
  `OME_ENGINE_ENDPOINT=http://<isvc>-engine-rev-<hash>...` into each
  pod's environment, with `<hash>` matching the pod's own revision.
  Cheapest for simple cases; doesn't survive sidecar discovery.
- **Label-based selector at the runtime.** The runtime sidecar reads
  EndpointSlices filtered by `ome.io/revision: <self-hash>`.
  Requires runtime cooperation.

OME does not mandate one approach — that's per-mode reconciler
design. But the contract is: **internal discovery MUST be
generation-scoped during a rollout**. Per-mode reconciler
implementations that don't honor this contract cause silent
cross-version traffic in PD workloads, which is worse than not
supporting rollouts at all.

This OEP flags the hazard explicitly; OEP-0007 (OMENative) and any
follow-up PD-mode reconciler OEPs must address it.

#### HTTPRoute Weighted BackendRefs

The HTTPRoute builder
(`pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/httproute_builder.go`)
reads `Status.Components[<c>].Traffic[]` and emits weighted backendRefs:

```yaml
# Canary at 10% (or any percent 1-99)
rules:
  - matches: [{ path: { type: PathPrefix, value: / } }]
    backendRefs:
      - name: <isvc>-<component>-rev-<stable-hash>
        weight: 90
      - name: <isvc>-<component>-rev-<canary-hash>
        weight: 10
```

```yaml
# Blue-green standby (0%)
rules:
  - matches: [{ path: { type: PathPrefix, value: / } }]
    backendRefs:
      - name: <isvc>-<component>-rev-<stable-hash>
        weight: 100
      - name: <isvc>-<component>-rev-<canary-hash>
        weight: 0
```

A `weight: 0` backendRef is valid Gateway API — the gateway controller
keeps the endpoint resolution active but routes no traffic. Useful
for blue-green standby (and as the explicit way to express "this
revision exists but receives no traffic").

When `Traffic[]` has one entry (Stable phase), backendRefs collapse to
a single backend. Each per-Component HTTPRoute (top-level, router,
engine, decoder) gets the same treatment.

The HTTPRoute name and matching rules are unchanged across phase
transitions. Only `backendRefs` change.

#### PD Coupling

For ISVCs with both `engine` and `decoder` present (PDDisaggregated
mode), engine and decoder must roll out together. Admission webhook
enforces: if both `engine.canaryTrafficPercent` and
`decoder.canaryTrafficPercent` are set, they must be equal. If only
one is set, the defaulter sets the other to match.

Reconciler treats `engine.canaryTrafficPercent` as authoritative;
decoder mirrors. Router is independent (router revisions can roll out
separately).

For Serverless mode each Component is its own Knative Service —
coupling does not apply; existing per-Component behavior preserved.

#### Concurrent Spec Changes

What happens when the operator edits the spec while a canary or
blue-green standby is in flight (`rolloutPhase` is `Canarying`,
`BlueGreenStandby`, or `Pending`)?

**Behavior:** the in-flight canary is **replaced**, not stacked. The
controller:

1. Computes the new revision hash from the latest spec.
2. If the new hash equals the in-flight canary's hash (e.g. operator
   reverted just the `canaryTrafficPercent` without changing pod-
   shaping fields): no-op for the canary; just adjust the weight.
3. If the new hash differs: emit a Warning event
   `RolloutSupersededByNewSpec`, drain the in-flight canary pods,
   start a new canary at the same `canaryTrafficPercent` (whatever
   it currently is) targeting the new revision. The previous canary
   becomes an orphan revision and is garbage-collected per the
   per-mode reconciler's retention policy.

The stable revision is unaffected by spec changes during a canary —
it continues serving its existing traffic share throughout.

This is intentionally simpler than queueing or stacking. Operators
who want serialized rollouts pause between spec edits (no
admission-time enforcement; manual discipline).

#### Failure Handling

A rollout enters the `Failed` phase when the canary or blue-green
standby pods fail to reach Ready within the configured timeout.

**Timeout:** read from the annotation `ome.io/rollout-ready-timeout`
(duration, default `15m`). The per-mode reconciler tracks the
elapsed time since the canary was first created.

**On timeout:**
- `rolloutPhase` set to `Failed`
- `RolloutFailed` Warning event emitted with the timeout duration and
  observed pod conditions
- Canary pods are **retained** (not auto-deleted) for diagnosis
- HTTPRoute backendRefs remain at the operator-set weight (the
  canary may be `weight: 10` but unhealthy — Envoy Gateway's
  endpoint health checks will exclude unready endpoints from the LB
  pool automatically, so traffic does not reach the bad pods)

**Operator recovery options:**
1. Investigate the canary pods (logs, events, describe).
2. Roll back: set `canaryTrafficPercent: 0` or revert the spec change
   that introduced the bad revision. Canary pods drain on next
   reconcile.
3. Force retry: set `canaryTrafficPercent: 0` then back to the
   intended percent — the reconciler treats this as a fresh canary
   attempt with a new timeout window.

Auto-rollback on failure is intentionally **not in alpha**. Operators
must explicitly decide.

#### Out of Scope: Workload Orchestration

Per-mode reconcilers (RawDeployment, MultiNode, PDDisaggregated,
OMENative) are responsible for:

- Detecting spec changes that require a new revision
- Computing the revision hash (canonical input set, exclusions)
- Materializing per-revision pod sets at the requested capacity:
  - Simple form: `MinReplicas × canaryTrafficPercent/100`, rounded up to min 1 (or min 1 for explicit `0` = blue-green standby)
  - Step-based form: `MinReplicas × newCapacity` and `MinReplicas × oldCapacity` per the current step (both pod sets exist simultaneously; the step may have total > `MinReplicas` for capacity surge)
- Creating per-revision Services with the naming convention above
- Ensuring **generation-scoped internal discovery** (see [Generation-Scoped Discovery (PD Hazard)](#generation-scoped-discovery-pd-hazard))
- Updating `Status.Components[*].Traffic[]` with the revision pair and weights
- Detecting Ready timeout and surfacing `Failed` phase
- Honoring the per-step execution order (scale up → wait ready → traffic → drain → scale down)
- Honoring `ScaleDownDelaySeconds` between traffic shift and old scale-down
- Handling `ome.io/rollout-promote` and `ome.io/rollout-rollback` annotations for step-based rollouts (clearing them after consumption)
- Promotion (percent → 100, or final step): scaling canary to full, draining stable
- Rollback (percent → 0, or rollback annotation): scaling canary down, restoring stable
- Concurrent spec change handling (drain in-flight canary, start new)
- Garbage collection of orphan revisions per the retention policy

OEP-0007 (OMENative) is the reference implementation. RawDeployment,
MultiNode, and PDDisaggregated follow in subsequent reconciler PRs.
Alpha graduation requires OMENative to have implemented the contract
across all three strategy values (rolling/blue-green/canary) AND the
step-based form; Beta extends to the other modes.

### Part 3: Composition

The two parts compose without either knowing about the other.

When backend policy is set AND canary is active:

- HTTPRoute has weighted `backendRefs` listing per-revision Services
- BackendTrafficPolicy `targetRefs` lists every OME-emitted HTTPRoute
  for the ISVC
- Envoy Gateway applies the LB algorithm (e.g. `ConsistentHash`)
  **within each weighted backend pool**

Result: weight-split across pools, sticky within each pool. A session
with `X-Session-ID=abc` lands in either pool (weighted), then sticks
to a specific pod within that pool.

Per-revision policy override (different LB algorithm on canary vs
stable) is not in alpha — operator hand-authors a sibling BTP
targeting only one revision's Service if needed.

### Status Surface

One new top-level field on `InferenceServiceStatus`:

```go
type InferenceServiceStatus struct {
    // ... existing fields ...

    // Traffic reflects the resolved backend policy and the emitted
    // BackendTrafficPolicy. Populated only when traffic intent is
    // declared.
    // +optional
    Traffic *TrafficStatus `json:"traffic,omitempty"`
}

type TrafficStatus struct {
    // LoadBalancerType reflects the resolved algorithm.
    // One of RoundRobin | LeastRequest | Random | ConsistentHash | Default.
    // +optional
    LoadBalancerType string `json:"loadBalancerType,omitempty"`

    // BackendPolicyResource references the OME-emitted policy.
    // +optional
    BackendPolicyResource *BackendPolicyRef `json:"backendPolicyResource,omitempty"`

    // TargetedHTTPRoutes lists the HTTPRoute names the policy targets.
    // +optional
    // +listType=atomic
    TargetedHTTPRoutes []string `json:"targetedHTTPRoutes,omitempty"`

    // Conditions includes BackendPolicyReady.
    // +optional
    // +listType=map
    // +listMapKey=type
    Conditions []metav1.Condition `json:"conditions,omitempty"`
}

type BackendPolicyRef struct {
    APIVersion string `json:"apiVersion"`
    Kind       string `json:"kind"`
    Name       string `json:"name"`
}
```

Per-Component `Traffic[]` fields are reused as-is (no schema change).
Per-mode reconcilers populate them following the same pattern Knative
already uses.

One small additive schema change on `ComponentStatusSpec`:
`RolloutPhase string` (documented in [Lifecycle Phases](#lifecycle-phases)
above). Backward-compatible: old clients ignore it; Components with no
rollout in flight leave it empty.

`BackendPolicyReady` reasons:

| Status | Reason | When |
|---|---|---|
| `True`    | `AcceptedByGateway`     | Policy emitted, gateway acknowledged. |
| `False`   | `ConflictingPolicy`     | Same-name hand-authored policy exists. |
| `False`   | `UnsupportedField`      | Translator cannot honor an annotation. |
| `False`   | `NoTranslatorAvailable` | No Gateway implementation matches. |
| `False`   | `GatewayRejected`       | Gateway reported `Accepted=False`. |
| `Unknown` | `Pending`               | Awaiting gateway acknowledgment. |

### Webhook Validation

CRD enum tags handle most validation. Webhook adds cross-field and
annotation rules:

| Rule | Reason |
|---|---|
| `traffic.algorithm: ConsistentHash` requires `consistentHash` block | `MissingConsistentHashSpec` |
| `traffic.algorithm != ConsistentHash` forbids `consistentHash` block | `UnexpectedConsistentHashSpec` |
| `consistentHash.type: Header` requires non-empty `headers` | `MissingHashKey` |
| `consistentHash.type: Cookie` requires `cookie.name` | `MissingHashKey` |
| Multiple hash sources set | `MultipleHashKeys` |
| `endpointOverride.type: Header` requires non-empty `headers` | `MissingEndpointOverrideKey` |
| `ome.io/*` annotation int that fails `strconv.Atoi` | `InvalidIntValue` |
| `ome.io/*` annotation duration that fails `time.ParseDuration` | `InvalidDuration` |
| `ome.io/retry-attempts > 0` without `ome.io/retry-on` | `MissingRetryOn` |
| `ome.io/circuit-breaker-per-endpoint-max-connections > 1` with `ConsistentHash` | Admission **warning** only |
| Unknown `ome.io/*` key within Levenshtein 2 of a documented key | `UnknownTrafficAnnotation` (Did-you-mean) |
| `ome.io/btp.*` set when active translator is not Envoy Gateway | `UnsupportedPassthrough` |
| `ome.io/*` traffic annotation on Component metadata | `TrafficAnnotationOnComponent` (hint: move to ISVC) |
| `engine.canaryTrafficPercent != decoder.canaryTrafficPercent` in PD | `MismatchedCanaryPercent` |
| `canaryTrafficPercent` < 0 or > 100 | `InvalidCanaryPercent` |
| `canaryTrafficPercent` set with no matching Component | `OrphanCanary` |
| `spec.traffic.rollout` set AND any Component's `canaryTrafficPercent` set | `MixedRolloutForms` (pick one form per ISVC) |
| `spec.traffic.rollout.steps` empty | `EmptyRolloutSteps` |
| `spec.traffic.rollout.steps[].trafficWeight` < 0 or > 100 | `InvalidStepTrafficWeight` |
| `spec.traffic.rollout.steps[].newCapacity` parses to < 0 or > 100% (when percent) | `InvalidStepCapacity` |
| Last `spec.traffic.rollout.steps[].trafficWeight != 100` | `IncompleteRolloutPlan` (rollout never reaches 100%) |
| `spec.traffic.rollout.steps[].pause.duration` not a valid duration | `InvalidDuration` |
| `spec.traffic.rollout.scaleDownDelaySeconds` < 0 | `InvalidScaleDownDelay` |
| `ome.io/rollout-promote` value not an int in range or `"full"` | `InvalidRolloutPromoteTarget` |
| `ome.io/rollout-promote` set when `spec.traffic.rollout` is unset | Admission **warning** only (annotation is a no-op) |
| `ome.io/rollout-ready-timeout` not a valid duration | `InvalidDuration` |
| `ome.io/rollout-ready-timeout` < `1m` | `RolloutTimeoutTooShort` (admission warning) |

### Translator Architecture

Sketch:

```
pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/
├── reconciler.go         # resolve → translate → apply → status
├── resolve.go            # merge typed + annotations into ResolvedIntent
├── annotations.go        # per-key parsers, allowlist, Did-you-mean
├── interfaces/translator.go
├── factory/factory.go    # picks translator by detected CRDs at startup
└── translators/
    ├── envoy_gateway.go  # → BackendTrafficPolicy
    ├── istio.go          # → DestinationRule (stub in alpha)
    └── noop.go           # surfaces NoTranslatorAvailable
```

```go
type Translator interface {
    Name() string
    SupportedAnnotations() sets.Set[string]
    Translate(
        isvc *v1beta1.InferenceService,
        targetHTTPRoutes []string,
        intent ResolvedIntent,
    ) (client.Object, []string, error)  // resource, passthroughKeysApplied, error
    Watches() client.Object
}
```

The translator does not know about canary. The HTTPRoute builder
handles weighted backendRefs from status. The translator just sees
the HTTPRoute names and emits one policy targeting all of them.

Detection at controller startup mirrors `cmd/manager/main.go`'s
existing optional-CRD pattern. Selection: priority order, first match
wins (Envoy Gateway > Istio > Noop).

Reconcile order:
1. Per-mode reconciler creates per-revision pod sets and Services,
   updates `Status.Components[*].Traffic`.
2. HTTPRoute builder reads that status and emits weighted backendRefs.
3. Traffic translator emits one BTP targeting all HTTPRoutes once
   they are `Accepted` by the gateway controller.
4. Status updater computes `Status.Traffic` from the BTP's
   acknowledgment.

### Coexistence with Hand-Authored Policies

**Same-name conflict.** A hand-authored BTP with the name OME would
use (`<isvc>`):

| Phase | OME behavior |
|---|---|
| Alpha | Defer. `BackendPolicyReady=False, Reason=ConflictingPolicy`. Warning event. Do not overwrite. |
| Beta | Same plus admission warning suggesting migration. |
| GA | Reject ISVC update at admission unless `ome.io/managed-by-conflict-acked: true` is set. |

**Different-name overlap.** A hand-authored BTP with a different name
targets some of OME's HTTPRoutes. Not a conflict; OME emits an
informational event (`MultipleTrafficPoliciesAttached`). Gateway
implementation merges the policies per its own attachment rules. This
is the supported fallback for operators wanting per-Component or
per-revision policy differentiation in alpha.

### Field Coverage Matrix

| Intent | Envoy Gateway | Istio (planned) | Notes |
|---|---|---|---|
| `algorithm` (4 values) | ✅ | ✅ | |
| `consistentHash.type: Header` | ✅ multi-header | ⚠️ single | Istio: multi-header → `UnsupportedField` |
| `consistentHash.type: Cookie` | ✅ | ✅ | |
| `consistentHash.type: SourceIP` | ✅ | ✅ | |
| `endpointOverride.type: Header` | ✅ | ❌ | Istio: `UnsupportedField` |
| `circuit-breaker-max-connections` | ✅ | ✅ via `connectionPool.tcp.maxConnections` | |
| `circuit-breaker-max-parallel-requests` | ✅ | ✅ via `connectionPool.http.http2MaxRequests` | Approximate |
| `circuit-breaker-per-endpoint-max-connections` | ✅ | ❌ | Istio: `UnsupportedField` |
| `retry-*` | ✅ | ✅ via DR `retries` | |
| `timeout-idle` | ✅ | ✅ | |
| `timeout-tcp-connect` | ✅ | ✅ | |
| `timeout-max-connection-duration` | ✅ | ❌ | Istio: `UnsupportedField` |
| Pass-through `ome.io/btp.*` | ✅ stitched | n/a | Envoy Gateway only |
| Pass-through `ome.io/dr.*` | n/a | ✅ stitched | Istio only |
| Canary weighted `backendRefs` | ✅ Gateway API standard | ✅ Gateway API standard | Both implementations honor |

### Observability

**Metrics:**

| Metric | Labels | Purpose |
|---|---|---|
| `ome_traffic_policy_translated_total` | `isvc,translator,outcome` | Translation outcome per reconcile. |
| `ome_traffic_policy_translation_failures_total` | `isvc,reason` | Error detail for alerting. |
| `ome_traffic_annotation_used_total` | `key` | Promotion criterion (annotation → typed). |
| `ome_traffic_passthrough_field_total` | `translator,field` | Promotion criterion driver. |
| `ome_traffic_unsupported_field_total` | `isvc,translator,field` | Per-field unsupported counter. |
| `ome_traffic_per_component_fallback_total` | `isvc` | Hand-authored per-Component sibling — per-Component promotion criterion. |
| `ome_backend_policy_accepted_total` | `translator,namespace` | Gateway acknowledgments. |
| `ome_backend_policy_rejected_total` | `translator,namespace,reason` | Gateway rejections. |
| `ome_rollout_phase_total` | `mode,isvc,phase` | Time-series of `rolloutPhase` per ISVC. Phase ∈ {`Stable`, `Canarying`, `BlueGreenStandby`, `Paused`, `Pending`, `Promoting`, `RollingBack`, `Failed`}. Alert on `Failed` and on `Paused` stuck past a threshold. |
| `ome_rollout_step_total` | `mode,isvc,step,outcome` | Step-based rollouts: per-step entry/completion. Outcome ∈ {`entered`, `completed`, `superseded`}. |
| `ome_rollout_promote_annotation_total` | `isvc,target` | Counter for `ome.io/rollout-promote` consumption. Target = step index or `"full"`. |
| `ome_rollout_rollback_annotation_total` | `isvc,source_phase` | Counter for `ome.io/rollout-rollback` consumption, labeled by the phase the rollback was triggered from. |
| `ome_rollout_transition_total` | `mode,from,to,outcome` | Operator-initiated transitions (e.g. `Canarying → Promoting`, `Canarying → RollingBack`, `BlueGreenStandby → Promoting`). |
| `ome_rollout_failure_total` | `mode,reason` | Counter for entering `Failed`. Reason ∈ {`ReadyTimeout`, `RevisionMismatch`, ...}. |
| `ome_rollout_revision_count` | `isvc,component` | Live revision count per Component. Alert if `> revision-history-limit`. |

**Events on InferenceService:**

- `TrafficPolicyTranslated` (Normal) — first emission
- `ConflictingTrafficPolicy` (Warning) — same-name manual policy
- `MultipleTrafficPoliciesAttached` (Normal) — different-name overlap
- `UnsupportedTrafficField` (Warning) — translator gap
- `NoTranslatorAvailable` (Warning) — no Gateway impl matches
- `BackendPolicyAccepted` / `BackendPolicyRejected` (Normal / Warning)
- `PhaseTransition` (Normal) — `rolloutPhase` transition between any two phases
- `RolloutSupersededByNewSpec` (Warning) — concurrent spec change replaced an in-flight rollout
- `RolloutFailed` (Warning) — rollout entered `Failed` phase (canary not Ready within timeout)
- `RolloutReadyTimeoutAdjusted` (Normal) — admission applied default `15m` because operator didn't set `ome.io/rollout-ready-timeout`
- `RouteBackendsUpdated` (Normal) — HTTPRoute weighted backendRefs changed (transitions between single-backend and multi-backend)
- `RolloutStepEntered` (Normal) — step-based rollout advanced into step N
- `RolloutPaused` (Normal) — entered the `Paused` phase at a step boundary
- `RolloutPromoteAcknowledged` (Normal) — controller consumed `ome.io/rollout-promote`
- `RolloutRollbackAcknowledged` (Normal) — controller consumed `ome.io/rollout-rollback`

**Logs:** structured JSON; each reconcile includes
`{isvc, translator, intent_summary, passthrough_keys, targeted_httproutes, canary_percent}`.

## Opt-In and Rollout

- **No Kubernetes feature gate.** Opt-in is per-ISVC by setting
  `spec.traffic`, an `ome.io/*` annotation, or any non-Serverless
  `canaryTrafficPercent`.
- **Absence preserves today's behavior.** No backend policy emitted,
  gateway default LB, no weighted backendRefs.
- **Rollback** in alpha = remove the field/annotation or revert the
  controller. No global kill switch.

## Test Plan

### Unit Tests

Target coverage:

- `traffic/resolve.go`: ≥ 90% (typed + annotation merge)
- `traffic/annotations.go`: ≥ 90% (parsing, allowlist, Did-you-mean)
- `traffic/factory/factory.go`: ≥ 85%
- `traffic/translators/envoy_gateway.go`: ≥ 90%
- `traffic/reconciler.go`: ≥ 85%
- Webhook additions: ≥ 90%
- HTTPRoute builder weighted-backendRef logic: ≥ 90%

### Integration Tests

Envtest harness with `tests/integration/dep-crds/envoy-gateway/`
covering the EG CRD subset OME translates against.

**Part 1 — Backend policy (alpha gate):**

1. No traffic intent → HTTPRoutes only, no BTP.
2. `traffic.algorithm: LeastRequest` → one BTP, all HTTPRoutes in `targetRefs`, `BackendPolicyReady=True`.
3. `algorithm: ConsistentHash` + `consistentHash.type: Header` + headers → BTP with right block.
4. `endpointOverride.type: Header` → BTP with `loadBalancer.endpointOverride`.
5. Annotation `ome.io/circuit-breaker-per-endpoint-max-connections: "1"` → BTP with that field; `loadBalancerType=Default`.
6. Pass-through `ome.io/btp.loadBalancer.slowStart.window: 30s` → BTP with the field stitched; condition message lists the key.
7. ISVC deleted → owned BTP garbage-collected.
8. Webhook rejects bad enum, missing hash key, multiple hash keys, malformed annotations, unknown annotation with Did-you-mean.
9. Engine HTTPRoute always emitted (even when router is present). BTP targets all OME-emitted HTTPRoutes.

**Part 2 — Rollout strategies (alpha gate):**

10. **Rolling (default):** ISVC with `canaryTrafficPercent` unset → spec change → existing rolling behavior, no per-revision Services, HTTPRoute has one backendRef. `rolloutPhase: Stable` throughout.
11. **Canary:** test fixture populates `status.components.engine.traffic[]` with two entries (90/10) → HTTPRoute has two backendRefs weighted 90/10. `rolloutPhase: Canarying`.
12. **Blue-green standby:** `canaryTrafficPercent: 0` → two revisions live → HTTPRoute has two backendRefs (100/0). `rolloutPhase: BlueGreenStandby`. Per-revision Service is in-cluster addressable.
13. **Promotion:** transition `canaryTrafficPercent: 10` → `100` → `rolloutPhase` goes `Canarying` → `Promoting` → `Stable`. `LatestRolledoutRevision` updated to canary's hash; old stable drained.
14. **Cutover from standby:** transition `canaryTrafficPercent: 0` → `100` → `rolloutPhase` goes `BlueGreenStandby` → `Promoting` → `Stable`. Single atomic step at the HTTPRoute layer.
15. **Rollback from canary:** transition `canaryTrafficPercent: 10` → `0` → `rolloutPhase` goes `Canarying` → `RollingBack` → `BlueGreenStandby`. Canary pods drain; stable unchanged.
16. **Concurrent spec change during canary:** mid-canary spec edit → Warning event `RolloutSupersededByNewSpec`; in-flight canary drains; new canary starts at the same percent targeting the new revision.
17. **Failure handling:** force canary pods to fail (bad image) → after `ome.io/rollout-ready-timeout` (set short for the test) → `rolloutPhase: Failed`, Warning event `RolloutFailed`, canary pods retained for diagnosis, HTTPRoute weights unchanged (Envoy excludes unhealthy endpoints from the LB pool).
18. **Recovery from Failed:** from Failed, set `canaryTrafficPercent: 0` → canary drains, `rolloutPhase: Stable`.
19. Webhook rejects `canaryTrafficPercent` out of range, mismatched in PD, set without a Component, `ome.io/rollout-ready-timeout` not a duration.
20. **End-to-end with Serverless:** existing Knative canary flow unchanged. `rolloutPhase` field populated from Knative status.
21. **End-to-end with OMENative (alpha):** spec change with `canaryTrafficPercent=10` → two revision Services emitted by the OMENative reconciler → HTTPRoute reflects 90/10 split → BTP attached to all routes → `rolloutPhase: Canarying`.

**Part 2 — Step-based rollouts (alpha gate):**

22. **Step-based with Manual promotion:** `spec.traffic.rollout` with 3 steps, `promotionPolicy: Manual`, `pause: {}` on each. First step enters `Paused`. Operator sets `ome.io/rollout-promote=1` → advances to step 2 → `Paused` again. Setting `ome.io/rollout-promote=full` → advances to final step → `Stable`. Annotation is cleared by the controller each time.
23. **Step-based with Auto promotion and pause duration:** `promotionPolicy: Auto`, `pause: { duration: 5s }` on step 0. Controller auto-advances after 5s. `rolloutPhase` transitions `Paused → <next step> → ...`.
24. **Capacity vs traffic decoupling:** step with `newCapacity: "100%", oldCapacity: "100%", trafficWeight: 0` → both revisions at full scale, HTTPRoute backendRefs 100/0. Per-revision Service for new revision addressable in-cluster.
25. **Step-based rollback:** `ome.io/rollout-rollback: true` mid-step → instant traffic shift to 100% old, new drains after `scaleDownDelaySeconds`, `rolloutPhase: RollingBack → Stable`. Annotation cleared.
26. **Per-step execution order:** instrument the per-mode reconciler to record the order. Verify for each step: scale up → wait ready → traffic shift → drain wait → scale down. Especially verify the drain wait honors `scaleDownDelaySeconds` (set to 5s for the test; assert observed wait >= 5s).
27. **Concurrent spec change during step-based rollout:** mid-step (Paused) spec edit → controller restarts the rollout from step 0 targeting the new revision (the operator's plan still applies; the target spec is the new one). Warning event `RolloutSupersededByNewSpec`.
28. **Webhook rejects** the step-based form: mixed forms (both `rollout` and `canaryTrafficPercent` set), empty steps, final step's `trafficWeight != 100`, invalid `pause.duration`, invalid `newCapacity` percent.

**Part 3 — Composition (alpha gate):**

29. ConsistentHash on `X-Session-ID` + `canaryTrafficPercent=10` on OMENative → BTP with `consistentHash`, HTTPRoute with weighted backends. Verify (a) BTP attached, (b) sticky within each pool, (c) ~10% of distinct sessions land in canary pool.
30. Same with `canaryTrafficPercent=0` (blue-green standby) → ~0% of session traffic to canary; per-revision Service still routes when called directly.
31. ConsistentHash + step-based rollout (3 steps) on OMENative → BTP with `consistentHash` applied across all steps; HTTPRoute weights advance through the steps under Manual promotion.

**Coexistence (beta gate):**

32. Adding `spec.traffic` to ISVC with pre-existing same-name manual BTP: deferred, event emitted.
33. Different-name manual BTP overlap → event `MultipleTrafficPoliciesAttached`; OME's policy still applied.

**Generation-scoped discovery (beta gate, PD-specific):**

34. PD ISVC with engine and decoder; spec change triggers rollout; verify (via instrumented runtime) that new-revision engine pods discover new-revision decoder pods and vice versa, never cross-version. Implementation-specific: OMENative reconciler uses one of the documented discovery strategies (per-revision ConfigMap, env injection, or label-selector sidecar).

**Other modes (beta gate):**

35. RawDeployment + rolling/canary/blue-green contract.
36. MultiNode + rolling/canary/blue-green contract.
37. PDDisaggregated + canary contract (engine + decoder coupled).

**Chaos (beta gate):**

38. Controller restart mid-translation → resume, idempotent.
39. Controller restart mid-Promoting → resume; canary completes scaling to full, stable completes draining.
40. Controller restart mid-Paused (step-based) → resume; phase stays Paused; annotations still work post-restart.
41. Operator deletes OME-managed BTP out-of-band → recreated; Warning event.
42. Gateway slow to acknowledge → `Unknown / Pending` until ack.

## Graduation Criteria

### Alpha

- All unit tests passing.
- Part 1 integration tests 1–9 passing.
- Part 2 integration tests 10–21 passing — all three rollout strategies (rolling, canary, blue-green standby) on the simple `canaryTrafficPercent` form, promotion, rollback, concurrent spec changes, failure handling, and recovery, exercised on both Serverless (verified) and OMENative (implemented).
- Part 2 integration tests 22–28 passing — step-based rollouts on OMENative: Manual + Auto promotion, capacity vs traffic decoupling, rollback annotation, execution-order verification, concurrent spec change, webhook validation.
- Part 3 integration tests 29–31 passing (composition: ConsistentHash × canary, ConsistentHash × blue-green standby, ConsistentHash × step-based on OMENative).
- Envoy Gateway translator implemented; Istio translator stub returning `NoTranslatorAvailable`.
- Documentation covers: typed core, annotation extension, pass-through, no-feature-gate contract, all three rollout strategies and their lifecycle phases, step-based rollouts with operator verbs, generation-scoped discovery hazard, failure handling, coexistence behavior, power-user fallback via hand-authored sibling.

### Beta

- Part 2 integration tests 35–37 passing (RawDeployment, MultiNode, PDDisaggregated with all three rollout strategies AND the step-based form).
- Generation-scoped discovery test 34 passing for at least one PD-mode reconciler.
- Coexistence + chaos tests 32–33 and 38–42 passing.
- 2+ production users running ISVCs with OME-managed traffic policies AND at least one non-rolling rollout strategy ≥ 60 days without Sev1/Sev2 incidents.
- Migration guide published.
- Annotation surface frozen except for additive new keys.
- Promotion/demotion criterion exercised at least once (annotation → typed, or ISVC-level → per-Component).
- Istio translator either ships or remains a documented stub.

### GA

- 3 months Beta with no regressions.
- Hand-authored coexistence at phase 3 (admission rejection without ack).
- Test coverage ≥ 85% across `pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/`.
- Field coverage matrix complete for at least two implementations.

## Implementation History

- 2026-05-15: Initial draft (pure annotation, per-Component, policy only).
- 2026-05-16: Hybrid typed-core + annotation API; UX-focused user stories; simplified to ISVC-level only; unified with canary into one Traffic Management OEP; trimmed to focus on the traffic surface (workload orchestration deferred to per-mode reconciler design); added explicit rollout-strategy framing (rolling, canary, blue-green standby via one field), lifecycle phases (`rolloutPhase` status field), concurrent-spec-change behavior, and failure handling.
- 2026-05-16: Added optional step-based rollout plan (`spec.traffic.rollout`) with `Manual`/`Auto` promotion policy and per-step `pause`, decoupling pod capacity from traffic weight; operator verbs (`ome.io/rollout-promote`, `ome.io/rollout-rollback`); pinned per-step execution order (scale up → ready → traffic → drain → scale down); flagged generation-scoped internal discovery as a PD hazard the per-mode reconciler must address.

## Drawbacks

1. **No per-Component policy differentiation in alpha.** Operators fall back to hand-authoring a sibling BTP. Documented and metricated.
2. **Two API surfaces for policy.** Typed core + annotation extension. Common path is typed; long tail annotation.
3. **Canary requires per-mode reconciler work.** This OEP defines the contract; each per-mode reconciler must implement it (workload duplication, per-revision Services, generation-scoped discovery, ready-timeout detection, scale-down delay, annotation consumption). Alpha requires OMENative across both the simple form and the step-based form; Beta adds the others.
4. **Two rollout forms (simple + step-based) on one ISVC.** Operators must pick. Mutually exclusive enforced at admission. The simpler form covers the common case; the step form unlocks multi-step plans and capacity-vs-traffic decoupling.
5. **PD coupling is a sharp edge.** Setting `decoder.canaryTrafficPercent` to a different value than engine's is admission-rejected; some operators will be surprised.
6. **Generation-scoped discovery is a hard requirement, not a nice-to-have, for PD.** Per-mode reconciler implementations that skip it cause silent cross-version internal traffic — worse than not supporting rollouts at all. The OEP flags the hazard but doesn't enforce it programmatically; review of per-mode reconciler PRs must catch this.
7. **Pass-through namespace can be abused.** Usage metric makes it visible; GA documentation calls it unsupported.
8. **Cross-implementation translation is lossy.** Field coverage matrix bounds the divergence per translator.
9. **Coexistence cost.** Operators with hand-authored policies migrate over the deprecation window.

## Alternatives

1. **Two separate OEPs (policy vs canary).** Rejected: both are traffic management from the operator's perspective; composition story would be cross-reference-heavy.
2. **Per-Component typed surface in alpha.** Cleaner power-user UX; rejected because the simpler ISVC-level surface covers the 90% case. Promotion criterion makes the decision reversible.
3. **Pure typed fields, no annotations.** Every long-tail knob requires a CRD bump; rejected.
4. **Pure annotations.** No enum validation, no `kubectl explain`; rejected for the common path.
5. **New `InferenceServiceTrafficPolicy` or `InferenceServiceCanary` CRD.** Adds user-visible CRDs (against OEP-0007 philosophy); rejected.
6. **Generate BTP from defaults on `ServingRuntime`.** Couples policy to the runtime author; wrong layer.
7. **Build on Gateway API `BackendLBPolicy` (GEP-1619).** Standardized backend policy attachment, but the current spec covers **only session persistence** (cookie/header) — LB algorithm, circuit breaker, retries, and connection timeouts are still implementation-specific. The CRD name `BackendLBPolicy` is reserved so future GEPs can extend the same resource with LB-algorithm and connection-pool fields. Track upstream; migrate OME's translator to emit `BackendLBPolicy` for the session-persistence portion once Envoy Gateway and Istio both make it canonical, and revisit broader migration once future GEPs extend its scope.
8. **External rollout controller (Argo Rollouts / Flagger).** Adds CRD vocabulary, duplicates the revision concept OME's status already models. Rejected: keep canary on the ISVC.
9. **Top-level `spec.canaryTrafficPercent` instead of per-Component.** Cleaner for PD; rejected because the per-Component field already exists and is used by Knative. PD coupling rule covers the multi-Component concern.

## Open Questions

1. **Promotion thresholds.** 25% / 6 weeks for annotation → typed; 10% / 8 weeks for ISVC-level → per-Component. Revisit at beta with real data.
2. **Multiple Gateway implementations in one cluster.** Alpha picks the first detected. Add `ome.io/traffic-policy-translator` annotation as override if a real cluster needs both.
3. **Gateway API `BackendLBPolicy` (GEP-1619) upstream scope expansion.** Today's `BackendLBPolicy` covers only session persistence. Whether future GEPs extend the same resource to cover LB algorithm, circuit breaker, and retries (the bulk of OME's translator surface) determines whether OEP-0011 can eventually migrate to a fully upstream-standardized backend policy or remains tied to per-implementation translators forever.
4. **Rate limiting.** Envoy Gateway supports `RateLimit` via BTP. Defer until operator request.
5. **Backend TLS.** `BackendTLSPolicy` is a separate Gateway API resource. Follow-up OEP.
6. **Multi-version traffic split across separately-deployed ISVCs.** Canary in this OEP is intra-ISVC. A/B/C/D across multiple ISVCs is a separate concern; could share the same translator and HTTPRoute machinery.
7. **Auto-promotion based on health signals.** Alpha is operator-initiated only.
8. **Header-based canary routing.** Today canary is weight-based across all traffic. Header-based (e.g. `X-Canary: true` → always canary) is a separate routing pattern.
9. **Per-revision policy override.** Hand-authored sibling today; typed surface is a beta+ question.
10. **Revision-hash sharing with OEP-0007.** Both this OEP (canary identification) and OEP-0007 (in-place vs recreate update decisions) need a stable revision-input canonicalization. A shared helper in `pkg/apis/ome/v1beta1` is the right place; lock down at alpha-implementation time.
