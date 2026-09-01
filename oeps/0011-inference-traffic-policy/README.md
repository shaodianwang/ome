# OEP-0011: InferenceService Backend Traffic Policy

> **Scope note.** `spec.traffic` is **load-balancing and backend policy only** —
> LB algorithm, session affinity, circuit breaker, retries, connection timeouts.
> It does **not** carry progressive rollout. Cross-Component rollout coupling
> (blue-green / rolling / sequential across Engine, Decoder, Router) lives at
> `spec.rollout.coordination` (OEP-0007.1, shipped). The per-revision
> step/canary plan is **reserved** — shape-only, admission-rejected, no executor
> — at `spec.rollout.canary` (OEP-0015). This OEP scoped down to Part 1 (backend
> policy); the rollout half it once proposed is [superseded](#superseded-progressive-rollout-moved-to-specrollout).

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Today's gaps](#todays-gaps)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Conceptual overview](#conceptual-overview)
  - [Terminology](#terminology)
  - [User stories](#user-stories)
- [Design Details](#design-details)
  - [The typed core](#the-typed-core)
  - [The annotation contract](#the-annotation-contract)
  - [The pass-through escape hatch](#the-pass-through-escape-hatch)
  - [One BackendTrafficPolicy per ISVC](#one-backendtrafficpolicy-per-isvc)
  - [Engine HTTPRoute always emitted](#engine-httproute-always-emitted)
  - [Status surface](#status-surface)
  - [Translator architecture](#translator-architecture)
  - [Coexistence with hand-authored policies](#coexistence-with-hand-authored-policies)
  - [Webhook validation](#webhook-validation)
  - [Field coverage matrix](#field-coverage-matrix)
  - [Observability](#observability)
- [Superseded: progressive rollout moved to spec.rollout](#superseded-progressive-rollout-moved-to-specrollout)
- [Opt-in and rollout](#opt-in-and-rollout)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

Tl;dr: an operator who wants anything past a request timeout on an OME ingress —
session affinity, a circuit breaker, retries, a non-default load-balancing
algorithm — has to hand-author a gateway-implementation-specific policy resource
next to the `InferenceService` (ISVC). That policy drifts on ISVC delete, locks
the manifest to one gateway implementation, and never reports its state back to
the ISVC. This OEP promotes backend traffic policy to a typed field on the ISVC
(`spec.traffic`), with an `ome.io/*` annotation contract for the long tail and a
verbatim pass-through escape hatch for the rest, and emits exactly one
gateway-native policy per ISVC, owned by the ISVC, with status surfaced through
`InferenceService.Status.Traffic`.

This is **shipped**. The typed core, the annotation contract, the translator
factory (Envoy Gateway > Istio > noop), the `BackendTrafficPolicy` builder, and
the `TrafficStatus` surface all merged — see [Implementation
History](#implementation-history). The remainder of this document describes the
design as built, and marks the gaps (metrics) honestly.

**One term up front, because the doc turns on it.** *Backend policy* is
steady-state load balancing for a serving pool — how the gateway picks which
healthy pod gets the next request, and how it protects that pod (circuit
breaker, retries, timeouts). It is **not** rollout: rollout is *which revision's
pool exists and what fraction of traffic it gets over time*. This OEP owns the
first; the second moved to `spec.rollout` (see
[Superseded](#superseded-progressive-rollout-moved-to-specrollout)).

The OEP introduces **no new CRDs** and one new spec field,
`InferenceServiceSpec.Traffic` (`pkg/apis/ome/v1beta1/inference_service.go`),
typed by `TrafficSpec` (`pkg/apis/ome/v1beta1/traffic_types.go`). Per OEP-0007
precedent: **no Kubernetes feature gate**. Opt-in is per-ISVC by setting any
`spec.traffic` field or any `ome.io/*` traffic annotation. The change is
**additive**: nothing changes for an ISVC that does not opt in.

## Motivation

### Today's gaps

`ComponentExtensionSpec.TimeoutSeconds` is the only ingress knob OME types.
Everything past request timeout — LB algorithm, hash-based session affinity,
circuit breakers, retries, connection timeouts, backend TLS, rate limiting — is
hand-authored as a sibling gateway-implementation-specific resource. A real
example on a production cluster: a chat-serving ISVC carried a separate
`BackendTrafficPolicy` with `loadBalancer: LeastRequest` plus a circuit breaker
and connection timeouts, all maintained outside the ISVC's lifecycle.

That arrangement breaks in three concrete ways, each a failure mode the typed
surface is meant to prevent:

1. **Drift on ISVC delete.** The hand-authored policy is not owned by the ISVC,
   so deleting the ISVC orphans it. The next ISVC with the same name inherits a
   stale policy nobody remembers writing.
2. **Gateway-implementation lock-in.** A hand-authored `BackendTrafficPolicy`
   (Envoy Gateway) or `DestinationRule` (Istio) bakes the implementation into
   the manifest. Moving clusters means rewriting the policy by hand.
3. **Status blindness.** When the gateway controller rejects the policy, nothing
   surfaces on the ISVC. The operator finds out from a latency graph, not a
   condition.

Each gap pushes the operator outside OME's lifecycle. Promoting backend policy
into the ISVC collapses all three to one declaration that rides OME's existing
admission and ownership paths.

### Goals

1. **One declaration per ISVC** for backend policy intent. *Rationale:* a single
   owned resource cannot drift independently of the workload it protects.
2. **Implementation-agnostic surface.** Envoy Gateway, Istio behind the same
   operator-facing fields. *Rationale:* the manifest should outlive a gateway
   migration.
3. **ISVC-level scope.** One policy per ISVC targeting every OME-managed
   HTTPRoute. *Rationale:* the common case wants consistent behavior whether a
   client hits the top-level URL or a per-Component subdomain; per-Component
   differentiation is the rare case (handled by coexistence, below).
4. **Lifecycle-bound.** The emitted policy is owned by the ISVC and
   garbage-collected with it. *Rationale:* this is the direct fix for drift.
5. **Discoverable common path, escape hatch for the long tail.** Common LB knobs
   typed (`kubectl explain`, IDE completion); the long tail as validated
   annotations; the truly implementation-specific as verbatim pass-through.
   *Rationale:* typing every gateway feature is a losing race, but leaving the
   common path untyped sacrifices validation.

### Non-Goals

1. **Not modeling every Gateway implementation feature.** The pass-through
   escape hatch covers what OME does not type. Typing the long tail is a
   case-by-case promotion, not a goal.
2. **Not per-Component policy differentiation as a typed field.** An operator
   who needs a different algorithm on one Component hand-authors a sibling
   policy targeting only that Component's HTTPRoute (coexistence rules apply).
   A typed per-Component surface is a beta+ question, gated on real demand.
3. **Not a Gateway resource manager.** OME does not own `Gateway` /
   `GatewayClass`.
4. **Not a rate limiter or auth gate.** Those are separate Gateway API
   resources; see [Open Questions](#open-questions).
5. **Not a feature gate.** Per OEP-0007.
6. **Not a top-level CRD.** No `InferenceServiceTrafficPolicy`.
7. **Not progressive rollout.** Canary, blue-green, and cross-Component
   coordination are **out of scope** and live at `spec.rollout` — see
   [Superseded](#superseded-progressive-rollout-moved-to-specrollout). This
   OEP's surface is steady-state load balancing only.

## Proposal

### Conceptual overview

```
InferenceService
├── spec.traffic                     (typed: algorithm + consistentHash + endpointOverride)
└── metadata.annotations[ome.io/*]   (annotation contract + impl-specific pass-through)

                  │  resolve typed + annotations -> one intent
                  ▼
   ┌──────────────────────────────────────────────────┐
   │  Traffic reconciler                               │
   │  ├── factory picks a translator by installed CRDs │
   │  │     (Envoy Gateway > Istio > noop)             │
   │  ├── translates intent -> one backend policy      │
   │  │     targeting every OME-managed HTTPRoute      │
   │  └── writes InferenceService.Status.Traffic       │
   └──────────────────────────────────────────────────┘
                  │
                  ▼
   Owned by the ISVC. Auto-cleanup on delete.
```

Two surfaces, one ISVC declaration:

| Surface | Holds | Notes |
|---|---|---|
| `spec.traffic` (typed) | LB algorithm, consistent-hash key, endpoint override | Primary; validated, `kubectl explain`-able |
| `metadata.annotations[ome.io/*]` | Circuit breaker, retries, sub-second timeouts, verbatim pass-through | Long-tail and escape hatch |

The typed field is the primary surface. The annotation contract is the **long
tail**, not the front door — a deliberate inversion of this OEP's first draft,
which was annotation-first. Reason: annotations get no enum validation and no
`kubectl explain`, so the common knobs (algorithm, hash key) belong in the type
system; only the rarely-set knobs (circuit-breaker bounds, retry policy) stay as
annotations.

### Terminology

- **Backend policy** : steady-state load balancing for one serving pool — LB
  algorithm, session affinity, circuit breaker, retries, connection timeouts.
  Distinct from rollout (which revision's pool exists, and its traffic share).
- **Translator** : the per-gateway-implementation component that turns OME's
  resolved intent into a concrete policy resource (`BackendTrafficPolicy` for
  Envoy Gateway, `DestinationRule` for Istio). One translator is active per
  cluster, chosen at controller startup.
- **Pass-through** : an `ome.io/btp.*` or `ome.io/dr.*` annotation whose dotted
  path is stitched **verbatim** into the emitted resource. OME does not
  validate it; the gateway controller does. The escape hatch for fields OME
  does not model.
- **Component** : an Engine, Decoder, or Router half of an ISVC. Each Component
  gets its own HTTPRoute; one backend policy targets all of them.

### User stories

#### Story 1 — Add session affinity (the SGLang case)

```yaml
spec:
  traffic:
    algorithm: ConsistentHash
    consistentHash:
      type: Header
      headers: [{ name: X-Session-ID }]
```

OME emits one `BackendTrafficPolicy` named `<isvc>` targeting every OME-managed
HTTPRoute (top-level + router + decoder + engine) via `targetRefs`. Status
surfaces `algorithm: ConsistentHash` and `BackendPolicyReady=True` once the
gateway controller acknowledges.

#### Story 2 — Tune a circuit breaker via annotation

```yaml
metadata:
  annotations:
    ome.io/circuit-breaker-max-connections: "1024"
    ome.io/circuit-breaker-max-pending-requests: "256"
    ome.io/retry-attempts: "2"
    ome.io/retry-on: "5xx,reset,connect-failure"
```

The webhook coerces each string (`strconv.Atoi`, `time.ParseDuration`) and maps
it onto the emitted policy's circuit-breaker and retry blocks. `algorithm` stays
at the gateway default (`Default` in status), because no `spec.traffic.algorithm`
was set.

#### Story 3 — An Envoy-specific field via pass-through

```yaml
metadata:
  annotations:
    ome.io/btp.loadBalancer.slowStart.window: 30s
    ome.io/btp.loadBalancer.slowStart.aggression: "1.5"
```

`ome.io/btp.<path>` is stitched verbatim into the emitted
`BackendTrafficPolicy.spec.<path>`. OME does not validate the field; the gateway
controller does. A rejection surfaces as `BackendPolicyReady=False,
Reason=GatewayRejected`. Using `ome.io/btp.*` on a cluster where Istio is the
active translator surfaces a `BackendPolicyUnsupportedFields` condition listing
the dropped keys (and is rejected at admission as `UnsupportedPassthrough`).

#### Story 4 — Migrate off a hand-authored policy

An operator has a hand-authored `BackendTrafficPolicy` with the same name OME
would emit (`<isvc>`), then adds `spec.traffic` to the ISVC.

Failure mode if OME blindly overwrites: it clobbers a policy the operator may
still depend on. Mitigation: OME **defers**. Status reads
`BackendPolicyReady=False, Reason=ConflictingPolicy` with a message naming the
conflicting resource; the hand-authored policy stays in effect. The operator
deletes the manual policy, and OME takes ownership on the next reconcile. See
[Coexistence](#coexistence-with-hand-authored-policies).

## Design Details

### The typed core

One Go struct, one new field on `InferenceServiceSpec`. As built
(`pkg/apis/ome/v1beta1/traffic_types.go`):

```go
// TrafficSpec configures the load-balancing policy applied to all
// HTTPRoutes OME emits for the InferenceService. Long-tail knobs
// (circuit breaker, retries, sub-second timeouts) live as ome.io/*
// annotations. Progressive rollout lives on spec.rollout, not here.
type TrafficSpec struct {
    // +kubebuilder:validation:Enum=RoundRobin;LeastRequest;Random;ConsistentHash
    // +optional
    Algorithm *LoadBalancingType `json:"algorithm,omitempty"`

    // Required when Algorithm=ConsistentHash; rejected otherwise (webhook).
    // +optional
    ConsistentHash *ConsistentHashSpec `json:"consistentHash,omitempty"`

    // Routes to a specific pod when a header (e.g.
    // "X-Endpoint-HostPort: 10.0.0.5:30000") is present and healthy;
    // falls back to Algorithm otherwise. Envoy Gateway only in alpha.
    // +optional
    EndpointOverride *EndpointOverrideSpec `json:"endpointOverride,omitempty"`
}
```

- `Algorithm` is `LoadBalancingType` (`RoundRobin | LeastRequest | Random |
  ConsistentHash`). Absent = the gateway implementation's default (LeastRequest
  for Envoy Gateway). Note the type name is `LoadBalancingType`, not
  `LoadBalancerType` — a detail the first draft got wrong.
- `ConsistentHash` is required exactly when `Algorithm=ConsistentHash`, and
  rejected otherwise (webhook, not just CRD enum). Its `Type` is
  `Header | Cookie | SourceIP`; `Headers` (one or more, concatenated before
  hashing) is required for `Header`, `Cookie.Name` for `Cookie`, neither for
  `SourceIP`.
- `EndpointOverride` is **Envoy-Gateway-only** in alpha — there is no Istio
  equivalent, so the Istio translator surfaces it as `UnsupportedField`. It
  exists for the disaggregated-serving case where a client already knows which
  pod holds the relevant KV cache and wants to pin to it.

The field on `InferenceServiceSpec` is `Traffic *TrafficSpec`
(`pkg/apis/ome/v1beta1/inference_service.go`). Operators get `kubectl explain
isvc.spec.traffic` and IDE completion for free.

Note what is **not** here: there is no `Rollout` field on `TrafficSpec`. The
first draft proposed `spec.traffic.rollout`; OEP-0015 deleted it and reserved the
shape at `spec.rollout.canary` instead. The `traffic_types.go` header comment
states this in-tree: "Progressive rollout lives on spec.rollout, not here."

### The annotation contract

Long-tail backend-policy knobs on the ISVC metadata. The webhook validates and
coerces per key. Constants live in `pkg/constants/traffic_annotations.go`; the
parsers and the Did-you-mean allowlist live in `pkg/validation/`.

| Key | Type | Maps to |
|---|---|---|
| `ome.io/circuit-breaker-max-connections` | int | `circuitBreaker.maxConnections` |
| `ome.io/circuit-breaker-max-parallel-requests` | int | `circuitBreaker.maxParallelRequests` |
| `ome.io/circuit-breaker-max-pending-requests` | int | `circuitBreaker.maxPendingRequests` |
| `ome.io/circuit-breaker-max-parallel-retries` | int | `circuitBreaker.maxParallelRetries` |
| `ome.io/circuit-breaker-per-endpoint-max-connections` | int | `circuitBreaker.perEndpoint.maxConnections` |
| `ome.io/retry-attempts` | int (default 0) | `retry.numRetries` |
| `ome.io/retry-on` | comma list (`5xx`, `reset`, `gateway-error`, `connect-failure`, `retriable-status-codes`) | `retry.retryOn` |
| `ome.io/retry-per-try-timeout` | duration | `retry.perTryTimeout` |
| `ome.io/timeout-idle` | duration | `timeout.http.connectionIdleTimeout` |
| `ome.io/timeout-max-connection-duration` | duration | `timeout.http.maxConnectionDuration` |
| `ome.io/timeout-tcp-connect` | duration | `timeout.tcp.connectTimeout` |
| `ome.io/managed-by-conflict-acked` | `"true"` | Acknowledges a same-name hand-authored policy (suppresses GA rejection). |

Request-level timeout stays on the existing per-Component
`ComponentExtensionSpec.TimeoutSeconds` — that is an HTTPRoute-level setting, not
a backend-policy one, so it does not belong in this surface.

Conventions:
- Annotation values are strings; the webhook coerces (`strconv.Atoi`,
  `time.ParseDuration`) and rejects malformed values at admission.
- An unknown `ome.io/*` key within Levenshtein distance 2 of a documented key is
  rejected at admission with a Did-you-mean. *Rationale:* a typo in an
  annotation key is otherwise silently ignored — the convenience of a string map
  becomes a silent footgun.

### The pass-through escape hatch

For implementation-specific fields OME does not model:

- `ome.io/btp.<dotted-path>` → Envoy Gateway `BackendTrafficPolicy.spec.<path>`
- `ome.io/dr.<dotted-path>` → Istio `DestinationRule.spec.<path>`

The value is stitched **verbatim** into the emitted resource; OME does not
validate it. The pass-through namespace is **per-translator**: using `ome.io/btp.*`
when Istio is the active translator surfaces a `BackendPolicyUnsupportedFields`
condition and is rejected at admission. The active translator publishes its
known pass-through prefixes (`SupportedPassthroughPrefixes()`), wired into the
webhook at startup (`cmd/manager/main.go`).

Failure mode this guards: an operator stays on pass-through forever, evading the
typing that would give them validation. Mitigation today is documentation —
pass-through is documented as unsupported and subject to change. A per-field
usage metric to make the staleness *measurable* is planned but not yet
implemented (see [Observability](#observability)).

### One BackendTrafficPolicy per ISVC

OME emits exactly **one** policy per ISVC, named `<isvc>`, targeting every
OME-managed HTTPRoute via `targetRefs`, owned by the ISVC
(`pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/backendtrafficpolicy_builder.go`).

Why one policy for all routes, not one per Component? The common case wants the
same backend behavior whether a client addresses the top-level URL or a
per-Component subdomain — and one owned resource cannot drift relative to the
others. The rare case (different algorithm per Component) is served by
coexistence: hand-author a sibling policy targeting only one Component's
HTTPRoute, and OME's policy plus the sibling both attach (the gateway merges them
per its own attachment rules). See [Coexistence](#coexistence-with-hand-authored-policies).

Ownership is the direct fix for drift: the policy carries an owner reference to
the ISVC, so `kubectl delete isvc` garbage-collects it.

### Engine HTTPRoute always emitted

Before this OEP, the engine HTTPRoute was omitted when a router was present (the
top-level route fell back to the router). This OEP changes that: OME always emits
`<isvc>-engine`. Reason: direct engine access is operationally useful
(debugging, a per-Component hand-authored sibling policy, mesh integration), and
a per-Component policy needs a per-Component route to target.

This is backward-compatible: emitting a new HTTPRoute does not change existing
traffic, because no client was using the engine-subdomain hostname — it did not
exist before.

### Status surface

One field on `InferenceServiceStatus`: `Traffic *TrafficStatus`. As built
(`pkg/apis/ome/v1beta1/traffic_status_types.go`):

```go
type TrafficStatus struct {
    // Resolved algorithm: RoundRobin | LeastRequest | Random |
    // ConsistentHash | Default. "Default" = no Algorithm set, gateway
    // default applies.
    Algorithm string `json:"algorithm,omitempty"`

    // The OME-emitted policy (e.g. gateway.envoyproxy.io/v1alpha1
    // BackendTrafficPolicy). Empty when none was emitted (e.g. deferred
    // to a conflicting hand-authored policy).
    BackendPolicyResource *BackendPolicyRef `json:"backendPolicyResource,omitempty"`

    // The HTTPRoute names the emitted policy targets.
    TargetedHTTPRoutes []string `json:"targetedHTTPRoutes,omitempty"`

    // BackendPolicyReady (and, when keys were dropped,
    // BackendPolicyUnsupportedFields).
    Conditions []metav1.Condition `json:"conditions,omitempty"`
}
```

`TrafficStatus` is populated only when traffic intent is declared (any
`spec.traffic` field or any `ome.io/*` traffic annotation); otherwise it is nil,
so older clients see nothing.

Two condition types are surfaced:

- **`BackendPolicyReady`** — the headline. Its reasons:

  | Status | Reason | When |
  |---|---|---|
  | `True`    | `AcceptedByGateway`     | Policy emitted, gateway acknowledged. |
  | `False`   | `ConflictingPolicy`     | Same-name hand-authored policy exists. |
  | `False`   | `UnsupportedField`      | Translator cannot honor a declared field. |
  | `False`   | `NoTranslatorAvailable` | No Gateway implementation matches. |
  | `False`   | `GatewayRejected`       | Gateway reported `Accepted=False`. |
  | `False`   | `TranslationFailed`     | Translator errored turning intent into a resource. |
  | `Unknown` | `Pending`               | Awaiting gateway acknowledgment. |

  Note: `TranslationFailed` is one reason beyond the set the first draft listed —
  it surfaces an intent shape the active implementation cannot honor, with the
  translator's error string in the message.

- **`BackendPolicyUnsupportedFields`** — added (Status=True,
  Reason=`UnsupportedField`) only when the operator declared annotations the
  active translator does not honor (e.g. an `ome.io/dr.*` Istio pass-through on
  an Envoy-Gateway cluster). The message lists every dropped key. Omitted
  entirely when nothing was dropped (positive-polarity: absence = nothing to
  worry about) and on noop / `TranslationFailed` branches, where
  `BackendPolicyReady` already explains the situation.

This conditions surface — **not** a metrics surface — is how observability works
today; see [Observability](#observability).

### Translator architecture

```
pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/
├── reconciler.go        # resolve -> translate -> apply -> status
├── intent.go            # merge typed + annotations into one resolved intent
├── translator.go        # the Translator interface
├── unsupported.go       # per-translator unsupported-field accounting
├── factory/factory.go   # picks the translator by detected CRDs at startup
├── status/status.go     # computes TrafficStatus + conditions
└── translators/
    ├── envoygateway/    # -> BackendTrafficPolicy
    ├── istio/           # -> DestinationRule
    └── noop.go          # surfaces NoTranslatorAvailable
```

The factory (`factory/factory.go`) probes the cluster for well-known
backend-policy CRDs at controller startup and selects the **highest-priority
match**, falling back to noop:

1. **Envoy Gateway** — `gateway.envoyproxy.io/v1alpha1` `BackendTrafficPolicy`.
2. **Istio** — `networking.istio.io/v1` `DestinationRule`.
3. **noop** — no supported policy CRD installed; every reconcile surfaces
   `NoTranslatorAvailable`.

A clean "CRD not present" answer advances to the next probe; only a probe error
(e.g. a transport failure talking to the API server) is fatal. Wiring is in
`cmd/manager/main.go` (`trafficfactory.New(cfg)` →
`traffic.NewReconciler(...)`), mirroring the manager's existing optional-CRD
detection pattern.

Reconcile order per ISVC:
1. The HTTPRoute builders emit the per-Component routes (engine always emitted).
2. The traffic reconciler resolves typed + annotation intent, picks the
   translator, and emits one policy targeting all routes.
3. The status updater computes `Status.Traffic` from the policy's
   acknowledgment.

### Coexistence with hand-authored policies

**Same-name conflict.** A hand-authored policy with the name OME would use
(`<isvc>`):

| Phase | OME behavior |
|---|---|
| Alpha (today) | Defer. `BackendPolicyReady=False, Reason=ConflictingPolicy`. Do not overwrite. |
| Beta | Same, plus an admission warning suggesting migration. |
| GA | Reject the ISVC update at admission unless `ome.io/managed-by-conflict-acked: true` is set. |

The three-phase ramp exists so that an operator with an existing hand-authored
policy is never silently overwritten on upgrade: alpha defers, beta warns, GA
requires an explicit acknowledgment. The failure mode it prevents is a silent
clobber of a policy the workload still depends on.

**Different-name overlap.** A hand-authored policy with a *different* name
targets some of OME's HTTPRoutes. This is not a conflict: OME emits its own
policy, and the gateway merges the two per its own attachment rules. This is the
supported fallback for per-Component or per-revision policy differentiation in
alpha — point a sibling policy at exactly one Component's route.

### Webhook validation

CRD enum tags handle the easy cases; the webhook adds cross-field and annotation
rules (`pkg/validation/traffic.go`, `pkg/validation/traffic_annotations.go`):

| Rule | Reason |
|---|---|
| `algorithm: ConsistentHash` requires `consistentHash` | `MissingConsistentHashSpec` |
| `algorithm != ConsistentHash` forbids `consistentHash` | `UnexpectedConsistentHashSpec` |
| `consistentHash.type: Header` requires non-empty `headers` | `MissingHashKey` |
| `consistentHash.type: Cookie` requires `cookie.name` | `MissingHashKey` |
| Multiple hash sources set | `MultipleHashKeys` |
| `endpointOverride.type: Header` requires non-empty `headers` | `MissingEndpointOverrideKey` |
| `ome.io/*` int that fails `strconv.Atoi` | `InvalidIntValue` |
| `ome.io/*` duration that fails `time.ParseDuration` | `InvalidDuration` |
| `ome.io/retry-attempts > 0` without `ome.io/retry-on` | `MissingRetryOn` |
| Unknown `ome.io/*` key within Levenshtein 2 of a documented key | `UnknownTrafficAnnotation` (Did-you-mean) |
| `ome.io/btp.*` when the active translator is not Envoy Gateway | `UnsupportedPassthrough` |

### Field coverage matrix

Cross-implementation translation is lossy by nature; the matrix bounds the
divergence per translator, and any unsupported field surfaces as a condition
rather than a silent drop.

| Intent | Envoy Gateway | Istio | Notes |
|---|---|---|---|
| `algorithm` (4 values) | yes | yes | |
| `consistentHash.type: Header` | yes (multi-header) | partial (single) | Istio: multi-header → `UnsupportedField` |
| `consistentHash.type: Cookie` | yes | yes | |
| `consistentHash.type: SourceIP` | yes | yes | |
| `endpointOverride.type: Header` | yes | no | Istio: `UnsupportedField` |
| `circuit-breaker-max-connections` | yes | yes (`connectionPool.tcp.maxConnections`) | |
| `circuit-breaker-max-parallel-requests` | yes | yes (`connectionPool.http.http2MaxRequests`) | approximate |
| `circuit-breaker-per-endpoint-max-connections` | yes | no | Istio: `UnsupportedField` |
| `retry-*` | yes | yes (DR `retries`) | |
| `timeout-idle` | yes | yes | |
| `timeout-tcp-connect` | yes | yes | |
| `timeout-max-connection-duration` | yes | no | Istio: `UnsupportedField` |
| Pass-through `ome.io/btp.*` | yes (stitched) | n/a | Envoy Gateway only |
| Pass-through `ome.io/dr.*` | n/a | yes (stitched) | Istio only |

### Observability

**Today, observability is via `TrafficStatus` conditions, not metrics.** The
operator reads `BackendPolicyReady` (and `BackendPolicyUnsupportedFields` when
keys were dropped) on `InferenceService.Status.Traffic`. This is the shipped,
authoritative surface.

The metrics below are **planned but NOT YET IMPLEMENTED** — grep the controller
for any of these names as of 2026-06-04 and you get zero hits. They are listed so
a future implementer has the intended set; do not cite them as existing.

| Metric (TBD) | Labels | Purpose |
|---|---|---|
| `ome_traffic_policy_translated_total` | `isvc,translator,outcome` | Translation outcome per reconcile. |
| `ome_traffic_policy_translation_failures_total` | `isvc,reason` | Error detail for alerting. |
| `ome_traffic_annotation_used_total` | `key` | Promotion criterion (annotation → typed). |
| `ome_traffic_passthrough_field_total` | `translator,field` | Promotion criterion driver. |
| `ome_traffic_unsupported_field_total` | `isvc,translator,field` | Per-field unsupported counter. |
| `ome_traffic_per_component_fallback_total` | `isvc` | Hand-authored per-Component sibling counter. |
| `ome_backend_policy_accepted_total` | `translator,namespace` | Gateway acknowledgments. |
| `ome_backend_policy_rejected_total` | `translator,namespace,reason` | Gateway rejections. |

The seven `ome_rollout_*` metrics this OEP once proposed have **moved out** with
the rollout half. The live cross-Component coordination metrics belong to the
OMENative rollout-group package
(`pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/metrics.go`)
and carry `ome_omenative_rollout_group_*` / `ome_omenative_*` names — not the
`ome_rollout_*` names proposed here. See OEP-0007.1 and OEP-0015.

**Events on the InferenceService** (the implemented surface):
`TrafficPolicyTranslated` (Normal, first emission), `ConflictingTrafficPolicy`
(Warning, same-name manual policy), `UnsupportedTrafficField` (Warning,
translator gap), `NoTranslatorAvailable` (Warning), `BackendPolicyAccepted` /
`BackendPolicyRejected`.

## Superseded: progressive rollout moved to spec.rollout

The first draft of this OEP carried a second part — progressive rollout
strategies (canary, blue-green, rolling) and a per-ISVC step machine on
`spec.traffic.rollout`, with promote/rollback annotation verbs, a `RolloutPhase`
status field, per-revision Services, and PD generation-scoped discovery. That
~400-line design has been **removed from this OEP** to keep the two concerns
separate. It did not vanish — it was relocated and reshaped:

1. **Cross-Component rollout coordination shipped via OEP-0007.1** and lives at
   `spec.rollout.coordination` (`pkg/apis/ome/v1beta1/coordination_types.go`,
   `RolloutCoordinationSpec`). It declares how the Engine / Decoder / Router
   Components of one ISVC roll together — `BlueGreen | RollingUpdate |
   Sequential | Independent`, with `PerComponent` / `RatioBalanced` pacing. This
   is the live, KIND-tested cross-Component coordination surface.

2. **The per-ISVC step machine was consolidated by OEP-0015** and reserved at
   `spec.rollout.canary` (`pkg/apis/ome/v1beta1/rollout_types.go`, `CanarySpec`).
   The shape is fixed — `steps[]` with independent `NewCapacity` / `OldCapacity`
   / `TrafficWeight`, `PromotionPolicy` `Manual | Auto`, per-step `Pause` — so
   enabling it later is not an API migration. But it is **reserved**: setting it
   is rejected at admission ("not yet implemented") because **no step executor
   exists**. A reader will ask: if coordination already does blue-green and
   sequential, why reserve a separate canary field? Honest answer: coordination
   couples *Components within a revision*; canary shifts *traffic between
   revisions over time* — a different axis with no executor yet. The field is
   shape-only precisely so the rollout surface is not forked again later.

3. **`canaryTrafficPercent` was removed** (from
   `pkg/apis/ome/v1beta1` — grep returns zero hits today). The first draft built
   all three rollout strategies on three semantic points of that one field; the
   field is gone, and with it the strategy-via-magic-value scheme.

What this OEP once anchored on Knative is also gone. The `ksvc_reconciler`-as-
canary-producer, the `Serverless` carve-outs, the `knservingv1.TrafficTarget`
status type, and the Serverless test were all dropped with Serverless mode.
Per-Component / per-revision differentiation now re-anchors on **OMENative
per-revision Services** (delivered by OEP-0007.1), not Knative revisions.

For the live progressive-rollout surface, read **OEP-0007.1** (coordination, the
shipped half) and **OEP-0015** (the unified `spec.rollout` block and the reserved
canary). This OEP is backend policy only.

## Opt-in and rollout

- **No Kubernetes feature gate.** Opt-in is per-ISVC by setting `spec.traffic`
  or any `ome.io/*` traffic annotation.
- **Absence preserves today's behavior.** No backend policy is emitted; the
  gateway applies its default load balancing.
- **Rollback** = remove the field/annotation, or revert the controller. There is
  no global kill switch and none is needed: the surface is additive and
  per-ISVC.

## Test Plan

Backend policy is covered by unit tests alongside each package
(`reconcilers/traffic/...`, `pkg/validation/...`,
`reconcilers/ingress/builders/backendtrafficpolicy_builder_test.go`) and by an
envtest integration suite using `tests/integration/dep-crds/envoy-gateway/` for
the EG CRD subset OME translates against.

The scenarios the suite must hold:

1. No traffic intent → HTTPRoutes only, no policy emitted, `Status.Traffic` nil.
2. `algorithm: LeastRequest` → one policy, all HTTPRoutes in `targetRefs`,
   `BackendPolicyReady=True`.
3. `algorithm: ConsistentHash` + `consistentHash.type: Header` + headers →
   policy with the right hash block.
4. `endpointOverride.type: Header` → policy with the endpoint-override block
   (Envoy Gateway); Istio → `UnsupportedField`.
5. Circuit-breaker / retry / timeout annotations → mapped onto the policy;
   `algorithm: Default` in status when none set.
6. Pass-through `ome.io/btp.<path>` → stitched verbatim; the active translator
   echoes the applied keys.
7. ISVC deleted → owned policy garbage-collected.
8. Webhook rejects bad enum, missing/multiple hash keys, malformed annotations,
   unknown annotation (Did-you-mean), `ome.io/btp.*` under a non-Envoy
   translator.
9. Engine HTTPRoute always emitted (even when a router is present); policy
   targets every OME-emitted route.
10. Same-name hand-authored policy → defer, `ConflictingPolicy`. Different-name
    overlap → both attach.
11. Translator factory: Envoy Gateway CRD present → Envoy translator; only Istio
    → Istio; neither → noop with `NoTranslatorAvailable`.

## Graduation Criteria

### Alpha (met)

- Typed core, annotation contract, pass-through, translator factory, and policy
  builder implemented and merged.
- Scenarios 1–11 covered by unit + envtest integration.
- Envoy Gateway and Istio translators implemented; noop fallback surfaces
  `NoTranslatorAvailable`.
- Documentation covers the typed core, annotation contract, pass-through,
  no-feature-gate contract, coexistence behavior, and the per-Component fallback
  via a hand-authored sibling.

### Beta

- The eight `ome_traffic_*` / `ome_backend_policy_*` metrics implemented (the
  promotion criteria for typing annotations and for a per-Component surface
  depend on them).
- Coexistence at phase 2 (admission warning on same-name conflict).
- 2+ production users running ISVCs with OME-managed backend policies ≥ 60 days
  without a high-severity incident attributable to the traffic reconciler.
- Annotation surface frozen except for additive keys.
- Istio translator either matches the Envoy field coverage or remains a
  documented partial with the gaps in the matrix above.

### GA

- 3 months Beta with no regressions.
- Coexistence at phase 3 (admission rejection without `ome.io/managed-by-conflict-acked`).
- Field coverage matrix complete for at least two implementations.

## Implementation History

- 2026-05-15: Initial draft (annotation-first, per-Component, policy only).
- 2026-05-16: Hybrid typed-core + annotation API; ISVC-level scope; merged a
  canary/rollout half into the doc (later removed — see below).
- 2026-05-18: API merged — `TrafficSpec` / `TrafficStatus` + annotation
  constants.
- 2026-05-19: Reconciler merged — backend-policy translator + reconciler +
  Envoy Gateway / Istio / noop + integration tests. Part 1 alpha.
- 2026-06-03: `canaryTrafficPercent` removed from the API.
- 2026-06-04: OEP-0015 unified the rollout surface under `spec.rollout`,
  deleting `spec.traffic.rollout` and reserving `spec.rollout.canary`.
- 2026-06-04: This OEP scoped down to backend traffic policy (Part 1); the
  rollout half marked
  [superseded](#superseded-progressive-rollout-moved-to-specrollout) and
  pointed at OEP-0007.1 / OEP-0015. Status advanced to `implementable`.

## Drawbacks

1. **No per-Component policy differentiation as a typed field.** An operator who
   needs it hand-authors a sibling policy. Documented; a typed surface is a
   beta+ question gated on demand.
2. **Two API surfaces for policy** (typed core + annotation contract). The
   common path is typed; the long tail is annotations. The split is deliberate
   but is a thing operators must learn.
3. **Pass-through can be abused.** An operator can stay on `ome.io/btp.*` forever
   and never request typing. Documentation calls it unsupported; the usage
   metric that would make staleness measurable is not yet implemented.
4. **Cross-implementation translation is lossy.** The field coverage matrix
   bounds the divergence per translator; unsupported fields surface as
   conditions, not silent drops.
5. **Coexistence has an upgrade cost.** Operators with same-name hand-authored
   policies migrate over the three-phase deprecation window.
6. **Observability is condition-only today.** Until the `ome_traffic_*` metrics
   land, fleet-wide aggregation (how many ISVCs use ConsistentHash? how often do
   translations fail?) requires scraping conditions, not a counter.

## Alternatives

1. **Pure typed fields, no annotations.** Every long-tail knob requires a CRD
   bump. Rejected: the long tail is large and changes per gateway
   implementation.
2. **Pure annotations, no typed core.** No enum validation, no `kubectl
   explain`. Rejected for the common path — and this was the rejected
   annotation-first framing of the first draft.
3. **A new `InferenceServiceTrafficPolicy` CRD.** Adds a user-visible CRD against
   OEP-0007 philosophy. Rejected.
4. **Generate the policy from defaults on `ServingRuntime`.** Couples policy to
   the runtime author — the wrong layer. Rejected.
5. **Build on Gateway API `BackendLBPolicy` (GEP-1619).** A standardized
   attachment point, but the current GEP covers **only session persistence**
   (cookie/header) — LB algorithm, circuit breaker, retries, and connection
   timeouts are still implementation-specific. Tradeoff: adopting it now buys one
   field's worth of standardization and a per-implementation translator for
   everything else. Track upstream; migrate the session-persistence portion once
   Envoy Gateway and Istio both make `BackendLBPolicy` canonical, and revisit
   the rest if a future GEP extends its scope.

## Open Questions

1. **Metrics.** The eight `ome_traffic_*` / `ome_backend_policy_*` counters are
   designed but not implemented. Owner + timing TBD — they gate the beta
   promotion criteria (typing annotations, deciding on a per-Component surface).
2. **Multiple Gateway implementations in one cluster.** The factory picks the
   first detected in priority order (Envoy Gateway > Istio). If a real cluster
   needs both active at once, add an `ome.io/traffic-policy-translator` override
   annotation. TBD — no such cluster observed yet.
3. **`BackendLBPolicy` (GEP-1619) scope.** Whether a future GEP extends the
   resource to cover LB algorithm, circuit breaker, and retries determines
   whether OME can eventually emit a fully upstream-standardized backend policy
   or stays tied to per-implementation translators. Track upstream.
4. **Rate limiting and backend TLS.** Envoy Gateway supports `RateLimit` via the
   policy; `BackendTLSPolicy` is a separate Gateway API resource. Both are
   deferred to follow-up OEPs, not this surface.
5. **Per-revision policy override.** A different LB algorithm on a canary pool vs
   the stable pool is a hand-authored sibling today. Whether it becomes a typed
   field depends on the progressive-rollout work in OEP-0015 landing an executor
   first — until there are two revision pools, the question is moot.
