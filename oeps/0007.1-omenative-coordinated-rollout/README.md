# OEP-0007.1: OMENative Coordinated Rollout

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [What Coordination Buys You](#what-coordination-buys-you)
  - [The Five Rollout Scenarios](#the-five-rollout-scenarios)
  - [Division of Responsibility with OEP-0011](#division-of-responsibility-with-oep-0011)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Conceptual Overview](#conceptual-overview)
  - [Scenario to API Mapping](#scenario-to-api-mapping)
  - [User Stories](#user-stories)
  - [Risks and Mitigations](#risks-and-mitigations)
- [API](#api)
  - [The `rolloutCoordination` Block](#the-rolloutcoordination-block)
  - [Coordination Policies](#coordination-policies)
  - [Pacing](#pacing)
  - [Defaults](#defaults)
- [Design Details](#design-details)
  - [Per-Revision Services](#per-revision-services)
  - [Generation-Scoped Discovery](#generation-scoped-discovery)
  - [`Status.Components.*.Traffic[]` Producer](#statuscomponentstraffic-producer)
  - [Controller State Machine](#controller-state-machine)
  - [The Sequential Gate](#the-sequential-gate)
  - [The Dispatcher's Requeue Contract](#the-dispatchers-requeue-contract)
  - [In-Place Upgrade and the Revision Label](#in-place-upgrade-and-the-revision-label)
  - [Failure Handling](#failure-handling)
  - [Migration Verb Integration](#migration-verb-integration)
  - [Webhook Validation](#webhook-validation)
  - [Defaulter Behavior](#defaulter-behavior)
  - [Observability](#observability)
- [Backwards Compatibility](#backwards-compatibility)
- [Coexistence with OEP-0011 Step-Based Rollouts](#coexistence-with-oep-0011-step-based-rollouts)
- [Recipes](#recipes)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Issues](#open-issues)
- [Open Questions](#open-questions)
- [Implementation History](#implementation-history)
<!-- /toc -->

## Summary

OEP-0007.1 extends OMENative (OEP-0007) with cross-Component rollout
coordination. It exists because, for a prefill-decode workload, "each
Component rolls independently" silently produces mixed-version
traffic during the rollout window: a v2 engine pod calling a v1
decoder pod, KV-cache shapes mismatched, requests silently corrupted.

The principles:

1. **Coordination is about cross-Component invariants** —
   keeping prefill/decode ratios in band, never surging two
   Components at once when capacity is tight, never letting the
   engine roll ahead of the decoder when the operator ordered it that
   way.
2. **The state machine is the contract.** Each coordination group
   walks a single state machine (`Idle → Surging → Waiting →
   Shifting → Draining → ScalingDown → Idle`); every gate that
   constrains a per-Component operation reads from that state
   machine. Everything else is implementation detail.
3. **Soft-fail over hard-fail.** When something is misconfigured the
   controller surfaces a condition; it doesn't refuse to deploy.
   Operators always have a way out.
4. **Coordination policy and update strategy compose.** They are
   orthogonal axes. The OEP makes the matrix obvious; it does not
   bury it in prose.

The OEP introduces no new user-visible CRDs. One new optional field
on `InferenceServiceSpec` — `spec.rolloutCoordination.groups[]` —
plus the controller plumbing (per-revision Services,
environment-variable-based generation-scoped discovery, the
`Status.Components.<c>.Traffic[]` writer) needed to make coordinated
rollouts safe.

Five rollout scenarios are expressible by combining one of four
`policy` values (BlueGreen / Independent / RollingUpdate / Sequential)
with one of two `pacing.type` values (PerComponent / RatioBalanced):

1. **BlueGreen three-way** — router + engine + decoder roll as one
   unit.
2. **All independent** — each Component rolls on its own (today's
   v1 default; preserved as v1.1 default for back-compat).
3. **RollingUpdate** — engine + decoder roll in lockstep but any vN
   engine pod can talk to any vM decoder pod (operator asserts
   protocol back-compat).
4. **Ratio-aware pacing** — orthogonal to 1–3; for asymmetric
   topologies (e.g., 40 prefill : 20 decode), enforces the original
   ratio across the rollout.
5. **Sequential / phased** — decoder rolls fully to 100% on the new
   revision, soaks for as long as the operator wants, only then does
   engine begin its own rollout cycle.

OEP-0007.1 is the **workload-coordination** half of the rollout story;
[OEP-0011](../0011-rollout-traffic-strategies/README.md) owns the
**traffic-policy** half (what percentage goes where, step plans,
blue-green semantics). The two compose orthogonally — any OEP-0011
traffic strategy works with any OEP-0007.1 coordination policy. See
[Recipes](#recipes) for the named operational patterns.

OEP-0007.1 lands after OEP-0007 v1. Existing OMENative
`InferenceService`s upgrade with zero behavior change — coordination
is opt-in via explicit `groups[]` declaration.

OEP-0007.1 also satisfies the OEP-0011 Part 2 alpha-gate contract: it
populates `Status.Components.<c>.Traffic[]` with revision-pair weights
and creates the per-revision Services that OEP-0011's HTTPRoute
weighted-backendRef consumer reads from.

## Motivation

### What Coordination Buys You

OEP-0007's §Coordinated Cross-Component Rollout is explicit that
single-Component correctness ships first and "richer cross-Component
coordination can layer on later." This OEP is the "later." It
addresses the deferred concerns without altering the OEP-0007 v1
controller kernel — coordination plugs in as an additional state
machine layer on top of the existing per-Component reconciliation.

Real prefill-decode deployments need cross-Component rollout
coordination because prefill and decode pods talk to each other
internally during inference: the router fans out to prefill, prefill
streams partial KV state to decode, decode generates tokens.
Mid-rollout, "v2 engine talks to v1 decoder" can mean:

- KV-cache layout incompatibility producing silent garbage outputs
- gRPC/protobuf schema drift producing connection failures (visible)
  or field-level corruption (silent)
- Tensor-shape mismatch producing a segfault on the decoder side

The OEP-0011 producer contract closes the same time. OEP-0011 Part 2
assigns the producer side of the canary contract to per-mode
reconcilers — materialize per-revision pod sets and per-revision
Services, ensure generation-scoped internal discovery, populate
`Status.Components[*].Traffic[]`, detect Ready timeouts. OEP-0011's
HTTPRoute weighted-backendRef consumer (shipped earlier) reads
`Status.Components[*].Traffic[]` and emits weighted backendRefs to
per-revision Service names. Today it has nothing to consume because
no per-mode reconciler produces those entries. OEP-0007 v1 is silent
on the producer contract. OEP-0007.1 fills both gaps in one design.

### The Five Rollout Scenarios

#### 1. BlueGreen three-way rollout

Router + engine + decoder roll together. Surge new pods for all
three; wait for all three new sets to be Ready; atomically switch
traffic. If any one fails, roll back all three.

Slowest. Safest. The default for operators who don't want to think
about coordination — declare the group, let the controller do the
gang.

#### 2. All independent (today's v1 default)

Each Component rolls on its own. Fastest. The operator asserts that
the router doesn't need to match engine/decoder versions (true —
router is stateless and version-agnostic) and that the engine doesn't
need to match the decoder (true only when the protocol is
back-compat). Risky for prefill-decode without back-compat protocol;
preserved as the v1.1 default for back-compat. Explicit `groups[]`
declaration is the way to upgrade.

#### 3. Engine + decoder RollingUpdate, router independent

Engine and decoder roll in lockstep at the **spec level** (operator
bumps both image tags in one apply), but during the rollout window
any vN engine pod is allowed to talk to any vM decoder pod. Router
rolls on its own.

Operationally common when engine/decoder are co-released by the
runtime team with guaranteed protocol back-compat across minor
versions, and the router is on a different cadence (CPU pod,
dependency-only changes). Looser than BlueGreen, safer than fully
Independent.

#### 4. Ratio-aware pacing

Asymmetric prefill-decode topologies are common: 40 prefill
(compute-bound) talking to 20 decode (memory-bound) at a 2:1 ratio.
A naive rollout that surges 25% of prefill (10 pods) and 25% of
decode (5 pods) in parallel can produce momentary 2.5:1 skew if the
prefill surge completes first — 10 new prefill pods routing to 0 new
decode pods, starvation.

Ratio-aware pacing computes the original cross-Component ratio at
rollout start and refuses to let the rollout drift past it. Orthogonal
to the policy enum: ratio-aware applies to BlueGreen and RollingUpdate
groups (Independent groups by definition don't track cross-Component
ratios).

#### 5. Sequential / phased

Decoder rolls fully to 100% on the new revision and stays there for
an operator-controlled soak window. Only after the operator is
satisfied does engine begin its own rollout cycle. The two
Components' rollouts are strictly serialized along the operator-pinned
order.

This is the staged-validation pattern: the operator wants to prove
the new decoder under real production traffic for an hour (or a day,
or a week) before touching engine. BlueGreen is too coupled — it
forces both to complete in the same window. Independent is too loose
— engine and decoder both start rolling the moment the spec bumps
and end up mid-rollout simultaneously. Sequential is the missing
middle ground:

- Strict ordering: engine literally cannot enter Surging until
  decoder is Idle on the new revision.
- Operator gates each phase explicitly. If they want engine to never
  start, they leave the engine spec unchanged — Sequential only
  advances on per-Component spec bumps in the declared order.
- Composes with OEP-0011 step plans: each Component walks the full
  step plan (e.g., 10 → 50 → 100 with manual promotions) before the
  next Component starts its plan.
- Failure in the early Component blocks all later Components.

Different from BlueGreen with a pinned surge order (which still
traffic-shifts the whole gang together) — Sequential walks the full
lifecycle per Component, with a wait at each step for the operator's
confidence.

### Division of Responsibility with OEP-0011

OEP-0011 is **per-Component-shaped** — it answers "what percentage of
traffic goes to which revision and on what schedule." OEP-0007.1 is
**across-Components-shaped** — it answers "which Components must
coordinate the answer, in what order, with what coupling."

| Concern | OEP-0011 | OEP-0007.1 |
|---|---|---|
| What percentage of traffic goes to the new revision | `canaryTrafficPercent` (simple) or `spec.traffic.rollout.steps[].trafficWeight` (step form) | — |
| When to advance / pause / promote | `spec.traffic.rollout.steps[].pause`, `ome.io/rollout-promote` | — |
| Per-Component rollout state | `Status.Components.<c>.RolloutPhase` | — |
| Step plan + manual approval | `spec.traffic.rollout.promotionPolicy` | — |
| HTTPRoute weighted backendRefs | Consumer (shipped) | — |
| **Which Components roll together** | OEP-0011 asserts "PD components coupled" but doesn't implement it | `policy: BlueGreen` enforces it |
| **What order Components roll** | — | `order:` (Sequential) or surge order (BlueGreen) |
| **Cross-Component pacing / ratio** | — | `pacing.type: RatioBalanced` |
| Per-revision pod sets + Services | Explicit out-of-scope | `<isvc>-<c>-rev-<hash>` Services |
| Generation-scoped peer discovery | Out-of-scope | Env-var injection |
| `Status.Components.<c>.Traffic[]` writes | Consumer side | Producer side |

The key insight: OEP-0011's "PD components coupled" guarantee only
becomes real when something enforces the coupling at the workload
layer. OEP-0007.1 is what makes it real. Without OEP-0007.1,
OEP-0011's `canaryTrafficPercent` on a `PDDisaggregated`
`InferenceService` happily lets engine and decoder canary at different
percentages, which violates OEP-0011's own design intent.

The composition rule: OEP-0011's surface defines the **step plan**
(what percentage, when, with what pauses). OEP-0007.1's `groups[]`
defines the **Component coupling** (which Components walk the plan
together, sequentially, RollingUpdate, or independently). Any
OEP-0011 traffic strategy composes with any OEP-0007.1 coordination
policy.

### Goals

1. Address the five rollout scenarios with one extensible API on the
   `InferenceService`. Operators express coordination intent
   declaratively; the controller picks the right state machine.
2. Close OEP-0011's Part 2 producer contract for OMENative —
   per-revision Services, generation-scoped discovery, and
   `Status.Components.<c>.Traffic[]` writes — so OEP-0011 Part 2
   alpha-gate tests pass.
3. Backwards compatible with OEP-0007 v1. Default behavior is
   unchanged for `InferenceService`s that don't declare
   `rolloutCoordination`.
4. Zero new user-visible CRDs. Inherits OEP-0007's hard constraint.
5. No Kubernetes feature gate. Opt-in is per-`InferenceService`, not
   per-cluster.
6. Reuse OEP-0007 primitives: the four-level hierarchy, the
   `ControllerRevision` stream, the `ome.io/serving` readiness gate,
   the migration verb. OEP-0007.1 adds a layer; it doesn't replace
   the kernel.
7. Compose with OEP-0011 step-based rollouts. Coordination policy is
   orthogonal to the simple vs step-based rollout form.

### Non-Goals

1. Not a feature gate.
2. Not changing OEP-0007 v1's API.
3. Not solving exotic non-prefill-decode multi-Component cases
   beyond the five scenarios. Such patterns are operator-expressible
   via the `groups[]` block but not exhaustively tested.
4. Not changing in-place upgrade eligibility. OEP-0007's
   eligible-fields list is preserved; OEP-0007.1 uses the
   labels-eligible rule to mutate `ome.io/revision-hash` during
   in-place upgrades that change the revision.
5. Not auto-defaulting BlueGreen. Backwards-compat means Independent
   stays the default; BlueGreen is operator-declared.
6. Not implementing rollback verbs. Operator triggers rollback by
   reverting the spec; the controller drives it through the same
   state machine in reverse.
7. Not solving cross-cluster coordination.
8. Not implementing the controller-side step state machine for
   OEP-0011's `spec.traffic.rollout` step form. The operator-facing
   API exists in OEP-0011; the controller side is OEP-0011's
   responsibility. OEP-0007.1's groups[] applies to the underlying
   coordination regardless of which rollout form triggers it.

## Proposal

### Conceptual Overview

```
InferenceService
├── spec.router         (existing)
├── spec.engine         (existing)
├── spec.decoder        (existing)
├── spec.rolloutCoordination          (NEW — this OEP)
│   └── groups[]
│       ├── components: [...]         (which Components are coordinated)
│       ├── policy: BlueGreen | Independent | RollingUpdate | Sequential
│       └── pacing: { type, maxSurge, maxUnavailable }

                  │
                  ▼
   ┌──────────────────────────────────────────────────────┐
   │  OMENative coordination layer                        │
   │  ├── resolves groups[] → coordination state machine  │
   │  ├── creates per-revision Services per Component     │
   │  ├── injects OME_<PEER>_ENDPOINT env vars per pod    │
   │  ├── walks the per-policy reconcile loop             │
   │  └── writes Status.Components.<c>.Traffic[]          │
   └──────────────────────────────────────────────────────┘
                  │
                  ▼
   OEP-0011 HTTPRoute builder (shipped) reads
   Status.Components.<c>.Traffic[] and emits weighted backendRefs
   pointing at the per-revision Services this OEP creates.
```

Two layers cooperate:

- **Per-Component reconciliation** (OEP-0007 v1) owns the
  Component's pods, ControllerRevisions, in-place vs recreate
  decision, drain, restart policy.
- **Coordination layer** (this OEP) owns cross-Component ordering,
  ratio enforcement, atomic gang surge/wait, and the
  `Status.Traffic[]` writes.

The coordination layer is a thin orchestrator over the per-Component
reconcilers. It does not own pods directly; it constrains when and
how the per-Component reconcilers fire.

### Scenario to API Mapping

| Operator intent | YAML |
|---|---|
| (1) BlueGreen three-way | `groups: [{components: [router, engine, decoder], policy: BlueGreen}]` |
| (2) All independent | `rolloutCoordination` omitted (default), or `groups: []` (explicit empty) |
| (3) Engine + decoder RollingUpdate, router independent | `groups: [{components: [engine, decoder], policy: RollingUpdate}]` (router unlisted → independent) |
| (4) Ratio-aware | Any group with `pacing: { type: RatioBalanced }` |
| (5) Sequential (decoder fully done before engine) | `groups: [{components: [decoder, engine], policy: Sequential, order: [decoder, engine]}]` |
| BlueGreen + ratio-aware | `groups: [{components: [engine, decoder], policy: BlueGreen, pacing: {type: RatioBalanced, maxSurge: 25%}}]` |
| Sequential + per-step pacing within each phase | `groups: [{components: [decoder, engine], policy: Sequential, order: [decoder, engine]}]` plus `spec.traffic.rollout.steps: [...]` (each Component walks the step plan before the next starts) |
| Multiple groups in one `InferenceService` | `groups: [{components: [engine, decoder], policy: RollingUpdate}, {components: [router], policy: BlueGreen}]` (rare; router-alone BlueGreen is equivalent to Independent for a single-Component group, but allowed for symmetry) |

### User Stories

#### Story 1 — BlueGreen three-way for safety

A prefill-decode `InferenceService` with router + engine + decoder.
The operator hasn't validated that engine v2 talks safely to decoder
v1 and doesn't want to think about it. They declare BlueGreen:

```yaml
spec:
  rolloutCoordination:
    groups:
      - components: [router, engine, decoder]
        policy: BlueGreen
        pacing: { maxSurge: 25% }
```

The operator bumps engine and decoder image tags via PR. The
controller surges 25% new pods for all three Components in parallel,
waits for all to be Ready, shifts traffic atomically, drains old
pods, repeats. If any one Component fails to reach Ready within
timeout, all three roll back.

Operator-visible state:

```yaml
status:
  rolloutCoordinationStatus:
    groups:
      - name: "0"
        components: [router, engine, decoder]
        phase: Surging
        observedSurge: 25%
        observedRatio:
          original: { router: 1, engine: 40, decoder: 20 }
          current:  { router: 1, engine: 40, decoder: 20 }
          newPods:  { router: 0, engine: 10, decoder: 5 }
```

#### Story 2 — RollingUpdate for runtime co-released bumps

The runtime team co-releases engine and decoder with guaranteed
back-compat. Router is on a different cadence:

```yaml
spec:
  rolloutCoordination:
    groups:
      - components: [engine, decoder]
        policy: RollingUpdate
        pacing: { type: RatioBalanced, maxSurge: 25% }
    # router not listed → rolls independently
```

The operator bumps engine and decoder image tags in one PR. The
controller surges new pods proportionally (maintaining the 2:1 ratio
for a 40:20 topology). During the rollout window the HTTPRoute fans
engine traffic to both v1 and v2 engine pods, and decoder traffic to
both v1 and v2 decoder pods. Cross-version calls are allowed because
the operator asserted protocol back-compat. Router updates happen on
a separate PR, rolling independently.

#### Story 3 — Sequential for staged validation

A prefill-decode `InferenceService` with engine + decoder. The
operator is rolling a new decoder runtime that has rewritten the
KV-cache layout. They want to prove decoder v2 in production for at
least 24 hours before letting engine touch its v2 image:

```yaml
spec:
  rolloutCoordination:
    groups:
      - components: [decoder, engine]
        policy: Sequential
        order: [decoder, engine]
        pacing: { maxSurge: 25% }
  traffic:
    rollout:
      steps:
        - trafficWeight: 10
          pause: {}
        - trafficWeight: 50
          pause: { duration: 30m }
        - trafficWeight: 100
      promotionPolicy: Manual
```

Day 0 at 09:00 the operator bumps the decoder image. Engine spec is
unchanged.

- Decoder enters `Surging`. New-revision decoder pods come up at 25%
  surge.
- Decoder enters `Shifting` at step 1 (10% traffic, paused).
- Operator promotes step 1 → step 2 (50% traffic, 30-minute
  auto-pause).
- 30 minutes later, step 2 auto-advances to step 3 (100% traffic).
- Decoder enters `Draining → ScalingDown → Idle`. Decoder v2 serves
  100%.
- Engine is not allowed to surge during any of this even if its spec
  is bumped — Sequential blocks until decoder is Idle.

Day 1 at 09:00 (after a 24-hour soak) the operator bumps the engine
image. Engine walks the same step plan to Idle.

```yaml
status:
  rolloutCoordinationStatus:
    groups:
      - name: "0"
        components: [decoder, engine]
        order: [decoder, engine]
        phase: Sequential.AwaitingNextComponent
        currentComponent: engine          # next in order
        previousComponent: decoder        # most recently completed
```

If decoder v2 had failed mid-rollout, engine would never have entered
Surging. The operator can either revert the decoder spec (Sequential
walks decoder back through RollingBack) or accept the failure and
revert engine too.

#### Story 4 — Migration verb in a coordinated group

Alfred wants to defragment Node3 by moving one engine Instance to
another node. Engine is in a BlueGreen group with decoder. The
migration verb `Migrate(component=engine, instance=0)` is rejected
with `MigrationRequiresGroupMigration` — moving one Component's
Instance independently would break the BlueGreen invariant. Alfred
either falls back to a different node-drain strategy or triggers a
full-group migration (planned for Alfred's next iteration). For
Independent or RollingUpdate groups, single-Component migration
proceeds as in OEP-0007 v1.

### Risks and Mitigations

**The coordination state machine adds reconcile-loop complexity.**
Mitigation: keep the per-Component reconcilers entirely unaware of
coordination. The coordination layer constrains them via labels and
status writes; it never reaches into their state. Each layer remains
unit-testable independently.

**RollingUpdate's "cross-version is safe" assertion may be wrong.**
The operator declares `policy: RollingUpdate` expecting protocol
back-compat; a runtime regression breaks it; prefill-decode requests
silently corrupt. Mitigation: log per-Component image hashes and emit
a `MixedPairingObserved` metric when distinct revision pairs are seen
in flight, so observability surfaces the assertion at operator review
time. Operators can flip to BlueGreen if they don't trust the
assertion.

**Ratio-aware pacing math is fiddly for non-integer ratios.** 40:20
= 2:1 is clean. 11:7 is not. Mitigation: ratio is enforced as
"don't let new-pod-count / old-pod-count drift past the original
ratio + tolerance" with a small default tolerance (5%) tunable via
`pacing.ratioTolerance`.

**Per-revision Services proliferate.** During a multi-revision
rollout an `InferenceService` may have 4–8 active per-revision
Services. Mitigation: GC tied to OMENative's existing revision
retention policy (`ome.io/revision-history-limit`, default 3). A
per-revision Service exists only as long as at least one pod carries
the matching label.

**In-place upgrades change pod revision labels mid-flight.**
OEP-0007 v1's in-place upgrade is per-Component; OEP-0007.1's
coordination layer must observe label flips correctly. Mitigation:
the coordination layer's `Waiting` phase blocks on
`pods.readyCount(rev=new) >= expected`; the controller updates label
and image as one patch so the transition is atomic from the
apiserver's perspective.

## API

### The `rolloutCoordination` Block

New optional field on `InferenceServiceSpec`:

```go
type InferenceServiceSpec struct {
    // ... existing fields ...

    // RolloutCoordination declares cross-Component rollout coupling
    // for OMENative-managed Components. Each group lists the
    // Components that roll together and the policy that governs
    // their coordination. Components not listed in any group roll
    // independently (the default).
    //
    // Only meaningful when at least one referenced Component uses
    // ome.io/deploymentMode=OMENative. Webhook rejects groups that
    // reference non-OMENative Components.
    // +optional
    RolloutCoordination *RolloutCoordinationSpec `json:"rolloutCoordination,omitempty"`
}

type RolloutCoordinationSpec struct {
    // Groups is the list of coordination groups. Each group binds a
    // set of Components under one policy. A Component may appear in
    // at most one group; webhook rejects multi-membership.
    // +optional
    // +listType=atomic
    Groups []RolloutCoordinationGroup `json:"groups,omitempty"`
}

type RolloutCoordinationGroup struct {
    // Components lists the Component names included in this group.
    // Valid values: "router", "engine", "decoder". Order is not
    // significant unless `order` is set.
    // +kubebuilder:validation:MinItems=1
    // +listType=atomic
    Components []ComponentType `json:"components"`

    // Policy selects the coordination semantic for this group.
    // +kubebuilder:validation:Enum=BlueGreen;Independent;RollingUpdate;Sequential
    Policy CoordinationPolicy `json:"policy"`

    // Pacing controls how surge / drain pacing is computed within
    // the group. When omitted, each Component uses its own
    // omenative.updateStrategy.rollingUpdate.
    // +optional
    Pacing *CoordinationPacing `json:"pacing,omitempty"`

    // Order has two meanings depending on Policy:
    //   - BlueGreen: pins the surge sequence within the single
    //     rollout unit, but all Components traffic-shift together
    //     once every set is Ready. Optional; default no ordering.
    //   - Sequential: REQUIRED. Pins the order in which Components
    //     walk their full state machine. The next Component cannot
    //     enter Surging until the previous Component is back at
    //     Idle on the new revision.
    //   - Independent / RollingUpdate: ignored.
    // +optional
    // +listType=atomic
    Order []ComponentType `json:"order,omitempty"`
}

type CoordinationPolicy string

const (
    CoordinationPolicyBlueGreen      CoordinationPolicy = "BlueGreen"
    CoordinationPolicyIndependent    CoordinationPolicy = "Independent"
    CoordinationPolicyRollingUpdate  CoordinationPolicy = "RollingUpdate"
    CoordinationPolicySequential     CoordinationPolicy = "Sequential"
)

type CoordinationPacing struct {
    // Type selects the pacing computation. PerComponent (default)
    // makes each Component use its own omenative pacing
    // independently; RatioBalanced surges proportionally across all
    // Components in the group, never letting the cross-Component
    // ratio drift past the original.
    // +kubebuilder:validation:Enum=PerComponent;RatioBalanced
    // +optional
    Type CoordinationPacingType `json:"type,omitempty"`

    // MaxSurge is the maximum fraction (or absolute count) of pods
    // OMENative may add above the desired Component replica count
    // during a coordinated rollout. Applies group-wide. Default 25%.
    // +optional
    // +kubebuilder:validation:XIntOrString
    MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty"`

    // MaxUnavailable is the maximum fraction (or absolute count) of
    // pods that may be not-Ready at once during the rollout. Applies
    // group-wide. Default 0 (no Component drops below MinReplicas).
    // +optional
    // +kubebuilder:validation:XIntOrString
    MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

    // RatioTolerance is the percentage drift from the original
    // cross-Component ratio that RatioBalanced allows before
    // refusing further surge. Default 5.
    // +optional
    // +kubebuilder:validation:Minimum=0
    // +kubebuilder:validation:Maximum=100
    RatioTolerance *int32 `json:"ratioTolerance,omitempty"`
}

type CoordinationPacingType string

const (
    CoordinationPacingPerComponent  CoordinationPacingType = "PerComponent"
    CoordinationPacingRatioBalanced CoordinationPacingType = "RatioBalanced"
)
```

### Coordination Policies

**BlueGreen.** All Components in the group surge, wait-ready, shift
traffic, drain, and scale down **together**. The controller treats
the group as a single rollout unit:

- Surge new-revision pods for every Component up to the group's
  `maxSurge` (subject to `RatioBalanced` if set).
- Block all Components' traffic shifts until every Component's
  new-revision pods are Ready.
- Shift traffic on all Components simultaneously.
- Block scale-down of any Component's old-revision pods until every
  Component's new-revision pods are still Ready (handles flap).
- If any Component enters Failed, the entire group enters Failed;
  all in-flight surges are reverted.

`order:` (optional, default no ordering) pins the surge sequence
within BlueGreen: e.g., decoder surges first, waits for new pods to
be Ready, then engine surges, then router. Useful when upstream peers
depend on downstream peers being available in the new revision before
they surge.

**Independent.** Each Component rolls on its own per OEP-0007 v1
semantics. The group declaration is essentially documentation — it
asserts that the operator has thought about it and decided
coordination isn't needed. Controller behavior is identical to
omitting the group.

**RollingUpdate.** Components must be at the same **spec
generation** (operator bumps engine and decoder image tags in the
same PR), but during the rollout window any vN pod can talk to any
vM pod within the group. The controller:

- Enforces spec-generation lockstep via admission (engine and decoder
  must both bump in the same generation if either bumps).
- Surges new-revision pods per Component as per-Component pacing
  allows, in parallel.
- Routes traffic via the **generic** Service (not per-revision) so
  cross-revision peer calls are allowed.
- Drains old-revision pods per Component as per-Component pacing
  allows.
- Reports cross-pairing observability (`MixedPairingObserved` metric)
  so operators can audit that their back-compat assertion held.

RollingUpdate is the common case for runtime co-release patterns
(SGLang/vLLM ship engine + decoder together with protocol back-compat
across minor versions).

**Sequential.** Components walk their full state machine **one at a
time**, in the strict order pinned by `order:`. The next Component
in the order cannot enter Surging until the previous Component is
back at Idle on the new revision (i.e., its full
Surging → Waiting → Shifting → Draining → ScalingDown cycle has
completed).

Concretely for `policy: Sequential, order: [decoder, engine]`:

1. Operator bumps the decoder image to v2. Decoder's coordination
   group enters Surging. Engine is **not** eligible to surge (even
   if engine was also bumped in the same PR) until decoder reaches
   Idle.
2. Decoder walks Surging → Waiting → Shifting → Draining →
   ScalingDown → Idle. During this window, 100% of decoder traffic
   shifts to v2 per the OEP-0011 step plan in effect.
3. Operator observes decoder under prod load for as long as they want
   — minutes, hours, or days. Sequential makes no time guarantee.
4. Operator bumps the engine image. Engine's group enters Surging
   and walks the same cycle to Idle.

`order` is required for Sequential — admission rejects empty or
omitted `order` with `SequentialRequiresOrder`. Admission also
rejects `order` that doesn't cover every Component in `components`
with `SequentialIncompleteOrder`.

**Failure blocks downstream Components.** If decoder enters Failed
(timeout on Ready, etc.), engine never starts — the operator must
either roll decoder back to a working state (Sequential walks back
through `RollingBack` for the failed Component) or revert the engine
spec change too.

**OEP-0011 composition.** Sequential composes with
`spec.traffic.rollout.steps[]` per-Component: each Component walks
the full step plan (e.g., 10 → 50 → 100 with manual promotions)
before the next Component starts its plan. Operators get nested
gating: per-step manual promotion within each Component, plus
operator-controlled soak between Components.

Use Sequential when staged validation under real production traffic
is required before letting the next Component touch the new revision,
when the protocol coupling between Components is strong enough that
RollingUpdate's "any vN talks to any vM" isn't safe, or when the
rollout window is allowed to span days.

### Pacing

**PerComponent** (default when `pacing` is omitted). Each Component's
existing OEP-0007 `omenative.updateStrategy.rollingUpdate.maxUnavailable`
and `partition` apply independently. The coordination policy still
governs ordering and gang-readiness, but the surge math is computed
per-Component, not coupled across the group.

The coordination-specific `pacing.maxSurge` is the **group-wide
surge ceiling** that this OEP adds on top — OEP-0007's
`rollingUpdate` has no `maxSurge` of its own because per-Component
rolling updates don't surge above declared replica count. Coordination
needs a surge budget to bring new-revision pods up before old ones
drain. Default is 25% of the largest Component in the group.

Use PerComponent when Component replica counts are similar (so
independent pacing doesn't produce skew) or when BlueGreen is used
primarily for gang-readiness rather than ratio enforcement.

**RatioBalanced.** The controller computes the original ratio of
`desiredReplicas` across Components in the group at rollout start
(e.g., 40:20 = 2:1). During the rollout it refuses to surge new pods
for one Component if doing so would skew the new-vs-old ratio of any
other Component past `(originalRatio × (1 ± ratioTolerance%))`.

Concretely for 40 engine : 20 decoder with `maxSurge: 25%` and
`ratioTolerance: 5`:

- Original ratio: engine:decoder = 2:1
- Tolerance band: 1.9:1 to 2.1:1
- Controller surges up to 10 new engine pods and up to 5 new decoder
  pods in parallel (maintains 2:1 exactly).
- Controller refuses surging an 11th engine pod while decoder still
  has 5 new pods (would skew to 2.2:1, outside the band).
- Controller proceeds: when decoder surges its 6th new pod, engine
  can advance to 12 new pods (still 2:1).

This prevents the "40 new engine, 5 new decoder, starvation"
pathology. The math falls out cleanly because the coordination layer
is the only writer of new-pod counts within a RatioBalanced group.

### Defaults

- `rolloutCoordination` omitted is equivalent to `groups: []`. All
  Components roll independently. **This is the v1.1 default**,
  preserving OEP-0007 v1 behavior.
- `groups[]` declared with no `pacing`: each group uses
  `pacing.type=PerComponent` with `maxSurge=25%` and
  `maxUnavailable=0`.
- `policy: BlueGreen` with no `order`: Components surge in parallel;
  no intra-group sequencing.

No auto-detection. Operators who want BlueGreen must declare it.
Discoverability comes via the quickstart and a hint event
`ConsiderCoordinationGroup` emitted on multi-Component
`InferenceService`s without any declared group.

## Design Details

### Per-Revision Services

For every active per-Component revision (every revision hash carried
by at least one pod), the OMENative controller maintains two
Services:

| Service | Name pattern | Type | Selector | Purpose |
|---|---|---|---|---|
| Per-revision routing | `<isvc>-<component>-rev-<hash>` | ClusterIP | `ome.io/inferenceservice=<isvc>, ome.io/component=<c>, ome.io/revision=<hash>` | HTTPRoute backend target; in-cluster addressable for direct revision targeting |
| Per-revision headless | `<isvc>-<component>-rev-<hash>-headless` | Headless | same | Pod-level DNS within the revision (multi-node tensor-parallel groups need per-pod addressability among the leader + workers of one revision) |

These coexist with OEP-0007's existing three-Service-per-Component
model — they are additive. The OEP-0007 Services are revision-agnostic
(select all live pods regardless of revision); the new per-revision
Services are revision-scoped.

Lifecycle: created when any pod with that revision hash is created
(idempotent — Get first, Create on NotFound); owned by the
`InferenceService` (controller-ref + GC); garbage-collected when no
pod carries the revision hash AND OMENative's existing revision
retention (`revision-history-limit`, default 3) has expired the
revision. The two-condition gate avoids deleting Services that the
HTTPRoute weighted-backendRef might briefly still reference during a
rollout completion.

Hash computation reuses OEP-0007's existing `ControllerRevision`
hash — the same value labeled onto pods as `ome.io/revision-hash`.
No new hash function.

A single-revision Component carries 2 per-revision Services (routing
+ headless). During a rollout with 2 active revisions that's 4
per-revision Services per Component, plus the 3 revision-agnostic
Services OMENative already creates. For a prefill-decode
`InferenceService` mid-rollout: router + engine + decoder =
3 Components × (3 existing + 4 per-rev) = 21 Services peak.
Acceptable — Services are cheap.

### Generation-Scoped Discovery

The OMENative controller injects two environment variables per (pod,
peer-Component) pair at pod creation time:

| Env var | Value | Used by |
|---|---|---|
| `OME_<PEER>_ENDPOINT` | `<isvc>-<peer>.<ns>.svc.cluster.local` (generic, revision-agnostic) | Runtimes in Independent or RollingUpdate groups — cross-revision peer calls are allowed |
| `OME_<PEER>_REVISION_ENDPOINT` | `<isvc>-<peer>-rev-<this-pod's-revision-hash>.<ns>.svc.cluster.local` (per-revision) | Runtimes in BlueGreen groups — pod talks only to same-revision peers |

The runtime image picks which to honor via a config flag or by
preferring `OME_<PEER>_REVISION_ENDPOINT` if set. Both are always
populated; the runtime's role is to pick.

For a prefill-decode `InferenceService` with router + engine +
decoder, every engine pod receives:

```
OME_INFERENCESERVICE_NAME=llama-70b
OME_COMPONENT=engine
OME_COMPONENT_REPLICAS=2
OME_ROUTER_ENDPOINT=llama-70b-router.prod.svc.cluster.local
OME_ROUTER_REVISION_ENDPOINT=llama-70b-router-rev-abc123.prod.svc.cluster.local
OME_DECODER_ENDPOINT=llama-70b-decoder.prod.svc.cluster.local
OME_DECODER_REVISION_ENDPOINT=llama-70b-decoder-rev-abc123.prod.svc.cluster.local
```

Where `abc123` is **this pod's own revision hash** — the pod is told
to talk to peers in its own revision (BlueGreen) or to the generic
Service (RollingUpdate / Independent).

**Why environment variables (not a ConfigMap mount or sidecar):** env
vars are read-once at process startup, which matches the LLM-runtime
pattern (SGLang, vLLM read peer addresses at startup; neither watches
mounted files or talks to sidecars). Env vars are immutable on a
running pod, which is correct: a pod's revision is fixed for its
lifetime, and the peers it can talk to are determined by that
revision.

For in-place image upgrades that change the revision hash (OEP-0007's
in-place updates `metadata.labels`), the env var stays the same —
but the pod's `ome.io/revision` label flips, so the pod is reselected
by the new per-revision Service. The old peer endpoint in the env
var still resolves correctly because it either points at the generic
Service (RollingUpdate, resolves to any peer regardless of revision)
or at the per-revision Service for the new revision (because the env
was set based on the new pod's incoming revision).

The invariant: env vars are set at pod creation time with the
**incoming** revision hash. For in-place upgrade, the incoming
revision is the new one. For recreate, same logic. Either way, the
env points at the right per-revision Service.

### `Status.Components.*.Traffic[]` Producer

The coordination layer writes `Status.Components.<c>.Traffic[]`
during a rollout to satisfy OEP-0011's Part 2 producer contract. Each
entry corresponds to one revision currently serving traffic for the
Component:

```yaml
status:
  components:
    engine:
      latestReadyRevision: llama-70b-engine-rev-newhash
      latestRolledoutRevision: llama-70b-engine-rev-oldhash
      previousRolledoutRevision: llama-70b-engine-rev-prevhash
      traffic:
        - revisionName: llama-70b-engine-rev-oldhash
          percent: 75
          tag: prev
          latestRevision: false
        - revisionName: llama-70b-engine-rev-newhash
          percent: 25
          tag: latest
          latestRevision: true
      rolloutPhase: Canarying          # OEP-0011 field
```

OEP-0011's HTTPRoute builder reads this and emits weighted
backendRefs:

```yaml
backendRefs:
  - name: llama-70b-engine-rev-oldhash
    weight: 75
  - name: llama-70b-engine-rev-newhash
    weight: 25
```

The producer-consumer contract:

- The producer populates `RevisionName` with the **per-revision
  Service name** (not just the hash) so the consumer can use it as a
  Service backend reference directly.
- The producer populates `Percent` with the percentage of
  new-revision pods relative to total pods in the Component.
- The producer maintains `LatestRolledoutRevision`,
  `PreviousRolledoutRevision`, and `LatestReadyRevision` per
  OEP-0007's existing semantics.
- The consumer (HTTPRoute builder) reads the slice and emits weighted
  backendRefs.

For coordinated groups, the `Status.Traffic[]` writes are also
coordinated:

- **BlueGreen:** the controller writes Traffic[] for all Components
  in the group simultaneously (at step boundaries), so HTTPRoute
  weights for every Component shift atomically. `latestRolledoutRevision`
  also advances atomically.
- **RollingUpdate:** writes are per-Component independent, but the
  controller ensures all Components in the group remain at the same
  spec generation (otherwise admission rejected).
- **Independent / no group:** writes are per-Component, asynchronous.

### Controller State Machine

The coordination layer runs a state machine per group per reconcile.
Each group walks:

```
       ┌──── Idle ────┐
       │              │
       ▼              ▲
    Surging           │
       │              │
       ▼              │
    Waiting ──────────┘ (failure → RollingBack → Idle or Failed)
       │
       ▼
    Shifting
       │
       ▼
    Draining
       │
       ▼
    ScalingDown
       │
       ▼
    (loop to Surging if more pods to roll, else Idle)
```

| Phase | What it does | Exit |
|---|---|---|
| `Idle` | No rollout in flight. Watches for spec generation bumps affecting any Component in the group. | Spec bump → `Surging` |
| `Surging` | Create new-revision pods up to `pacing.maxSurge`, subject to ratio constraint if RatioBalanced. For BlueGreen, surge all Components in parallel (or per `order:` if set). | All surge targets met → `Waiting` |
| `Waiting` | Block on `pods.readyCount(rev=new) >= surgeTarget` for every Component. Honor `ome.io/rollout-ready-timeout` annotation. | All Components Ready → `Shifting`. Timeout → `Failed` |
| `Shifting` | Write `Status.Components.<c>.Traffic[]` with new weights. OEP-0011's HTTPRoute builder picks up; weighted backendRefs shift. Emits `RouteBackendsUpdated` event. | Traffic writes succeed → `Draining` |
| `Draining` | Wait `scaleDownDelaySeconds` (default 30s, per OEP-0011) for in-flight requests to complete on old-revision pods. | Time elapsed → `ScalingDown` |
| `ScalingDown` | Delete old-revision pods (or scale Instances down) per pacing. Update `LatestRolledoutRevision` if rollout is complete. | Old pods gone, all replicas at new revision → `Idle`. More to roll → `Surging` |
| `Failed` | Surge timed out or Ready watch errored permanently. Operator decides. | Operator reverts spec → `RollingBack`. Operator forces continue → `Idle` |
| `RollingBack` | Reverting in-flight surge: drain and delete new-revision pods, restore old-revision pods. | Original state restored → `Idle` |
| `Paused` | Rollout held at the current phase. Sources: the `ome.io/rollout-paused: "true"` annotation, or a step-boundary pause in an OEP-0011 step-based rollout. Pacing math and ratio enforcement still apply when the rollout resumes. | Annotation removed or pause expired → previous phase |

For Independent groups (or no group), each Component runs its own
state machine without coordination — equivalent to OEP-0007 v1.

For RollingUpdate, the per-Component state machines run in parallel
but observe a shared `groupSpecGeneration` invariant: if engine is in
`Shifting` for generation N, decoder must also be at generation N
(any spec edit that doesn't bump both is rejected at admission).

For Sequential, the group runs **one Component's state machine at a
time** in the strict order declared. The coordination layer exposes
both the base state machine phase (for metrics) and a composite
operator-facing field:

```yaml
status:
  rolloutCoordinationStatus:
    groups:
      - name: "0"
        components: [decoder, engine]
        order: [decoder, engine]
        phase: Surging                          # base phase (metric label)
        currentComponent: decoder               # which Component owns this phase
        previousComponent: ""                   # most recently completed
        compositePhase: decoder.Surging         # composite operator-facing string
```

| Composite phase | Base `phase` | Meaning |
|---|---|---|
| `Idle` | `Idle` | No Component in the order has a pending spec bump. Steady state. |
| `<Component>.<base-phase>` (e.g., `decoder.Surging`) | the base phase | The currently active Component is in this state machine phase. |
| `Sequential.AwaitingNextComponent` | `Idle` | Previous Component reached Idle on the new revision; next Component has no pending spec bump yet. Operator-controlled soak window. |
| `Sequential.Failed` | `Failed` | One Component entered Failed; subsequent Components blocked. |

Transition rule: a Component at order position `i+1` can only leave
Idle when the Component at order position `i` is at Idle on its
latest spec generation **and** the Component at order position `i+1`
has a pending spec change that bumps its revision hash. If Component
`i+1`'s spec is bumped before Component `i` reaches Idle, the bump
is observed but not acted on — the group stays in
`Sequential.AwaitingNextComponent` (or `<previous-component>.<phase>`
if previous is still in flight) until the gating rule allows.

Metric labels use the base `phase` (single enum, low cardinality);
the composite is operator-status surface only.

### The Sequential Gate

`CheckSequentialGate(isvc, component, group)` is the consumer side
that the OMENative operations dispatcher
(`pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/ops/dispatch.go`)
calls before kicking off a per-Instance Update or Surge. The gate
returns `(allow, denyReason)`; on denial the dispatcher requeues at
the gate's `requeueAfter` instead of starting an operation the
Sequential invariant would have to roll back.

The gate's `observeSequentialComponentsForGate` helper derives a
`RolloutInFlight` boolean per Component from two independent signals:

1. **Revision-hash skew** — `UpdateRevision != CurrentRevision`. The
   classic signal. Fires once the deferred status writer has stamped
   the new target name onto the Component being reconciled.
2. **Generation skew** — `cs.OMENative.ObservedGeneration <
   isvc.Generation`. Fires at the **moment of the spec bump**, before
   any deferred status writer has run.

Both signals matter. The dispatcher is invoked between the revision
computation and the deferred status writer; at that instant the
revision-skew signal is silent but every per-Component status block
is one reconcile behind `isvc.Generation`. Combining the two —
`RolloutInFlight = revisionSkew || generationSkew` — closes the
window. The active-Component selector picks the first in-flight
Component in `Order` and denies the rest. Over-detection from the
generation signal is harmless: the update-trigger detector
independently returns `needs=false` for any Component whose pods
already match the target revision, so the noise clears on the next
reconcile when the status writer bumps `ObservedGeneration`.

**The soak-gate clock.** When `group.Soak > 0`, the Sequential gate
adds a wall-clock denial between the previous Component reaching
`Available=True` and the next Component being allowed to start. The
clock anchor is the **previous Component's `Available` condition's
`LastTransitionTime`**, read from
`isvc.Status.Components[<prev>].OMENative.Conditions[Available]`.

The choice of anchor is load-bearing. The group's own
`LastTransitionTime` only updates when the base `Phase` changes;
Sequential's soak window holds the group at `Phase=Idle` (only
`CompositePhase` advances to `Sequential.AwaitingNextComponent`). So
the group's `LastTransitionTime` would stay pinned at the pre-rollout
Idle, and the time check would always be past `soakDuration` —
making the soak window a no-op. The Component's `Available` condition
is the correct anchor because `apimeta.SetStatusCondition` only bumps
`LastTransitionTime` on a real status transition (False → True),
i.e., the exact moment the previous Component finished its rollout.
When the condition is absent or False, the gate returns zero time
and treats it as "no soak clock, allow."

### The Dispatcher's Requeue Contract

Every gate-denial path (Sequential soak, Sequential active-Component
selector, RatioBalanced budget, paused rollout, in-flight Update or
Create) returns a non-zero `RequeueAfter` so the dispatcher polls
itself back into reconcile at the next gate-eligible moment without
relying on an unrelated watch event.

For this to work end-to-end, the outer per-Component reconcilers
must propagate the dispatcher's `ctrl.Result.RequeueAfter` through
every intermediate call:

```go
// pkg/controller/v1beta1/inferenceservice/components/{engine,decoder,router}.go
deploymentResult, err := e.reconcileDeployment(ctx, isvc, objectMeta, podSpec, size, workerPodSpec)
if err != nil {
    return deploymentResult, err
}
if err := e.updateStatus(...); err != nil {
    return ctrl.Result{}, err
}
return deploymentResult, nil   // preserves RequeueAfter
```

Without this preservation, the outer reconciler silently drops
`ratioGateRequeueInterval` and `updateRequeueInterval`. The visible
symptom is a Sequential post-decoder stall: the soak gate correctly
requests a requeue at `soak - elapsed`, the outer reconciler swallows
it, and the rollout sits until an unrelated watch event happens to
fire. The same loss would affect RatioBalanced budget denials and any
other Update or Create requeue.

**Contract for new per-Component reconcilers:** any
`reconcileDeployment` (or equivalent) that returns a non-zero
`RequeueAfter` MUST have that value preserved by every caller on the
success return path.

### In-Place Upgrade and the Revision Label

OEP-0007 lists `metadata.labels` in the in-place-eligible field list.
OEP-0007.1 uses that to update the `ome.io/revision` label as part of
an in-place upgrade when the new `ControllerRevision` hash differs
from the old. Concretely, an in-place upgrade that changes the
container image does both: patches `spec.containers[].image` to the
new tag **and** patches `metadata.labels["ome.io/revision"]` to the
new hash. Both happen in a single `Patch` call so the apiserver
applies them atomically from the controller-runtime watch perspective.
The pod immediately becomes selected by the new per-revision Service
and deselected by the old. EmptyDir-backed model weights survive the
container restart (OEP-0007's existing in-place value-add).

For the coordination layer, this means an in-place upgrade is
**indistinguishable** from a recreate-style upgrade from the
per-revision Service's perspective — the new revision's pod count
increments either way. The state machine doesn't need an
in-place-aware branch.

If the in-place upgrade is for image-only changes that do not bump
the revision hash (e.g., a `:latest` tag re-pull with no template
change), the revision label stays. No coordination state transitions
are triggered. This matches OEP-0007's semantics (generation didn't
bump → ControllerRevision didn't bump → no rollout).

**The `ome.io/rollout-paused` annotation is excluded from the
revision hash.** The pause annotation is the operator-facing kill
switch for an in-flight coordinated rollout; it must not enter the
`ControllerRevision` spec-hash, or toggling pause would flip the
revision name and drive a phantom rollout on a spec the user never
edited. The filter list in `revision/revision.go::isLifecycleAnnotation`
covers `ome.io/rollout-paused` alongside the
`ome.io/migration-request-v1-*` prefix. Any new controller-mutated
annotations added later must be added to the same filter.

### Failure Handling

**For BlueGreen groups:**

- One Component's pods fail to reach Ready within
  `ome.io/rollout-ready-timeout` → whole group enters Failed.
- `RolloutFailed` event emitted (OEP-0011 inheritance) naming which
  Component failed and why.
- New-revision pods are retained for diagnosis (canary-preservation
  rule from OEP-0011 §Failure Handling).
- HTTPRoute backendRefs remain at the operator-set weight (gateway
  endpoint health excludes unhealthy backends from the LB pool).
- Operator recovery: revert the spec to enter `RollingBack`, or set
  `canaryTrafficPercent: 0` (simple form) to drain the new revision
  while keeping the spec intact for re-attempt.

**For RollingUpdate groups:**

- One Component's failure surfaces as that Component's individual
  Failed phase (per OEP-0007 v1).
- Other Components in the group are not rolled back automatically —
  RollingUpdate's contract is "mixable," and partial-revision states
  are OK.
- The operator decides whether to revert the failing Component
  individually or revert the whole spec.

**For Independent or no group:** per OEP-0007 v1 semantics.

### Migration Verb Integration

Alfred's migration verb (OEP-0007 + OEP-0008) interacts with
coordination as follows:

| Group's policy | Migration verb behavior |
|---|---|
| BlueGreen | Single-Component migration rejected with `MigrationRequiresGroupMigration`. Alfred falls back to a different defragmentation strategy or initiates full-group migration (planned for Alfred v2). |
| RollingUpdate | Single-Component migration proceeds as in OEP-0007 v1. The mixable contract makes it safe (peer pods in the other Component are version-mixable). |
| Independent / no group | Single-Component migration proceeds as in OEP-0007 v1. |

A future `Migrate(group=<name>)` verb that drains and replaces all
Components in a group atomically is out of scope for OEP-0007.1; see
[Open Questions](#open-questions).

### Webhook Validation

Admission rules added to the `InferenceService` validator:

| Rule | Reason |
|---|---|
| Each Component appears in at most one `groups[]` entry | `DuplicateComponentInCoordinationGroups` |
| `groups[].components[]` references only `router`, `engine`, `decoder` | `InvalidComponentInCoordinationGroup` |
| Every Component listed in `groups[]` has `ome.io/deploymentMode=OMENative` | `CoordinationRequiresOMENative` |
| `policy: BlueGreen` with `pacing.type: RatioBalanced` and `pacing.ratioTolerance > 50%` | `RatioToleranceTooHigh` (admission warning) |
| `groups[]` non-empty but `InferenceService` has no Components matching any group | `OrphanCoordinationGroup` |
| `groups[].order[]` references Components not in `groups[].components[]` | `InvalidCoordinationOrder` |
| `policy: RollingUpdate` with only one Component | `RollingUpdateRequiresMultipleComponents` |
| Spec edit bumps one Component's image in a `RollingUpdate` group without bumping the others | `RollingUpdateLockstepViolation` |
| `policy: Sequential` with `order` empty or omitted | `SequentialRequiresOrder` |
| `policy: Sequential` with `order` not covering every Component in `components` | `SequentialIncompleteOrder` |
| `policy: Sequential` with `order` containing Components not in `components` | `InvalidCoordinationOrder` |
| `policy: Sequential` with only one Component | `SequentialRequiresMultipleComponents` |

OEP-0011's `MixedRolloutForms` rule (mutual exclusivity between the
`spec.traffic.rollout` step form and per-Component
`canaryTrafficPercent`) continues to apply unchanged.

### Defaulter Behavior

The mutating webhook does **not** auto-populate `groups[]`. Absent
`rolloutCoordination` means no coordination. When `pacing` is
present, the defaulter fills in:

- `pacing.type: PerComponent` if no type was given
- `pacing.maxSurge: "25%"` if absent
- `pacing.maxUnavailable: 0` if absent
- `pacing.ratioTolerance: 5` if `pacing.type=RatioBalanced` without
  explicit tolerance

### Observability

**Conditions** on `InferenceServiceStatus`:

| Type | Status | Reason | Meaning |
|---|---|---|---|
| `RolloutCoordinationReady` | True | `Idle` | All groups are in Idle phase. No rollout in flight. |
| `RolloutCoordinationReady` | False | `InProgress` | At least one group is mid-rollout. Message lists the phases. |
| `RolloutCoordinationReady` | False | `Failed` | At least one group has Failed. |
| `RolloutCoordinationReady` | Unknown | `Pending` | Groups declared but controller hasn't observed them yet. |

**Events** emitted on the `InferenceService`:

- `CoordinationGroupSurging` — group enters Surging
- `CoordinationGroupShifting` — group enters Shifting
- `CoordinationGroupCompleted` — group returned to Idle after a
  rollout
- `CoordinationGroupFailed` — group entered Failed
- `RatioSkewRejected` — surge attempt rejected to avoid ratio drift
- `MixedPairingObserved` — RollingUpdate group observed
  cross-revision peer pairs in flight (informational; helps operators
  audit back-compat assertion)
- `SequentialComponentCompleted` — Sequential group's active
  Component reached Idle; next Component is now eligible to start
- `SequentialAwaitingNext` — Sequential group entered the soak window
  between Components
- `SequentialNextSpecBumpDeferred` — operator bumped Component `i+1`
  while Component `i` was still mid-rollout; bump observed but not
  acted on yet
- `ConsiderCoordinationGroup` — informational hint on
  multi-Component `InferenceService`s without any declared group

**Metrics:**

- `ome_omenative_rollout_group_phase_total{isvc,group,phase}` — counter per phase entry
- `ome_omenative_rollout_group_transition_total{isvc,group,from,to}` — counter per transition
- `ome_omenative_rollout_group_failure_total{isvc,group,reason}` — failure counter
- `ome_omenative_ratio_skew_total{isvc,group}` — counter for RatioBalanced rejections
- `ome_omenative_per_revision_service_total{isvc,component}` — gauge of live per-revision Services
- `ome_omenative_mixed_pairing_total{isvc,group}` — count of distinct revision-pair combinations observed (RollingUpdate diagnostic)

**Structured logs:** each coordination reconcile emits
`{isvc, group, phase, components, observedRatio, surgeTarget}`.

## Backwards Compatibility

OEP-0007 v1 ships first. OEP-0007.1 is strictly additive:

- **Spec additions are optional.** Existing `InferenceService`s that
  don't declare `spec.rolloutCoordination` see zero behavior change.
- **Per-revision Services are created** for OMENative-managed
  Components even when no group is declared. They are additive
  Kubernetes resources; existing consumers that reference the
  revision-agnostic Services keep working.
- **Env var injection extends but doesn't replace** OEP-0007's
  existing env vars. New vars are `OME_<PEER>_ENDPOINT` and
  `OME_<PEER>_REVISION_ENDPOINT` per peer Component. Runtimes that
  don't read these are unaffected.
- **`Status.Components.<c>.Traffic[]` starts being populated** even
  when no group is declared (single-entry, `latestRolledoutRevision @
  100%`). OEP-0011's HTTPRoute builder reads this and emits a
  single-backend HTTPRoute pointing at the per-revision Service
  instead of the generic Service.

That last bullet is a meaningful operational change. The HTTPRoute
backend switches from `<isvc>-engine` (generic) to
`<isvc>-engine-rev-<hash>` (per-revision) when v1.1 ships. Two
options:

- **Option A:** flip the switch in v1.1 unconditionally. The new
  Service exists, the HTTPRoute points at it. Cleanest design, one
  less code path.
- **Option B:** gate the HTTPRoute backend switch behind any
  `rolloutCoordination` declaration. `InferenceService`s without
  `groups[]` keep the generic backend (today's v1 behavior).

Option A is recommended: the per-revision Service contains exactly
the same pods as the generic Service for a single-revision Component,
so routing behavior is identical. The benefit is that any time the
operator adds `groups[]`, the HTTPRoute is already routed through
the per-revision plumbing, so the transition is invisible.

## Coexistence with OEP-0011 Step-Based Rollouts

OEP-0011's `spec.traffic.rollout` step-based form is the
operator-facing API for "I want a multi-step canary plan."
OEP-0007.1's `spec.rolloutCoordination.groups[]` is the
operator-facing API for "I want these Components to coordinate." They
compose orthogonally:

- `InferenceService` with `spec.traffic.rollout` but no `groups[]`
  → step-based canary per Component, independent.
- `InferenceService` with `groups[]` but no `spec.traffic.rollout`
  → coordinated rolling upgrades (no per-step pacing).
- Both → coordinated step-based rollouts; the controller walks the
  step plan with coordination policy applied at each step.

For the both-set case, the controller-side step state machine
consumes the coordination groups to decide gang vs per-Component
surge per step. Implementation is part of this OEP; the step-based
form already has its admission validation.

OEP-0011's `MixedRolloutForms` rule (mutual exclusivity between
simple `canaryTrafficPercent` and step-based `spec.traffic.rollout`)
applies unchanged. Coordination groups apply to either form.

## Recipes

Common operational patterns are compositions of one OEP-0011 setting
and one OEP-0007.1 policy. These named patterns let reviewers reach
for the recipe instead of deriving the composition.

### Recipe 1: Prefill-decode-coordinated canary at 10%

**Intent:** "Canary the new revision at 10% for all prefill-decode
Components simultaneously. Engine and decoder must shift together; no
version mismatch across the pair."

```yaml
spec:
  rolloutCoordination:
    groups:
      - components: [engine, decoder]
        policy: BlueGreen
  engine:
    canaryTrafficPercent: 10
  decoder:
    canaryTrafficPercent: 10
```

BlueGreen ensures both Components surge new-revision pods together,
traffic-shift to 10% together, and stay there together. OEP-0011's
per-Component `canaryTrafficPercent: 10` defines the weight;
BlueGreen enforces that both Components apply it in the same
reconcile.

Without BlueGreen (today's v1 behavior), the operator could set
`engine.canaryTrafficPercent: 10` and forget decoder, ending up with
engine at 10% v2 talking to decoder at 0% v2 — silent prefill-decode
mismatch. This recipe is how the "PD components coupled" guarantee
gets enforced.

### Recipe 2: Blue-green at PD group level

**Intent:** "Stand up a full second copy of the entire prefill-decode
group on v2, validate via direct endpoint, then atomically cut over
all traffic from v1 to v2."

```yaml
spec:
  rolloutCoordination:
    groups:
      - components: [router, engine, decoder]
        policy: BlueGreen
        pacing: { maxSurge: "100%" }
  router:
    canaryTrafficPercent: 0
  engine:
    canaryTrafficPercent: 0
  decoder:
    canaryTrafficPercent: 0
```

Phase 1 (standby): operator bumps all three Component images. The
BlueGreen group ensures all three standby pod sets come up together.
Decoder, engine, and router rev-`<new>` pods all reach Ready, but at
0% traffic. The group enters Shifting then immediately stops at the
0% weight; HTTPRoute weights are `100% old, 0% new`.

Validation: operator hits the per-revision Service
(`<isvc>-router-rev-<newhash>.<ns>.svc.cluster.local`) directly with
test traffic. Validates v2 is healthy.

Phase 2 (cutover): operator edits `canaryTrafficPercent` from 0 to
100 on all three Components in one apply. BlueGreen flips all three
Components from 0% to 100% in the same reconcile. HTTPRoute weights
swap atomically. Old pods drain after `scaleDownDelaySeconds`.

Rollback (if v2 has a problem): edit `canaryTrafficPercent` back to
0 on all three. BlueGreen flips together. Old pods are still alive
(within `scaleDownDelaySeconds`), so revert is instant.

### Recipe 3: Multi-step canary with PD coupling

**Intent:** "Walk the new prefill-decode revision through 10% →
soak → 50% → manual approval → 100%. Engine and decoder must walk
the plan together."

```yaml
spec:
  rolloutCoordination:
    groups:
      - components: [engine, decoder]
        policy: BlueGreen
  traffic:
    rollout:
      steps:
        - trafficWeight: 10
          pause: { duration: 30m }
        - trafficWeight: 50
          pause: {}
        - trafficWeight: 100
      promotionPolicy: Manual
      scaleDownDelaySeconds: 60
```

Operator bumps engine + decoder image tags. BlueGreen ensures both
surge together. OEP-0011's step machine walks 10% (auto 30m), 50%
(manual pause), 100% (final). At each step, BlueGreen ensures engine
and decoder's HTTPRoute weights advance together — the operator
promotes once, both shift.

`MixedRolloutForms` means no per-Component `canaryTrafficPercent` is
set when using the step form; BlueGreen doesn't change that rule.

### Recipe 4: Staged Sequential validation (cross-day rollout)

**Intent:** "Roll the new decoder to 100% under production traffic
for 24 hours. Only then start engine. Engine v2 has matching KV-cache
support; decoder v2 carries the risky change."

```yaml
spec:
  rolloutCoordination:
    groups:
      - components: [decoder, engine]
        policy: Sequential
        order: [decoder, engine]
  traffic:
    rollout:
      steps:
        - trafficWeight: 10
          pause: {}
        - trafficWeight: 50
          pause: { duration: 30m }
        - trafficWeight: 100
      promotionPolicy: Manual
```

Day 0: operator bumps decoder. Decoder walks the full step plan
(10 → soak → 50 → soak → 100 → promote). Engine spec is unchanged;
engine doesn't roll. After decoder reaches Idle on v2, the group
enters `Sequential.AwaitingNextComponent`.

Day 1 (after 24h soak): operator bumps engine. Engine walks the
same step plan. After engine reaches Idle, the group rollout is
complete.

### Recipe 5: Engine + decoder RollingUpdate, router rolls separately

**Intent:** "Engine + decoder are released together by the runtime
team with guaranteed protocol back-compat. Router is a CPU pod on a
different release cadence."

```yaml
spec:
  rolloutCoordination:
    groups:
      - components: [engine, decoder]
        policy: RollingUpdate
        pacing: { type: RatioBalanced, ratioTolerance: 5 }
  # router not listed → Independent (today's v1 default)
```

Operator co-bumps engine + decoder images. RollingUpdate allows
cross-revision peer talk during the rollout window. RatioBalanced
keeps the asymmetric topology (e.g., 40:20) from drifting past 5% of
the original ratio. `MixedPairingObserved` metric exercises; the
operator audits that protocol back-compat held. Router updates happen
on a different PR, rolling independently.

### Recipe 6: Independent — today's v1 behavior

**Intent:** "I know what I'm doing. Each Component is fully
independent. No coordination."

```yaml
spec:
  # rolloutCoordination omitted, or:
  rolloutCoordination:
    groups: []
```

Same behavior as OEP-0007 v1: each Component reconciles independently,
no cross-Component gating, no prefill-decode coupling enforcement.
Recommended only when the operator has validated each Component is
safe to roll without peer awareness — which is **not** true for
default prefill-decode topologies.

The OMENative controller emits a `ConsiderCoordinationGroup`
informational event on multi-Component `InferenceService`s without
any declared group to nudge operators toward Recipe 1 or 4.

## Test Plan

### Unit Tests

- `coordination/groups.go` — group resolution + Component-to-group
  membership lookup: ≥ 90%
- `coordination/state.go` — state machine transitions per policy: ≥ 90%
- `coordination/ratio.go` — RatioBalanced pacing math: ≥ 95%
  (boundary cases for integer and non-integer ratios)
- `coordination/discovery.go` — env var injection per pod, per peer:
  ≥ 90%
- `coordination/services.go` — per-revision Service create / Get /
  GC: ≥ 85%
- Webhook additions: ≥ 90%

### Integration Tests

The coordination integration suite runs on KIND. The envtest harness
cannot exercise drain races, per-revision Service convergence, or
pod-UID rotation reliably (the `PodKubeletSimulator` had timing races
that made the suite chronically flaky); KIND with a real kubelet,
scheduler, and containerd is now authoritative for the per-policy
correctness tests below. The suite lives under
`tests/integration/singlecluster/controller/omenative_coordination_kind/` with
a 2-node default and a 90-minute outer timeout; see
[`CLAUDE.md`](../../CLAUDE.md) for run instructions
(`make test-kind`).

The envtest harness is retained for webhook and defaulter
admission tests and for OEP-0011's existing EG `BackendTrafficPolicy`
integration. Per-revision Services are typed (`corev1.Service`), so
no new CRD vendoring.

**Per-policy correctness (alpha gate):**

1. Independent / no group — bump engine alone; engine rolls, decoder
   unchanged.
2. BlueGreen two-Component (engine + decoder) — bump engine image;
   group enters Surging, blocks until decoder also has new pods.
3. BlueGreen three-way — bump all three image tags in one update;
   group walks Surging → Waiting → Shifting → Draining → ScalingDown
   → Idle. HTTPRoute weights for all Components shift in the same
   reconcile.
4. RollingUpdate engine + decoder — bump both in one update;
   per-Component reconciliation, generic Service for cross-revision
   talk, `MixedPairingObserved` event fires when distinct revision
   pairs are observed.
5. RatioBalanced 40:20 — bump engine + decoder; controller surges
   pods proportionally, never lets ratio drift past 2.05:1.
6. Sequential (decoder, engine) — bump decoder image alone; decoder
   walks to Idle, engine stays at v1. Bump engine after decoder Idle;
   engine walks to Idle. Group phase reaches Idle.
7. Sequential gating — bump decoder + engine images in the same
   apply; only decoder enters Surging. Engine bump is observed (event
   `SequentialNextSpecBumpDeferred`) but engine stays at v1 until
   decoder reaches Idle, then engine starts.
8. Sequential failure block — bump decoder with a bad image; decoder
   enters Failed. Engine never enters Surging even after operator
   bumps engine spec. Operator reverts decoder; decoder walks
   RollingBack → Idle; engine spec bump then becomes actionable.
9. Recipe 4 end-to-end (cross-day Sequential with step plan) — bump
   decoder; decoder walks 10 → 50 → 100 step plan with manual
   promotions; engine doesn't move. Advance clock and re-apply engine
   spec; engine walks the same plan.

**OEP-0011 producer contract (alpha gate):**

10. Single-revision Component (no rollout in flight) —
    `Status.Components.engine.Traffic[]` has one entry,
    `RevisionName=<isvc>-engine-rev-<hash>`, `Percent=100`. OEP-0011's
    HTTPRoute has one backendRef pointing at the per-revision Service.
11. Mid-rollout (2 revisions live) — `Status.Components.engine.Traffic[]`
    has two entries with proportional weights. OEP-0011's HTTPRoute
    has two weighted backendRefs.
12. Per-revision Service lifecycle — created on first pod with the
    revision hash, deleted after the last pod is gone AND the
    revision is past the retention limit.

**Discovery (alpha gate):**

13. Engine pod env vars include `OME_DECODER_ENDPOINT` (generic) AND
    `OME_DECODER_REVISION_ENDPOINT` (per-revision matching the pod's
    own revision).
14. In-place upgrade that bumps revision hash — env vars unchanged
    (per-revision endpoint already pointed at the new revision since
    the env was set at pod create time with the new revision); label
    flips, pod is reselected by the new per-revision Service.

**Migration verb (alpha gate):**

15. `Migrate(engine, instance=0)` on a BlueGreen-grouped
    `InferenceService` — rejected with
    `MigrationRequiresGroupMigration`.
16. `Migrate(engine, instance=0)` on a RollingUpdate
    `InferenceService` — proceeds per OEP-0007 v1 semantics.

**Webhook (alpha gate):**

17. Duplicate Component in groups → `DuplicateComponentInCoordinationGroups`
18. Component listed without OMENative annotation →
    `CoordinationRequiresOMENative`
19. RollingUpdate with one Component →
    `RollingUpdateRequiresMultipleComponents`
20. Spec edit that bumps only one Component in a RollingUpdate group
    → `RollingUpdateLockstepViolation`
21. Sequential without `order` → `SequentialRequiresOrder`
22. Sequential with `order` missing a Component in `components` →
    `SequentialIncompleteOrder`
23. Sequential with one Component → `SequentialRequiresMultipleComponents`

**Failure handling (alpha gate):**

24. BlueGreen group with a bad image in one Component — all
    Components enter Surging, decoder reaches Ready, engine never
    does, group enters Failed after timeout. HTTPRoute weights
    unchanged (gateway excludes unhealthy backends). `RolloutFailed`
    event emitted.
25. Recovery from Failed — operator reverts spec; group enters
    RollingBack; new-revision pods drained; group back to Idle.

**Coexistence (beta gate):**

26. `InferenceService` with both `spec.traffic.rollout` (step-based)
    and `groups[]: BlueGreen` — step state machine walks each step
    with BlueGreen group surge applied; per-step pause works as
    documented in OEP-0011.

### End-to-End (beta gate)

27. Real OMENative pods on KIND with router + engine + decoder. Hit
    the top-level URL with curl, observe sticky-session behavior,
    mid-rollout observe both old and new revision pods receiving
    traffic per the coordination policy.

## Graduation Criteria

### Alpha

- All unit tests passing.
- Integration tests 1–25 passing.
- OEP-0011 Part 2 alpha-gate tests passing on OMENative (which this
  OEP enables — `Status.Components.*.Traffic[]` populated,
  per-revision Services available, HTTPRoute weights flow).
- Documentation: operator quickstart (`quickstart.md`), migration
  from OEP-0007 v1 (`migration.md`), API reference (`api.md`).
- At least one production-grade prefill-decode `InferenceService`
  running on OMENative with `policy: BlueGreen` for ≥ 14 days without
  Sev1/Sev2 incidents.

### Beta

- Integration test 26 passing (coexistence with step-based rollouts).
- 2+ production prefill-decode `InferenceService`s running with
  `policy: RollingUpdate` for ≥ 60 days; `MixedPairingObserved`
  metric exercised in real rollouts without operator-reported
  corruption.
- 2+ production prefill-decode `InferenceService`s running with
  `pacing.type: RatioBalanced` for ≥ 60 days without starvation
  incidents.
- End-to-end test 27 passing on at least one supported platform.
- Migration verb integration with Alfred v2 (full-group migration)
  shipped or documented as deferred.

### GA

- 3 months beta with no regressions.
- OEP-0007 v1's "deferred until OEP-0007.1" language removed from
  OEP-0007 README (the deferral is no longer pending).
- Coordination group declaration is part of the recommended
  prefill-decode `InferenceService` pattern in OME's operator guide.
- Alfred v2 full-group migration verb shipped.

## Drawbacks

1. **Adds an orthogonal API surface on `InferenceService`.**
   `spec.rolloutCoordination` is a new top-level field alongside
   `spec.traffic`. Two related surfaces with different scopes
   (traffic = data plane, coordination = workload). Operators must
   learn both. Mitigation: cross-link from each OEP's quickstart.
2. **RollingUpdate depends on operator-asserted protocol back-compat.**
   A wrong assertion silently corrupts prefill-decode requests.
   Mitigation: emit `MixedPairingObserved` so operators see what was
   paired; document the assertion in the OEP and quickstart.
3. **RatioBalanced math has tolerance for non-integer ratios.** A
   40:21 topology can't enforce 2:1 exactly. Mitigation: configurable
   `ratioTolerance` (default 5%).
4. **Per-revision Service count grows with active revisions.** Bounded
   by `revision-history-limit` (default 3) × Components × 2 (routing
   + headless) — at most ~24 per `InferenceService`. Cheap Kubernetes
   objects, but visible in `kubectl get svc` output. The distinct name
   pattern makes filtering easy.
5. **The coordination state machine adds reconcile-loop complexity.**
   More places for bugs. Mitigation: per-Component reconcilers
   (OEP-0007 v1) stay entirely unaware of coordination; coordination
   is a thin orchestrator. Each layer is unit-testable independently.
6. **Sequential rollouts span operator-controlled time windows.** The
   soak between Components is operator-driven, not time-bounded — a
   half-completed Sequential rollout can sit in
   `Sequential.AwaitingNextComponent` for days. Mitigation: emit a
   `SequentialStaleSoak` event after a configurable threshold
   (default 7 days). Operationally benign — the active Component is
   fully on its new revision; the inactive Component is still on its
   v1 revision.
7. **BlueGreen groups slow down rollouts.** Surging all Components in
   parallel and waiting for all to be Ready means rollout time scales
   with the slowest Component. Mitigation: BlueGreen is
   operator-opt-in, not the default. Operators who want speed use
   RollingUpdate or Independent.

## Alternatives

1. **Separate `InferenceServiceRolloutPolicy` CRD.** Considered and
   rejected — see the design conversation. Summary: no demonstrated
   need for cross-`InferenceService` policy reuse; OEP-0007's
   "no new CRDs" hard constraint stands.
2. **Per-Component `coupledWith[]` field instead of
   `InferenceService`-level `groups[]`.** Rejected: more verbose for
   the three-way BlueGreen case (every Component declares its peers);
   harder admission validation (bidirectional consistency must be
   enforced).
3. **Single `InferenceService`-level `coordinationPolicy` enum (no
   groups).** Rejected: doesn't generalize to mixed scenarios (e.g.,
   "router atomic with engine, decoder RollingUpdate with both").
   YAGNI in the short term but blocks the door on operationally-real
   cases.
4. **Auto-detect BlueGreen for multi-Component `InferenceService`s.**
   Rejected: backwards-incompatible with OEP-0007 v1's Independent
   default. Operators who want BlueGreen declare it.
5. **Per-revision ConfigMap mount for peer discovery.** Rejected:
   LLM runtimes (SGLang, vLLM) read peer addresses once at startup;
   they don't watch mounted files. Env var injection matches the
   actual runtime pattern.
6. **Sidecar-based discovery.** Rejected: every pod gains a sidecar
   container; the runtime must call localhost API. Heavier than env
   vars for no functional benefit on LLM-runtime patterns.
7. **Step-based rollouts within OEP-0007.1.** Rejected: the operator
   API for step-based rollouts already exists in OEP-0011
   (`spec.traffic.rollout`). OEP-0007.1's coordination groups apply
   underneath either rollout form.
8. **Sequential as a flavor of BlueGreen (with `order:` mandatory).**
   Rejected. BlueGreen's defining contract is "the gang traffic-shifts
   together"; Sequential's is "one Component's full lifecycle, then
   the next." Overloading BlueGreen with `order: required` plus
   "actually walk full lifecycle per Component" produces ambiguous
   semantics for the partial-BlueGreen-order case. Sequential as its
   own policy keeps BlueGreen's contract clean.
9. **Blue-green / canary as new OEP-0007.1 policies.** Rejected.
   Blue-green and canary are traffic-policy semantics (% of traffic
   shifted) owned by OEP-0011. OEP-0007.1's role is to enforce that
   prefill-decode Components apply the traffic semantic together. The
   compositional pattern (Recipe 2 for blue-green) is the user-facing
   surface, not a new policy enum.

## Open Issues

Live items that current implementation has surfaced but not fully
resolved. Distinct from the research-level Open Questions below.

1. **Bumping spec while a per-Instance rollout is in flight does not
   consistently retarget to the latest revision.** Cross-listed with
   OEP-0007's open issues. Surfaces independently of coordination
   policy. The OMENative-side fix likely runs through the per-Instance
   status mirror and the dispatcher's target-revision retarget path;
   the coordination layer would inherit the fix for free — no API
   change here.
2. **Generation-skew over-detection on long-tail Components.** The
   Sequential gate flags every Component whose `ObservedGeneration`
   is behind `isvc.Generation`, not just the Component whose target
   revision actually changed. Today this is harmless because
   `detectUpdateTrigger` independently returns `needs=false` for no-op
   Components. If a future code path skips that check (e.g., a
   recovery or repair path that fires unconditionally), the gate
   could deny work unnecessarily. Flag for review when adding any
   non-Update dispatcher path.
3. **The soak gate depends on the `Available` condition's
   `LastTransitionTime` being set only on real False → True
   transitions.** `apimeta.SetStatusCondition` guarantees this today;
   the soak gate's correctness will break silently if a future PR
   replaces that helper with raw condition construction. The gate
   already has a defensive fallback (zero time → "no soak clock,
   allow") so the failure mode is "soak never enforced," not "rollout
   deadlocks" — but a regression test is warranted if the
   condition-writer call site is refactored.

### Generic Service routing under BlueGreen

When `policy: BlueGreen`, the generic Service (`<isvc>-<component>`)
still selects all live pods regardless of revision. External callers
hitting the generic Service mid-rollout will see a mix. The current
behavior — keep the generic Service revision-agnostic — avoids load
drops on the new-revision pods while they are Ready but old-revision
pods are still draining. Documented here so operators don't expect
strict per-revision routing from the generic Service.

## Open Questions

1. **Full-group migration verb.** Alfred's defragmentation needs a
   way to move all Components in a BlueGreen group together. Current
   workaround: reject single-Component migration in BlueGreen groups
   and force the operator to revert spec, wait, re-deploy. Better:
   `Migrate(group=<name>)` that drains and replaces atomically.
   Tracked for Alfred v2.
2. **Ratio computation for autoscaled Components.** OMENative v1
   doesn't ship HPA, so replica count is operator-declared. If future
   HPA shipping changes replica counts mid-rollout, the "original
   ratio" baseline drifts. Open: snapshot the ratio at rollout start,
   or recompute every reconcile?
3. **Cross-group coordination.** An `InferenceService` could declare
   multiple groups (e.g., `[{router, engine}, {decoder}]`). Each
   group rolls independently of the others. Should there be a
   cross-group ordering knob? Probably not (YAGNI), but worth
   flagging.
4. **RollingUpdate revision skew tolerance.** RollingUpdate's
   contract is "any vN can talk to any vM in the group." If the
   operator skews the spec generation across Components (intentionally
   or via Git conflict), should the controller catch it? Today's
   webhook rule `RollingUpdateLockstepViolation` rejects the spec
   edit, but doesn't detect drift that happens via separate edits to
   engine.image vs decoder.image. Open: add a status condition for
   `SpecGenerationSkew`?
5. **Version-pair compatibility DAG (deferred).** Today's policies
   express coupling (BlueGreen, Sequential, RollingUpdate) but not
   version-pair constraints. A future need: "engine v3 requires
   decoder ≥ v2; engine v4 requires decoder = v4 (lockstep)." Forms a
   partial-order DAG over revisions per Component pair. YAGNI for
   alpha; flagged because real multi-version runtime fleets will
   eventually want it. Possible API: `groups[].versionConstraints:
   [{from: {engine: v3}, requires: {decoder: ">=v2"}}]`.
6. **Asymmetric failure policy (deferred).** Today's failure rules are
   uniform per policy: BlueGreen fails the whole group, Sequential
   blocks downstream Components, RollingUpdate fails per-Component.
   Operators have asked for asymmetric: "independent going up, atomic
   on failure" (each Component rolls on its own, but if one fails,
   roll all back). Possible API: `groups[].failurePolicy:
   GroupRollback | ComponentRollback`. The wire format is small but
   the controller-side implementation is fiddly (tracking "if I
   failed and a peer is still rolling forward, do I block its
   progress?").
7. **Sequential auto-advance.** Today Sequential's "next Component
   starts only when its spec bumps" is operator-driven. Some
   operators have asked for time-based auto-advance: "after decoder
   reaches Idle for N hours, automatically start engine if its spec
   has been bumped." Could be expressed via OEP-0011's per-step pause
   applied to the Sequential boundary, but the boundary isn't a step
   today. Open: extend the soak window into the API, or keep it
   operator-driven?

## Implementation History

- **2026-05-20** — Initial draft. Triggered by Phase 1 of OEP-0011
  completing and the realization that OEP-0007 v1's deferred
  coordination and OEP-0011's "per-mode producer" gap are the same
  set of work.
- **2026-05-21 → 2026-05-26** — API, state machine, per-revision
  Service, and env-var injection landed.
- **2026-05-27 (stabilisation)** — Sequential gate wired into the
  dispatcher; soak-gate clock re-anchored on the previous Component's
  `Available` condition; `RolloutInFlight` augmented with the
  generation-skew signal to close the gap between the spec bump and
  the deferred status writer;
  `ome.io/rollout-paused` filtered out of the revision hash; KIND
  coordination test suite scaffolded; per-Component reconciler
  `RequeueAfter` preserved on the success path.
