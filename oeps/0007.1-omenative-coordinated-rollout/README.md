# OEP-0007.1: OMENative Coordinated Rollout

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [What OEP-0007 v1 Defers](#what-oep-0007-v1-defers)
  - [The Five PD Rollout Scenarios](#the-five-pd-rollout-scenarios)
  - [The OEP-0011 Contract Gap](#the-oep-0011-contract-gap)
  - [Division of Responsibility with OEP-0011](#division-of-responsibility-with-oep-0011)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Conceptual Overview](#conceptual-overview)
  - [Scenario → API Mapping](#scenario--api-mapping)
  - [User Stories](#user-stories)
  - [Risks and Mitigations](#risks-and-mitigations)
- [API](#api)
  - [The `rolloutCoordination` Block](#the-rolloutcoordination-block)
  - [Coordination Policies](#coordination-policies)
  - [Pacing](#pacing)
  - [Defaults](#defaults)
- [Design Details](#design-details)
  - [Per-Revision Services](#per-revision-services)
  - [Generation-Scoped Discovery via Env Injection](#generation-scoped-discovery-via-env-injection)
  - [Status.Components.*.Traffic[] Producer](#statuscomponentstraffic-producer)
  - [Controller State Machine](#controller-state-machine)
  - [In-Place Upgrade and the Revision Label](#in-place-upgrade-and-the-revision-label)
  - [Failure Handling](#failure-handling)
  - [Migration Verb Integration](#migration-verb-integration)
  - [Webhook Validation](#webhook-validation)
  - [Defaulter Behavior](#defaulter-behavior)
  - [Observability](#observability)
- [Backwards Compatibility](#backwards-compatibility)
- [Coexistence with OEP-0011 Step-Based Rollouts](#coexistence-with-oep-0011-step-based-rollouts)
- [Recipes (OEP-0011 × OEP-0007.1 Composition)](#recipes-oep-0011--oep-00071-composition)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

OEP-0007.1 extends OMENative (OEP-0007) with cross-Component rollout
coordination, per-revision Services, generation-scoped peer discovery,
and the `Status.Components.<c>.Traffic[]` producer that closes
OEP-0011 Part 2's alpha-gate contract.

OEP-0007 v1 ships intra-Component lifecycle (in-place upgrade,
migration verb, four-level hierarchy) but explicitly defers
cross-Component coordination — each Component rolls independently. For
prefill-decode (PD) workloads where engine and decoder talk to each
other internally, "each Component rolls independently" produces silent
mixed-version traffic during the rollout window: a v2 engine pod
calling a v1 decoder pod, KV-cache shapes mismatched, requests
silently corrupted. This OEP addresses that gap.

The OEP introduces **no new user-visible CRDs** (per OEP-0007's hard
constraint). One new optional field on `InferenceServiceSpec`:
`spec.rolloutCoordination.groups[]`, plus the controller-level
plumbing (per-revision Services, env-var-based generation-scoped
discovery, `Status.Components.<c>.Traffic[]` writes) needed to make
coordinated rollouts safe.

The five scenarios PD operators have asked for:
1. **BlueGreen three-way** — router + engine + decoder roll as one unit
2. **All independent** — each Component rolls on its own (today's v1 default; preserved as v1.1 default for back-compat)
3. **RollingUpdate** — engine + decoder roll in lockstep but any v2 engine pod can talk to any v1 decoder pod and vice versa (operator asserts protocol back-compat)
4. **Ratio-aware pacing** — orthogonal to 1-3; for asymmetric topologies (e.g., 40 prefill : 20 decode), enforces the original ratio across the rollout so neither side gets starved
5. **Sequential / phased** — decoder rolls fully to 100% on the new revision, soaks for as long as the operator wants, only then does engine start its own rollout cycle. Useful for staged validation: prove the new decoder under real production traffic before risking the engine.

All five are expressible by combining one of four `policy` enum
values (BlueGreen / Independent / RollingUpdate / Sequential) with one
of two `pacing.type` values (PerComponent / RatioBalanced) inside a
`groups[]` entry.

OEP-0007.1 is the **workload-coordination** half of the rollout story;
OEP-0011 owns the **traffic-policy** half (what % goes where, step
plans, blue-green semantics). See [Division of Responsibility with
OEP-0011](#division-of-responsibility-with-oep-0011) and
[Recipes](#recipes-oep-0011--oep-00071-composition) for how the two
OEPs compose. Blue-green and multi-step canary are OEP-0011 patterns
that become PD-safe by adding a BlueGreen group from this OEP — they're
not separate scenarios.

OEP-0007.1 lands **after** OEP-0007 v1. Existing OMENative ISVCs
upgrade with **zero behavior change** — coordination is opt-in via
explicit `groups[]` declaration. The "extremely robust default" goal
is met through declared intent, not silent reconfiguration.

OEP-0007.1 also satisfies the OEP-0011 Part 2 alpha-gate contract: it
populates `Status.Components.<c>.Traffic[]` with revision-pair weights
and creates the per-revision Services that OEP-0011's HTTPRoute
weighted-backendRef consumer (already shipped in PR #147) reads from.

## Motivation

### What OEP-0007 v1 Defers

OEP-0007's §Coordinated Cross-Component Rollout (README:842-849) is
explicit:

> Coordinated engine/decoder rollout is **deferred from OMENative v1**.
> This OEP keeps the concept because it remains a plausible future
> extension, but the v1 controller contract is intentionally narrower:
> single-Component correctness first, cross-Component skew
> coordination later.
>
> In v1:
> - each OMENative-managed Component rolls independently
> - Alfred migration targets one Component Instance at a time
> - no `CoordinationPaused` condition or coordination-specific metrics
>   are required

OEP-0007's Goal #6 reinforces:

> Keep the controller kernel narrow and implementable. Single-Component
> correctness ships first; richer cross-Component coordination can
> layer on later.

This OEP **is** the "later." It addresses the deferred concerns
without altering the OEP-0007 v1 controller kernel — coordination
plugs in as an additional state machine layer on top of the existing
per-Component reconciliation.

### The Five PD Rollout Scenarios

Real PD deployments need cross-Component rollout coordination because
prefill and decode pods talk to each other internally during inference:
the router fans out to prefill, prefill streams partial KV state to
decode, decode generates tokens. Mid-rollout, "v2 engine talks to v1
decoder" can mean:

- KV-cache layout incompatibility → silent garbage outputs
- gRPC/protobuf schema drift → connection failures (visible) or
  field-level corruption (silent)
- Tensor-shape mismatch → segfault on the decoder side

The five scenarios operators have asked for:

#### Scenario 1: BlueGreen three-way rollout

Router + engine + decoder roll together. Surge new pods for all
three. Wait for all three new sets to be Ready. Atomically switch
traffic. If any one fails, roll back all three.

Slowest. Safest. The default for operators who don't want to think
about coordination — declare the group, let the controller do the
gang.

#### Scenario 2: All independent (today's v1)

Each Component rolls on its own. Fastest. Operator asserts that:
- Router doesn't need to match engine/decoder versions (true — router
  is stateless, talks to engine via Service DNS, version-agnostic)
- Engine doesn't need to match decoder (true only when the protocol
  is back-compat)

Risky for PD without back-compat protocol. The default in v1.1 for
back-compat reasons; explicit `groups[]` is the way to upgrade.

#### Scenario 3: Engine + decoder RollingUpdate, router independent

Engine and decoder roll in lockstep at the **spec level** (operator
bumps both image tags as one PR), but during the rollout window any
vN engine pod is allowed to talk to any vM decoder pod. Router rolls
on its own.

Operationally common when:
- Engine/decoder are co-released by the runtime team and protocol is
  guaranteed back-compat across minor versions
- Router is updated on a different cadence (CPU pod, dependency-only
  changes)

The middle-ground policy. Looser than BlueGreen, safer than fully
Independent.

#### Scenario 4: Ratio-aware pacing

Asymmetric PD topologies are common: 40 prefill (compute-bound) talk
to 20 decode (memory-bound) at a 2:1 ratio. A naive rollout that
surges 25% of prefill (10 pods) and 25% of decode (5 pods) in parallel
can produce momentary 2.5:1 skew if the prefill surge completes first
— 10 new prefill pods routing to 0 new decode pods, starvation.

Ratio-aware pacing computes the original cross-Component ratio at
rollout start and refuses to let the rollout drift past it. Orthogonal
to the policy enum: ratio-aware applies to BlueGreen and RollingUpdate
groups (Independent groups by definition don't track cross-Component
ratios).

#### Scenario 5: Sequential / phased rollout

Decoder rolls **fully** to 100% on the new revision and stays there
for an operator-controlled soak window. Only after the operator is
satisfied does engine begin its own rollout cycle. The two
Components' rollouts are strictly serialized along the operator-pinned
`order`.

Operationally this is the staged-validation pattern: PD operator
wants to prove the new decoder under real production traffic for an
hour (or a day, or a week) before touching engine. BlueGreen is too
coupled — it forces both to complete in the same rollout window;
Independent is too loose — engine and decoder both start rolling the
moment the spec bumps and end up mid-rollout simultaneously.

Sequential is the missing middle ground:

- Strict ordering: engine literally cannot enter `Surging` until
  decoder is `Idle` on the new revision (the previous-in-order
  Component finished its full state machine cycle)
- Operator gates each phase explicitly. If the operator wants engine
  to never start, they revert the engine half of the spec or leave it
  unchanged (Sequential only advances on per-Component spec bumps in
  the order's declared sequence)
- Composes with OEP-0011 `spec.traffic.rollout` step plans: each
  Component walks the full step plan (e.g., 10 → 50 → 100 with manual
  promotions) before the next Component starts its plan
- Failure in the early Component blocks all later Components: if
  decoder enters `Failed`, engine never starts — operator must
  either roll decoder back to a working state or revert the engine
  spec too

Different from BlueGreen + `order:` (which pins surge **sequence**
within a single rollout unit where the whole gang traffic-shifts
together) — Sequential walks the full lifecycle per Component, with
a wait at each step for the operator's confidence.

### The OEP-0011 Contract Gap

OEP-0011 Part 2 (rollout strategies) §Out of Scope assigns the
**producer side** of the canary contract to per-mode reconcilers
(OEP-0011 README:1101-1124). Specifically per-mode reconcilers must:

- Materialize per-revision pod sets and per-revision Services named
  `<isvc>-<component>-rev-<hash>`
- Ensure **generation-scoped internal discovery** so peer pods resolve
  same-revision peers (the PD hazard at OEP-0011 README:934-986)
- Populate `Status.Components[*].Traffic[]` with revision-pair weights
- Detect Ready timeout, honor `ScaleDownDelaySeconds`, etc.

OEP-0011's HTTPRoute weighted-backendRef consumer (shipped in PR
#147) reads `Status.Components[*].Traffic[]` and emits weighted
backendRefs to the per-revision Service names. Today it has nothing
to consume because **no per-mode reconciler produces those entries**.

OEP-0007 v1 is silent on the producer contract — zero references to
OEP-0011, `Status.Components.*.Traffic[]`, per-revision Services, or
generation-scoped discovery. OMENative v1 ships one Service per
Component (`<isvc>-<component>`) plus the existing headless peer DNS;
neither is revision-scoped.

OEP-0007.1 fills both gaps: ships the per-revision Services,
generation-scoped env-var injection, and the
`Status.Components.<c>.Traffic[]` writer. With this OEP merged and
implemented, OEP-0011 Part 2 alpha for OMENative graduates.

### Division of Responsibility with OEP-0011

OEP-0011 is **per-Component-shaped** — it answers "what % of traffic
goes to which revision and on what schedule." OEP-0007.1 is
**across-Components-shaped** — it answers "which Components must
coordinate the answer, in what order, with what coupling."

| Concern | OEP-0011 | OEP-0007.1 |
|---|---|---|
| What % of traffic goes to the new revision | `canaryTrafficPercent: 0-100` (simple) or `spec.traffic.rollout.steps[].trafficWeight` (step form) | — |
| When to advance / pause / promote | `spec.traffic.rollout.steps[].pause`, `ome.io/rollout-promote` | — |
| Per-Component rollout state | `Status.Components.<c>.RolloutPhase: Stable\|Canarying\|BlueGreenStandby\|Paused\|Failed` | — |
| Step plan + manual approval | `spec.traffic.rollout.promotionPolicy: Manual\|Auto` | — |
| HTTPRoute weighted backendRefs | Consumer (PR #147, shipped) | — |
| **Which Components roll together** | OEP-0011 asserts "PD components coupled" (README:138-142) but doesn't implement it | `policy: BlueGreen` enforces it |
| **What order Components roll** | — | `order:` (Sequential) or surge order (BlueGreen) |
| **Cross-Component pacing / ratio** | — | `pacing.type: RatioBalanced` |
| Per-revision pod sets + Services | Explicit out-of-scope (README:1101-1124) | `<isvc>-<c>-rev-<hash>` Services |
| Generation-scoped peer discovery | Out-of-scope | Env-var injection |
| `Status.Components.<c>.Traffic[]` writes | Consumer side | Producer side |

**Key insight:** OEP-0011's "PD components coupled" guarantee
(README:138-142) only becomes real when something enforces the
coupling at the workload layer. OEP-0007.1 is what makes it real.
Without OEP-0007.1, OEP-0011's `canaryTrafficPercent` on a
PDDisaggregated ISVC happily lets engine and decoder canary at
different percentages, which violates OEP-0011's own design intent.

**Composition rule:** OEP-0011's surface defines the **step plan**
(what %, when, with what pauses). OEP-0007.1's `groups[]` defines the
**Component coupling** (which Components walk the plan together,
sequentially, RollingUpdate, or independently). They're orthogonal:
any OEP-0011 traffic strategy composes with any OEP-0007.1
coordination policy. See [Recipes](#recipes-oep-0011--oep-00071-composition)
for the named operational patterns this produces.

### Goals

1. **Address the four PD rollout scenarios** with one extensible API
   surface on the ISVC. Operators express coordination intent
   declaratively; the controller picks the right state machine.
2. **Close OEP-0011's Part 2 producer contract for OMENative** —
   per-revision Services + generation-scoped discovery +
   `Status.Components.<c>.Traffic[]` writes — so OEP-0011 Part 2
   alpha-gate test items 10-20 pass on OMENative.
3. **Backwards compatible with OEP-0007 v1.** Default behavior is
   unchanged for ISVCs that don't declare `rolloutCoordination`.
   Coordination is opt-in via explicit declaration.
4. **Zero new user-visible CRDs.** Inherits OEP-0007's hard
   constraint. One new optional field on `InferenceServiceSpec`:
   `spec.rolloutCoordination`.
5. **No Kubernetes feature gate.** Inherits OEP-0007 / OEP-0011
   precedent. Opt-in is per-ISVC, not per-cluster.
6. **Reuse OEP-0007 primitives.** The four-level hierarchy
   (InferenceService → Component → Instance → Runner), the
   `ControllerRevision` stream, the `ome.io/serving` readiness gate,
   the migration verb. OEP-0007.1 adds a layer; it doesn't replace
   the kernel.
7. **Compose with OEP-0011 step-based rollouts.** Coordination policy
   is orthogonal to the simple vs step-based rollout form. Operators
   can declare `groups[]` regardless of which rollout form they use.

### Non-Goals

1. **Not a feature gate.** No new cluster-wide config; opt-in is
   per-ISVC.
2. **Not changing OEP-0007 v1's API.** No fields are renamed or
   removed; the v1 API is preserved.
3. **Not solving non-PD multi-Component cases not in the four
   scenarios.** Exotic patterns (e.g., "router and decoder paired but
   engine independent") are operator-expressible via the `groups[]`
   block but not exhaustively tested or documented.
4. **Not changing in-place upgrade eligibility.** OEP-0007's
   eligible-fields list (`containers[].image`, `imagePullPolicy`,
   `metadata.labels`, `metadata.annotations`) stays. OEP-0007.1 uses
   the labels-eligible rule to mutate `ome.io/revision-hash` during
   in-place upgrades that change the revision.
5. **Not auto-defaulting BlueGreen.** Backwards-compat requirement
   (OEP-0007 v1 ships first with Independent default) means OEP-0007.1
   keeps Independent as the default; BlueGreen is operator-declared.
6. **Not implementing rollback verbs.** Operator triggers rollback by
   reverting the spec; controller drives the rollback through the
   same state machine in reverse.
7. **Not solving cross-cluster coordination.** Single-cluster scope.
8. **Not solving step-based rollouts on per-mode reconcilers.**
   `spec.traffic.rollout` step-based form ships as the operator-facing
   API; the controller-side step state machine is OEP-0011's
   responsibility (currently API-only). OEP-0007.1's groups[] applies
   to the underlying coordination regardless of which rollout form
   triggers it.

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
│       ├── policy: BlueGreen | Independent | RollingUpdate
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
   OEP-0011 HTTPRoute builder (already shipped) reads
   Status.Components.<c>.Traffic[] and emits weighted backendRefs
   pointing at the per-revision Services this OEP creates.
```

Two layers cooperate:

- **Per-Component reconciliation** (OEP-0007 v1) — owns the
  Component's pods, ControllerRevisions, in-place vs recreate decision,
  drain, restart policy.
- **Coordination layer** (this OEP) — owns the cross-Component
  ordering, ratio enforcement, atomic gang surge/wait, and the
  Status.Traffic[] writes.

The coordination layer is a thin orchestrator over the per-Component
reconcilers. It does not own pods directly; it constrains when and how
the per-Component reconcilers fire.

### Scenario → API Mapping

| Operator intent | YAML |
|---|---|
| (1) BlueGreen three-way | `groups: [{components: [router, engine, decoder], policy: BlueGreen}]` |
| (2) All independent | `rolloutCoordination` omitted (default), or `groups: []` (explicit empty) |
| (3) Engine+decoder RollingUpdate, router independent | `groups: [{components: [engine, decoder], policy: RollingUpdate}]` (router unlisted → independent) |
| (4) Ratio-aware | Any group with `pacing: { type: RatioBalanced }` |
| (5) Sequential (decoder fully done before engine) | `groups: [{components: [decoder, engine], policy: Sequential, order: [decoder, engine]}]` |
| BlueGreen + ratio-aware | `groups: [{components: [engine, decoder], policy: BlueGreen, pacing: {type: RatioBalanced, maxSurge: 25%}}]` |
| Sequential + per-step pacing within each phase | `groups: [{components: [decoder, engine], policy: Sequential, order: [decoder, engine]}]` AND `spec.traffic.rollout.steps: [{trafficWeight: 10, pause: {}}, {trafficWeight: 100}]` (each Component walks the step plan before the next starts) |
| Multiple groups in one ISVC | `groups: [{components: [engine, decoder], policy: RollingUpdate}, {components: [router], policy: BlueGreen}]` (rare; router-alone BlueGreen is equivalent to Independent for a 1-Component group, but allowed for symmetry) |

### User Stories

#### Story 1 — Operator declares BlueGreen three-way for safety

PD ISVC with router + engine + decoder. Operator hasn't validated
that engine v2 talks safely to decoder v1, doesn't want to think
about it. Declares BlueGreen:

```yaml
spec:
  rolloutCoordination:
    groups:
      - components: [router, engine, decoder]
        policy: BlueGreen
        pacing: { maxSurge: 25% }
```

Operator bumps engine + decoder image tags via PR. Controller surges
25% new pods for all three Components in parallel, waits for all to
be Ready, shifts traffic atomically (via OEP-0011's HTTPRoute weighted
backendRefs), drains old pods, repeats. If any one Component fails to
reach Ready within timeout, all three roll back.

Operator-visible state on the ISVC:

```yaml
status:
  rolloutCoordinationStatus:
    groups:
      - name: "0"                     # auto-named by index
        components: [router, engine, decoder]
        phase: Surging               # Idle | Surging | Waiting | Shifting | Draining | ScalingDown | Paused | Failed | RollingBack
        observedSurge: 25%
        observedRatio:               # only present for RatioBalanced
          original: { router: 1, engine: 40, decoder: 20 }
          current:  { router: 1, engine: 40, decoder: 20 }
          newPods:  { router: 0, engine: 10, decoder: 5 }
```

#### Story 2 — Operator declares RollingUpdate for runtime co-released bumps

The runtime team co-releases engine and decoder with guaranteed
back-compat across minor versions. Router is on a different cadence:

```yaml
spec:
  rolloutCoordination:
    groups:
      - components: [engine, decoder]
        policy: RollingUpdate
        pacing: { type: RatioBalanced, maxSurge: 25% }
    # router not listed → rolls independently
```

Operator bumps engine image tag, decoder image tag, in the same PR.
Controller surges new engine pods and new decoder pods proportionally
(maintaining the 2:1 ratio for the 40:20 topology). During the
rollout window, the HTTPRoute fans engine traffic to **both** v1 and
v2 engine pods, and decoder traffic to **both** v1 and v2 decoder
pods. Cross-version calls are allowed because operator asserted
protocol back-compat.

Operator updates router separately on a different PR; that rollout
is independent (no coordination).

#### Story 3 — Operator declares Sequential for staged validation

PD ISVC with engine + decoder. Operator is rolling a new decoder
runtime that has rewritten KV-cache layout. Engine and decoder are
both at v1; operator wants to prove decoder v2 in production for at
least 24 hours before letting engine touch its v2 image (which has
matching KV-cache support but operator wants real evidence first):

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
          pause: {}                  # manual promotion
        - trafficWeight: 50
          pause: { duration: 30m }
        - trafficWeight: 100
      promotionPolicy: Manual
```

Day 0, 09:00 — operator bumps `spec.decoder.runner.containers[0].image`
to v2. Engine spec is unchanged.
- Decoder enters `Surging`. New-revision decoder pods come up at 25%
  surge.
- Decoder enters `Shifting` at step 1 (10% traffic, paused).
- Operator promotes step 1 → step 2 (50% traffic, 30min auto-pause).
- 30 minutes later, step 2 auto-advances to step 3 (100% traffic).
- Decoder enters `Draining` → `ScalingDown` → `Idle`. Decoder v2
  serving 100%.
- Engine is **not** allowed to surge during any of this even if its
  spec is bumped — Sequential blocks until decoder is `Idle`.

Day 1, 09:00 (operator soaked decoder v2 under prod traffic for 24h)
— operator bumps `spec.engine.runner.containers[0].image` to v2.
- Engine enters `Surging`. Walks the same step plan.
- Engine reaches `Idle`. Group rollout complete.

Operator-visible status throughout:

```yaml
status:
  rolloutCoordinationStatus:
    groups:
      - name: "0"
        components: [decoder, engine]
        order: [decoder, engine]
        phase: Sequential.AwaitingNextComponent   # day 0 end → day 1 start
        currentComponent: engine                  # next in order
        previousComponent: decoder                # most recently completed
```

If decoder v2 had failed mid-rollout, engine would never have
entered Surging. Operator can either revert decoder spec
(Sequential walks decoder back through RollingBack) or accept the
failure and revert engine spec too.

#### Story 4 — Migration verb (Alfred) in a coordinated group

Alfred wants to defragment Node3 by moving one engine Instance to
another node. Engine is in an BlueGreen group with decoder.

Migration verb (`Migrate(component=engine, instance=0)`) is rejected
with `MigrationRequiresGroupMigration` because moving one Component's
Instance independently would break the BlueGreen invariant. Alfred either:
- Falls back to a different node-drain strategy
- Triggers a full-group migration (planned for Alfred's next iteration)

For Independent or RollingUpdate groups, single-Component migration
proceeds as in OEP-0007 v1.

### Risks and Mitigations

**Risk: Coordination state machine adds reconcile-loop complexity.**
The state machine walks several phases per group per reconcile.
Mitigation: keep the per-Component reconcilers entirely unaware of
coordination — coordination layer constrains them via labels and
status writes, never reaches into their state. Each layer remains
unit-testable independently.

**Risk: RollingUpdate's "cross-version is safe" assertion may be
wrong.** Operator declares `policy: RollingUpdate` expecting protocol
back-compat; runtime regression breaks it; PD requests silently
corrupt. Mitigation: log per-Component image hashes and emit a
`MixedPairingObserved` metric when distinct revision pairs are seen
in flight, so observability surfaces the assertion at operator review
time. Operators can flip to BlueGreen if they don't trust the assertion.

**Risk: Ratio-aware pacing math is fiddly for non-integer ratios.**
40:20 = 2:1 is clean. 11:7 is not. Mitigation: ratio is enforced via
"don't allow new-pod-count / old-pod-count to drift past the original
ratio + tolerance" with a small tolerance (default 5%). Operator can
tune via `pacing.ratioTolerance`.

**Risk: Per-revision Services proliferate.** During a multi-revision
rollout, an ISVC may have 4-8 active per-revision Services. Mitigation:
GC tied to OMENative's existing revision retention policy
(`ome.io/revision-history-limit` default 3). Per-revision Service
exists only as long as at least one pod carries the matching label.

**Risk: In-place upgrades change pod revision labels mid-flight.**
OEP-0007 v1's in-place upgrade is per-Component; OEP-0007.1's
coordination layer must observe label flips correctly. Mitigation:
coordination's `Waiting` phase blocks on `pods.readyCount(rev=new) >=
expected`; the controller updates label and image as one patch so the
transition is atomic from the apiserver's perspective.

## API

### The `rolloutCoordination` Block

New optional field on `InferenceServiceSpec`:

```go
// pkg/apis/ome/v1beta1/inference_service.go (additive)

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
    // significant unless `order` is set (only meaningful for BlueGreen).
    // +kubebuilder:validation:MinItems=1
    // +listType=atomic
    Components []ComponentType `json:"components"`

    // Policy selects the coordination semantic for this group.
    // +kubebuilder:validation:Enum=BlueGreen;Independent;RollingUpdate;Sequential
    Policy CoordinationPolicy `json:"policy"`

    // Pacing controls how surge / drain pacing is computed within
    // the group. When omitted, defaults to per-Component pacing
    // (each Component uses its own omenative.updateStrategy.rollingUpdate).
    // +optional
    Pacing *CoordinationPacing `json:"pacing,omitempty"`

    // Order has two meanings depending on Policy:
    //   - BlueGreen: pins the surge sequence within the single rollout
    //     unit (decoder surges first, then engine, etc.) but all
    //     Components traffic-shift together once every set is Ready.
    //     Optional; default no ordering (parallel surge).
    //   - Sequential: REQUIRED. Pins the order in which Components
    //     walk their full state machine. The next Component cannot
    //     enter Surging until the previous Component is back at Idle
    //     on the new revision.
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
    // Type selects the pacing computation.
    //   - PerComponent: each Component's omenative.updateStrategy.
    //     rollingUpdate.maxSurge / maxUnavailable applies independently.
    //     Default.
    //   - RatioBalanced: pacing surges proportionally across all
    //     Components in the group, never letting the cross-Component
    //     ratio drift past the original.
    // +kubebuilder:validation:Enum=PerComponent;RatioBalanced
    // +optional
    Type CoordinationPacingType `json:"type,omitempty"`

    // MaxSurge is the maximum fraction (or absolute count) of pods
    // OMENative may add above the desired Component replica count
    // during a coordinated rollout. Applies group-wide.
    // Default: 25%.
    // +optional
    // +kubebuilder:validation:XIntOrString
    MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty"`

    // MaxUnavailable is the maximum fraction (or absolute count) of
    // pods that may be not-Ready at once during the rollout. Applies
    // group-wide. Default: 0 (no Component drops below MinReplicas).
    // +optional
    // +kubebuilder:validation:XIntOrString
    MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

    // RatioTolerance is the percentage drift from the original
    // cross-Component ratio that the RatioBalanced pacing allows
    // before refusing further surge. Default: 5 (5% slack).
    // +optional
    // +kubebuilder:validation:Minimum=0
    // +kubebuilder:validation:Maximum=100
    RatioTolerance *int32 `json:"ratioTolerance,omitempty"`
}

type CoordinationPacingType string

const (
    CoordinationPacingPerComponent CoordinationPacingType = "PerComponent"
    CoordinationPacingRatioBalanced CoordinationPacingType = "RatioBalanced"
)
```

### Coordination Policies

**BlueGreen**

All Components in the group surge → wait-ready → shift traffic →
drain → scale down **together**. The controller treats the group as
a single rollout unit:

- Surge new-revision pods for every Component in the group up to the
  group's `maxSurge` (subject to `RatioBalanced` if set)
- Block all Components' traffic shifts until **every** Component's
  new-revision pods are Ready
- Shift traffic on all Components simultaneously
- Block scale-down of any Component's old-revision pods until **every**
  Component's new-revision pods are still Ready (handles flap)
- If any Component enters Failed, the entire group enters Failed; all
  in-flight surges are reverted

`order: [decoder, engine, router]` (optional, default no ordering)
pins the surge sequence within BlueGreen: decoder surges first, waits for
its new pods to be Ready, then engine surges, then router. Useful when
upstream peers depend on downstream peers being available in the new
revision before they're allowed to surge.

**Independent**

Each Component rolls on its own per OEP-0007 v1 semantics. The group
declaration is essentially documentation — it asserts that the
operator has thought about it and decided coordination isn't needed.
The controller behavior is identical to omitting the group.

**RollingUpdate**

Components in the group must be at the same **spec generation**
(operator bumps engine and decoder image tags in the same PR), but
during the rollout window any vN pod can talk to any vM pod within
the group. The controller:

- Enforces spec-generation lockstep via admission (engine and decoder
  must both bump in the same generation if either bumps)
- Surges new-revision pods per Component as the per-Component pacing
  allows, in parallel
- Routes traffic via the **generic** Service (not per-revision) so
  cross-revision peer calls are allowed
- Drains old-revision pods per Component as the per-Component pacing
  allows
- Reports cross-pairing observability (`MixedPairingObserved` metric)
  so operators can audit that their back-compat assertion held

RollingUpdate is the operationally common case for runtime co-release
patterns (SGLang/vLLM ship engine + decoder together; protocol is
back-compat across minor versions).

**Sequential**

Components in the group walk their full state machine **one at a
time**, in the strict order pinned by `order:`. The next Component
in the order cannot enter `Surging` until the previous Component is
back at `Idle` on the new revision (i.e., its full Surging → Waiting
→ Shifting → Draining → ScalingDown cycle is complete).

Concretely for `policy: Sequential, order: [decoder, engine]`:

1. Operator bumps `spec.decoder.runner.containers[0].image` to v2.
   Decoder's coordination group enters `Surging`. Engine is **not**
   eligible to surge (even if `spec.engine.runner.containers[0].image`
   was also bumped in the same PR) until decoder reaches `Idle`.
2. Decoder walks Surging → Waiting → Shifting → Draining → ScalingDown
   → Idle. During this window, 100% of decoder traffic shifts to v2
   per the OEP-0011 step plan in effect (`canaryTrafficPercent`,
   `spec.traffic.rollout.steps[]`, or default rolling).
3. Operator observes decoder under prod load for as long as they want
   — minutes, hours, or days. Sequential makes no time guarantee;
   the soak window is whatever lag the operator chooses between
   advancing the engine spec.
4. Operator bumps `spec.engine.runner.containers[0].image` to v2 (or
   it was bumped in the same PR but held pending). Engine's
   coordination group enters `Surging`.
5. Engine walks the same cycle to `Idle`. Group rollout complete.

**Order is required** for Sequential — admission rejects
`spec.rolloutCoordination.groups[i].order` empty or omitted with
`SequentialRequiresOrder`. Admission also rejects `order` that
doesn't cover every Component in `components` with
`SequentialIncompleteOrder` (operator must be explicit about the
total order, not just a partial one).

**Failure blocks downstream Components.** If decoder enters `Failed`
(timeout on Ready, etc.), engine never starts — operator must
either roll decoder back to a working state (Sequential walks back
through `RollingBack` for the failed Component) or revert the
engine spec change too.

**OEP-0011 composition.** Sequential composes with
`spec.traffic.rollout.steps[]` per-Component: each Component walks
the full step plan (e.g., 10 → 50 → 100 with manual promotions)
before the next Component starts its plan. Operators get nested
gating: per-step manual promotion within each Component, plus
operator-controlled soak between Components.

Different from BlueGreen with `order:` (which still treats the gang as
one rollout unit and traffic-shifts everything together) —
Sequential walks the full lifecycle per Component, with an
operator-controlled wait at each Component boundary.

Use Sequential when:

- You want staged validation under real prod traffic before letting
  the next Component touch the new revision
- The protocol coupling between Components is strong enough that
  RollingUpdate's "any vN talks to any vM" isn't safe
- The rollout window is allowed to span days, not minutes

### Pacing

**PerComponent** (default when `pacing` is omitted)

Each Component's existing OEP-0007
`omenative.updateStrategy.rollingUpdate.maxUnavailable` and
`partition` apply independently. The coordination policy still
governs ordering and gang-readiness, but the surge / unavailability
math is computed per-Component, not coupled across the group.

The coordination-specific `pacing.maxSurge` is the
**group-wide surge ceiling** that this OEP adds on top — OEP-0007's
`rollingUpdate` has no `maxSurge` of its own because per-Component
rolling updates don't surge above declared replica count. Coordination
needs a surge budget to bring new-revision pods up before old ones
drain. Default `25%` of the largest Component in the group.

Use PerComponent pacing when:

- Component replica counts are similar (so independent pacing doesn't
  produce skew)
- You're using BlueGreen primarily for gang-readiness, not for ratio
  enforcement

**RatioBalanced**

The controller computes the original ratio of `desiredReplicas` across
Components in the group at rollout start (e.g., 40:20 = 2:1). During
the rollout, the controller refuses to surge new pods for one
Component if doing so would skew the new-vs-old ratio of any other
Component in the group past `(originalRatio × (1 ± ratioTolerance%))`.

Concretely for 40 engine : 20 decoder with `maxSurge: 25%` and
`ratioTolerance: 5`:

- Original ratio: engine:decoder = 2:1
- Tolerance band: 1.9:1 to 2.1:1
- Controller may surge: up to 10 new engine pods AND up to 5 new
  decoder pods in parallel (maintains 2:1 ratio exactly)
- Controller refuses: surging an 11th engine pod while decoder still
  has 5 new pods (would skew to 2.2:1, outside the band)
- Controller proceeds: when decoder surges its 6th new pod, engine
  can advance to 12 new pods (still 2:1)

This prevents the "40 new engine, 5 new decoder, starvation" pathology
the user identified. The math falls out cleanly because the
coordination layer is the only writer of new-pod counts within a
RatioBalanced group.

### Defaults

- `rolloutCoordination` omitted: equivalent to `groups: []`. All
  Components roll independently. **This is the v1.1 default**, preserving
  OEP-0007 v1 behavior for back-compat.
- `groups[]` declared with no `pacing`: each group uses
  `pacing.type=PerComponent` with `maxSurge=25%` and
  `maxUnavailable=0`.
- `policy: BlueGreen` with no `order`: Components surge in parallel; no
  intra-group sequencing.

No auto-detection. Operators who want BlueGreen must declare it.
Discoverability is via OEP-0007.1's quickstart doc + status
condition messages that nudge operators toward declaring groups when
their workload pattern would benefit (e.g., a PD ISVC without any
group declaration logs a hint event `ConsiderCoordinationGroup`).

## Design Details

### Per-Revision Services

For every active per-Component revision (i.e., every revision hash
that at least one pod carries), the OMENative controller maintains
two Services:

| Service | Name pattern | Type | Selector | Purpose |
|---|---|---|---|---|
| Per-revision routing | `<isvc>-<component>-rev-<hash>` | ClusterIP | `ome.io/inferenceservice=<isvc>, ome.io/component=<c>, ome.io/revision=<hash>` | HTTPRoute backend target (consumed by OEP-0011's weighted backendRefs); in-cluster addressable for direct revision targeting |
| Per-revision headless | `<isvc>-<component>-rev-<hash>-headless` | Headless | same | Pod-level DNS within the revision (multi-node tensor-parallel groups need per-pod addressability among the leader + workers of one revision) |

These coexist with OEP-0007's existing three-Service-per-Component
model — they are additive. The OEP-0007 Services (`<isvc>`,
`<isvc>-<component>`, `<isvc>-<component>-headless`) are
revision-agnostic (select all live pods regardless of revision); the
new per-revision Services are revision-scoped.

**Lifecycle:**

- **Created** by the OMENative controller when any pod with that
  revision hash is created. The Service Create is idempotent (Get
  first, Create on NotFound).
- **Owned** by the `InferenceService` (controller-ref + GC).
- **Garbage-collected** when no pod carries the revision hash AND
  OMENative's existing revision retention (`revision-history-limit`,
  default 3) has expired the revision. The two-condition gate avoids
  deleting Services that the HTTPRoute weighted-backendRef might
  briefly still reference during a rollout completion.

**Hash computation:** reuses OEP-0007's existing `ControllerRevision`
hash — the same value that's labeled onto pods as
`ome.io/revision-hash`. No new hash function.

**Quantity:** a single-revision Component carries 2 per-revision
Services (routing + headless). During a rollout with 2 active
revisions, that's 4 per-revision Services per Component, plus the 3
revision-agnostic Services OMENative already creates. For a PD ISVC
mid-rollout: router + engine + decoder = 3 Components × (3 existing
+ 4 per-rev) = 21 Services peak. Acceptable — Services are cheap.

### Generation-Scoped Discovery via Env Injection

The OMENative controller injects two env vars per (pod, peer-Component)
pair at pod creation time:

| Env var | Value | Used by |
|---|---|---|
| `OME_<PEER>_ENDPOINT` | `<isvc>-<peer>.<ns>.svc.cluster.local` (generic, revision-agnostic) | Runtimes in Independent or RollingUpdate groups — cross-revision peer calls are allowed |
| `OME_<PEER>_REVISION_ENDPOINT` | `<isvc>-<peer>-rev-<this-pod's-revision-hash>.<ns>.svc.cluster.local` (per-revision) | Runtimes in BlueGreen groups — pod talks only to same-revision peers |

The runtime image picks which to honor via a config flag or by
preferring `OME_<PEER>_REVISION_ENDPOINT` if set. Both env vars are
always populated; the runtime's role is to pick.

For a PD ISVC with router + engine + decoder, every engine pod
receives:

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

**Why env vars (vs ConfigMap mount or sidecar)?** Env vars are
read-once at process startup, which matches the LLM-runtime pattern
(SGLang, vLLM read peer addresses at startup; neither watches mounted
files or talks to sidecars). Env vars are immutable on a running pod,
which is correct: a pod's revision is fixed for its lifetime, and
peers it can talk to are determined by that revision.

For in-place image upgrades that change the revision hash (OEP-0007's
in-place updates `metadata.labels` as part of the eligible-fields
list), the env var stays the same — but the pod's `ome.io/revision`
label flips, so the pod is now selected by the new per-revision
Service. The old peer endpoint (in the env var) still resolves
correctly because either:
- It points at the generic Service (RollingUpdate) → resolves to any
  peer pod regardless of revision
- It points at the per-revision Service for the NEW revision (because
  the env was set based on the new pod's incoming revision, not the
  old) → resolves to new-revision peer pods

The latter is a subtle but important invariant: env vars are
**set at pod creation time** with the **incoming** revision hash. For
in-place upgrade, the incoming revision is the new one. For recreate,
same logic. Either way, the env points at the right per-revision
Service.

### Status.Components.*.Traffic[] Producer

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
        - revisionName: llama-70b-engine-rev-oldhash   # per-revision Service name
          percent: 75
          tag: prev
          latestRevision: false
        - revisionName: llama-70b-engine-rev-newhash
          percent: 25
          tag: latest
          latestRevision: true
      rolloutPhase: Canarying          # OEP-0011 field
```

OEP-0011's HTTPRoute builder (PR #147, `createWeightedHTTPRouteRule`)
reads this and emits weighted backendRefs:

```yaml
backendRefs:
  - name: llama-70b-engine-rev-oldhash    # per-revision Service this OEP creates
    weight: 75
  - name: llama-70b-engine-rev-newhash
    weight: 25
```

The producer ↔ consumer contract:

- Producer (this OEP) populates `RevisionName` with the **per-revision
  Service name** (not just the revision hash) so the consumer can use
  it as a Service backend reference directly.
- Producer populates `Percent` with the percentage of new-revision
  pods relative to total pods in the Component (the natural "what
  fraction of capacity is the new revision").
- Producer maintains `LatestRolledoutRevision`,
  `PreviousRolledoutRevision`, and `LatestReadyRevision` per
  OEP-0007's existing semantics.
- Consumer (HTTPRoute builder) reads the slice and emits weighted
  backendRefs. Already shipped.

For coordinated groups, the Status.Traffic[] writes are also
coordinated:
- **BlueGreen group:** controller writes Traffic[] for all Components in
  the group simultaneously (at step boundaries), so the HTTPRoute
  weights for every Component shift atomically. `latestRolledoutRevision`
  also advances atomically across the group.
- **RollingUpdate:** controller writes Traffic[] per-Component
  independently, but ensures all Components in the group remain at
  the same spec generation (otherwise admission rejected).
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

Per phase:

| Phase | What it does | Exit |
|---|---|---|
| `Idle` | No rollout in flight. Controller watches for spec generation bumps that affect any Component in the group. | Spec bump → `Surging` |
| `Surging` | Create new-revision pods up to `pacing.maxSurge`, subject to ratio constraint if `RatioBalanced`. For BlueGreen, surge all Components in parallel (or per `order:` if set). | All surge targets met → `Waiting` |
| `Waiting` | Block on `pods.readyCount(rev=new) >= surgeTarget` for every Component. Honor `ome.io/rollout-ready-timeout` annotation. | All Components Ready → `Shifting`. Timeout → `Failed` |
| `Shifting` | Write `Status.Components.<c>.Traffic[]` with new weights. OEP-0011's HTTPRoute builder picks up; weighted backendRefs shift. Emits `RouteBackendsUpdated` event. | Traffic[] writes succeed → `Draining` |
| `Draining` | Wait `scaleDownDelaySeconds` (default 30s, per OEP-0011) for in-flight requests to complete on old-revision pods. | Time elapsed → `ScalingDown` |
| `ScalingDown` | Delete old-revision pods (or scale Instances down) per pacing. Update `LatestRolledoutRevision` if rollout is complete. | Old pods gone, all replicas at new revision → `Idle`. More to roll → `Surging` (next batch) |
| `Failed` | Surge timed out or Ready watch errored permanently. Operator decides. | Operator reverts spec → `RollingBack`. Operator forces continue → `Idle` (new attempt) |
| `RollingBack` | Reverting in-flight surge: drain and delete new-revision pods, restore old-revision pods to original count. | Original state restored → `Idle` |
| `Paused` | Rollout is held at the current phase. Sources: (1) `ome.io/rollout-paused: "true"` annotation, (2) step boundary pause in an OEP-0011 step-based rollout (`pauseDuration`/manual resume). Pacing math and ratio enforcement still apply when the rollout resumes. | Annotation removed or pause expired → previous phase |

For Independent groups (or no group), each Component runs its own
state machine without coordination — equivalent to OEP-0007 v1.

For RollingUpdate, the per-Component state machines run in parallel
but observe a shared `groupSpecGeneration` invariant: if engine is in
`Shifting` for generation N, decoder must also be at generation N
(any spec edit that doesn't bump both is rejected at admission).

For Sequential, the group runs **one Component's state machine at a
time** in the strict `order:` declared. The coordination layer's
status surface exposes both the base state machine phase (for
metrics) and a composite operator-facing field:

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

| Composite phase | base `phase` | Meaning |
|---|---|---|
| `Idle` | `Idle` | No Component in the order has a pending spec bump. Steady state. |
| `<Component>.<base-phase>` (e.g., `decoder.Surging`) | the base phase | The currently active Component (`currentComponent`) is in this state machine phase. |
| `Sequential.AwaitingNextComponent` | `Idle` | Previous Component reached `Idle` on the new revision; next Component in the order has no pending spec bump yet. Operator-controlled soak window. Metric records `Idle`; composite reflects soak state. |
| `Sequential.Failed` | `Failed` | One Component entered `Failed`; subsequent Components blocked. |

The transition rule: a Component at order position `i+1` can only
leave `Idle` when:
- Component at order position `i` is at `Idle` on its latest spec
  generation, AND
- Component at order position `i+1` has a pending spec change that
  bumps its revision hash

If Component `i+1`'s spec is bumped before Component `i` reaches
`Idle`, the bump is **observed but not acted on** — the group
remains in `Sequential.AwaitingNextComponent` (or
`<previous-component>.<phase>` if previous is still in flight) until
the gating rule allows.

Metric labels use the base `phase` (single enum value, low
cardinality); the composite is operator-status surface only.
`compositePhase` is human-readable and changes shape across policies;
metrics need a stable enum.

### In-Place Upgrade and the Revision Label

OEP-0007 lists `metadata.labels` in the in-place-eligible field list
(README:773). OEP-0007.1 uses that to update the `ome.io/revision`
label as part of an in-place upgrade when the new ControllerRevision
hash differs from the old.

Concretely, an in-place upgrade that changes the container image (and
thus the revision hash) does both:
1. Patch `spec.containers[].image` to the new tag
2. Patch `metadata.labels["ome.io/revision"]` to the new hash

Both happen in a single `Patch` call so the apiserver applies them
atomically from the kube-controller-runtime watch perspective. The
pod immediately becomes selected by the new per-revision Service and
deselected by the old. EmptyDir-backed model weights survive the
container restart (OEP-0007's existing in-place value-add).

For the coordination layer, this means an in-place upgrade is
**indistinguishable** from a recreate-style upgrade from the per-revision
Service's perspective — the new revision's pod count increments
either way. The state machine doesn't need an in-place-aware branch.

**Exception:** if the in-place upgrade is for image-only changes that
do NOT bump the revision hash (e.g., a `:latest` tag re-pull with no
template change), the revision label stays. No coordination state
transitions are triggered. This matches OEP-0007's semantics
(generation didn't bump → ControllerRevision didn't bump → no rollout).

### Failure Handling

For BlueGreen groups:

- One Component's pods fail to reach Ready within
  `ome.io/rollout-ready-timeout` → whole group enters `Failed`
- `RolloutFailed` event emitted (OEP-0011 inheritance) with which
  Component failed and why
- New-revision pods retained for diagnosis (canary-preservation rule
  from OEP-0011 §Failure Handling)
- HTTPRoute backendRefs remain at the operator-set weight (gateway
  endpoint health excludes unhealthy backends from the LB pool)
- Operator recovery: revert spec → `RollingBack`; or set
  `canaryTrafficPercent: 0` (if using simple form) to drain the new
  revision while keeping the spec intact for re-attempt

For RollingUpdate groups:

- One Component's failure surfaces as that Component's individual
  `Failed` phase (per OEP-0007 v1)
- Other Components in the group are NOT rolled back automatically
  (RollingUpdate's contract is "mixable" — partial-revision states
  are OK)
- Operator decides whether to revert the failing Component
  individually or revert the whole spec

For Independent / no group: per OEP-0007 v1 semantics.

### Migration Verb Integration

Alfred's migration verb (`Migrate(component, instance)`, OEP-0007 +
OEP-0008) interacts with coordination as follows:

| Group's policy | Migration verb behavior |
|---|---|
| BlueGreen | Single-Component migration **rejected** with `MigrationRequiresGroupMigration`. Alfred falls back to a different defragmentation strategy or initiates full-group migration (planned for Alfred v2). |
| RollingUpdate | Single-Component migration **proceeds** as in OEP-0007 v1. The mixable contract makes single-Component migration safe (peer pods in the other Component are version-mixable). |
| Independent / no group | Single-Component migration proceeds as in OEP-0007 v1. |

Future: a `Migrate(group=<group-name>)` verb that drains and replaces
all Components in a group atomically. Out of scope for OEP-0007.1;
tracked as an open question.

### Webhook Validation

Admission rules added to the ISVC validator:

| Rule | Reason |
|---|---|
| Each Component appears in at most one `groups[]` entry | `DuplicateComponentInCoordinationGroups` |
| `groups[].components[]` references only `router`, `engine`, `decoder` (string-typed enum) | `InvalidComponentInCoordinationGroup` |
| Every Component listed in `groups[]` must have `ome.io/deploymentMode=OMENative` annotation | `CoordinationRequiresOMENative` |
| `policy: BlueGreen` with `pacing.type: RatioBalanced` and `pacing.ratioTolerance > 50%` | `RatioToleranceTooHigh` (admission warning) |
| `groups[]` non-empty but ISVC has no Components matching any group | `OrphanCoordinationGroup` |
| `groups[].order[]` references Components not in `groups[].components[]` | `InvalidCoordinationOrder` |
| `policy: RollingUpdate` with only one Component in `components[]` | `RollingUpdateRequiresMultipleComponents` |
| Spec edit bumps one Component's image in a `RollingUpdate` group without bumping the others | `RollingUpdateLockstepViolation` (rejects the Update) |
| `policy: Sequential` with `order` empty or omitted | `SequentialRequiresOrder` |
| `policy: Sequential` with `order` not covering every Component in `components` | `SequentialIncompleteOrder` |
| `policy: Sequential` with `order` containing Components not in `components` | `InvalidCoordinationOrder` (shared with BlueGreen) |
| `policy: Sequential` with only one Component in `components[]` | `SequentialRequiresMultipleComponents` (one-Component Sequential is equivalent to Independent; admission rejects to force operator to pick a meaningful policy) |

The OEP-0011 `MixedRolloutForms` rule (mutual exclusivity between
`spec.traffic.rollout` step-based form and per-Component
`canaryTrafficPercent`) continues to apply unchanged.

### Defaulter Behavior

The mutating webhook does **not** auto-populate `groups[]`. Defaults
are inert — absent `rolloutCoordination` means no coordination.

The defaulter does:
- Fill in `pacing.maxSurge: "25%"` when `pacing` is present without it
- Fill in `pacing.maxUnavailable: 0` when `pacing` is present without it
- Fill in `pacing.ratioTolerance: 5` when `pacing.type=RatioBalanced`
  without explicit tolerance
- Fill in `pacing.type: PerComponent` when `pacing` is present without
  a type

### Observability

**New conditions** on `InferenceServiceStatus`:

| Type | Status | Reason | Meaning |
|---|---|---|---|
| `RolloutCoordinationReady` | True | `Idle` | All groups are in `Idle` phase. No rollout in flight. |
| `RolloutCoordinationReady` | False | `InProgress` | At least one group is mid-rollout. Message lists the phases. |
| `RolloutCoordinationReady` | False | `Failed` | At least one group has Failed. |
| `RolloutCoordinationReady` | Unknown | `Pending` | Groups declared but controller hasn't observed them yet. |

**Events** emitted on the ISVC:

- `CoordinationGroupSurging` — group enters Surging phase
- `CoordinationGroupShifting` — group enters Shifting phase
- `CoordinationGroupCompleted` — group returned to Idle after a rollout
- `CoordinationGroupFailed` — group entered Failed phase
- `RatioSkewRejected` — Surge attempt rejected to avoid ratio drift
- `MixedPairingObserved` — RollingUpdate group observed cross-revision
  peer pairs in flight (informational; helps operators audit
  back-compat assertion)
- `SequentialComponentCompleted` — Sequential group's active
  Component reached Idle; next Component is now eligible to start
- `SequentialAwaitingNext` — Sequential group entered the soak window
  between Components; waiting for operator to bump the next
  Component's spec
- `SequentialNextSpecBumpDeferred` — operator bumped Component `i+1`
  while Component `i` was still mid-rollout; bump observed but not
  acted on yet
- `ConsiderCoordinationGroup` — informational hint emitted on multi-Component
  ISVCs without any declared group, suggesting coordination might help

**Metrics** (see `oep.yaml`):

- `ome_omenative_rollout_group_phase_total{isvc,group,phase}` — counter per phase entry
- `ome_omenative_rollout_group_transition_total{isvc,group,from,to}` — counter per transition
- `ome_omenative_rollout_group_failure_total{isvc,group,reason}` — failure counter
- `ome_omenative_ratio_skew_total{isvc,group}` — counter for RatioBalanced rejections
- `ome_omenative_per_revision_service_total{isvc,component}` — gauge of live per-revision Services
- `ome_omenative_mixed_pairing_total{isvc,group}` — count of distinct revision-pair combinations observed (RollingUpdate diagnostic)

**Structured logs:** each coordination reconcile emits
`{isvc, group, phase, components, observedRatio, surgeTarget}`.

## Backwards Compatibility

OEP-0007 v1 ships first. OEP-0007.1 lands as a strictly-additive
follow-up:

- **Spec additions are optional.** Existing ISVCs that don't declare
  `spec.rolloutCoordination` see zero behavior change.
- **Per-revision Services start being created** for OMENative-managed
  Components even when no group is declared. Services are
  back-compat (additive K8s resources); existing consumers that
  reference the revision-agnostic Services keep working. Existing
  consumers that reference the per-revision Services for the first
  time get the new resource.
- **Env var injection extends but doesn't replace** OEP-0007's
  existing env vars. New vars are `OME_<PEER>_ENDPOINT` and
  `OME_<PEER>_REVISION_ENDPOINT` per peer Component. Runtimes that
  don't read these are unaffected.
- **`Status.Components.<c>.Traffic[]` starts being populated** even
  when no group is declared (single-entry, `latestRolledoutRevision @
  100%`). OEP-0011's HTTPRoute builder reads this and emits a
  single-backend HTTPRoute pointing at the per-revision Service
  instead of the generic Service. Behavior is equivalent at the
  traffic level; the difference is the backend Service name.

**Important:** the third bullet — pointing HTTPRoute at the
per-revision Service even when no group is declared — is a meaningful
operational change. It means OMENative-managed Components will have
their HTTPRoute backends switch from `<isvc>-engine` (generic) to
`<isvc>-engine-rev-<hash>` (per-revision) when v1.1 ships. Two ways to
handle this:

- **Option A:** flip the switch in v1.1 unconditionally. The new
  Service exists, the HTTPRoute points at it. Cleanest design,
  one less code path.
- **Option B:** gate the HTTPRoute backend switch behind any
  `rolloutCoordination` declaration. ISVCs without `groups[]` keep
  the generic backend (today's v1 behavior).

I recommend **Option A**: the per-revision Service contains exactly
the same pods as the generic Service for a single-revision Component,
so routing behavior is identical. The benefit is that any time the
operator adds `groups[]`, the HTTPRoute is already routed through the
per-revision plumbing, so the transition is invisible.

## Coexistence with OEP-0011 Step-Based Rollouts

OEP-0011's `spec.traffic.rollout` step-based form is the
operator-facing API for "I want a multi-step canary plan." OEP-0007.1's
`spec.rolloutCoordination.groups[]` is the operator-facing API for "I
want these Components to coordinate."

They compose orthogonally:

- ISVC with `spec.traffic.rollout` but no `groups[]` → step-based
  canary per Component, independent
- ISVC with `groups[]` but no `spec.traffic.rollout` → coordinated
  rolling upgrades (no per-step pacing)
- ISVC with both → coordinated step-based rollouts; the controller
  walks the step plan with coordination policy applied at each step

For the both-set case, the controller-side step state machine
(currently API-only per OEP-0011 §Out of Scope) consumes the
coordination groups to decide gang vs per-Component surge per step.
Implementation is part of this OEP; the step-based form already has
its admission validation (per OEP-0011 webhook B3).

Mutual exclusivity rule (OEP-0011 `MixedRolloutForms`) applies
unchanged: an ISVC may use the simple `canaryTrafficPercent` form OR
the step-based `spec.traffic.rollout` form, not both. Coordination
groups apply to either.

## Recipes (OEP-0011 × OEP-0007.1 Composition)

OEP-0011 owns traffic-policy semantics (what % goes where, step
plans, manual promotion). OEP-0007.1 owns workload-coordination
semantics (which Components walk the plan together, in what order,
with what coupling). Common operational patterns are compositions
of one OEP-0011 setting and one OEP-0007.1 policy. This section
documents the named patterns so reviewers don't need to derive them.

### Recipe 1: PD-coordinated canary at 10%

**Operator intent:** "Canary the new revision at 10% for all PD
Components simultaneously. Engine and decoder must shift together;
no version mismatch across the pair."

```yaml
spec:
  rolloutCoordination:
    groups:
      - components: [engine, decoder]
        policy: BlueGreen
  engine:
    canaryTrafficPercent: 10           # OEP-0011 simple form
  decoder:
    canaryTrafficPercent: 10           # must match (BlueGreen enforces)
```

What happens: BlueGreen ensures both Components surge new-revision
pods together, traffic-shift to 10% together, and stay there
together. OEP-0011's per-Component `canaryTrafficPercent: 10`
defines the weight; OEP-0007.1's BlueGreen policy enforces that both
Components apply it in the same reconcile.

Without BlueGreen (today's v1 behavior): operator could set
`engine.canaryTrafficPercent: 10` and forget decoder, ending up with
engine at 10% v2 talking to decoder at 0% v2 — silent PD
mismatch. OEP-0011 README:138-142 declares "PD components coupled"
but doesn't enforce it; this recipe is how it gets enforced.

### Recipe 2: Blue-green at PD group level

**Operator intent:** "Stand up a full second copy of the entire PD
group on v2, validate via direct endpoint, then atomically cut over
all traffic from v1 to v2."

```yaml
spec:
  rolloutCoordination:
    groups:
      - components: [router, engine, decoder]
        policy: BlueGreen
        pacing: { maxSurge: "100%" }    # full second copy
  router:
    canaryTrafficPercent: 0            # standby: pods exist, 0% traffic
  engine:
    canaryTrafficPercent: 0
  decoder:
    canaryTrafficPercent: 0
```

Phase 1 (standby): operator bumps all three Component images, sets
`canaryTrafficPercent: 0` on each. BlueGreen ensures the standby pods
for all three Components come up together. Decoder rev-`<new>` pods
ready, engine rev-`<new>` pods ready, router rev-`<new>` pods ready
— but 0% traffic. Group enters `Shifting` then immediately stops at
the 0% weight; HTTPRoute weights are `100% old, 0% new`.

Validation phase: operator hits the per-revision Service
(`<isvc>-router-rev-<newhash>.<ns>.svc.cluster.local`) directly with
test traffic. Validates v2 is healthy.

Phase 2 (cutover): operator edits `canaryTrafficPercent` from 0 to
100 on all three Components in one apply. BlueGreen ensures all three
Components flip from 0% to 100% in the same reconcile.
HTTPRoute weights swap to `0% old, 100% new` atomically. Old pods
drain after `scaleDownDelaySeconds`. Done.

Rollback (if v2 has a problem): edit `canaryTrafficPercent` back to
0 on all three. BlueGreen flips together. Old pods are still alive
(within `scaleDownDelaySeconds`), so revert is instant.

### Recipe 3: Multi-step canary with PD coupling

**Operator intent:** "Walk the new PD revision through 10% →
soak → 50% → manual approval → 100%. Engine and decoder must walk
the plan together; promotion at each step applies to both."

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
          pause: { duration: 30m }    # auto-advance after 30m
        - trafficWeight: 50
          pause: {}                   # manual promotion
        - trafficWeight: 100
      promotionPolicy: Manual
      scaleDownDelaySeconds: 60
```

Operator bumps engine + decoder image tags. BlueGreen ensures both
surge together. OEP-0011's step state machine walks: 10% (auto
30m), 50% (manual pause), 100% (final). At each step, BlueGreen
enforces that engine and decoder's HTTPRoute weights advance
together — operator promotes once, both Components shift.

Mutual exclusivity (OEP-0011's `MixedRolloutForms`) means no
per-Component `canaryTrafficPercent` is set when using the step
form; BlueGreen policy doesn't change that rule.

### Recipe 4: Staged Sequential validation (cross-day rollout)

**Operator intent:** "Roll the new decoder to 100% under prod
traffic for 24 hours. Only then start engine. Engine v2 has
matching KV-cache support; decoder v2 carries the risky change."

```yaml
spec:
  rolloutCoordination:
    groups:
      - components: [decoder, engine]
        policy: Sequential
        order: [decoder, engine]      # required for Sequential
  traffic:
    rollout:
      steps:
        - trafficWeight: 10
          pause: {}                   # manual within each Component
        - trafficWeight: 50
          pause: { duration: 30m }
        - trafficWeight: 100
      promotionPolicy: Manual
```

Day 0: operator bumps decoder image. Decoder walks the full step
plan (10 → soak → 50 → soak → 100 → promote). Engine spec is
unchanged; engine doesn't roll. After decoder reaches Idle on v2,
group enters `Sequential.AwaitingNextComponent`.

Day 1 (after 24h soak): operator bumps engine image. Engine walks
the full step plan. After engine reaches Idle, group rollout
complete.

Composes naturally with the per-Component canary form too (set
`decoder.canaryTrafficPercent: 10`, observe, set to 100, then bump
engine spec and repeat).

### Recipe 5: Engine + decoder RollingUpdate, router rolls separately

**Operator intent:** "Engine + decoder are released together by the
runtime team with guaranteed protocol back-compat across minor
versions. Router is a CPU pod on a different release cadence."

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
keeps the asymmetric topology (e.g., 40:20) from drifting past 5%
of the original ratio. `MixedPairingObserved` metric exercises;
operator audits that protocol back-compat held.

Router updates happen on a different PR, roll independently of
engine + decoder.

### Recipe 6: Independent — today's v1 behavior (escape hatch)

**Operator intent:** "I know what I'm doing. Each Component is
fully independent. No coordination."

```yaml
spec:
  # rolloutCoordination omitted, or:
  rolloutCoordination:
    groups: []
```

Same behavior as OEP-0007 v1: each Component reconciles
independently, no cross-Component gating, no PD coupling
enforcement. Recommended only when operator has validated each
Component is safe to roll without peer awareness — which is
**not** true for default PD topologies.

The OMENative controller emits a `ConsiderCoordinationGroup`
informational event on multi-Component ISVCs without any declared
group to nudge operators toward Recipe 1 or 4.

## Test Plan

### Unit Tests

- `coordination/groups.go` — group resolution + Component → group
  membership lookup: ≥ 90%
- `coordination/state.go` — state machine transitions per policy: ≥ 90%
- `coordination/ratio.go` — RatioBalanced pacing math: ≥ 95% (boundary
  cases for integer + non-integer ratios)
- `coordination/discovery.go` — env var injection per pod, per peer:
  ≥ 90%
- `coordination/services.go` — per-revision Service create / Get /
  GC: ≥ 85%
- Webhook additions: ≥ 90%

### Integration Tests

Envtest harness with the OMENative reconciler + OEP-0011's existing
EG `BackendTrafficPolicy` CRD bundle. Per-revision Services are
typed (`corev1.Service`), so no new CRD vendoring.

**Per-policy correctness (alpha gate):**

1. Independent / no group → ISVC with engine + decoder, spec bump
   on engine alone → engine rolls, decoder unchanged.
2. BlueGreen two-Component (engine + decoder) → bump engine image →
   group enters Surging, blocks until decoder also has new pods
   (i.e., spec bump on decoder is required for the rollout to
   complete; if only engine bumps, group stays in Surging-Waiting
   until decoder bump).
3. BlueGreen three-way (router + engine + decoder) → bump all three
   image tags in one update → group walks Surging → Waiting →
   Shifting → Draining → ScalingDown → Idle. All Components transition
   atomically; HTTPRoute weights for all Components shift in the same
   reconcile.
4. RollingUpdate engine + decoder → bump both in one update →
   per-Component reconciliation, generic Service for cross-revision
   talk, `MixedPairingObserved` event fires when distinct
   engine-rev × decoder-rev pairs are observed.
5. RatioBalanced 40:20 → bump engine + decoder → controller surges
   pods proportionally, never lets ratio drift past 2.05:1. Assert
   via observed pod counts at each step.
5a. Sequential (decoder, engine) → bump decoder image alone →
    decoder walks Surging → ... → Idle, engine stays at v1. Bump
    engine image after decoder Idle → engine walks Surging → ... →
    Idle. Group phase reaches `Idle`.
5b. Sequential gating: bump decoder + engine images in the same
    apply → only decoder enters Surging. Engine bump is observed
    (event `SequentialNextSpecBumpDeferred`) but engine stays at
    v1 until decoder reaches Idle, then engine starts.
5c. Sequential failure block: bump decoder image with a bad image
    tag → decoder enters Surging → Waiting → Failed (timeout).
    Engine never enters Surging even after operator bumps engine
    spec. Operator reverts decoder spec → decoder walks
    RollingBack → Idle; engine spec bump then becomes actionable.
5d. Recipe 4 end-to-end (cross-day Sequential with step plan): bump
    decoder, decoder walks 10 → 50 → 100 step plan with manual
    promotions; assert engine doesn't move; advance clock /
    re-apply engine spec; engine walks the same plan.

**OEP-0011 producer contract (alpha gate):**

6. Single-revision Component (no rollout in flight) → `Status.Components
   .engine.Traffic[]` has one entry, `RevisionName=<isvc>-engine-rev-
   <hash>`, `Percent=100`. OEP-0011's HTTPRoute has one backendRef
   pointing at the per-revision Service.
7. Mid-rollout (2 revisions live) → `Status.Components.engine.Traffic[]`
   has two entries with proportional weights. OEP-0011's HTTPRoute has
   two weighted backendRefs.
8. Per-revision Service lifecycle: created on first pod with the
   revision hash, deleted after the last pod is gone AND the revision
   is past the retention limit.

**Discovery (alpha gate):**

9. Engine pod env vars include `OME_DECODER_ENDPOINT` (generic) AND
   `OME_DECODER_REVISION_ENDPOINT` (per-revision matching the pod's
   own revision).
10. In-place upgrade that bumps revision hash → env vars unchanged
    (per-revision endpoint already pointed at the new revision since
    the env was set at pod create time with the new revision); label
    flips, pod is reselected by the new per-revision Service.

**Migration verb (alpha gate):**

11. `Migrate(engine, instance=0)` on an BlueGreen-grouped ISVC → rejected
    with `MigrationRequiresGroupMigration` reason.
12. `Migrate(engine, instance=0)` on a RollingUpdate ISVC → proceeds
    per OEP-0007 v1 semantics.

**Webhook (alpha gate):**

13. Duplicate Component in groups → `DuplicateComponentInCoordinationGroups`
14. Component listed without OMENative annotation →
    `CoordinationRequiresOMENative`
15. RollingUpdate with one Component → `RollingUpdateRequiresMultipleComponents`
16. Spec edit that bumps only one Component in a RollingUpdate group →
    `RollingUpdateLockstepViolation`
16a. Sequential without `order` → `SequentialRequiresOrder`
16b. Sequential with `order` missing a Component listed in `components` →
     `SequentialIncompleteOrder`
16c. Sequential with one Component → `SequentialRequiresMultipleComponents`

**Failure handling (alpha gate):**

17. BlueGreen group with a bad image in one Component → all Components
    enter Surging, decoder reaches Ready, engine never does, group
    enters Failed after timeout. HTTPRoute weights unchanged (gateway
    excludes unhealthy backends). `RolloutFailed` event emitted.
18. Recovery from Failed: operator reverts spec → group enters
    RollingBack → new-revision pods drained → group back to Idle.

**Coexistence (beta gate):**

19. ISVC with both `spec.traffic.rollout` (step-based) and
    `groups[]: BlueGreen` → step state machine walks each step with
    BlueGreen group surge applied; per-step pause works as documented
    in OEP-0011.

### End-to-End (beta gate)

20. Real OMENative pods on envtest (with `KIND_EXPERIMENTAL_PROVIDER=podman`
    or similar) with router + engine + decoder. Hit the top-level
    URL with curl, observe sticky-session behavior, mid-rollout
    observe both old and new revision pods receiving traffic per the
    coordination policy.

## Graduation Criteria

### Alpha

- All unit tests passing.
- Integration tests 1-18 passing.
- OEP-0011 Part 2 alpha-gate tests 10-20 passing on OMENative (which
  this OEP enables — `Status.Components.*.Traffic[]` populated,
  per-revision Services available, HTTPRoute weights flow).
- Documentation: operator quickstart (`quickstart.md`), migration
  from OEP-0007 v1 (`migration.md`), API reference (`api.md`).
- At least one production-grade PD ISVC running on OMENative with
  `policy: BlueGreen` for ≥ 14 days without Sev1/Sev2 incidents.

### Beta

- Integration test 19 passing (coexistence with step-based rollouts).
- 2+ production PD ISVCs running with `policy: RollingUpdate` for
  ≥ 60 days; `MixedPairingObserved` metric exercised in real rollouts
  without operator-reported corruption.
- 2+ production PD ISVCs running with `pacing.type: RatioBalanced`
  for ≥ 60 days without starvation incidents.
- End-to-end test 20 passing on at least one supported platform.
- Migration verb integration with Alfred v2 (full-group migration)
  shipped or documented as deferred.

### GA

- 3 months beta with no regressions.
- OEP-0007 v1's "deferred until OEP-0007.1" language removed from
  OEP-0007 README (the deferral is no longer pending).
- Coordination group declaration is part of the recommended PD ISVC
  pattern in OME's operator guide.
- Alfred v2 full-group migration verb shipped.

## Implementation History

- 2026-05-20: Initial draft. Triggered by Phase 1 of OEP-0011
  completing and the realization that OEP-0007 v1's deferred
  coordination + OEP-0011's "per-mode producer" gap are the same
  set of work.

## Drawbacks

1. **Adds an orthogonal API surface on ISVC.** `spec.rolloutCoordination`
   is a new top-level field alongside `spec.traffic`. Two related
   surfaces with different scopes (traffic = data plane, coordination
   = workload). Operators must learn both. Mitigation: cross-link
   from each OEP's quickstart.
2. **RollingUpdate depends on operator-asserted protocol back-compat.**
   Wrong assertion silently corrupts PD requests. Mitigation: emit
   `MixedPairingObserved` event so operators see what was paired;
   document the assertion in the OEP and quickstart.
3. **RatioBalanced math has tolerance for non-integer ratios.** A
   40:21 topology can't enforce 2:1 exactly. Mitigation: configurable
   `ratioTolerance` knob with 5% default.
4. **Per-revision Service count grows with active revisions.** Bounded
   by `revision-history-limit` (default 3) × Components × 2 (routing +
   headless) — at most ~24 per ISVC. Cheap K8s objects, but visible
   in `kubectl get svc` output. Mitigation: distinct name pattern
   makes filtering easy.
5. **Coordination layer adds a state machine to the OMENative
   controller.** More reconcile-loop complexity, more places for bugs.
   Mitigation: keep the per-Component reconcilers (OEP-0007 v1)
   entirely unaware of coordination; coordination is a thin
   orchestrator. Each layer unit-testable independently.
6. **Sequential rollouts span operator-controlled time windows.**
   Sequential's defining property — the soak between Components is
   operator-driven, not time-bounded — means a half-completed
   Sequential rollout can sit in `Sequential.AwaitingNextComponent`
   for days. Mitigation: emit a `SequentialStaleSoak` event after a
   configurable threshold (default 7 days) so operators don't forget
   half-rolled fleets. Operationally also benign — the active
   Component is fully on its new revision; the inactive Component is
   still on its v1 revision.
7. **BlueGreen groups slow down rollouts.** Surging all Components in
   parallel + waiting for all to be Ready means rollout time scales
   with the slowest Component. Mitigation: BlueGreen is operator-opt-in,
   not the default. Operators who want speed use RollingUpdate or
   Independent.

## Alternatives

1. **Separate `InferenceServiceRolloutPolicy` CRD.** Considered and
   rejected — see [the design conversation](https://github.com/mslsrc/ome/pull/162)
   for the trade-offs. Summary: no demonstrated need for cross-ISVC
   policy reuse; OEP-0007's "no new CRDs" hard constraint stands.
2. **Per-Component `coupledWith[]` field instead of ISVC-level
   `groups[]`.** Rejected: more verbose for 3-way BlueGreen case (every
   Component needs to declare its peers); harder admission validation
   (bidirectional consistency must be enforced).
3. **Single ISVC-level `coordinationPolicy` enum (no groups).**
   Rejected: doesn't generalize to mixed scenarios (e.g., "router
   atomic with engine, decoder RollingUpdate with both"). YAGNI in
   the short term but blocks the door on operationally-real cases.
4. **Auto-detect BlueGreen for multi-Component ISVCs.** Rejected:
   backwards-incompatible with OEP-0007 v1's Independent default.
   Operators who want BlueGreen declare it.
5. **Per-revision ConfigMap mount for peer discovery.** Rejected: LLM
   runtimes (SGLang, vLLM) read peer addresses once at startup; they
   don't watch mounted files. Env var injection matches the actual
   runtime pattern.
6. **Sidecar-based discovery.** Rejected: every pod gains a sidecar
   container; runtime must call localhost API. Heavier than env vars
   for no functional benefit on LLM-runtime patterns.
7. **Step-based rollouts within OEP-0007.1.** Rejected: the operator
   API for step-based rollouts already exists in OEP-0011
   (`spec.traffic.rollout`). OEP-0007.1's coordination groups apply
   underneath either rollout form.
8. **Sequential as a flavor of BlueGreen (with `order:` mandatory).**
   Rejected during review. BlueGreen's defining contract is "the gang
   traffic-shifts together"; Sequential's defining contract is "one
   Component's full lifecycle, then the next." Trying to overload
   BlueGreen with `order: required` + "but actually walk full lifecycle
   per Component if order is required" produces ambiguous semantics
   for the partial-BlueGreen-order case. Sequential as its own policy
   keeps BlueGreen's contract clean.
9. **Blue-green / canary as new OEP-0007.1 policies.** Rejected.
   Blue-green and canary are traffic-policy semantics (% of traffic
   shifted) owned by OEP-0011. OEP-0007.1's role is to enforce that
   PD Components apply the traffic semantic together. The
   compositional pattern (e.g., Recipe 2 for blue-green) is the
   user-facing surface, not a new policy enum.

## Open Questions

1. **Full-group migration verb.** Alfred's defragmentation needs a
   way to move all Components in an BlueGreen group together. Current
   workaround: reject single-Component migration in BlueGreen groups,
   force operator to revert spec → wait → re-deploy. Better:
   `Migrate(group=<name>)` verb that drains and replaces atomically.
   Tracked for Alfred v2.
2. **Ratio computation for autoscaled Components.** OMENative v1
   doesn't ship HPA, so replica count is operator-declared. If
   future HPA shipping changes replica counts mid-rollout, the
   "original ratio" baseline drifts. Open question: snapshot the
   ratio at rollout start vs recompute every reconcile.
3. **Cross-group coordination.** An ISVC could declare multiple
   groups (e.g., `[{router, engine}, {decoder}]`). Each group rolls
   independently of the others. Should there be a cross-group
   ordering knob? Probably not (YAGNI), but worth flagging.
4. **Generic Service routing under BlueGreen.** When `policy: BlueGreen`,
   the generic Service (`<isvc>-<component>`) still selects all live
   pods regardless of revision. External callers hitting the generic
   Service mid-rollout will see a mix. Is that the right behavior, or
   should the generic Service flip to selecting only-current-revision?
   Probably the former (avoid load drops on the new-revision pods
   while they're Ready but old-revision pods are still draining),
   but worth documenting explicitly.
5. **RollingUpdate revision skew tolerance.** RollingUpdate's contract
   is "any vN can talk to any vM in the group." If the operator skews
   the spec generation across Components (intentionally or via Git
   conflict), should the controller catch it? Today's webhook rule
   `RollingUpdateLockstepViolation` rejects the spec edit, but doesn't
   detect drift that happens via separate edits to engine.image vs
   decoder.image. Open: add a status condition for `SpecGenerationSkew`?
6. **DAG / version-pair compatibility (deferred).** Today's policies
   express coupling (BlueGreen, Sequential, RollingUpdate) but not
   version-pair constraints. A future need: "engine v3 requires
   decoder ≥ v2 (back-compat with v2 and v3); engine v4 requires
   decoder = v4 (lockstep)." Forms a partial-order DAG over revisions
   per Component pair. YAGNI for alpha; flagged because real
   multi-version runtime fleets will eventually want it. Possible
   API shape: `groups[].versionConstraints: [{from: {engine: v3},
   requires: {decoder: ">=v2"}}]`.
7. **Asymmetric failure policy (deferred).** Today's failure rules
   are uniform per policy: BlueGreen fails the whole group, Sequential
   blocks downstream Components, RollingUpdate fails per-Component.
   Operators have asked for asymmetric: "independent going up, atomic
   on failure" (each Component rolls on its own, but if one fails,
   roll all back). Possible API: `groups[].failurePolicy:
   GroupRollback | ComponentRollback`. Flagged because the wire
   format is small but the controller-side implementation is fiddly
   (need to track "if I failed and a peer is still rolling forward,
   do I block its progress?").
8. **Sequential auto-advance.** Today Sequential's "next Component
   starts only when its spec bumps" is operator-driven. Some
   operators have asked for time-based auto-advance: "after decoder
   reaches Idle for N hours, automatically start engine if its spec
   has been bumped." Could be expressed via OEP-0011's
   `spec.traffic.rollout.steps[].pause: { duration: <d> }` applied to
   the Sequential boundary, but the boundary isn't a step today.
   Open: extend the soak window into the API, or keep it operator-driven?
