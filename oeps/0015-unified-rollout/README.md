# OEP-0015: Unified InferenceService Rollout (`spec.rollout`)

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Today's gaps](#todays-gaps)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [The three real axes](#the-three-real-axes)
  - [API shape](#api-shape)
- [Design Details](#design-details)
  - [Why a single policy enum, not type × ordering](#why-a-single-policy-enum-not-type--ordering)
  - [Coordination relocates unchanged; status stays frozen](#coordination-relocates-unchanged-status-stays-frozen)
  - [Canary is reserved and admission-rejected in v1](#canary-is-reserved-and-admission-rejected-in-v1)
  - [traffic.rollout is deleted; its status surface is kept reserved](#trafficrollout-is-deleted-its-status-surface-is-kept-reserved)
  - [Validation inventory](#validation-inventory)
- [Migration](#migration)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [Implementation History](#implementation-history)
<!-- /toc -->

## Summary

Rollout behavior for an `InferenceService` is spread across several overlapping
spec surfaces — `spec.traffic.rollout`, `spec.rolloutCoordination`, per-Component
`lifecycle.updateStrategy`, the (being-removed) `canaryTrafficPercent`, and
raw-mode `deploymentStrategy`. Two are inert and one overloads `spec.traffic` (a
load-balancing block) with progressive-delivery state.

This OEP consolidates the rollout surface into a single `spec.rollout` block and
returns `spec.traffic` to load-balancing only. The change is **spec-only and
coordination-status-preserving**:

- Cross-Component coordination — a shipped, heavily-tested feature — is
  **relocated unchanged** from `spec.rolloutCoordination` to
  `spec.rollout.coordination` (same group shape, same `CoordinationPolicy` enum,
  byte-identical `status.rolloutCoordination`).
- The inert `spec.traffic.rollout` step machine is **deleted**, its spec shape
  **reserved** under `spec.rollout.canary` (admission-rejected until a step
  executor exists), and its **status surface kept reserved** (not deleted).

This is feasible now because coordination, while shipped, has **no external
users yet** — so the relocation needs no long deprecation window, only a
one-release in-cluster conversion (below).

**Prerequisite:** this OEP depends on **the removal of `canaryTrafficPercent`
landing first** — that field, `ValidateCanaryPercent`, and the
`MixedRolloutForms` mutual-exclusion rule are still on `main` until that
removal merges (the change is drafted and ready to merge, not yet merged).

## Motivation

### Today's gaps

1. **Several overlapping rollout/traffic surfaces** with mutual-exclusion rules
   enforced only in webhooks, not the type system.
2. **`spec.traffic` is overloaded** — otherwise a steady-state load-balancing
   block, it also carries `rollout` (a multi-step capacity+traffic progression).
3. **"BlueGreen" means two things** — a cross-Component coordination policy
   (gang flip) and a single-revision capacity-ahead-of-traffic step plan.
4. **`spec.traffic.rollout` has no executor.** It has zero applied YAML and **no
   controller step execution** — but it is NOT consumer-free: `ValidateRollout`
   (a real webhook validator that checks `steps[]` capacity/weights and
   last-step==100), `crossRulePromoteWithoutRollout` (the `ome.io/rollout-promote`
   annotation cross-rule, `traffic_annotations.go`), and a `HasIntent()` boolean
   (`reconcilers/traffic/intent.go`) all read it. So it **validates a plan the
   controller never executes** — inert in behavior, not in surface. OEP-0011 is
   `provisional/alpha` and the step executor was never built.
5. **An over-clever alternative was rejected** (see [Alternatives](#alternatives)):
   splitting coordination into independent `type × ordering` axes. A pressure-test
   against the implemented behaviors showed the axes are not independent, so this
   OEP keeps the faithful single enum.

### Goals

- One home for rollout configuration: `spec.rollout`.
- `spec.traffic` is load-balancing only.
- Delete dead *execution* surface without shipping fields that advertise behavior
  the controller can't deliver.
- **No behavior change and no status change** — `status.rolloutCoordination` stays
  byte-identical, and the canary status surface is kept (reserved), not deleted.
- Reserve a future progressive-delivery (canary) slot honestly.

### Non-Goals

- Building the canary/step executor (operator-authored progressive traffic
  shifting). The slot is reserved but admission-rejected.
- BlueGreen failure teardown/rollback, or unifying the two coordination engines
  (the observability state machine vs the enforcement gate). Pre-existing gaps,
  untouched. *(Note: BlueGreen intra-unit surge `Order` is NOT a gap — it is a
  shipped, validated input; its admission validation is preserved, even though
  the runtime state machine does not currently act on it.)*
- Any change to per-Component `lifecycle.updateStrategy` or `spec.scalingPolicy`.

## Proposal

### The three real axes

| Concern | Scope | Field | Status |
|---|---|---|---|
| **Per-pod replacement** (surge / recreate / in-place) | per-Component | `spec.{engine,decoder,router}.lifecycle.updateStrategy` | shipped — **unchanged** |
| **Cross-Component coordination** (gang / rolling / sequential / independent) | per-group | `spec.rollout.coordination` *(was `spec.rolloutCoordination`)* | shipped — **relocated, unchanged** |
| **Progressive traffic shift** (canary steps) | service | `spec.rollout.canary` | **reserved**, admission-rejected |

`spec.traffic` is none of these — it is steady-state load balancing.

### API shape

Condensed shape:

```go
type InferenceServiceSpec struct {
    // ...
    Rollout       *RolloutSpec   `json:"rollout,omitempty"`
    Traffic       *TrafficSpec   `json:"traffic,omitempty"`        // LB only now
    ScalingPolicy *ScalingPolicy `json:"scalingPolicy,omitempty"`
    // REMOVED: RolloutCoordination (-> Rollout.Coordination)
}

type RolloutSpec struct {
    // Existing spec.rolloutCoordination, relocated UNCHANGED.
    Coordination *RolloutCoordinationSpec `json:"coordination,omitempty"`
    // Reserved step-based canary; admission-rejected in v1 (no executor).
    // Carries NO inner kubebuilder markers while reserved (see Design Details).
    Canary       *CanarySpec              `json:"canary,omitempty"`
}

// RolloutCoordinationSpec / RolloutCoordinationGroup / CoordinationPolicy
// {RollingUpdate,BlueGreen,Sequential,Independent} / CoordinationPacing:
// reused verbatim from coordination_types.go. Group shape + enum UNCHANGED.
// CanarySpec is the former TrafficSpec.Rollout, renamed (see name-collision note).

type TrafficSpec struct {                 // Rollout field removed
    Algorithm        *LoadBalancingType    `json:"algorithm,omitempty"`
    ConsistentHash   *ConsistentHashSpec   `json:"consistentHash,omitempty"`
    EndpointOverride *EndpointOverrideSpec `json:"endpointOverride,omitempty"`
}
```

**Name collision:** the existing inert step machine is *also* named `RolloutSpec`
(`rollout_types.go`). It must be renamed to `CanarySpec` first to free the name
for the new top-level `RolloutSpec`; this touches every `RolloutSpec`/`RolloutStep`
literal in the traffic tests and the deepcopy/openapi generation.

## Design Details

### Why a single policy enum, not `type × ordering`

An earlier draft split coordination into a service-level `type`
(`RollingUpdate|Canary|BlueGreen`) and a per-group `ordering`
(`Parallel|Sequential|Independent`). It is illusory factoring:

- `BlueGreen`, `RollingUpdate`, and `Canary` all imply parallel motion.
- The implemented `Sequential` walk is already a per-Component blue-green surge
  (it delegates each active Component to a synthetic single-Component
  `blueGreenTransition` in `state.go`), so `Sequential` is a coordination
  behavior, not "an ordering applied to a strategy."

So `ordering` is not an independent axis — most `(type, ordering)` cells are
contradictory (e.g. `BlueGreen + Sequential` = gang-parallel vs one-at-a-time),
and the split would need both new admission rules to forbid them and a new
`status.policy` derivation. This proposal keeps the existing four-value
`CoordinationPolicy` enum, which faithfully captures the four distinct
implemented machines, and pays none of that cost.

### Coordination relocates unchanged; status stays frozen

`spec.rolloutCoordination` moves to `spec.rollout.coordination` with **no shape
change**: `RolloutCoordinationGroup` (`Components`, `Policy`, `Pacing`, `Order`,
`Soak`) and the `CoordinationPolicy` enum are reused verbatim. Verified by the
migration sweep: **no status writer depends on the spec field path** — every
writer consumes the `ResolveGroups` output (group contents), so
`status.rolloutCoordination.*` is byte-identical and `status...groups[].policy`
stays a **direct mirror** (no derivation). Frozen-status holds because the
status group `Name` is index-derived (`strconv.Itoa(idx)`), independent of the
spec path.

It is still a **code change**, not zero-touch — four non-test read sites repoint
from `Spec.RolloutCoordination` to `Spec.Rollout.Coordination`
(`coordination/reconciler.go:93`, `core/render.go:81,85`,
`coordination/gate_context.go:63,71`), plus the validators and the defaulter.
And the **human-readable field-path strings** embedded in ~12 `coordination.go`
error messages and several doc comments (`spec.rolloutCoordination.groups[%d]…`)
must be rewritten to `spec.rollout.coordination.groups[…]`. (Keep all `Reason*`
constants; audit whether any test asserts on the full path vs the `Reason`.)

**Defaults are preserved, including a pre-existing fork to leave alone:** the
runtime resolver `pacingWithDefaults` (`groups.go`) defaults `MaxUnavailable` to
**25%**, but the webhook defaulter (`inference_service_defaults.go`) writes
**0**, and the type doc comment says **0**. This three-way inconsistency predates
this OEP and is out of scope — the reshape must not *change* any of them (just
repoint the read path). Pinning them consistently is a separable cleanup.

### Canary is reserved and admission-rejected in v1

`spec.rollout.canary` (`CanarySpec`) carries the former `TrafficSpec.Rollout`
fields. Admission **rejects** any ISVC that sets it ("`spec.rollout.canary` is
not yet implemented") until a step executor lands. To make that the *only* error
an operator sees, `CanarySpec` carries **no inner kubebuilder markers**
(MinItems/Enum/default/Minimum) while reserved — otherwise apiserver structural
validation/defaulting would fire before the webhook (e.g. `canary: {}` would hit
a MinItems error). Markers are re-added when the executor lands.

### `traffic.rollout` is deleted; its status surface is kept reserved

`TrafficSpec.Rollout` and the standalone step machine are removed: delete the
`ValidateRollout` step validator, the `crossRulePromoteWithoutRollout` annotation
cross-rule (and reconsider `ome.io/rollout-promote`'s contract), and the
`|| r.Traffic.Rollout != nil` clause in `HasIntent()`. `spec.traffic` becomes
LB-only.

The **canary status surface** — top-level `status.rolloutPhase`
(`inference_service_status.go`) and the `RolloutPhase` enum
(`traffic_status_types.go`: Stable/Canarying/BlueGreenStandby/Promoting/…) — has
no controller writer today. Rather than delete it (which *would* be a status
change), it is **kept reserved**, mirroring the reserved `spec.rollout.canary`,
so the future executor has its status ready and the "no status change" guarantee
holds end-to-end.

### Validation inventory

**New / changed:**
- Reject `spec.rollout.canary` ("not yet implemented") — replaces `ValidateRollout`.
- Reject `ratioBalanced` (RatioBalanced pacing) on any group whose policy does
  **not** honor it: it is only consulted for `RollingUpdate` (Sequential already
  rejects it; BlueGreen/Independent silently ignore it). State the rule as
  "RatioBalanced is only valid with `policy=RollingUpdate`" rather than the
  narrower (and partly redundant — RollingUpdate already requires ≥2)
  "requires ≥2 Components."

**Preserved (re-pointed at `spec.rollout.coordination`), full list:**
`ValidateCoordination`, `ValidateCoordinationUpdate` (the RollingUpdate
all-or-nothing image-bump lockstep — a *separate* `ValidateUpdate` entry point
needing `oldSpec`), `ValidateSequentialMultiPodOrder` (`isvc.go`), the
zero-budget pacing rule (`validatePacingNotZeroBudget`), the duplicate-/complete-
Order rules, the **BlueGreen Order-subset** check (`validateOrderSubset`), and
`CoordinationRatioToleranceWarning`. Keep every `Reason*` constant. The runtime
safety-net `ValidateGroupShape` (`groups.go`) mirrors these — add the new
RatioBalanced rule to **both** layers, per the existing pattern.

## Migration

No external users today, but persisted in-cluster ISVCs (the live
`config/samples/inferenceservice.yaml`, the PD-disaggregated sample, ~40 test
fixtures) **do** carry `spec.rolloutCoordination`. A hard rename would make their
coordination silently stop being read. So a **one-release conversion is
recommended, not optional**: have the defaulter accept `spec.rolloutCoordination`
and copy it into `spec.rollout.coordination` (emit a deprecation warning), for
one release, then remove.

Blast radius (from the migration sweep):

- **Relocate `spec.rolloutCoordination` → `spec.rollout.coordination`:** the ~15
  reconciler files (4 read sites repointed; status writers unchanged), the
  defaulter (`inference_service_defaults.go`), `pkg/validation/coordination.go`
  (read paths + ~12 field-path message strings) and `pkg/validation/isvc.go`
  (`ValidateSequentialMultiPodOrder`), the perm suite (`cases_test.go`) and both
  webhook suites (re-key the spec writes), and the live sample. **The ~38 KIND
  coordination specs largely compile unchanged** — the spec write is centralized
  at `fixtures_test.go` (one edit); the spec files only build the unchanged
  `RolloutCoordinationGroup`.
- **Delete `spec.traffic.rollout`:** `traffic_types.go`, `traffic_rollout.go`
  (delete), `intent.go` (`HasIntent`), `traffic_annotations.go`
  (`crossRulePromoteWithoutRollout` + the `ome.io/rollout-promote` contract),
  the `howto-update-and-roll.md` canary section, and the webhook/validation tests.
  Free the `RolloutSpec` name (rename to `CanarySpec`) before adding the new
  top-level `RolloutSpec`.
- **Helm:** the `ome-isvc` chart values already use a `coordination:` key, but the
  *rendered* CRD field (`ome.io_inferenceservices.yaml`) re-nests under `rollout:`.
- **Regenerate:** `make manifests && make generate`. The CRD/OpenAPI/swagger/
  deepcopy diff is **large but wire-neutral** (new `spec.rollout` subtree, removed
  `spec.rolloutCoordination` + `spec.traffic.rollout`); the `dep-crds`/manifest
  drift check is the gate. This also drives the **spec-gen → Python SDK + API
  reference** regeneration chain.

Sequencing: **the `canaryTrafficPercent` removal merges first** → free the
`RolloutSpec` name + delete `traffic.rollout` → relocate coordination with the
conversion defaulter. Status output unchanged throughout; run envtest + the
KIND coordination suite locally before merge (CI runs `make test-kind`
soft-fail).

## Test Plan

- Re-key the KIND coordination suite + coordination unit tests onto
  `spec.rollout.coordination`; assert `status.rolloutCoordination` output is
  **byte-identical** to the pre-change baseline (the frozen-status guarantee).
- Conversion: an ISVC applied with legacy `spec.rolloutCoordination` is defaulted
  into `spec.rollout.coordination` and behaves identically (one-release window).
- Webhook: `spec.rollout.canary` rejected with the friendly message (and `canary:
  {}` is NOT pre-empted by an apiserver structural error); `ratioBalanced` outside
  `RollingUpdate` rejected; every existing coordination rejection case passes
  against the new path.
- `dep-crds`/manifest drift after regen; spec-gen/openapi round-trips.

## Graduation Criteria

- **Alpha:** coordination relocated (+ conversion defaulter), status verified
  identical, `traffic.rollout` removed, `canary` reserved+rejected with its status
  surface kept.
- **Beta/stable:** tied to coordination's own graduation; the reserved `canary`
  slot graduates separately when a step executor is built.

## Drawbacks

- Test/sample/doc churn on a shipped feature (mostly mechanical; the conversion
  defaulter prevents in-cluster breakage).
- `spec.rollout.canary` (and its kept `status.rolloutPhase`) are reserved-but-
  unusable — mitigated by a clear admission error.

## Alternatives

1. **`type × ordering` split** — rejected; the axes are not independent, needs new
   validation + status derivation, identical behavior.
2. **Delete `traffic.rollout` only; leave `spec.rolloutCoordination` in place** —
   smaller, but no single rollout home; `rollout` config stays split.
3. **Status quo** — keeps the overloaded `spec.traffic` and the scattered surface.

## Open Questions

1. **Conversion window length** — one release of `spec.rolloutCoordination`
   acceptance, or longer? (Recommended: one release, given no external users but
   live in-cluster objects.)
2. **Reserved-field name** — `spec.rollout.canary` vs `progressive` vs `steps`.
3. **Pacing cleanup** — keep `CoordinationPacing` verbatim (this OEP) or flatten
   `pacing.type=RatioBalanced` to a bool + promote `maxSurge`/`maxUnavailable`;
   and whether to fix the MaxUnavailable default fork (doc/webhook 0 vs runtime
   25%) as part of that. Separable follow-up.

## Implementation History

- **Prerequisite change (pending merge)** — remove
  `spec.engine.canaryTrafficPercent` (this also removes `ValidateCanaryPercent`
  and the `MixedRolloutForms` rule). Ready to merge, not yet merged.
- **This OEP** — relocate coordination, delete `traffic.rollout` (keep its status
  reserved), reserve `canary`.
