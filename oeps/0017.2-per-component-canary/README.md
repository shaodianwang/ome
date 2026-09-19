# OEP-0017.2: Per-Component Canary Plans

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Canary units](#canary-units)
  - [What an operator writes](#what-an-operator-writes)
  - [Risks](#risks)
- [Design Details](#design-details)
  - [Admission changes](#admission-changes)
  - [Status: one run per unit](#status-one-run-per-unit)
  - [Executor: from one group to N runs](#executor-from-one-group-to-n-runs)
  - [Why concurrent units are safe here](#why-concurrent-units-are-safe-here)
  - [Traffic and capacity](#traffic-and-capacity)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

Admission permits **at most one canary group per InferenceService**
(`pkg/validation/canary.go:70`) and requires that group to contain the external
entrypoint — the router when one exists (`canary.go:115`). An ISVC therefore
gets exactly one step ladder, one set of timeouts, and one set of gates, shared
across every Component the group lists.

This OEP raises that to one canary run per **canary unit**: the router is one
unit, and the engine+decoder pair is a second unit that stays internally
lockstepped. Each unit carries its own `steps[]`, its own `readyTimeout` and
`scaleDownDelaySeconds`, and its own `analysis` — sourced inline or from
separate `RolloutPolicy` objects. The singular `status.canary` becomes a run
per unit, recorded on the per-Component status the controller already keys
everything else by.

The single-group shape stays valid and behaves exactly as it does today.

## Motivation

**The failure mode: a router change and an engine change cannot be gated
differently, so one of them is gated wrongly or not at all.**

Router and engine fail differently and recover at different speeds, and their
correct gate parameters differ by more than an order of magnitude:

| | router | engine (vLLM on TPU) |
|---|---|---|
| pod ready, backends warm | ~23 s (p50 across the router pods of a dev cluster) | 40–90 min (weight load + XLA compile) |
| sensible `readyTimeout` | 10 m | 180 m |
| useful gates | routing overhead, pool health, per-revision 5xx | request-failure rate, e2e p95 vs the stable revision |
| cost of a spurious revert | seconds to re-roll | 40–90 min of recompute |

A single shared plan cannot serve both. Sizing `readyTimeout` for the engine
leaves a router that cannot start half-rolled for three hours; sizing it for
the router reverts every engine rollout before the first replica is Ready.

Today an operator has two bad options, and we have run both:

1. **Gate only the router** (`components: [router]`). A GitOps change
   that touches both specs then rolls the **engine ungated**. Observed
   directly: with `components: [router]`, bumping a label that alters both pod
   templates sent the router through its gated ladder while the engine was
   recreated immediately and recompiled for ~20 min with no gate on it.
2. **Gate both together** (`components: [router, engine]`). Legal, and it
   works — but the ladder and the gate set are shared, so the engine's 10 m
   `initialDelay` and 180 m `readyTimeout` are also the router's, and the
   router-specific gates are absent.

Neither is a configuration problem an operator can solve; both follow from the
one-group rule.

**The adjacent constraint that makes this tractable:** the analysis layer is
*already* per-Component. `analysis_sampler.go` builds each request with
`Component`, `CanaryRevision` and `StableRevision` for the Component being
evaluated, so a multi-Component group today runs the same gate list once per
Component against different revisions. The gate plumbing is component-aware;
only the *plan* is shared. This OEP mostly moves a boundary that the lower
layer already respects.

### Goals

- One canary run per canary unit, with independent `steps[]`, `readyTimeout`,
  `scaleDownDelaySeconds`, `initialDelay`/`interval`/`failureLimit`, and
  `analysis`.
- Each unit may source its plan from its own `RolloutPolicy` via `policyRef`,
  so a fleet can share one router policy and one engine policy.
- Keep the engine+decoder pair a single lockstepped unit — same steps, same
  gates, advancing together.
- Preserve the existing single-group behaviour bit-for-bit for ISVCs that do
  not opt in.

### Non-Goals

- Splitting engine and decoder into independent runs. They stay one unit. The
  P/D pair must keep a pairable engine+decoder serving through a transition;
  `pkg/validation/coordination.go:163` already rejects an uncoordinated
  pairing-protocol change, and that rule is retained and widened, not relaxed.
- Ordering or dependency between units ("roll the engine only after the router
  promotes"). Units are independent; sequencing is out of scope and called out
  in [Open Questions](#open-questions).
- Making `traffic` meaningful. It is inert today — see
  [Traffic and capacity](#traffic-and-capacity) — and this OEP does not change
  that.
- Any change to blueGreen or rollingUpdate groups.

## Proposal

### Canary units

**Definition — canary unit:** the smallest set of Components that must advance
together through a canary ladder. There are exactly two possible units on an
ISVC:

- **`router`** — when the ISVC declares a router.
- **`engine` (+ `decoder` when declared)** — always one unit. An engine-only
  ISVC has an engine unit of one Component; a P/D ISVC has an engine unit of
  two, lockstepped.

*Rationale for making the pair the unit rather than the Component:* a P/D
engine and decoder speak a pairing protocol to each other. Rolling them
independently can put a new-protocol prefill in front of an old-protocol decode
with no pairable peer, which fails requests rather than degrading them.
Component is the wrong granularity; the pair is the smallest safe one.

### What an operator writes

Two single-unit canary groups, each with its own policy:

```yaml
spec:
  rollout:
    groups:
      - components: [router]
        policyRef:
          name: router-standard
          progression: canary
      - components: [engine, decoder]
        policyRef:
          name: vllm-engine-standard
          progression: canary
```

Both groups are canary, which admission rejects today. Each pins its own run
and walks its own ladder.

### Risks

- **Two concurrent surges.** Both units may surge capacity at once, so peak
  headroom is the sum, not the max. For a tier with 10 routers and 32 engines
  at a 10% first step, that is 1 extra router plus 4 extra engine replicas
  (a 2x2x2 TPU slice each) rather than one or the other. Operators sizing
  quota for a rollout must plan for the sum. Mitigation options are in
  [Open Questions](#open-questions); at alpha we document rather than enforce.
- **Mixed outcomes.** The router unit can promote while the engine unit
  reverts. The result is a new router talking to the old engine revision.
  This is safe *because* the router/engine mesh is full (see below), but it
  means "the rollout succeeded" is no longer a single fact about the ISVC —
  tooling that reads one boolean must be updated.
- **More concurrent status writes.** Two runs both write
  `status.components[c]`. They touch different map keys so they do not
  conflict logically, but the write volume roughly doubles during a
  simultaneous roll.

## Design Details

### Admission changes

Three rules change. Each is stated with what breaks if it is wrong.

1. **`ReasonMultipleCanaryGroups` (`canary.go:70`) is narrowed, not deleted.**
   Today: more than one canary group is rejected outright. Proposed: reject
   only when two canary groups **overlap on a unit** — i.e. more than one
   canary group names Components from the same unit, or any canary group
   names Components from both units.
   *Failure mode if dropped entirely:* two canary runs driving the same
   Component would each believe they own its revision and step counter, and
   would fight over `status.components[c].traffic` — the same class of
   duplicate-write defect that makes the apiserver reject an entire status
   update and freeze reconciliation for the ISVC.

2. **The entrypoint rule (`canary.go:115`) becomes per-unit.** Today: the
   single canary group must contain the ISVC entrypoint (router if present).
   Proposed: a canary group must contain the entrypoint **of its own unit** —
   which for the engine unit is the engine. An engine-only canary group on an
   ISVC that has a router becomes legal.
   *Why that is sound, and the objection it has to answer:* the current rule's
   stated reason is that a group without the entrypoint "only shifts traffic on
   an internal Service, so the operator's steps never move real traffic." That
   is true of the `traffic` field and false of `capacity`. The router
   discovers engine endpoints by label selector with no revision term, so a
   step that puts 10% of engine replicas on the new revision does move ~10% of
   requests onto it — the split is capacity-driven and mediated by the router's
   own balancing, not by a gateway weight. Measured on a dev cluster: an ISVC
   with 50 engine pods on one revision and 50 on another had every router
   reporting a healthy-backend pool of 100, i.e. both revisions.
   *Calibration:* "~10%" is approximate. The realised share depends on the
   router's balancing policy and is not exact; a tier with uneven placement
   will see a different split than its capacity percentage. This is a real
   weakening versus a gateway weight and is why capacity steps, not traffic
   percentages, are the contract.

3. **The pairing-protocol rule (`coordination.go:163`) is widened.** Today it
   requires engine and decoder to share a group only when the protocol
   changes. Proposed: a canary group naming either of engine/decoder on an ISVC
   that declares both must name both, unconditionally.
   *Rationale:* the unit is the safety boundary; making it conditional on a
   protocol change leaves the pair splittable in every other case, which is the
   granularity this OEP has just argued is wrong.

### Status: one run per unit

`status.canary` is a single `*CanaryStatus` on the ISVC
(`inference_service_status.go:66`), carrying `CanaryRevisionHash`,
`StableRevisionHash`, `CurrentStep`, `ObservedTrafficWeight`,
`AnalysisFailedChecks`, `RolledBackRevisionHash` and the rest. One object
cannot represent two runs.

Proposed: add `Canary *CanaryStatus` to `ComponentStatusSpec`, so the run lives
at `status.components[<component>].canary`. Every Component in a unit carries
the same run state; the unit's entrypoint Component is authoritative and the
others mirror it.

`status.canary` is **retained and kept populated** for the single-unit case,
aliasing the entrypoint unit's run. *Rationale:* it is the field every existing
consumer reads, including the GitOps rollout tooling and the canary
runbook's `jq .status.canary`. Removing it in the same change that adds
concurrency would break readers for a reason unrelated to the feature. When two
units run concurrently, `status.canary` reflects the ISVC entrypoint's unit
only, and a new condition `MultiUnitCanaryInFlight` is set so a reader cannot
mistake it for the whole picture. Deprecation of the alias is deferred and
tracked in [Open Questions](#open-questions).

### Executor: from one group to N runs

`EffectiveCanaryGroup()` (`rollout_run_types.go:260`) returns *the first* canary
group and is documented as safe because admission guarantees at most one. It
gains a sibling `EffectiveCanaryGroups()` returning all of them; the singular
form is kept for the single-unit path and for callers that genuinely want the
entrypoint unit.

`primaryComponent()` (`dispatch.go:620`) picks router > engine > decoder across
the whole group. Under per-unit groups a group contains one unit, so the
function becomes "the entrypoint of this group" — the same ordering, scoped to
the group rather than the ISVC. For the engine unit that is the engine; the
decoder remains a secondary that stages capacity and gates the step without
carrying a traffic weight, exactly as today.

The dispatch loop runs once per canary group instead of once per ISVC. Each
iteration already receives its Component, revision pair and analysis block, so
the per-run work is unchanged.

### Why concurrent units are safe here

The property that makes two simultaneous canaries tolerable is that **the
router↔engine mesh is full**: routers discover engines through
`--label-selector=component=engine,ome.io/runner=leader,ome.io/inferenceservice=<name>`,
which carries no revision term. Old and new routers therefore talk to old and
new engines throughout. Verified on a dev cluster: routers on revision
`0ee53037` served a fleet whose engines were all on `91596017` — unrelated
hashes, pool fully populated — and a second tier mid-rollout had 50+50 engines
across two revisions with every router reporting all 100.

*The objection this has to answer:* "doesn't a concurrent router and engine
canary mean an untested combination serves traffic?" Yes — and it already does.
Because the mesh is full, a new router talks to old engines today, in the
single-group case, at every step before 100%. Per-unit runs do not introduce
the mixed-version combination; they only make its duration independent per
unit. What they do introduce is *two* revision axes moving at once, so the
number of live combinations goes from two to four. We consider that acceptable
for components that are already version-skew tolerant by construction, and we
do not propose it for components that are not.

### Traffic and capacity

`capacity` is per-Component and load-bearing. `traffic` is documented as "not
per-Component" and is, as of 2026-09-12, **inert**: on the clusters surveyed no
HTTPRoute carries weighted or per-revision backendRefs, even though per-revision
Services exist. The realised share tracks pod count.

This OEP does not fix that. It does mean per-unit runs are coherent without it:
each unit's exposure is its own capacity percentage, and the units do not
contend for a single traffic weight. If the weighted-backendRef consumer is
later wired, only the entrypoint unit can own the gateway weight, and the
engine unit stays capacity-driven — worth stating now so the later change does
not assume otherwise.

## Test Plan

- **Unit — admission.** Table tests for: two single-unit canary groups accepted;
  two canary groups overlapping a unit rejected; a canary group naming engine
  without decoder on a P/D ISVC rejected; an engine-only canary group on a
  router-bearing ISVC accepted; the existing single-group shapes unchanged.
- **Unit — executor.** `EffectiveCanaryGroups()` ordering and unit resolution;
  `primaryComponent()` scoped to a group; status written to the right
  Component keys; `status.canary` alias populated for one unit and the
  `MultiUnitCanaryInFlight` condition set for two.
- **Integration (envtest).** Two concurrent runs advancing at different rates;
  one unit reverting while the other promotes; neither run observing the
  other's step counter.
- **Integration (KIND).** The suite under
  `tests/integration/singlecluster/controller/omenative_coordination_kind/`
  is authoritative for pod-lifecycle timing. Add specs for concurrent
  router+engine canaries with real pods, including the mixed-outcome case.
  New spec files must be classified in `KIND_SPEC_GROUPS` in
  `.buildkite/generate_pipeline.py` or pipeline generation fails.
- **Regression.** Every existing canary spec must pass unchanged; the
  single-group path is the compatibility contract.

## Graduation Criteria

- **Alpha.** Per-unit groups accepted; concurrency gated behind a chart value
  (default off) so a cluster can take the status change without accepting two
  simultaneous runs. `status.canary` alias populated. KIND specs for the
  concurrent and mixed-outcome cases green.
- **Beta.** Concurrency on by default. Metrics
  (`ome_canary_concurrent_components`, `ome_canary_component_outcome_total`)
  published and dashboarded. At least one production tier running a
  router-unit and engine-unit policy pair for two weeks with no manual
  intervention attributable to the split.
- **Stable.** `status.canary` alias deprecation decided (see Open Questions)
  and, if deprecating, a release with both fields populated before removal.

## Drawbacks

- **It doubles the rollout state an operator must reason about.** "Is this ISVC
  mid-rollout" stops being one question. The `MultiUnitCanaryInFlight`
  condition mitigates but does not remove this.
- **Peak capacity is the sum of both units' surges**, which can matter on a
  TPU-constrained cluster where an engine replica is a 2x2x2 slice.
- **It adds an admission rule shaped like a graph constraint** (no two canary
  groups overlapping a unit), which is harder to explain than "at most one".
  The rejection message must name the unit, not just the Component, or the
  operator cannot act on it.
- **It does not solve ordering.** An operator who wants "engine first, then
  router" still cannot express it, and now has two independent runs that might
  interleave in an order they did not choose.

## Alternatives

1. **Per-Component gate sets inside one group.** Keep one canary group and one
   ladder; allow `analysis` to be specified per Component.
   *Pro:* smallest change; no concurrency, no status split, ordering stays
   trivially defined. *Con:* does not solve the parameter problem, which is the
   larger half — `readyTimeout` 10 m versus 180 m and `initialDelay` 5 m versus
   10 m are step-machine properties, not gate properties, and they remain
   shared. *Rejected* because it addresses the symptom we noticed first rather
   than the one that actually forces the bad choice.

2. **Query-template branching on `.Component`.** `RenderQuery` is plain
   `text/template`, so a gate can already branch with
   `{{ if eq .Component "engine" }}`. *Pro:* zero API change; works today.
   *Con:* same parameter problem as (1), and it encodes component dispatch in
   PromQL strings, which is unreviewable at scale. *Rejected as a solution*,
   though it remains a useful escape hatch and is worth documenting.

3. **A separate `spec.rollout.perComponent{}` map, bypassing `groups[]`.**
   *Pro:* the clearest possible surface for exactly this use case. *Con:* a
   second grammar for the same concept; `policyRef`, pinning and the
   progression one-of all already hang off a group, and duplicating them
   invites divergence. *Rejected* in favour of reusing the group grammar.

4. **Do nothing; tell operators to use `[router, engine]`.** *Pro:* free.
   *Con:* it is the status quo whose cost this OEP opens with — the engine's
   timeouts govern the router, and the router's gates are unavailable. Worth
   stating as a real option because the shared-group path does work and is
   what we would keep recommending if this OEP is rejected.

## Open Questions

- **Should the units be allowed to contend for capacity, or should the
  controller serialise their surges under a headroom budget?** Summing is
  simplest and is what this OEP proposes; a budget is more operable on a
  TPU-constrained cluster but needs a quota source we have not identified.
  `TBD` before beta.
- **Does `status.canary` get deprecated, and on what timeline?** Keeping it
  indefinitely as an entrypoint-unit alias is defensible and cheap; removing it
  is cleaner but breaks readers. No position taken here.
- **Is there demand for ordering between units?** Explicitly out of scope, but
  if the answer is yes the group grammar's existing `order` field is the
  obvious place, and `coordination.go:510` already documents that cross-group
  ordering is honoured only for single-Component blueGreen runs — so ordering
  canary units would need that rule revisited too.
- **Unverified:** this OEP assumes two concurrent runs do not contend on
  anything beyond `status.components[]`. That is reasoning from the code, not
  an observation — there is no cluster today running two canary units, because
  admission forbids it. The envtest concurrency spec in the Test Plan is what
  would turn this from an assumption into a fact, and it should be written
  before the admission change lands, not after.

**Next action:** agreement on the unit boundary (router | engine+decoder) and
on whether `status.canary` is aliased or replaced. Both are load-bearing for
the API shape and neither is settled here.
