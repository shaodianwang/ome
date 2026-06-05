# OEP-0015.1: Progressive (Canary) Rollout Executor

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Terminology](#terminology)
  - [The router-agnostic boundary](#the-router-agnostic-boundary)
  - [User stories](#user-stories)
- [Design details](#design-details)
  - [Admission: from blanket rejection to a real validator](#admission-from-blanket-rejection-to-a-real-validator)
  - [The step state machine](#the-step-state-machine)
  - [Capacity: per-revision pod counts](#capacity-per-revision-pod-counts)
  - [Traffic: the external weight](#traffic-the-external-weight)
  - [Pause and promotion](#pause-and-promotion)
  - [Scale-down, drain, and completion](#scale-down-drain-and-completion)
  - [Rollback](#rollback)
  - [Status and RolloutPhase](#status-and-rolloutphase)
  - [The PD same-revision pairing dependency](#the-pd-same-revision-pairing-dependency)
  - [Interaction with coordination and autoscaling](#interaction-with-coordination-and-autoscaling)
- [Test plan](#test-plan)
- [Graduation criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** OEP-0015 reserved `spec.rollout.canary` — the step shape, promotion
policy, and status phases — and admission-rejects it because no controller drives
it. This OEP builds that controller: a **router-agnostic canary executor** that
walks an InferenceService through the declared `Steps`, setting per-revision pod
**capacity** and the **external traffic weight** at each step, pausing for manual
(`ome.io/rollout-promote`) or timed (`Pause.Duration`) advancement, and writing
`RolloutPhase`. It reuses the per-revision Services and the HTTPRoute
weighted-backendRef path that blue-green already established, and adds no mesh and
no new data plane. It is fully coherent for single-component services on day one;
for prefill/decode (PD) it correctly drives capacity and external traffic, and
names the one remaining correctness item — same-revision internal pairing — as a
tracked dependency on a separate router contract, not part of this OEP.

## Motivation

OME can already roll a service three ways via `spec.rollout.coordination`:
RollingUpdate (parallel, mixed revisions tolerated), Sequential (one component at
a time), and BlueGreen (bring up a full new copy, then cut over atomically). What
none of them does is **shift a fraction of real traffic to a new revision and
hold there.** BlueGreen exposes 0% then 100% — the first real request hits the
whole fleet. The class of regression that only shows under live traffic — a bad
checkpoint, a sampling-parameter change, a quantization regression that degrades
output without crashing the pod — is exactly what a blue-green cutover cannot
catch before full exposure.

A progressive canary closes that gap: run the new revision at a small capacity,
route a small slice of traffic to it, and let an operator (or a timer) decide
whether to proceed. *Failure mode it prevents:* a silent quality regression
reaching 100% of users in one step, with no smaller blast radius to catch it
first.

OEP-0015 already settled the **shape** of this — `CanarySpec`, `RolloutStep`
(with `NewCapacity`/`OldCapacity` independent of `TrafficWeight`),
`PromotionPolicy` (Manual/Auto), `ScaleDownDelaySeconds` — and reserved it so
adding the executor later is not an API migration. Today `spec.rollout.canary` is
**admission-rejected** (`ValidateRolloutCanaryReserved`): an operator who sets it
gets `spec.rollout.canary is not yet implemented (RolloutCanaryReserved)`. This
OEP turns that reservation into a working feature.

### Goals

1. **Execute the reserved `CanarySpec` faithfully.** Drive an InferenceService
   through `Steps` in order, applying each step's capacity and traffic weight, and
   honoring its `Pause`. *Rationale:* the shape is already agreed; the value is the
   missing engine.
2. **Keep the executor router-agnostic.** Control only what OME owns without any
   serving-runtime cooperation: per-revision pod capacity and the *external*
   (client→router) traffic weight. *Rationale:* the canary must not depend on how
   a runtime discovers or routes internally; coupling there is what stalled this
   work before.
3. **Reuse, don't rebuild.** Drive traffic through the existing per-revision
   Services + HTTPRoute weighted-backendRef path (built for BlueGreen) and the
   existing lifecycle/capacity machinery. *Rationale:* a second traffic mechanism
   is a second thing to get wrong.
4. **Support Manual and Auto promotion, plus rollback**, via the reserved
   `ome.io/rollout-promote` / `ome.io/rollout-rollback` verbs.
5. **Be observable and reversible.** Write `RolloutPhase`, emit step events and
   metrics, and keep the old revision serving until the canary completes — a stuck
   step never takes the service down.
6. **Be coherent for single-component services on day one**, and drive PD
   correctly for the dimensions OME owns (capacity + external traffic).

### Non-Goals

1. **Metric-gated promotion / auto-rollback is out of scope — by deferral, not
   inability.** OME bundles Prometheus (`charts/ome-resources`, single-replica,
   short-retention, shipped so KEDA works out of the box) and integrates the
   prometheus-operator, so the metric *source* exists. What metric-gating adds is
   new API (per-step success/failure conditions) and a controller-side
   query-and-evaluate path the OME controller does not have today — KEDA, not the
   controller, queries Prometheus now. That is a real build and gets its own
   design: **OEP-0015.2**. This executor leaves a clean hook for it — promotion is
   a single decision point (see [Pause and promotion](#pause-and-promotion)) that a
   metric mode plugs into without touching capacity, traffic, or status.
   *Calibration:* the `Auto` policy here is *timed*, not health-aware — safe for
   attended or low-risk rollouts, but it does not by itself catch a regression.
   Auto-rollback-on-regression is exactly what OEP-0015.2 adds.
2. **No service mesh and no new internal data plane.** The executor does not
   inject proxies or weight component-to-component traffic.
3. **No new API.** This OEP implements the `CanarySpec` reserved by OEP-0015
   verbatim. Any field change is out of scope and would be its own follow-up.
4. **Does not deliver PD same-revision internal pairing.** Guaranteeing that a
   request served by a v2 prefill also uses a v2 decode requires a router contract
   that is explicitly a separate effort (see
   [the dependency](#the-pd-same-revision-pairing-dependency)). v1 drives PD
   capacity and external traffic; full PD internal coherence lands when that
   contract does.
5. **Not a replacement for coordination.** Canary and `spec.rollout.coordination`
   are mutually exclusive per InferenceService.

## Proposal

The executor is a controller path that activates only when an InferenceService
sets `spec.rollout.canary` and a new revision exists. It reads the declared
`Steps`, tracks progress in status, and at each step performs three operations in
a fixed order — **capacity → traffic → pause** — then advances. It writes
`RolloutPhase` throughout and tears the old revision down once the final step
(`trafficWeight: 100`) completes.

### Terminology

- **Revision**: a saved pod-template snapshot (a `ControllerRevision`,
  `<isvc>-<component>-<hash>`). A canary always runs exactly two: the *stable*
  (old) revision and the *canary* (new) revision.
- **Step** (`RolloutStep`): one stage of the rollout. Carries `NewCapacity` and
  `OldCapacity` (pod counts, as a count or percent of desired replicas),
  `TrafficWeight` (percent of external traffic to the canary revision), and an
  optional `Pause`. Capacity and traffic are *independent* — a step may run the
  canary at full capacity with 0% traffic (validate before exposing) or at low
  capacity with low traffic (symmetric canary).
- **Per-revision Service** (`<isvc>-<component>-rev-<hash>`): a Service whose
  selector targets one revision's pods. These already exist; the HTTPRoute
  weighted-backendRef consumer reads `status.components.<c>.traffic[]` and
  programs the gateway from them.
- **External traffic weight**: the client→router split, expressed as gateway
  HTTPRoute weights between per-revision Services. The one traffic layer OME
  controls without runtime cooperation.
- **`RolloutPhase`**: the per-component status enum reserved by OEP-0015
  (`Stable`, `Canarying`, `Paused`, `Promoting`, `Pending`, `Failed`,
  `RollingBack`). This executor is its first writer.

### The router-agnostic boundary

This is the load-bearing design decision, so it is stated up front.

A canary has two dimensions — **capacity** (how many pods of each revision) and
**traffic** (what fraction of requests reach the canary). OME owns capacity
outright (it is pod management). Traffic, for a PD service, has three hops:
client→router, router→engine, router→decoder. **OME owns only the first hop** —
the gateway HTTPRoute weight — because the internal hops are made *by the router,
directly to pod IPs that the router discovers itself.* OME is not on that data
path.

The executor therefore draws its boundary at exactly what OME can control with no
serving-runtime cooperation: **per-revision capacity and the external traffic
weight.** It does not attempt to weight or isolate internal hops.

The consequence, stated honestly: for a **single-component** service this is a
*complete, coherent* canary — there are no internal hops. For **PD**, the executor
correctly steps capacity and external traffic, but a v2 router can still discover
v1 engine/decoder pods, so a request could pair a v2 prefill with a v1 decode.
That pairing is unsafe across incompatible revisions and is the one correctness
item the executor cannot solve alone; it is treated as a named dependency on a
router contract (see
[that section](#the-pd-same-revision-pairing-dependency)), not smuggled into the
executor.

> **Anticipated objection.** *"If PD isn't fully coherent in v1, why ship it?"*
> Because the executor is the hard, reusable part, and it is correct for the
> dimensions OME owns. Single-component canaries work immediately; PD canaries
> work for capacity + external traffic and become fully coherent the day the
> pairing contract lands — with no change to the executor. Building the executor
> now does not commit us to any particular resolution of the pairing question.

### User stories

- **Story 1 — manual progressive shift.** An operator ships a new checkpoint with
  `steps: [{newCapacity: 10%, trafficWeight: 10, pause: {}}, {newCapacity: 100%,
  trafficWeight: 100}]` and `promotionPolicy: Manual`. The canary comes up at 10%
  capacity, 10% traffic, and holds. The operator watches their own dashboards,
  then `kubectl annotate isvc <name> ome.io/rollout-promote=<canary-revision>` to
  advance to 100%.
- **Story 2 — timed soak.** Same steps with `promotionPolicy: Auto` and
  `pause: {duration: 30m}` on the first step: the canary holds 30 minutes at 10%,
  then advances automatically. (Timed, not health-gated — the operator accepts
  that.)
- **Story 3 — validate at full capacity, zero traffic.** `steps: [{newCapacity:
  100%, oldCapacity: 100%, trafficWeight: 0, pause: {}}, {trafficWeight: 100, …}]`
  brings the canary fully up and Ready while serving no traffic, for a smoke check
  before any exposure.
- **Story 4 — abort.** A step looks wrong; the operator
  `ome.io/rollout-rollback`s; traffic returns to 100% stable and the canary pods
  scale to zero.

## Design details

### Admission: from blanket rejection to a real validator

Replace `ValidateRolloutCanaryReserved` (which rejects any `spec.rollout.canary`)
with a validator that accepts a well-formed plan and rejects a broken one. Rules,
each chosen to prevent a concrete failure:

- **`Steps` is non-empty** — an empty plan has nothing to execute.
- **The final step's `TrafficWeight` is 100** — otherwise the rollout never
  reaches the new revision and would hang forever.
- **`TrafficWeight` is in `[0,100]`** (already enforced by the type) and
  **non-decreasing across steps** — a canary that walks traffic backward is almost
  always a mistake; flag it. *(Open question: warn vs reject.)*
- **`NewCapacity`/`OldCapacity` resolve to ≥0**, and a step with `TrafficWeight >
  0` must have `NewCapacity > 0` — you cannot route traffic to zero pods.
- **`PromotionPolicy` is set when any step has a `Pause`** — otherwise a paused
  step is ambiguous (wait for what?).
- **Mutually exclusive with `spec.rollout.coordination`** — both is rejected; an
  ISVC rolls *either* by coordination groups *or* by a canary plan, never both.

### The step state machine

The executor tracks, in status: the **canary revision hash**, the **current step
index**, the **phase**, and the **time the current step was entered** (for `Auto`
timing). On each reconcile it computes the desired state for the current step and
converges toward it; it advances the step index only when the step's operations
are complete and its `Pause` is satisfied.

Per step, operations run in this fixed order:

1. **Capacity** — reconcile per-revision pod counts to `NewCapacity` /
   `OldCapacity` (see below). Wait until the canary revision's required pods are
   Ready before touching traffic. *Failure mode guarded:* shifting traffic to pods
   that aren't Ready yet would drop requests.
2. **Traffic** — write the step's `TrafficWeight` to
   `status.components.<c>.traffic[]`; the HTTPRoute consumer programs the gateway.
3. **Pause** — hold per [Pause and promotion](#pause-and-promotion).

A step with no `Pause` advances as soon as (1) and (2) converge.

### Capacity: per-revision pod counts

`NewCapacity`/`OldCapacity` are fractions (or counts) of the component's desired
replicas. The executor resolves them against each component's replica count and
drives the per-revision pod sets through the existing lifecycle/per-revision
machinery — the same code that brings up per-revision pods for BlueGreen. For a
multi-component (PD) ISVC, the fraction applies **per component**: `newCapacity:
10%` means 10% of engine, 10% of decoder, and 10% of router run the canary
revision. *Rationale:* the components form one stack; bringing them up out of
proportion would starve one side.

`OldCapacity` defaults to `100% - NewCapacity`. Setting both above 50% is a
deliberate capacity surge (extra headroom during the step) and is allowed.

### Traffic: the external weight

The executor writes one `RevisionWeight` per live revision into
`status.components.<c>.traffic[]`: `TrafficWeight` to the canary revision's
per-revision Service, `100 - TrafficWeight` to the stable revision's. The existing
HTTPRoute weighted-backendRef consumer turns those into gateway route weights.
This is the *external* (client→router) split.

Note the deliberate difference from BlueGreen: BlueGreen computes weights from
*live pod counts* (`ComputeWeightsFromPods`); canary writes the *explicit*
per-step `TrafficWeight`. That is the whole point of the capacity/traffic
independence — you can run the canary at 50% capacity but 10% traffic.

### Pause and promotion

- **`PromotionPolicyManual`**: at a step with a `Pause`, hold in phase `Paused`
  until the operator sets `ome.io/rollout-promote=<canary-revision-hash>`. The
  revision hash in the annotation value guards against a promote racing a newer
  spec change (promoting the wrong thing).
- **`PromotionPolicyAuto`**: at a step with `Pause.Duration`, advance once
  `now - stepEnteredTime >= Duration`. A step with no `Pause`, or `Auto` with no
  `Duration`, advances immediately.

Promotion advances exactly **one** step. The `ome.io/rollout-promote` annotation
is consumed (cleared) on advance so it is not re-applied to the next step.

> **Hook for OEP-0015.2.** Promotion is deliberately a single decision point —
> *"advance this step?"* Manual and Auto are two answers (annotation, timer); a
> future metric-gated mode is a third — *"advance when a Prometheus success
> condition holds; roll back when a failure condition holds"* — and plugs in here
> without changing capacity, traffic, or status. Designing that mode (the
> condition API + the controller-side query/evaluate path against the bundled
> Prometheus) is OEP-0015.2. Keep the decision point a clean interface so that
> layering is a drop-in.

### Scale-down, drain, and completion

When the final step (`TrafficWeight: 100`) is reached and its traffic weight is
programmed, the canary revision becomes the stable revision. The executor then
waits `ScaleDownDelaySeconds` — letting in-flight requests drain off the old
revision now that it receives no new traffic — and scales the old revision's pods
to zero. Phase goes `Promoting` → `Stable`. *Failure mode guarded:* scaling the
old revision down the instant traffic flips would kill requests still being served
on it.

### Rollback

`ome.io/rollout-rollback` aborts the canary: set `TrafficWeight` back to 0 (all
traffic to the stable revision), scale the canary revision to zero (after the same
drain delay), and set phase `RollingBack` → `Stable` on the stable revision.
Because the stable revision was never scaled below `OldCapacity` and never lost
traffic share it wasn't explicitly given, rollback is always available and cheap.

### Status and RolloutPhase

This executor is the first writer of `RolloutPhase`
(`status.components.<c>.rolloutPhase`):

- `Pending` — canary pods coming up, not yet Ready.
- `Canarying` — two revisions live, traffic split per the current step.
- `Paused` — held at a step boundary (Manual: awaiting promote; Auto: within
  `Duration`).
- `Promoting` — final step reached, draining + scaling the old revision down.
- `Stable` — one revision live at 100%.
- `Failed` — a step's canary pods did not reach Ready within the timeout; the
  stable revision keeps serving. Operator fixes the spec or rolls back.
- `RollingBack` — a rollback is in progress.

Status also carries the current step index and the canary revision hash so
`kubectl get isvc` shows where a rollout is. Metrics
(`ome_canary_step_total`, `ome_canary_phase_total`, …) mirror these transitions.

### The PD same-revision pairing dependency

This is the one thing the router-agnostic executor cannot guarantee, stated
plainly so it is not mistaken for solved.

In PD, the router receives a request and selects a prefill (engine) worker and a
decode (decoder) worker, then orchestrates a disaggregated exchange (prefill
computes a KV cache, hands it to decode). For that exchange to be correct, prefill
and decode must be **compatible** — same model, same KV format. Across an
*incompatible* revision (a new checkpoint, a quantization change), a v2 prefill
paired with a v1 decode produces wrong output or fails.

The router discovers engine/decoder pods by label
(`component=engine,ome.io/inferenceservice=<name>`), which is **not
revision-scoped** — so during a canary a router can pair across revisions. Making
this coherent requires *something on the router→engine hop to be revision-aware*,
and OME is not on that hop. The only places that awareness can live are: the
router (filter by revision, or consume an OME-provided per-revision backend), or a
proxy between them (a mesh — ruled out). There is no fully router-agnostic,
mesh-free option, because OME cannot influence a call it does not sit in.

**Scope decision:** this is a separate effort with its own contract, not part of
this OEP. The recommended resolution is the *router-dumb* one — OME hands the
router its revision-scoped backend (a per-revision Service/EndpointSlice OME
curates) and the router consumes it instead of self-discovering, so the router
never learns the word "revision." That is a one-time router-interface change
pursued with the runtime owners.

Until it lands, v1 canary is: **fully coherent for single-component services;
capacity + external-traffic for PD, safe for backward-compatible revisions.** The
validator SHOULD surface this — e.g. an advisory condition on a PD canary noting
internal pairing is not isolated — so operators are not surprised. *(Open
question: warn-only, or gate PD canary behind an explicit opt-in until the
contract exists.)*

### Interaction with coordination and autoscaling

- **Coordination**: mutually exclusive (enforced at admission). A canary plan is
  an ISVC-level rollout strategy; coordination groups are the alternative.
- **Autoscaling (OEP-0013)**: during a canary, replica counts are driven by the
  step's `NewCapacity`/`OldCapacity`, not by demand. The executor must therefore
  **suspend autoscaler-driven scaling for the ISVC while a canary is in
  progress** (or the autoscaler must defer, mirroring the read-only seam Alfred
  uses). *Failure mode guarded:* the autoscaler and the canary fighting over
  replica counts mid-step. *(Open question: exact suspend mechanism — annotation,
  status flag, or autoscaler-side check.)*

## Test plan

**Unit:**
- Validator: accepts well-formed plans; rejects empty steps, non-100 final
  weight, traffic-to-zero-capacity, coordination+canary, missing promotion policy.
- Step machine: step advancement on convergence; Manual hold until promote;
  Auto advance on duration; promote consumes the annotation and advances exactly
  one step; rollback returns to stable.
- Weight computation: explicit per-step `TrafficWeight` (not pod-count-derived).

**Integration (envtest):** a canary ISVC progresses Pending→Canarying→Paused;
a promote advances it; status/`RolloutPhase`/step index reflect each transition;
rollback returns to Stable.

**KIND (real kubelet):** end-to-end on a single-component service — capacity
steps bring up canary pods, gateway weights shift, Auto timing advances, the old
revision drains after `ScaleDownDelaySeconds` and scales to zero; a canary pod
stuck in `ImagePullBackOff` parks the rollout in `Failed` with the stable revision
still serving; rollback works from each phase.

**PD KIND:** capacity fans out per component and external weight shifts correctly;
the test asserts the *documented* v1 behavior (capacity + external traffic), and
explicitly does **not** assert internal same-revision pairing (that arrives with
the router contract).

## Graduation criteria

- **Alpha:** the executor for single-component services — validator, step machine,
  capacity, external traffic, Manual + Auto promotion, rollback, `RolloutPhase`,
  metrics. PD supported for capacity + external traffic with the pairing caveat
  documented and surfaced.
- **Beta:** PD same-revision pairing coherent via the router contract; the advisory
  caveat removed for contract-supporting runtimes. Soak in a real environment.
- **Follow-up (OEP-0015.2):** metric-gated promotion and auto-rollback — new API
  for per-step success/failure conditions + a controller-side Prometheus
  query/evaluate path, plugged into this executor's promotion hook. Not required
  for this OEP's GA.

## Drawbacks

- **Two revisions cost capacity.** A canary runs old and new concurrently; for
  large GPU stacks that is real headroom (mitigated by low `NewCapacity` early
  steps; `OldCapacity` can drop as the canary grows).
- **Timed Auto is not safety.** Without metric gates, `Auto` shifts traffic on a
  clock; an unattended `Auto` canary will happily advance into a regression. The
  doc and the `Non-Goals` are explicit about this; metric gating is the answer and
  is deferred.
- **PD is partial in v1.** Honestly a drawback until the pairing contract lands;
  see the dependency section.

## Alternatives

1. **Metric-gated from the start.** Build the analysis layer (per-step
   success/failure conditions, auto-promote/rollback against the bundled
   Prometheus) as part of this OEP. Deferred, not rejected: it is a larger build
   with its own API surface and a controller-side query/evaluate path, and it
   layers cleanly on this executor's promotion hook. It is planned as
   **OEP-0015.2**; keeping it separate lets the executor ship and be validated
   first.
2. **Internal traffic-split data plane (mesh/sidecars).** Independently weight
   router→engine and router→decoder. Rejected: a large new data plane, runtime
   overhead, and unnecessary — stack isolation (the pairing contract) plus the
   single external weight gives coherence without per-hop weighting.
3. **Capacity-coupled canary (no explicit traffic weight).** Let the external
   split follow pod counts. Rejected: it throws away the capacity/traffic
   independence the reserved API deliberately provides (validate-at-full-capacity,
   zero-traffic is impossible).
4. **Leave `spec.rollout.canary` reserved; rely on BlueGreen.** Rejected: it
   leaves the live-traffic-regression gap open, which is the whole motivation.

## Open questions

1. **Non-decreasing traffic weight** — reject a backward step, or warn? (Leaning
   reject; a backward canary is almost always an error.)
2. **PD caveat enforcement** — warn-only on a PD canary, or gate it behind an
   explicit opt-in until the pairing contract exists? (Leaning warn-only in Alpha,
   revisit for Beta.)
3. **Autoscaler suspend mechanism** — annotation, status flag, or an
   autoscaler-side "is a canary running?" check. Needs alignment with OEP-0013.
4. **The router contract itself** — the exact shape of "OME hands the router its
   revision-scoped backend" is deferred to its own design with the runtime owners;
   this OEP only depends on *some* such contract for full PD coherence.
5. **Multi-component capacity rounding** — how `NewCapacity: 10%` rounds for small
   replica counts per component (e.g. 1 replica), so a step never resolves to zero
   canary pods while `TrafficWeight > 0`.
