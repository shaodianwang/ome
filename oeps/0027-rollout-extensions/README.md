# OEP-0027: Rollout extensions — pairs, splits, targeted rollback, windows, cross-cluster progression, capacity loss

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
- [Terminology](#terminology)
- [Goals and Non-Goals](#goals-and-non-goals)
- [Proposal 1: prefill and decode as a pair](#proposal-1-prefill-and-decode-as-a-pair)
- [Proposal 2: long-lived two-revision serving](#proposal-2-long-lived-two-revision-serving)
- [Proposal 3: rollback to any retained revision](#proposal-3-rollback-to-any-retained-revision)
- [Proposal 4: rollout windows](#proposal-4-rollout-windows)
- [Proposal 5: cross-cluster progression](#proposal-5-cross-cluster-progression)
- [Proposal 6: capacity loss mid-roll](#proposal-6-capacity-loss-mid-roll)
- [API changes](#api-changes)
- [Ordering and dependencies](#ordering-and-dependencies)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** Six things operators ask a rollout to do that `spec.rollout` cannot
say today. Each becomes one field, or one member of an existing one-of, on an
object that already exists. None adds a CRD. None lets the controller pick a
strategy the user did not write: every extension either moves information
earlier (a token, a condition) or adds a switch whose absence is exactly
today's behavior. Five of the six change nothing in the workload engine; the
one that does is named, and the frozen instance table (OEP-0020.1) plus the
123 replay goldens (OEP-0020.2) are how each claim is checked.

| # | Name | The one field | Engine change | Lock |
|---|---|---|---|---|
| 1 | Prefill and decode as a pair | `spec.rollout.groups[].paired: true` | **Yes** — two denials in the pairing gate (coordination side); instance table untouched | KIND `pd-topology-isolation`; replay cannot reach it (single-Component harness) |
| 2 | Long-lived two-revision serving | `spec.rollout.groups[].split: {capacity, traffic}` (fourth one-of member) | No — held rows already rest at `Ready × spec.partition = defer` (`cells-noop.yaml:256`); one candidacy note edited | replay `ready-held-by-partition-indefinite`; KIND `canary-lifecycle` |
| 3 | Rollback to any retained revision | `ome.io/rollout-rollback: <revision-name>` (existing annotation takes a value) | No — the adapter already resolves any CR name to the roll target (`ir/reconciler.go:457-502`) | existing replay `T-ready-rollback-surge`, `ready-rollback-inplace`, `ready-rollback-recreate`; new `ready-rollback-two-back`; KIND `canary-lifecycle` |
| 4 | Rollout windows | `spec.rollout.window` and `RolloutPolicy.spec.window` (one type, two homes) | No — the engine sees `spec.paused`, an input it already models (`cells-noop.yaml:250`) | envtest fake clock; KIND `pause-cancel-interleavings`; goldens do not move |
| 5 | Cross-cluster progression | `spec.placement.rollout: {order, soak, onFailure}` (hub only) | No — members run an ordinary derived ISVC that is paused or not | envtest `tests/integration/multicluster`; KIND `scaleout_kind` |
| 6 | Capacity loss mid-roll | `spec.rollout.groups[].onCapacityLoss: {action, threshold}` plus a derived condition | No — condition derived from IR rows; `Hold` is a run-level gate, never a hold token | replay `surge-capacity-loss-multi`; envtest; KIND `runtime-retry-recovery` |

## Motivation

Each paragraph is the real problem the investigation found and the defect it
exposed. *Fixed independently of this OEP* marks a bug that ships as its own PR
whether or not this OEP is accepted.

**1. A prefill/decode rollout mixes revisions in every progression.** Every
engine pod reaches decoders through the stable Service `<isvc>-decoder`, whose
selector is isvc + component + managed-by — every revision
(`components/base.go:1120-1137`). OEP-0007.1 designed
`OME_<PEER>_REVISION_ENDPOINT` so blue-green pods talk only to same-revision
peers and says "both are always populated"
(`oeps/0007.1-omenative-coordinated-rollout/README.md:842-852`). Production
never emits it: the render hook passes a nil hash function because a peer's
hash is unknowable at render time (`omenative/coordination/discovery.go:91-95`,
`:107-113`; `omenative/core/render.go:105`). So a blueGreen `[engine, decoder]`
group with both images bumped serves mixed pairs for the whole roll, and the
`GroupBlueGreen` doc's "flips traffic atomically" (`rollout_types.go:125-127`,
`:240-243`) describes the external hop at best. The doc correction is
independent; the variable itself is Proposal 1, because only a run-level token
can be known at render time.

**2. A canary paused at 50% for a month reads as stuck.** The IR `Ready`
condition says `Unknown / RolloutInProgress` all month
(`inferencereplica/status.go:715-719`). The `Staged` branch that would say
`True` exists (`status.go:706-714`) but reads `spec.pacing.partition`
(`status.go:481-501`), which nothing projects: the projector preserves `Pacing`
and the only writer sets `rollbackToRevision` (`irprojector/projector.go:362`;
`omenative/canary/dispatch.go:301-304`); the real partition travels as
`lifecycle.updateStrategy.rollingUpdate.partition` (`canary/effective.go:71-99`).
*Fixed independently of this OEP:* `stagedAtPartition` reads the lifecycle
partition, as `IRPartition` does (`reconcilers/irprojector/irstatus_read.go:39-48`).
Second defect: the canary computes the split from `MinReplicas`
(`canary/dispatch.go:632-657`) while the projector preserves the HPA's
`spec.replicas` (`projector.go:497-516`), so every scale-out lands on the new
revision and 50/50 drifts. *Fixed independently of this OEP.*

**3. Rollback can only reach the pre-canary stable, only while the canary is
armed.** `ome.io/rollout-rollback` is a boolean at admission
(`pkg/validation/traffic_annotations.go:62`) and the executor accepts only
`"true"` (`canary/promotion.go:18-25`); blueGreen and rollingUpdate groups have
no rollback verb (`canary/reconciler.go:195-197`). Yet the IR primitive takes
any ControllerRevision name (`inferencereplica_types.go:286-293`) and the
instance table already has `spec.rollbackTarget[set|clear]`
(`machines/instance/events.yaml:59`). Only the chooser is missing.

**4. Nothing keeps a rollout out of business hours.** The only hold is the
user's `ome.io/rollout-paused` annotation, set and cleared by hand or by a
CronJob that then fights a human unpause.

**5. A spec change on a hub ISVC reaches every member in one loop.** `fanOut`
writes the new spec to every connected home in one pass
(`placement/controller.go:958-985`, `:1203`); nothing reads a member's run
outcome first. The manual verbs are stripped from derived copies
(`placement/constants.go:74-81`) and the hub warns that a placed manual gate
can only be advanced on each member (`rolloutpreflight.go:375-393`). Worse:
`ome.io/rollout-paused` is *not* stripped, and `applyDerived` merges
annotations with `mergeOwnedKeys`, which overlays keys and never deletes
(`controller.go:1212-1230`) — a pause set on the hub cannot be cleared from the
hub. *Fixed independently of this OEP*, and a prerequisite for Proposal 5.

**6. When surge targets lose their nodes, the run cannot tell loss from slow
model load.** The canary sees `readyCanary < newCount`, enters `Pending`, and
fails at `readyTimeout` (`canary/reconciler.go:387-403`, `:620-635`); the only
signals are `RolloutPhase=Pending` and, in the Terminating shape,
`PodForceDeleted` events. Two engine gaps surfaced alongside. The Unknown-phase
recovery `recoverUnknownPhaseTargets` is called from `ops/restart.go:399` and,
through `recoverUnknownPhaseInstances` (`ops/unknown_phase.go:107-117`), from
`ops/create.go:246`, never from the surge path, so a proven-dead Unknown surge
target burns its deadline unheld — the gap recorded at
`cells-update-surge.yaml:60-62`. *Fixed independently of this OEP.* And a
single-pod surge can never earn a relocation directive: the deadline path hands
the disposition the whole index bucket, source and target
(`evidence/stuckpod.go:78-81`), so `SingleLiveNode` returns `""` whenever they
sit on different nodes (`evidence/terminating.go:266-284`;
`escalation/disposition.go:165-169`); only a gang surge relocates
(`cells-update-surge.yaml:50` is the gang case). *Fixed independently of this
OEP.*

## Terminology

Each term is defined once; the consequence is part of the definition.

- **Component / Instance / IR**: a serving role (`engine` = prefill in a PD
  topology, `decoder`, `router`), materialized as one InferenceReplica (IR)
  whose reconciler owns its pods; an Instance is one replica — a pod or a gang
  — and one row of the frozen instance table.
- **Revision / retained revision**: one ControllerRevision per Component, a
  hash of that Component's own pod template; retained = in the live set
  (current, update, every row's running/target, `revision.go:809-831`) or among
  the newest N under the configured limit (`inferencereplica/retention.go:15-97`).
- **Group / progression / run**: one `spec.rollout.groups[]` entry and its
  one-of `canary | blueGreen | rollingUpdate` (OEP-0017); a run is the pinned
  execution of the plan for one target set, opened at `rolloutrun.go:316`,
  closed `Completed | RolledBack | Superseded` (`rollout_run_types.go:33-45`).
- **Step boundary**: where an in-flight operation finishes its current step
  (`Surge`, `SurgeDrain`, `InPlace`, `Drain`, …). A pause acts at the boundary,
  never inside a step (`workload/holds/doc.go:27-30`): a hold set at 04:59 lets
  a step begun at 04:58 finish after 05:00.
- **Hold token**: the short string an external authority writes on
  `Operation.Waiting` (`Paused`, `NodeUnknown`, quota, scheduler, PodGroup,
  source-unrouted), which parks the InstanceReadyTimeout
  (`types/scheduling.go:41-44`). A row with a token starts no new operation —
  which is why Proposal 6 must not use one.
- **Gate**: a read-only check before one Instance's update starts; a denial is
  the table event `gate.deny` with one of seven names
  (`Plan, Pairing, Ratio, Sequential, Budget, RetryBlock, Held`).
- **Pairing protocol / pair / cohort**: the operator's opaque token
  `spec.rollout.pairingProtocol` (`rollout_types.go:72-86`), folded into engine
  and decoder revision hashes and stamped on pods (`inferencereplica_types.go:113-122`;
  `projector.go:443-447`). A pair is one engine and one decoder that may serve
  a request together; a cohort is the set of pods carrying the same token —
  today "same operator token", after Proposal 1 "same run".
- **Split**: two revisions of one group serving side by side as a steady state
  with a fixed capacity share and traffic weight; no ladder ends it.
- **Sticky reject**: the hash a rollback refused, kept so clearing the
  annotation never retries it (`canary/reconciler.go:256-272`).
- **Window**: a daily wall-clock interval in a named IANA zone, on named days,
  inside which a rollout may *advance*; outside it nothing new starts.
- **Hub / member / derived ISVC**: the control-plane cluster running the
  placement controller; a workload cluster registered as a `WorkloadCluster`;
  the copy of the hub ISVC the hub writes onto a member (`placement/derive.go:29`),
  an ordinary ISVC to that member (OEP-0018).
- **Rollout-control field**: an IR spec field the composer (the ISVC controller
  steering a rollout) may write — `pacing.*`, `paused`, `pauseMode`,
  `pairingProtocol` (OEP-0026 §4). Every IR write below is to one of these.

## Goals and Non-Goals

Goals: the six Summary rows, each with semantics an engineer can implement
without asking, each locked in the lane that can reach it.

Non-goals: **reloadable rollout config (idea 7) and image pre-pull (idea 8)**,
dropped by the owner. **No strategy inference** — the controller never derives
a progression, window, order or hold the user did not write; every default is
"what happens today". **No new CRD, no new annotation key.** **No
cross-cluster traffic weighting** — `TrafficMap` weights homes from admitted
capacity and lists per-revision canary weights as a non-goal
(`oeps/0025-multi-cluster-routing/README.md:126-129`); Proposal 5 sequences
runs, it does not steer requests. **No merged reconciler and no general group
sequencer** — OEP-0017's open item and OEP-0017.3 (PR #1653) respectively.

## Proposal 1: prefill and decode as a pair

### What happens today

Motivation 1, plus the gate. `CheckPairing` runs first in the stack
(`coordination/gate_eval.go:25-29`, `:77-83`) but is inactive unless the group
spans engine and decoder, the token is non-empty, and some serving Instance
runs a *different* non-empty token (`pairing.go:121-136`, `:187-189`); active,
it promises only that **at least one** pairable pair remains
(`pairing.go:69-73`, `:241-256`) — a floor, not a fence. The stable Service
never filters on the token (`services.go:277-281`, `:292-294`), each old pod
drains as soon as **its own** replacement is Ready (`workload/ops/update.go:172-175`),
and the canary's secondary gate holds only the *external* weight
(`canary/dispatch.go:444-449`).

### The change

**Field.** `spec.rollout.groups[].paired: true` (bool, default false).
Admission: components exactly `[engine, decoder]`, progression `blueGreen` or
`canary` — the shape the pairing-protocol rule already requires
(`pkg/validation/coordination.go:135-165`); rejected on `rollingUpdate`, whose
definition is mixed revisions (`rollout_types.go:130-133`).

**Cohort token.** At run open (`rolloutrun.go:316`) the controller computes
`cohort = digest(pairingProtocol, engineTargetHash, decoderTargetHash)` and
projects it as `IR.spec.pairingProtocol` on both Components in place of the
bare operator token (`projector.go:443-447`); the operator token is folded in,
so its equality contract survives. The token already enters both hashes and
pod labels (`inferencereplica_types.go:113-122`), and `CheckPairing` activates
because a non-empty token is changing (`pairing.go:133-135`, `:187-189`). Cost,
stated plainly: an engine-only image change re-mints the decoder revision too,
because the decoder's hash now carries a token derived from the engine's
target. That is what "one unit" means, and why `paired` is opt-in.

**Data-path pin.** Coordination ensures one Service per (peer, cohort),
`<isvc>-<peer>-pair-<cohort>`, built like the per-revision Service
(`services.go:108`, `:271-316`) but selected by component +
`ome.io/pairing-protocol`. The render hook passes a hash function returning the
cohort (`discovery.go:99-116` already emits `OME_<PEER>_REVISION_ENDPOINT` when
handed a value; `core/render.go:105`). The cohort is a run-level value, so it
*is* known at render time — the reason a peer hash could not be.

**Two denials**, inside `CheckPairing`'s step simulation (`pairing.go:201-234`),
active only when the group is `paired`: (a) *decoders in first* — deny an
engine step onto the new cohort while no new-cohort decoder is serving; (b)
*decoders out last* — deny a decoder step whose simulated outcome leaves the
old cohort with no serving decoder while any old-cohort engine serves. The gate
evaluates at operation start over the simulated step, so a denial lands before
a surge, not at its drain; the floor and the 1×1 escapes (`pairing.go:258-278`)
stay. Timeline for 4+4 blueGreen: t0 decoder targets surge; t1 first new
decoder Ready → engines admitted; t2 engines move, each draining its own
source; t3 the last old decoder drains after the last old engine is gone.

**Completion** is run completion, which already requires both Components at
their pinned revisions (`rolloutrun.go:474-487`); with the cohort in the hash,
"on target" implies "paired". **Failure**: decoder v2 never Ready → rule (a)
keeps engines on the old cohort, group `Failed` (`state.go:183-189`), stable
serving; a decoder that fails *after* engines moved leaves a pair Service with
no endpoints — loud, not silently mispaired; the operator acts.
**Pause/freeze** compose as today (a denial is a `defer`, a pause a token).
**Rollback**: canary rollback re-targets both Components (`dispatch.go:228-240`)
to the closed run's stable pair; blueGreen has no rollback verb
(`oeps/0017-unified-rollout-v2/README.md:399-401`) and a spec revert mints a
**third** cohort — Proposal 3 gives it a verb. **Scale**: the group's surge
budget applies; `maintainRatio` bounds skew, not total (`rollout_types.go:154-161`).
**Spec change** closes the run `Superseded`; the next run mints a new cohort.

### Engine and table impact

The instance machine sees a gate verdict and env vars; its scope is one
Instance of one Component (`machines/instance/machine.yaml:1-3`). Rules (a) and
(b) are new *reasons* for the existing event `gate.deny[Pairing]`, already
`defer` in every applicable state: no cell, arrow or `frozen` sidecar change.
The coordination machine (not frozen) gains the two guards in `pairing.go`;
that is the engine change the Summary names.

### Lock

- **Today:** none covers the window. `pairing_protocol_kind_test.go:33-58`,
  `:133-141` check only that *status* names one pairable pair;
  `bluegreen_kind_test.go:31-37` holds "traffic stays on v1 until all v2
  serving" and "atomic flip" as `PIt`; nothing under `tests/integration`
  mentions `OME_DECODER_ENDPOINT`.
- **KIND** (`pd-topology-isolation`, `.buildkite/generate_pipeline.py:118`):
  PD blueGreen `[engine, decoder]`, `paired: true`, both images bumped.
  `Consistently`: every serving engine's `OME_DECODER_REVISION_ENDPOINT`
  Service selects only its cohort's decoders, and no old-cohort engine serves
  after the last old-cohort decoder is gone; `Eventually`: run `Completed`.
  Second `It`: decoder v2 image broken → engines never move, group `Failed`.
- **Replay:** unreachable — one owner and one Component per driver
  (`workload/replay/driver.go:37-60`, `:86-95`, `:141-142`), `RenderHook`
  unwired (`tests/rolloutspec/replay/README.md:534-536`, `:612-617`). Seeding a
  peer IR's `instanceStatuses` would make (a)/(b) replayable — Open Question 5.

### Alternatives rejected

Pin by operator token alone: the gate stays a floor and the Service
revision-blind, so mismatched pairs remain. Decoders first as two ordered
groups: one-way mixing for the decoder roll plus soak, twice the wall clock,
correct only with backward-compatible decoders — today's recipe
(`0007.1 README.md:94-96`), still available.

### Risks

Double surge: both Components surge under the 25% default
(`coordination/status.go:258-263`) and no new engine serves until a new decoder
does, so peak is old + new of both sets for the pin window.

## Proposal 2: long-lived two-revision serving

### What happens today

A canary must end: the last step must be `traffic: 100`, `capacity: 100%`
(`pkg/validation/canary.go:169-170`, `:196-198`); a manual step is a hold with
no clock (`canary/reconciler.go:503-505`). Held rows carry nothing — no
Operation, deadline, token or `RolloutHold` (`workload/plan_decision.go:545-557`;
`workload/pass_update.go:29-33`). Beyond Motivation 2: any `spec.rollout` edit
is inert as `RolloutPlanDrift` until repin (`rolloutrun.go:135-141`, `:524-571`),
and on the next spec change a fresh run keeps the *original* stable
(`rolloutrun.go:145-162`) while held rows are chosen by lowest index regardless
of revision (`escalation/budget.go:123-159`), so v1 and v2 rows may both be
held while traffic names only v3 and v1.

### The change

**Field.** `split: {capacity: <IntOrString>, traffic: <0..100>}` as a fourth
member of the group one-of (`rollout_types.go:98`, the count-≤-1 rule gains a
term). Same meanings as a canary step (`rollout_types.go:213-238`): `capacity`
is the share of the group's replicas on the target revision, `traffic` the
service-level weight for it. No ladder, pause or analysis. Admission: `traffic`
below 100 allowed; `maintainRatio` allowed on multi-Component splits; `soak`
rejected (nothing follows a resting group). `RolloutPolicy` is unchanged in v1
— its exactly-one rule (`rolloutpolicy_types.go:37`) would need a fourth arm, a
second change (Open Question 2).

**Semantics and completion.** A split is a canary with one step and no end:
the dispatcher stamps the partition for `capacity` exactly as
`StampStepPartition` does (`canary/effective.go:71-99`) and writes `traffic`
into `status.components[c].traffic[]`; the group rests `Staged`, a terminal
resting phase (`coordination_types.go:285-290`; `state.go:235-255`); the run
closes `Completed` when it does — the branch exists (`rolloutrun.go:484-486`)
and the outcome doc names it (`rollout_run_types.go:36-38`). A split is
therefore *never in flight*: `RolloutPlanReady=True/NoActiveRun`, IR
`Ready=True/Staged` once the read bug is fixed, and spec edits take effect on
the next run instead of parking as drift.

**Ends.** (1) Remove `split`, or set `capacity: 100%, traffic: 100` → the group
is plain blueGreen, converges, the old revision drains. (2) A third revision →
the run reopens with pinned stable v1, target v3: v2 rows roll to v3, v1 rows
stay held. (2) requires **hold-by-revision**: candidacy in `budget.go:123-159`
prefers stable-revision rows for the held set, then lowest index. Without it a
split silently becomes a three-revision serve.

**Failure**: target pods never Ready → the group is `Surging`, not `Staged`; the
run stays open and `readyTimeout` parks it `Failed` as any blueGreen. **Pause**
holds partition changes at the boundary. **Rollback**: `split` never arms the
sticky reject or writes `rollbackToRevision`; "rollback" is `traffic: 0` or
removing the field. **Scale**: the partition is recomputed from live
`ir.spec.replicas`, not `MinReplicas`, so the share survives an HPA scale-out.
**Spec change** on the target side is end (2).

### Engine and table impact

No new state, no new token: a held row has no Operation, so nothing parks.
`Ready × spec.partition = defer` (`cells-noop.yaml:256`) and
`Ready × spec.pauseTrue = ignore` (`:250`) are already the steady-state cells;
`frozen: true` (`machine.yaml:3`) untouched. Hold-by-revision changes *which*
rows are held, not what a held row does: the outcome of `Ready × spec.partition`
and `Ready × spec.revision` stays `defer`; their notes gain the selection rule,
their status moves `current → decided`, and the sidecar under
`hack/omenative-testing/statemachine/history/` records it
(`oeps/0020.1-rollout-state-machine-spec/README.md:269-273`). Three fixes
outside the table: (i) `stagedAtPartition` reads the lifecycle partition; (ii)
partition from live replicas; (iii) hold-by-revision in `budget.go`.

### Lock

- **Replay** `ready-held-by-partition-indefinite` (shape as
  `replay/driver_external_test.go:15-31`): replicas 4, partition 2, image v1;
  tick 1 `spec.revision` v2; ticks 2–3 `pod.ready` idx 2,3; tick 4
  `clock.advance 720h`. Assert rows 0,1 `Ready` on v1 with `Operation == nil`,
  rows 2,3 `Ready` on v2, no `RolloutHold`, no `DeadlineExceeded`
  (`escalation/deadline.go:22`). A companion for (iii): then v3 — the held rows
  are the v1 rows.
- **KIND** (`canary-lifecycle`, `generate_pipeline.py:129`; pattern
  `canary_lifecycle_promote_kind_test.go:60-115`): 4×v1 → `split {50%, 50}` +
  v2 → 2/2 Ready by revision label, traffic 50/50, IR `Ready=True/Staged`,
  `RolloutRunClosed … Completed`, `Consistently` unchanged; bump v3 → v1 rows
  held, v2→v3; drop `split` → all v3, v1 per-revision Service gone.

### Alternatives rejected

`pause: {indefinite: true}` on a step keeps the run pinned forever and the
third revision undefined. Two ISVCs behind one gateway: two uncoordinated
rollouts, two model caches. OEP-0026 referenced IRs are the long-term home
("not a canary that ends", `0026 README.md:47-49`); `split` ships now on
partition + Staged + per-revision Services and becomes two refs with per-ref
weights later (`0026 README.md:281`).

### Risks

`split` divides the *same* replicas (`effective.go:66-67`), so an equal-quality
A/B needs more replicas; one HPA averages both arms; PD arms need one
`pairingProtocol`, or Proposal 1's cohort per arm.

## Proposal 3: rollback to any retained revision

### What happens today

The target is always the pre-canary stable `cs.StableRevisionHash`
(`canary/reconciler.go:236-237`); the dispatcher resolves its CR name by hash
(`dispatch.go:262-306`, `:316-337`) and writes `spec.pacing.rollbackToRevision`
on every group Component (`dispatch.go:228-240`); a GC'd CR leaves the signal
unset (`:285-291`). The IR adapter reports the spec target as `UpdateRevision`,
then, if the named CR exists, passes that CR as the roll target to
`workload.Reconcile` (`ir/reconciler.go:457-502`, `:683`); NotFound falls
through (`:498-499`). `CurrentRevision` is promoted only toward the **spec**
target (`:529-537`; `ir/convert.go:1022-1025`), so a pinned IR reads
`current=<pre-roll>, update=Y, instances running X`. Capacity gating is not a
separate path: the pinned CR is a different target in `planUpdateSelection`
(`plan_decision.go:520-545`), so budgets, the coordination gate, quota and
InstanceReadyTimeout apply identically (`transitions.yaml:233-253`). Nothing
keeps a CR *because* it is a rollback target.

### The change

**Field.** The existing annotation `ome.io/rollout-rollback` takes a value:
`"true"` keeps today's meaning; `<revision-name>` names the ControllerRevision
to pin to and is valid **without** an armed canary — so blueGreen and
rollingUpdate groups, and "rollback after promote", are covered. Admission
widens `kindBool` to `bool | revision-name` (the promote precedent,
`validation/traffic_annotations.go:183-190`); shape only. At reconcile: X must
exist in this Component's retained list (`stableRevisionName`'s selector,
`dispatch.go:316-337`), X ≠ the sticky rejected hash, X ≠ the current target;
a failure emits a Warning `RollbackRejected` and consumes the annotation
(`promotion.go:294-306`), so it never loops silently.

**Semantics.** Pinned-until-spec-moves, generalized from today: the controller
writes `rollbackToRevision = X` on every group Component's IR, records
`rejected = Y` as the sticky reject, and closes the run `RolledBack` with each
Component's `stableRevision = X` (`rollout_run_types.go:102-111`; no new status
field). `UpdateRevision` keeps saying Y. The pin releases only when the spec's
template changes to something ≠ Y (`canary/reconciler.go:256-272` does this for
the canary case); a spec revert *to X* lifts the pin with nothing to move.
**Multi-Component groups**: one value, resolved for the primary; a secondary's X
is the same run's `targetRevisions[]` pair when X was a run target; otherwise
`RollbackRejected` rather than a guess.

**Completion**: `RollingBack` until every non-X pod is gone, then `RolledBack`
(`canary/reconciler.go:566-600`). **Failure**: X's image gone →
ImagePullBackOff → `Failed` row as any forward roll (`transitions.yaml:254-258`).
**Pause** and **freeze** hold the revert at the boundary (a revert is an
Update). **Window** (Proposal 4) never gates it. **Scale** during the pin: new
rows are created on X. **Spec change**: any template ≠ Y lifts the pin.
**Retention**: X joins `CollectLiveRevisionNames` while
`pacing.rollbackToRevision` names it, closing the window between the write and
the first Instance moving.

### Engine and table impact

Adapter-only target selection; `workload.Reconcile` takes a `*ControllerRevision`
and does not know it is older. Cells exercised, all existing:
`Ready / spec.rollbackTarget[set] / before → Update/{Surge, InPlace, Drain}`
(`cells-noop.yaml:244`; `transitions.yaml:233-253`), the `Update/Surge` unwind
(`cells-update-surge.yaml:138-139`), `Update/SurgeDrain` defer (`:275`),
`Update/InPlace` and `Update/Drain` retarget
(`cells-update-inplace-recreate.yaml:89`, `:178`), `Empty`/`Pending` ignores
(`cells-noop.yaml:73`, `:147`). ISVC-side work only: `stableRevisionName`
accepts any name; `stickyRejectHashes`, `divergedMember`, `retargeted`
(`observe.go:136-221`; `rolloutrun.go:403-421`) treat "pinned to X while spec
says Y" as a hold for non-canary groups too.

### Lock

- **Replay:** existing `T-ready-rollback-surge`, `ready-rollback-inplace`,
  `ready-rollback-recreate` (v1→v2→pin v1). New `ready-rollback-two-back`:
  v1→v2→v3→`spec.rollbackTarget[set]: {image: v1}` — the pin is by CR identity,
  not "previous"; paired with a retention assertion in `ir/retention_test.go`.
- **KIND** (`canary-lifecycle`): extend `canary_retarget_rollback_kind_test.go`
  (A→B→C→rollback→A) with A→B(complete)→C(complete)→`rollout-rollback: <A>`
  under `ome.io/revision-history-limit ≥ 2`. The spec sits in `KIND_QUARANTINE`
  (`generate_pipeline.py:216-226`, expiry 2026-10-04) for a spec-rewrite wedge
  after rollback (OEP-0020.2, Open) and is in no group today; the extension
  lands with the fix that moves it back into `canary-lifecycle`.

### Alternatives rejected

A field `spec.rollout.rollbackTo` is a pin that stays in spec after the revert.
A CLI or GitOps re-apply of the old template mints a *new* CR and runs a
forward roll — no sticky reject, no identity, no immediate traffic cut
(`canary/reconciler.go:576`); fine as sugar over the primitive.

### Risks

X incompatible with today's runtime or BaseModel — the CR payload is a pod
template only; nothing re-validates it (Open Question 3).

## Proposal 4: rollout windows

### What happens today

One verb, `ome.io/rollout-paused: "true" | "freeze"` (`pkg/constants/traffic_annotations.go:91`),
parsed by `RolloutPauseState` (`:124`) and projected onto the IR as
`spec.paused` / `spec.pauseMode` (`irprojector/projector.go:426-431`). Engine
contract: "no new operation or new step; an attempt in flight runs to its step
boundary" (`workload/types/plan.go:75-87`); the token parks the deadline
(`escalation/deadline.go:80-100`). The canary layer under pause is observe-only
with clocks intact and its pause gate also blocks the rollback verb
(`canary/reconciler.go:207-215`). The ISVC controller has **no** injected
clock: canary dispatch has a `Now` seam defaulting to `time.Now()`
(`canary/dispatch.go:33-37`), coordination takes `obs.Now` (`state.go:100-104`),
`controller.go:1070` calls `metav1.Now()`.

### The change

**Field.** One type, two homes:

```yaml
window: { start: "02:00", end: "05:00", timezone: "UTC", days: [Mon, Tue, Wed, Thu] }
```

at `spec.rollout.window` on the ISVC and `spec.window` on `RolloutPolicy`. The
inline value outranks the ref, the rule the run model already reports as
`ShadowedPolicyRef` (`rollout_run_types.go:202-205`). The window is pinned into
`status.rollout.activeRun` at run open, so a mid-run policy edit surfaces as
`PolicyNewerThanRun` (`:242`) and does not move a live window. A window is
behavior, not shape, so it fits the policy's rule (`rolloutpolicy_types.go:5-8`);
it is not under `lifecycle`, which is per-Component and projected to the IR.
Admission: `start ≠ end`; `timezone` a required, loadable IANA name (no
in-code default zone); `days` non-empty when set (absent = every day); the
window at least the operator-configured minimum from the rollout block of the
ConfigMap, so the hold cannot flap. Midnight-crossing (`22:00`–`02:00`) is legal.

**Semantics.** The window gates **advancement only**: canary — opening the next
step (its traffic/capacity write) and the completion scale-down; blueGreen —
the cutover (traffic shift and drain start); rollingUpdate — starting the next
Instance's update. Each is what `Plan.Paused` withholds today plus what the
canary and coordination pause gates skip, so the mapping is free. A step begun
inside the window finishes outside it; repair continues; the window is never a
freeze. **User pause ORs**: either alone holds, both must clear; if the user
set `freeze`, freeze wins. **Rollback is never window-gated**: `windowHold =
closed && !rollbackRequested`, a rollback being requested when the annotation
is present or `CanaryStatus.RolledBackRevisionHash != ""`
(`canary/reconciler.go:248-260`). A window keeps *forward* risk out of business
hours; a rollback removes risk. A concurrent user pause still holds the revert.

**Status, clock, requeue.** Condition `RolloutWindowClosed=True`, reason
`OutsideWindow`, message with the next open instant (RFC3339, in the window's
zone); `rolloutPhase` stays `Paused` (`traffic_status_types.go:121`). Waiting is
not `Stalled`: the deadline is parked. A non-existent local time
(spring-forward) counts as open; an ambiguous one (fall-back) as its first
occurrence. An injected `clock.Clock` on the ISVC reconciler, mirroring
`inferencereplica/reconciler.go:174-176`, feeds the canary `Now` seam and
`obs.Now` so all three read one instant per pass; `RequeueAfter = nextBoundary
− now` folds into the existing max-merge (`controller.go:769-793`);
`ConfigCacheTTL` (`:360`) is the safety resync.

**Which field is written.** `InferenceReplica.spec.paused = userPaused ||
windowHold` at `projector.go:426`; `pauseMode` from the user's value only.
OEP-0026 classifies both as rollout-control fields the composer writes on
inline IRs (§3–4); the user's annotation is never touched, so a human unpause
and the window cannot fight over one key. **Scale/spec change** while closed:
scale-up creates proceed (pause narrows creates to committed indices,
`workload/reconcile.go:340-349`); a spec change opens a run whose first step
waits.

### Engine and table impact

None. The engine consumes one boolean and never learns why;
`Ready × spec.pauseTrue = ignore` (`cells-noop.yaml:250`) and the pause token's
cells are the whole story. No golden re-record: `Paused` is a scenario input
(`workload/replay/scenario.go:108`; `driver.go:336`, `:639-650`).

### Lock

- **Unit/envtest**, `clock.NewFakeClock`: a pure `windowState(spec, now)
  (open, next)` at boundaries, midnight-crossing, `days`, DST both directions, a
  non-UTC zone, `RequeueAfter` arithmetic.
- **KIND** (`pause-cancel-interleavings`, `generate_pipeline.py:179`; fixture
  `canary_lifecycle_pausefreeze_kind_test.go:69-86`): closed window → step 0
  holds past its timer, `RolloutWindowClosed=True`, `ir.spec.paused=true`; open
  it → step advances within `kindShortTimeout`; second `It`: rollback
  annotation while closed → the revert runs.

### Alternatives rejected

A CronJob toggling the annotation: two writers on the user's key, no status,
no pin. Argo sync windows gate manifest sync, not step advance — a canary
opened at 04:59 keeps stepping all day. A maintenance-window CRD: a third
object to bind; the policy already is the shared object.

### Risks

Clock skew: leader election means one writer; skew shifts the boundary by
seconds. A window shorter than a step is legal; the step finishes past the close.

## Proposal 5: cross-cluster progression

### What happens today

Motivation 5. Each member runs its own run; the hub lifts a bounded view into
`status.placement.candidates[].rollout` (`inference_service_status.go:629-680`;
`rolloutpreflight.go:397-420`) — run id, digests, `lastRun.outcome`, no phase
— and only when the groups reference a RolloutPolicy (`rolloutpreflight.go:398`).
The `Dispatcher` (`placement/dispatcher.go:11-43`) widens the initial candidate
race in Single mode; it is not a stagger.

### The change

**Field.** Hub-only, under the block that already holds hub-only knobs
(`placement_types.go:34`):

```yaml
spec:
  placement:
    mode: All
    rollout:
      order: [canary-cluster, eu-1, us-1, us-2]   # WorkloadCluster names; required
      soak: 10m                                   # optional; absent = no wait
      onFailure: Hold                             # only value in v1
```

`DeriveISVC` strips it like `spec.routing` (`derive.go:42`); members never see
a cluster list. Admission: mode `All` or `Split`; unique names; at least one
OMENative Component (a RawDeployment member honors no pause). Unlisted homes
roll last, lexically — a stated rule, not an inference.

**Mechanism.** The hub keeps fanning the new spec to every home (no change to
`fanOut`) and stamps `ome.io/rollout-paused: "true"` on every derived ISVC not
yet released, deleting the key on release. The derived object is hub-written,
so this is not the user's annotation; the user's hub value still copies
through and ORs. Members honor the key with clocks intact
(`canary/reconciler.go:207-214`; `coordination/state.go:94`). Prerequisites:
(1) `applyDerived` deletes hub-owned keys it no longer sets (the
`mergeOwnedKeys` fix); (2) the lifted view carries phase and the closed run's
target digest, and is populated for inline groups too.

**Completion per member** = `lastRun.outcome == Completed`, `activeRunID`
empty, and the closed run's digest equal to the hub's expected render — a stale
earlier `Completed` is never mistaken for this one; then `soak`, then the next
release. **Failure** = `RolledBack`, `status.rolloutPhase == Failed`
(`inference_service_status.go:143`), or Pending past the run's `readyTimeout`.
The hub then stops releasing: later homes were never released and still serve
stable; completed homes keep the new revision (`Hold`); a hub-driven revert
needs a spec snapshot per home and is deferred (Open Question 4).
**Unreachable** member (skipped by `fanOut`, `controller.go:967-975`) is
`Waiting / MemberUnreachable`, never Completed. **Pause** on the hub ORs over
every member; **rollback** on the hub stays stripped — the operator rolls a
member back on the member, which the hub reads as failure. **Spec change**
restarts at position 0 against the new digest. **Hub restart**: position is
derived from `status.placement.rollout` alone — the preflight's in-memory map
is rebuilt from scratch (`rolloutpreflight.go:40-47`).

**Status.** `status.placement.rollout: {position, current, clusters: [{cluster,
state: Waiting|Released|Running|Soaking|Completed|Failed, runID, waitingReason:
AwaitingTurn|Soak|PriorClusterFailed|MemberUnreachable}], stoppedReason}`.

### Engine and table impact

None on the instance machine and none on the per-ISVC run machine: a member
executes an ordinary derived ISVC and sees "paused" or "not paused". The hub
sequencer is a run-of-runs keyed by cluster, in `pkg/controller/v1beta1/placement`.
It shares vocabulary (`Completed | RolledBack | Superseded`, a persisted cursor,
a completion predicate per progression) with the group sequencer of OEP-0017.3
(PR #1653), one level up and in a different process — shared table shape, not
shared code.

### Lock

- **envtest** `tests/integration/multicluster` (the suite already writes a
  derived ISVC's status directly, `routing_cutoff_test.go:205`): after a bump
  only home 1's derived lacks the key; setting home 1's
  `lastRun.outcome=Completed` with the matching digest clears home 2 after
  `soak`; `RolledBack` on home 1 leaves homes 2..n paused with `stoppedReason`;
  hub restart resumes at the same position.
- **KIND** `tests/integration/multicluster/scaleout_kind` (pipeline-owned via
  `MC_KIND_SUITE_DIR`, `generate_pipeline.py:83`): extend the spec-change spec
  (`lifecycle_kind_test.go:189`) to two ordered members — A's run closes before
  B's pods change; with a broken image on A, B's pods never change.
- No replay golden: the machine's inputs are lifted status fields.

### Alternatives rejected

GitOps promotion per environment gives up the single hub ISVC that All/Split,
TrafficMap and quota accounting are built around and cannot gate on a member's
run outcome. A cluster dimension on `spec.rollout.groups` is wrong-shaped: a
group is a set of Components inside one ISVC, and the derived copy must not see
clusters.

### Risks

Member-local edits change a member's run without the hub knowing; the hub
re-stamps annotations every pass (`controller.go:1201-1202`), so a local
unpause is overwritten. Document that the hub owns the derived pause key once
`placement.rollout` is set; a member's `Superseded` marks it Failed-by-drift.

## Proposal 6: capacity loss mid-roll

### What happens today

Per Instance: Node deleted → PodGC deletes the target → `pod.deleted[target] =
defer` recreates at the same surge ordinal, same deadline
(`cells-update-surge.yaml:42`). Node NotReady with the object kept →
`containersNotReady[target]` (`:56`), then `terminating[target]` (`:58`), swept
at max(DeletionTimestamp + slack, NotReady + `nodeUnreachableThreshold`)
(`evidence/terminating.go:158-163`, `:236-238`). The budget counts rows at
`Surge`/`SurgeDrain` from *status* (`escalation/budget.go:171-186`;
`coordination/ratio.go:494`), so a dead target still counts: loss never exceeds
MaxSurge; it parks budget until the rows fail out. Per run: `Pending`, `Failed`
at `readyTimeout`, and on return to serving `StepEnteredTime` re-stamps so the
soak restarts (`canary/reconciler.go:491-493`). Run conditions are
`RolloutPlanReady` / `RolloutPlanDrift` only (`rollout_run_types.go:226`, `:239`);
per-row `Operation.Waiting` tokens exist (`lifecycle_types.go:382-392`) and the
ISVC never aggregates them.

### The change

**Derived signal, always on.** ISVC condition `RolloutCapacityDegraded`, set as
`rolloutrun/conditions.go:31-47` sets `RolloutPlanReady`: True while a run is
active and, for any group Component, ready target capacity < the step's asked
capacity **and** at least one target Instance carries environment evidence
already on its IR row — `Operation.Waiting ∈ {NodeUnknown, Unschedulable}`, a
target pod Terminating or `Ready=False` whose node is not Ready, or an
in-flight `AutoRecover` ledger row (`workload/audit/autorecover.go:27`). Reason
`TargetsRelocating`; message `surge capacity lost: 3 of 6 targets relocating
(engine: 2 recreating, 1 NodeUnknown)`. Events `RolloutCapacityLost` on the
False→True edge only and `RolloutCapacityRestored` on True→False (edge rule as
`holds/table.go:102-109`). Inputs are at the dispatcher already
(`canary/dispatch.go:157`, `:201-214`); no Node reads on the ISVC path.

**Optional field.** `spec.rollout.groups[].onCapacityLoss: {action:
Continue|Hold, threshold: <IntOrString>}`, on the group so it covers every
progression. Absent = `Continue` = today. `threshold` is required with `Hold`:
a count of lost targets or a percent of the step's asked capacity (ceil via
`utils.ScaledCountFromIntOrString`, as `budget.go:66`).

**`Hold` semantics.** While the condition is True and lost ≥ threshold: canary
— no step advance, no traffic raise, and `capacityGateExpired` suspended (the
wait is proven environmental, so the ready-timeout does not park it `Failed`);
blueGreen/rollingUpdate — the coordination `UpdateGate` denies fresh Instance
starts. Release = condition False. Precedence: global pause first
(`canary/reconciler.go:213`); `ome.io/rollout-promote` overrides the hold as it
overrides a step hold (`:410-419`) — an explicit promote is the user's
decision; rollback always works (`:273`). `Hold` + freeze holds until unfrozen,
because freeze stops the repair that would release it — documented, not
special-cased. **Scale**: creates proceed; a scale-down that removes the lost
targets releases the hold. **Spec change**: the run closes `Superseded`; the
condition is recomputed against the new run.

**Why not the pause token.** Pause blocks new operations, and both a
re-entered surge after a disposable failure (`cells-update-surge.yaml:117`) and
a relocation (`cells-create.yaml:280`) are new operations; pausing the IR would
stall the recovery that releases the hold. So the canary-side hold is a
run-layer return (shaped like `pausedResult`, `canary/reconciler.go:518-531`)
and the coordination-side hold is a gate denial the rows already model as
`gate.deny`, recorded under the existing `Budget` gate name — the group's surge
budget is literally what the relocating targets hold — so no eighth gate name
enters `lifecycle_types.go:102-125` or the table's variant list; the message
names capacity loss.

### Engine and table impact

None: the condition is derived from IR status and pods, no IR write, no new
token, no new event variant. Doc-only: `cells-update-surge.yaml:60-62` is
rewritten once the Unknown-phase sweep lands (that fix wires
`recoverUnknownPhaseTargets` into the surge target set — its own PR).

### Lock

- **Replay** `surge-capacity-loss-multi`: replicas 6 (precedent
  `create-scaledown.yaml`), all six at `Step=Surge`, one tick with
  `pod.phaseTerminal[Unknown]` on three targets, `forceDelete` configured. The
  driver seeds no Node objects, so only `NodeGone` is expressible;
  `timer.forceDelete[nodeUnreachable]` is rejected (`replay/events.go:666-671`).
  Assert the condition's *inputs*: three rows recreating,
  `CurrentSurgeInFlight == 6`, no seventh admitted, deadlines unchanged; run
  the derivation as a pure function over the row store and pod list.
- **envtest**: an IR with three rows `Waiting=NodeUnknown` under an active run
  → `RolloutCapacityDegraded=True` with the exact counts, one
  `RolloutCapacityLost`, then False + `RolloutCapacityRestored` when Ready.
- **KIND** (`runtime-retry-recovery`, `generate_pipeline.py:114`): the pod-loss
  pattern (`canary_lifecycle_podloss_kind_test.go:37`) with the dead-node
  mechanism of `force_delete_kind_test.go` (`docker pause` a worker at `:46`,
  `expectNodeNotReadyAtLeast` at `:88`): condition True, phase Pending, `Hold`
  survives `readyTimeout`, releases after `docker unpause`; `Continue` parks
  `Failed` as today.

### Alternatives rejected

Do nothing: a blind `Pending` and a `Failed` exit the user did not choose.
PodDisruptionBudget guards voluntary eviction only. Node-pool pinning is a
pod-template choice the user owns. A "spot-aware" strategy the engine picks
violates the rule.

### Risks

Flapping: a node bouncing inside `nodeUnreachableThreshold` is
`NodeNotDeadLongEnough`, not loss; inputs are edge-stable tokens. A hold that
never releases (no capacity anywhere): targets sit `Unschedulable` → grace →
re-enter → loop; exits are `action: Continue`, promote, rollback, or
scale-down, all visible in the condition's counts.

## API changes

Writers follow OEP-0026: users write ISVC/RolloutPolicy spec and annotations;
the ISVC controller (as composer) writes IR rollout-control fields on inline
IRs; the hub writes the derived ISVC.

| Object | Path | Type | Default | Validation | Writer |
|---|---|---|---|---|---|
| InferenceService | `spec.rollout.groups[].paired` | bool | false | components exactly `[engine, decoder]`; progression blueGreen or canary | user |
| InferenceService | `spec.rollout.groups[].split` | `{capacity IntOrString, traffic int32}` | absent | fourth one-of member (count ≤ 1); `traffic` 0–100 allowed; `soak` rejected | user |
| InferenceService | `metadata.annotations["ome.io/rollout-rollback"]` | `"true"` \| revision name | — | shape at admission; existence, ≠ rejected, ≠ target at reconcile | user (consumed by controller) |
| InferenceService | `spec.rollout.window` | `RolloutWindow{start, end, timezone, days[]}` | absent | `start ≠ end`; IANA `timezone` required; `days` non-empty if set; ≥ configured minimum | user |
| RolloutPolicy | `spec.window` | `RolloutWindow` | absent | same; inline outranks | user |
| InferenceService | `spec.placement.rollout` | `{order []string, soak Duration, onFailure enum}` | absent | mode All/Split; unique names; `order` required; `onFailure ∈ {Hold}` | user; stripped by `DeriveISVC` |
| InferenceService | `spec.rollout.groups[].onCapacityLoss` | `{action enum, threshold IntOrString}` | absent (= Continue) | `threshold` required when `Hold` | user |
| InferenceService | `status.rollout.activeRun.window` | `RolloutWindow` | — | pinned copy | ISVC controller |
| InferenceService | `status.conditions[RolloutWindowClosed]` | condition | — | reason `OutsideWindow`, next-open in message | ISVC controller |
| InferenceService | `status.conditions[RolloutCapacityDegraded]` | condition | — | reason `TargetsRelocating` | ISVC controller |
| InferenceService | `status.placement.rollout` | `{position, current, clusters[], stoppedReason}` | — | hub only | hub placement controller |
| InferenceService | `status.placement.candidates[].rollout` | existing, gains `phase`, `targetDigest` | — | populated for inline groups too | hub placement controller |
| InferenceService (derived) | `metadata.annotations["ome.io/rollout-paused"]` | existing | — | set/deleted by the hub on the derived object only | hub placement controller |
| InferenceReplica | `spec.pairingProtocol` | existing | — | value becomes the cohort digest when `paired` | ISVC controller (composer) |
| InferenceReplica | `spec.paused` | existing | — | `userPaused \|\| windowHold` | ISVC controller (composer) |
| InferenceReplica | `spec.pacing.rollbackToRevision` | existing | — | any retained CR name | ISVC controller (composer) |
| Events | `RollbackRejected`, `RolloutCapacityLost`, `RolloutCapacityRestored` | Warning/Normal | — | edge-triggered | ISVC controller |

Seventeen rows: seven new spec fields or values; the rest are status, events,
or new values in existing controller-written fields.

## Ordering and dependencies

1. **Independent bug fixes first**, each its own PR, each valuable alone:
   (a) `stagedAtPartition` reads the lifecycle partition; (b) canary partition
   from live replicas, not `MinReplicas`; (c) `mergeOwnedKeys` deletes hub-owned
   keys no longer set; (d) `recoverUnknownPhaseTargets` wired into the surge
   target set; (e) single-pod surge relocation reads the target sub-bucket;
   (f) the `GroupBlueGreen` doc stops claiming an atomic internal flip.
2. **Ship now:** Proposal 4 (zero engine change); Proposal 6's condition and
   events; Proposal 1; Proposal 3 once the quarantined rollback spec is back in
   its gating group (2026-10-04 at the latest).
3. **After the fixes:** Proposal 2 needs (a), (b), then hold-by-revision;
   Proposal 6's `Hold` needs (d) so an Unknown target cannot hold a run
   indefinitely; Proposal 5 needs (c) and the lift change.
4. **Waits on other OEPs:** Proposal 5's completion predicate should match the
   per-progression predicate OEP-0017.3 defines, so it lands after that OEP is
   accepted. Proposal 4's IR write on a *referenced* IR depends on OEP-0026
   Open Question 1; on inline IRs it is today's projector path. Proposal 2's
   long-term home is OEP-0026 refs; `split` is not blocked on it.

## Test Plan

- **Replay** (`tests/rolloutspec/replay`): four new scenarios
  (`ready-held-by-partition-indefinite` and its v3 companion,
  `ready-rollback-two-back`, `surge-capacity-loss-multi`) plus the three
  existing rollback scenarios. The 123 goldens must not move for Proposals 3–6;
  hold-by-revision moves none (held rows write nothing); Proposal 1 is
  unreachable from the harness.
- **Table**: `rolloutspec-check` green with `frozen: true`; the two candidacy
  cells change note and status only, recorded in the sidecar.
- **envtest**: window arithmetic; rollback annotation admission; `split`
  admission; `RolloutCapacityDegraded` derivation; the multicluster ordered
  progression.
- **KIND**: one new spec per proposal in the groups named above, each
  classified in `KIND_SPEC_GROUPS`; Proposal 5 in `scaleout_kind`.

## Graduation Criteria

| Proposal | Alpha | Beta | Stable |
|---|---|---|---|
| 1 pair | field + cohort Service + two denials; KIND spec green | replay can seed a peer IR and lock (a)/(b) | runtime doc for `OME_<PEER>_REVISION_ENDPOINT` |
| 2 split | fixes (a)(b) merged; `split` on blueGreen-shaped groups; KIND | hold-by-revision; PD split | aligned with OEP-0026 refs |
| 3 rollback | annotation value; canary groups; replay + KIND | blueGreen/rollingUpdate via the run-hold rule | payload re-validation decided (OQ 3) |
| 4 window | ISVC field; fake-clock tests; KIND | policy field; pinned into runs | — |
| 5 cross-cluster | fix (c); envtest ordered run; `Hold` | KIND scaleout; unreachable handling | revert of completed homes decided (OQ 4) |
| 6 capacity | condition + events | `onCapacityLoss: Hold`; KIND dead-node spec | — |

## Alternatives

Two alternatives were weighed for the whole set. A **`RolloutPlan` CRD**
carrying pairs, splits, windows, cluster order and loss policy in one object:
rejected — a fourth rollout object beside `spec.rollout`, `RolloutPolicy` and
`status.rollout`, every field mirroring one that already has a home, and the
owner's constraint excludes a new CRD. **Letting the engine infer** (spot-aware
strategies, auto-pairing on PD topologies, a business-hours window from a
cluster annotation): rejected by rule — the engine never substitutes for a user
decision. Per-proposal alternatives are in each section.

## Open Questions

1. **Cohort token composition.** `digest(pairingProtocol, engineHash,
   decoderHash)` re-rolls the decoder on an engine-only change. An asymmetric
   form (engine cohort includes the decoder hash, not the reverse) would break
   rule (b), since old engines could not be told apart by their decoder's
   token. I lean symmetric; the owner decides.
2. **`split` on `RolloutPolicy`.** A fourth arm in the policy's exactly-one rule
   is a second change; is a fleet-shared split wanted before OEP-0026 refs?
3. **Rollback payload re-validation.** Re-run the webhook validator on X's pod
   template against today's ServingRuntime at pin time, or document it as
   operator responsibility?
4. **Cross-cluster revert.** `onFailure: RollbackCompleted` needs the hub to
   snapshot the previous spec per home; where does that live so it survives a
   hub restart?
5. **Replay peer seeding.** A scenario event seeding a peer IR's
   `instanceStatuses` is the only thing keeping Proposal 1 KIND-only. Owner
   and sequencing TBD.

Next action: file the six independent fixes as PRs, then the owner decides
Open Question 1 before Proposal 1's cohort digest is implemented. Scope: the
OMENative path; RawDeployment and MultiNode are unaffected.
