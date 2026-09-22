# OEP-0017.3: Rollout Group Sequencer

**Tl;dr:** `spec.rollout.groups[]` promises that group N completes before group
N+1 begins. The controller keeps that promise for one shape only — a run of
single-Component blueGreen groups, which `collapseSequential` folds into the
internal Sequential state machine — and since PR #1080 and PR #1513 admission
rejects every other multi-group list under the default `groupOrdering:
Sequential`. The promise is no longer broken silently; it is unavailable. This
OEP resolves issue #1040 by making it true for every shape: a group sequencer
inside the rollout run that admits one group at a time, defines completion per
progression, owns soak, and records its position in `status.rollout.activeRun`.

<!-- toc -->

## Decision

Issue #1040 poses one question: for the ordered shapes admission accepts, either
enforce the order or reject the shape. Two options:

a) **Reject the shapes the engine cannot sequence.** — *Tradeoff:* the v2 API's
   central promise (ordered groups, each with its own progression) shrinks to
   its degenerate case; `blueGreen then rollingUpdate` cannot be written.
b) **Enforce the order for every accepted shape.** — *Tradeoff:* a new layer
   above two engines that do not share a writer, and a definition of "group N
   is complete" per progression.

**(a) is already on main.** PR #1080 rejects a non-collapsing multi-group list
(`GroupOrderingNotHonored`) and any non-empty `order` (`OrderNotHonored`); PR
#1513 added `spec.rollout.groupOrdering: Concurrent` so an operator who wants
independent groups can say so. Nothing accepted today runs in an order it did
not ask for. That was the right immediate step regardless, and it stays until
the sequencer enforces each shape it lifts — lifting first reopens the gap.

**This OEP is (b).** The rejection is a stopgap: the `sequence + canary`
combination that motivated OEP-0017 is still unwritable under `Sequential`,
and `Concurrent` is an escape hatch, not an ordering. An operator who wants
"rolling-update the decoder, then blue-green the engine" today writes two spec
edits from a runbook — the failure the ordered list exists to prevent. I
recommend building the sequencer below and lifting each rejection in the same
change that enforces the shape it guarded.

## Summary

The sequencer is a per-run cursor over the pinned plan of OEP-0017.1
(`status.rollout.activeRun.plan.groups[]`):

- **One active group at a time**: the lowest-index pinned group with work
  that is not yet complete. Later groups with work are held — coordination
  Components refused a fresh update, canary groups not dispatched.
- **Completion is defined per progression** over persisted status, with
  old-revision capacity drained before the next group may start.
- **Soak is the sequencer's**: the wait between group i completing and group
  i+1 starting, after any progression, per group, anchored to a persisted
  timestamp.
- **A failed, paused, rolling-back, or rolled-back group holds every later
  group** until an operator acts. Nothing force-advances.
- **Position lives in `activeRun`**; a restart resumes the same group and soak.
- **`order` on a multi-Component blueGreen or rollingUpdate group** rolls the
  members one at a time in that order; it stays rejected on canary groups.
- **`groupOrdering: Concurrent` is unchanged.** The sequencer applies only to
  the default `Sequential`.

Scope: OMENative (IR-managed) InferenceServices only. RawDeployment has no
group surface.

## Motivation

### What main does today

Facts, each checked against the code named:

| Shape (2+ groups, `groupOrdering: Sequential`) | Admission | Execution |
|---|---|---|
| Run of single-Component blueGreen groups (no canary) | accepted | ordered: `collapseSequential` (`coordination/groups.go`) folds them into one internal `Sequential` group; `CheckSequential` (`coordination/sequential_gate.go`) refuses a fresh update to any Component but the active one; soak is the `max()` of the non-last groups' soaks |
| Any list containing a rollingUpdate group, a canary group, or a multi-Component group | rejected (`GroupOrderingNotHonored`, `pkg/validation/coordination.go`) | would run concurrently: `controller.go` dispatches `canary.Dispatch` per canary group and `coordination.Reconcile` for the rest in the same pass |
| Same lists under `groupOrdering: Concurrent` | accepted | concurrent, by declaration |
| Any group with non-empty `order` | rejected (`OrderNotHonored`), under either ordering | no progression reads `RolloutGroup.Order`; the collapsed run derives its order from list position |
| `soak` on a canary group, or on a list that does not collapse | rejected (`SoakNotHonored`) | only the Sequential machine waits |

The rejections apply on create and on any update that changes `spec.rollout`;
a stored object keeps reconciling until its rollout is next edited.

Two facts from OEP-0017.1/0017.2 shape the design. A **rollout run** opens
when any grouped Component's IR target diverges from its current revision,
pins the rendered plan into `status.rollout.activeRun`, and closes `Completed`
when every pinned group's members have converged (or rest `Staged`),
`RolledBack` when a canary's sticky hold settles, or `Superseded` on a retarget
(`rolloutrun/rolloutrun.go`, `closedOutcome`) — whole-plan, with no per-group
"done" and no "current" group. A canary group is a **canary unit** (router, or
engine+decoder); admission rejects two canary groups on one unit
(`MultipleCanaryGroups`) and the controller dispatches every canary group.

### Goals

- Group N+1 does not start before group N is complete and soaked, for every
  list admitted under `Sequential`; soak per group, after any progression.
- One completion predicate per progression over persisted status.
- A failed, paused, rolling-back, or rolled-back earlier group holds later
  groups, with a machine-readable reason; position survives a restart.
- `order` either enforced (blueGreen, rollingUpdate) or rejected (canary).
- Lift each admission rejection in the change that enforces its shape.

### Non-Goals

- Merging the canary and coordination engines (OEP-0017's open item);
  cross-group automatic rollback; changing `Concurrent`; queueing retargets.
- The shared group budget ledger (issue #1029): one active group at a time
  makes cross-group budget interaction nil by construction.

## Proposal

### Terminology

- **Group** : one entry of the pinned `activeRun.plan.groups[]`. A Component
  is in at most one group, so groups partition the rolling Components.
- **Progression** : the group's one-of `blueGreen | rollingUpdate | canary`
  (absent = blueGreen); each is observable through different status.
- **Work** : a group has work when at least one member's pinned target
  (`activeRun.targetRevisions[]`) differs from the revision it was on at
  open. A group with no work is **Skipped** for ordering.
- **Active** : the one group the sequencer lets progress. **Pending** : a
  later group with work, held. **Complete** : the per-progression predicate
  holds. Failed, Paused, RollingBack, and RolledBack are not Complete.
- **Cursor** : the persisted position — per-group completion timestamps in
  `activeRun` plus one condition naming why the next group waits.

### What must be true

1. At most one group is Active. A group becomes Active only when every earlier
   group is Complete (and its soak has elapsed) or Skipped.
2. An earlier group that is mid-progression, soaking, Failed, Paused,
   RollingBack, or RolledBack keeps every later group Pending. No timeout
   advances the sequence; a stuck group is an operator decision.
3. The Active group is a function of `activeRun` plus engine status, so a
   controller computes the same answer before and after a crash.
4. Work already dispatched is never revoked.
5. Every Pending group's wait has a reason an operator can read from status.

## Design Details

### Where the sequencer sits

a) **Merge the engines, then sequence inside the merged reconciler.** —
   *Tradeoff:* blocked on the unified `status.rollout` phase mapping OEP-0017
   marks TBD.
b) **A cursor inside the rollout run, enforced at the two dispatch points.** —
   *Tradeoff:* the engines survive another release; the sequencer reads their
   status rather than owning one machine.

We choose (b). The run already pins the group list in order, knows each
Component's target and stable revision, and decides whole-plan completion;
per-group completion refines that decision. Enforcement reuses two hooks: the
coordination dispatcher's fresh-update gate (`gate_eval.go` consults
`CheckSequential` before a fresh per-Instance update; a cross-group check is a
sibling) and the canary partition projection (`canary.PlanGateHoldPartition` already
holds a canary group whose plan is unresolvable; a Pending group takes the
same hold).

### Completion, per progression

A group is Complete when every member is stable on its pinned target: all
Ready capacity on the target revision, no old-revision pod left past its
drain. "Stable" is load-bearing — starting group N+1 while group N still holds
surged old capacity stacks two groups' surge against one quota.

- **blueGreen** : the coordination group rests `Idle` (past `Surging`,
  `Waiting`, `Shifting`, `Draining`, `ScalingDown`) with every member IR
  converged on the pinned target (`current == target == pinned`,
  `updatedReplicas == replicas`). `Staged` — surged under a partition, not
  flipped — closes a run `Completed` today but is **not** Complete for
  sequencing: the old revision still serves. `Failed`, `Paused`,
  `RollingBack` are not Complete.
- **rollingUpdate** : every member IR converged on the pinned target with no
  pod on another revision. Mixed revisions are the progression's point
  mid-roll and disqualifying at completion.
- **canary** : the run's existing per-group rule (step counter past the last
  step, every member IR fully updated) plus the primary Component's
  `rolloutPhase == Stable`, stamped only after `scaleDownDelaySeconds` and the
  old revision's scale-down. `Canarying`, `Paused`, `Promoting`, `Failed`,
  `RollingBack` are not Complete; `RolledBack` is a terminal hold — the
  operator chose the old revision, so later groups must not move onto the new.

The predicate reads `activeRun.targetRevisions[]`, IR status, and the engines'
own status blocks — nothing in memory. That is what makes rule 3 hold.

### Active-group selection and enforcement

Each pass, after `rolloutrun.Reconcile` and before the Component reconcilers:

1. Classify each pinned group in order: Skipped (no work), Complete
   (predicate true; `completedAt` written once), or has work.
2. The Active group is the first with work whose predecessors are all Complete
   or Skipped and whose nearest non-Skipped predecessor's soak has elapsed
   (`completedAt + soak <= now`). Every later group with work is Pending.
3. A Pending coordination-style group's Components are refused a fresh update
   with reason `SequencedBehindGroup` (the "already mid-update" bypass stays —
   rule 4); a Pending canary group gets the full partition hold and no
   `canary.Dispatch`. A status read error fails closed, as `CheckSequential`
   does today.

Once Active, each engine runs its group exactly as it does now; the sequencer
serializes at dispatch, so no new inter-engine protocol is needed.

### `order` inside a multi-Component group

For a blueGreen or rollingUpdate group with `order: [d, e]`, member d rolls
to completion under the group's progression, then member e; the group is
Complete when its last member is, and its soak applies once, after the group.
This is the v1 Sequential machine restricted to one group's members — the
cross-group and in-group gates are one check at two granularities.

`order` with `maintainRatio` is rejected: one member rolling at a time leaves
no cross-Component ratio to hold (the existing `ValidateGroupShape` rule for
Sequential). `order` on a canary group stays rejected (`OrderNotHonored`): a
canary unit stages capacity on all its members before each traffic step, so
"decoder first" inside one ladder has no meaning. Splitting into
single-Component groups remains valid and equivalent, except that split
groups can carry different progressions and soaks.

### Soak

`groups[i].soak` is the wait between group i Complete and group i+1 Active.
The deadline is `completedAt + soak`, recomputed from the persisted timestamp
every pass; there is no in-memory timer and no poll interval — the sequencer
requeues at the deadline exactly, so no tunable is added. Consequences:

- Honored after any progression, including canary, and per group: the `max()`
  in `collapseSequential` goes with the collapse, so `soak: 1m` after group 0
  and `soak: 10m` after group 1 wait 1m and 10m. This is the one intended
  behavior change for lists that already work today.
- A Skipped group's soak does not run (no completion event). Soak on the last
  group stays a no-op; admission should reject it rather than document it.

### Pause, failure, rollback, restart

- **Paused** (manual canary hold, timed step, analysis bake, coordination
  `Paused`): later groups Pending, reason `BlockedByPausedGroup`.
- **Failed** (canary ready-timeout or analysis escalation, coordination
  `Failed`, Instances escalated to Failed — e.g. ImagePullBackOff): later
  groups Pending, reason `BlockedByFailedGroup`, until the operator fixes the
  spec (a retarget re-plans from group 0) or rolls back.
- **RollingBack / RolledBack** (canary sticky reject; coordination
  `RollingBack`): later groups Pending, reason `BlockedByRolledBackGroup`; the
  run closes `RolledBack` as today, and nothing after that group ever started.
- **No backward cascade.** If group 0 completed and group 1 fails, group 0
  stays on the new revision — the same mixed state the happy path passes
  through. Recovery is a spec revert, itself an ordered rollout back.
- **Controller restart.** The sequencer holds no memory; it recomputes the
  cursor from `activeRun` and engine status. By rule 4 a group mid-roll keeps
  its in-flight bypass, so a crash resumes the group rather than restarting it.
- **Retarget mid-sequence.** As in OEP-0017.1: the run closes `Superseded`, a
  fresh run opens, the cursor restarts from group 0 (groups already on target
  Skipped). `completedAt` never carries across runs, so neither does soak.
- **Repin** (`ome.io/rollout-repin`) keeps run identity and re-renders the
  plan. The cursor keeps `completedAt` for each index whose member set is
  unchanged and clears the rest. A starting position, not settled (Open
  Questions).

### Status: the cursor in `activeRun`

No new spec field. Two status additions, both under the run they serve:

```yaml
status:
  rollout:
    activeRun:
      plan:
        groups:
          - group: {components: [decoder], rollingUpdate: {...}}
            completedAt: "2026-09-21T09:14:02Z"   # new; write-once per run
          - group: {components: [engine], blueGreen: {}}
  conditions:
    - type: RolloutGroupSequence                   # new condition; False while a group waits
      reason: SoakingBehindGroup   # WaitingOnGroup | SoakingBehindGroup | BlockedByPausedGroup |
      message: 'groups[1] waits: groups[0] soaking until 2026-09-21T09:19:02Z'
                                   # BlockedByFailedGroup | BlockedByRolledBackGroup; True when idle
```

`completedAt` on `RolloutRunGroup` is the only persisted sequencer state; it
anchors the soak deadline, and the Active index is derived from it (first
group without one that has work). The condition is the waiting reason issue
#1040 asks for, with a fixed enum for dashboards. `status.rollout.groups[]`
(the resolution view) and the engines' blocks are untouched.

### Admission: what lifts, what stays

| Rejection today | With the sequencer |
|---|---|
| `GroupOrderingNotHonored` | lifted in the same change that enforces the shape; `Sequential` then admits any list |
| `OrderNotHonored` | lifted for blueGreen and rollingUpdate groups; kept for canary groups; new: `order` with `maintainRatio` rejected |
| `SoakNotHonored` | replaced by one rule: soak on the last group is rejected |
| `MultipleCanaryGroups` (one canary run per unit) | unchanged — the unit rule is about shared Components, not ordering |
| `groupOrdering: Concurrent` | unchanged; still the way to write OEP-0017.2's two independent canary units |

### Relation to the frozen instance machine and what locks this

The sequencer composes runs; it never touches an Instance cell. Its only
effect on the instance machine is the existing `gate.deny` event with a new
reason. The frozen table under `tests/rolloutspec/machines/instance/` stays so.

OEP-0020.1 planned five machine tables and only the instance one exists. The
rollout run machine (no run, parked, open, closed with three outcomes) is the
sequencer's home: its open state becomes `Open/Active(n)`, `Open/Soaking(n)`,
`Open/Blocked(n, reason)`. The sequencer should ship with that table under
`tests/rolloutspec/machines/run/`, every (state, event, window) cell decided
before the KIND specs are written, so the specs derive from the table.

### Interaction with OEP-0026

OEP-0026 lets `groups[].components` name a replica ref (`engine/llama-h100`),
a bare role meaning every ref in that role. The sequencer's member is already
an IR (completion is read per IR), so members generalize from Components to
IRs with no change to the predicate or cursor. Two rules carry over: groups
must still partition the IRs, and `order` entries must be members of the same
group. Whether a bare role expanding to several refs is one member or several
for `order` is OEP-0026's to decide.

### Landing order

1. Cursor and condition land with `collapseSequential` in place; on collapsed
   runs the sequencer observes and must agree with the Sequential machine on
   every existing KIND spec.
2. `collapseSequential` is removed; a run of single-Component blueGreen groups
   becomes N ordered groups under the cursor, with the same KIND specs as the
   bar minus the per-group soak change.
3. Admission lifts per the table, in the same PR as each shape's enforcement.
4. Stored objects admitted before PR #1080 with a non-collapsing list fall
   under rule 4 on upgrade: groups in flight finish, the rest wait their turn.

No progress deadline is introduced (rule 2); if one is ever wanted it is a
spec field, not operator config.

## Test Plan

- **Machine table** (`tests/rolloutspec/machines/run/`): every cell decided,
  under the existing well-formedness test.
- **Unit** (`rolloutrun`, `coordination`, `pkg/validation`): completion
  predicate per progression over synthesized status (every non-Complete phase
  above; `Staged`; Skipped); cursor selection (Skipped predecessors, soak
  elapsed / pending / absent, each blocking phase, fail-closed on read error,
  `completedAt` write-once); repin rule; the admission matrix.
- **envtest** (webhook suite): lifted shapes admitted under `Sequential`;
  last-group soak, canary `order`, `order + maintainRatio` rejected.
- **KIND** (`omenative_coordination_kind`, one spec file per case, each
  updating every Component in one generation and proving order from pod
  creation timestamps and `completedAt`): blueGreen then rollingUpdate;
  rollingUpdate then canary; a multi-Component group with `maintainRatio`
  after a router group; `order` inside a two-Component blueGreen group; soak
  with a manager restart mid-soak; ImagePullBackOff in group 0 holding group
  1 until fixed; manual canary pause in group 0 holding group 1; canary
  rollback in group 0 with group 1 never started; retarget during group 1;
  the existing Sequential specs on the general path; a `Concurrent` list
  still concurrent. New spec files are classified in `KIND_SPEC_GROUPS`.

## Graduation Criteria

- **Alpha:** cursor and condition shipped; run table frozen; every KIND case
  above green; admission lifted per the table; `collapseSequential` removed.
- **Beta:** one release of production use with no ordering violation; the
  console reads `RolloutGroupSequence` and `completedAt`.
- **Stable:** a further release unchanged; OEP-0026 members validated.

## Drawbacks

- A third coordinator until the engine merge, thin but real.
- Sequenced rollouts are slower by construction (`Concurrent` is the escape),
  and holding group 2 behind a paused group 1 lengthens the mixed-revision
  window; nothing here checks cross-revision compatibility.

## Alternatives

a) **Merge the engines first.** — *Tradeoff:* ordering waits on the unified
   phase mapping. Rejected on sequencing, not destination.
b) **Keep the rejection; `Concurrent` is the only multi-shape option.** —
   *Tradeoff:* un-ships OEP-0017's ordered list. Rejected.
c) **Grow `collapseSequential` shape by shape.** — *Tradeoff:* mixed lists
   need the general mechanism anyway. Rejected: enumerating the general case.
d) **Retire `order`; split into groups.** — *Tradeoff:* loses one progression
   body, soak, and policyRef for a coupled pair rolled one member at a time.
   Close call: if review prefers the smaller API, `order` stays rejected.
e) **A separate `status.rollout.sequence` block.** — *Tradeoff:* a second
   lifetime to keep in step with the run. Rejected: the cursor is run state.

## Open Questions

- **Repin and the cursor.** The unchanged-members rule is a starting position.
  Owner: implementation review.
- **Skipped-group soak.** "Skip means skip" is simpler; an operator might
  expect the wait regardless. Decide in the run table.
- **`order` under OEP-0026 role expansion.** One member or several? Owner:
  OEP-0026. **Cross-group rollback** stays out until an incident shows the
  manual revert is too slow.

**Next step:** review of the completion predicate by the owners of the
coordination and canary engines — every other mechanism leans on it — then the
run table, then the KIND specs.

**See also:** OEP-0017 (the contract this makes true), OEP-0017.1 (the run
this extends), OEP-0017.2 (canary units), OEP-0020.1 (the tables), OEP-0026
(members as refs), issue #1040, PRs #1080 and #1513 (the stopgap).
