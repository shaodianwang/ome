# Replay scenarios and golden traces

This directory is the behavior lock for the OMENative rollout engine: each
scenario drives `workload.Reconcile` over a scripted timeline and its
golden records, byte for byte, what the engine wrote. A diff means the
engine behaves differently than it did when the golden was captured.

The driver lives in `pkg/controller/v1beta1/workload/replay`; this
directory holds the scenarios, the goldens and the gates.

```
scenarios/<name>.yaml   one timeline; the file name is the scenario name
golden/<name>.trace     its recorded trace
```

## Running

```sh
go test ./tests/rolloutspec/replay/... -count=1 -timeout 300s
go test ./tests/rolloutspec/replay/... -run TestReplayMatchesGolden -update   # rewrite goldens
```

Four gates run here:

| test | proves |
|---|---|
| `TestReplayMatchesGolden` | every scenario still produces its recorded trace |
| `TestReplayIsDeterministic` | ten replays of each scenario are byte-identical |
| `TestArrowCoverage` | every cited arrow exists and is workload-lane; reports how much of the table is driven |
| `TestCellCitationsResolve` | every cited cell is a key of the expanded grid |

A scenario is loaded through `tests/rolloutspec`, so an event, variant,
arrow or cell the instance table does not declare fails the load. The
scenario vocabulary and the table cannot drift.

These four are the Go half of the behavior lock. The python half runs
ahead of them under the same `make rollout-behavior-lock` target:
`check_history.py` and `check_coverage.py` resolve the grid's provenance
and coverage both ways, and `check_test_layout.py` enforces one test file
per production file across `pkg/controller/v1beta1/workload/...` and
confines row writes to the packages that write rows. All three live in
`hack/omenative-testing/statemachine/`, and its README states their rules.

## Lanes: which arrows this directory is responsible for

A transition of the instance table carries a `lane`. The default,
`workload`, means its guard reads only what `workload.Reconcile`
receives, so a scenario here can drive it and `TestArrowCoverage` counts
it toward the Part 1 exit. `lane: adapter` marks an edge decided on
something only the InferenceReplica adapter produces — the owner
teardown deadline, the published `PodCount` / `AvailablePodCount`, the
`spec.resetInstances` mailbox — which no driver that runs the engine
alone can honestly reach. Those 20 arrows are covered by Go tests in
`pkg/controller/v1beta1/inferencereplica` that name them in an
`// Arrows:` comment, gated by `rolloutspec-check`; a scenario here that
claims one fails `TestArrowCoverage`. The **Unlocked paths** entries
below explain, per arrow, which adapter input is missing.

A scenario counts as coverage for the cells it drives: the cell names the
scenario in `coverage.replay`, `rolloutspec-check` resolves that name to a
file here, and `TestCellCitationsResolve` resolves the scenario's `cells:`
ids back to the grid.

## Golden provenance

Goldens were captured against the engine at `cf75eb72`
(`[OEP] 0020.2 rollout engine behavior lock`) and track `main` from there.
No branch recording them changes engine code, so the recorded traces are
the behavior of `main` at the commit each was last captured on — most
recently `c990ad96` (`[Bugfix] Pause freezes repair, not hold evidence`),
which moved the two `hold-*` traces that pair the scheduler with a pause.
The rest have been re-recorded only for the trace format and for the
driver's fidelity fixes (deadline parking, pod-write values, the owner
generation, the per-revision routed Service). Each of those changed how a
run is rendered, or what the driver put in front of the engine — never
what the engine does with it.

Engine fixes the lock itself surfaced are recorded the other way round:
they move the goldens of the scenarios that walk the fixed route, and
only those, with the scenario's `intentional:` note and the machine cell
rewritten in the same commit. `failed-runtime-ready-adopt` is one — the
Failed row now adopts pods already on the target instead of re-surging.

## Changing a golden

A golden diff is either the point of the change or a regression, and the
review cannot tell which from the diff alone. A commit that rewrites a
golden must also either change the table cell the behavior belongs to, or
set `intentional:` in the scenario naming the decision. An unexplained
golden rewrite is a defect.

## Scenario format

```yaml
scenario: T-surgedrain-promote     # file name; checked against transitions.yaml when it starts with T-
arrows: [T-ready-surge, T-surge-target-ready, T-surgedrain-promote]
cells:
  - instance/Update/SurgeDrain/pod.deleted[source]/boundary@T-surgedrain-promote
intentional: >-               # why this scenario's golden reads the way it does
  the source is deleted only once the per-revision routed Service has
  dropped its endpoint
initial:
  spec:    {replicas: 1, strategy: SurgeThenDrain, image: registry.example.com/runtime:v1, instanceReadyTimeout: 30m}
  config:  {requeueOperation: 5s, requeueGate: 3s}
  rows:    [{index: 0, phase: Ready, runningRevision: current, readySince: 0s}]
  pods:    [{index: 0, ready: true, serving: true, routed: true}]
timeline:
  - tick: 1
    note: what this tick is about; the note is carried into the trace
    events:
      - pod.deleted[source]: {pod: {index: 0, ordinal: 0}}
      - clock.advance: 5s
```

A tick's `note` is a plain string and is compared as a trace line, so it
is a claim about the tick rather than a comment. `routed: true` seeds the
pod's endpoint in both the Services that publish it (see **Endpoints**
below).

`initial.spec` is the driver's mutable owner spec: every tick rebuilds the
plan and the target ControllerRevision from it through the production
`workload.BuildPlan` and `revision.EnsureControllerRevision`, so a
`spec.revision` event is an image rewrite and the new hash follows from
the engine's own hashing.

## Pod age

Every pod the driver admits — created by the engine or seeded by
`initial.pods` — is stamped with the driver clock's reading as its
`creationTimestamp`, because the apiserver stamps one and the fake client
does not. The stuck-pod escalation measures a wedged pod's dwell from that
stamp and treats a pod without one as never stuck, so a scenario that wants
the fast escalation has to age the pod: either advance the clock before the
waiting reason lands (`T-create-stuck`) or use `timer.stuckPodGrace`
afterwards (`T-create-stuckgrace`).

## Terminating pods, and the one whose kubelet is gone

The driver holds every deleted pod as a Terminating object, and records the
instant the delete landed on the **injected** clock: the fake client stamps
a deletion timestamp off the wall clock, which no deadline the engine
measures can be compared against. The recorded instant is the request time
plus the grace the pod is owed, as the apiserver computes it; a fixture that
names no `terminationGracePeriodSeconds` is therefore overdue from the
instant of the request.

Ordinarily the object is held by the driver's finalizer, which is the
scenario's own hand releasing it (`pod.deleted`). `pod.stuckTerminating` is
the other way a pod stays: the kubelet behind it is gone, so the pod carries
its deletion timestamp with **no** finalizer on it and nothing will ever
remove it. That distinction is the whole of the force-delete predicate — a
Terminating pod that still carries a finalizer classifies as
`foreign-finalizers` and is only ever reported, never force-deleted.

Such a pod leaves the fake apiserver on exactly two events: the scenario's
own `pod.deleted`, or a delete with `gracePeriodSeconds: 0`, which is the
escalation's. A graceful delete on it changes nothing, as it changes nothing
on a real cluster where the object is already on its way out.

The escalation's other half is node-death evidence, and the fake cluster
holds no Node objects, so a pod the scenario has scheduled (`pod.scheduled`)
classifies as `node-gone`. The unreachable-taint and NotReady branches need
Nodes seeded first and are not driven (see **Unlocked paths**).

## The runner container

`initial.spec.containerName` names the single container the template
renders; it defaults to the fixture's own name. The restart trigger reads
restart evidence off the runner container alone and identifies it by the
name production renders it under, so a scenario driving
`pod.containerRestart` has to render that name
(`T-ready-restart-crash`). `pod.containerRestart` bumps the container's
restart count, files the previous run's termination on `lastState` and
starts the current run at the clock's reading — the kubelet's in-place
restart, where the pod keeps its UID and stays Running and the only
evidence is on the container status.

`initial.spec.env` renders the container's environment. It is part of the
pod template and therefore of the revision, and unlike the image it is not
a diff the in-place strategies may converge on, which is what makes an
`InPlaceOnly` rejection reachable.

## Spec events

`initial.spec` is the driver's mutable owner spec and a spec event is a
field write on it; every tick rebuilds the plan and the target revision
from what it says.

- `spec.revision` edits the pod template: `image:` rewrites the container
  image (the in-place-eligible diff), `env:` rewrites its environment (a
  diff the in-place strategies refuse), and an edit must name at least one
  of them.
- `spec.teardown[deleted]` begins owner deletion. From that pass on the
  planned index set reads as empty, so every Instance is a scale-down
  extra and nothing else runs. The `ownerRotated` variant — an owner
  replaced under a new UID — is not modelled and fails the run rather than
  quietly reading as an ordinary deletion.
- `spec.migrateRequest` records an operator migration request on the owner
  (`{instance, fromNode, reason, uuid}`; an unnamed request is numbered).
  That record is the only thing the migrate pass reads, so recording it is
  the whole of consuming the request annotation. It needs
  `config.migrationAudit`, without which the engine holds every request.
- `spec.rollbackTarget[set]` pins the roll to a stored revision, named by
  the `image:` whose template minted it; `[clear]` releases the pin. A pin
  overrides the rendered template with the stored payload and becomes the
  roll target, while the owner keeps reporting the rendered template as
  its update revision.
- `spec.gangWidth[grow|shrink]` rewrites `to:` as the per-Instance worker
  count. The width is outside the revision payload, so the expected pod
  set moves at an unchanged target hash.

`initial.spec.workers` makes the Component multi-pod: one `leader` Runner
and a `worker` Runner of that size, which is the unit a gang is scheduled
in. Most multi-pod scenarios also set `topologyKey` and `schedulerName`,
which keeps the two advisory gang Warnings out of their traces so each
scenario records only what it is about. `gang-advisory-warnings` is the
one that declares neither, and it is where those two Warnings are locked.

## Gangs

`initial.spec.gangScheduling` advertises the scheduler-plugins PodGroup CRD
as installed. The driver then runs the same PodGroup pass the adapter runs
ahead of the dispatcher: one observation of the owner's groups, a group
announced for every multi-pod Instance in the plan before any of its
members is created, the classification of each group recorded into
`input.Gangs`, and the inline surge prerequisite the gang-surge op reaches
back through. PodGroup creates and deletes appear in the trace by name, so
a golden shows the announce-before-pods ordering; the group's spec does
not.

`gang.podGroup[<state>]` writes what a group says onto the object itself,
so the classification the engine reads is the one it derives:
`Terminating` (deleted behind a finalizer), `Deleted`, `OwnershipConflict`
(the controller reference rewritten to another owner) and `PhaseFailed`.
The `Unschedulable` and `ScheduleTimeout` variants have no writer: whether
a gang can be PLACED never reaches the engine through the PodGroup — it
arrives on the member pods' `PodScheduled=False/Unschedulable`
conditions, which `pod.unschedulable` already writes.

A gang scenario also sets `schedulerName`, for the same reason it sets
`topologyKey`: a gang announced for pods that name no gang-aware scheduler
draws a once-per-process Warning, and a one-shot cannot be in a golden.

## Timer events

A deterministic harness can only express a timer as a clock move, so every
`timer.*` event resolves the deadline it is named for out of the state the
engine itself carries and moves the clock onto it, plus the `slack` the
scenario names. The slack is required: how far past a deadline an
observation lands is a property of the walk, not a default the harness may
invent. A deadline already behind the clock records `already-elapsed` and
moves nothing.

| event | deadline it resolves |
|---|---|
| `timer.operationDeadline` | the earliest in-flight `Operation.Deadline` |
| `timer.stuckPodGrace` | the earliest `pod.creationTimestamp + config.stuckPodGrace` over the pods holding a terminal waiting reason |
| `timer.retryAt` | the earliest `retryBlocks[].nextRetryAt` |
| `timer.migrationDeadline` | the earliest `Deadline` over the non-terminal migration records |
| `timer.forceDelete[overdueSlack]` | the earliest `pod.deletionTimestamp + config.forceDelete.overdueSlack` over the Terminating pods |

Because the deadline is read rather than written, a scenario states no
duration that config already fixes: `T-create-stuckgrace` sets
`config.stuckPodGrace` once, `T-failed-retry` never restates the backoff its
`config.updateRetry` policy computed, and the migrate scenarios never restate
the `instanceReadyTimeout` the migration record's deadline was minted from. An event fired with nothing to
resolve — no in-flight deadline, no wedged pod, no due block — fails the
run rather than silently doing nothing.

`timer.teardownDeadline` has no applier. It is decided entirely in the IR
adapter's teardown pass, outside `workload.Reconcile`, so the driver cannot
reach the arrows that fire on it (see **Unlocked paths**).

`timer.forceDelete[overdueSlack]` resolves the earliest
`pod.deletionTimestamp + config.forceDelete.overdueSlack` over the
Terminating pods. The `nodeUnreachable` variant has no deadline to resolve
and is rejected: the fake cluster holds no Node objects, so a scheduled
pod's node reads as gone rather than as unreachable for a measurable time,
and there is no interval to age (see **Unlocked paths**).

## Trace format

```
tick=3 note the pod template changes, so the rollout surges a replacement
tick=3 apply spec.revision image=registry.example.com/runtime:v2 generation=1 now=t+0s
tick=3 gate consult strategy=SurgeThenDrain surge=0 unavail=0 verdict=allow
tick=3 row=0 write incarnation=1 phase=Updating runningRevision=svc-a-engine-882059bb targetRevision=svc-a-engine-6f8b1e6c readySince=t+0s op=op#2{type=Update step=Surge target=svc-a-engine-6f8b1e6c strategy=SurgeThenDrain started=t+0s lastProgressAt=t+0s deadline=t+30m0s} precondition=ok
tick=3 event Normal RecreateUpdateStarted OMENative component=engine instance=0 surge to revision svc-a-engine-6f8b1e6c (newOrdinal=1)
tick=3 pod create name=svc-a-engine-0-default-1 revision=6f8b1e6c incarnation=1 ordinal=1
tick=3 hold cleared
tick=3 pass result=after:5s
```

Three rules hold the format together:

1. **A write prints the whole row, never a diff.** Every committed write
   renders the row's full projected state after the write, in one fixed
   field order, so the first write and the twentieth read the same and a
   line can be understood without folding the ones above it into it. An
   absent value prints as `nil`. The order is `incarnation`, `phase`,
   `runningRevision`, `targetRevision`, `activeOrdinal`, the pod counters,
   `readySince`, `op{...}`, `conditions`, `lastFailure`; the first three,
   `readySince` and `op` are always present, the rest appear when the row
   carries them. A mutation that did not commit has no state to report,
   so its line carries the attempt and the verdict alone
   (`row=0 write no-op precondition=no-change`).
2. **A tick's `note:` is a trace line.** The scenario writes the note, the
   driver copies it into the trace at that tick as `tick=N note <text>`,
   and it is compared like any other golden line — so a walk that stops
   reading as its notes describe fails.
3. **Identities print as the engine sees them.** ControllerRevision names
   and revision hashes are printed verbatim. Only what nothing in the
   fixture names is normalized: operation ids to `op#N` and object UIDs
   to `uid#N`, both in first-appearance order, because the engine and the
   apiserver mint them at random and a golden cannot carry a value that
   differs between runs. Instants render as `t+<duration>` from the
   scenario start, including timestamps embedded in event messages. Pod
   names are already deterministic and are printed verbatim. Anything
   else that varies between runs is a determinism defect, reported rather
   than normalized away.

Line kinds: `note`, `apply` (a scenario event staged), `row` (one buffered
status mutation with its precondition verdict), `retryblock`, `event` (a
Kubernetes event), `pod` / `podgroup` (create / patch / delete, including
a rejected or failed one), the callback lines (`gate consult`, `hold`,
`warn`, `condition`, `migration`), and `pass result=` closing every tick.

A row write after the `pass result=` line of a tick is the adapter's
deadline park/rearm step, which runs after the dispatcher on every
reconcile (and, when paused, once before it as well).

## Endpoints: which Service a pod is published in

The engine reads two endpoint views, and the driver writes both.

The **per-revision routed Service** (`<owner>-<component>-rev-<hash>`) is
what the drain gate, the in-rotation gate and the source-rotation report
read: a pod is resolved to it by its revision-hash label, and a gang
worker has none because workers serve no customer traffic. The
**Component headless Service** is what the status aggregate counts
available pods from. A pod in rotation holds an endpoint in both, and a
rotation write sweeps it out of every Service that no longer publishes it
— an in-place roll rewrites the revision label, which moves the pod to a
different routed Service.

This is why a drain is a real gate in a replay: a source is deleted only
once the scenario has taken it out of the routed Service, and a source
dropped from that Service while its surge replacement is still coming up
produces the `SourceUnrouted` hold (`source-unrouted-midsurge`). The
driver creates no Service object for a revision, only the slice, so a
revision no pod was ever routed on reads as "nothing routes here" and
drains trivially — the same answer the engine gives for a Service that
does not exist.

## Deviations from the OEP-0020.2 design details

The OEP's shapes are the starting point; these are the places the engine
or determinism forced a change, and they are the ones to review.

1. **`arrows:` is a list, separate from the scenario name.** The OEP wrote
   the arrow id in `scenario:`. A walk normally drives several arrows on
   the way to the one it is written for (the surge scenario drives three),
   and the arrow-coverage join needs all of them. A scenario name starting
   with `T-` is still checked against `transitions.yaml`; a free name — a
   hold that drives no transition — is not, and cites cells instead.
2. **Revisions render as the names the engine sees, not `rev-a` / `rev-b`.**
   The driver mints real ControllerRevisions through the production
   hasher, so a golden carries the content hash the engine computed. A
   hashing change therefore rewrites every golden, which is the honest
   report: the names the engine passes around really did change.
3. **A row line is the whole row, and non-committing mutations are
   recorded too.** `row=0 write no-op precondition=no-change` is an
   attempted write the engine made that turned out to change nothing —
   engine behavior, and a line a refactor can move.
4. **A tick's `apply` line comes with the clock reading.** The OEP's
   example showed only the effects; a walk is much easier to read when the
   observation and the instant it happened at are in the trace.
5. **`initial.rows` seeds no in-flight operation.** `RowSpec` carries
   phase, incarnation, running revision, active ordinal and `readySince`,
   and nothing else. A hand-written operation can contradict invariants the
   engine maintains between a row and its pods; a scenario reaches an
   operation by driving the engine into it. Most scenarios therefore start
   from an empty cluster and walk, which also puts the create path in every
   golden.
6. **Pod deletion is two observations, not one.** The driver stamps a
   finalizer on every pod, so the engine's delete leaves a Terminating
   object and the scenario's `pod.deleted` is what removes it. That is the
   real sequence, and it is what lets a scenario hold a pod in Terminating.
   `pod.stuckTerminating` is the second hold, the one with no finalizer
   behind it (see **Terminating pods** above).
7. **`clock.advance` is the one event outside the table's vocabulary.** It
   is harness control, not an observation. `timer.operationDeadline` is a
   table event and is implemented as a clock move onto the earliest
   in-flight deadline plus a slack.
8. **The driver assigns pod UIDs.** The fake client assigns none and the
   engine refuses to delete a pod without an observed UID. They are numbered
   in creation order and normalized to `uid#N`.
9. **Expectations are confirmed by the driver, not by an informer.** A pod
   create is observed as soon as the object exists; a delete is observed
   when the object leaves the apiserver, whether the scenario's
   `pod.deleted` took it out or the force-delete sweep did.
10. **The deadline park/rearm step is modelled, the rest of the aggregate
    pass is not.** The adapter parks every in-flight deadline before the
    dispatcher when paused, and parks or rearms admission-held deadlines
    after it on every reconcile; the driver does both. The counters and
    conditions the same aggregate pass publishes are not computed.
11. **An event may carry only the arguments it declares.** The parser
    rejects anything else, so a mistyped argument fails the load instead
    of silently doing nothing, and `timer.operationDeadline` requires its
    slack rather than defaulting to one the harness invented.
12. **A row line omits `ReadyPodCount`, `ScheduledPodCount` and
    `NodesOccupied`.** Those are materialized only on the status
    publication path, which runs outside `workload.Reconcile`, and the
    workload package's removable-field audit forbids reading them
    anywhere else. A trace cannot honestly report a field the run never
    writes.

## What is not modelled yet

The table events the driver can stage are the ones the scenarios here need:
`pod.scheduled`, `pod.containersReady`, `pod.ready`, `pod.servingOn/Off`,
`pod.terminating`, `pod.stuckTerminating`, `pod.deleted`,
`pod.phaseTerminal`, `pod.waiting`,
`pod.unschedulable`, `pod.containerRestart`, `endpoint.rotationIn/Out`, `api.quotaDenied`,
`api.invalid`, `spec.revision`, `spec.replicasUp/Down`,
`spec.pauseTrue/pauseFreeze/unpause`, `spec.strategy`, `spec.teardown`,
`spec.migrateRequest`, `spec.rollbackTarget`, `spec.gangWidth`,
`gang.podGroup`,
`timer.operationDeadline`, `timer.stuckPodGrace`, `timer.retryAt`,
`timer.migrationDeadline`, `timer.forceDelete[overdueSlack]`,
and `clock.advance`. A scenario naming any other declared event fails to load
with that as the reason, so the gap is always visible rather than silent.


## Mutation check

The lock is only worth its runtime if a behavior change fails a golden.
The procedure, run once under this trace format on the goldens recorded
above:

1. Edit one predicate in the engine to the wrong constant answer.
2. `go test ./tests/rolloutspec/replay/ -count=1 -run TestReplayMatchesGolden -v`
   and record every scenario that fails.
3. `git checkout` the file. Nothing here is committed mutated.

Recorded result, over 118 scenarios:

| mutation | goldens failed |
|---|---|
| `isSurgeOwnedStatus` → `false` **and** `effectiveUpdateStrategy`'s pin dropped (`ops/update.go`) | 1: `strategy-edit-midsurge` |
| `supersedes` → `false` (`types/externalhold.go`) | 3: `hold-pause-then-quota`, `hold-pause-then-unrouted`, `hold-pause-then-unschedulable` |
| `supersedes` → `true` | 4: `hold-pause-then-quota`, `hold-pause-then-unrouted`, `hold-quota-then-pause`, `hold-unschedulable-then-pause` |
| the first-writer-wins guard in `MarkExternalHold` removed | the same 4 |
| `schedulerHoldWritable` → `true` (`unschedulable.go`) | none |
| that guard removed **and** `schedulerHoldWritable` → `true` | 6: `hold-gang-then-unschedulable`, `hold-nodeunknown-then-unschedulable`, `hold-pause-then-quota`, `hold-pause-then-unrouted`, `hold-quota-then-pause`, `hold-unschedulable-then-pause` |
| that guard removed **and** `gangHoldWritable` / `nodeUnknownHoldWritable` → `true` | 6: `hold-pause-then-quota`, `hold-pause-then-unrouted`, `hold-quota-then-pause`, `hold-unschedulable-then-gang`, `hold-unschedulable-then-nodeunknown`, `hold-unschedulable-then-pause` |
| `MarkExternalHold` never marking (the control) | 13: the 10 `hold-*` scenarios plus `quota-hold-parks-deadline`, `source-unrouted-midsurge`, `surge-force-clock` |

Three things to read off it.

**The surge guards are only sensitive as a pair.** Flipping
`isSurgeOwnedStatus` alone changes nothing, because
`effectiveUpdateStrategy` already pins the mechanism from
`Operation.Strategy`, which every row this build writes carries; the two
are redundant for any row the harness can produce.
`isSurgeOwnedStatus` is load-bearing only for a surge row persisted
*without* a pinned strategy, a shape an older build wrote, and the
scenario format deliberately cannot seed an in-flight operation. With
both disabled, `strategy-edit-midsurge` fails — it locks that a strategy
edit does not derail an attempt already in flight.

**Hold precedence is locked by the `hold-*` scenarios.** Each one puts a
second authority on a row that already carries a token, which is the only
situation the precedence rules decide, and records the token after every
authority arrives and after every one clears. Inverting `supersedes` in
either direction, or dropping the first-writer-wins guard, now fails a
golden. The pairs and what wins:

| row already holds | authority that arrives | outcome | scenario |
|---|---|---|---|
| `Paused` | `SourceUnrouted` | takes the row (the one ordered pair) | `hold-pause-then-unrouted` |
| `SourceUnrouted` | `Paused` | refused; the surge keeps it | `hold-unrouted-then-pause` |
| `Paused` | `QuotaExceeded` | takes the row | `hold-pause-then-quota` |
| `QuotaExceeded` | `Paused` | refused; quota keeps it | `hold-quota-then-pause` |
| `Unschedulable` | `Paused` | refused; the scheduler keeps it | `hold-unschedulable-then-pause` |
| `Paused` | `Unschedulable` | takes the row | `hold-pause-then-unschedulable` |
| `PodGroupTerminating` | `Unschedulable` | refused; the gang keeps it | `hold-gang-then-unschedulable` |
| `Unschedulable` | `PodGroupTerminating` | refused; the scheduler keeps it | `hold-unschedulable-then-gang` |
| `NodeUnknown` | `Unschedulable` | refused; the recovery keeps it | `hold-nodeunknown-then-unschedulable` |
| `Unschedulable` | `NodeUnknown` | refused; the scheduler keeps it | `hold-unschedulable-then-nodeunknown` |

Every one of them also records the release order — the incumbent's
authority reports its wait over and the other takes the row, in the same
pass where the two writers are ordered that way and on the next pass
where they are not.

**The per-authority eligibility rules are redundant with the shared
guard, the same way the surge guards are.** `schedulerHoldWritable`,
`gangHoldWritable` and `nodeUnknownHoldWritable` each refuse an operation
parked on another token, which is what `MarkExternalHold`'s
first-writer-wins guard already decides; flipping one alone changes no
golden, and it takes the pair to move one. The two rules are not the same
rule — the writable ones also refuse a `Delete`, and each names the
operator pause as its one exception, without which `supersedes` could
never fire for them — but no row the harness can build separates them.

A pause reaches that rule like any other incumbent, because the evidence
half of the end-of-pass bookkeeping runs on a paused Component
(`holdevidence.go`) while the repair half stays suspended: the scheduler,
the gang arm and the name recovery go on recording and releasing their
holds under a pause, and a fact they report takes the row from it in the
pass they report it. `hold-pause-then-unschedulable` records the
scheduler taking a paused row and the unpause changing nothing on it;
`hold-unschedulable-then-pause` records the mirror, a placed pod
releasing the scheduler's token while the pause is still on.

One pair the engine cannot be made to contend on, recorded as such:

- **`Paused` and `NodeUnknown`.** The recovery writes its hold from the
  Create pass, and a `Create` is not an operation the pause holds
  (`pauseHeldOperation`), so the two are never on the same row. The one
  shape that could put them there — a repair continuation finishing its
  step under a frozen pause — is one the scenario format cannot stage.

## Unlocked paths

What the goldens do NOT record, so a reader knows the lock's edges. Each
is a real surface of `workload.Reconcile` that this driver either does not
reach or does not observe.

Callbacks and deps left unwired:

- `FinalizeInstanceResources` — per-Instance resource teardown.
- `Deps.RenderHook` — production wires it unconditionally to inject peer
  env vars. In-place eligibility diffs on env, so this is the most
  consequential omission on the list.
- `ScaleUpPodBatchSize` / `ScaleDownPodBatchSize` — left nil, so every
  pass is unbounded and the batch-budget branches never run.
- `AuthoritativePods` — no preloaded observation, so every destructive
  consumer takes the per-pass List path.

Effects the driver does not record:

- The migration audit ledger ConfigMap. `MutateMigration` and
  `AppendMigration` are backed by an in-memory record list and traced, but
  the ledger the audit package writes is not.
- PodGroup detail. A PodGroup create or delete is traced by name; its
  spec (minMember, topology key, schedule timeout) is not.
- A PodGroup's collection FINISHING. `gang.podGroup[Terminating]` holds
  the object behind the driver's finalizer and `[Deleted]` issues a
  delete, which on an object already Terminating changes nothing — there
  is no `pod.deleted` equivalent for a group. So a `PodGroupTerminating`
  hold can be recorded and refused but never released, and the gang arm
  taking over from a cleared pod token is recorded from the other side
  (`hold-unschedulable-then-gang`) instead.

- The owner's teardown deadline. `timer.teardownDeadline` and the 14
  arrows that fire on it are decided in the IR adapter's teardown pass,
  which removes the finalizer; `workload.Reconcile` never sees it.
  Adapter lane.
- Timestamps on aggregate conditions and on `LastFailure`, because the
  engine stamps both from the wall clock (see Follow-ups).

- The migration completion tail. The surge's availability gate reads the
  row's `PodCount` and `AvailablePodCount`, which only the status
  publication path writes (see deviation 12 above), so a replayed
  migration never advances past `SurgePending` and the promote and
  completion arrows are out of reach. Everything the record decides before
  that gate — early wedge evidence, the deadline, teardown — is reachable.
  Adapter lane.

- The force-delete sweep in the migration drain, and
  `T-migrate-force-delete` with it. It sits behind `surgeTailGatesPassed`,
  which reads the same publication-path counters the migration completion
  tail needs (above), so a replayed migration parks at `SurgePending` and
  never reaches the source drain the sweep runs in. The sweep itself is
  driven from the scale-down wave and from the top of the update pass.
  Adapter lane.

- The force-delete escalation's other two evidence branches,
  `node-unreachable-taint` and `node-not-ready`. Both age from a Node
  object's own taint or condition, and the fake cluster holds none, so a
  scheduled pod's node classifies as `node-gone`. Driving them needs a Node
  applier the scenarios here do not call for.

- The gang-member-loss rebuild of a row below Ready. Outside
  `Phase=Failed` the trigger reads the row's recorded `PodCount` before
  it compares live pods against the expectation, and `PodCount` is the
  same publication-path counter the migration tail needs, so the two
  Pending restart arrows cannot be reached. The same trigger from a
  Failed row skips that check and is driven (`failed-restart`).
  Adapter lane.

- The operator reset mailbox. `spec.resetInstances` is decided in the IR
  adapter, outside `workload.Reconcile`, and has no applier, so the reset
  arrow out of Failed is out of the replay lane. Adapter lane.

Reads, which are never traced at all: EndpointSlices, Services,
ControllerRevisions, and Nodes.

Two apiserver behaviors are absent: there is no conflict retry (the fake
client raises no 409, so the adapter's `RetryOnConflict` loop is never
entered), and a rejection injected by a scenario is delivered
synchronously rather than racing a concurrent writer.

## Follow-ups

- A clock seam for `podreadiness` (`readiness.go:160,245`) and
  `ops/restart.go:501`, both of which read `metav1.Now()` directly. Until
  they take the injected clock, the trace deliberately omits the values
  they stamp (the serving condition's transition time, `LastFailure.Time`).
- Modelling `Deps.RenderHook`, which production wires unconditionally and
  which injects peer env vars. In-place eligibility diffs on env, so a
  hook that changed env would change in-place behavior; the driver leaves
  it nil because a faithful stand-in needs the coordination package the
  workload layer deliberately does not import.
