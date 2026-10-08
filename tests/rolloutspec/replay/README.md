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

A fifth gate, `TestCoverageFloorHolds` in `tests/rolloutspec`, holds each
table to a ratchet: every machine has a `machines/<name>.coverage-floor.yaml`
recording how many of its authored rows cite no check (`uncovered`: every
row with an outcome other than n/a and an empty `coverage`) and how many
are not yet `decided` (`undecided`: status current or review), with the
date the numbers were last lowered. A count at or under its number passes;
a count above it fails and lists the rows behind the number, so a row added
without coverage or a decision has to be covered or decided in the same
change. The numbers only go down: lower them when rows gain coverage or are
decided, and refresh `set_on`.

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
generation, the per-revision routed Service, the grace-zero delete
removing its pod at once). Each of those changed how a run is rendered,
or what the driver put in front of the engine — never what the engine
does with it.

Engine fixes the lock itself surfaced are recorded the other way round:
they move the goldens of the scenarios that walk the fixed route, and
only those, with the scenario's `intentional:` note and the machine cell
rewritten in the same commit. `failed-runtime-ready-adopt` is one — the
Failed row now adopts pods already on the target instead of re-surging.
One such fix touches every disposition rather than one route: the
deadline and stuck-pod disposition reads the fresh row through the row
seam before it writes, and the trace renders that read as a
`row=N write no-op precondition=no-change` line ahead of the
disposition's writes in every scenario that reaches one.

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

A revision field in `initial` — a row's `runningRevision` or
`targetRevision`, an operation's `targetRevision`, a retry block's
`revision`, `initial.currentRevision` — names a revision the way
`spec.rollbackTarget` does: `current` is the revision the initial spec
renders, and an image reference is the revision that image's template
mints. A pod's `previousImage` renders the pod from that template, so a
seeded pod and a seeded row can agree on a revision the spec has already
moved past.

## Operations in flight at the start

A row may be seeded mid-operation, which is what a controller restarted
mid-rollout finds in the owner status:

```yaml
initial:
  currentRevision: registry.example.com/runtime:v1
  rows:
    - index: 0
      phase: Updating
      runningRevision: registry.example.com/runtime:v1
      targetRevision: current
      readySince: 0s
      operation: {type: Update, step: Surge, strategy: SurgeThenDrain}
  pods:
    - {index: 0, ordinal: 0, previousImage: registry.example.com/runtime:v1, ready: true, serving: true, routed: true}
    - {index: 0, ordinal: 1, phase: Pending}
```

`operation` carries what the engine's own stamps write: `type`, `step`,
`targetRevision` (an Update defaults it to the row's), `strategy`,
`surgeIndex` (a gang surge's replacement, or the other half of a migration
pair), `requestUUID` (a Migrate pin), `reason`, `waiting`, `retryCount`,
and `startedAt`, `lastProgressAt` and `deadline` as offsets from the
scenario start. A deadline left out is `startedAt` plus the spec's
`instanceReadyTimeout`, as the stamps derive it, and the operation id is
minted in the shape the engine mints it, so the row reads in a trace
exactly as one the engine opened. The seed is checked against the engine's
own owner table: a phase and operation that `types.StateOf` resolves to no
state of the machine fails the run before the first tick, because no pass
would drive it.

A Migrate pin resumes from its `status.migrations` record, so it needs one
in `initial.migrations` (`{uuid, instance, surgeIndex, fromNode, reason,
phase, startedAt, allocatedAt}`; a record with a surge index reads
`SurgePending` and allocated at its start, one without reads `Accepted`).
A seeded gang's PodGroups are announced by the first pass, as for any
gang whose members already stand, and a pod seeded `terminating: true` is
held behind the driver's finalizer the way `pod.terminating` leaves it.

The driver seeds the shape and nothing else: the pods a seeded step
implies are the scenario's to declare, and a row whose pods contradict it
is answered by the engine in the trace, not by the loader. The
`*-resumed-from-status` scenarios start one family each mid-operation —
surge, in-place, recreate, gang surge, migrate, restart and scale-down
delete — and run it to settle; `surge-resumed-without-strategy-pin` seeds
the shape of a surge row persisted without a pinned strategy while the
desired strategy has moved on. Most other scenarios still start from an
empty cluster and walk, which puts the create path in every golden.

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
on a real cluster where the object is already on its way out. A grace-zero
delete removes any pod at once, Terminating or not: the apiserver does not
wait for a kubelet on it, and the driver's finalizer, which stands in for
that kubelet, does not survive it.

The escalation's other half is node-death evidence. The fake cluster holds
no Node until a scenario names one, so a pod the scenario has scheduled
(`pod.scheduled`) classifies as `node-gone`; `pod.nodeLost` writes the
other two forms (see **Nodes**).

## Nodes

`pod.nodeLost[<form>]` is the node under a bound pod dying, in the three
forms the force-delete evidence tells apart. `NodeGone` deletes the Node
object, if the cluster holds one. `UnreachableTaint` writes the Node with
the `node.kubernetes.io/unreachable` taint stamped `TimeAdded` now and
`NodeReady` Unknown; `NodeNotReady` writes it with `NodeReady` False
transitioned now. Those are the instants the evidence ages from, so
`timer.forceDelete[nodeUnreachable]` resolves the earliest of them plus
`config.forceDelete.nodeUnreachableThreshold` over the nodes the pods are
bound to. The last two forms also take the pod out of `Ready` for the
node lifecycle controller's `NodeNotReady` reason, as that controller does
for every pod on such a node; a kubelet that stopped reporting leaves the
container statuses as they were. An unbound pod has no node to lose and
fails the run.

## Scheduling gates

`pod.schedulingGated` is a queue admission holding a pod: the pod carries
a scheduling gate, stays Pending and reports `PodScheduled=False` with
the scheduler's `SchedulingGated` reason, which is the object the
engine's admission predicate reads. `pod.admitted` takes every gate and
that report off and leaves the pod to the scheduler, whose verdict is a
separate observation (`pod.scheduled` or `pod.unschedulable`). A real
cluster stamps the gate on admission; the driver writes it onto the
created pod after the fact. A bound pod cannot be gated and an ungated pod
cannot be admitted; either fails the run.

## Readiness regression

`pod.containersNotReady` is a running pod's readiness regressing: the
kubelet drops `ContainersReady`, folds that into `Ready`, and reports the
containers not ready while they keep running. It is an observation about
a pod that was ContainersReady, so a pod that never was fails the run.

## Kubelet admission rejection

`pod.admissionRejected[<reason>]` is the node refusing a scheduled pod:
the pod goes Failed with the refusal as its pod-level `status.reason`,
`message:` as the kubelet's message, and no container ever runs.
`UnexpectedAdmissionError` is the reason itself; `OutOfResource` stands
for the kubelet's `OutOf<resource>` family and names the resource through
`resource:`, so `resource: nvidia.com/gpu` reads as `OutOfnvidia.com/gpu`,
the spelling the engine's predicate recognizes. Only a bound pod can be
refused by its node, so an unscheduled one fails the run.

## A pod reappearing under a new UID

`pod.uidRotated` is a pod of the same name reappearing under a new UID:
the object the engine observed is gone and an identical one — same
labels and template, fresh, unscheduled, Pending — stands where it was.
The watch would see a delete and an add, so the expectations balance,
and a delete the engine pinned to the old UID no longer matches anything.

## Eviction

`pod.evicted` is the kubelet evicting a bound pod under node pressure:
the pod goes Failed with the `Evicted` reason and `message:` as the
kubelet's message, its containers killed. An eviction through the
Eviction API is a graceful delete and is staged as `pod.terminating`
instead; only a node evicts under pressure, so an unbound pod fails the
run.

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

## Apiserver refusals

`api.quotaDenied`, `api.invalid`, `api.notFound`, `api.alreadyExists`,
`api.throttled[TooManyRequests|ServiceUnavailable]` and
`api.namespaceTerminating` arm an apiserver refusal for the engine's next
matching write (`verb`, `resource`, `count`); a write the scenario itself
stages never consumes one. The verb defaults to the write the refusal is
the natural answer to — `create` for a quota, validation, already-exists,
shed or terminating-namespace refusal, `delete` for not found. A shed
refusal carries the `retryAfter:` the scenario names, if any, which is
what the engine's pacing reads. The two answers that state a fact about
the cluster make it true as they are given: behind a not-found answer the
pod the write addressed leaves the apiserver, and behind an already-exists
answer the pod the engine rendered lands as the earlier writer's object,
observed like any create, so the next read finds exactly what the answer
said.

`api.conflict` is the one refusal with a retry behind it. On the owner
status — its default resource, the one the row store stands in for — it
answers the `write:`-th status write of the next pass with a 409, `count`
attempts in a row, and the store does what the adapter's
`RetryOnConflict` does: every refused attempt is traced as
`status conflict write=N attempt=A/B`, the batch is re-applied against a
fresh snapshot, and the budget exhausted hands the conflict to the engine
with nothing committed. A conflict armed for a write the pass never
reaches fails the run. On any other resource it is an ordinary refusal of
the engine's next matching write, `update` by default.

## Spec events

`initial.spec` is the driver's mutable owner spec and a spec event is a
field write on it; every tick rebuilds the plan and the target revision
from what it says.

- `spec.revision` edits the pod template: `image:` rewrites the container
  image (the in-place-eligible diff), `env:` rewrites its environment (a
  diff the in-place strategies refuse), `annotations:` rewrites the pod
  template's annotations with ones the hasher reads as intent (a diff an
  in-place step patches without replacing a container), and an edit must
  name at least one of them.
- `spec.annotationOnly` rewrites the pod template's `labels:` and
  `annotations:` (`initial.spec.podLabels` / `podAnnotations` seed them)
  with an edit the revision hasher derives rather than reads as intent —
  a queue label, a placement marker — so the target revision stands. The
  hasher is the judge: an edit that moves the hash is a template change
  and fails the load, because the table spells that edit `spec.revision`.
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
- `spec.partition` rewrites `rollingUpdate.partition` to `to:`, the canary
  hold that keeps the Instances below it on their revision
  (`initial.spec.partition` seeds it). The partition is outside the
  revision payload, so the edit retargets nothing; what it changes is
  which rows the next update pass may select.
- `spec.gangWidth[grow|shrink]` rewrites `to:` as the per-Instance worker
  count. The width is outside the revision payload, so the expected pod
  set moves at an unchanged target hash.
- `spec.policyTuning[<knob>]` rewrites one lifecycle knob to `value:`,
  spelled as the owner spec spells it: `restartPolicy`, `migrationPolicy`
  (the migration mode), `instanceReadyTimeout` (a duration),
  `minReadySeconds` (a count of seconds) and `markNotReady` (a boolean).
  No knob feeds the revision payload, so the edit retargets nothing; what
  it changes is the next decision that reads the knob, which is what a
  scenario lands against a roll in flight.

`initial.spec.workers` makes the Component multi-pod: one `leader` Runner
and a `worker` Runner of that size, which is the unit a gang is scheduled
in. Most multi-pod scenarios also set `topologyKey` and `schedulerName`,
which keeps the two advisory gang Warnings out of their traces so each
scenario records only what it is about. `gang-advisory-warnings` is the
one that declares neither, and it is where those two Warnings are locked.

## Config events

`config.changed[<knob>]` rewrites one operator-configuration knob between
two passes, which is what the adapter's short-lived ConfigMap cache turns
an operator edit into: the very next pass reads the new value. The
argument is the knob's whole new value — `{config: {stuckPodGrace: 1m}}`
— so a group left empty clears the knob, and a field outside the named
knob fails the load, because the variant says which knob changed. The
knobs are `updateRetry`, `stuckPodGrace`, `forceDelete`,
`teardownDeadline`, `scaleDownInterval` (`scaleDownRequeueInterval`),
`autoMigrateBudget`, `audit` (`migrationAudit`) and `requeue` (both
cadences). `gangScheduleTimeout` and `retryBlockHistoryLimit` are refused
by name: the driver carries neither the PodGroup schedule clamp nor the
adapter's RetryBlock history prune, so there is nothing for the edit to
change.

## Controller events

`ctrl.crash` is the controller process restarting. The cluster and the
owner status are durable and stay as they are; what the restarted engine
loses is the expectations cache, the one thing it keeps in memory between
passes. The driver drops it, so the next pass reads every pending create
and delete as satisfied and re-derives each row's step from status and
the pods alone. A scenario places it between a pass that issued a write
and the observation that would confirm it (`surge-abandon-resumed`).

`ctrl.staleRead` is the informer lagging one event behind the apiserver
for one pass. The engine holds two readers — the cached client and the
live reader (`Deps.Client` and `Deps.APIReader`) — and the driver hands
it exactly that pair: during a stale pass every read through the cached
client is served from a copy of the cluster taken at the end of the
previous tick, every write through it goes to the apiserver, and the live
reader stays current. The adapter's own cached read, the deadline park
step, lags with it. The snapshot copies every kind
the fake cluster holds, so whatever the engine chooses to read through
its cache is stale; what it reads live is not. In this engine that line
falls almost entirely between pods and everything else: the update pass,
the create pass's existing-pod list, the hold pass, the escalation pass,
the force-delete sweep and the planning screens read pods from the cache,
while EndpointSlices, ControllerRevisions, PodGroups, Nodes and the audit
ledger are only ever read live, and every destructive read of pods goes
live too. A scenario names the event on the tick whose pass should lag
(`surge-stale-cache-replacement-ready`); the driver copies the cluster at
the end of every tick only for a scenario that names it.

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
| `timer.repairRetry` | the earliest `lastFailure.time` plus the `config.updateRetry` delay for the next attempt, over the Restarts parked at Failed |
| `timer.migrationDeadline` | the earliest `Deadline` over the non-terminal migration records |
| `timer.forceDelete[overdueSlack]` | the earliest `pod.deletionTimestamp + config.forceDelete.overdueSlack` over the Terminating pods |
| `timer.forceDelete[nodeUnreachable]` | the earliest unreachable-taint `TimeAdded` or `NodeReady` transition plus `config.forceDelete.nodeUnreachableThreshold` over the nodes the pods are bound to |
| `timer.teardownDeadline` | the owner's deletion instant (`spec.teardown`) plus `config.teardownDeadline` |

Because the deadline is read rather than written, a scenario states no
duration that config already fixes: `T-create-stuckgrace` sets
`config.stuckPodGrace` once, `T-failed-retry` never restates the backoff its
`config.updateRetry` policy computed, and the migrate scenarios never restate
the `instanceReadyTimeout` the migration record's deadline was minted from. An event fired with nothing to
resolve — no in-flight deadline, no wedged pod, no due block — fails the
run rather than silently doing nothing.

## The teardown deadline

The owner's teardown deadline is decided in the IR adapter's teardown
pass, ahead of the dispatch into `workload.Reconcile`, and the driver
stands in for exactly that check: when a deleted owner (`spec.teardown`)
has held its finalizer past `config.teardownDeadline`, the tick records
the adapter's `TeardownDeadlineExceeded` Warning with the pods that
survive, then `owner deleted rows=N` — the finalizer release, which takes
the owner object and every row and retry block it carried in one stroke
— and runs no pass. Every later tick records `owner gone; nothing
reconciles`, which is the late observation the boundary and after cells
describe. The surviving pods stay where they are: the fake apiserver runs
no background GC. An unconfigured deadline holds strictly, so
`timer.teardownDeadline` fails the run rather than inventing one, and the
owner's deletion instant is the clock's reading when `spec.teardown`
first applies (a later `spec.teardown` is the deletion observed again and
moves nothing), so a deadline that elapsed before the controller ever
observed the deletion is staged by naming both in one tick. The arrows that fire
on the deadline stay adapter-lane: a scenario here cites their cells, and
the Go tests in the adapter package keep the arrows.

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
status mutation with its precondition verdict), `retryblock`,
`status conflict` (a status write the apiserver refused with a 409, by
its ordinal in the pass and the retry attempt), `event` (a Kubernetes
event), `pod` / `podgroup` (create / patch / delete, including a
rejected or failed one), the callback lines (`gate consult`, `hold`,
`warn`, `condition`, `migration`), and `pass result=` closing every tick.

A `pod create` line carries `image=` only when the created pod does not
run the image the spec currently renders: a pod rebuilt from a stored
revision after the template moved. The revision label says which revision
the engine stamped; the image says which template it rendered, and the
two must name the same revision.

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
produces the `SourceUnrouted` hold (`source-unrouted-midsurge`). A drain
the per-Component budget withholds is reported the way the adapter
reports it: the driver hands the engine the drain-hold sink, so the
`hold` line names the Instance the roll waits on
(`held-crash-policy-flip-fix-after-rebuild`). The
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
5. **Pod deletion is two observations, not one.** The driver stamps a
   finalizer on every pod, so the engine's delete leaves a Terminating
   object and the scenario's `pod.deleted` is what removes it. That is the
   real sequence, and it is what lets a scenario hold a pod in Terminating.
   `pod.stuckTerminating` is the second hold, the one with no finalizer
   behind it (see **Terminating pods** above).
6. **`clock.advance` is the one event outside the table's vocabulary.** It
   is harness control, not an observation. `timer.operationDeadline` is a
   table event and is implemented as a clock move onto the earliest
   in-flight deadline plus a slack.
7. **The driver assigns pod UIDs.** The fake client assigns none and the
   engine refuses to delete a pod without an observed UID. They are numbered
   in creation order and normalized to `uid#N`.
8. **Expectations are confirmed by the driver, not by an informer.** A pod
   create is observed as soon as the object exists; a delete is observed
   when the object leaves the apiserver, whether the scenario's
   `pod.deleted` took it out or the force-delete sweep did.
9. **The deadline park/rearm step and the published pod count are
   modelled, the rest of the aggregate pass is not.** The adapter parks
   every in-flight deadline before the dispatcher when paused, and parks or
   rearms admission-held deadlines after it on every reconcile; the driver
   does both. The adapter also writes each row's `PodCount` from the pass's
   pod observation, which the next pass reads; the driver hands the engine
   the previous pass's count on every row the same way, without rendering
   it. The other counters and the conditions the same aggregate pass
   publishes are not computed.
10. **An event may carry only the arguments it declares.** The parser
    rejects anything else, so a mistyped argument fails the load instead
    of silently doing nothing, and `timer.operationDeadline` requires its
    slack rather than defaulting to one the harness invented.
11. **A row line omits `ReadyPodCount`, `ScheduledPodCount` and
    `NodesOccupied`.** Those are materialized only on the status
    publication path, which runs outside `workload.Reconcile`, and the
    workload package's removable-field audit forbids reading them
    anywhere else. A trace cannot honestly report a field the run never
    writes.
12. **The teardown deadline is a driver stand-in, not the adapter's
    code.** The check is three lines the adapter runs ahead of its
    dispatch — deletion instant plus deadline, a Warning, the finalizer
    lifted — and the driver mirrors them rather than importing the
    adapter, which the workload tree never does. The Warning's reason is
    pinned to the adapter's constant by a test; its message is the
    driver's own wording.

## What is not modelled yet

The table events the driver can stage are the ones the scenarios here need:
`pod.scheduled`, `pod.containersReady`, `pod.containersNotReady`,
`pod.ready`, `pod.servingOn/Off`,
`pod.terminating`, `pod.stuckTerminating`, `pod.deleted`, `pod.uidRotated`,
`pod.phaseTerminal`, `pod.admissionRejected`, `pod.evicted`, `pod.nodeLost`,
`pod.waiting`,
`pod.unschedulable`, `pod.schedulingGated`, `pod.admitted`,
`pod.containerRestart`, `endpoint.rotationIn/Out`, `api.quotaDenied`,
`api.invalid`, `api.notFound`, `api.alreadyExists`, `api.throttled`,
`api.namespaceTerminating`, `api.conflict`, `spec.revision`,
`spec.annotationOnly`,
`spec.replicasUp/Down`,
`spec.pauseTrue/pauseFreeze/unpause`, `spec.strategy`, `spec.teardown`,
`spec.migrateRequest`, `spec.rollbackTarget`, `spec.gangWidth`, `spec.partition`,
`config.changed` (every knob but `gangScheduleTimeout` and
`retryBlockHistoryLimit`), `gang.podGroup`,
`timer.operationDeadline`, `timer.stuckPodGrace`, `timer.retryAt`,
`timer.repairRetry`, `timer.migrationDeadline`, `timer.forceDelete`,
`timer.teardownDeadline`, `ctrl.crash`, `ctrl.staleRead`,
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
| `isSurgeOwnedStatus` → `false` (`ops/update.go`) | 1: `surge-resumed-without-strategy-pin` |
| `isSurgeOwnedStatus` → `false` **and** `effectiveUpdateStrategy`'s pin dropped (`ops/update.go`) | 2: `strategy-edit-midsurge`, `surge-resumed-without-strategy-pin` |
| `supersedes` → `false` (`types/externalhold.go`) | 3: `hold-pause-then-quota`, `hold-pause-then-unrouted`, `hold-pause-then-unschedulable` |
| `supersedes` → `true` | 4: `hold-pause-then-quota`, `hold-pause-then-unrouted`, `hold-quota-then-pause`, `hold-unschedulable-then-pause` |
| the first-writer-wins guard in `MarkExternalHold` removed | the same 4 |
| `schedulerHoldWritable` → `true` (`unschedulable.go`) | none |
| that guard removed **and** `schedulerHoldWritable` → `true` | 6: `hold-gang-then-unschedulable`, `hold-nodeunknown-then-unschedulable`, `hold-pause-then-quota`, `hold-pause-then-unrouted`, `hold-quota-then-pause`, `hold-unschedulable-then-pause` |
| that guard removed **and** `gangHoldWritable` / `nodeUnknownHoldWritable` → `true` | 6: `hold-pause-then-quota`, `hold-pause-then-unrouted`, `hold-quota-then-pause`, `hold-unschedulable-then-gang`, `hold-unschedulable-then-nodeunknown`, `hold-unschedulable-then-pause` |
| `MarkExternalHold` never marking (the control) | 13: the 10 `hold-*` scenarios plus `quota-hold-parks-deadline`, `source-unrouted-midsurge`, `surge-force-clock` |

Three things to read off it.

**The surge guards are redundant for every row this build writes.**
Flipping `isSurgeOwnedStatus` alone moves no golden recorded from an
empty cluster, because `effectiveUpdateStrategy` already pins the
mechanism from `Operation.Strategy`, which every row this build writes
carries.
`isSurgeOwnedStatus` is load-bearing only for a surge row persisted
*without* a pinned strategy, a shape an older build wrote, which no walk
from an empty cluster produces. `surge-resumed-without-strategy-pin`
seeds exactly that row beside a desired strategy of RecreatePod, so the
guard is locked on its own: with `isSurgeOwnedStatus` alone flipped, that
scenario drains the source for a rebuild instead of finishing the surge.
With both disabled, `strategy-edit-midsurge` fails as well — it locks that
a strategy edit does not derail an attempt already in flight.

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
- `ScaleUpPodBatchSize` / `ScaleDownPodBatchSize` / `RepairBatchSize` —
  left nil, so every pass is unbounded and the batch-budget branches
  never run.
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

- The teardown pass itself. The driver stands in for the adapter's
  deadline check (see **The teardown deadline**), but not for the rest of
  that pass: the authoritative pod snapshot it hands the engine, the
  orphan PodGroup sweep, the headless Service delete, the ledger close
  and the strict-hold `TeardownBlocked` Warning. The arrows that fire on
  the deadline stay adapter-lane.
- Timestamps on aggregate conditions and on `LastFailure`, because the
  engine stamps both from the wall clock (see Follow-ups).

- The migration completion tail. The surge's availability gate reads the
  row's `PodCount` and `AvailablePodCount`, which only the status
  publication path writes (see deviation 11 above), so a replayed
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

A rejection injected by a scenario is delivered synchronously rather than
racing a concurrent writer, and a status conflict is answered by the
store's own copy of the adapter's retry rather than by a second writer
whose change the retry would read. A stale read is staged per pass and
lags by exactly one tick; a cache that lags by more, or that lags for some
kinds and not others, is not modelled.

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
