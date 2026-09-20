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
| `TestArrowCoverage` | every cited arrow exists; reports how much of the table is driven |
| `TestCellCitationsResolve` | every cited cell is a key of the expanded grid |

A scenario is loaded through `tests/rolloutspec`, so an event, variant,
arrow or cell the instance table does not declare fails the load. The
scenario vocabulary and the table cannot drift.

A scenario counts as coverage for the cells it drives: the cell names the
scenario in `coverage.replay`, `rolloutspec-check` resolves that name to a
file here, and `TestCellCitationsResolve` resolves the scenario's `cells:`
ids back to the grid.

## Golden provenance

Goldens were captured against the engine at `cf75eb72`
(`[OEP] 0020.2 rollout engine behavior lock`), which is the `main` commit
this branch is based on. The branch changes no engine code, so the
recorded traces are the behavior of `main` at that commit. They have been
re-recorded once since, when the driver's fidelity fixes (deadline
parking, pod-write values, the owner generation) changed what the driver
observed — not what the engine does.

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

`initial.spec` is the driver's mutable owner spec: every tick rebuilds the
plan and the target ControllerRevision from it through the production
`workload.BuildPlan` and `revision.EnsureControllerRevision`, so a
`spec.revision` event is an image rewrite and the new hash follows from
the engine's own hashing.

## Trace format

```
tick=3 note the pod template changes, so the rollout surges a replacement
tick=3 apply spec.revision image=registry.example.com/runtime:v2 now=t+0s
tick=3 gate consult strategy=SurgeThenDrain surge=0 unavail=0 verdict=allow
tick=3 row=0 write phase=Ready→Updating targetRevision=nil→rev#2 op=nil→op#2{...} precondition=ok
tick=3 event Normal RecreateUpdateStarted OMENative component=engine instance=0 surge to revision rev#2 (newOrdinal=1)
tick=3 pod create name=svc-a-engine-0-default-1 revision=rev#2 incarnation=1 ordinal=1
tick=3 hold cleared
tick=3 pass result=after:5s
```

Line kinds: `note`, `apply` (a scenario event staged), `row` (one buffered
status mutation, field by field, with its precondition verdict),
`retryblock`, `event` (a Kubernetes event), `pod` / `podgroup`
(create / patch / delete, including a rejected or failed one), the
callback lines (`gate consult`, `hold`, `warn`, `condition`, `migration`),
and `pass result=` closing every tick.

A row write after the `pass result=` line of a tick is the adapter's
deadline park/rearm step, which runs after the dispatcher on every
reconcile (and, when paused, once before it as well).

Normalization: operation ids to `op#N`, UIDs to `uid#N`, revision names and
hashes to `rev#N`, all in first-appearance order; instants to `t+<duration>`
from the scenario start, including timestamps embedded in event messages.
Pod names are already deterministic and are printed verbatim. Anything else
that varies between runs is a determinism defect, reported rather than
normalized away.

## Deviations from the OEP-0020.2 design details

The OEP's shapes are the starting point; these are the places the engine
or determinism forced a change, and they are the ones to review.

1. **`arrows:` is a list, separate from the scenario name.** The OEP wrote
   the arrow id in `scenario:`. A walk normally drives several arrows on
   the way to the one it is written for (the surge scenario drives three),
   and the arrow-coverage join needs all of them. A scenario name starting
   with `T-` is still checked against `transitions.yaml`; a free name — a
   hold that drives no transition — is not, and cites cells instead.
2. **Revisions render as `rev#N`, not `rev-a` / `rev-b`.** The driver mints
   real ControllerRevisions through the production hasher, so their names
   carry a content hash. Normalizing them keeps a golden from churning on
   an unrelated hashing change while still showing a revision swap.
3. **Row diffs name every changed field, and non-committing mutations are
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
7. **`clock.advance` is the one event outside the table's vocabulary.** It
   is harness control, not an observation. `timer.operationDeadline` is a
   table event and is implemented as a clock move onto the earliest
   in-flight deadline plus a slack.
8. **The driver assigns pod UIDs.** The fake client assigns none and the
   engine refuses to delete a pod without an observed UID. They are numbered
   in creation order and normalized to `uid#N`.
9. **Expectations are confirmed by the driver, not by an informer.** A pod
   create is observed as soon as the object exists; a delete is observed
   when the scenario's `pod.deleted` removes it.
10. **The deadline park/rearm step is modelled, the rest of the aggregate
    pass is not.** The adapter parks every in-flight deadline before the
    dispatcher when paused, and parks or rearms admission-held deadlines
    after it on every reconcile; the driver does both. The counters and
    conditions the same aggregate pass publishes are not computed.
11. **An event may carry only the arguments it declares.** The parser
    rejects anything else, so a mistyped argument fails the load instead
    of silently doing nothing, and `timer.operationDeadline` requires its
    slack rather than defaulting to one the harness invented.
12. **A row diff omits `ReadyPodCount`, `ScheduledPodCount` and
    `NodesOccupied`.** Those are materialized only on the status
    publication path, which runs outside `workload.Reconcile`, and the
    workload package's removable-field audit forbids reading them
    anywhere else. A trace cannot honestly report a field the run never
    writes.

## What is not modelled yet

The table events the driver can stage are the ones the scenarios here need:
`pod.scheduled`, `pod.containersReady`, `pod.ready`, `pod.servingOn/Off`,
`pod.terminating`, `pod.deleted`, `pod.phaseTerminal`, `pod.waiting`,
`pod.unschedulable`, `endpoint.rotationIn/Out`, `api.quotaDenied`,
`api.invalid`, `spec.revision`, `spec.replicasUp/Down`,
`spec.pauseTrue/pauseFreeze/unpause`, `spec.strategy`,
`timer.operationDeadline`, and `clock.advance`. A scenario naming any other declared event fails to load
with that as the reason, so the gap is always visible rather than silent.

Gang (multi-pod) Instances, migration and teardown are not driven yet;
the driver renders single-pod Instances only.

## Sensitivity (the recorded mutation check)

Run against this scenario set, on the goldens recorded above.

| mutation | result |
|---|---|
| `isSurgeUpdateStep` forced to `false` (`ops/update_status.go`) | `T-surgedrain-promote` fails |
| `isSurgeOwnedStatus` forced to `false` (`ops/update.go`) | no golden fails |
| `isSurgeOwnedStatus` **and** `effectiveUpdateStrategy`'s pin both disabled | `strategy-edit-midsurge` fails |

The middle row is not a coverage gap in the scenarios, it is a property of
the engine: `effectiveUpdateStrategy` already pins the mechanism from
`Operation.Strategy`, which every row this build writes carries, so the
two guards are redundant for any row the harness can produce.
`isSurgeOwnedStatus` is load-bearing only for a surge row persisted
*without* a pinned strategy — a shape an older build wrote — and the
scenario format deliberately cannot seed an in-flight operation. The third
row is the honest sensitivity claim: `strategy-edit-midsurge` locks that a
strategy edit does not derail an attempt already in flight, and fails when
the pinning is removed.

## Unlocked paths

What the goldens do NOT record, so a reader knows the lock's edges. Each
is a real surface of `workload.Reconcile` that this driver either does not
reach or does not observe.

Callbacks and deps left unwired:

- `FinalizeInstanceResources` — per-Instance resource teardown; only gang
  Instances have any, and gangs are not modelled.
- `Deps.RenderHook` — production wires it unconditionally to inject peer
  env vars. In-place eligibility diffs on env, so this is the most
  consequential omission on the list.
- `Deps.EnsureGangPodGroup` — the PodGroup-before-pods choke point for a
  gang surge.
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
- Timestamps on aggregate conditions and on `LastFailure`, because the
  engine stamps both from the wall clock (see Follow-ups).

Reads, which are never traced at all: EndpointSlices, the headless
Service, ControllerRevisions, and Nodes. No Node objects exist in the fake
cluster, so the force-delete escalation always takes its read-error branch
rather than the unreachable-node branch — a scenario that needs the latter
has to seed Nodes first.

Two apiserver behaviors are absent: there is no conflict retry (the fake
client raises no 409, so the adapter's `RetryOnConflict` loop is never
entered), and a rejection injected by a scenario is delivered
synchronously rather than racing a concurrent writer.

## Follow-ups

- `transitions.yaml` declares **138** transitions, not the 148 OEP-0020.2
  states. The exit criterion should be restated against the count the
  table actually carries.
- A clock seam for `podreadiness` (`readiness.go:160,245`) and
  `ops/restart.go:501`, both of which read `metav1.Now()` directly. Until
  they take the injected clock, the trace deliberately omits the values
  they stamp (the serving condition's transition time, `LastFailure.Time`).
- Modelling `Deps.RenderHook`, which production wires unconditionally and
  which injects peer env vars. In-place eligibility diffs on env, so a
  hook that changed env would change in-place behavior; the driver leaves
  it nil because a faithful stand-in needs the coordination package the
  workload layer deliberately does not import.
