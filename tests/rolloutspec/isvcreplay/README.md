# InferenceService pass: replay scenarios and golden traces

This directory is the behavior lock for the InferenceService controller's
OMENative pass: each scenario drives one pass per tick through the
controller's own stages and its golden records, byte for byte, what every
stage wrote. A diff means the pass behaves differently than it did when
the golden was captured.

The driver lives in
`pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/replay`;
this directory holds the scenarios, the goldens and the gates. The
instance machine has its own lane under `tests/rolloutspec/replay`; this
lane drives the four machines that pass decides in one tick: `run`,
`coordination`, `canary` and `revision`.

```
scenarios/<name>.yaml   one timeline; the file name is the scenario name
golden/<name>.trace     its recorded trace
```

## Running

```sh
go test ./tests/rolloutspec/isvcreplay/... -count=1
go test ./tests/rolloutspec/isvcreplay/... -run TestReplayMatchesGolden -update   # rewrite goldens
```

Four gates run here, under the same `make rollout-behavior-lock` target as
the instance lane:

| test | proves |
|---|---|
| `TestReplayMatchesGolden` | every scenario still produces its recorded trace |
| `TestNoOrphanGoldens` | every golden has its scenario |
| `TestReplayIsDeterministic` | ten replays of each scenario are byte-identical |
| `TestCellCitationsResolve` | every cited cell is a key of its machine's expanded grid; reports the count cited per machine |

A scenario is loaded through `tests/rolloutspec`, so an event, variant or
cell none of the four tables declares fails the load. The vocabulary is the
union of the four machines' events; a cell id carries its machine's prefix
(`run/Open/ctrl.staleRead[missesOpen]/before`). A scenario counts as
coverage for the cells it cites once the cell names it in
`coverage.replay`, which `rolloutspec-check` resolves to a file here and
which moves the machine's coverage floor.

## What one tick runs

A tick applies its events, lets every replica publish, and then runs one
pass in the controller's order:

1. the pass copy of the service from the cache, and the canary base taken
   from it;
2. the run layer, with the referenced policies read from the cache and the
   replica revision pairs read live;
3. the canary bind on an opened run, and the immediate flush when the run
   boundary moved (a pass whose canary record was stale ends here);
4. the replica projection through the production projector, carrying the
   step partition or the plan-gate hold;
5. the canary dispatch per canary group, with pods and replicas read live;
6. the coordination pass, with pods and replicas read from the cache;
7. the aggregate of the replica status onto the service;
8. the final flush, then the removal of the verbs the pass applied.

The flush is the controller's own `FlushStatusThenConsume`; the stages
are the production packages. What the driver supplies is the apiserver,
the replica controller's publication, the clock and the trace.

## The replica controller's stand-in

The replica controller does not run here. Each Component is a member the
scenario drives: a revision pair, dense per-Instance rows and pods, all
named by the image whose template mints the revision (`current` is the
Component's own image). Before every pass each member publishes its
status the way the replica controller publishes it: the pair, the counters
derived from the rows and their pods, the rows, a Ready condition and the
observed generation. The machines' events are the only way to move a
member: `spec.revision[new]` bumps the target, `member.surgePodCreated`,
`member.readyOnTarget`, `member.oldPodsGone` and `member.converged` walk a
roll, `ir.converged` and `ir.straggling` settle a replica in one step,
`capacity.reached` stands the Instances above the projected partition on
the canary revision, and so on. An event the engines produce inside the
pass (`canary.parked`, `coord.staged`, `seq.nextTurn`, `flush.staleOverwrite`,
the `write.*` and `read.*` events of the revision machine) cannot be
staged; the loader refuses it with the reason, and a scenario cites the
cell instead.

The projection catches up one pass behind a template edit, as it does in
the controller: the run layer runs before the projector, so the pass that
first sees a new generation finds the replica's parent-generation stamp
behind it and waits, and the next pass opens the run.

## The lagging cache

`ctrl.staleRead`, `pass.staleRecord` and `read.staleCopy` stage a pass
whose cached copy of the service is the one the previous pass found: it
misses what that pass flushed. Only the service lags; its replicas, pods,
Services and policies are read current, because that is what the four
tables' stale-read events describe (a copy of the service that predates a
flush). A stale pass still writes live, and the flush's live read is
current, so the preserve rules decide what survives. The variant is a
claim about what the stale copy misses, checked against the live object
before the pass runs; a claim the cluster does not bear out fails the run.
`ctrl.staleRead[missesRepin]` lags the status alone and shows the live
annotations: the verb's removal is a metadata write of its own, which the
cache delivers apart from the status flush.

## Trace format

```
tick=3 note the replica pair is fresh and diverged, so the run opens
tick=3 apply ctrl.resync now=t+0s
tick=3 publish component=engine current=svc-a-engine-f332566b update=svc-a-engine-46e64ada observedGeneration=2 replicas=2 ready=2 serving=2 updated=0 updatedReady=0 rows=[0:Ready@f332566b 1:Ready@f332566b]
tick=3 pass cache=live generation=2
tick=3 event Normal RolloutRunOpened run svc-a-e2931de6a354 opened (rp1:8aa97d86f011)
tick=3 run outcome=opened stateChanged=true requeueAfter=10s policies=disabled
tick=3 run active id=svc-a-e2931de6a354 openedAt=t+0s pinnedAt=t+0s targets=[engine=46e64ada/stable=f332566b] digests=[Inline:rp1:cca45ec1fb0f]
tick=3 run conditions RolloutPlanReady=True(Pinned) RolloutPlanDrift=False(InSync)
tick=3 flush which=boundary result=written statusWrites=1 run=desired groups=0:unchanged
tick=3 coordination group=0 before={phase=Surging composite=Surging current=nil previous=nil transition=t+0s anchor=nil} after={phase=Surging ...}
tick=3 revisions component=engine ready=f332566b latest=f332566b previous=nil phase=nil
tick=3 flush which=final result=short-circuit
tick=3 pass result=after:10s
```

Line kinds, in the order a pass emits them: `note`, `apply` (a staged
event with the clock reading), `publish` (a member's status as the
replica controller would publish it), `pass` (which cache the pass read),
`run` (the run layer's outcome, the pinned run with its id, clocks,
targets and digests, the last run, the two plan conditions, the policies
observed), `bind` (the record the bind left on a unit), `flush` (see
below), `dispatch` (a unit's state, phase and record, with the executor's
requeue and the verbs it applied), `traffic` (a member's targets),
`coordination` (a group's entry before and after the merge: phase,
composite, current and previous Component, transition time and ratio
anchor; then the condition), `revisions` (a Component's three rolled-out
revision fields and projected phase), the engine writes (`ir patch`,
`isvc patch`, `service create`, `service delete`, `isvc status-conflict`,
`isvc status-failed`) with the stage they landed in, `event` (a Kubernetes
event), and `pass result=` closing every tick.

The `flush` line says what the controller's flush did, derived from the
cached copy, the live object before and after:

- `result=` is `short-circuit` (desired equals the cached copy, nothing
  written), `written`, `no-change` (written with no status difference),
  `failed` (a non-conflict error) or `conflict`;
- `statusWrites=` counts the logical status writes, retries folded in;
- `run=` is `same`, `desired` (the ordinary write), `desired(overwrote-live)`
  (the pass's pair written over a live pair its copy had not seen),
  `live-kept(newer-clock)` (the preserve rule kept the live pair) or
  `live-kept`;
- `groups=` names, per group, `unchanged`, `added`, `updated{...}` (the
  ordinary write of a current copy) or `overwrote-newer-live{...}` (the
  pass's entry written over a live entry its copy had not seen), with the
  phase, composite, current Component, transition time and anchor that
  moved;
- `canary=kept-live` when the flush kept the live canary-owned fields over
  the pass's and marked the pass stale;
- `consume=` lists the verbs removed after the write.

An annotation removal is an `isvc patch ... stage=<stage>` line, so the
order of a verb's consumption relative to the flush is read off the stage:
`stage=run` is the run layer's own removal before the boundary flush,
`stage=flush:final` the controller's removal after the final write.

Identities print as the engines see them: run ids, canary target ids,
revision names and digests are deterministic functions of the fixture and
the injected clock. Instants render as `t+<duration>` from the scenario
start, including timestamps embedded in event messages. Nothing else is
normalized; a value that differs between runs is a determinism defect.

## Scenario format

```yaml
scenario: stale-read-misses-open      # the file name
cells:
  - run/Open/ctrl.staleRead[missesOpen]/before
intentional: >-                       # why this golden reads the way it does
  ...
initial:
  service:
    components:
      engine: {replicas: 2, image: registry.example.com/runtime:v1}
    groups:                           # spec.rollout.groups, in the API's spelling
      - components: [engine]
        blueGreen: {}
    annotations: {}
  policies:                           # RolloutPolicy objects
    - name: canary-policy
      spec: {canary: {steps: [{capacity: "50%", traffic: 10, pause: {}}, {capacity: "100%", traffic: 100}]}}
  config: {requeue: 10s, parkedRequeue: 1m, defaultReadyTimeout: 10m, rolloutPolicyEnabled: true}
  replicas:                           # a member's initial publication; absent means Ready on the image
    engine: {current: current, rows: [{index: 0, phase: Ready}], pods: [{index: 0, ready: true}]}
timeline:
  - tick: 1
    note: a claim about the tick; compared as a trace line
    events:
      - clock.advance: 1m
      - ctrl.staleRead[missesOpen]
      - spec.revision[new]: {component: engine, image: registry.example.com/runtime:v2}
```

An event is a bare key, a one-key mapping or a duration scalar. Each event
declares the arguments it reads and the parser rejects anything else. The
config is explicit: an unconfigured cadence asks for the rate-limited
requeue, an unconfigured timeout never escalates, and references resolve
only with `rolloutPolicyEnabled`.

## Timer events

`timer.soak` and `timer.readyTimeout` resolve their deadline out of the
state the engines persisted (a group's `lastTransitionTime` plus its soak;
a unit's `capacityWaitSince` plus the ready timeout) and move the clock
onto it plus the `slack` the scenario names. A deadline already behind
the clock records `already-elapsed` and moves nothing; nothing to resolve
fails the run.

## Changing a golden

A golden diff is either the point of the change or a regression, and the
review cannot tell which from the diff alone. A commit that rewrites a
golden must also either change the table cell the behavior belongs to, or
set `intentional:` in the scenario naming the decision.
`hack/check_lock_rewrite.py` refuses a rewritten golden here that names
no eval rerun, as it does for the instance lane.

## What the goldens record that the cells do not say

The DECIDE cells these scenarios cite describe the engine from the code;
the traces are the engine. Two differ:

- `run/Open/ctrl.staleRead[missesRepin]` says the per-unit canary record
  can lose the repin's pre-step hold because the flush re-arms it on the
  alias alone. `stale-read-misses-repin` records the flush keeping the
  live record through the canary base (`canary=kept-live`) and the hold
  still on the per-unit record on the next pass; the drift reads True for
  the one stale pass, as the cell says.
- `coordination/Surging/flush.staleOverwrite` describes the final flush
  copying the pass's entry over a newer live one. `flush-overwrites-group-fields`
  records that the boundary flush of a run opened on the stale copy
  already does it, one stage earlier, and the final flush then writes the
  restamped entry.

## What is not modelled

Stageable events are the ones the scenarios here need; a scenario naming
any other declared event fails to load with that as the reason. Not
modelled yet: the analysis sampler (`analysis.*`), the crash and restart
readings (`pod.canaryCrashed`, `pod.canaryRestarted`, `pod.canaryDeleted`),
the timed soak and drain window timers, a replacement for the released
floor (`pod.replacementReady`), a staged member under a user partition
(`member.staged`), a persisted record from before a field existed
(`record.legacy`), a policy body edit or return (`policy.changed`,
`policy.resolved`), a missing replica (`ir.missing`), a replica read error,
a hash collision and a rotated replica scope, and
a Component leaving OMENative (`write.modeSwitch`): the mode-switch
cleanup runs inside the controller's `cleanupReplacedBackends`, which the
driver does not enter. The IR counters are a stand-in: Ready, Serving,
Available and Updated count rows whose pods meet the single-pod threshold,
so a multi-pod Instance is not modelled.
