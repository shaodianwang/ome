# OEP-0020.2: Rollout engine — behavior lock before restructure

Tl;dr: `pkg/controller/v1beta1/workload` is organized by verb (create, update,
restart, migrate, delete) while the behavior we decided in OEP-0020.1 is defined
per state × event. Every cross-cutting decision therefore lands as five or six
edits, and the misses are found in review or in KIND. Before we move any of that
code we lock what it does today with a deterministic replay harness in the unit
lane, close test coverage on every decided cell and freeze the instance machine,
and make the KIND lane's red mean something. Those three gates are this OEP.
The restructure they unblock — row ownership and the event columns given single
owners, package layout unchanged, no per-step files — is OEP-0020.3.

## Summary

Three gates, and nothing moves until all three hold.

1. **Replay harness** (Part 1). Drives `workload.Reconcile` over scripted event
   timelines with the fake client and mutation store the unit tests already use,
   and records every status write, event and requeue as a golden trace per
   scenario, one scenario per transition arrow of the instance table at minimum.
2. **Coverage and freeze** (Part 2). Coverage of the 356 decided cells closed
   from 172 to 356 and `frozen: true` set on the instance machine, so the existing
   CI gate turns a decided cell without a test into a build failure.
3. **A trustworthy KIND lane** (Part 3). A quarantine rule for specs that go red
   on `main` without a code change, with owners and expiries, and a fix list for
   the ones that did this week.

## Motivation

### Terminology

- **Row**: one Instance's persisted status in the InferenceReplica —
  `Phase + Operation.{Type, Step}` is its state in the OEP-0020.1 table; every
  pass reads and rewrites it.
- **Verb pass**: one of the five loops in `ops/` — create, update (surge, gang,
  in-place, recreate), restart, migrate, delete — that advances rows owned by
  that operation type. `Plan` selects them in a fixed order and `Execute` runs
  them with early returns between (`workload/reconcile.go`, `plan_decision.go`).
- **Event column**: a table event that means the same thing in most rows and is
  not tied to a verb — `api.quotaDenied`, `pod.unschedulable`,
  `gang.podGroup[Terminating]`, `pod.phaseTerminal[Unknown]`,
  `pod.servingOff[source]`, `spec.pauseTrue`, the timers. Today each verb pass
  handles the columns it happens to meet.
- **Hold**: an externally owned wait recorded on `Operation.Waiting` — quota,
  scheduler, PodGroup, node unknown, source unrouted, paused — that parks the
  operation deadline. Six holds share one string slot with a precedence rule
  bolted on (`types/externalhold.go`).
- **Behavior lock**: a test that asserts the engine does *exactly* what it did at
  a named commit, decided or not. Distinct from a correctness guard, which asserts
  the engine does what the table says. A refactor needs the first; the table
  process produces the second.
- **Golden trace**: the ordered list of row diffs, events and pass results a
  scenario produces, stored in the repo and compared byte-for-byte after
  normalizing ids and timestamps.

### The cost we are paying now

Each row below is one owner decision from the 2026-09-17 walkthrough and the
number of places it had to be implemented. The last column is what the review
or CI found missing — always the pass nobody thought of.

| decision | passes or files changed | the miss |
|---|---|---|
| quota hold (#1501) | create at six sites, in-place, `deadline.go`, `escalation.go` | pacing absent at four create sites; escalation skip missing |
| scheduler hold (#1503) | `escalation.go`, `deadline.go`, `unschedulable.go`, migrate rows | Migrate rows skipped before the hold ran, so migrations never parked |
| PodGroup hold (#1506) | gang pass, `escalation.go`, `deadline.go`, `externalhold.go` | Delete rows escalated; token handover lost a pass |
| one promote bar (#1537) | create, restart, recreate, in-place, surge, gang | the IR watch never woke on PodReady, found only in KIND |
| terminal-pod recycle (#1494, lifted in #1503) | create and restart had it; surge, gang, in-place, migrate did not | lifted later with a `statusIdx, podIdx` split |
| pause at step boundaries (#1542) | `plan_decision.go`, `pause.go`, create, surge, `externalhold.go`, escalation | superseded creates restarted under pause; Paused hid SourceUnrouted |
| requeue cadence (#1547) | five literal sites | two sites disagreed on whether an explicit wait beats the backoff |

The cause is structural, not carelessness: with V verb passes and E event
columns there are V × E cells to get right, and nothing owns a column.

### What guards the package today

Measured on `main` at 2026-09-19.

| guard | proves | does not prove |
|---|---|---|
| unit tests in `workload/...` (55k lines, tagged `// Cells:`) | the cells they cite, in seconds | 184 of 356 decided cells have no test; the 986 `current` cells are untested by construction |
| `TestAllMachinesWellFormed` + the `rolloutspec-check` CI step | the table is complete over its vocabulary and every cited test and cell id exists | nothing about behavior |
| envtest suites | webhooks, defaulting, status contracts | nothing about the engine |
| KIND suites (about 80 spec files, 20 hard-gating groups) | end-to-end behavior under a real kubelet | trustworthiness: `main` went red on builds 4422 and 4425 with no engine change; the revert spec failed on #1547 for a pre-existing race |
| perm corpus (968 cases) | that the cases parse | the runner is not a hard gate |

Decided behavior is about half locked, undecided behavior is not locked at all,
and the suite that exercises everything is the one we cannot trust to be red for
a reason. Moving the engine under those conditions is how a refactor ships a
regression nobody can attribute.

### Goals

1. A behavior lock that runs in the unit lane in under 60 seconds and fails on
   any change in what the engine writes, for every transition arrow in the
   instance table at least once.
2. Every decided cell of the instance machine cited by at least one Go test, and
   the machine frozen so the gate keeps it that way.
3. A KIND lane where a red build on `main` is treated as a defect with an owner,
   not retried.
4. Gates that OEP-0020.3 can state its exit in terms of: "golden-identical" and
   "the frozen machine still passes" are the two sentences every restructure PR
   will use.

### Non-Goals

- Deciding the 986 `current` cells. The lock records them; the table process
  decides them later, one family at a time.
- Per-step files (`step_surge.go` and the like). The owner rejected that shape:
  an operation is a real thing in the row and the verb passes are the right home
  for advancing it.
- Changing the package layout. `types`, `query`, `drain`, `podreadiness`,
  `revision`, `podgroup`, `gang` and `audit` are single-purpose leaf libraries
  and stay as they are.
- A simulator with its own oracle. The harness below is the simulator's driver
  and recorder; the oracle stays the table and arrives with the generator, later.
- Fixing KIND flakes by deleting specs.

## Proposal

### Part 1: the replay harness

**What must be true.**

- Given the same scenario, two runs produce the same trace. No wall clock, no
  goroutines, no map-order dependence in what is recorded.
- A scenario can express every event in the instance table's vocabulary: pod
  phase and condition changes, pod deletion, EndpointSlice membership, PodGroup
  phase, apiserver rejections by class, spec edits (revision, replicas,
  strategy, pause, migrate request, reset), config changes, and clock advances.
- The trace records everything the engine writes or asks for: each row diff in
  commit order with its precondition outcome, each event, each pass result
  (requeue, error), each pod or PodGroup create, patch or delete.
- Capturing goldens from a named commit and diffing against them is a `go test`
  run, and updating a golden is an explicit, reviewable file change.

**Mechanism.** The pieces exist; the harness composes them.

- Driver: `workload.Reconcile` called once per tick with a `ReconcileInput`
  built by `minimalInput` and friends (`workload/reconcile_test.go`), the
  controller-runtime fake client with an interceptor for injected rejections,
  `installTestAtomicMutationStore` for rows, `SnapshotWithPodsForTest` for pods,
  and the already-injectable clock (`input.Now()`).
- Scenario: a YAML file with an initial state (plan, rows, pods, revisions) and a
  timeline of ticks; each tick applies zero or more events, then runs one
  reconcile. Event names are the table's event ids with their variants, so a
  scenario reads as a walk through the table.
- Trace: one line per recorded effect, normalized (operation ids, timestamps and
  UIDs replaced by stable tokens; ordering by commit sequence).
- Storage: `tests/rolloutspec/replay/scenarios/*.yaml` and
  `.../golden/*.trace`, next to the tables they derive from, so the CI gate that
  already runs `tests/rolloutspec` runs them.

**Scenario sources**, in the order they are built:

1. Hand-written, one per transition arrow of the instance table (148 arrows
   today), each named by the arrow id and citing its cells. These are the
   minimum for exit.
2. Translated perm cases: the 968 generated cases under
   `hack/omenative-testing/perm/isvcs` describe an initial shape and a fault
   phase; a small translator emits the engine-level scenario. Their `expect=`
   verdicts are not asserted at first — the golden is the assertion.
3. Generated table walks (state, event, window), when the table process reaches
   the simulator projection in OEP-0020.1 Part 4. Not required for this OEP.

**Exit criteria for Part 1.**

| criterion | measure |
|---|---|
| every transition arrow driven | 148 of 148, checked by a test that joins scenario headers with `transitions.yaml` |
| goldens captured from a named commit | the commit sha recorded in `replay/README.md` |
| runtime | the replay package under 60 seconds in the unit lane |
| sensitivity | flipping any one ownership predicate or hold rule fails at least one golden (a mutation check run once, recorded) |
| determinism | ten consecutive runs, identical traces |

### Part 2: close coverage and freeze

184 decided cells have no test. By family: surge 47, in-place and recreate 36,
migrate 30, noop rows 24, gang 18, restart 14, create 7, failed 7, delete 1.
Each gets either a tag on an existing test that truly drives it or a new test in
the same style as the `// Cells:` convention. When the count reaches zero the
instance machine sets `frozen: true`; from then on the existing gate fails the
build for any decided cell without coverage, which is the durable form of this
goal.

The replay scenarios from Part 1 count as coverage where they drive the cell,
so Parts 1 and 2 share most of their work.

### Part 3: a KIND lane whose red means something

Rule: a spec that fails on `main` without an engine change, or that passes on
retry without a change, is a defect with an owner. It is moved to a nightly
soft-fail group until fixed, and the fix PR moves it back. A retry is never the
fix.

Known this week, with the state of each:

| spec | cause | state |
|---|---|---|
| `cancel_midrollout_kind_test.go` revert reopen | `UpdatedReplicas` counts a mid-surge row as running the update revision, so the IR reports converged while a roll is in flight; a later promote opens a new run | root cause known; fix is an owner call between correcting the counter and a test-side stability gate |
| `auto_migrate*_kind_test.go` | timing of the relocation directive against the promote | passed after the PodReady watch fix in #1537; watch |
| Canary lifecycle and RatioBalanced groups | failed on `main` builds 4422 and 4425 | TBD, needs triage |

Exit: ten consecutive green `main` builds with no retries.

### Part 4: what the lock unblocks

The restructure of the engine's root and `ops/` layers — one row-ownership
function, the event columns given their own packages, one status writer — is
OEP-0020.3. It does not start until Parts 1–3 have met their exit criteria, and
every one of its steps is defined as "golden-identical, or a table cell changes
in the same PR". This OEP owns the gates; 0020.3 owns the moves.

## Design Details

### Scenario format

```yaml
scenario: T-surge-drain-promote          # arrow or free name; cells listed below
cells:
  - instance/Update/SurgeDrain/pod.deleted[source]/boundary@T-surgedrain-promote
initial:
  plan: {replicas: 1, strategy: SurgeThenDrain, minReadySeconds: 0}
  rows:
    - {index: 0, phase: Updating, operation: {type: Update, step: SurgeDrain, target: rev-b, surgeIndex: 1}, runningRevision: rev-a}
  pods:
    - {name: e-0, index: 0, revision: rev-a, ready: true, serving: false, routed: false}
    - {name: e-1, index: 1, revision: rev-b, ready: true, serving: true, routed: true}
timeline:
  - tick: 1                                 # reconcile with the initial state
  - tick: 2
    events: [{pod.deleted: {name: e-0}}]    # the drain converged and the delete landed
  - tick: 3
    events: [{clock.advance: 5s}]
```

The event vocabulary is the instance table's `events.yaml`; a scenario naming an
event the table does not declare fails to load, so the two cannot drift.

### Trace format

```
tick=1 pass result=requeue:5s
tick=2 row=0 write phase=Updating→Ready op=Update/SurgeDrain→nil runningRevision=rev-a→rev-b   precondition=ok
tick=2 event Normal SurgePromoted instance=0 revision=rev-b
tick=2 pod delete name=e-0 grace=default
tick=3 pass result=none
```

Normalization replaces operation ids, UIDs and timestamps with `op#1`, `uid#1`,
`t+5s`. Anything else that varies between runs is a determinism bug in the
harness or the engine and is fixed, not normalized away.

### What the harness does not model

The kubelet, the scheduler and the endpoint controller are not simulated; the
scenario states their outcomes as events. That is deliberate: the lock is about
what the engine writes given what it observed, and the KIND lane owns the
question of whether those observations arrive in that order in a real cluster.

## Failure modes and mitigations

- **Goldens rot into noise.** If every PR regenerates goldens, the lock proves
  nothing. Mitigation: a golden change requires a table cell change in the same
  PR, or an explicit `intentional:` note in the scenario naming the decision;
  the CI step diffs goldens against the PR base and fails on an unexplained
  change. Mitigation, not full fix — reviewers still have to read the diff.
- **The harness is deterministic but the engine is not.** Map iteration in pod
  ordering was one real case this week (the unschedulable hold picked pods by
  informer order). Mitigation: the ten-run determinism check in Part 1's exit
  criteria; each nondeterminism found is fixed in the engine.
- **Coverage closure by tagging tests that do not really drive the cell.** The
  checker validates names, not truth. Mitigation: the tag convention already
  requires the tag to be the last doc line of the test that drives the cell, and
  the reviews this week rejected several tags on exactly that ground; the replay
  scenarios make the claim mechanical because the trace shows the row.
- **The KIND quarantine hides regressions.** A quarantined spec that fails
  nightly for a new reason is invisible in the PR lane. Mitigation: quarantine
  has an owner and an expiry (two weeks), after which the spec returns to the
  gating group fixed or the group goes soft-fail with a visible reason.
- **The restructure changes behavior while claiming golden-identity.** The
  goldens cover every arrow, not every path. Mitigation: each move PR also runs
  the full KIND lane; and step 1 (ownership) is the one most likely to surface
  disagreements, which is why it goes first and records rather than fixes them.

## Anticipated objections

Q. Is the replay harness not the simulator OEP-0020.1 deferred?
A. It is the simulator's driver and recorder without its oracle. The oracle is
the table plus the generator that walks it; that work is unchanged and comes
later. What we need now is the ability to say "nothing changed", which needs no
oracle.

Q. Why not lean on KIND, which already exercises everything?
A. Because a red KIND build on `main` was retried twice this week and once it was
a real, pre-existing race. Until Part 3 holds, KIND cannot distinguish a refactor
regression from timing, and it takes 30 minutes to say either.

Q. The event-column packages will grow their own if/else per row.
A. Some, yes: a hold is not writable on a Delete row, a pod-level hold outranks
a gang hold. The difference is that those rules live in one table each, read by
every pass, instead of being restated at every site. The count goes from V × E
to V + E.

Q. This is a large change for a package that mostly works.
A. It mostly works because of the reviews and the KIND lane, both of which
found real defects in every one of the seven PRs above. The point of the lock is
that the restructure can be done as five golden-identical PRs rather than one
large one, and can be stopped after any of them with the package in a better
state than before.

## Test Plan

- Part 1: the replay package under `tests/rolloutspec/replay` runs in the unit
  lane; the arrow-coverage test, the determinism test and the golden diff are
  gates; the mutation check is a documented one-time procedure.
- Part 2: the existing `rolloutspec-check` step plus `frozen: true`.
- Part 3: the Buildkite generator gains a soft-fail nightly group; moving a spec
  is a one-line change with the owner and expiry in the group definition.
## Graduation Criteria

### Alpha

- Part 1 exit criteria met; goldens captured from a named `main` commit.

### Beta

- Part 2 complete: zero decided cells without coverage, machine frozen.
- Part 3 exit met: ten consecutive green `main` builds.

### Stable

- OEP-0020.3 step 1 has merged golden-identical, i.e. the lock has caught or
  cleared a real refactor once; the golden-diff CI step has rejected at least one
  unexplained change.

## Drawbacks

- The goldens are a second source of truth beside the tables until the
  generator projection unifies them. Until then a decided cell and a golden can
  disagree, and the reconciliation is manual.
- Coverage closure on 184 cells is a week of test-writing that produces no
  user-visible change.

## Alternatives

1. **Restructure first, rely on KIND.** Rejected: KIND is slow and currently
   untrustworthy for exactly the question a refactor asks.
2. **Per-step files.** Rejected by the owner; it splits operations that are
   real units and does not give the event columns an owner.
3. **Consolidate test files only.** Rejected: it addresses file size, not the
   V × E cost.
4. **Decide all 986 `current` cells first, then refactor against the table.**
   Not rejected, but not a prerequisite: the lock records current behavior
   without deciding it, and the table process continues in parallel.

## Open Questions

- Owner and fix choice for the revert reopen (counter versus test-side gate).
- Whether translated perm cases should ever assert their `expect=` verdict in
  the replay lane, or stay golden-only.
- The scenario count target beyond one per arrow; the mutation check will say
  whether one per arrow is enough sensitivity.
- Where quarantined KIND specs are listed so the expiry is visible.

Next action: build the replay driver and record goldens for the first ten
arrows, then bring the trace format here for review before the remaining 138.
