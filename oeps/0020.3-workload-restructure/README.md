# OEP-0020.3: Workload engine restructure — row ownership and event columns

Tl;dr: `pkg/controller/v1beta1/workload` is organized by verb and behaves per
state × event. The verbs are the right home for advancing an operation; they are
the wrong home for everything that is not a verb — holds, evidence, pause,
recycle, the promote bar — which was restated in every pass that happened to
meet it. This OEP gives those two unowned things exactly one owner each: an
`Owner(row)` function that answers "who drives this row this pass" once, and two
packages, `holds/` and `evidence/`, that own the table's event columns. Row
writes are confined to a `status/` package and the two callers that still author
their own. The package layout, the leaf libraries and the five verb passes stay.
A sixth step closes the test layout the first five leave behind: one production
file, one test file, checked rather than asserted. Four of the six steps are
golden-identical; two re-record, and each of those states which lines move and
why. A seventh step, after the six landed, thins the root to orchestration and
planning and gives escalation its own package; it is golden-identical.

The design is final and the code is in review on `refactor/oep-0020-3-restructure`
— 50 commits, one PR for all six steps. Every normative statement below is a
statement about that branch.

## Summary

Six moves, in order, each measured against the OEP-0020.2 lock. Four are
**golden-identical** and two are **re-recording** — the distinction is what kind
of diff a reviewer should expect, and it is stated per step in **The six steps,
as built**.

1. `types/owner.go` (**golden-identical**): one function replaces the ten
   predicates that decided whether a pass may act on a row, `ClaimOf` answers
   the second question beside it — which verb the row's operation still belongs
   to, whatever the phase — and the ownership table carries an Interruptible
   column.
2. `holds/` (**re-recording**): the six external holds, one file each, with one
   precedence table, evaluated once per row in one hold pass at the top of the
   reconcile. The five per-authority writability rules and the `supersedes` pair
   are deleted. Rules and call sites moved together, so tokens land at a
   different pass than they did — a timing diff, re-recorded in the same branch.
3. `evidence/` (**golden-identical**): stuck pod, readiness limbo, gate not
   folded, crash-loop wedge, terminating pod and the node under it, template
   diff, rejection class — pure reads over the observation, consumed by
   escalation and the operations.
4. `status/` (**re-recording**): every row stamp behind one identity
   precondition, decision fields kept apart from evidence fields; `LastFailure`
   is evidence again and an `Announced` record on the row carries every
   once-only warning and event, including the two gang warnings whose dedup was
   process-global.
5. `ops/` trimmed to advancing its own step (**golden-identical**); the
   order-dependent early returns in `executeActions` replaced by per-owner row
   lists.
6. Test consolidation (**golden-identical**): one production file, one test
   file, across the whole tree, plus a layout gate in the lock. No production
   code moves.
7. Root thinning (**golden-identical**): the reconcile root keeps orchestration
   and planning only — `reconcile.go`, the three pass arms, `plan.go`,
   `plan_decision.go`, `snapshot.go`, `observation.go` — and the end-of-pass
   repair moves into `escalation/`. The aggregate counters join `status/`, the
   operator reset joins `ops/`, the headless Service gets a `service/` leaf,
   and the `workload.X` forwarders are deleted in favour of the one `types`
   spelling.

## Motivation

### Terminology

- **Row**: one Instance's persisted status; `Phase + Operation.{Type, Step}` is
  its state in the OEP-0020.1 table.
- **Verb pass**: one of create, update (surge, gang, in-place, recreate),
  restart, migrate, delete in `ops/`; each loops its rows and advances their
  step.
- **Owner** of a row: the one verb pass, or escalation, or nobody, that may write
  the row's phase and operation this pass.
- **Event column**: a table event that is not a verb and means the same thing in
  most states — `api.quotaDenied`, `pod.unschedulable`,
  `gang.podGroup[Terminating]`, `pod.phaseTerminal[Unknown]`,
  `pod.servingOff[source]`, `spec.pauseTrue`, the timers.
- **Hold**: an externally owned wait recorded on `Operation.Waiting` that parks
  the operation deadline. Six exist: quota, scheduler, PodGroup, node unknown,
  source unrouted, paused.
- **Hold pass**: `holds.Run`, run once per row from `runHoldPass` at the top of
  the reconcile and before the verb passes — releasing first, then recording.
- **Window**: OEP-0020.1's `before` / `boundary` / `after` — where in a step's
  execution an event lands. A hold recorded in the `before` window of a step the
  owner is already executing does not, on its own, stop that step.
- **Interruptible**: a column of the ownership table. Per (state, event) it says
  whether the event takes the row from its owner immediately or is honored at
  the owner's next step boundary. Recording a token and acting on it are
  different things, and this column is what separates them.
- **Announced**: a per-row record of once-only messages, so a warning fires once
  per episode per row rather than once per process.
- **Episode**: the key an `Announced` marker is scoped to, by the message's
  scope. A *standing* message — about the Instance as built — keys on the
  incarnation, so an attempt opening or concluding does not re-deliver it. An
  *attempt-scoped* message keys on the operation in flight, or on the
  incarnation when the row carries none.
- **Evidence**: a classification of observed pods that escalation and the
  operations read. Evidence never selects a disposition branch on its own; the
  deadline disposition does, and relocation keeps first try.
- **Golden-identical**: the OEP-0020.2 replay traces for every scenario are
  byte-identical before and after.

### What the lock gives this OEP

OEP-0020.2 merged. Its three gates are the measuring instruments every step here
is stated against.

| guard | what it is | what a step of this OEP gets from it |
|---|---|---|
| replay goldens | 118 scenarios and 118 `.trace` files under `tests/rolloutspec/replay/`, one per workload-lane arrow of the instance table (118 of the table's 138 arrows), about 11 s in the unit lane, byte-identical over ten consecutive runs. 119 on this branch: step 4 adds `gang-advisory-warnings` | the definition of "nothing changed" for a move |
| adapter lane | the other 20 arrows carry `lane: adapter` in `tests/rolloutspec/machines/instance/transitions.yaml` and are each named by a Go test in `pkg/controller/v1beta1/inferencereplica` through an `// Arrows:` comment, resolved in both directions by `rolloutspec-check` | the boundary: a move that shifts work across the engine/adapter line shows up as an arrow whose lane must change |
| frozen machine | `frozen: true` in `tests/rolloutspec/machines/instance/machine.yaml`; `rolloutspec-check` reports all 8071 decided grid cells covered, so a decided cell losing its test is a build failure | a step cannot delete a test without deleting or re-tagging the cell it cited |
| CI | the "Rollout Behavior Lock" step runs `make rollout-behavior-lock MACHINE=instance` — the checkers, the replay goldens, and the engine packages the decided-cell tests live in | one named red for a golden diff or an uncovered cell |
| KIND lane | the quarantine rule with owner and expiry in `KIND_QUARANTINE` (`.buildkite/generate_pipeline.py`); an expired entry fails pipeline generation | a red KIND build during the sequence is a defect with an owner, not a retry |

**The rule per step**: a step is golden-identical, or it rewrites goldens and the
instance-machine cells they belong to with the scenario's `intentional:` note.
There is no third option. Steps 1, 3, 5 and 6 take the first branch; steps 2 and
4 take the second.

### What the mutation check said about the duplication

The lock ran a mutation check once and recorded it in
`tests/rolloutspec/replay/README.md`. Two of its findings are the direct design
input for this OEP, because they are measurements of the duplication, not of a
gap in the scenarios.

| finding | what it measures |
|---|---|
| flipping `isSurgeOwnedStatus` alone fails no golden; disabling it **together with** `effectiveUpdateStrategy`'s pin fails `strategy-edit-midsurge` | two independent guards decided the same thing for every row the engine writes. One was redundant, and no test could say which |
| flipping `schedulerHoldWritable`, `gangHoldWritable` or `nodeUnknownHoldWritable` alone fails no golden; each paired with removing the first-writer-wins guard in `MarkExternalHold` fails 6 | three per-authority rules refused exactly what the shared guard already refused. They were not the *same* rule — each also refused a `Delete` and each named the operator pause as its one exception — but no row the harness can build separates them |

That is what `Owner(row)` and one hold precedence table remove: not code, but the
condition where two rules answer one question and nothing says which is
authoritative.

- Step 1 deletes `isSurgeOwnedStatus` and keeps the strategy pin. `Owner()`
  answers the ownership half; the pin answers the mechanism half.
  `strategy-edit-midsurge` is unchanged, and the pair is no longer redundant
  because only one member survives.
- Step 2 deletes `schedulerHoldWritable`, `gangHoldWritable`,
  `nodeUnknownHoldWritable`, `sourceUnroutedWritable` and `pauseHoldWritable` in
  favour of one table whose columns are the `Delete`-refusal and the pause
  exception.

### The failure mode

Seven owner decisions landed in the week before the lock (OEP-0020.2, "The cost
we are paying now"). Each was implemented in five or six places, and in every one
the review or CI found the place that was missed: escalation skipped the rows a
new hold needed, a hold's token overwrote another's, one pass's requeue disagreed
with another's, the terminal recycle existed in two passes out of six. With V
verb passes and E event columns there are V × E cells to get right and nothing
owned a column. The next hold, or the next evidence shape, cost the same again.

The if/else the owner noticed is the same fact seen from inside a file. It was
five kinds of branch, and four of them existed only because a rule had no single
home.

| kind of branch | where it lived | count |
|---|---|---|
| ownership: may this pass act on this row | `isSurgeOwnedStatus`, `createAttemptOwnsPods`, `gang.terminalOwnedIndices`, `GangRowOwnedElsewhere`, `gangVerdictActionable`, `restartInFlight`, `pausedUpdateContinuations`, `pauseHeldOperation`, the `excluded` skip set threaded through `escalateFromEvidence`, the `Phase==Failed` skip in `reconcileExternalHolds` | ~10 predicates, each read in 2–6 places |
| hold writability and precedence | `schedulerHoldWritable`, `gangHoldWritable`, `nodeUnknownHoldWritable`, `sourceUnroutedWritable`, `pauseHoldWritable`, `supersedes` | 6 rules for 6 holds |
| event branches inside a verb pass | rejection classification in create and in-place; unknown phase in create and restart; terminal recycle across the passes; pause checks in surge | the V × E product |
| identity preconditions inside mutation callbacks | every stamp re-checked incarnation, operation id and target on the fresh row inside `MutateInstance` | ~40, one per stamp |
| pass order and early returns | the scale-down commit boundary, migrate in flight, the update stop, create last, all in `executeActions` | ~8 return points |

The fourth kind is the correctness contract and stays — as one helper in
`status/precondition.go` rather than forty hand-written checks. The other four
are the scope of this OEP.

### What is sound and stays

- The loop: observe, plan, execute with preconditioned writes. Every stamp
  re-reads the fresh row inside `MutateInstance`; that is why the stale-copy
  hazards the lock found were suppressions and duplicate events, never a wrong
  write.
- The verb passes as the home for "advance my own operation one step". An
  operation is a real thing in the row, and the surge, gang, in-place and
  recreate sub-machines are genuinely different sequences.
- The leaf libraries: `types`, `query`, `drain`, `podreadiness`, `revision`,
  `podgroup`, `gang`, `audit`. Single-purpose and reused correctly.

### Goals

1. Exactly one place answers "who drives this row this pass", and every pass and
   every hold asks it.
2. Exactly one place per event column: a new hold is one file plus one row in a
   precedence table; a new evidence shape is one file.
3. Row writes are confined to an allow-list of writers, with decision state and
   evidence state in separate fields.
4. Each move is reviewable on its own commit range, golden-identical or
   accompanied by the cell it changes, and the sequence can stop after any step
   with the package in a better state than before.
5. Exactly one test file per production file, in every package under
   `pkg/controller/v1beta1/workload/...`, enforced by a check in the lock rather
   than by a reviewer noticing. A behavior with no obvious test file to open is
   a behavior whose owner is unclear — the same defect as an event column with
   no owner, seen from the test tree.

### Non-Goals

- Per-step files. Rejected by the owner: it splits operations that are real units
  and gives the event columns no owner either.
- Changing the package layout of the leaf libraries.
- Changing behavior. Where a step exposed a disagreement between two predicates
  or two passes, the disagreement is recorded as a golden diff and decided
  through the table.
- Changing *when an owner acts on* a hold. Step 2 moved both the rules and the
  call sites, so a token is recorded earlier; it moved no step boundary, because
  the Interruptible column keeps every boundary where it was.
- The coordination, canary and rollout-run machines under
  `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative`. Same disease,
  later OEP — with one exception listed under **Cross-cutting issues**.

## Proposal

### Owner: one function

`Owner(row) → create | update | restart | migrate | delete | none`, computed once
per pass **from the persisted row alone** — `Phase`, `Operation.Type`,
`Operation.Step`. No observation, no live pod list, no published counter. The
reason is bounded blast radius: a function that reads only durable state gives
the same answer to every caller in the pass, which is the whole point of having
one owner, and it cannot silently import a fact the engine does not write (see
**What `Owner()` may not read**).

The table lives in `types/owner.go`, keyed by the state ids of
`tests/rolloutspec/machines/instance/states.yaml`. It answers `Owner(row)`,
`StateOf(row)`, `OwnerOfOperation`, `Interruptible(row, event)` and
`RowsByOwner`. Interruptibility is a **column of the same table**, not a second
function: per (state, event) the table says who owns the row and whether that
event takes the row from its owner immediately or is honored at the owner's next
step boundary. The escalation pass is its production caller: before either
clock may end a row's step it asks `Interruptible(row, EventStuckPodGrace)` and
`Interruptible(row, EventOperationDeadline)`, which is how a migration pair and
a teardown are left to their own timeout authority without the pass naming
them.

The table answers a second question beside `Owner(row)`: `ClaimOf(row)`, the
verb the row's operation still belongs to whatever the phase says. The two
differ on a Failed row. Its preserved operation is a spent attempt no pass
drives, so `Owner` is `OwnerNone`; but the pinned revision, the surge index and
the migration pair it names are still that verb's. A read that decides whether
to *start* something on a row asks `ClaimOf` — the paused update selection
(`anyUpdateContinuation`), the update and restart triggers and the wreckage scan
(`isMigrateOwnedStatus`) — and a read that decides whether a pass may *advance*
the row asks `Owner`. `pause-failed-update-continues` is the golden that tells
them apart: a Failed row with a preserved Update is re-driven under pause,
because its claim is an Update even though nobody owns it.

Three tests in `tests/rolloutspec/owner_table_test.go` hold the table and the
machine together:

| test | what it refuses |
|---|---|
| `TestOwnerTableMatchesInstanceMachine` | a state in the machine the table does not answer for, a state in the table the machine does not declare, and a state whose own declared (phase, operation) does not resolve through `StateOf` |
| `TestInterruptibleMatchesBeforeWindow` | an Interruptible cell that disagrees with the machine's `before`-window outcome, over all 15 states × 10 events |
| `TestInterruptingEventHasAnArrow` | an interrupt the machine declares no outgoing arrow for |

The cross-check ran clean: **zero** of the 150 cells disagree with the decided
`before` window, so step 1 recorded no pending disagreement.

The table's current content:

| Phase | Operation | Owner | holds writable | Interruptible | escalation acts | notes |
|---|---|---|---|---|---|---|
| Empty, Pending | none | create (materialize) | no operation to hold | stuck-pod grace on Pending, as the leftover-pod escalation | no | the truth pass under pause demotes |
| Creating | Create | create | yes | boundary: every hold; immediate: the two clocks | yes (deadline, stuck) | create retires its own superseded attempt |
| Ready | none | none; triggers may start update, restart, repair, scale-down | no operation to hold | stuck-pod grace only | crash-loop repair after grace only | |
| Updating | Update/Surge, SurgeDrain | update (surge machine) | yes; SourceUnrouted only here | boundary: every hold, including a pause arriving mid-surge; immediate: the two clocks | yes | |
| Updating | Update/GangSurgeTarget, Cleanup | update (gang machine) | yes on the surge row; the source's clock via SurgeIndex | boundary, per row: the surge row and the source row reach their boundaries independently | yes | the surge source never reads the surge's group |
| Updating | Update/InPlace, Drain | update | yes | boundary; immediate: the two clocks | yes | pod loss re-resolves InPlace to Drain in place |
| Restarting | Restart/Drain | restart | yes; Paused only under Freeze | boundary; immediate: the two clocks | yes | |
| Migrating | Migrate (source and surge rows) | migrate (the record) | scheduler and PodGroup yes; gang verdicts no | boundary; no clock interrupts — the record is its own timeout authority | no | the scheduler hold is scoped to the row whose own pod is unplaceable, so a serving source is never held for its surge's fate |
| Failed | none or a spent operation | none | no | nothing to interrupt | evidence refresh only | sticky; the retry gate may start a fresh attempt, and the adopt shape promotes on the target |
| Deleting | Delete | delete batch | never | nothing interrupts: `DeleteBatch` paces its own removal past any grace | no; overdue-drain warning only | force-delete policy-gated |

The predicates deleted in favour of it — the ten ownership rules plus the five
hold-writability rules and their `supersedes` pair:

| predicate | had lived in | call site now |
|---|---|---|
| `isSurgeOwnedStatus` | `ops/update.go` | `Owner(row) == OwnerUpdate` |
| `isMigrateOwnedStatus` | `ops/update.go` | kept, as the `Migrating` pin or `ClaimOf(row) == OwnerMigrate` — a claim, since a Failed pair row is still the record's |
| `createAttemptOwnsPods` | `ops/create.go` | `StateOf(row)` |
| `gangSurgeSourceOwnsRemoval` | `ops/update_gang.go` | kept: a claim matcher on the surge index, not an ownership read |
| `gangSurgeTargetOwnsRemoval` | `ops/update_gang.go` | kept: a claim matcher on the marker step, not an ownership read |
| `gangSurgeTargetClaimMatches` | `ops/update_gang.go` | `status.GangSurgeTargetClaimMatches` |
| `gangSurgeActiveTargetClaimMatches` | `ops/update_gang.go` | `status.GangSurgeActiveTargetClaimMatches` |
| `gangSurgeCleanupTargetClaimMatches` | `ops/update_gang.go` | `status.GangSurgeCleanupTargetClaimMatches` |
| `migrationSourceOwnsRemoval` | `ops/migrate.go` | kept: a claim matcher on the request UUID and pair index, not an ownership read |
| `migrationSurgeOwnsPromotion` | `ops/migrate.go` | kept: same |
| `hasDeleteOwnedInstance` | `plan.go` | `RowsByOwner(...)[OwnerDelete]` |
| `validateTerminalMutationOwner` | `ops/terminal_recycle.go` | `status.RequireOwner` |
| `isSurgeUpdateStep` | `ops/update.go` | `status.SurgeUpdateStep` |
| the five `*HoldWritable` rules + `supersedes` | `pause.go`, `unschedulable.go`, … | `holds/table.go`'s `mayOwn` + `takesRowFrom` |

The strategy pin survives deliberately: it is the one predicate that reads the
operation's recorded `Strategy` rather than deriving ownership, and
`strategy-edit-midsurge` is the golden sensitive to it.

#### What `Owner()` may not read

Two reads in the tree decide on counters the engine does not write:
`instanceLostGangMember` compares live pods against the row's `PodCount` and the
migration tail's `surgeTailGatesPassed` requires `AvailablePodCount >=
PodCount`. Both counters come from the status publication path in the adapter,
not from `workload.Reconcile` — which is why 20 arrows carry `lane: adapter`.

`Owner()` does not take them. They stay where they are, reached through one
accessor in `types/` that names them as adapter-lane inputs, so the dependence
is visible instead of laundered through the one function every pass consults.
Whether these reads move into the verb that owns them, or the counters become
observation-derived inside the engine, is a later decision with its own goldens.
Taking them into `Owner()` would have locked the engine's dependence on
adapter-published state into the piece of code least able to shed it.

### Holds: one package, one precedence table

`holds/table.go` is the whole rule, consulted in this order once per row at the
top of the reconcile. `mayOwn` is the whole of the writability rule; the rest is
the token-precedence guard.

| # | authority | token | may own | invariant it encodes |
|---|---|---|---|---|
| 1 | `quotaHold` | `QuotaExceeded` | any owner, a teardown and an unowned row included | admission can refuse any create, and the refusal is recorded wherever the create site left it. Read off the recorded `CapacityRefusedAt` — a refusal is an apiserver answer no later pass can re-read |
| 2 | `nodeUnknownHold` | `NodeUnknown` | every owner except `Delete` and `None` | a pod on its way out occupies no name anyone is waiting to rebuild. Reports only the pods the force-delete sweep leaves: a name the sweep frees on this pass is never a wait |
| 3 | `sourceUnroutedHold` | `SourceUnrouted` | `Update` only | only a surge takes its source out of rotation, so only a row the update pass owns can report one out of it |
| 4 | `schedulerHold` | `Unschedulable` | every owner except `Delete` and `None` | a teardown's pods need no placement and `DeleteBatch` paces its own removal past any grace; a row with no owner has no attempt to hold |
| 5 | `gangHold` | `PodGroupTerminating` | every owner except `Delete` and `Migrate` | refused on the two durable claims nothing read off a PodGroup may take a row from: the teardown, and the migration record, which is the timeout authority for both pair rows |
| 6 | `pauseHold` | `Paused` | `pauseHeldOperation` — the attempts a pause stands in front of; marked `everyRow`, so it also releases its own token on the rows the fact-bearing holds never touch | the weakest, and the only token reporting a decision rather than a fact |

The order is the order the verb passes read these facts in: the create pass
read the silent node, the update pass read the rotation, and the scheduler and
the PodGroup were read at the end of the pass. Two facts fresh on one unheld
row report in consultation order — the first recorded is the incumbent and
first-writer-wins keeps it — so the order is an invariant of what an operator
sees, not a listing convenience. `hold-nodeunknown-with-unschedulable` and
`hold-unrouted-with-unschedulable` lock the two cells that distinguish this
order from the one the table first shipped with.

`takesRowFrom(token, current)`: an empty or identical incumbent always yields;
otherwise the **one** ordered pair is *any fact token takes a row from the
pause* — the fact is what an operator needs to see, and the pause goes on
holding the rollout either way. Everything else is first-writer-wins: replacing
an incumbent fact token would tell an operator the row is queued behind
something it is not.

**The hold pass: record early, act at the boundary.** `holds.Run` runs once per
row from `runHoldPass`, before the verb passes — releasing first, then
recording. Three positions collapsed into it: the pause hold after the actions,
the scheduler and PodGroup holds at the end of the pass in the evidence half,
the quota hold at the create site.

Those three positions were load-bearing for a reason that survives the move:
"a step in flight finishes to its boundary, then holds" is not a statement about
*when the token is written*. It is a statement about *when the owner stops*.

- **Record early.** The token is written as soon as the condition is observed —
  at the top of the pass, from the persisted row plus the observation.
- **Act at the boundary.** The Interruptible column decides, per (state, event),
  whether the owner abandons its step now or finishes the step it is on. No hold
  interrupts in any state, so recording a `Paused` token before the surge step
  runs does not stop that step from running.

The cost is the whole of step 2's diff: tokens land in a different pass than they
did, and the goldens print tokens. The quota hold — the one input that is *not*
readable from the observation, because it is an apiserver rejection from a call
the create pass made — inverts: the create site classifies the rejection and
records `CapacityRefusedAt` on the operation, and the hold pass converts that
record into the `QuotaExceeded` token on the following pass.

The sharpest consequence had to be answered, not assumed: a one-pass unparked
deadline could expire an operation that used to survive. It does not, because
the park does not wait for the token. `types.OperationCapacityRefused` reads the
record, and the deadline-parking step reads that — so the clock stops on the
pass admission said no, and only the token lands a pass later.
`quota-hold-parks-deadline` prints exactly that: the deadline is parked in tick 1
and re-armed in tick 3, while the token appears in tick 2.

### Evidence: one package, reads only

`evidence/` holds the classifications that lived in `types/stuckpod.go`,
`types/notready.go`, `types/apierror.go`, the node-death reader in
`ops/force_delete.go` and the limbo readers in `disposition.go`. Seven files:
`stuckpod.go`, `readiness.go`, `rejection.go`, `terminating.go`, `crashloop.go`,
`templatediff.go`, `doc.go`. Each returns a typed fact with the pod, the reason
and the anchor time. Escalation consumes them for the deadline disposition and
the fast path; create consumes the rejection class; **nothing in `evidence/`
writes**, which the layout gate's row-writer rule enforces. The relocation-first
rule is untouched: evidence names, the disposition chooses.

Step 2's rejection carrier was born in `ops/` where the classifier still lived,
and step 3 relocated it to `evidence/rejection.go` unchanged — which is why step
3 is golden-identical. Running step 3 first, so the carrier was born in its final
home, was considered and rejected: it delays the only timing change in the
sequence behind a pure move, and every step after a re-recording step should be
measured against the final timings.

### Status: one writer

`status/` holds every stamp in eight files: `decision.go` (phase, operation,
the step transitions, the promote terminator, the Failed stamps), `gangsurge.go`
and `migration.go` (the two decisions that write a pair of rows in one batch),
`evidence.go` (`LastFailure`, the capacity-refusal record, the waiting token),
`announced.go`, `terminal.go`, `precondition.go` and `doc.go`. Two contracts:

- Every stamp is preconditioned on the row identity it was decided against —
  incarnation, operation id, target — through `status/precondition.go`, so the
  forty hand-written checks are one function and a table of expectations.
- Decision fields (`Phase`, `Operation`, `RunningRevision`, `TargetRevision`) and
  evidence fields (`LastFailure`, `Operation.Waiting`, `CapacityRefusedAt`) are
  written by different stamps and never by the same one. Edge triggers key on
  decision fields.

The writers are named by kind, and `status/doc.go` states the rule: a decision
writer is `Stamp<State>` (`StampFailed`, `StampDeadline`, `StampStep`,
`StampRecreateFromInPlace`), with the few that move a row by a rule rather than
to one named state named for the rule (`DemoteUnbacked`, `StartGangSurge`); an
evidence writer is `Record<Fact>` with `Release<Fact>`/`Clear<Fact>` as its
inverse (`RecordWaiting`, `ReleaseWaiting`, `RecordCapacityRefusal`); a
once-only message is `Announce<X>` (`Announce`, `AnnounceDrainOverdue`); the
RetryBlock writers are `RetryBlock<Verb>`; and the in-mutation helper a stamp
composes is named for the state it enters (`EnterReady`).

`LastFailure` was an edge-trigger marker in two places: the overdue-drain warning
fired once per episode by comparing `LastFailure.Time` against the deadline, and
the unschedulable hold left its record in place after clearing so the message
survived. Both now key on `Announced`, and `LastFailure` carries diagnostics
only.

**`Announced` is the one home for once-only messages** — all six, not just the
two that were overloaded onto `LastFailure`:

| message | where its dedup lived | scope now | why the row is the right key |
|---|---|---|---|
| `DrainOverdue` | `LastFailure.Time` vs the deadline | attempt (the Delete operation) | it is a per-row episode, and `LastFailure` is evidence, not a marker |
| the unschedulable message | the `LastFailure` record left in place after the hold cleared | attempt | same |
| `RepairHeld` | none: re-emitted on every held pass | attempt; the row carries no operation while a repair is held, so the incarnation | **a deliberate cadence change.** Once per row episode instead of once per pass — an operator reads it once and the row keeps the record |
| `PodGroupReset` | none: re-emitted on every reset | attempt | **a deliberate cadence change**, same: a second reset inside one attempt is not announced again |
| `GangSplitRisk` | package-level `sync.Map` in `ops/gangsplit.go` | standing (the incarnation) | process-global dedup means the second replay of a scenario prints nothing the first printed — untestable by construction; a warning about the template is not re-delivered because an attempt opened or concluded |
| `MaybeNoGangScheduler` | package-level `sync.Map` in `gang/gang.go` | standing | same |

Each message states its scope, and the scope is the key: a standing warning
about the Instance as built keys on the incarnation alone, so it is not
re-delivered because an attempt opened or concluded; an attempt-scoped message
keys on the operation, or on the incarnation when the row carries none. The
prune keeps a marker while its episode is the row's current attempt or its
current incarnation, so clearing is derived and no site has to remember it.

Both `sync.Map`s are deleted. That is the point of the change, not a side effect:
a warning deduped on the process cannot be replayed. The two standing warnings
now fire once per Instance rather than once per process, and
`MaybeNoGangScheduler` lands one pass after a fresh create, since it needs a row
to record on.

The `Announced` field must exist in the CRD before a controller writing it is
rolled: against an older CRD the apiserver prunes the field, the marker never
persists, and the message is re-emitted on every pass until the CRD is updated.

`GangSplitRisk` is recorded on the Instance whose worker earned it. The scheduler
heuristic is about the Component's template, so `MaybeNoGangScheduler` takes the
first row whose group the pass ensured, and waits for one to exist rather than
announcing into a slot the create pass has not opened.

### Cross-cutting issues the lock surfaced

| issue | pointer | what this OEP does with it |
|---|---|---|
| **A per-pass spec write starves every fresh-snapshot consumer.** The IR projector runs in Step 6 of the ISVC reconcile and the canary dispatch runs in Step 6a of the same pass; the dispatch gates on `ir.Status.ObservedGeneration == ir.Generation`. A projector that bumps `Generation` every pass means that gate never opens | the `canary_retarget_rollback_kind_test` entry in `KIND_QUARANTINE`, owner `slin1237`, expiry 2026-10-04 | the same disease one layer up: a writer and a reader in one pass with no owner between them. Not fixed here; the resolution belongs in the later coordination OEP |
| **Warning dedup in package-level `sync.Map`s outlived the run.** | `tests/rolloutspec/replay/README.md`, Unlocked paths | **Done:** `Announced` is the per-row store, both `sync.Map`s are deleted, and `gang-advisory-warnings` is the scenario that locks both warnings |
| **The engine reads counters only the adapter writes.** | the `lane: adapter` marking on 20 arrows | **Done:** `Owner()` reads only the persisted row and takes neither counter. Both reads stay where they are, behind one `types/` accessor that names them as adapter-lane inputs |
| **Ownership scattered across passes, as a worked example.** Whether a Held RetryBlock denies only a fresh start or also the Create pass's fill of a lost member was decided in two places that disagreed | `ops/retryblock_gate.go`, `ops/create.go` | it is one gate at every create site, at the revision each pod is for. That is what step 1 does for the other ten predicates |

### Structure, as built

The tree gains five packages: `holds/`, `evidence/` and `status/` in steps 2–4,
`escalation/` and `service/` in step 7. The reconcile root holds orchestration
and planning and nothing else:

```
workload/
  reconcile.go            observe → holds → plan → operations → escalation → wake
  pass_restart.go pass_update.go pass_migrate.go   one verb pass each, (result, stop, error)
  plan.go plan_decision.go
  snapshot.go observation.go doc.go
  escalation/ escalation.go gang.go scheduler.go refresh.go disposition.go deadline.go
              budget.go retryblock_prune.go doc.go
  holds/      pass.go table.go quota.go scheduler.go gang.go nodeunknown.go sourcerotation.go pause.go doc.go
  evidence/   stuckpod.go readiness.go rejection.go terminating.go crashloop.go templatediff.go doc.go
  status/     aggregate.go decision.go gangsurge.go migration.go evidence.go announced.go terminal.go
              precondition.go doc.go
  service/    service.go
  ops/        create.go create_rejection.go unknown_phase.go
              update.go update_surge.go update_gang.go update_inplace.go update_recreate.go
              restart.go migrate.go migrate_expire.go delete_batch.go render.go reset.go
              terminal_recycle.go force_delete.go doc.go
  types/ query/ drain/ podreadiness/ revision/ podgroup/ gang/ audit/ replay/   unchanged in shape
```

Each escalation arm pairs with its hold by file name across the two packages —
`holds/gang.go` reports the wait, `escalation/gang.go` ends the attempt — and
`escalation.Run` takes the hold pass's `Result` as input, so the pairing is a
call and a name rather than a directory. The pass derives its own per-row
evidence (`InstanceEvidence`, from the same memoized pod read the hold pass
judged on); the root's snapshot hands it that read and nothing else, which is
what keeps `escalation` from importing the root. `PodAdmissionGated` lives in
`types/scheduling.go` for the same reason: `status/` and `escalation/` both
read it. `holds.RowsForPlan` builds the hold pass's rows from the plan, so the
root and the escalation tests drive the hold pass through one constructor.

The table lives in `types/owner.go`: the verb passes under `ops/` and the gang
package reach `types` but not the root package, and one table cannot have two
homes. The root package asks `types` directly — the three files that consult
the table already import it — so there is no second spelling of the answer.

Two files stay in `ops/` that a reading of the target shape would move out:

| file | why it stays |
|---|---|
| `ops/terminal_recycle.go` | `recycleTerminalPods` frees the names terminal and admission-rejected pods hold. Create, restart and every replacement-pod path — per-pod surge, recreate rebuild, gang surge — reach it, and it deletes pods and stamps expectations. It is verb work several verbs share, not an event column |
| `ops/force_delete.go` | `forceDeleteOnNodeDeath` removes a pod whose node is provably gone. The *reading* of node death moved to `evidence/terminating.go`; the deletion is a verb acting on its own row |

Production line counts, `origin/main` before step 7 against the step-7 head.
Whole-file counts of the `.go` files in each directory.

| directory | before files | before lines | head files | head lines |
|---|---:|---:|---:|---:|
| `workload/` (root) | 21 | 5068 | 9 | 2508 |
| `workload/escalation/` | — | — | 9 | 1793 |
| `workload/service/` | — | — | 1 | 124 |
| `workload/ops/` | 16 | 9574 | 17 | 9629 |
| `workload/types/` | 26 | 3882 | 26 | 3890 |
| `workload/holds/` | 9 | 792 | 9 | 816 |
| `workload/evidence/` | 7 | 1100 | 7 | 1100 |
| `workload/status/` | 8 | 1746 | 9 | 2076 |
| `workload/query/` | 4 | 823 | 4 | 823 |
| `workload/replay/` | 8 | 4002 | 8 | 4003 |
| `workload/audit/` | 4 | 740 | 4 | 740 |
| `workload/gang/` | 2 | 816 | 2 | 816 |
| `workload/podgroup/` | 2 | 660 | 2 | 660 |
| `workload/revision/` | 1 | 831 | 1 | 831 |
| `workload/drain/` | 1 | 381 | 1 | 381 |
| `workload/podreadiness/` | 1 | 621 | 1 | 621 |
| **total** | **110** | **31036** | **110** | **30811** |

The root sheds 2560 lines and 12 files. The tree as a whole is 225 lines
smaller, which is the size of the deleted `aliases.go` less the doc comments and
import blocks two new package boundaries cost. Test files: 118 → 120, because
the root and `escalation/` external test packages each carry a
`helpers_external_test.go` for the fixtures the split left on both sides of
the boundary, and `escalation/` gains an internal `helpers_test.go` for its
seam.

Three test names changed hands with the forwarders: the three in
`aliases_test.go` pinned the alias mirror itself and went with it, and
`TestObservedSnapshot_EvidenceDeadline` became `TestEvidenceFor_Deadline` when
the evidence read moved off the snapshot. No `coverage.unit` entry named any of
the four.

### The seven steps, as built

| step | kind | what it does | golden outcome |
|---|---|---|---|
| 1 | golden-identical | `types/owner.go`: `Owner(row)`, `StateOf`, `OwnerOfOperation`, `RowsByOwner` and the Interruptible column; the ten predicates deleted, the strategy pin kept; the adapter-counter reads behind one named accessor | **zero.** All 118 byte-identical, `strategy-edit-midsurge` among them; all 8071 decided cells still covered |
| 2 | **re-recording** | `holds/`: six hold files, one `table.go`, one `pass.go` per row at the top of the reconcile; the five `*HoldWritable` rules and `supersedes` deleted; the call sites moved with the rules; the create site records `CapacityRefusedAt` | **ten goldens, three shapes, nothing else.** No phase, no operation, no step boundary and no action order moves |
| 3 | golden-identical | `evidence/`: seven files of typed facts; step 2's rejection carrier relocated to `evidence/rejection.go` | **zero** |
| 4 | **re-recording** | `status/`: every stamp behind one precondition helper, decision fields split from evidence fields; `Announced` with all six clients; both `sync.Map`s deleted | **zero existing goldens move**, plus one new file: `gang-advisory-warnings.trace`, 21 lines |
| 5 | golden-identical | `ops/` trimmed to advancing its own step; the order-dependent early returns in `executeActions` replaced by per-owner row lists, each naming the constraint it enforces | **zero**, over all 119 |
| 6 | golden-identical | test consolidation across the tree; `check_test_layout.py` added to the lock | **zero**, over all 119, and structural: the step declares no production file |
| 7 | golden-identical | the root thinned to orchestration and planning; `escalation/` (the pass, its gang and scheduler arms, the disposition, the deadline arithmetic, the budget, the prune), `status/aggregate.go`, `ops/reset.go`, `service/`; `aliases.go` deleted and every caller repointed to `types`; the reconcile root taken off the row-writer allow-list | **zero**, over all 119 |

Step 2's ten, with the shape each carries:

| golden | ± | shape |
|---|---:|---|
| `hold-pause-then-quota.trace` | +21/−7 | (a) `Paused` one pass earlier; (c) `QuotaExceeded` one pass later |
| `hold-quota-then-pause.trace` | +21/−9 | (b) release-then-record printed at the top of the pass; (c) |
| `hold-pause-then-unrouted.trace` | +17/−8 | (a) + (b) |
| `hold-unschedulable-then-pause.trace` | +15/−7 | (a) + (b) |
| `hold-unrouted-then-pause.trace` | +13/−5 | (a) + (b) |
| `hold-pause-then-unschedulable.trace` | +12/−6 | (a) |
| `hold-unschedulable-then-nodeunknown.trace` | +10/−6 | (b) authority swap at the top of the pass |
| `quota-hold-parks-deadline.trace` | +10/−4 | (c) — the gating one; the park and re-arm lines are byte-identical, because the park anchors to the recorded refusal |
| `source-unrouted-midsurge.trace` | +4/−2 | (b) |
| `surge-force-clock.trace` | +4/−2 | (b) |

The shapes: **(a)** the `Paused` token appears one pass earlier, or one position
earlier within its pass, on the scenarios that pause a row mid-operation; **(b)**
a row swapping authority prints its release and the next record at the top of one
pass instead of across two; **(c)** the `QuotaExceeded` token appears one pass
later, with the deadline parked on the same pass as before.
`hold-pause-then-quota` gains a fifth tick: its fourth used to record the whole
hand-back and now records only the take, and without the extra pass the scenario
would stop naming the contract it exists for.

Step 4 moved no existing golden. The draft predicted that the gang scenarios
which declare `topologyKey` and `schedulerName` to dodge the two advisory
warnings would have those declarations stripped and gain the warning lines.
**That was rejected during the step.** Stripping them would have made every gang
scenario carry two lines that are not what it is about — a surge-deadline
scenario is not the place to lock a co-location warning — and it would have
re-recorded a dozen traces to prove one property. `gang-advisory-warnings` is the
one scenario that declares neither, and it is where both warnings are locked: one
delivery each and a third pass that repeats neither. Every other gang golden
stays byte-identical, and `tests/rolloutspec/replay/README.md` now says why the
rest keep their declarations.

Why this order, and not another:

1. **Step 1 first, and golden-identical, is not negotiable.** It is OEP-0020.2's
   own Stable criterion — the lock is unproven until it has cleared a real
   refactor once — and every later step's diff is only legible if ownership
   already has one answer.
2. **The re-recording step goes as early as it can.** Step 2 changes when tokens
   land; every step after it is measured against the final timings.
3. **Step 3 after step 2, deliberately.** Both start from `holdevidence.go`, but
   step 2 needs a rejection carrier, and paying for it with a few lines in `ops/`
   that step 3 relocates is cheaper than delaying the timing change.
4. **Step 4 late, because it is an API addition.** `Announced` needs a
   regenerated CRD and a codec round-trip; nothing else depends on it.
5. **Step 5 is the last step that touches production code, because it is the
   only one that is pure subtraction.** It deletes branches the earlier steps
   made unreachable; run it first and it would be deleting live code.
6. **Step 6 last, because its input is the other five.** Which test file a test
   belongs in is decided by which production file owns the logic. Going last also
   means the layout check is added once, to a layout that is already true.

### The two API additions

Both on `InstanceStatus` / `InstanceOperation` in
`pkg/apis/ome/v1beta1/lifecycle_types.go`, with deepcopy, CRD, openapi and the
`v1beta1convert` round-trip regenerated.

**`InstanceOperation.CapacityRefusedAt *metav1.Time`** (step 2) — when admission
last refused one of the operation's pod creates for lack of quota. A refusal is
the apiserver's answer to a call the create pass made, so no later pass can read
it off the cluster; it is recorded on the row, and the hold pass converts it into
the `QuotaExceeded` token on every pass it stands. The deadline park reads the
record directly, so the clock stops on the pass admission said no. Cleared by a
create that completes with no refusal. The record carries **the moment and
nothing else**: a quota message names current usage and the pod that lost the
race, so it differs every pass and would rewrite status for as long as the wait
lasts.

**`InstanceStatus.Announced []string`** (step 4, `listType=set`) — the once-only
messages this Instance has already emitted, one entry per message as
`<event reason>@<episode>`. Recording it on the row rather than in controller
memory is what makes "once" hold per workload rather than per process, and
survive a restart.

The ColumnarV2 encoding carries it too: `InstanceStatusEntry` gains an
`announced` field with `MinItems=1`, and the entry's "at least one record"
`XValidation` rule now names it alongside conditions, readySince, operation and
lastFailure. Without that, a row whose only sparse record is an announcement
would be dropped on the wire and the message would repeat.

### The test-file rule

Tl;dr: a step ends with exactly one test file per production file it touched,
named `<file>_test.go`. This bound every step, 1 through 6.

- **Named for the file it tests.** Tests for `ops/create.go` live in
  `ops/create_test.go` and nowhere else. *Rationale:* a reader who must guess
  which of six files holds the assertion they need is paying the same tax the
  engine's ten ownership predicates charged — one question, several places that
  answer it.
- **Tests travel with the logic.** A moved test is *moved*, not rewritten: the
  assertion that pinned a rule is the evidence the rule survived the move.
- **A test that covers two production files after the move is split along that
  line.** "Which file owns this behavior" is the question this OEP exists to
  answer, and a test file that straddles two files is that question left open in
  the test tree.
- **Shared fixtures live in one `helpers_test.go` per package**, declared in the
  package under test so it can hold unexported fixtures *and* export the
  test-only seams. Where a package's tests split across the internal and external
  test packages, its external half is `helpers_external_test.go`: a fixture a
  `package x_test` file calls cannot be declared in a `package x` file.

### The layout gate

`hack/omenative-testing/statemachine/check_test_layout.py`, with its unit tests
in `check_test_layout_test.py` beside the other checkers', invoked from
`make rollout-behavior-lock` next to `check_history.py` and `check_coverage.py`.
It reports under the existing "Rollout Behavior Lock" CI step and runs before the
Go packages do — a layout failure should cost a second of python, not a full test
run. It walks every package under `pkg/controller/v1beta1/workload/...` and
enforces two properties.

**Test layout**, three conditions:

| condition | what it catches |
|---|---|
| a production file that declares at least one `func` has no `<file>_test.go` | a file whose behavior is only tested somewhere else, which is how a test goes stale unnoticed when the file moves |
| a production file has more than one test file | the duplication this OEP removes, growing back in the test tree |
| a test file matches no production file | the scenario-named tail step 6 homed; without the gate it regrows one file per incident |

Allowed per package: `helpers_test.go` and `helpers_external_test.go`, exempt
from all three. Where Go forces the split — an internal test reaching unexported
state, an external test pinning the package's surface as a consumer sees it —
`<file>_test.go` and `<file>_external_test.go` are both allowed for that file and
nothing else.

**Row writes.** A row-mutation closure — `func(s *InstanceStatus) bool`, the
callback the status adapter applies — may be declared only in `status/`, `ops/`
and the reconcile root. The check is an **allow-list of writers**, not a list of
readers, so a package added later is checked by default: `holds/`, `evidence/`,
`types/`, `query/`, `gang/`, `audit/`, `revision/`, `podgroup/`, `drain/`,
`podreadiness/` and `replay/` are locked out today, and so is any package added
tomorrow.

The gate was added **in step 6, once the layout was already true** — never
earlier. A check added in step 1 would have failed every build from step 1
through step 5 for a layout that no step before 6 was permitted to fix in full,
and a gate that is red for five consecutive PRs does not enforce anything: it
teaches the team the bypass. Name the anti-pattern so it stays named — *a gate
merged ahead of the invariant it guards is a bypass with a green checkmark on
it.*

What the gate turned from red to green, measured by mapping each test file to the
longest production stem its name starts with:

| | main | head |
|---|---:|---:|
| test files matching no production file | 36 | **0** |
| production files with no test file | 30 | **0** |
| production files with more than one test file | 16 | **7** |
| tests retired | — | **0** |

All 7 remaining are the forced internal + external split:

| file | why Go forces it |
|---|---|
| `escalation/escalation.go` | the escalation pass is engine-internal; the external half drives it through the `helpers_test.go` seam |
| `reconcile.go` | `scaleDownPollResult` is unexported; the external half drives `Reconcile` end to end |
| `plan.go` | the plan internals are unexported; the external half calls `BuildPlan` as a consumer |
| `ops/create.go` | the rejection dispositions reach unexported classifiers; the external half calls `Create` |
| `ops/update.go` | pre-existing on `main` |
| `ops/terminal_recycle.go` | the recycle guard reaches unexported state; the external half calls the exported entry |
| `replay/driver.go` | the expectation test reaches `newDriver` / `d.store`; the external half runs scenarios |

The 16 on `main` were not this shape — they were six copies of `create_test.go`,
five each of `migrate_test.go` and `update_test.go`, four of
`delete_batch_test.go`, and so on.

**No test was retired.** Every one of the 36 unmapped files and every test in
them was *moved*: the set of `func Test…` names in the workload tree is identical
at the start of step 6 and at the branch head. The OEP allowed retiring a test
that is provably a third copy of a replay golden plus a decided cell, and step 6
declined the allowance: deciding that for ~1200 tests is an audit, not a move,
and it would have made step 6 a behavioral change. It is a follow-up, with
`rolloutspec-check` as the gate. (Steps 1–5 did delete 15 test names, each one
pinning a predicate or a `sync.Map` that no longer exists — among them
`TestTerminalRemovalOwnershipPredicates` and
`TestEnsurePodGroups_MaybeNoGangScheduler_DedupPerProcess`.) `// Cells:` tags
travelled with their test bodies, and no `coverage.unit` entry needed
repointing: a `unit` entry names a Go function, and no function was renamed.

## What the table cannot express yet

Four things the restructure surfaced and did not close. Each is a follow-up, and
each names what will prove it. (Claim versus ownership was a fifth; `ClaimOf`
closes it — see **Owner: one function** — and the gang surge pair's per-revision
claim matchers in `status/` stay as they are, since they compare a pinned
revision rather than name a verb.)

1. **`(Creating, no operation)` has two readings.** `StateOf` answers
   `StateUnknown`, which is the right refusal — an unrecognized combination is
   not a step anything knows how to advance. But `surgeFreeIndex` in
   `ops/create.go` reads the same shape as create's, because it arises from a
   partially applied status write and the next create repairs it. No pass is
   wrong today; the table simply does not name the state. *Follow-up:* give the
   partially-applied shape a state of its own in the machine and the table,
   proved by `TestOwnerTableMatchesInstanceMachine` and by `create-superseded`
   and `T-empty-create-retry` staying byte-identical.
2. **"Failed with no operation" is two rows printing one state.** A spent attempt
   whose operation was cleared and a row demoted by the truth pass both resolve
   through `StateFailed` — the phase check short-circuits before `StateOf` looks
   at the operation at all. Both answer `OwnerNone` and both answers are correct;
   a reader of the table cannot tell which row they are looking at without the
   `LastFailure` evidence beside it. *Follow-up:* split `StateFailed` by whether
   an operation record survives, proved by the fourteen `failed-*` goldens and by
   `TestOwnerTableMatchesInstanceMachine` holding the split against the machine.
3. **The quota release lands a pass before the hold pass reads it.**
   `ClearCapacityRefusal` runs inside the create pass, after `holds.Run` has
   already read the row this pass — so the `QuotaExceeded` token outlives the
   block by one pass. `quota-hold-parks-deadline` prints it: at tick 3 the pod is
   created and the row still writes `waiting=QuotaExceeded`. It is cosmetic today
   because the deadline is anchored to the record and not the token, but it is
   the hold pass reading state a verb pass is about to invalidate. *Follow-up:*
   release from the observation the hold pass will read next, proved by that
   trailing line disappearing from `quota-hold-parks-deadline` and nothing else
   moving.
4. **The row-writer rule is not `status/`-only.** Step 5's premise was that
   `status/` becomes the only writer. It is not yet true: `ops/` still authors 26
   row-mutation closures (`update_gang.go` 11, `create.go` 5, `migrate.go` 5,
   `delete_batch.go` 2, `migrate_expire.go` 2, `reset.go` 1) and
   `escalation/refresh.go` authors 1, against 32 in `status/`; the reconcile
   root authors none. The two the step named — the promote terminator and the
   migration pin — are routed; routing the remaining 27 is production change
   well past step 6's "move no production code". The gate therefore enforces
   the **true** rule, an allow-list of three directories (`status/`, `ops/`,
   `escalation/`), rather than an aspiration.
   *Follow-up:* tighten `ROW_WRITERS` in `check_test_layout.py` to `status/`
   alone, proved by the gate going green with all 119 goldens byte-identical.

One more duplicate is left deliberately, and is not a follow-up:
`sameFailureIdentity` and `status.SameEvidence` in `status/evidence.go` are **not
the same predicate**. One keys on the failure's identity — pod, container, reason,
exit code, never the moment — so a re-capture does not churn the field; the other
keys on the episode's moment, so a same-name pod held again later opens a new
record, and it reads a nil fresh record as "nothing to say". Both doc comments
name the difference and say folding them would be wrong.

## Failure modes and mitigations

- **The owner table encodes a disagreement as truth.** Two predicates differ and
  step 1 picks one silently. *Mitigation, and what happened:* the Interruptible
  column was cross-checked against the machine's decided `before` window over all
  150 cells, and no cell disagreed. Nothing had to be picked.
- **Holds lose a subtle per-site rule.** For example the scheduler hold's scoping
  to the row whose own pod is unplaceable. *Mitigation:* each per-site rule is a
  row in the precedence table with the test that pinned it moved, not rewritten;
  the ten `hold-*` contention goldens are what proves the columns right.
- **A re-recorded golden hides a real change inside a declared one.** This is the
  specific hazard of steps 2 and 4: once a trace is allowed to move, a second
  unintended line moves with it and the `intentional:` note launders both.
  *Mitigation:* the allowed diff was written per step as a shape and a scenario
  count before the step was written, and the recorded traces are reviewed against
  that prediction. Ten files moved in three named shapes; nothing else moved.
- **Step 2's quota gap is a regression, not a timing diff.** *Mitigation, and
  what happened:* the park is anchored to `CapacityRefusedAt`, not to the token,
  so the deadline stops on the pass admission said no.
  `quota-hold-parks-deadline`'s park and re-arm lines are byte-identical.
- **`Announced` is API surface.** *Mitigation:* optional, string-keyed, no
  defaults, regenerated CRD and codec, carried through ColumnarV2. The
  alternative — keeping markers in `LastFailure` — is exactly the overloading
  being removed.
- **The test layout regresses.** *Mitigation:* the gate, from step 6 on.
- **A golden the harness cannot reach changes anyway.** The Unlocked paths list
  in `tests/rolloutspec/replay/README.md` is the known edge: `Deps.RenderHook`,
  the batch-size knobs, the migration completion tail, the two Pending restart
  arrows. *Mitigation:* the full KIND lane runs against the branch.

## Anticipated objections

Q. The verb passes stay, so what actually got smaller?
A. `ops/` went from 29 files and 11486 lines to 14 and 9617, and the root from 20
and 5711 to 19 and 5183. The passes lost every branch that is not about advancing
their own step. The tree as a whole grew 1116 lines, because three package
boundaries cost doc comments and exported names — so the claim is not "less
code", it is "one answer per question".

Q. Is `Owner()` not just an eleventh predicate?
A. It is the only one. The other ten are deleted and the test for `Owner()` is
the machine itself, held against the table in three directions. A new pass or
hold cannot add a predicate; it adds a row, and `TestOwnerTableMatchesInstanceMachine`
fails if it does not.

Q. The mutation check said three of the hold rules are insensitive. Does deleting
them not risk a silent behavior change?
A. It would if the rules were only insensitive. They were insensitive *and*
redundant with a rule that is sensitive. The two extra things they said — refuse
a `Delete`, except under an operator pause — are now the `mayOwn` and
`takesRowFrom` columns, and the ten contention goldens are what proves them.

Q. You claimed step 2 was a pure move in the first draft and it is not. Why
should I believe the shape of its diff?
A. Because the shape was derived before the step was written and then checked
against what the step produced: three consequences, ten files, and the counts in
the table above. Do not believe a "pure move" claim about step 2; check the ten
recorded traces against the three shapes.

Q. If the hold pass records a `Paused` token before the surge step runs, has the
pause not taken effect a pass early?
A. The token has; the pause has not. Recording marks the row as waiting and parks
the deadline. Whether the owner abandons its step is the Interruptible column's
answer, and no hold interrupts in any state — so the surge step still runs to its
boundary in the pass it would have before.

Q. Why did step 4 not strip `topologyKey` and `schedulerName` from the gang
scenarios, as the draft said it would?
A. Because a scenario should record only what it is about. Stripping them would
have added two advisory-warning lines to every gang trace — a surge-deadline
scenario is not where a co-location warning belongs — and re-recorded a dozen
files to lock one property. One scenario that declares neither locks both
warnings, and the rest keep their declarations for the reason the replay README
now states.

Q. Is a test-layout gate not a style rule with a build failure attached?
A. It would be, if the property were cosmetic. "Where is this behavior tested"
and "which file owns this behavior" are the same question, and this whole OEP is
about questions with more than one answer. The concrete cost was measured: six
test files for `create.go`, 36 naming no file at all, 30 production files with no
test.

Q. Why an API field for `Announced` instead of a label or an annotation?
A. Row status is the only durable per-Instance store the engine has; labels are
per pod and annotations are per IR. An optional record is smaller than a second
status list.

## Test Plan

Gates, as run against the branch head:

| gate | result |
|---|---|
| `gofmt -l pkg/ tests/` | clean |
| `go vet ./pkg/controller/v1beta1/...` | clean |
| `go test ./pkg/controller/v1beta1/workload/...` | ok, all 16 packages |
| `go test ./pkg/controller/v1beta1/workload/... -race` | ok |
| `go test ./tests/rolloutspec/...` | ok — goldens byte-identical, no `-update` |
| `go test ./pkg/controller/v1beta1/inferencereplica/` | ok |
| `python3 -m unittest discover -s hack/omenative-testing/statemachine -p '*_test.py'` | 74 tests, ok |
| `make rollout-behavior-lock MACHINE=instance` | green, including the layout gate |

Per-step assertions, all present on the branch:

- Step 1: a table test over every ownership row, plus the three
  `tests/rolloutspec/owner_table_test.go` cross-checks against the frozen
  machine.
- Step 2: the existing hold tests moved, not rewritten; a precedence-table test;
  `holds.Run` is the only caller of the mark and release writers; the ten
  re-recorded traces carry `intentional:` notes naming their shape.
- Step 4: `Announced` round-trips through `v1beta1convert` and the ColumnarV2
  codec; the `DrainOverdue` and unschedulable edge triggers assert on it; no
  package-level dedup map remains; `gang-advisory-warnings` replays the same
  attempt three times in one process and prints each warning once.
- Step 6: `check_test_layout.py` green over every package under
  `pkg/controller/v1beta1/workload/...`, and its own unit tests green.

The checker's unit tests are named `check_test_layout_test.py`, not
`test_check_test_layout.py`: every checker in that directory is `<name>_test.py`,
and the README documents discovery as `-p '*_test.py'`.

No step reduced the covered decided-cell count; the freeze enforces it.

## Graduation Criteria

All six steps ship in one PR, so the tiers are met together rather than in
sequence.

### Alpha

- Step 1 merged golden-identical; the ownership table is a test. **This was also
  OEP-0020.2's own Stable criterion** — the lock is not proven until it has
  cleared or caught a real refactor once, and step 1 was that refactor. OEP-0020.2
  reached Stable with the merge (#1641, 2026-09-21).

### Beta

- Steps 2 and 3 merged; `holds.Run` is the only place a hold is recorded or
  released, no hold writability rule exists outside `holds/`, no evidence
  classifier exists outside `evidence/`, and step 2's re-recorded traces matched
  the predicted shape.

### Stable

- Steps 4, 5 and 6 merged, and every package under
  `pkg/controller/v1beta1/workload/...` has exactly one test file per production
  file, enforced by `check_test_layout.py` in the lock. **Not yet met:** the
  criterion also asks that adding a hold be one file plus one table row,
  *verified by adding the next real one that way*. No new hold has been added
  since the package landed, so the claim is untested. The next hold is the test.

## Drawbacks

- One PR of 50 commits with no user-visible change, and two of its steps are not
  pure movement: they re-record goldens, which is the most expensive kind of
  review to do carefully. The mitigation is that each re-recording step's diff
  shape was written down before the step was written.
- The tree gained 1116 production lines. Three package boundaries are not free.
- Step 6 re-homes on the order of 100 test files across two packages. It is
  mechanical, and reviewable only because it contains no production change — so
  that is the first thing to check, and it is cheap: the diff touches `*_test.go`
  and nothing else.
- The `Announced` record is an API addition made for internal hygiene.
- The goldens remain a second source of truth beside the tables until the
  generator projection unifies them, so each step's "identical" claim is checked
  against traces, not against the spec.

## Alternatives

1. **Per-step files** (`step_surge.go` and the like). Rejected by the owner; it
   also leaves the event columns unowned.
2. **A transition-table-driven reconciler** generated from the OEP-0020.1 YAML.
   Not rejected forever, but it is a rewrite, and it needs the same lock and the
   same owners as prerequisites.
3. **Consolidate the test files and stop there.** Rejected as a *substitute*: it
   changes nothing about V × E, and homing 100-odd test files onto the old 49
   production files would re-home most of them again the moment steps 2–5 split
   those files. Not rejected as *work*: it is step 6.
4. **Holds: move the rules, leave the call sites** (the first draft's step 2).
   Tradeoff: step 2 stays golden-identical and cheap to review — but the six
   holds keep three recording points, so "one owner per event column" is true of
   the rules and false of the pass. Rejected on 2026-09-21: a column whose owner
   still runs in three places at three times is the failure mode this OEP exists
   to remove.
5. **An `escalation/` package**, as the draft's target structure showed. It
   exists, as of step 7; the arm and its hold pair by file name across the two
   packages and by the `holds.Result` the pass takes as input.
6. **Leave it; keep reviewing hard.** The reviews caught every miss at the cost of
   two to three rounds per PR. That cost recurs per decision.

## Decisions

Five questions were decided by the owner on **2026-09-21**; two more were decided
inside the implementation and are recorded here because each changes what a
step's diff is allowed to look like.

1. **Holds: the rules and the call sites move together.** Holds are evaluated
   once for every row at the top of the pass, before the verb passes. Reconciled
   with "a step in flight finishes to its boundary, then holds" as **record
   early, act at the boundary**. *Consequence:* step 2 is not golden-identical —
   ten traces, three shapes.
2. **`Owner(row)` reads only the persisted row** — phase, operation type, step.
   The two adapter-counter reads stay where they are, behind one accessor that
   names them. Whether they move is a later decision.
3. **Interruptible is a column of the ownership table**, not a second function
   and not a second table, checked against the machine's `before` / `boundary` /
   `after` windows.
4. **`Announced` lives on the row and carries every once-only warning and
   event** — the two existing `LastFailure` markers, `RepairHeld`,
   `PodGroupReset`, and the two gang warnings. The package-level `sync.Map`s go.
5. **The restructure consolidates the tests as well: one production file, one
   test file.** The owner asked for this as part of the restructure, not as a
   follow-up. *Consequence:* the test-file rule binds every step, step 6 homes
   the remainder, and `check_test_layout.py` joins the lock.
6. **A production file that declares no `func` owes no test file** — a *computed*
   exemption, not a hand-maintained allow-list, because an allow-list is exactly
   the drift the gate exists to catch. Decided inside step 6, which is where the
   OEP left it. This is what lets the 20 declaration-only files in `types/` pass
   the gate without a ceremonial test each; a file that grows its first function
   owes a test file from that commit on.
7. **`Announced` clearing is derived from the episode, not stamped.** A marker is
   `<reason>@<episode>`, and the episode is the operation id, or `#<incarnation>`
   when the row carries no operation. A stamp that opens a new operation or
   rebuilds the Instance therefore starts a fresh episode and the previous
   episode's markers simply stop matching — there is no clear to remember to
   write, which is the failure mode a stamped clear would have: one writer
   forgetting it and a warning going permanently silent. Stale markers are pruned
   by the next write that has something to say, since `MarkAnnounced` is their
   only reader.
8. **Escalation is its own package, and the root is orchestration and
   planning only.** Decided on 2026-09-21, reversing the step-3 call: the
   pairing of a hold with the arm that ends the attempt is a call and a file
   name, not a directory.

## Open Questions

The three the draft left open are answered. What remains is the four follow-ups
in **What the table cannot express yet**, of which one is worth naming as the
next action:

- **Tighten the row-writer allow-list to `status/` alone.** 28 closures to route,
  owner unassigned. The gate change is one tuple in `check_test_layout.py`, and
  the goldens are the proof.

Prerequisite outside this OEP: the `canary_retarget_rollback_kind_test`
quarantine expires 2026-10-04, and this branch relies on a green KIND lane.

See also: OEP-0020.2 for the lock this is measured against, and OEP-0020.1 for
the state × event tables that define the event columns. Scope: the OMENative
workload engine only — RawDeployment and MultiNode are unaffected.
