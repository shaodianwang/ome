# Instance machine — review results

Scope: 16 states, 57 events, 138 arrows, 1,331 authored cells. Nine reviewers judged every outgoing arrow and every cell against the event meanings in `events.yaml` and the rubric (safety, liveness, truthfulness, least surprise, consistency, PR intent). Per-family tables are in `instance-<family>.md` beside this file; this file is the roll-up.

## Counts

| family | arrows | KEEP | CHANGE | ASK | exceptions | root causes |
|---|---|---|---|---|---|---|
| noop (Empty, Pending, Ready) | 32 | 20 | 6 | 6 | 22 | 12 |
| failed | 15 | 7 | 10 | 0 | 21 | 11 |
| create | 9 | 4 | 6 | 0 | 20 | 9 |
| update-surge | 26 | 17 | 12 | 1 | 19 | 10 |
| update-gang | 14 | 10 | 4 | 0 | 15 | 8 |
| update-inplace-recreate | 21 | 11 | 8 | 2 | 24 | 10 |
| restart | 7 | 5 | 2 | 1 | 15 | 8 |
| migrate | 10 | 4 | 4 | 2 | 19 | 9 |
| delete | 4 | 0 | 3 | 1 | 17 | 8 |
| **total** | **138** | **78** | **55** | **13** | **172** | 85 rows → 13 distinct |

## Root causes across the machine (85 per-family rows collapse to these)

| # | root cause | where | families | fix shape |
|---|---|---|---|---|
| 1 | Pod presence is tested by name only; a Terminating, terminal, or unschedulable pod counts as present and often as healthy | `query.AllPodsRuntimeReady`, `ops/create.go:missingPodTargets`, `ops/restart.go` Phase B, `ops/update_gang.go` replacement loop, `ops/update_surge.go` release gate | create, failed, restart, surge, gang, inplace | one shared "pod occupies its name and is live" predicate used by every diff and every readiness gate |
| 2 | No permanence classifier beyond container waiting reasons: PodScheduled, pod phase, PodGroup status, and apiserver error classes (422, namespace terminating, quota, throttled) are never read | `status_aggregate.go:terminalPullFailureReasons`, no `IsInvalid` call site, `watches.go:podGroupPredicate` drops status updates | all nine | a permanence predicate over PodScheduled + phase + PodGroup status, and one `classify(err)` helper: terminal → Failed with reason, retriable → honored backoff |
| 3 | Escalation is skipped on many paths: `Execute` returns before it on any error and at the scale-down step; Failed, Delete, and Migrate rows are excluded; op-free rows have no clock; `podSetFullyServing` ignores deleting pods | `reconcile.go:Execute`, `escalation.go:escalateFromEvidence`, `status_aggregate.go:ShouldCheckForStuckPods` | all nine | run escalation on the error path; scope the scale-down return to indices in the wave; give op-free, Delete and Migrate rows a visible non-Failed escalation; treat a pod stuck past grace as disqualifying |
| 4 | Deadlines are stamped once, absolute, and never re-armed or parked: surge deadline at entry, migration record deadline at accept; pause parks nothing except admission-gated clocks | `ops/update_status.go:patchInstanceStatusSurgingForUpdate`, `migration_accept.go:consumeMigrationRequests`, `deadline.go` | surge, migrate, inplace, restart | re-arm on each step advance; park on pause, queue, and admission; rebase the migration deadline from execution start |
| 5 | Force-delete is wired into only the scale-down and migration drains, and only on node-death evidence; a pod wedged Terminating on a healthy node is never force-deleted, escalated, or warned about | `ops/force_delete.go:stuckTerminatingEvidence`, `ops/update_surge.go`, `ops/restart.go` Phase A, `ops/update_gang.go` cleanup | delete, surge, gang, restart, inplace, migrate | shared force-delete evaluation from every drain; an "overdue with no evidence" branch that warns and conditions |
| 6 | Promotion bars differ by path: create, restart, and in-place promote on ContainersReady with no PodReady and no minReadySeconds unless set; recreate and surge require PodReady plus availability | `ops/create.go` ready branch, `ops/restart.go` Phase C, `ops/update_inplace.go` promote block | noop, create, restart, inplace, surge | one promote predicate (PodReady plus available) for every path |
| 7 | The gang path lacks the single-pod safeguards: the source is unrouted and deleted in one pass with no drain check or settle; the strategy-abandon check sits below the gang branch; the Failed branch deletes a healthy replacement; survivors keep serving after member loss; the Cleanup step is miscounted as unavailable capacity | `ops/update_gang.go:gangSurgeUpdate`, `abandonFailedGangSurge`, `ops/update_surge.go:surgeUpdate` entry, `coordination/ratio.go:unavailableInFlight` | gang, surge, failed | reuse the single-pod drain and settle sequence; move the abandon predicate above the branch; revoke serving on a partial gang; add Cleanup to the exclusion switch |
| 8 | The restart pass is unpaced and ungated, a restart that fails once is permanently disarmed, and repair has no backoff or cap | `plan_decision.go:Plan` restart action, `status_aggregate.go:disposableAttempt`, `ops/restart.go:instanceLostGangMember` | noop, restart, failed | pace restarts through the unavailability budget; allow a RetryBlock-gated re-arm; bound repair with a backoff and a visible cap |
| 9 | Failed is not what the table claims: the Create pass promotes a Failed row phase-blind and skips the RetryBlock gate whenever an operation is preserved | `ops/create.go:createFilteredBatched`, `allowCreateForMissingInstance` | failed, restart | declare the two undeclared arrows; gate on RetryBlock regardless of a preserved operation; write down the stickiness rule |
| 10 | Strategy and shape are not pinned to the operation: the update mode is re-resolved every pass so a strategy edit hijacks a committed in-place or recreate; gang width is read live and surplus members are never pruned | `ops/update.go:UpdateWithPods`, `ops/update_surge.go:surgeUpdate` entry, `ops/update_recreate.go` Phase B | inplace, surge, gang, noop, failed | pin strategy and shape on the operation at entry; reap surplus members where MinMember is lowered |
| 11 | Pause is not honored by deadlines or by status truth: no deadline parks on pause; the paused truth pass covers only Ready rows, so a held Creating row reports Creating | `deadline.go`, `plan_decision.go:planUnbackedDemotions` | create, inplace, noop | park operation deadlines on pause; extend the truth pass to transient rows |
| 12 | Behavioral values baked into the binary or read once at startup: 30s settle grace from the in-place field, scale-down cadence and batch size, audit caps | `ops/update_surge.go:waitForSurgeDrainSettle`, `cmd/manager/main.go`, `audit/audit.go` | surge, delete, migrate | own config fields resolved through the TTL config cache; no literal in Go (repo hard blocker) |
| 13 | Table hygiene: cells name transitions the arrow set lacks; some arrows are keyed to the announcing event rather than the guarding one; fan-out cells lack a transition key; two delete arrows are one code path | `transitions.yaml`, several `cells-*.yaml` | create, failed, surge, gang, restart, delete | a validator rule: an outcome `transition` cell must match a declared arrow from its state on its event to its `to`; then fix the table |

## The 13 arrows marked ASK (owner calls)

| arrow | question | reviewer's lean |
|---|---|---|
| T-pending-promote | Pending → Ready on ContainersReady while every other promote requires PodReady | raise to PodReady |
| T-pending-stuck-failed, T-pending-stuckgrace | Pending exists only under pause and escalation is off while paused; do these edges need an unpause precondition | add it |
| T-ready-scaledown | the scale-down wave consults no availability floor; the "pod budget" is an unbounded batch size | gate the wave on the unavailability budget |
| T-ready-gangwidth-create | members are added to a live gang while MinMember is raised in the same pass | raise MinMember after the new members bind |
| T-ready-gangwidth-restart | every instance restarts on the same pass for a width change; no budget | pace it |
| T-restart-deadline | the preserved Restart operation permanently disarms repair after one deadline | allow a gated re-arm |
| T-drain-retarget | every retarget re-bumps the incarnation and restarts Phase A; a fast-moving target never converges | let Phase B finish the current incarnation first |
| T-drain-strategy-surge | a mid-recreate switch to surge inherits a source Phase A already drained | hold the recreate once its incarnation bump committed |
| T-surge-source-lost-promote | promotes on ContainersReady and frees the surge budget for a pod not yet routed | require PodReady |
| T-delete-teardown-deadline | releasing the finalizer strands a pod already wedged Terminating | unroute before release, or document the hard abandon |
| T-migrate-teardown-deadline, T-migratetarget-teardown-deadline | teardown of a pinned pair leaves the ledger able to re-import the request | best-effort terminal ledger mirror that never blocks release |

## The 6 DECIDE cells, with recommendations

| cell | recommendation |
|---|---|
| Restart/Drain × pod.evicted | reap: Phase B deletes any bumped-incarnation pod that no longer occupies its name and recreates it next pass (root cause 1) |
| Restart/Drain × pod.waiting[DeadlineExceeded] | same reap; the variant is structurally unreachable as a waiting reason, so mark the fast-escalation claim n/a |
| Restart/Drain × pod.phaseTerminal | same reap; route Unknown (node lost) to force-delete instead (root cause 5) |
| Update/InPlace × pod.deleted[source] | re-resolve to recreate immediately (transition → Update/Drain) |
| Update/Surge × pod.servingOff[source] | defer with a visible waiting reason; replace "old pods exist" with "a source pod is still in rotation" so an unrouted source takes the fast path |
| Update/Surge × endpoint.rotationOut[source] | decide together with the row above so the pod-gate and the EndpointSlice views agree |

Two more cells the reviewers say should have been DECIDE: Delete/Drain × timer.operationDeadline (a stamped deadline nothing consumes: consume it with a warning and condition, or stop stamping it) and Create/CreatePods × spec.pauseTrue (a held attempt reports Creating; make the hold visible or let a committed attempt finish).
