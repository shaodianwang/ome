# Instance event vocabulary — completeness audit

Subject: `tests/rolloutspec/machines/instance/events.yaml` as audited (38 events, 6 families)
against one Instance of one OMENative Component as persisted in `InferenceReplica.status.instanceStatuses[]`.

Result: 163 inputs enumerated → covered 71 · MISSING 50 · out of scope 42.
The 50 unrepresented inputs collapse into 27 proposed events.

## Adoption

19 of the 27 proposals landed, taking the table to 57 events across 9 families
(pod, gang, endpoint, spec, config, coordination, timer, controller, apiserver).

Adopted as proposed: `pod.terminating`, `pod.unschedulable`,
`pod.containersNotReady`, `pod.phaseTerminal`, `pod.admitted`, `gang.podGroup`,
`endpoint.rotationIn`, `endpoint.rotationOut`, `spec.gangWidth`,
`spec.rollbackTarget`, `spec.policyTuning`, `api.invalid`, `api.throttled`,
`api.namespaceTerminating`, `timer.stuckPodGrace`, `timer.teardownDeadline`.

Adopted with a change:

- `config.lifecycleChanged` landed as `config.changed`, same variants.
- `ctrl.resync` landed without variants: the requeue that produced a tick is
  not observable from the Instance row, so the six cadences cannot be told
  apart in a cell.
- `timer.forceDeleteGrace` and `timer.nodeUnreachable` landed as one
  `timer.forceDelete` with variants `overdueSlack` and `nodeUnreachable`.
  Force-delete requires both clocks in sequence, so a separate event per clock
  would name two halves of one condition.
- `ctrl.ownerRotated` landed as the `spec.teardown[ownerRotated]` variant, per
  the rename note below.

Renames applied: family `operator` → `spec`; `timer.stepDeadline` →
`timer.operationDeadline`; `timer.progressDeadline` → `timer.migrationDeadline`;
the re-descriptions of `pod.containerRestart` and `pod.deleted`; the
`pod.nodeLost` and `spec.teardown` variant splits. `api.quotaDenied` is kept
as spelled: it is a desired-behavior event the code does not yet distinguish.

Not adopted: `pod.imageConverged`, `pod.servingGateCorrupt`,
`ctrl.generationChanged`, `timer.podGrace`, `timer.configTTL`,
`timer.migrationCapacityWindow`, and the `gate.deny[Budget]` split. Each names
an internal predicate or a sub-operation clock rather than an input the
Instance row can be observed to receive; they belong in the guard text of the
arrows they qualify.

The last rename note — declaring `timer.operationDeadline` not applicable in
`Migrate/CreateSurge`, `Migrate/SurgeTarget` and `Delete/Drain` — is also not
adopted. Those three states do stamp the clock; the escalation pass merely
skips them. An event the code ignores is a cell with an `ignore` outcome, not
a row the event cannot reach.

## MISSING (proposed additions)

| proposed id | family | variants | what it is | produced by | states where it matters |
|---|---|---|---|---|---|
| `spec.gangWidth` | spec | `grow`, `shrink`, `runnerSetChanged` | desired pod count per Instance changes without a revision change | `Runner.Size` is absent from `revision.DataPayload` (revision.go:77) but drives `InstancePlan.TotalPods()` (types/plan.go:177) and PodGroup `MinMember` | Ready, Create/CreatePods, all Update/*, Restart/Drain |
| `spec.rollbackTarget` | spec | `set`, `clear` | the roll target is overridden by a stored ControllerRevision, bypassing the rendered template | `Spec.Pacing.RollbackToRevision`, read at inferencereplica/reconciler.go:425 | Ready, Update/Surge, Update/InPlace, Update/Drain, Failed |
| `spec.policyTuning` | spec | `restartPolicy`, `migrationPolicy`, `instanceReadyTimeout`, `minReadySeconds`, `gracePeriod`, `markNotReady` | a `lifecycle.*` knob outside the pod template changes | `LifecycleSpec` (lifecycle_types.go:473) projected verbatim at irprojector.go:409; none feed the hash | every state — they arm timers, gate repair, and set PodGroup `ScheduleTimeoutSeconds` |
| `config.lifecycleChanged` | config | `updateRetry`, `stuckPodGrace`, `forceDelete`, `teardownDeadline`, `scaleDownInterval`, `autoMigrateBudget` | the operator `inferenceservice-config` lifecycle block changes behavior mid-flight | TTL-cached ConfigMap reads at inferencereplica/reconciler.go:650, 881, 933; `AutoMigrateMaxAttempts` at convert.go:189 | Failed, Create/CreatePods, all Update/*, Delete/Drain |
| `pod.terminating` | pod | `source`, `target` | `deletionTimestamp` appears while the pod still exists | watched edge at watches.go:215; every live-pod filter reads `DeletionTimestamp != nil` | Update/SurgeDrain, Update/SurgeDrainSettle, Update/Drain, Restart/Drain, Delete/Drain |
| `pod.admitted` | pod | — | the admission scheduling gate is removed (quota granted) | `PodAdmissionGated` gate-exit at deadline.go:91; sets `InstanceStatus.Admitted` and restarts the parked clock | Create/CreatePods, Update/Surge, Update/GangSurgeTarget, Migrate/SurgeTarget |
| `pod.phaseTerminal` | pod | `Failed`, `Succeeded`, `Unknown` | `pod.Status.Phase` reaches a terminal value | read at ops/restart.go:98, 439, 449 and types/termination.go:85 (`Failed` only; `Succeeded` / `Unknown` are unread) | Ready, Create/CreatePods, all Update/*, Restart/Drain |
| `pod.containersNotReady` | pod | `source`, `target` | `ContainersReady` regresses True→False on a live pod | watched edge at watches.go:224; consumed by `CountReadyPods`, `podSetFullyServing` | Ready, Update/Surge, Update/SurgeDrain, Update/InPlace |
| `pod.imageConverged` | pod | — | kubelet stamps the target image into `status.containerStatuses[*].Image` | `podRuntimeImagesMatch` (ops/update_imagediff.go:33) — the only proof an in-place roll finished | Update/InPlace |
| `pod.unschedulable` | pod | `source`, `target` | `PodScheduled=False` reason `Unschedulable`, incl. topology-spread / gang anti-affinity failure | kube-scheduler; nothing in `workload/` reads `PodScheduled` or the `FailedScheduling` Event | Create/CreatePods, Update/Surge, Update/GangSurgeTarget, Migrate/SurgeTarget |
| `pod.servingGateCorrupt` | pod | — | the `ome.io/serving` writer list fails to parse | `parseList` error at podreadiness/readiness.go:587; every Add/Remove on that pod then errors | Update/SurgeDrain, Update/InPlace, Update/Drain, Restart/Drain, Delete/Drain |
| `endpoint.rotationOut` | endpoint | `source`, `target` | the pod's EndpointSlice endpoint goes absent or `Ready!=true` | `drain.IsPodDrained` (drain.go:36); watched via `EndpointSliceToIR` (watches.go:76) | Update/SurgeDrain, Update/SurgeDrainSettle, Update/Drain, Restart/Drain, Delete/Drain |
| `endpoint.rotationIn` | endpoint | `source`, `target` | the pod appears Ready and non-Terminating in an EndpointSlice | `drain.EndpointAvailable` / `AvailablePodSet` (status_aggregate.go:108) — the real input to the Available counters | Ready, Update/Surge, Update/GangSurgeTarget, Migrate/SurgeTarget |
| `gang.podGroup` | gang | `Terminating`, `OwnershipConflict`, `Deleted`, `Unschedulable`, `ScheduleTimeout`, `PhaseFailed` | the Instance's PodGroup blocks or fails the gang | `ErrPodGroupTerminating` / `ErrPodGroupOwnershipConflict` (podgroup.go:66, 70); the last four have **no reader at all** — `.Status` is never read and `podGroupPredicate` drops status-only updates (watches.go:45) | Create/CreatePods, Update/GangSurgeTarget, Update/GangSurgeTargetCleanup, Migrate/SurgeTarget |
| `api.invalid` | apiserver | — | a write is rejected 422 (immutable field on an in-place patch, schema violation on a status write) | `IsInvalid` has zero occurrences in `workload/` or `inferencereplica/`; the error falls into blind exponential backoff | Update/InPlace, Update/Surge, Create/CreatePods, Failed |
| `api.throttled` | apiserver | `TooManyRequests`, `ServiceUnavailable` | the apiserver sheds the write | propagated verbatim out of `ops.Create` (create.go:909); `RetryAfterSeconds` is never honored | Create/CreatePods, Update/Surge, Update/GangSurgeTarget, Delete/Drain |
| `api.namespaceTerminating` | apiserver | — | a create is rejected because the namespace is terminating | `HasStatusCause(…, NamespaceTerminatingCause)` exists only on the ISVC side (coordination/services.go:366), never in `workload/` | Create/CreatePods, Update/Surge, Delete/Drain |
| `ctrl.generationChanged` | controller | — | the authoritative owner snapshot no longer matches the decision that produced the mutation batch; the whole batch is rejected and the pass must replan | `ErrStatusMutationPrecondition` (types/input.go:258), consumed at ~20 `ops/*` sites | every state with an in-flight operation |
| `ctrl.ownerRotated` | controller | `uidMismatch`, `recreated` | the owning IR is gone or came back under a new UID | `ErrStatusOwnerGone` (types/input.go:253); `ownerUID` salts the revision hash (revision.go:157), so a recreate restarts revision history and orphans the pods | every state |
| `ctrl.resync` | controller | `createPoll`, `updatePoll`, `restartPoll`, `migratePoll`, `ratioGatePoll`, `scaleDownPoll` | a level-triggered reconcile arrives with nothing changed | the six explicit requeue cadences (create.go:24, update.go:34, restart.go:23, migrate.go:30 = 5s; reconcile.go:43 = 3s; config `lifecycle.scaleDownRequeueInterval`). There is **no** informer `SyncPeriod` and no global resync — these requeues are the entire level-triggered surface | every state — it is the "nothing happened" cell of every row, and `planUnbackedDemotions` (plan_decision.go:315) and the scale-down wave only ever advance on it |
| `timer.stuckPodGrace` | timer | — | `lifecycle.stuckPodGracePeriod` elapses since pod `creationTimestamp`, arming the fast escalator | the stuck-pod evidence in `status_aggregate.go` — a pod held in a terminal kubelet waiting reason past the grace; fires **before** `timer.stepDeadline` and is a different clock with a different origin | Create/CreatePods, all Update/*, Restart/Drain |
| `timer.podGrace` | timer | — | the pod's own `terminationGracePeriodSeconds` elapses | set at admission (isvc defaulter:94); `DeletionTimestamp` already encodes it, and it is the baseline every "overdue" measurement starts from | Update/SurgeDrain, Update/Drain, Restart/Drain, Delete/Drain |
| `timer.forceDeleteGrace` | timer | — | `forceDelete.overdueSlack` elapses past a Terminating pod's deletion deadline | `ForceDeletePolicy` (types/input.go:330), force_delete.go:160; also defers a `Draining` migration record (migrate_expire.go:269) | Update/Drain, Restart/Drain, Delete/Drain, Migrate/CreateSurge |
| `timer.nodeUnreachable` | timer | — | `forceDelete.nodeUnreachableThreshold` elapses on the unreachable taint's `TimeAdded` or `NodeReady`'s `LastTransitionTime` | force_delete.go:202; the second, independent condition force-delete requires — `timer.forceDeleteGrace` alone never acts | Update/Drain, Restart/Drain, Delete/Drain |
| `timer.teardownDeadline` | timer | — | `lifecycle.teardown.deadline` elapses since the IR's `deletionTimestamp` | teardown.go:92; releases `TeardownFinalizer` and degrades the Instance to background GC mid-drain | Delete/Drain |
| `timer.configTTL` | timer | — | `ConfigCacheTTL` expires, so the next reconcile may observe different operator policy | `--config-cache-ttl` (reconciler.go:93) — the sole producer of an observable `config.lifecycleChanged` | every state |
| `timer.migrationCapacityWindow` | timer | `rateCap`, `statusTrim` | the trailing 1h `CapacityRateWindow` elapses | audit.go:469 — gates whether a new `spec.migrateRequest` is accepted, and when a terminal `status.migrations` row is dropped (migration_accept.go:547) | Migrate/CreateSurge, Migrate/SurgeTarget, Ready |

## Renames or splits worth making

- **family `operator` → `spec`.** Every id in the family is already spelled `spec.*`, and the writers are plural: the ISVC controller writes most of the spec, HPA/KEDA write `spec.replicas` through the `/scale` subresource (`+kubebuilder:subresource:scale`, inferencereplica_types.go:33), and the canary executor writes `spec.pacing.partition` / `spec.pacing.rollbackToRevision` (canary/dispatch.go:290). "operator" names one of four actors.
- **`timer.stepDeadline` is not a step deadline.** `Operation.Deadline` is stamped once at *operation* entry (update_status.go:111, 154, 208) and is never re-stamped on a step transition — `SurgeDrain` and `SurgeDrainSettle` update only `LastProgressAt` (update_status.go:230). One 30-minute budget covers Surge + SurgeDrain + SurgeDrainSettle together. Rename to `timer.opDeadline`; the API doc on `InstanceOperation.Deadline` (lifecycle_types.go:341) carries the same error and should be corrected with it.
- **`timer.progressDeadline` cites a field that does not exist.** `MigrationPolicy` has only `Mode` (lifecycle_types.go:651). The real clock is `MigrationStatus.Deadline` = accept time + `InstanceReadyTimeout` (inferencereplica_types.go:612, stamped at migration_accept.go:212). Rename to `timer.migrationDeadline` and re-source it.
- **`pod.containerRestart` is described against a field the code does not read.** watches.go:265 explicitly lists `RestartCount` among the fields *not* compared; the real signal is `cs.LastTerminationState.Terminated.FinishedAt.After(ReadySince)` (ops/restart.go:536). Redescribe as "a container terminated after the Instance last became Ready" — the `ReadySince` anchor is load-bearing and invisible in the current wording.
- **`pod.deleted` conflates two edges.** "disappears from the apiserver" is the *second* edge; the first (deletionTimestamp appears, pod still listed, excluded from every live count) is the one every drain step gates on. Split out `pod.terminating`.
- **`pod.nodeLost` needs the evidence variants.** force_delete.go:22-50 classifies node death into `NodeGone`, `UnreachableTaint`, `NodeNotReady`, plus the non-node blockers `ForeignFinalizers` and `Unscheduled`; only the first three are actionable, and each is time-qualified by `timer.nodeUnreachable`. One undifferentiated event cannot express "NotReady but not dead long enough".
- **`gate.deny[Budget]` merges two gates.** `EvaluateUpdateGate` picks `CheckSurge` *or* `CheckUnavailability` by strategy (gate_eval.go:110-118) and both report `Budget`; the workload-local budget raises the same gate from a different computation (reconcile.go:453, 468). Split to `Budget.Surge` / `Budget.Unavailability`, or at minimum record the per-Component vs cross-Component origin.
- **`api.quotaDenied` has no producer.** Nothing in `workload/` or `inferencereplica/` inspects `IsForbidden` on a write or matches "exceeded quota" — a quota rejection becomes an opaque `podCreateError` (create.go:842) and returns to controller-runtime backoff with no event, condition, or escalation. Either delete the event or generalize it to `api.createRejected`, the opaque class that genuinely reaches the machine.
- **`spec.teardown` should carry the abort variant.** IR deletion and "owner UID no longer matches" have the same shape but different causes; the second is `ErrStatusOwnerGone`, proposed above as `ctrl.ownerRotated`.
- **states.yaml gap implied by the timer audit.** `Migrate/CreateSurge`, `Migrate/SurgeTarget`, and `Delete/Drain` all stamp `Operation.Deadline` (migrate.go:1286, delete_batch.go:267) but `escalateFromEvidence` unconditionally skips those operation types (escalation.go:114-119), so the clock is never consumed. Those three states should declare `timer.stepDeadline` under `not_applicable`; today only the non-operation states do.

## Full mapping

### 1. Spec inputs

| input | produced by | classification | event id / reason |
|---|---|---|---|
| `spec.parentRef` | ISVC controller at create | out of scope | immutable after create (inferencereplica_types.go:66) |
| `spec.component` | ISVC controller at create | out of scope | immutable; a move requires recreating the IR |
| `spec.replicas` increase | irprojector `desiredReplicas` (projector.go:494); HPA/KEDA via `/scale` | covered | `spec.replicasUp` |
| `spec.replicas` decrease | same | covered | `spec.replicasDown` |
| `spec.minReadySeconds` | `minReadySecondsFromComponentExt` (projector.go:410) | MISSING | `spec.policyTuning[minReadySeconds]` — re-arms `timer.minReady`; not hashed |
| `spec.topologyKey` | projector.go:421 | covered | `spec.revision` (folded in by `HashWithWorkerAndTopology`, revision.go:194) — also re-keys the PodGroup |
| `spec.topologySpread` / `topologySpreadKey` | projector.go:422-423 | covered | `spec.revision` (rendered as a `topologySpreadConstraint` inside the template) |
| `spec.pairingProtocol` | projector.go:431 | covered | `spec.revision` (`DataPayload.PairingProtocol`, revision.go:90) + `gate.deny[Pairing]` |
| `spec.runners[].template` | `runnersFromParams` (projector.go:408) | covered | `spec.revision` |
| `spec.runners[].size` | same | MISSING | `spec.gangWidth` — absent from `DataPayload`, so no hash change |
| a Runner added/removed (single↔gang) | same | MISSING | `spec.gangWidth[runnerSetChanged]` — `workerPodSpec` is `omitempty`, so single→multi does mint a revision, but the shape flip itself is not expressible |
| `lifecycle.updateStrategy.type` | `lifecycleFromComponentExt` (projector.go:409) | covered | `spec.strategy` |
| `rollingUpdate.partition` | same | covered | `spec.partition` |
| `rollingUpdate.maxUnavailable` / `maxSurge` | same | out of scope | Component-level budget; reaches the Instance only as `gate.deny[Budget]` (reconcile.go:453) |
| `inPlaceUpdateStrategy.gracePeriodSeconds` | same | MISSING | `spec.policyTuning[gracePeriod]` — sets the `Update/SurgeDrainSettle` window (and is *not* honored on the in-place path at all; see §6) |
| `inPlaceUpdateStrategy.markNotReadyDuringLifecycle` | same | MISSING | `spec.policyTuning[markNotReady]` — decides whether `pod.servingOff` happens at all |
| `lifecycle.restartPolicy` | same | MISSING | `spec.policyTuning[restartPolicy]` — gates `instanceLostGangMember` (ops/restart.go:136) |
| `lifecycle.readyPolicy` | same | out of scope | `None` is admission-restricted to single-pod, where it is behaviorally identical to `AllPodReady` (lifecycle_types.go:640) |
| `lifecycle.instanceReadyTimeout` | same | MISSING | `spec.policyTuning[instanceReadyTimeout]` — sets `Operation.Deadline`, the migration deadline, and PodGroup `ScheduleTimeoutSeconds` (podgroup.go:122) |
| `lifecycle.migrationPolicy.mode` | same | MISSING | `spec.policyTuning[migrationPolicy]` — gates disposition branch 2 (`migrationModeAllowsRelocation`, disposition.go:278) |
| `spec.paused=true` (Recover) | projector.go:412 | covered | `spec.pauseTrue` — also parks `Operation.Deadline` (status.go:333) |
| `spec.pauseMode=Freeze` | projector.go:415 | covered | `spec.pauseFreeze` |
| `spec.paused` cleared | projector.go:412 | covered | `spec.unpause` |
| `spec.pacing.partition` | coordination / canary | covered | `spec.partition` |
| `spec.pacing.maxUnavailable` | coordination | out of scope | budget; surfaces as `gate.deny[Budget]` |
| `spec.pacing.rollbackToRevision` set/cleared | canary/dispatch.go:290 | MISSING | `spec.rollbackTarget` |
| `spec.autoscaler` | projector.go:444 | out of scope | renders an HPA/ScaledObject; the Instance only ever sees the resulting `spec.replicas*` |
| `spec.revisionHistoryLimit` | `revisionHistoryLimitFromISVC` (projector.go:417) | out of scope | prunes non-live ControllerRevisions only; live revisions are never deleted |
| annotation `ome.io/migration-request-v1-<uuid>` | operator / ISVC | covered | `spec.migrateRequest` |
| annotation `ome.io/revision-excluded-annotation-keys` | irprojector | covered | `spec.revision` or `spec.annotationOnly`, depending on which keys move |
| template labels/annotations excluded from the hash (migration prefix, `rollout-paused`, `rollout-promote`, `rollout-rollback`, `kueue.x-k8s.io/queue-name`, `kueue.x-k8s.io/priority-class`, `LocalQueue`, placement markers) | revision.go:324-427 | covered | `spec.annotationOnly` |
| `metadata.deletionTimestamp` on the IR | operator / ISVC GC | covered | `spec.teardown` |
| IR deleted and recreated at a new UID | operator | MISSING | `ctrl.ownerRotated[recreated]` — `ownerUID` salts the hash (revision.go:157) |
| operator config `lifecycle.updateRetry` | reconciler.go:650 | MISSING | `config.lifecycleChanged[updateRetry]` |
| operator config `lifecycle.stuckPodGracePeriod` | reconciler.go:881 | MISSING | `config.lifecycleChanged[stuckPodGrace]` |
| operator config `lifecycle.forceDelete` | reconciler.go:933 | MISSING | `config.lifecycleChanged[forceDelete]` |
| operator config `lifecycle.teardown.deadline` / `lifecycle.scaleDownRequeueInterval` | teardown.go:92, reconciler.go:105 | MISSING | `config.lifecycleChanged[teardownDeadline, scaleDownInterval]` |
| operator config `lifecycle.autoMigrate.maxAttempts` | convert.go:189 | MISSING | `config.lifecycleChanged[autoMigrateBudget]` |

### 2. Pod status inputs

| input | produced by | classification | event id / reason |
|---|---|---|---|
| `phase=Pending` | kubelet / scheduler | out of scope | the zero state; nothing branches on it |
| `phase=Running` | kubelet | out of scope | never read — readiness comes from conditions, not phase |
| `phase=Succeeded` | kubelet | MISSING | `pod.phaseTerminal[Succeeded]` — unread; leaves the Instance live-but-never-ready until the op deadline |
| `phase=Failed` | kubelet | MISSING | `pod.phaseTerminal[Failed]` — read at ops/restart.go:98 and types/termination.go:85, but has no event |
| `phase=Unknown` | apiserver on node partition | MISSING | `pod.phaseTerminal[Unknown]` — unread |
| `PodScheduled=True` / `spec.nodeName` set | kube-scheduler | covered | `pod.scheduled` (`CountScheduledPods`, status_aggregate.go:161) |
| `PodScheduled=False` reason `Unschedulable` / `FailedScheduling` Event | kube-scheduler | MISSING | `pod.unschedulable` — no reader |
| `Initialized` condition | kubelet | out of scope | never read; init failures surface through `InitContainerStatuses` instead |
| `ContainersReady=True` | kubelet | covered | `pod.containersReady` (`IsContainersReady`, readiness.go:337) |
| `ContainersReady` True→False | kubelet | MISSING | `pod.containersNotReady` — watched (watches.go:224) but has no event |
| `PodReady=True` | kubelet (gates AND'ed) | covered | `pod.ready` (`IsPodReady`, readiness.go:355) |
| `ome.io/serving` gate → True | this controller | covered | `pod.servingOn` |
| `ome.io/serving` gate → False | this controller | covered | `pod.servingOff` |
| `ome.io/serving` writer list unparsable | corruption / foreign writer | MISSING | `pod.servingGateCorrupt` (readiness.go:587) |
| `DisruptionTarget` condition | eviction API / node drain | out of scope | never read; the effect reaches the machine as `pod.evicted` / `pod.terminating` |
| readiness gate other than `ome.io/serving` | pod template | out of scope | only `ome.io/serving` is appended by Render; others are opaque inputs to kubelet's `PodReady` |
| container `Waiting` reason in the terminal set | kubelet | covered | `pod.waiting` — its 7 non-`DeadlineExceeded` variants match `terminalPullFailureReasons` exactly (status_aggregate.go:35), and the "first four" note matches `workloadCausedWaitingReasons` exactly (disposition.go:65) |
| init-container `Waiting` reason | kubelet | covered | `pod.waiting` — `InitContainerStatuses` is scanned by the same classifiers |
| transient `Waiting` (`PodInitializing`, `ContainerCreating`, `ImagePulling`) | kubelet | out of scope | deliberately unclassified; `timer.stuckPodGrace` exists to skip them |
| `State.Terminated` (OOMKilled / Error / ContainerCannotRun, exit code) | kubelet | covered | `pod.containerRestart` (+ `pod.phaseTerminal[Failed]` when the pod's restartPolicy does not restart) — read by `types.PodTermination` (termination.go:76) |
| `LastTerminationState.Terminated.FinishedAt` after `ReadySince` | kubelet | covered | `pod.containerRestart` — but see the rename note; this, not `restartCount`, is the signal |
| `restartCount` | kubelet | out of scope | explicitly excluded from the watch diff and never read (watches.go:265) |
| `status.containerStatuses[*].Image` flips to the target | kubelet | MISSING | `pod.imageConverged` — `podRuntimeImagesMatch` (update_imagediff.go:33) is the sole completion proof for in-place |
| `deletionTimestamp` appears | any deleter | MISSING | `pod.terminating` |
| pod disappears from the apiserver | kubelet / GC | covered | `pod.deleted` |
| deletionTimestamp past grace, pod still present | foreign finalizer / dead kubelet | covered | `pod.stuckTerminating` |
| same-name pod under a new UID | recreate race | covered | `pod.uidRotated` (`ErrPodIdentityChanged`, readiness.go:88) |
| `spec.nodeName` changes after bind | — | out of scope | set-once by the scheduler; the watch diff on it only ever fires the bind |
| scheduling gate present | Kueue / admission plane | covered | `pod.schedulingGated` (`PodAdmissionGated`, deadline.go:58) |
| scheduling gate removed | Kueue admission | MISSING | `pod.admitted` — the gate-exit edge that restarts the parked deadline (deadline.go:91) |
| eviction API / kubelet pressure eviction | kubelet / node drain | covered | `pod.evicted` |
| scheduler preemption | kube-scheduler | covered | `pod.deleted` (preemption deletes the victim) |
| OwnerReferences mutated on a live pod | — | out of scope | no in-tree writer strips a controller ref, and adoption is not implemented |
| routing labels (`revision-hash`, `instance-index`, `managed-by`) mutated on a live pod | — | out of scope | controller-owned at create; diffed by the watch only to catch the controller's own writes |

### 3. Gang and scheduler inputs

| input | produced by | classification | event id / reason |
|---|---|---|---|
| PodGroup `status.phase` (`Pending`…`Failed`) | scheduler-plugins | MISSING | `gang.podGroup[PhaseFailed]` — `.Status` has **zero** readers, and `podGroupPredicate` (watches.go:45) admits only generation/meta changes, so a status update never even enqueues |
| PodGroup `Unschedulable` condition | scheduler-plugins | MISSING | `gang.podGroup[Unschedulable]` — same |
| PodGroup `ScheduleTimeoutSeconds` expiry (co-scheduling timeout) | scheduler-plugins | MISSING | `gang.podGroup[ScheduleTimeout]` — the controller *writes* the clamped timeout (podgroup.go:122) but never observes it firing |
| deterministic PodGroup name occupied by a terminating object | prior Instance teardown | MISSING | `gang.podGroup[Terminating]` (`ErrPodGroupTerminating`, podgroup.go:66) |
| PodGroup owned by a different UID | recreated owner | MISSING | `gang.podGroup[OwnershipConflict]` (`ErrPodGroupOwnershipConflict`, podgroup.go:70) |
| PodGroup deleted out from under the Instance | operator | MISSING | `gang.podGroup[Deleted]` — watched and re-ensured, but not expressible |
| topology-spread / gang anti-affinity unsatisfiable | kube-scheduler | MISSING | `pod.unschedulable` — surfaces only as an unschedulable pod |
| partial gang-member loss | any deleter | covered | derived from `pod.deleted` by `instanceLostGangMember` (ops/restart.go:132) |
| PodGroup CRD absent from the cluster | cluster admin | out of scope | resolved once at manager start (`GangSchedulingAvailable`, manager.go:91); a change is a `ctrl.crash` |
| scheduler-plugins CRD removed at runtime | cluster admin | out of scope | tolerated as "gang unavailable" (`IsNoMatchError` triple, gang/inventory.go:66); never a lifecycle blocker |

### 4. Node and cluster inputs

| input | produced by | classification | event id / reason |
|---|---|---|---|
| Node object deleted | cluster autoscaler | covered | `pod.nodeLost` — `evidenceNodeGone` (force_delete.go:44); split-variant note above |
| `NodeReady=False`/`Unknown` past the threshold | node controller | covered | `pod.nodeLost` — `evidenceNodeNotReady`, time-qualified by `timer.nodeUnreachable` |
| `node.kubernetes.io/unreachable` taint past the threshold | node controller | covered | `pod.nodeLost` — `evidenceNodeUnreachableTaint` |
| node unhealthy but not dead long enough | node controller | covered | the absence of `pod.nodeLost`; `evidenceNodeNotDeadLongEnough` is a non-transition that schedules a recheck |
| foreign finalizers pinning a Terminating pod | any finalizer owner | covered | `pod.stuckTerminating` — `evidenceForeignFinalizers` is the non-actionable branch |
| node cordon / drain | operator | covered | reaches the Instance as `pod.evicted` |
| other taints added to the node | operator | out of scope | affects only future scheduling; no reader |
| ResourceQuota denial on pod create | apiserver | out of scope as spelled | `api.quotaDenied` exists but **has no producer** — see the rename note |
| Namespace `Terminating` | operator | MISSING | `api.namespaceTerminating` |
| PodDisruptionBudget eviction | eviction API | out of scope | OMENative creates no PDB; the effect arrives as `pod.evicted` |
| LimitRange / mutating webhook altering the created pod | apiserver | out of scope | the render is the hash subject; post-admission mutation is invisible to the revision check |

### 5. Apiserver responses the code handles

| input | produced by | classification | event id / reason |
|---|---|---|---|
| 409 Conflict on a status write (after `RetryOnConflict` exhaustion) | apiserver | covered | `api.conflict` (reconciler.go:264, 480, 513; status.go:276) |
| 409 Conflict on a UID-preconditioned pod delete | apiserver | covered | `api.conflict` — treated identically to NotFound (delete_batch.go:426, force_delete.go:291) |
| 409 AlreadyExists on pod create | apiserver | covered | `api.alreadyExists` (create.go:905) |
| 409 AlreadyExists on ControllerRevision create | apiserver | covered | `api.alreadyExists` → re-Get + collision gating (revision.go:653) |
| 404 NotFound on pod delete | apiserver | covered | `api.notFound` (restart.go:329, update_recreate.go:111, update_gang.go:322, update_surge.go:263, migrate.go:632, cleanup.go:148) |
| 404 NotFound on pod re-read before an in-place patch | apiserver | covered | `api.notFound` (update_inplace.go:125) |
| 404 NotFound on a ControllerRevision get | apiserver | covered | `api.notFound` (revision.go:632, update.go:289, reconciler.go:441) |
| 404 NotFound on the owner IR get / status write | apiserver | covered | `api.notFound` (reconciler.go:215, 260; status.go:153, 259) |
| audit-ledger ConfigMap CAS conflict | synthesized `NewConflict` (audit.go:341, 355) | covered | `api.conflict` |
| 403 Forbidden | apiserver | out of scope as handled | the sole `IsForbidden` site (reconciler.go:1307) guards the parent-ISVC *read* and is folded into "no parent" |
| 422 Invalid | apiserver / webhook | MISSING | `api.invalid` — zero occurrences; blind backoff |
| 429 TooManyRequests | apiserver | MISSING | `api.throttled[TooManyRequests]` — propagated verbatim (asserted in create_batch_test.go:1001) |
| 503 ServiceUnavailable | apiserver | MISSING | `api.throttled[ServiceUnavailable]` |
| 410 Gone / `IsResourceExpired` | apiserver | out of scope | handled entirely inside controller-runtime's informer relist; never surfaces to this machine |
| `NamespaceTerminatingCause` on a create | apiserver | MISSING | `api.namespaceTerminating` |
| webhook rejection on a status write | IR validating webhook | out of scope | the webhook is `create;update` on the main resource; status goes through `/status` and is not gated (validator.go:38) |
| owner generation changed mid-pass (whole batch rejected) | this controller's own precondition | MISSING | `ctrl.generationChanged` (`ErrStatusMutationPrecondition`, types/input.go:258) |
| owner UID mismatch on a status write | this controller's own precondition | MISSING | `ctrl.ownerRotated[uidMismatch]` (`ErrStatusOwnerGone`, types/input.go:253) |

### 6. Timers

| input | produced by | classification | event id / reason |
|---|---|---|---|
| `Operation.Deadline` elapses (op `StartedAt` + `InstanceReadyTimeout`, default 30m at plan.go:152) | `operationDeadlinePassed` (deadline.go:25) | covered, misnamed | `timer.stepDeadline` — it is per-operation, not per-step; see the rename note |
| Create arm of that deadline | create.go:949, stamped once and never re-stamped on retry | covered | `timer.stepDeadline` |
| Update arms (InPlace / Recreate / Surge / gang source / gang target) | update_status.go:111, 154, 208, 336, 355, 486 | covered | `timer.stepDeadline` |
| Restart arm | restart.go:423 | covered | `timer.stepDeadline` |
| Migrate arm (migrate.go:1286) | stamped but never consumed — escalation skips `Type=Migrate` | out of scope | a dead clock; the authority is the migration record's own deadline |
| Delete arm (delete_batch.go:267) | stamped but never consumed — escalation skips `Type=Delete` | out of scope | a dead clock; DeleteBatch owns progress |
| deadline parked to zero while gated or paused | `ReconcileGatedDeadlines` (deadline.go:87); `held` folded from Kueue gate on own pods, gate on the `SurgeIndex` bucket, or `plan.Paused` (status.go:333) | covered | armed by `pod.schedulingGated` / `spec.pauseTrue`; the un-park edge is the MISSING `pod.admitted` |
| `minReadySeconds` window after `PodReady` | `IsPodAvailable` (readiness.go:377) | covered | `timer.minReady` |
| next-availability wake folded into `RequeueAfter` | `CountAvailablePods` (status_aggregate.go:184), folded at reconciler.go:524 | covered | `timer.minReady` — it is that window's wake |
| `retryBlocks[].nextRetryAt` (exponential backoff from `lifecycle.updateRetry`) | types/retryblock.go:52; gate at retryblock_gate.go:44 | covered | `timer.retryAt` |
| create/delete expectation TTL (hardcoded 2m) | types/expectations.go:13 | covered | `timer.expectationsTTL` |
| surge drain-settle window (`gracePeriodSeconds`, default 30s) measured from `LastProgressAt` | update_surge.go:542 | covered | `timer.drainSettle` |
| in-place grace between mark-not-ready and mutation | **not implemented** — update_inplace.go:57 gates only on `drain.IsPodDrained` | out of scope | the API doc (lifecycle_types.go:618) promises a wait that no code performs; the field's only consumer is the surge settle above |
| migration record `Deadline` (accept + `InstanceReadyTimeout`) | migration_accept.go:212; consumed by `ExpireMigrations` (migrate_expire.go:106) | covered, mis-sourced | `timer.progressDeadline` — its stated source `migrationPolicy.progressDeadlineSeconds` **does not exist**; `MigrationPolicy` carries only `Mode` |
| stuck-pod grace since pod `creationTimestamp` | `lifecycle.stuckPodGracePeriod` → the stuck-pod evidence in `status_aggregate.go` | MISSING | `timer.stuckPodGrace` — arms the fast escalator on a different clock and origin than `timer.stepDeadline` |
| pod `terminationGracePeriodSeconds` | admission defaulter (isvc defaults:94) | MISSING | `timer.podGrace` — the baseline every "overdue" measurement starts from |
| `forceDelete.overdueSlack` past a Terminating pod's deletion deadline | force_delete.go:160 | MISSING | `timer.forceDeleteGrace` |
| `forceDelete.nodeUnreachableThreshold` on taint `TimeAdded` / `NodeReady` transition | force_delete.go:202 | MISSING | `timer.nodeUnreachable` — the second, independent precondition force-delete requires |
| migration-expiry deferral of a wedged `Draining` source (reuses `overdueSlack`) | migrate_expire.go:269 | MISSING | `timer.forceDeleteGrace` |
| `lifecycle.teardown.deadline` since the IR's `deletionTimestamp` | teardown.go:92 | MISSING | `timer.teardownDeadline` — releases the finalizer and degrades to background GC |
| `ConfigCacheTTL` expiry | reconciler.go:93 | MISSING | `timer.configTTL` |
| 1h `CapacityRateWindow` (migration rate cap + terminal-record trim) | audit.go:469; migration_accept.go:547 | MISSING | `timer.migrationCapacityWindow` |
| the six requeue cadences (create/update/restart/migrate 5s, ratio gate 3s, scale-down from config) | create.go:24, update.go:34, restart.go:23, migrate.go:30, reconcile.go:43, reconciler.go:105 | MISSING | `ctrl.resync` — a level-triggered tick, not a deadline |
| exact force-delete wake at the evidence boundary | delete_batch.go:150 | covered | `timer.forceDeleteGrace` / `timer.nodeUnreachable` — the wake is those deadlines, scheduled precisely |
| global informer resync / `SyncPeriod` | — | out of scope | none is configured; the requeues above are the entire level-triggered surface |
| RetryBlock supersede prune | retryblock_prune.go:36 | out of scope | liveness-based, not time-based; releases a hold as a consequence of `spec.revision` |
| RetryBlock history cap (hardcoded 3) | convert.go:1030 | out of scope | count-based status trimming; no Instance effect |
| audit ledger ring buffer (hardcoded 200) | audit.go:247 | out of scope | count-based; history only |
| `autoMigrate.maxAttempts` budget | disposition.go:421 | out of scope | count-based, not a timer; the budget itself is `config.lifecycleChanged[autoMigrateBudget]` |
| revision retention sweep | retention.go:15 | out of scope | count-based; live revisions are never touched |
| kubelet CrashLoopBackOff backoff (~5m cap) | kubelet | out of scope | not controller-observable; its steady state is seen as `pod.waiting[CrashLoopBackOff]` |

### 7. Controller lifecycle

| input | produced by | classification | event id / reason |
|---|---|---|---|
| manager restart | process | covered | `ctrl.crash` |
| leader-election loss | lease expiry | covered | `ctrl.crash` — observationally identical (expectations and memo caches die with the process) |
| cached reader lags the controller's own write | controller-runtime cache | covered | `ctrl.staleRead` |
| level-triggered reconcile with no change | the six explicit requeue cadences (there is no `SyncPeriod`) | MISSING | `ctrl.resync` |
| owner generation changed between decision and write | this controller | MISSING | `ctrl.generationChanged` |
| the IR is deleted | operator / ISVC GC | covered | `spec.teardown` |
| the IR is recreated at a new UID | operator | MISSING | `ctrl.ownerRotated[recreated]` |
| webhook rejects a status write | — | out of scope | the status subresource is not webhook-gated (validator.go:38) |
| CRD version conversion | — | out of scope | `v1beta1` is the only served version; no conversion webhook |
| `maybeNoGangSchedulerSeen` process cache reset | process restart (gang.go:26) | out of scope | suppresses a duplicate warning Event only; no state effect |

### 8. Cross-component inputs

| input | produced by | classification | event id / reason |
|---|---|---|---|
| update gate admits | `EvaluateUpdateGate` (gate_eval.go:123) | covered | `gate.allow` |
| gate denies — no pinned plan | `gateCtx.Hold` (gate_eval.go:74) | covered | `gate.deny[Plan]` |
| gate denies — P/D pair floor | `CheckPairing` (gate_eval.go:81) | covered | `gate.deny[Pairing]` |
| gate denies — RatioBalanced | `CheckRatio` (gate_eval.go:98) | covered | `gate.deny[Ratio]` |
| gate denies — surge budget | `CheckSurge` (gate_eval.go:111) | covered | `gate.deny[Budget]` — split note above |
| gate denies — unavailability budget | `CheckUnavailability` (gate_eval.go:115) | covered | `gate.deny[Budget]` — same variant, different gate |
| gate denies — Sequential turn / soak window | `CheckSequential` (gate_eval.go:120) | covered | `gate.deny[Sequential]` |
| denied by a same-target RetryBlock in Backoff | retryblock_gate.go | covered | `gate.deny[RetryBlock]` |
| denied by an exhausted RetryBlock | same | covered | `gate.deny[Held]` |
| workload-local budget denial | reconcile.go:453, 468 | covered | `gate.deny[Budget]` — produced by a different computation than the coordination gate |
| operator release of a Held block | release_held.go | covered | `timer.retryAt` / `gate.allow` on the next pass |
| `RatioGateBypassed` for in-place strategies | gate_eval.go:102 | out of scope | an event + metric only; the Instance observes the resulting `gate.allow` |
| partition (canary hold) | coordination / canary | covered | `spec.partition` |
| pacing `maxSurge` / `maxUnavailable` | coordination | out of scope | folded into `gate.deny[Budget]` before it reaches the Instance |
| pairing-protocol change | ISVC `spec.rollout.pairingProtocol` | covered | `spec.revision` + `gate.deny[Pairing]` |
| `RollbackToRevision` | canary/dispatch.go:290 | MISSING | `spec.rollbackTarget` |
| canary `rollout-promote` / `rollout-rollback` verbs | operator on the ISVC | out of scope | hash-excluded (revision.go:404); reach the Instance only as partition / rollbackTarget |
| `rollout-paused` annotation | operator on the ISVC | out of scope | hash-excluded; reaches the Instance as `spec.pauseTrue` / `spec.pauseFreeze` |
| traffic weights (`status.traffic` → HTTPRoute) | ISVC controller | out of scope | Service-level routing; no per-Instance effect |
| per-revision routed Service convergence | kube-proxy / EndpointSlice controller | MISSING | `endpoint.rotationIn` / `endpoint.rotationOut` |
| `CoordinationGroupRef` binding change | coordination reconciler | out of scope | a back-reference only; the behavioral effect is the gate verdict |
