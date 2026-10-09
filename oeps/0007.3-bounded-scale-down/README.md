# OEP-0007.3: Bounded, gang-aware OMENative scale-down

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Failure mode: one desired-count change becomes thousands of full-object operations](#failure-mode-one-desired-count-change-becomes-thousands-of-full-object-operations)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Terminology](#terminology)
- [Proposal](#proposal)
  - [The contract](#the-contract)
  - [Configuration](#configuration)
  - [Wave admission](#wave-admission)
  - [Durable deletion and replica rebound](#durable-deletion-and-replica-rebound)
  - [State model](#state-model)
  - [Resource ordering](#resource-ordering)
  - [Teardown](#teardown)
- [Design Details](#design-details)
  - [Current execution path and measured cost](#current-execution-path-and-measured-cost)
  - [Proposed execution path](#proposed-execution-path)
  - [One authoritative Pod observation](#one-authoritative-pod-observation)
  - [Batched InferenceReplica status](#batched-inferencereplica-status)
  - [Indexed drain observation](#indexed-drain-observation)
  - [PodGroup ownership](#podgroup-ownership)
  - [Shared Instance finalization](#shared-instance-finalization)
  - [Concurrency and the admission linearization point](#concurrency-and-the-admission-linearization-point)
  - [Failure semantics](#failure-semantics)
  - [Interactions with other lifecycle features](#interactions-with-other-lifecycle-features)
  - [Observability](#observability)
  - [Compatibility and rollout](#compatibility-and-rollout)
- [Implementation Plan](#implementation-plan)
- [Test Plan](#test-plan)
  - [Selection and convergence](#selection-and-convergence)
  - [Configuration and observability](#configuration-and-observability)
  - [API-operation bounds](#api-operation-bounds)
  - [Ordering and failure injection](#ordering-and-failure-injection)
  - [Concurrency](#concurrency)
  - [Gang and PodGroup lifecycle](#gang-and-podgroup-lifecycle)
  - [Teardown and adjacent lifecycle behavior](#teardown-and-adjacent-lifecycle-behavior)
  - [KIND and performance tests](#kind-and-performance-tests)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

An OMENative `InferenceReplica` (IR) manages Pods directly. Today, reducing an
IR from 2,000 replicas to 1 asks one reconcile to drive all 1,999 extra
Instances through deletion. The controller repeatedly lists the same Component
Pods, writes the complete IR status once per Instance transition, and performs
unbounded Pod and PodGroup effects. The workload is eventually correct, but the
control-plane burst can make the Kubernetes API server or etcd reject requests.

This OEP makes scale-down a **durably admitted, gang-aware wave**:

1. The manager reads optional `lifecycle.scaleDownPodBatchSize` and
   `lifecycle.scaleDownRequeueInterval` settings from the existing
   `inferenceservice-config` ConfigMap at startup.
2. One authoritative Component Pod list supplies the cost and Pod set for every
   scale-down candidate in the reconcile.
3. The controller resumes admitted deletions first. It admits a deterministic
   prefix of new extra Instances only when no DeleteOwned Instance remains; a
   multi-Pod gang is indivisible.
4. One IR status write commits every newly admitted `Deleting` intent before
   any Pod or PodGroup effect. That write is the admission linearization point.
5. The controller shares one indexed drain observation across the wave, deletes
   a gang's PodGroup only after its Pods are gone, and removes completed
   Instance statuses in one status write.

The proposal does **not** introduce another CRD. The hierarchy remains
`InferenceService -> InferenceReplica -> Pod`; the IR remains the durable
ReplicaSet-like owner of per-Instance lifecycle.

An absent scale-down field preserves today's unbounded **selection**: all
eligible extras may be admitted together. A configured field intentionally
changes timing: fresh deferred extras receive no scale-down effects and normally
continue serving until a later wave. That is a capacity-release tradeoff, not a
zero-behavior refactor.

This change reduces repeated reads and writes; it does not make an individual
IR status update smaller. An IR that already exceeds the API server or etcd
object-size limit still needs the separate status-compaction work.

## Motivation

### Failure mode: one desired-count change becomes thousands of full-object operations

The current path is status-driven and serial by Instance:

```text
IR reconcile
  -> EnsurePodGroupsWithTopology
  -> workload.Plan: ActionScaleDown(all extra indices)
  -> deleteExtraInstances
       -> Delete(instance 1)
       -> Delete(instance 2)
       -> ...
       -> Delete(instance 1999)
```

Each `Delete(instance)` performs a live Component Pod `LIST` and filters it to
one index, writes `Phase=Deleting` through a full IR status update, patches
serving readiness, creates a per-Instance drain batcher, and issues Pod deletes.
Once the Pods disappear, another full IR status update removes that one
Instance entry.

For a 2,000 -> 1 scale-down, this produces the following order-of-magnitude
work before retries, watches, and etcd write-ahead-log amplification:

| Shape | Current work in one logical scale-down phase |
| --- | ---: |
| Single-Pod Instances | 1,999 Component Pod lists; about 4 million repeatedly decoded Pod objects |
| Eight-Pod gangs | 1,999 Component Pod lists; about 32 million repeatedly decoded Pod objects |
| IR status | Up to 1,999 full-object `Deleting` writes plus 1,999 shrinking removal writes |
| Pod effects for eight-Pod gangs | Up to 15,992 serving-condition patches and 15,992 deletes |
| PodGroup effects for eight-Pod gangs | Up to 1,999 deletes before Pod drain begins |

A measured 1,000-Instance IR status is approximately 505 KiB; a comparable
2,000-Instance status is approximately 1 MiB. On that shape, the sequential
`Deleting` and removal writes carry roughly 3 GiB of IR status request bodies.
This estimate excludes responses, informer fan-out, retries, and etcd storage
amplification. The exact total varies with conditions, node lists, failures, and
migrations; the scaling law does not: the controller writes an O(N)-sized
object O(N) times.

Raising controller-runtime client QPS or Burst does not fix this failure mode.
It only lets OME deliver the same amplification to the API server faster.

Gang scale-down has a second correctness problem. The top-level PodGroup pass
deletes every extra PodGroup **before** the ordinary Pod drain pipeline runs.
For 2,000 -> 1, that is an unbounded PodGroup delete burst and contradicts the
documented ownership rule that a drained Instance loses its PodGroup. Other
terminal Instance-removal paths can remove status before deleting the
PodGroup, leaving no durable Instance record from which cleanup can resume.

### Goals

1. **Bound destructive work by Pod cost, without splitting a gang.** A budget
   of 100 admits at most twelve fully populated eight-Pod gangs (96 Pods); it
   never admits half of the thirteenth gang. A partially missing gang costs its
   current live Pod count.
2. **Make the bound durable across reconciles and restarts.** Repeated Pod or
   status events must not admit every remaining extra Instance before the first
   wave completes.
3. **Reduce repeated observation from O(N x P) to O(P).** One authoritative
   Component Pod list is bucketed by Instance and reused for the whole pass.
4. **Bound lifecycle status writes per pass.** One write admits new work and one
   write removes all completions observed in that pass; completion timing may
   require more than one pass per wave.
5. **Preserve lifecycle ordering.** Durable `Deleting` intent precedes every
   serving-gate, Pod, and PodGroup mutation; every gang leaves traffic and drains
   before any member is deleted.
6. **Make scale-down safe under stale caches and replica rebound.** A stale
   reconcile must not delete newly desired Pods, and an already committed
   deletion must not be silently abandoned.
7. **Align PodGroup lifetime with the delete owner.** Scale-down deletes an
   Instance's PodGroup after its Pods are gone and before forgetting the
   Instance status.
8. **Apply the same control to normal owner teardown.** Deleting an IR with
   thousands of Instances must not bypass the bound before its existing
   deadline escape and reproduce the same burst.
9. **Preserve the current resource model.** No new CRD or intermediate workload
   object.

### Non-Goals

1. **Faster scale-down.** The configured limit deliberately trades release
   latency for bounded control-plane load. An operator needing immediate release
   may omit the field or configure a larger value.
2. **Cross-Component proportional scale-down.** This OEP bounds each IR's
   destructive work; it does not add a new engine/decoder ratio coordinator.
3. **Autoscaler policy.** KEDA, HPA, manual `/scale`, and proportional policy
   continue deciding the desired replica count. This OEP only converges the
   current population toward that count.
4. **Orphan adoption.** A Pod whose Instance index has no InstanceStatus is an
   adjacent correctness problem. This proposal does not silently adopt or delete
   it during normal scale-down or normal teardown. Empty owned PodGroups are
   deleted during teardown because the owner's `deletionTimestamp` already
   supplies durable delete intent.
5. **IR status compaction.** Deriving live counters from Pods reduces terminal
   object size, but it is a separate design. This OEP reduces write count while
   preserving the existing status contract.
6. **Per-IR user API.** The first version uses the operator ConfigMap, matching
   scale-up pacing. It does not add a field to `InferenceReplica.spec` or
   `InferenceService.spec`.
7. **Normal scale-to-zero.** The current IR projection and planner clamp a
   non-positive replica count to one. Changing that API behavior is outside
   this proposal; owner teardown is covered because it already deletes every
   Instance.
8. **A cluster-wide rate limiter.** The budget is per IR/Component. Concurrent
   scale-downs can each use their own window; adaptive or global admission is a
   separate control-plane policy.

## Terminology

- **Instance:** one OMENative replica, represented by one
  `OMENativeInstanceStatus` and one or more Pods. An Instance is the atomic
  lifecycle unit.
- **Gang:** a multi-Pod Instance. Every gang member must leave service, drain,
  and be selected for deletion together; individual Pod delete RPCs may still
  complete at different times.
- **Extra Instance:** an InstanceStatus index no longer covered by the desired
  Component plan, excluding migration-protected source/surge pairs.
- **Delete-owned Instance:** an Instance whose persisted status is
  `Phase=Deleting` with `Operation.Type=Delete`. This status is durable work,
  not merely a derived observation.
- **Active wave:** the ordered budget-sized prefix selected to progress in the
  current reconcile. A newly admitted prefix remains the fresh wave until its
  statuses are removed. A larger pre-upgrade DeleteOwned backlog advances in
  bounded prefixes, but every remaining entry blocks fresh admission.
- **Pod cost:** `max(1, authoritative live Pod count)` for one Instance,
  including Terminating Pods. A Podless status costs 1 so status and PodGroup
  cleanup cannot become an unbounded zero-cost burst. Live count, rather than a
  stale status counter, measures the destructive work that remains.
- **Admission linearization point:** the successful IR status update that
  persists `Deleting` intent for every newly selected extra Instance. Before
  this point, no external delete effect is allowed; after it, the controller
  finishes the durable delete even if desired replicas rebound.
- **Authoritative observation:** an uncached API read. Destructive planning uses
  it because a stale informer view can incorrectly claim that Pods are gone.

## Proposal

### The contract

For every IR scale-down, treating an absent budget as unbounded:

1. The controller MUST use one authoritative Component Pod observation to
   identify live Pods and compute Instance costs.
2. The controller MUST select whole Instances in deterministic order. It MUST
   NOT skip a non-fitting earlier gang to admit a smaller later one.
3. The controller MUST prioritize already delete-owned Instances. Persisted work
   survives process restart and cannot be starved by newly extra Instances. If
   any DeleteOwned Instance exists, the controller MUST admit no fresh extra.
4. The active wave's total cost MUST NOT exceed the configured budget, except
   that the first eligible Instance proceeds alone when the active selected
   cost is zero and that Instance alone exceeds the budget. Without this
   exception, one large gang can deadlock forever.
5. The controller MUST commit all newly admitted `Deleting` statuses in one
   authoritative, conflict-retried IR status update before external effects.
6. The admission update MUST verify the IR UID, generation, and each
   candidate's observed identity/ownership used to build the plan. A different
   UID, a generation change, a changed incarnation, or a new competing
   operation aborts the complete fresh batch with zero new external effects.
7. Every selected gang MUST have all serving holds applied and all endpoints
   observed drained before the first member Pod delete. A serving-hold write
   MUST verify the Pod UID from the authoritative snapshot, and every Pod
   DELETE MUST carry that UID as a precondition. NotFound or a UID-precondition
   conflict proves only that the observed object is gone; a same-name successor
   MUST remain untouched.
8. Every terminal Instance-removal path that can own a PodGroup MUST use one
   finalization contract: authoritative zero Pods, request PodGroup deletion,
   then wait for authoritative absence (or optional-kind unavailability)
   before InstanceStatus removal. A failed or incomplete PodGroup deletion
   retains the status so the work remains discoverable.
9. Completed selected InstanceStatus removals MUST commit in one status update.
   Per-Instance expectation cleanup runs after the corresponding absence is
   authoritatively confirmed, including a retry after an ambiguous write
   result.
10. A scale-down admission or batched completion status commit MUST end the pass
    with an immediate requeue. Later actions cannot use the pre-commit plan or
    Pod snapshot.
11. While any extra or delete-owned Instance remains deferred or incomplete,
    the workload pipeline MUST NOT advance Restart, Migrate, Update, or Create.
12. Deferred revision promotion and status aggregation MUST verify the IR
    generation captured at reconcile start. A generation change ends the pass
    with an immediate replan before stale rollup, aggregation, or retention
    effects can run.

These rules preserve the existing high-level behavior: scale-down completes
before later lifecycle verbs. They change the amount of work allowed to advance
at once.

### Configuration

The existing `inferenceservice-config` ConfigMap gains scale-down budget and
polling settings:

```json
{
  "lifecycle": {
    "scaleUpPodBatchSize": 100,
    "scaleDownPodBatchSize": 100,
    "scaleDownRequeueInterval": "5s"
  }
}
```

`scaleDownPodBatchSize` is startup-scoped, like
`scaleUpPodBatchSize`:

- **Absent:** unbounded candidate selection. This disables pacing, while the
  batched status and ordering mechanics still apply.
- **Positive:** maximum active-wave Pod cost, with the oversized-first exception.
- **Zero or negative:** invalid; manager startup fails.
- **Malformed or unreadable lifecycle configuration:** manager startup fails.

The binary contains no default. The Helm chart and rendered ConfigMap will own
the default value. This OEP proposes `100`, matching the scale-up Pod budget and
bounding a fully populated eight-Pod deployment to twelve newly admitted gangs
per wave. A Helm-driven change rolls the manager through the existing ConfigMap
checksum; a manual ConfigMap edit requires restarting every manager replica.
The startup path loads and validates the scale-up, scale-down, and polling
fields from one lifecycle configuration read so the manager settings cannot
observe different ConfigMap versions.

`scaleDownRequeueInterval` controls only the periodic wake-up while drain, Pod
deletion, or per-Instance cleanup remains in flight:

- **Absent:** no cadence timer. Pod, EndpointSlice, PodGroup, and owner events
  continue to drive progress. Configured force-delete and teardown deadlines
  independently schedule their exact next evaluation, so time-based safety and
  liveness policies do not depend on a later resource event.
- **Positive duration:** poll at the configured cadence.
- **Zero, negative, or malformed duration:** invalid; manager startup fails.

The binary contains no polling default. The Helm chart and rendered ConfigMap
own the `5s` default, preserving the existing cadence for managed installs.
An older hand-authored ConfigMap that omits the field degrades to watch- and
deadline-driven progress rather than receiving a hidden fallback or entering
an immediate requeue loop.

Scale-up and scale-down keep separate fields. Reusing the scale-up value would
silently change destructive behavior for every installation that configured
only creation pacing.

### Wave admission

The selector has two mutually exclusive modes:

1. if any delete-owned Instance exists, select only from that set, oldest
   `Operation.StartedAt` first and then highest Instance index as the
   deterministic tie-breaker;
2. otherwise, select newly extra Instances, those with a pod on a cordoned
   node (`spec.unschedulable`) first and then highest Instance index, and
   admit the selected prefix in one status write.

`InstanceStatuses` has Kubernetes map-list semantics, so serialized slice order
is not an ordering contract. Descending index is an explicit ordinal-shrink
proxy, **not** a claim that the highest index is the newest Instance: migration
can reuse sparse indices and status has no Instance creation timestamp, so
neither this rule nor the planner can order by age.

The planner decides the victim set (OEP-0007 controller mechanics §7.4:
in-flight migration and gang-surge pairs stay, then Instances on a cordoned
node leave first, then the health ranks and the lowest index decide); this
rule only orders bounded progress within that set. Cordoned extras lead the
fresh admission for the same reason they lead the victim set: when a budget
splits a reduction across waves, the first wave empties the nodes an operator
is draining instead of leaving them occupied until the last one.

Delete-owned work keeps its admission order whatever the cordon. Reordering it
when a node is cordoned mid-wave would let a later candidate's deletes start
while an earlier one is still draining, and break the concurrency bound below.

For each candidate, the selector computes
`cost = max(1, authoritativeLivePodCount)`. It
admits a stable prefix until the next candidate would exceed the remaining
budget. Once a candidate does not fit, selection closes; later candidates do
not bypass it. If active selected cost is zero and the first candidate alone
exceeds the budget, it is admitted alone. The escape never piles an oversized
gang on top of other active work.

This is a **strict wave concurrency bound**, not a wall-clock rate limit. A
fresh wave remains closed to new admission until every DeleteOwned Instance in
that admitted batch is removed. The rule intentionally leaves freed Pod budget
unused during finalization: without a persisted admission cost, refilling from
falling live counts fragments an eight-Pod batch into progressively smaller
admissions and recreates status-write amplification. If zero-grace Pods
disappear immediately, successive waves can still advance quickly; the
Kubernetes client rate limiter governs throughput. This OEP does not claim
token-bucket semantics across passes.

Example — 1,999 extra eight-Pod gangs, budget 100:

```text
wave 1: 12 gangs / 96 live Pods admitted; gang 13 does not fit
wave 1 remains closed until all 12 statuses are removed
wave 2: next 12 gangs admitted
...
```

### Durable deletion and replica rebound

The successful batched `Deleting` status write owns the operation. After that
commit, a later desired-replica increase does **not** cancel deletion. The
controller finishes drain, Pod deletion, PodGroup deletion, and status removal;
the next plan recreates any capacity that is desired again.

This rule may look wasteful when scale-down and scale-up race, but cancellation
is not safe without a separate protocol. By the time a rebound is observed,
some gang members may be NotServing, some Pod deletes may have succeeded, and
the PodGroup may be gone. Removing the Delete readiness key alone can expose a
partial gang. Finish-then-recreate has one durable owner and one recovery path.

The planner must therefore continue selecting every delete-owned Instance for
`ActionScaleDown`, even if ordinary desired-index allocation would otherwise
pull that index back into the plan. Later lifecycle actions remain blocked until
the durable deletion completes.

### State model

The protocol adds no new API type and only two durable status states:
Unadmitted and DeleteOwned. Execution substeps are reconstructed from status
and live resources on every pass; Complete is represented by status absence.

| State | Durable or resource evidence | Allowed transition or effect |
| --- | --- | --- |
| Unadmitted extra | Outside the current plan with no Delete operation | Selection only; no external effect. A desired-count change may make it non-extra. A UID-, generation-, incarnation-, and ownership-guarded batch write admits it. |
| DeleteOwned / Draining | Authoritative status has `Phase=Deleting`, `Operation.Type=Delete`, `Operation.Step=Drain`; live Pods exist | Mark all selected live Pods NotServing, then read the shared drain index. |
| DeleteOwned / DeletingPods | Every routed endpoint for that Instance is drained; live or Terminating Pods exist | Record expectation before each Pod DELETE; wait or force-delete under the existing policy. |
| DeleteOwned / Finalizing | The authoritative Component snapshot has zero Pods for the index | Request PodGroup deletion; enqueue status removal only after a fresh authoritative inventory proves absence. |
| Complete | Batched status removal is authoritatively confirmed | Forget expectations for that absent index and immediately requeue before index reuse. |

The admission write is the first linearization point. The batched status
removal is the second. `Operation.Step` remains `Drain` throughout DeleteOwned;
writing each resource-derived substep would recreate the status amplification
this OEP removes.

### Resource ordering

For the selected wave, the executor preserves this sequence:

```text
batched IR Deleting intent (new admissions only)
  -> immediate requeue and fresh authoritative observation
  -> stuck-Terminating escalation and expectations check by Instance
  -> mark all selected live Pods NotServing
  -> readiness-patch barrier
  -> build one routed-endpoint drain index
  -> evaluate each Instance and issue per-Pod expectation, then Pod DELETE
  -> wait for authoritative live Pod set to become empty
  -> request PodGroup deletion (if applicable)
  -> wait for a fresh authoritative inventory to prove PodGroup absence
  -> batched InstanceStatus removal
```

The Pod delete RPCs are not a transaction; Kubernetes may accept some and reject
others. Gang atomicity here means **selection and drain atomicity**, not
simultaneous physical deletion. Persisted Delete intent makes partial progress
resumable.

An unsatisfied expectation or an undrained endpoint defers only the affected
Instance; other selected Instances may progress. A hard API error stops later
effects in deterministic order and retains all committed intents for retry.

### Teardown

IR deletion normally uses the same bounded executor. The finalizer remains
until an authoritative Component Pod list is empty and every owned PodGroup is
absent from an authoritative inventory. This avoids replacing a controlled Pod
delete wave with an unbounded owner-reference cascade of thousands of
PodGroups.

Teardown also discovers an owned PodGroup whose InstanceStatus is already
absent. The IR `deletionTimestamp` is durable authorization to delete that
orphaned group, so an empty group enters the deterministic teardown candidate
order at cost 1 without inventing status. A group whose index still has live
Pods is retained; the existing statusless-Pod completion check continues to
hold the finalizer for external repair or the deadline escape.

Teardown keeps its existing force-delete deadlines and migration inclusion:
normal scale-down excludes migration-protected source/surge pairs; teardown
includes every observed Instance because the owner must disappear. A
configured batch may lengthen finalizer lifetime, but it does not weaken the
final live-resource check. The existing teardown deadline remains absolute and
does not reset per wave. The reconciler evaluates expiry before executor or
resource-read errors can return; otherwise a persistent API error could disable
the escape indefinitely.

Deadline expiry and manual finalizer removal remain deliberate safety escapes.
They release the IR to Kubernetes owner garbage collection even when work is
deferred, so that final cascade is not guaranteed to obey the configured
budget. Bounded normal teardown must not be described as overriding this
existing liveness policy.

## Design Details

### Current execution path and measured cost

The current implementation has four independent amplification points:

1. `deleteExtraInstances` calls `ops.Delete` once per extra index.
2. Each `ops.Delete` calls `LiveListPodsForInstance`; the authoritative reader
   has no field index, so each call lists the full Component Pod population and
   filters client-side.
3. Each status transition uses a full IR read-modify-write. The IR status grows
   with Instance count, so O(N) transitions serialize O(N) data each.
4. Each Instance constructs a separate drain batcher. EndpointSlice lists and
   endpoint scans repeat for Instances on the same revision Service.

PodGroup reconciliation happens even earlier, before `workload.Reconcile`, and
currently owns both planned-group ensure and extra-group deletion. That call
order prevents a later Pod budget from bounding extra PodGroup deletes.

### Proposed execution path

The scale-down action becomes a multi-Instance executor:

```text
Plan desired and extra indices
  -> one authoritative Component Pod LIST
  -> bucket Pods by Instance index and share with PodGroup prerequisites
  -> select active wave
  -> generation-guarded batch of new Deleting intents
  -> immediate requeue after a successful admission write
  -> resume selected Instances in deterministic order
       -> readiness-patch barrier
       -> shared drain index
       -> per-Instance expectation / Pod effects
       -> PodGroup cleanup after zero Pods
  -> one batch of completed InstanceStatus removals
  -> immediate requeue after a successful completion write
```

`EnsurePodGroupsWithTopology` becomes ensure-only for planned indices outside
the terminal-finalization-owned set. Extra PodGroup cleanup moves behind shared
Instance finalization because only the terminal owner knows whether its Pods
are gone.

The implementation SHOULD reuse the existing atomic Instance mutation adapter
introduced for scale-up batching. It already supports multi-index mutation,
removal, authoritative conflict retries, owner disappearance, and committed
callbacks. The delete path must call the owner-aware adapter directly; an
instance-only compatibility wrapper that converts owner disappearance to
success is unsafe before external delete effects.

### One authoritative Pod observation

On a scale-down or teardown pass, the Component reconciler calls the existing
live Component Pod snapshot once, before PodGroup prerequisites and workload
execution, and buckets by `ome.io/instance` index. The same snapshot supplies:

- immutable-Pod topology evidence used by PodGroup ensure;
- Pod cost for wave selection;
- the exact Pod set for serving-gate changes and drain checks;
- stuck-Terminating escalation candidates;
- the zero-Pod proof required before PodGroup/status removal.

One pass uses one snapshot deliberately. An Endpoint or Pod change after the
list can delay completion until the next reconcile, but it cannot justify an
unsafe early removal: destructive completion still requires the listed Pods to
be gone, and a subsequent reconcile repeats the authoritative list.

The finalizer's later completion pass uses that pass's sole authoritative
Component list before declaring owner teardown complete; it does not issue a
second list in the same pass. A cached empty list is never sufficient.

The observation must be hoisted above the existing PodGroup helper. Calling
one list for PodGroup ensure and another for scale-down would preserve the
hidden two-list amplification and violates the one-list target.

### Batched InferenceReplica status

New admissions use one atomic mutation set. For each selected new extra, the
mutation stamps the same fields the current per-Instance helper writes:

- `Phase=Deleting`;
- `Operation.Type=Delete`;
- `Operation.Step=Drain`;
- per-Instance ID, start/progress time, and deadline.

The batch preserves each Instance's timestamp and operation identity; batching
does not require every operation to share one timestamp.

After external effects, every Instance proven Podless and successfully cleaned
of its PodGroup contributes a `Remove` mutation to one completion batch.
The batch-level precondition verifies the owner UID and every candidate's exact
incarnation plus Delete operation ID/type. A changed or reused slot rejects the
whole removal batch; conflict retry must never remove the new incumbent.
Untouched status entries and status order remain unchanged. The callback for a
committed removal forgets only that Instance's expectations; a failed status
write forgets none. If a transport result is ambiguous, an authoritative IR
reread determines which removals committed; expectation buckets are forgotten
for indices authoritatively absent even if the original success callback did
not run.

The scale-down executor performs at most two lifecycle status writes per pass:
one admission write and one completion write. This is not a global bound on
unrelated status writers. In a scale-down-only fixture with stable adjacent
conditions, aggregation may add at most one write; integration tests count any
other writer separately. Aggregate status must rebase from an authoritative or
server-returned status so an adjacent lifecycle write cannot be lost or
immediately conflict against stale cache state.

### Indexed drain observation

The current drain batcher memoizes an EndpointSlice list per Service but scans
all endpoints for each Pod. It also repeats the fallback Service `GET` for each
Pod when a Service has no EndpointSlices.

The executor first attempts every selected Instance's serving-condition
patches. After that readiness-patch barrier, the wave-scoped drain observer
builds, per distinct revision Service:

- the EndpointSlice list result;
- a set of Pod target names whose endpoint is still routable;
- the empty-slice Service existence result.

Building the set costs O(E), and checking P Pods costs O(P). A single reconcile
therefore performs at most one EndpointSlice list and one empty-slice Service
lookup per distinct revision Service, rather than per Instance or Pod.

The snapshot is conservative. If an endpoint disappears after the list, the
controller waits one extra reconcile; if it appears or remains ready in the
snapshot, the controller does not delete that Pod in the current pass.

### PodGroup ownership

PodGroup creation remains a prerequisite for planned multi-Pod Instances. The
planner derives a `terminal-finalization-owned` index set from persisted
lifecycle ownership plus the shared Pod snapshot. It includes DeleteOwned
Instances, a migration or gang-update source that has entered terminal source
cleanup, and a failed-surge target being abandoned. The prerequisite MUST skip
every index in this set. Recreating its PodGroup between a successful delete and
a retried status removal would violate the terminal owner's ordering.

When the optional API exists, the prerequisite phase authoritatively lists the
Component's PodGroups once, verifies their controller owner UID, and buckets
the owned objects by Instance index. That inventory is shared by ensure and
finalization. It records actual objects, so a multi-Pod-to-single-Pod shape
change cannot hide an old PodGroup after the last labeled Pod disappears.

PodGroup deletion moves out of the prerequisite pass and behind the terminal
owner:

1. while any Instance Pod exists, retain the PodGroup;
2. after the authoritative Pod bucket is empty, issue the deterministic
   PodGroup delete;
3. retain InstanceStatus after an accepted delete and end the pass;
4. remove InstanceStatus only after a fresh authoritative inventory reports
   the PodGroup absent.

Already-Terminating PodGroups consume the configured active budget. Repeated
watch or poll reconciles may fill unused capacity, but MUST NOT accumulate new
PodGroup deletes beyond the bound while earlier deletions remain pending.

An owned PodGroup with `deletionTimestamp` is a creation gate. Ensure returns a
requeue without treating a matching Terminating object as success, and the
workload executor creates neither its replacement PodGroup nor member Pods
until an authoritative inventory reports NotFound. Keeping terminal status
through the same absence proof makes restart and index reuse reconstructible.

This order makes failures resumable. If PodGroup deletion fails while status
still exists, the next reconcile can retry. Removing status first would make
the stale PodGroup undiscoverable to the normal status-driven scale-down path.

If the PodGroup CRD is absent at manager startup, PodGroup effects remain
skipped under the existing soft-fail contract. If that API is removed at
runtime, NoMatch is treated like NotFound because deleting a CRD also deletes
its custom resources. This OEP does not turn an optional API into a teardown
blocker.

### Shared Instance finalization

Removing eager extra-PodGroup garbage collection changes more than
`ActionScaleDown`. Migration source promotion, gang-update source promotion,
and failed-surge abandonment also remove an InstanceStatus after its Pods are
gone. Each path MUST call the same terminal finalizer:

```text
authoritative Component Pod bucket is empty for index
  -> owned PodGroup DELETE requested
  -> fresh authoritative inventory reports absent, or optional kind unavailable
  -> InstanceStatus Remove commits
  -> expectations for confirmed-absent index are forgotten
```

Their selection, drain, promotion, ledger, and retry semantics do not change.
Only the final resource/status ordering is centralized. The old eager
extra-PodGroup deletion may be removed only after every terminal status-removal
caller uses this contract; otherwise a source PodGroup can become unreachable
as soon as its status entry disappears.

Each caller supplies an exact removal precondition for its lifecycle owner:
owner UID, Instance incarnation, and the captured Delete, Migrate, Update, or
failed-surge operation identity. A conflict that changes ownership aborts the
removal rather than deleting a reused status slot.

### Concurrency and the admission linearization point

The dangerous race is:

1. reconcile A reads desired replicas 1 and plans 1,999 deletions;
2. a scaler writes desired replicas 2,000;
3. reconcile A retries its status write against the fresh object but blindly
   applies stale `Deleting` mutations;
4. reconcile A deletes Pods that are desired again.

Conflict retry alone does not prevent this race; it refreshes status but does
not validate the decision's spec generation.

The admission adapter receives the IR UID, generation, and per-candidate
identity captured by planning. On every authoritative retry read, it verifies:

- UID and `metadata.generation` still identify the planned IR incarnation and
  spec;
- each candidate still has the expected Instance incarnation;
- each candidate's Phase and operation identity still equal the planning
  snapshot;
- the complete candidate set still satisfies admission preconditions.

The guard detects a **change after planning**; it does not ban an operation type
categorically. Normal planning already excludes migration-protected pairs and
may intentionally let higher-precedence scale-down supersede an unchanged
Restart or Update. Teardown may intentionally supersede the captured migration
operation. Either transition is valid only while that captured identity remains
unchanged.

Any mismatch rejects the **whole** fresh batch and returns a replan signal
before the status write. Partial admission would make prefix ordering and the
Pod budget depend on conflict timing. No new serving, Pod, or PodGroup effect
occurs on that pass.

This requires a batch-level precondition in the atomic adapter. Per-slot
preconditions that silently skip a changed candidate are insufficient because
they can commit a partial prefix.

After the status write succeeds, `Deleting` is the linearization point. A later
generation change cannot revoke committed work; the finish-then-recreate rule
applies. Existing delete-owned work does not need a new generation guard because
its authority predates the current plan.

Any admission or completion status commit forces an immediate requeue. The
controller does not fall through to Restart, Migrate, Update, or Create using
the pre-commit plan and Pod snapshot. In particular, completion cannot remove a
PodGroup/status and then let stale Create recreate Pods before the prerequisite
pass runs again.

For an ambiguous admission response, an authoritative IR reread determines
whether the complete mutation set committed. The controller either resumes the
server-confirmed DeleteOwned batch or replans; it never guesses and starts
external effects from an unconfirmed write.

### Failure semantics

| Failure | Required behavior |
| --- | --- |
| IR disappears before admission | Abort; zero external effects. Owner garbage collection owns teardown. |
| IR name is recreated with a different UID | Reject the stale batch; zero new external effects. |
| IR generation changes before admission | Abort and replan; zero new external effects. |
| Admission status conflict | Re-read authoritatively; re-check UID, generation, every incarnation, and whole-batch candidacy before retrying the complete mutation set. |
| Admission status write fails or is throttled | Return error; zero new external effects. |
| Context cancellation after admission | Keep `Deleting` intents; the next reconcile resumes them. |
| Serving-condition patch fails | Stop later effects in stable order; retain all committed intents. |
| An Instance has unsatisfied expectations or a routed endpoint | Defer that Instance; continue evaluating other selected Instances. |
| EndpointSlice/Service read fails | Delete no Pod that depends on the failed observation; retain intent. A hard API error stops later effects in stable order. |
| Pod delete fails | Roll back that Pod's expectation when no watch event can satisfy it; retain intent and stop later effects. |
| Some Pod deletes succeed | Do not compensate or re-expose the gang; resume deletion from authoritative observation. |
| PodGroup delete fails | Retain InstanceStatus and retry; do not lose cleanup ownership. |
| Completion status result is failed or ambiguous | Reread authoritatively; retain discoverable status or confirm absence before expectation cleanup. |
| Replica count rebounds after admission | Finish delete, remove status, then recreate desired capacity. |
| Controller restarts | Reconstruct the active wave from persisted Delete operations and live Pods. |

Unlike scale-up Pod creation, committed scale-down intent is not rolled back on
a later effect failure. Pod deletion is irreversible, and removing drain keys
after partial deletion could expose a broken gang. Resume is the only safe
general recovery.

### Interactions with other lifecycle features

- **Paused:** unchanged. Scale-down already runs before the Paused gate, so it
  continues to release capacity while Update, Restart, Migrate, and Create stay
  paused.
- **Migration:** normal planning keeps excluding migration-protected source and
  surge pairs. Teardown includes them. A delete-owned Instance remains delete-
  owned; a new migration cannot take ownership from Delete.
- **Restart and Update:** while any delete work is active or deferred, these
  later actions do not run. This preserves the existing pipeline precedence.
- **Create:** a replica rebound after admission waits for deletion completion,
  then Create restores the missing Instance. The controller never creates and
  deletes the same index concurrently.
- **PodGroup ensure:** terminal-finalization-owned indices are excluded from
  ensure; a Terminating owned PodGroup also gates PodGroup and Pod creation
  until authoritative absence.
- **Rollout coordination:** ordinary monotonic scale-down does not delete the
  retained desired set. A replica rebound after admission is different:
  finish-then-recreate can temporarily leave capacity below the new desired
  count by at most the committed fresh wave. A pre-upgrade unbounded DeleteOwned
  set can create a larger deficit, although its effects are resumed in bounded
  prefixes.
- **Expectations:** expectations remain per Instance. The executor records each
  expected delete before the corresponding API call and reverses it on a failed
  call; committed status removal forgets the completed Instance only.
- **Force deletion:** stuck-Terminating escalation remains before the normal
  expectations gate, preserving the dead-node recovery schedule.

### Observability

The implementation adds these controller metrics:

- `ome_omenative_scale_down_batch_pods{component}` — histogram of selected
  Pod-equivalent cost, including the cost-1 floor for Podless work;
- `ome_omenative_scale_down_active_pods{namespace,isvc,component}` — current
  selected Pod-equivalent cost, reset to zero on convergence and series-deleted
  when the IR disappears;
- `ome_omenative_scale_down_deferred_instances{namespace,isvc,component}` —
  eligible extra or DeleteOwned candidates outside the selected active prefix,
  reset to zero on convergence and series-deleted when the IR disappears;
- `ome_omenative_scale_down_instance_duration_seconds{component}` — histogram
  from Delete admission to confirmed status removal;
- `ome_omenative_scale_down_oversized_batch_total{component}` — count of the
  single-Instance progress escape.

Status-write outcomes reuse `ome_isvc_status_update_total`; the implementation
does not add another result-labelled write counter.

Structured reconcile logs include the IR key, configured budget, active Pod
cost, admitted Instance count, and deferred count. The controller does not emit
one Kubernetes Event per wave or Instance; at 2,000 replicas that would create a
second control-plane burst. Existing warning Events continue to report actual
failures. Teardown warnings use a bounded summary: survivor count when
observation succeeded, the observation failure otherwise, and a stable
Component label selector. They never include a comma-separated list of Pod
names.

### Compatibility and rollout

Compatibility is explicit:

| Binary and configuration | Behavior |
| --- | --- |
| Old binary with the new field | Field is ignored; scale-down remains unbounded. |
| New binary with the field absent | All eligible candidates may be admitted together; there is no capacity pacing, but batched ordering still applies. |
| New binary with a positive field | Gang-aware bounded behavior is enabled. |
| New binary with zero, negative, or malformed configuration | Manager startup fails. |
| Restart in the middle of a wave | Work is reconstructed from DeleteOwned status and live resources. |
| Budget lowered below committed work | Committed victims advance in bounded prefixes; no fresh victim is admitted until every DeleteOwned status completes. |

Go source compatibility is intentionally narrower than Kubernetes API
compatibility. The implementation removes the exported controller-internal
`ops.Delete` function and its hard-coded `ops.DeleteRequeueInterval`; the
multi-Instance executor and configured cadence replace them. There are no
in-repository callers, but downstream Go code importing either symbol must
migrate to the batch executor and provide the owner-aware atomic status adapter.
Keeping a wrapper would be misleading because the old signature cannot express
the admission transaction or distinguish admission from effect progress.

The proposed Helm value is `100`, so the implementation's chart upgrade will opt
the deployment into bounded scale-down. This is deliberate and must be called
out in release notes because capacity release can take multiple waves. During a
rolling manager upgrade, operators should avoid initiating an extreme
scale-down until the new leader is active; an old leader does not understand
the new bound.

The controller accepts IRs with existing `Deleting` operations created by an
older binary. It selects a bounded stable prefix of those operations and resumes
them; it does not allow a pre-upgrade unbounded intent set to bypass all Pod
effect limits after restart.

Removing the field and restarting every manager replica disables pacing but
keeps the new batching, ordering, and PodGroup mechanics. Restoring the old
mechanics requires a binary downgrade. That downgrade ignores the field and can
resume every persisted Delete at once, so operators must not perform it during
a large active scale-down. Neither path attempts unsafe cancellation.

## Implementation Plan

1. **Observation and accounting:** add counting tests, hoist one live Component
   Pod list above PodGroup prerequisites and workload execution, bucket it by
   Instance, and share one owned-PodGroup inventory when that API exists.
2. **Configuration and selector:** load both Pod batch fields through one
   startup path, then add the gang-aware active-wave selector. With the
   scale-down field absent, assert victim-set and terminal-state parity with the
   current unbounded path.
3. **Status batching:** commit new `Deleting` intents and completed removals
   through the owner-aware atomic adapter, with UID, generation, incarnation,
   and all-or-none candidacy guards.
4. **Multi-delete executor:** replace the per-index `ops.Delete` dispatch loop
   while retaining the current per-Instance effect order and error boundary.
5. **Drain indexing:** add the readiness-patch barrier, share
   EndpointSlice/Service observations across the wave, and index routable target
   names.
6. **Shared finalization:** route scale-down, migration source, gang-update
   source, and failed-surge status removal through zero-Pod -> PodGroup -> status
   ordering before removing eager extra-group deletion.
7. **Teardown and rebound:** route teardown through the same executor and make
   delete-owned status override ordinary desired-index and PodGroup reuse until
   completion. Preserve the absolute deadline escape.
8. **Metrics and scale tests:** add synthetic 2,000-Instance request-count tests,
   bounded teardown Events, modest KIND coverage, dashboards, and release notes.

Each phase must keep the status-before-effect invariant. Partial delivery may
reduce reads before enabling pacing, but no phase may expose a configured budget
that PodGroup or teardown paths bypass.

## Test Plan

### Selection and convergence

Pure table tests cover:

- nil/unbounded configuration;
- exact-fit, under-budget, and over-budget candidates;
- stable prefix with no smaller-candidate bypass;
- first oversized gang proceeds alone;
- eight-Pod gangs are never split;
- mixed and partially missing gang sizes, charged by current live Pods;
- live and Terminating Pods both count;
- Podless statuses cost one;
- delete-owned work precedes new extras;
- a pre-upgrade DeleteOwned set larger than the budget resumes in deterministic
  prefixes and admits no fresh victim until every DeleteOwned status is removed;
- missing or equal `Operation.StartedAt` values use descending index as the
  tie-breaker;
- fresh extras with a pod on a cordoned node are admitted before every other
  fresh extra, while delete-owned work keeps its `Operation.StartedAt` order
  whatever the cordon;
- active work prevents event-driven over-admission;
- one-Pod-at-a-time disappearance never admits a fresh Instance before the
  current DeleteOwned wave is fully removed;
- scrambled map-list serialization produces the same victim order;
- convergence across every wave to 1 replica and through full IR teardown.

The canonical gang case pins the arithmetic: budget 100 with eight-Pod gangs
admits 12 gangs / 96 Pods, not 13 gangs / 104 Pods.

### Configuration and observability

Startup table tests cover an absent lifecycle key, an absent scale-down field,
a valid positive value, malformed JSON, a malformed field type, a missing
ConfigMap, zero, and a negative value. A counting client asserts one ConfigMap
GET supplies both scale-up and scale-down values from the same resourceVersion.

Helm tests cover the proposed default `100`, a positive override, explicit zero
rendering followed by startup rejection, and omission of the field. Changing
the rendered lifecycle configuration must change the Deployment's ConfigMap
checksum so every manager replica rolls.

Metric tests assert selected and deferred values, reset-to-zero convergence,
series cleanup on IR disappearance, duration observation only after confirmed
removal, and one oversized counter increment per admitted operation rather than
per retry. Recorder tests assert zero routine success Events per wave or
Instance and a bounded teardown warning containing a selector plus either a
count or observation error, but no Pod-name list.

### API-operation bounds

Instrumented fake clients/readers assert operation counts, not only final state.
Let `C` be selected live Pod cost, `K` selected Instances, and `D` distinct
routed revision Services. Without injected conflicts, every pass must perform:

- exactly one authoritative Component Pod list total, including the PodGroup
  prerequisite path, and zero per-Instance full Pod lists;
- at most one authoritative owned-PodGroup list when that optional API exists;
- at most one admission and one completion update from the scale-down executor;
  a scale-down-only fixture with otherwise stable state permits one additional
  aggregate update, while integration tests count unrelated writers separately;
- serving-condition reads/patches and Pod deletes for no more than `C` Pods;
- at most `D` EndpointSlice lists and `D` empty-slice Service lookups;
- at most `K` PodGroup deletes, only for authoritatively Podless Instances;
- zero Pod, PodGroup, or lifecycle-status mutations for deferred Instances.

For the first 2,000 -> 1 singleton admission with budget 100, `C=100`, the
retained desired Instance receives zero effects, and 1,899 extras remain
deferred.

For 2,000 eight-Pod gangs, a lightweight synthetic reader asserts one 16,000-Pod
list and O(P) bucketing, rather than 1,999 lists and about 32 million decoded Pod
objects. Routine unit CI must not create 16,000 real Kubernetes objects.

Serialization benchmarks record total IR status request bytes. For the measured
approximately 1 MiB, 2,000-Instance shape and budget 100, the immediate fake
fixtures have these upper bounds:

| Fixture | Waves | Passes including final verification | Lifecycle status writes |
| --- | ---: | ---: | ---: |
| 2,000 -> 1 singleton | 20 | 61 | 40 |
| 2,000 -> 1, eight Pods per gang, no PodGroups | 167 | 502 | 334 |
| 2,000 -> 1, eight Pods per gang with PodGroups | 167 | 669 | 334 |

Without a per-Instance PodGroup, each wave needs admission, effect, and
completion passes. A gang with a PodGroup needs a fourth pass: the first
Podless pass issues the PodGroup DELETE, and the next authoritative inventory
must prove absence before status removal. The final `+1` pass verifies that no
extra Instance remains. Endpoint or termination delay may add passes but cannot
increase the per-pass bounds.

The benchmark records lifecycle and aggregate writes separately, total logical
JSON bytes, and maximum individual request size. Aggregate counter writes may
occur between lifecycle transitions as Pod status changes; `40` and `334` are
therefore **lifecycle-write** counts, not total IR status-write counts. Batching
reduces cumulative pressure; the maximum single IR update remains O(N) and is
not rescued if it already exceeds an API server or etcd object-size limit.

### Ordering and failure injection

Tests record every status and external effect, then inject failure at each
boundary:

- admission write failure, ambiguous response, conflict, owner disappearance,
  owner recreation, API 413/429/5xx, and context cancellation;
- a successful admission returns an immediate requeue with zero Pod or PodGroup
  effects in that pass;
- Pod serving patch failure on the first and middle Instance;
- a readiness-patch failure prevents the shared drain-index read and every Pod
  delete in that pass;
- EndpointSlice and empty-slice Service lookup failures;
- unsatisfied expectations and an undrained Instance while another selected
  Instance is eligible to progress;
- Pod delete NotFound, UID-precondition conflict, transport error, and partial
  success, including a same-name replacement that remains serving;
- stuck-Terminating force-delete evidence;
- PodGroup delete failure;
- completion status conflict, failure, and ambiguous response with
  authoritative expectation cleanup;
- a successful completion returns an immediate requeue without same-pass
  Ensure, Create, Restart, Migrate, or Update;
- controller restart after intent, after drain, after partial Pod deletion,
  after PodGroup deletion, and before status removal.

Every test asserts the status-before-effect invariant. Failures after admission
retain intent and never compensate a partially deleted gang.

### Concurrency

Race-oriented unit tests cover:

1. the same IR name is recreated with a different UID — expect rejection and
   zero external effects;
2. spec generation changes immediately before the admission write — expect a
   replan and zero external effects;
3. one candidate's incarnation, Phase, or operation identity changes before
   commit — including Update, Restart, Migrate, or a different Delete ID —
   expect rejection of the whole fresh batch and zero external effects;
4. status-only readiness changes alter the recomputed victim set without a
   generation change — expect rejection of the whole batch;
5. an unrelated status-only conflict with unchanged candidacy — expect an
   authoritative retry preserving concurrent fields;
6. desired replicas rebound immediately after admission — expect deletion to
   finish and later Create to restore capacity;
7. desired replicas change 2,000 -> 1 -> 1,000 across admission — expect the
   committed batch to finish and only indices still extra after replanning to
   remain eligible;
8. a deleting index would otherwise be selected into the desired plan — expect
   Delete ownership and PodGroup exclusion to win until removal;
9. a completion conflict changes the incumbent incarnation or operation ID —
   expect rejection of the whole removal batch and no expectation cleanup;
10. owner deletion races ordinary scale-down — expect teardown to resume the
    same durable work, not duplicate or cancel it.
11. the generation changes before deferred revision promotion or aggregation —
    expect an immediate replan with no stale aggregation or retention sweep;
12. a same-name Pod replaces the observed UID before drain or deletion — expect
    the successor to remain serving and undeleted.

These tests use explicit synchronization hooks; sleeps are not accepted as race
proof.

### Gang and PodGroup lifecycle

- all gang Pods receive the Delete drain key before any member delete;
- all routed endpoints are absent before any member delete;
- selected live gang retains its PodGroup;
- selected Podless gang requests PodGroup deletion and waits for authoritative
  absence before status removal;
- unselected gang's PodGroup is untouched;
- a same-named PodGroup with a different controller UID is not deleted;
- PodGroup delete failure keeps InstanceStatus;
- PodGroup absence followed by status-removal failure reconstructs the
  terminal-finalization-owned set on the next pass and performs zero PodGroup
  create/update;
- a PodGroup held Terminating by a foreign finalizer retains terminal status
  and blocks replacement PodGroup and Pod creation through rebound and restart
  until authoritative NotFound;
- full teardown of 2,000 gang Instances cleans selected PodGroups in bounded
  waves;
- a statusless empty owned PodGroup costs one teardown unit, while one whose
  index still has a live Pod is retained;
- a multi-Pod-to-single-Pod shape change still discovers and removes the old
  owned PodGroup;
- missing PodGroup and unavailable PodGroup CRD follow soft-fail semantics;
- runtime PodGroup API removal treats NoMatch as absence;
- multiple revisions use one indexed drain observation per Service;
- empty EndpointSlices cause at most one Service lookup per Service;
- duplicate endpoint target refs and ready `nil` preserve current drain
  semantics;
- migration source, gang-update source, and failed-surge removal all use the
  shared PodGroup-before-status finalizer.

### Teardown and adjacent lifecycle behavior

- teardown respects the same Pod budget and keeps the finalizer through all
  normal waves;
- a final authoritative Pod list blocks completion when a statusless Pod still
  exists;
- a final authoritative PodGroup inventory blocks completion while any owned
  group has not received a bounded delete;
- deadline expiry remains absolute, releases the finalizer with deferred work,
  and explicitly permits an unbounded owner-GC escape;
- deadline evaluation precedes workload/list effects, so persistent Pod-list,
  PodGroup-list, or delete errors cannot prevent the configured escape;
- teardown warnings contain a stable label selector and bounded count/error
  summary, not every Pod name;
- migration source/surge pairs remain excluded in normal scale-down and included
  in teardown;
- Paused still permits scale-down but blocks later actions;
- deferred scale-down blocks Restart, Migrate, Update, and Create;
- expectation TTL and force-delete deadlines remain unchanged;
- a missing/invalid/zero/negative ConfigMap value has the documented startup
  behavior;
- absent field admits the full eligible victim set without capacity pacing;
- manual ConfigMap change does not apply until every manager replica restarts.

### KIND and performance tests

KIND validates behavior that fake clients cannot model faithfully:

- real kubelet readiness-gate and EndpointSlice convergence;
- a modest multi-Pod gang leaves service and drains before deletion;
- PodGroup lifetime around scale-down and owner teardown;
- controller restart mid-wave;
- replica rebound during a committed deletion.

The synthetic 2,000-Instance tests stay in unit/performance suites. Routine KIND
CI does not create 16,000 Pods. A dedicated scale environment validates one
2,000 -> 1 singleton run and one 2,000 -> 1 run with eight Pods per gang
(16,000 Pods) while collecting API server request rate, rejected requests, etcd
request latency, IR status bytes, and convergence time. The cluster shape and
baseline load remain `TBD` until the performance owner provisions that run.

The graduation counting harness will freeze the unbounded implementation as a
baseline. With immediate fake deletion and no injected conflicts, the bounded
path must reduce
authoritative destructive Component Pod-list calls by at least 95% for
singleton Instances and 65% for eight-Pod gangs with PodGroups. The latter
threshold reflects the required authoritative-absence pass: `1 - 669/1999` is
about 66.5%. Cached aggregation reads are measured separately and remain
unchanged. Lifecycle status-update calls must fall by at least 98% and 90%,
respectively. Pod readiness patches and deletes remain proportional to Pods
removed because they are necessary work.

The harness reports logical lifecycle and aggregate status bytes separately.
Cumulative lifecycle status request bytes must fall by at least 80%, and total
IR status request bytes including aggregation by at least 70%, against the
frozen baseline. The maximum individual IR request must not exceed the baseline
maximum; batching cannot hide an object-size regression.

## Graduation Criteria

### Alpha

- ConfigMap field, startup validation, and Helm value are documented.
- One authoritative Component Pod list supplies every scale-down candidate in a
  pass.
- New `Deleting` intents and completed removals are batched and guarded by
  owner UID plus whole-set lifecycle identity preconditions.
- Configured Pod effects and PodGroup deletes cannot exceed the active-wave
  selection.
- PodGroup cleanup occurs after zero Pods and before status removal for every
  terminal Instance-removal path.
- Nil configuration passes compatibility tests.
- All unit, request-count, failure-injection, and focused KIND tests pass.

### Beta

- The 2,000 -> 1 singleton and eight-Pod-gang scale tests complete with
  zero OME-attributable 413, 429, or 5xx responses under the documented
  test-cluster envelope and meet the operation- and byte-reduction gates above.
- Active scale-down passes record exactly one authoritative destructive
  Component Pod list, no more than two scale-down lifecycle IR status writes,
  and, in the stable-state fixture, no more than one additional aggregate
  write. Cached aggregation reads and unrelated writers are reported
  separately.
- A controller restart and a replica rebound during active deletion converge
  without a stranded NotServing key, PodGroup, or InstanceStatus.
- Operators have dashboards for active Pod cost, deferred Instances, error rate,
  and scale-down duration.
- The proposed default of 100 has soaked for one release without a control-plane
  regression or unacceptable capacity-release delay.

### Stable

- Two releases of production evidence show no recurrence of the 2,000-replica
  API/etcd rejection failure mode attributable to OMENative scale-down.
- Scale-down and normal teardown satisfy the same bounded-effect contract; the
  documented deadline/manual-finalizer escape remains exempt.
- Operational guidance documents how to size the budget for common gang shapes
  and how to disable pacing by omitting the field.
- Remaining open questions are resolved or explicitly deferred to a numbered
  follow-up OEP.

## Drawbacks

1. **Capacity releases more slowly.** With budget 100, 1,999 single-Pod extras
   require at least twenty waves. This is the intended price of protecting the
   shared control plane.
2. **Deferred extras continue serving.** The desired replica count and actual
   serving count diverge longer. Autoscaler and operator status must make that
   lag visible.
3. **Delete becomes a larger state machine.** Batching cannot be a loop limit;
   durable admission, restart recovery, generation safety, and gang ordering add
   real complexity.
4. **One global value cannot fit every workload.** Budget 100 represents 100
   singleton Instances but only twelve eight-Pod gangs. Pod units make load
   comparable, but per-cluster control-plane capacity still varies.
5. **Finish-then-recreate can do avoidable work.** A rapid down/up oscillation
   may delete and recreate a recently admitted Instance and temporarily place
   observed capacity below the rebound target by one committed wave. Safe
   cancellation is a separate protocol, not a conditional branch.
6. **Concurrent IRs can aggregate load.** Ten IRs with budget 100 can select up
   to 1,000 live Pods at once. Controller client QPS/Burst still limits request
   delivery, but this OEP does not reserve a cluster-global destructive budget.
7. **One stuck victim blocks fresh waves.** A foreign Pod finalizer can keep one
   DeleteOwned Instance live indefinitely. Force-delete does not strip foreign
   finalizers, and ordinary scale-down has no teardown-deadline escape, so later
   extras remain unadmitted until an operator repairs that Pod or removes its
   finalizer. This head-of-line blocking is the price of a durable wave without
   adding per-Instance admission-cost state.

## Alternatives

1. **Raise client QPS/Burst.** Rejected. It reduces client-side waiting while
   increasing the rate at which OME sends amplified requests to the API server
   and etcd.
2. **Limit only the existing per-Instance loop.** Rejected. It leaves one full
   Pod list, one full IR write, and one drain observation per Instance; frequent
   watch events can also start every nominal batch immediately.
3. **Use an Instance-count budget.** Rejected. One Instance may cost one Pod or
   dozens, so equal Instance counts do not bound API effects. Pod cost plus
   gang-atomic selection captures the real work.
4. **Reuse `scaleUpPodBatchSize`.** Rejected. Operators who chose creation pacing
   did not authorize slower destructive scale-down; the two directions also
   have different recovery semantics.
5. **Cancel deletion when replicas rebound.** Rejected for v1. Once any Pod is
   drained or deleted, cancellation needs gang repair, PodGroup reconstruction,
   and readiness-key compensation. Finish-then-recreate is slower but has one
   durable owner.
6. **Use a process-local work queue or token bucket.** Rejected. Restart loses
   admission state, multiple reconciles can duplicate work, and the queue cannot
   establish status-before-effect ordering.
7. **Create another CRD between IR and Pod.** Rejected. IR already owns exactly
   the durable per-Instance state needed for admission; another ReplicaSet-like
   layer adds API objects without removing a correctness problem.
8. **Delete Pods with `DeleteCollection` or uncontrolled parallelism.** Rejected.
   Kubernetes deletion does not provide OMENative's serving drain, gang,
   expectations, status, or PodGroup ordering.
9. **Drop per-Instance status and derive everything from Pods.** Rejected for
   this OEP. Live counters can be derived, but deletion intent must exist before
   Pod effects and survive the zero-Pod window.
10. **Adapt the budget to API latency or coordinate it globally.** Deferred. A
    feedback controller may protect a shared control plane better when many IRs
    shrink together, but it needs cluster-wide state, stability rules, and a
    failure policy. The deterministic per-IR Pod window is testable without
    introducing that new control loop.
11. **Refill a fresh wave as live Pods disappear.** Rejected for v1. An
    eight-Pod gang that loses Pods one at a time continuously frees apparent
    budget and can turn one intended wave into many smaller admission writes.
    Preserving its original charge requires another durable status field; the
    strict wave barrier gives the same destructive-work bound without growing
    IR status.

We choose durable Pod-budgeted waves because they bound the actual destructive
surface and make restart recovery part of the contract.

## Open Questions

1. **Should a later release support a per-IR override?** The global ConfigMap is
   sufficient to protect the controller, but clusters may want a smaller budget
   for very expensive Components or a larger one for cheap router Pods. Any
   override needs admission bounds so one tenant cannot disable cluster safety.
2. **Is 100 the right proposed chart default?** It matches scale-up and gives a
   concrete review target, but it is provisional until the 2,000-Instance
   singleton and gang benchmarks record API latency, rejection rate, and
   convergence time. The binary must not hide a fallback if the chart choice
   changes.
3. **When does OME need cluster-global destructive admission?** Per-IR windows
   do not prevent simultaneous large IRs from aggregating load. Production
   evidence should decide whether a later OEP adds a global token pool or
   latency-adaptive controller.
4. **Should orphan Pods without InstanceStatus be adopted or deleted during
   normal operation?** Teardown must continue detecting them before finalizer
   removal; normal-path ownership requires a separate, explicit policy.

The next action is review of the admission, rebound, and PodGroup ownership
contracts. Implementation starts only after those three points are accepted;
this proposal applies to OMENative/IR-managed workloads, while RawDeployment and
LWS-backed modes are unaffected.
