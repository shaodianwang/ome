# OEP-0007.4: Compact OMENative InferenceReplica Status

> **Decision amendment (2026-08-17).** This revision replaces the proposed
> configurable per-object migration protocol with one persistence contract.
> OMENative accepts existing values for the three removable fields but never
> treats them as authoritative and omits them from every new IR status write.
> There is no storage marker, migration mode, rehydration path, or dedicated
> storage-compatibility release.

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Failure mode: one status object approaches the storage limit](#failure-mode-one-status-object-approaches-the-storage-limit)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Terminology](#terminology)
- [Current-State Audit](#current-state-audit)
  - [The status currently has two jobs](#the-status-currently-has-two-jobs)
  - [Size evidence](#size-evidence)
  - [Top-level field audit](#top-level-field-audit)
  - [Per-Instance field audit](#per-instance-field-audit)
  - [Nested durable-state audit](#nested-durable-state-audit)
  - [What Pods and EndpointSlices already provide](#what-pods-and-endpointslices-already-provide)
  - [Where Pod derivation fails](#where-pod-derivation-fails)
  - [Consumer audit](#consumer-audit)
  - [Independent size risks and audit findings](#independent-size-risks-and-audit-findings)
- [Proposal](#proposal)
  - [The contract](#the-contract)
  - [Persist the lifecycle journal; derive the live observation](#persist-the-lifecycle-journal-derive-the-live-observation)
  - [Compact persisted row](#compact-persisted-row)
  - [Preserve published summaries](#preserve-published-summaries)
  - [Preserve read authority](#preserve-read-authority)
  - [Do not add Pod labels or annotations](#do-not-add-pod-labels-or-annotations)
  - [Unconditional storage contract](#unconditional-storage-contract)
  - [Status ownership](#status-ownership)
- [Design Details](#design-details)
  - [Preserve decision and publication epochs](#preserve-decision-and-publication-epochs)
  - [The PodCount prior-snapshot exception](#the-podcount-prior-snapshot-exception)
  - [Future columnar representation](#future-columnar-representation)
  - [Consumer migration](#consumer-migration)
  - [Upgrade boundary](#upgrade-boundary)
  - [Failure semantics](#failure-semantics)
  - [API and operator compatibility](#api-and-operator-compatibility)
  - [Implementation sequence](#implementation-sequence)
  - [Observability](#observability)
- [Test Plan](#test-plan)
  - [Differential and unit tests](#differential-and-unit-tests)
  - [Lifecycle and recovery tests](#lifecycle-and-recovery-tests)
  - [Consumer parity tests](#consumer-parity-tests)
  - [Persistence and upgrade tests](#persistence-and-upgrade-tests)
  - [Size and performance tests](#size-and-performance-tests)
  - [KIND tests](#kind-tests)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** for OMENative—OME's direct Pod-management mode—keep each
InferenceReplica's crash-recovery journal, but stop persisting three current Pod
observations in every Instance row. Workload code derives those observations
from the Pod snapshot it already owns, and every OME-owned IR status write
enforces the same smaller storage contract.

One `InferenceReplica` (IR) owns one `InferenceService` (ISVC)
Component—engine, decoder, or router—and one Instance is a single replica made
of either one Pod or an atomic leader/worker gang.
OEP-0007.3 records an approximately 505 KiB IR status at 1,000 Instances and an
approximately 1 MiB status at 2,000 Instances; the exact measurement fixture is
now checked in as `status_size_v1.csv` with real Go API types and named workload
shapes.
Every status update sends the complete object through the API server and etcd,
so richer failure or migration state can exhaust the remaining headroom even
after OEP-0007.3 reduces write count.

The obvious fix—delete `status.instanceStatuses` and reconstruct it from
Pods—is unsafe. The list is both a live observation cache **and** the durable
write-ahead journal for Create, Update, Restart, Migrate, and Delete. Pods do
not exist before Create, disappear during recreate and after Delete, and cannot
preserve the failure information of a Pod that was removed.

This OEP separates those two jobs without adding another CRD:

1. The IR keeps its published aggregate status and its dense per-Instance
   lifecycle records.
2. Epoch-scoped in-memory observations combine those durable records with the
   Pod and EndpointSlice snapshots already taken by the decision and
   publication paths. The proposal does not collapse them into one newer view.
3. The compact persistence contract stops serializing three fields whose
   consumers can be served at the same observation epoch without the row copy:
   `readyPodCount`, `scheduledPodCount`, and `nodesOccupied`.
4. `podCount`, `servingPodCount`, and `availablePodCount` remain persisted in
   the first version. Their previous values are lifecycle or decision inputs;
   replacing them with a fresh snapshot would change behavior timing.
5. In particular, the prior persisted `podCount` records whether the gang was
   complete at the previous publication epoch; replacing both sides of the
   repair check with one current count would break partial-gang-loss repair.
6. Per-Instance `admitted` remains persisted. Status-only row removal and
   teardown can bypass deferred observation, so a count cannot be decremented
   safely after the admitted row disappears.
7. The storage rule is unconditional: existing values of the three removable
   fields are ignored and the next successful status write removes them. The
   fields remain optional in the API so old objects and clients still decode.
8. A later lossless columnar format may range-encode the retained logical rows
   and store only exceptional operations or failures per index. That can save
   substantially more, but it is a separate representation proof rather than a
   reason to infer durable or prior-snapshot state from current Pods.

The intended invariant is narrow: for controller-produced state, lifecycle
decisions, rollout ordering, placement, autoscaling, Conditions, Kubernetes
Event emission, revision retention, and recovery must remain identical,
including which observation epoch feeds a decision. The intentional API
differences are the raw GET shape and raw IR watch timing when only an omitted
field changes. Once any compact write succeeds, rollback to an older writer is
unsupported: a pre-migration reader misinterprets the absence, while a
reader-compatible writer can regrow the oversized payload.

## Motivation

### Failure mode: one status object approaches the storage limit

OEP-0007.3 measured an approximately 505 KiB status for 1,000 Instances and an
approximately 1 MiB status for 2,000 Instances. Batching turns an O(N)-sized
object written O(N) times into an O(N)-sized object written a bounded number of
times; it does **not** make the object smaller.

That leaves three failure modes:

1. **A single update is rejected.** At sufficient replica count or diagnostic
   richness, the API server or etcd rejects the status update. The controller
   then cannot commit the operation that would make progress safe.
2. **Every small observation rewrites a large object.** One Pod becoming Ready
   changes a per-Instance counter, but the request still carries all 2,000
   rows. Watch fan-out and etcd write-ahead-log work scale with the complete
   object, not the changed field.
3. **Failure data can erase the remaining headroom.** `lastFailure.message` is
   not explicitly bounded. Two thousand one-KiB messages alone contribute
   roughly 2 MiB before surrounding JSON. Nonterminal migration records are
   also not currently bounded.

Raising client QPS or Burst cannot correct any of these. It only delivers large
requests faster.

### Goals

1. **Reduce the serialized IR status materially at 2,000+ Instances.** The
   first compact format targets the three proven fields; the checked-in
   measurement gate requires at least 30% reduction for steady singletons and
   50% for steady eight-Pod gangs at 2,000 Instances.
2. **Preserve every current control-plane decision for valid state.** Create,
   Update, Restart, Migrate, Delete, rollout coordination, placement,
   autoscaling, and revision retention must receive semantically identical
   inputs for controller-produced rows.
3. **Preserve crash recovery through zero-Pod windows.** Operation ownership,
   incarnation, canonical ordinal, revision identity, failure evidence, retry
   authority, and migration state remain durable on the IR.
4. **Preserve current read authority.** Cached observations remain acceptable
   for counters; destructive confirmation continues to use live API reads.
5. **Avoid a new workload layer.** The hierarchy remains
   `InferenceService -> InferenceReplica -> Pod`.
6. **Avoid moving etcd bytes from one object to another without benefit.** No
   counter or lifecycle-history annotations are copied onto Pods.
7. **Make the upgrade boundary explicit.** Compact writes may begin only after
   every eligible manager is decision-compatible with absent observations. No
   older writer is a supported rollback target afterward.
8. **Use one storage contract.** Field omission is an invariant, not a tunable
   lifecycle policy, so it has no feature gate or ConfigMap mode.

### Non-Goals

1. **Sparse or columnar lifecycle rows in the first version.** Eliding repeated
   healthy-row values or range-encoding them could save much more, but changes
   the persisted representation of index, incarnation, revision-retention, and
   total-loss recovery state. Dense compact rows are the conservative boundary.
   A lossless columnar candidate remains future work.
2. **Removing `podCount` in the first version.** Its prior published value is a
   recovery input in addition to its current-observation use.
3. **Removing per-Instance `admitted`.** Placement reads the row set after any
   status mutation. A top-level count can become stale when a row-removal or
   teardown write bypasses deferred aggregation.
4. **Changing rollout or lifecycle policy.** Batch size, victim selection,
   gang atomicity, deadlines, retry behavior, availability budgets, and
   operation ordering remain unchanged.
5. **Fixing `minReadySeconds`.** Current availability means nonterminating Ready
   EndpointSlice membership. The compact path preserves that behavior even
   though the API description suggests a readiness-age gate.
6. **Bounding migration queues or diagnostic strings in this change.** Those
   are real object-growth risks, but truncation, sharding, or retention changes
   are separate behavioral decisions.
7. **Deleting unused schema fields for size.** Empty `omitempty` fields cost no
   object bytes. Their eventual API cleanup follows normal deprecation.
8. **Changing controller client QPS/Burst.** Transport tuning is independent of
   object shape.
9. **Changing status ownership.** This proposal introduces no second status
   writer.
10. **Pruning Pod watch predicates.** NodeName, scheduling-gate, readiness, and
   serving transitions keep their current enqueue behavior in the first
   mechanics change, even when a removed field was their only raw status delta.

## Terminology

- **Instance:** one OMENative replica. It may be one Pod or an atomic gang of
  leader and worker Pods.
- **Durable lifecycle record:** the per-Instance facts that must survive a
  controller crash or a zero-Pod interval, such as Phase, Operation, and
  Incarnation.
- **Live observation:** facts about objects that exist now, such as Pod
  readiness, scheduling gate, node, and EndpointSlice membership.
- **Component observation:** an epoch-scoped, in-memory, index-keyed view
  containing both the durable lifecycle record and the objects observed by one
  existing controller path. It is not a new Kubernetes object or a universal
  reconcile snapshot.
- **Legacy stored shape:** an IR written before this change, where live
  per-Instance observations may still be copied inline into status. Their
  presence carries no authority after the consumer migration.
- **Compact persisted shape:** the sole shape written by the amended
  controller. It omits `readyPodCount`, `scheduledPodCount`, and
  `nodesOccupied` from each Instance row while retaining prior-snapshot decision
  fields. This is not a versioned representation.
- **Columnar representation:** a possible later format that losslessly groups
  equal durable and prior-snapshot values by canonical index ranges. This is a
  design candidate, not a selected schema.
- **Decision epoch:** the status and object observations available before the
  current reconcile starts effects. Some existing gates deliberately consume
  values published by the prior reconcile.
- **Publication epoch:** the deferred Pod and EndpointSlice observation after
  the current reconcile's effects, used to publish counters for the next pass.
- **Zero-Pod window:** a valid lifecycle interval in which an Instance status
  exists but no matching Pod does—for example, after old Pods are deleted and
  before recreate starts.
- **RatioBalanced:** the cross-Component rollout policy that gates surge and
  drain actions against the Components' serving-capacity ratio. Mixing
  observations from different epochs can authorize an unsafe action.
- **Authoritative read:** an uncached API read used before destructive effects
  or to prove absence. A cached observation is deliberately not authoritative.

## Current-State Audit

### The status currently has two jobs

`InferenceReplica.status.instanceStatuses` looks like one list but carries two
different classes of information:

```text
durable lifecycle journal                 current observation cache
--------------------------------------    ----------------------------
Index / Incarnation / Phase               PodCount
RunningRevision / TargetRevision          ReadyPodCount
Operation / ActiveOrdinal                 ServingPodCount
LastFailure                               AvailablePodCount
                                          ScheduledPodCount
                                          Admitted
                                          NodesOccupied
```

The IR controller writes an operation before its external effect, resumes it
after restart, and clears it only when completion is proven. Later in the same
reconcile, `aggregateAndWriteStatus` lists Pods and EndpointSlices and copies
the live counters into those same rows. Treating the complete row as either
“durable” or “derived” loses the distinction. The design must split the two
concepts first.

### Size evidence

The strongest observed evidence is the OEP-0007.3 measurement:

| Instances | Observed IR status size |
| ---: | ---: |
| 1,000 | approximately 505 KiB |
| 2,000 | approximately 1 MiB |

PR #1056 added real-type fixtures for steady singletons and gangs, Create,
in-place and surge updates, Delete, Kueue admission, realistic and large
failures, and migration-heavy status. The checked-in golden attributes each
field family's bytes and includes the full normalized status-update request;
the envtest suite also captures API-server managed-fields behavior.

Removing only the three fields and adding no per-object metadata gives these
steady-state numbers:

| Normalized 2,000-Instance request | Legacy bytes | Compact bytes | Reduction |
| --- | ---: | ---: | ---: |
| Single-Pod Instances | 490,520 B | 324,520 B | 166,000 B (33.84%) |
| Eight-Pod gangs on eight distinct nodes | 840,902 B | 324,902 B | 516,000 B (61.36%) |

The compact values are the golden's normalized legacy request minus its
`pod_observation_marginal_bytes`; the direct contract adds no per-object field.

The percentage changes with name lengths, optional fields, topology, and
failures. The useful conclusion is bounded: the three fields are a material
steady-state fraction, especially `nodesOccupied` for multi-node gangs. The
audit does **not** support a claim that the first safe singleton cut removes a
majority of the complete object. These fixtures are regression gates, not a
universal API-server or etcd size limit.

### Top-level field audit

Most top-level summaries are O(1), while `instanceStatuses` is O(Instances) and
nonterminal `migrations` is not currently bounded. Removing the O(1) summaries
offers little size benefit and would break published contracts.

| Field | Derivable from current Pods? | Disposition and reason |
| --- | --- | --- |
| `observedGeneration` | No | Keep. Sequential rollout uses generation skew before the target revision status catches up. |
| `replicas` | Not safely | Keep. `/scale`, print columns, readiness, and coordination read it even while Pods are absent. |
| `readyReplicas` | Yes, with desired shape | Keep as an O(1) published aggregate for ISVC projection, readiness, rollout coordination, and Split placement. HPA/KEDA `/scale` uses `replicas` and `labelSelector`, not this field. |
| `servingReplicas` | Yes, with desired shape | Keep. Availability and RatioBalanced use actual traffic capacity. |
| `availableReplicas` | Yes, from EndpointSlices | Keep. Preserve today's EndpointSlice semantics. |
| `updatedReplicas` | Partly | Keep. It combines committed `runningRevision` with desired revision; Pods alone are wrong in transition windows. |
| `updatedReadyReplicas` | Partly | Keep. It combines revision state with live readiness. |
| `currentRevision` | No | Keep. It means the last fully promoted revision, including during rollback. |
| `updateRevision` | No | Keep. It exists before target Pods and drives coordination and retry authority. |
| `collisionCount` | No | Keep. It is the monotonic ControllerRevision hash salt. |
| `labelSelector` | Deterministic, but required in status | Keep for the Kubernetes scale subresource. |
| `instanceStatuses` | Only in steady state | Keep dense compact lifecycle rows. |
| `retryBlocks` | No | Keep. They are restart-safe retry authority and are currently bounded to the live target plus limited history. |
| `migrations` | No | Keep. Nonterminal entries are authoritative resumable work. |
| `conditions` | No | Keep. `LastTransitionTime` is durable coordination state for Sequential soak. |
| `traffic` | No production writer or reader found | Deprecate separately. It is normally omitted and contributes no current bytes. |
| `coordinationGroupRef` | No production writer or reader found | Deprecate separately. It is normally omitted. |

No deferred-only placement aggregate is added. Per-Instance `admitted` stays in
the row because the row set itself is placement's current durable snapshot. If
a terminal mutation removes an admitted row and deferred aggregation fails—or
teardown bypasses it entirely—a scalar maintained only by publication cannot
know which removed row to decrement after restart.

Concrete failure: start with one admitted row, remove it through terminal
finalization, then fail the deferred publication or enter teardown. Current
placement counts the now-empty row set and returns zero; a separately persisted
count would still return one and could over-credit Single, All, or Split
placement.

### Per-Instance field audit

The verdicts below are the safe first-version boundary, not a claim that every
retained field is irreducible forever.

| Field | Current source and consumer | Zero-Pod risk | Compact persistence disposition |
| --- | --- | --- | --- |
| `index` | Pod label in steady state; planner key, sparse migration identity | Operations and relocated indices outlive Pods | Keep on every dense row. |
| `incarnation` | Pod label in steady state; identity fencing and restart | Increment is committed before old Pods disappear | Keep. |
| `phase` | Lifecycle state machine | Pod health cannot identify operation ownership | Keep. |
| `runningRevision` | Pod revision hash in steady state; rollout and revision GC | In-place labels advance before promotion; old CR must survive zero Pods | Keep. |
| `targetRevision` | Update intent and identity guards | Exists before replacement Pods | Keep; already omitted in steady state. |
| `podCount` | Current Pod list; aggregate classifiers | Previous value proves the last published gang view was complete | **Keep persisted.** |
| `readyPodCount` | `ContainersReady=True`; publication rollups, raw diagnostics, and the empty surge-slot integrity guard read it | No readiness decision needs its prior value; the integrity guard rejects nonzero legacy residue | Derive for internal rollups; stop serializing. Pod queries replace the raw diagnostic cache. |
| `servingPodCount` | `ContainersReady=True` and `ome.io/serving=True`; prior value drives failed-update exemption and RatioBalanced gang-peak accounting | Re-deriving current state advances decisions relative to today's deferred publisher | **Keep persisted.** |
| `availablePodCount` | Ready, nonterminating EndpointSlice target; prior value gates migration tail | Re-deriving current EndpointSlices can advance migration by one reconcile | **Keep persisted.** |
| `scheduledPodCount` | `Pod.spec.nodeName != ""`; the empty surge-slot integrity guard checks zero | No historical meaning outside that legacy-row integrity check | Derive; stop serializing. |
| `admitted` | Pods exist and none carries a scheduling gate; placement counts the current row set | Row removal/teardown may bypass publication, so a scalar aggregate can remain stale | **Keep persisted.** |
| `nodesOccupied` | Sorted unique `Pod.spec.nodeName` values; raw diagnostics and the empty surge-slot integrity guard read it | Current node observation only; migration history lives elsewhere | Derive; stop serializing. Pod queries replace the raw migration/debug cache. |
| `conditions` | No production writer found; the empty gang-surge-slot integrity predicate requires it to be empty | Legacy values may exist | Preserve on read and deprecate separately; normally omitted. |
| `operation` | Write-ahead lifecycle journal | Primary crash-recovery anchor | Keep. |
| `activeOrdinal` | Pod ordinal when unambiguous; canonical surge slot | Ambiguous with two Pods and absent with zero Pods | Keep. |
| `lastFailure` | Captured from a failing Pod before deletion | The source Pod is deliberately deleted | Keep. |

Exactly three fields are safe first serialization removals without changing
the observation epoch of an existing decision. `podCount`, `servingPodCount`,
`availablePodCount`, and `admitted` are the retained exceptions that a
field-name-only audit misses.
The non-obvious readers are `instanceLostGangMember` in
`workload/ops/restart.go`, the failed-update exemption in
`workload/plan_decision.go`, gang-peak accounting in `coordination/ratio.go`,
the migration tail in `workload/ops/migrate.go`, placement admission in
`placement/admission.go`, and the empty surge-slot guard in
`workload/ops/update_status.go`.

### Nested durable-state audit

`InstanceOperation` is operation-scoped rather than steady-state, but most of
its fields are load-bearing. Keep `id`, `type`, `step`, `startedAt`,
`lastProgressAt`, `deadline`, `targetRevision`, `reason`, `surgeIndex`, and
`requestUUID`. They provide idempotency, exact resume position, timeout,
source/target pairing, and migration correlation.

`retryCount` has no current production writer, and `fromNode` and
`hintTargetNodes` are legacy round-trip fields rather than the current
migration source of truth. They still participate in the terminal operation
identity guard. The compact contract therefore preserves all three. They are normally
empty, so removing their schema would save no ordinary object bytes and would
be an independent compatibility change.

`lastFailure` is durable operator evidence. In particular, its `reason` feeds
RolloutStalled and retry evidence; its message can feed RetryBlock reason text.
The first compact format keeps the complete record. `lastFailure.time` is the
controller's observation time, not Pod start time or the kubelet termination
time. The message has no explicit size bound, so this OEP records—but does not
hide—the remaining risk.

`retryBlocks` remain durable. The target revision, state, attempt count, resume
time, and recent failure evidence control whether work may restart after a
process crash. Their current cardinality is small relative to Instances.

`migrations` remain durable. Request UUID, trigger, source and surge indices,
placement hints, phase, timestamps, deadline, and outcome are the executor's
resume state. Contrary to a tempting “bounded history” description,
nonterminal records are never trimmed and queued Accepted records are
unbounded by design. That is a separate growth problem, not Pod-derived state.

### What Pods and EndpointSlices already provide

Every OMENative Pod already carries:

- an owner reference to the IR;
- ISVC, Component, managed-by, Instance index, Incarnation, Runner, revision
  hash, and Pod ordinal labels;
- a PodGroup label for multi-Pod gangs.

The Pod itself provides current node, creation/start time, deletion state,
scheduling gates, runtime and container health, and the controller-managed
serving condition. EndpointSlices provide current traffic membership. A
ControllerRevision maps the revision hash to immutable template content.

No additional Pod metadata is required to derive the three removable fields.

### Where Pod derivation fails

The following transition matrix explains why the lifecycle journal stays on
the IR:

| Situation | Current objects can show | Current objects cannot reconstruct after restart |
| --- | --- | --- |
| Initial Create before Pods | Desired spec and target revision | Admitted Create operation, ID, deadline, and incarnation |
| Partial gang loss | The surviving partial Pod set | Whether the prior published view contained the complete gang |
| Total Pod loss | No current Pods | Initial creation versus loss of an established Instance |
| In-place update | Current image and revision label | Whether the new revision was committed as running |
| Recreate or Restart | Old/new incarnations while either exists | Bumped incarnation and operation during the zero-Pod interval |
| Single-Pod surge | Both ordinal labels while both exist | Which ordinal is canonical after one disappears |
| Gang surge or migration | Both live Pod sets | Source/target relationship, request UUID, and committed source revision |
| Scale-down | Drain holds and EndpointSlice removal while Pods exist | Delete ownership after the final Pod disappears |
| Failed Pod deletion | Nothing from the deleted Pod | Reason, exit code, message, and recorded failure time |

These are not theoretical gaps. Create status is written before Pod creation;
Delete intent is written before drain and removal; Restart increments
Incarnation before deleting old Pods; in-place update changes the Pod revision
label before status promotion; and surge retains `activeOrdinal` until the
replacement is promoted.

### Consumer audit

Every internal reader of the three removable fields must move before compact
writes begin.

| Consumer | What it reads today | Required compact source |
| --- | --- | --- |
| Workload planner and operations | Dense lifecycle rows plus some copied live counters | Durable rows plus the correct decision-epoch view; destructive checks stay live. |
| Create batch rollback | Whole-row equality indirectly includes all three publication-only fields | Compare every retained transition-ownership field; preserve transient observations while restoring prior lifecycle state. |
| IR aggregate and conditions | Per-Instance counters, phases, revisions, failures | Publication-epoch observation plus durable rows; persist identical top-level values. |
| ISVC IR projector | Top-level counts, revisions, conditions | Unchanged. It intentionally does not mirror per-Instance rows. |
| RatioBalanced / surge gates | Top-level serving count, Operation steps, prior per-Instance serving peak | Unchanged persisted serving values. A later format needs an equivalent persisted predicate before removing them. |
| Sequential coordination | Generation/revision skew, dense Instance shape, Ready transition time | Durable rows and unchanged top-level condition. |
| Canary | Current and update revisions | Unchanged. |
| HPA/KEDA `/scale` | `replicas` and `labelSelector` | Unchanged. |
| Multi-cluster placement | Per-Instance `admitted`, failed phase, and top-level `readyReplicas` for Split accounting and endpoint weight | Unchanged durable rows and top-level summaries. No remote Pod informer or aggregate migration. |
| ControllerRevision GC | Current/update and every Instance running/target revision | Unchanged durable union. |
| Internal performance invariant harness | Per-Instance `readyPodCount` to avoid comparing two independently lagging watches | Read retained `podCount` and Phase atomically from the same IR object version. |
| KIND and operator-driven migration | KIND reads `Pod.spec.nodeName`; external clients may use `nodesOccupied[0]` to supply `fromNode` | Keep the Pod query and define the external diagnostic contract. Migration's existing live validation remains authoritative. |
| Test admitters and integration fixtures | Direct per-Instance `admitted` reads/writes and assertions on the three removable fields | Admission fixtures stay unchanged; assert live Pod facts where epoch coupling is not required. |
| Operators using raw IR YAML | All serialized fields | The three fields are absent after the next successful IR status write; query Pods for the current facts. |

The placement row is important. OEP-0018.2 deliberately avoids remote Pod
informers. Retaining `admitted` on each durable row preserves that boundary
without a new remote watch or a second admission journal.

### Independent size risks and audit findings

The audit found adjacent issues that this OEP must not silently fold into a
mechanical refactor:

1. **`lastFailure.message` and copied reason text are not bounded.** A separate
   proposal must decide whether to bound, hash, shard, or relocate full
   diagnostics while preserving operator access and retry semantics.
2. **Nonterminal migrations are unbounded.** Terminal aging does not constrain
   queued or stuck work. This can grow an IR independently of replica count.
3. **`admitted` is not mirrored back into the caller's same-pass IR snapshot.**
   Stored status is correct, but an in-memory caller can observe the prior
   value. Compaction must not accidentally mix this bug fix with representation
   changes; it deserves a standalone behavior decision and test.
4. **`minReadySeconds` is not enforced by availability.** Compaction reproduces
   current EndpointSlice behavior exactly.
5. **Several existing retention values are hardcoded.** Examples include
   `defaultRevisionRetention=10`, `maxHistoricalRetryBlocks=3`, the audit
   ledger's `maxTerminalEntries=200`, and migration execution caps/window of
   3 in flight, 10 per hour, and one hour. If later work changes these bounds,
   the replacement values must be supplied by configuration rather than adding
   another binary default.
6. **Current node and start time are already live Pod data.** IR status does not
   preserve Pod start-time history. Prior migration nodes already live in
   `MigrationStatus.fromNode` and the audit ledger. Compaction loses neither.
   A future need for deleted-Pod timestamps requires a bounded history design;
   copying them to another Pod annotation would not survive deletion.

## Proposal

### The contract

For every OME-owned IR status write:

1. The IR controller MUST keep durable lifecycle state separate from live
   Pod/EndpointSlice facts and MUST preserve the current decision and
   publication observation epochs.
2. Every lifecycle, rollout, placement, retention, and aggregate decision MUST
   remain equivalent for the same supported sequence of controller-produced
   status, cache, and live-read states—not merely for a perfectly synchronized
   fixture. Values in the three removable fields are caches. An impossible
   hand-edited combination is normalized rather than treated as lifecycle
   authority.
3. The IR MUST continue persisting dense Instance rows with `index`,
   `incarnation`, `phase`, `runningRevision`, `targetRevision`, `podCount`,
   `servingPodCount`, `availablePodCount`, `admitted`, `operation`,
   `activeOrdinal`, `lastFailure`, per-Instance Conditions, and any other
   non-compacted legacy fields already present.
4. The IR MUST NOT persist `readyPodCount`, `scheduledPodCount`, or
   `nodesOccupied`. Existing values are accepted on read, ignored for decisions,
   and removed by the next successful status write.
5. The IR MUST continue publishing all existing top-level aggregate,
   revision, selector, retry, migration, and condition fields with identical
   semantics.
6. A failure to obtain a required live observation MUST fail closed. It MUST
   NOT be converted into a zero-valued observation that authorizes a create,
   drain, delete, promotion, placement win, or revision sweep.
7. Destructive paths MUST retain their existing authoritative-read and UID
   precondition rules. A compact cached observation does not weaken them.
8. Compaction MUST introduce no more Pod or EndpointSlice LISTs than the
   corresponding current path. It may share formulas and an observation within
   one controller pass, but it MUST NOT pretend that the IR and ISVC reconcilers
   share process-local snapshots.
9. Status writer ownership and conflict-retry boundaries MUST remain unchanged.
   Every status write family MUST clear the three fields from the freshly read
   conflict-retry candidate so a non-aggregate write cannot regrow them.
   Compaction adds no Pod-to-status controller and no writer owned by the ISVC
   controller.
10. Pods MUST NOT receive copied counter, operation, or history annotations for
    this design.
11. This persistence invariant MUST NOT have a feature gate, ConfigMap mode, or
    per-object marker. It is not a user-selectable lifecycle policy.
12. The empty gang-surge-slot predicate MUST ignore exactly the three removable
    fields while retaining every other existing check. Terminal finalization
    keeps its existing lifecycle-owned identity and MUST NOT acquire a
    dependency on the removable caches. Controller-produced rows already
    guarantee that any nonzero removable observation implies a nonzero
    `podCount`; a hand-edited cache value does not own lifecycle work.

### Persist the lifecycle journal; derive the live observation

The Component observation is a process-local read model, not a second source of
truth. Conceptually:

```text
ComponentObservation
  durable[index] -> IR InstanceStatus row at this observation's read epoch
  live[index]    -> Pods, counts, serving/admission/node facts
  availablePods  -> EndpointSlice target set
```

Decision observations use the reconcile-entry rows. Publication observations
use the fresh IR rows re-read inside the existing status conflict-retry
boundary.

Consumers must ask for the class of fact they need. For example:

- Restart asks the durable row whether the last publication saw a complete
  gang and compares it with the current live Pod set.
- Publication asks the live view how many desired Pods are ContainersReady;
  pre-effect decisions continue reading the prior persisted serving and
  availability values where they do today.
- Revision GC asks the durable rows which full revision names are protected.
- Delete asks the durable Operation who owns work, then performs its existing
  live absence confirmation before removing the row.

This explicit split prevents a current count from overwriting a prior-snapshot
decision input in memory.

### Compact persisted row

The compact contract deliberately keeps the list dense. A steady row remains
roughly:

```yaml
- index: 42
  incarnation: 1
  phase: Ready
  runningRevision: example-engine-12345678
  podCount: 1
  servingPodCount: 1
  availablePodCount: 1
  admitted: true
```

Transition rows additionally carry target revision, Operation, failure, or
ordinal state exactly as today. The three removable fields are absent.

The compact shape is a persistence rule, not a signal to replace live facts
with zero in memory. The publication path computes the complete transient row,
derives top-level summaries, then clears the three fields on the persistence
candidate. It compares that normalized candidate—not the transient full
row—with the status re-read by the existing conflict-retry closure before
deciding to write. Otherwise Ready and UpdatedReady would collapse to zero, or
every steady reconcile would see the temporary fields as a false status delta.

Why stop here? Dense lifecycle rows preserve existing planner cardinality,
sparse-index handling, total-loss identity, and revision retention. A later
sparse representation could make Ready/default rows implicit, but it would need
a separate proof for Incarnation and migrated-index recovery. The first version
takes the modeled three-field, lower-risk reduction and leaves that harder
problem alone.

### Preserve published summaries

The controller still updates:

- `replicas`, `readyReplicas`, `servingReplicas`, `availableReplicas`;
- `updatedReplicas`, `updatedReadyReplicas`;
- `currentRevision`, `updateRevision`, `collisionCount`;
- `observedGeneration`, `labelSelector`, and Conditions;
- RetryBlocks and Migrations.

Per-Instance `admitted` also remains unchanged. A scalar summary would lose the
row identity needed under the current writer/read contract when a terminal
mutation removes an Instance without a successful publication pass. A future
admitted-index journal or new authoritative Pod read could replace it, but that
would add durable state or API traffic and is outside this mechanics-only cut.

These status writes remain necessary. The parent ISVC is enqueued by IR status
updates, `/scale` reads IR status directly, coordination reads the aggregates,
and Sequential soak reads a condition transition timestamp. Eliminating all
status writes would stall those consumers.

### Preserve read authority

The existing distinction remains:

| Purpose | Read source | Reason |
| --- | --- | --- |
| Counters, non-destructive observation, and aggregate publication | Manager cache | Eventual observation is acceptable and avoids a live LIST per reconcile. |
| Drain, delete, recreate boundary, teardown, and destructive scale-down | API reader | Stale absence can delete or recreate the wrong object. |
| Endpoint availability for published counters | Cached EndpointSlices | Matches current status semantics. |
| Drain completion | Existing live/conservative drain path | Traffic removal is a safety boundary. |

“Derive from Pods” therefore does not mean “perform one universal live LIST.”
That would increase API load and change timing. It also does not mean “trust
the cache for deletion.”

The views also retain their current membership rules. Normal aggregation uses
the selector-scoped cached set, while destructive scale-down uses a live set
owned by the current IR UID and rejects malformed owned Instance indices. Each
observation is tagged with its source and scope; a helper may share counting
formulas, but it may not substitute one membership rule for another.

### Do not add Pod labels or annotations

Existing labels already identify the Instance, Incarnation, Runner, revision,
and ordinal. The three compacted fields come directly from Pod spec/status.
EndpointSlices feed the retained availability counters.

Pod metadata is the wrong durability boundary for lifecycle history:

- Create intent exists before the first Pod.
- Delete intent must survive after the last Pod.
- Recreate deliberately passes through zero Pods.
- A gang cannot atomically patch every member.
- The member carrying copied history can be the member that disappears.
- A surge cannot atomically flip a “canonical” label between two Pods.

Copying state to Pods would add writes, move bytes rather than remove them, and
still lose the recovery record during deletion.

### Unconditional storage contract

The three fields are optional API compatibility fields, not a selectable
storage format. The manager accepts old IRs that still contain them, but every
status persistence path clears them from the fresh conflict-retry candidate
before calling the API. The first successful write therefore moves an object
to the compact shape as part of its ordinary lifecycle; no fleet sweep or
special conversion reconcile is needed.

This rule has no ConfigMap value, feature gate, status marker, schema preflight,
or rehydration path. Those mechanisms would create a second state machine for a
distinction that workload decisions no longer observe. The absence of a switch
is deliberate: omitting redundant cache fields is the status storage contract,
not a user-facing behavior default.

Legacy values are not validated before removal. For controller-produced rows,
normal aggregation already guarantees that Ready and Scheduled are between zero
and `podCount`, and that occupied nodes imply Pods. A hand-edited or stale cache
value does not become durable lifecycle evidence merely because it was stored.
The retained fields remain the authority for transition ownership and recovery.

### Status ownership

The IR controller remains the sole Kubernetes controller that writes IR status,
but production code has multiple write families inside that ownership boundary:

| Concern | Decision owner | Persistence path |
| --- | --- | --- |
| Per-Instance lifecycle transitions | Workload operation/state-machine code | IR mutation adapter and its conflict retry |
| RetryBlock and Migration mutations | Workload/IR lifecycle code | IR status adapters and their conflict retries |
| Collision count and revision promotion | IR revision logic | Dedicated IR status mutators |
| Live counters, Ready, and RolloutStalled Conditions | IR aggregate path | Deferred IR status update |
| Gang and workload aggregate Conditions | Workload gang/state-machine code | Dedicated IR condition mutator and its conflict retry |
| Terminal row removal and teardown | IR/workload teardown logic | Dedicated IR row-removal and teardown status mutators |

Every path that can call `Status().Update` MUST pass through one persistence
normalizer, or an audited equivalent with the same tests. The normalizer clears
the three removable fields on every write. Applying it inside each existing
conflict-retry boundary prevents a RetryBlock or migration-only update from
regrowing data copied from the freshly read object.

The ISVC controller, Pod watcher, placement controller, and any new controller
remain read-only for IR status; a second writer would recreate the status
conflict that this ownership boundary prevents.

## Design Details

### Preserve decision and publication epochs

The implementation extends the existing workload observation/snapshot seam
instead of adding an unrelated lister, but it does **not** collapse the
reconcile into one universal snapshot.

1. The **decision epoch** starts from the IR status read at reconcile entry.
   Existing operation paths add their current cached or authoritative Pod
   snapshots where they already do today. Prior persisted `podCount`,
   `servingPodCount`, and `availablePodCount` remain prior observations.
2. The **publication epoch** runs in the existing deferred aggregate tail after
   effects. It lists cached Pods and EndpointSlices, computes all current
   counters, persists the retained fields, and
   publishes unchanged top-level summaries.

The epochs share counter formulas and indexing helpers, not necessarily object
snapshots. The IR reconciler and ISVC coordination reconciler are separate
passes and cannot share process-local memory. The compact contract therefore retains
per-Instance serving and availability for RatioBalanced and migration instead
of adding another coordination Pod LIST or changing their observation time.

A consumer that needs only top-level IR status does not list Pods. A remote
placement consumer never lists Pods. A destructive operation still performs
the live confirmation required by its existing state machine.

The implementation must avoid converting the durable row in place. In
particular, replacing its `podCount` with the current count before Restart
evaluation destroys the prior-snapshot evidence described next.

### The PodCount prior-snapshot exception

`podCount` appears live-derived, but `instanceLostGangMember` compares two
different times:

```text
previous persisted PodCount >= expected    # the prior publication was whole
current live Pod count       < expected    # a member is now missing
current live Pod count       > 0           # survivors still hold placement
```

This catches a gang member that disappears after the published set becomes
complete but before Phase reaches Ready. If both sides use the current Pod
list, the first predicate becomes false and Restart never repairs the gang.
`podCount` is not a monotonic “once complete” latch: deferred aggregation
overwrites it on each pass. Its safety value comes from preserving the existing
decision-before-publication ordering.

The compact contract therefore retains `podCount` with its current write ordering. A later
format may replace the accidental dual use with an explicit per-Incarnation
“materialized” latch, but only after:

1. the latch is committed when a complete set is first observed;
2. Incarnation changes clear it atomically;
3. partial and total loss are distinguished exactly as today; and
4. the byte and complexity benefit justifies another schema field.

That later change is not required to deliver the first material saving.

### Future columnar representation

A range-encoded representation is a promising way to remove the repeated JSON
keys and values that remain after direct compaction. In a 500-Instance steady state,
for example, membership, Phase, RunningRevision, and Incarnation often contain
long runs of the same value, while only a small number of indices carry an
Operation, LastFailure, Condition, or nondefault active ordinal.

This is lossless storage compaction, not broader Pod derivation. A safe format
must also encode the retained prior-snapshot inputs that are absent from a
minimal lifecycle-only sketch: `podCount`, `servingPodCount`,
`availablePodCount`, and per-Instance `admitted`. It cannot recompute those
values from a newer Pod or EndpointSlice view without changing the decisions
identified by this audit. Revision groups must retain collision-safe full
ControllerRevision identities. An abbreviated hash alone cannot be assumed to
identify a revision unless the format maps it unambiguously to the full name.

Conceptually, a later format could look like this:

```yaml
instanceState:
  members: "0-499"
  phases: [{value: Ready, indexes: "0-499"}]
  runningRevisions: [{value: example-engine-2f32f6fe, indexes: "0-499"}]
  incarnations:
  - {value: 1, indexes: "0-8,10-114,116-499"}
  - {value: 2, indexes: "9,115"}
  podCounts: [{value: 1, indexes: "0-499"}]
  servingPodCounts: [{value: 1, indexes: "0-499"}]
  availablePodCounts: [{value: 1, indexes: "0-499"}]
  admitted: "0-499"
  activeSlotOne: "6,43,175"
  entries:
  - index: 9
    lastFailure: {podName: example-engine-9, reason: Error, exitCode: 143}
```

The example is illustrative, not a selected API. A follow-up design must prove:

1. a canonical index-set grammar with sorted, disjoint, nonoverlapping ranges;
2. exact membership coverage or a versioned absence, zero, or default rule for
   every required column, unique exceptional entries, and rejection of
   references outside `members`;
3. deterministic decode to the same index-sorted logical Instance rows and
   deterministic re-encoding, so repeated reconciles do not write;
4. bounded decoding of malformed ranges and fail-closed handling of an invalid
   representation;
5. fresh decode, mutation, validation, and re-encode inside every existing
   status conflict-retry boundary;
6. lossless round trips for zero-Pod windows, sparse migrated indices, every
   operation step, failures, Conditions, admission, and prior-snapshot fields,
   with unambiguous precedence between grouped columns and exceptional entries;
7. full decision, Event, recovery, upgrade, and downgrade parity, plus an
   explicit raw API contract and storage-version strategy; and
8. measured best- and worst-case sizes. Randomly distributed values can defeat
   range compression, so the format needs a deterministic fallback rather than
   assuming the steady-state example is representative.

The checked-in measurement fixtures provide the dense legacy and compact
baselines for a future comparison; they do not implement or measure this
candidate. A follow-up OEP must define the columnar migration and rollback
contract and add its measurements. This OEP does not authorize columnar writes.

### Consumer migration

Internal migration follows these rules:

1. **Workload core:** accept durable and live observations separately. No
   operation reads a removable raw status field directly.
2. **Aggregation:** compute the same top-level counters from the live view plus
   durable revision/phase state. It still persists serving and availability,
   but not the three removable fields.
3. **Coordination:** continue using top-level aggregates, durable Operations,
   and persisted per-Instance serving values. Compaction adds no Pod LIST to the
   ISVC reconciliation path.
4. **Placement:** continue counting per-Instance `admitted` from the current row
   set. No fallback, remote Pod informer, or observation-epoch change is added.
5. **Revision retention:** continue unioning top-level current/update and every
   durable running/target revision. No live-only revision may be swept while an
   operation can resume against it.
6. **ISVC projection, canary, and autoscaling:** continue reading the same
   top-level fields.

A raw value influencing a production decision outside the observation builder
is a review blocker. Wire conversion, transient snapshot cloning, create
rollback, and persistence clearing may copy the fields but cannot treat them as
authority; a focused repository test allowlists those mechanical accesses. The
empty gang-surge-slot predicate ignores the fields while retaining every other
existing check.

### Upgrade boundary

The minimum supported forward-upgrade predecessor contains the observation seam
from PR #1061 and the consumer migration from PR #1066. Its lifecycle consumers
derive the three observations from the workload observation, except that its
empty gang-surge-slot guard still contains redundant cache checks dominated by
`podCount` on controller-produced rows. An older leader therefore makes the same
decisions on supported state even though it may temporarily repopulate the
fields. The next compact write removes them again.

That mixed-writer window is logically safe but not storage-safe for an IR whose
legacy shape already exceeds the configured request limit. At that scale, an
older leader can fail while trying to regrow the larger payload. Operators must
finish the manager rollout without allowing an older writer to regain
leadership after the first compact write. The compact writer itself can still
shrink such an object because its outgoing request omits the three fields.

A normal rolling upgrade from any earlier binary is unsafe. Such a binary may
become leader after a compact write and interpret absent fields as authoritative
zeros before publication repopulates them. If the reader-compatible baseline
has not been deployed, operators must use a non-overlapping manager upgrade that
prevents the old binary from regaining leadership after the first new manager
starts writing status.

Rollback is unsupported once any compact status write succeeds. A pre-migration
reader can interpret absent fields as authoritative zeros, while the
reader-compatible predecessor still regrows the larger payload and can hit the
same request limit that prompted this OEP. There is no marker that can prove
which objects were compacted and no rehydration operation. This one-way boundary
is the explicit cost of avoiding a second storage-migration state machine.

No CRD ordering is required because this OEP adds no field. The three legacy
fields stay optional in the served schema so both old stored objects and old
client payloads decode; the controller simply stops publishing them.

### Failure semantics

| Failure | Required response |
| --- | --- |
| Cached Pod or EndpointSlice LIST fails | Return an error; retain the prior published observation and start no decision that depends on fabricated zeros. |
| Live destructive LIST fails | Fail closed exactly as today; perform no external effect. |
| Pod has missing/malformed identity labels | Preserve existing orphan/fencing behavior; do not bucket it into a guessed Instance. |
| Status update conflicts | Re-read, rebuild against the fresh durable status, clear the three fields again, and retry the complete mutation. |
| Manager crashes after compact status but before an effect | Durable Operation and identity fields resume the same work. |
| EndpointSlice lags Pod readiness | Publish the same temporarily lower availability as the current implementation. |
| Remote placement cannot read an IR | Fail/retry according to its current semantics; do not infer admission from missing data. |
| Status update is rejected for size | Surface the error and preserve durable work. Compaction reduces the common shape but does not claim to bound unbounded diagnostics or migrations. |

### API and operator compatibility

There is an unavoidable limit to the “zero behavior change” claim: removing
serialized bytes changes a raw API GET. An operator who reads
`instanceStatuses[42].nodesOccupied` will no longer find that field in
persisted status after the next write, even though the same current node remains
on the Pod. A change only to an omitted field may also stop producing an IR
status write and watch event when no retained aggregate changes. Internal
controllers must be independent of that event.

This OEP therefore makes two narrower guarantees:

1. **Workload and control-plane behavior is identical for controller-produced
   state.** No create, delete, drain, rollout, placement, autoscaling, condition
   transition, Kubernetes Event emission, or recovery decision changes.
2. **The raw observation API changes deliberately.** The three fields are
   compatibility-only caches, and field-only IR watch events are not preserved.

The CRD fields remain optional in OpenAPI and generated clients so old objects
and payloads decode. Callers must handle their absence. Current readiness comes
from `ContainersReady=True`, scheduling from a nonempty `Pod.spec.nodeName`, and
node occupancy from the unique nonempty `spec.nodeName` values of the
Instance's Pods.

No reconstructed status surface is introduced. Operators that need these live
facts query Pods, which are already the authoritative source. This is the API
tradeoff that buys the status reduction; claiming byte-identical raw GET or
watch behavior would be false.

### Implementation sequence

The work is split so each PR has one reviewable responsibility:

1. **Measurement and fixtures (landed in PR #1056).** Add named, reproducible
   size attribution for steady singletons, gangs, creates, updates, deletes,
   failures, and migrations using real API types and complete status-update
   request bodies. Compare legacy and compact dense rows.
2. **Observation seam (landed in PR #1061).** Make the durable-plus-live inputs
   explicit while preserving separate decision/publication epochs and each
   path's source and owner-filtering scope. Differential-test old and new
   decisions.
3. **Consumer migration (landed in PR #1066).** Move decision consumers to the
   correct epoch-scoped view. Its empty-slot predicate retains redundant cache
   checks that are decision-equivalent on controller-produced rows. Retained
   prior-snapshot and admission fields and placement behavior stay unchanged.
4. **Unconditional compact writer.** Remove those last redundant empty-slot
   checks and route every status writer family through one persistence boundary
   that clears the three fields. Keep the transient publication observation
   available for same-pass aggregation.
5. **Optional later proposals.** Decide sparse rows, explicit materialization,
   diagnostic bounds, and migration queue bounds independently.

Each production-code PR must keep its diff focused. Large differential test
matrices are expected; reviewers should be able to separate them from the
small persistence-mechanics change in the PR description.

### Observability

This mechanics change adds no runtime metric. Existing
`ome_isvc_status_update_total{controller="ir",result=...}` telemetry continues
to report outcomes for the IR status paths it already instruments, and
reconcile errors continue to surface through the existing controller logs.
Size and allocation evidence comes from the checked-in fixtures, envtest suite,
and benchmarks rather than a second serialization on the reconcile hot path.
No per-object storage-migration metric is needed because this design adds no
storage-migration state machine.

## Test Plan

### Differential and unit tests

Build paired fixtures at both observation epochs:

1. a legacy status carrying values in the three removable fields plus current
   Pods/EndpointSlices; and
2. the equivalent durable status with those fields absent plus the same current
   objects.

The persisted counters MUST intentionally lead and lag the current objects in
both directions. In particular, include prior-serving/current-not-serving,
prior-not-serving/current-serving, prior-available/current-unavailable, and the
reverse. Fixtures in which status always matches Pods would not test the
decision/publication boundary that keeps `podCount`, `servingPodCount`, and
`availablePodCount` persisted.

The three removable fields also lead and lag their current Pod facts in both
directions, including a changed node set. For controller-produced combinations,
their old values must influence no control decision, while the publication
result remains identical. Test impossible cache combinations, such as
`podCount=0` with a nonzero Ready count, separately to prove deterministic
normalization rather than old/new decision parity.

For each pair, assert identical:

- Component plan and ordered actions;
- operation admission and resume decisions;
- update, restart, migration, delete, and scale-down gates;
- aggregate status, Conditions, and Kubernetes Events;
- ISVC projection and rollout-coordination observations;
- ControllerRevision retention set.

Differential tests cover every effect-authorization site, including the
execute-time coordination gate and per-Instance Create and Update admission.
`workload.Plan` coverage alone is not sufficient because it does not own every
execute-time gate.

Cover zero, one, and N Instances; single-Pod and eight-Pod gangs; dense and
sparse indices; terminating, duplicate, malformed, foreign, and stale-
Incarnation Pods; and EndpointSlice present, absent, and lagging states.
Run selector-scoped aggregation and current-owner destructive views separately,
including a selector match owned by another UID and a malformed index owned by
the current IR.

Tests must verify each field formula independently:

- Ready = `ContainersReady=True`;
- Serving = Ready and controller serving condition True;
- Available = nonterminating Ready EndpointSlice target;
- Scheduled = nonempty `spec.nodeName`;
- Admitted = nonempty Pod bucket and no admission gate;
- Nodes = sorted unique nonempty node names.

Persistence-boundary tests start with nonzero removable fields and prove that
every writer family clears them on the fresh conflict-retry object. Focused
tests prove that empty gang-surge-slot selection ignores exactly those three
fields while preserving every other existing check.

### Lifecycle and recovery tests

At minimum:

- initial and batched Create before any Pod exists;
- partial gang loss before Ready, after Ready, after manager restart, and when
  the first Restart status write fails before deferred publication;
- total gang loss;
- RecreatePod zero-Pod interval;
- in-place revision-label-ahead-of-promotion;
- single-Pod SurgeThenDrain with both ordinals;
- gang surge source/target pairing and abandon paths;
- manual and automatic migration in every phase;
- DeleteOwned scale-down, rebound, and zero-Pod finalization;
- IR teardown with statusless Pods;
- RetryBlock Backoff, Held, release, and retry-in-progress;
- paused and admission-gated deadline parking;
- failed Pod capture and LastFailure preservation;
- partition, canary rollback, and ControllerRevision GC.

Crash/restart tests stop immediately before and after every durable status write
and every Pod/PodGroup effect, then reconstruct a new reconciler from persisted
objects. Outcomes must match whether the starting IR contains legacy cache
values or already has the compact shape.

### Consumer parity tests

Test the complete consumer matrix:

- ISVC component projection and top-level Ready;
- RatioBalanced, MaxUnavailable, MaxSurge, and gang serving-peak accounting;
- Sequential handoff and Ready-condition soak time;
- canary target selection and rollback;
- Single, All, and Split multi-cluster placement, including lost admission and
  terminal failure;
- `/scale`, HPA/KEDA selector behavior, and print columns;
- the internal performance harness's Ready-without-Pods invariant through the
  retained `podCount` from the same IR object version as Phase;
- performance test admitters and real-Kueue multi-cluster fixtures;
- revision retention during mixed-revision zero-Pod transitions.

Placement tests continue counting current per-Instance `admitted` values. Pin
the admitted-row removal, teardown, and failed-publication cases that make a
separate scalar count unsafe; legacy cache residue and compact rows must return
the same Single, All, and Split decisions.

Event-liveness tests also isolate changes that affect only an omitted field:

- a partial gang member becoming ContainersReady below the aggregate Ready
  threshold while serving remains false;
- a NodeName-only bind after the Instance is already admitted; and
- a scheduled or node change while every retained status field is fixed.

For each case, prove that suppressing the omitted-only IR status update does not
change any parent ISVC control decision, traffic decision, placement result,
Condition, or Event. This turns the static consumer audit into a regression gate
for the IR-to-ISVC watch edge.

### Persistence and upgrade tests

Inject:

- cache lag and watch gaps;
- Pod, EndpointSlice, IR, and ControllerRevision read errors;
- 409 conflicts, 413 rejections, 429 throttling, 5xx responses, and context
  cancellation;
- malformed labels and changed owner UID; and
- a crash immediately before and after a compact status write.

Upgrade tests exercise:

- an old stored object with all three fields populated, followed by each status
  writer family;
- a bounded object passing between two reader-compatible managers while one
  writer still publishes the legacy shape, proving decision safety and final
  convergence;
- an oversized legacy object whose older writer is rejected, followed by a
  successful compact write and completion of the rollout without returning
  leadership to the older writer;
- old optional fields and legacy Operation fields decoding through the current
  schema and generated clients.

The release notes state that rollback to any older writer is unsupported after
a compact write. There is no automated downgrade path to test.

Every status writer family gets a persistence-boundary test: single and
batch Instance mutation, standalone RetryBlock mutation, migration accept,
phase mutation, import/prune sync, and automatic-relocation outcome, collision
count, current-revision promotion, aggregate publication, the dedicated gang
Condition writer, terminal row removal, and teardown. None may persist the three
fields. Repeated reconciles after compaction must be idempotent and must not
create a status write storm.

Every case asserts no unintended Pod create, drain, patch, delete, PodGroup
delete, placement win, or revision sweep.

### Size and performance tests

Measure serialized bytes by field family at 0, 1, 100, 1,000, 2,000, and 5,000
Instances for:

- steady singleton;
- steady eight-Pod gang with realistic node placement;
- Creating with Operation;
- in-place and surge update;
- deleting wave;
- Kueue-gated Pods;
- realistic and intentionally large LastFailure;
- migration-heavy status.

Record legacy versus compact bytes for the complete normalized IR status-update
request with realistic spec and metadata. The checked-in golden omits
API-server-generated managed fields so it remains deterministic; envtest
separately records the actual request and served-object managed-fields behavior.
Also record bytes per write, write count, Pod/EndpointSlice LIST count, decoded
object count, reconcile CPU, allocation, and peak memory. At 5,000 Instances,
the checked-in normalized steady-state gate caps the compact singleton and gang
requests at 825,000 bytes and requires at least 400,000 bytes/30% singleton
reduction and 1,250,000 bytes/50% gang reduction. The controller binary does not
contain an object-size magic value.

Read-count tests assert:

- no new per-Instance Pod LIST loop;
- no more cached Pod or EndpointSlice LISTs in each decision/publication path
  than the corresponding current path;
- no remote Pod watch or LIST for placement;
- existing live safety reads remain present rather than being “optimized” away.

### KIND tests

The existing OMENative KIND suite is authoritative for kubelet and
EndpointSlice timing. Focused tests cover partial gang loss, recreate, in-place,
surge, migration, DeleteOwned scale-down, Kueue gating, Sequential soak, and
RatioBalanced behavior with compact persistence.

The envtest suite also sends a complete 2,000-Instance status request through a
real API server and verifies the served object and managed fields. Before Beta,
run a real scale trial with 2,000 single-Pod Instances and a gang
shape whose total Pod count is at least comparable. The exact larger gang trial
is recorded by the measurement milestone rather than invented here.

## Graduation Criteria

### Alpha

- The measurement fixtures and field attribution are checked in.
- Every internal consumer of a removable field uses the correct epoch-scoped
  observation; admission and placement remain on the durable row set.
- Every IR status writer family clears the three fields from its fresh
  conflict-retry candidate.
- Differential tests report no decision mismatch across the complete
  valid-state matrix, including stale legacy cache values. Separate tests cover
  normalization of impossible hand-edited combinations.
- Focused KIND lifecycle tests pass with compact persistence.
- Admitted-row removal and teardown prove identical placement behavior.
- API documentation identifies the fields as compatibility-only caches and points
  operators to Pods for current observations.

### Beta

- A 2,000-Instance scale trial demonstrates the approved size reduction and
  headroom for the named bounded singleton, gang, transition, failure, and
  migration fixtures. Unbounded diagnostics and migration queues are reported
  separately rather than covered by this claim.
- No reconcile path performs more Pod or EndpointSlice LISTs than its current
  counterpart, and placement opens no remote Pod informer.
- A forward rollout from the reader-compatible predecessor is tested at both a
  bounded size and a size where the legacy writer is rejected, including final
  convergence without returning leadership to the older writer.
- The unsupported rollback boundary is included in release documentation.

### Stable

- Fleet soak shows no lifecycle, rollout, placement, autoscaling, or revision
  retention regression.
- Remaining growth risks—unbounded diagnostics and nonterminal migrations—have
  explicit accepted follow-ups or documented bounds.
- Any proposal to make Instance rows sparse has its own recovery proof and is
  not implied by this OEP's completion.

## Drawbacks

1. **The code carries two views of one Instance.** This is deliberate but adds
   type and test complexity. The current code already has two meanings in one
   struct; making them explicit trades hidden coupling for visible structure.
2. **Raw compact status is less convenient for debugging.** Operators must read
   Pods for the three fields.
3. **Dense lifecycle rows remain O(N).** Direct compaction materially reduces the
   coefficient but does not make IR status O(1).
4. **Top-level status still rewrites the complete IR object.** Kubernetes status
   update semantics still carry dense durable rows. This proposal reduces bytes,
   not the fundamental whole-object update model.
5. **The first format does not bound every shape.** Large LastFailure text and
   unbounded nonterminal migrations can still exceed the object limit.
6. **Rollback has a hard floor.** After the first compact write, a binary that
   treats the three fields as authoritative cannot safely become leader.
7. **Raw watch timing changes.** A node or scheduling observation
   that changes no retained aggregate may no longer cause an IR status event.
   Consumers that need those changes must watch Pods.

## Alternatives

1. **Derive the complete Instance status from Pods.** Rejected. Create/Delete
   intent, zero-Pod operations, Incarnation, canonical ordinal, failure history,
   migration pairing, and committed revisions do not survive on Pods.
2. **Add another ReplicaSet-like CRD below the IR.** Rejected. IR is already the
   ReplicaSet-like lifecycle owner. A new object adds watches, writes, ownership,
   cleanup, and another status conflict surface without removing the need for a
   durable journal.
3. **Write lifecycle history onto Pod labels or annotations.** Rejected. Pods
   are deleted by the operation the history must recover, gang patches are not
   atomic, and this shifts bytes rather than removing them.
4. **Keep all per-Instance fields and only batch writes.** OEP-0007.3 does this.
   It reduces amplification but cannot prevent one oversized request.
5. **Make lifecycle rows sparse or columnar immediately.** Deferred pending the
   common measurement. It offers the largest steady-state saving but needs a
   lossless schema, canonical range validation, bounded decoding, full
   conversion/rollback support, and a raw API decision. The future columnar
   section records the proof boundary rather than treating it as rejected.
6. **Remove `podCount` with the other counters.** Rejected for this compaction. It
   breaks partial gang loss before Ready unless an explicit durable
   materialization latch lands first.
7. **Replace per-Instance `admitted` with one scalar count.** Deferred. Under
   current writers, a row can be removed without a successful publication pass,
   and the scalar cannot identify whether to decrement after restart. An
   admitted-index journal or new authoritative Pod read could solve that, but
   either expands state or API traffic beyond this mechanics-only format.
8. **Move all detailed state to a ConfigMap.** Rejected. It remains an etcd
   object with the same size limit, introduces cross-object transactions, and
   complicates owner recovery. The existing audit ConfigMap is history/dedup,
   not executor state.
9. **Shard status across ConfigMaps.** Not selected for live lifecycle state.
   Sharding can bound individual objects but creates a distributed journal whose
   atomicity, GC, and revision consistency are harder than the current IR.
10. **Increase API server or etcd limits.** Rejected as the primary fix. It
   raises the cliff and storage amplification for every watcher without
   removing redundant per-Instance observation.
11. **Use metadata-only or partial-object status patches.** Insufficient.
    Patches may reduce client request construction, but etcd stores and watches
    the complete resulting object; the terminal object-size problem remains.

## Open Questions

1. **What real-cluster headroom should Beta require?** The checked-in byte gates
   protect the normalized fixtures, but they do not define a universal
   API-server or etcd limit. The scale trial must record its configured limits,
   maximum observed request, and remaining headroom. Owner: implementation
   author; required before Beta.
2. **Should `podCount` be replaced later by an explicit materialization latch?**
   Only if the clearer invariant and any extra savings justify schema and
   migration complexity. Owner: OMENative maintainers; not required for this
   compaction.
3. **How should full failure diagnostics be bounded without losing operator
   access or changing retry reasons?** Options include a configured length
   bound, a bounded audit ledger, or a separate sharded diagnostic resource.
   This OEP does not pick one.
4. **How should queued and stuck nonterminal migrations be bounded?** Terminal
   retention does not solve that queue. Any cap, rejection, or expiry is a
   lifecycle policy and must be config-driven.
5. **Should a lossless columnar format replace the dense compact rows later?**
   Use the common measurement fixtures to compare reduction, worst-case growth,
   CPU/allocation cost, operability, and migration complexity. Owner: OMENative
   and API maintainers; requires a follow-up OEP before any columnar writer.

The next action belongs to the OMENative maintainers: land the unconditional
persistence boundary and its writer-family regression tests. This OEP applies
only to OMENative InferenceReplicas.
