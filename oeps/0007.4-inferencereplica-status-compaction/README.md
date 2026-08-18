# OEP-0007.4: Compact OMENative InferenceReplica Status

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
  - [First compact representation](#first-compact-representation)
  - [Preserve published summaries](#preserve-published-summaries)
  - [Preserve read authority](#preserve-read-authority)
  - [Do not add Pod labels or annotations](#do-not-add-pod-labels-or-annotations)
  - [Configuration and representation marker](#configuration-and-representation-marker)
  - [Status ownership](#status-ownership)
- [Design Details](#design-details)
  - [Preserve decision and publication epochs](#preserve-decision-and-publication-epochs)
  - [The PodCount prior-snapshot exception](#the-podcount-prior-snapshot-exception)
  - [Future columnar representation](#future-columnar-representation)
  - [Consumer migration](#consumer-migration)
  - [Upgrade, downgrade, and mixed-version safety](#upgrade-downgrade-and-mixed-version-safety)
  - [Failure semantics](#failure-semantics)
  - [API and operator compatibility](#api-and-operator-compatibility)
  - [Implementation sequence](#implementation-sequence)
  - [Observability](#observability)
- [Test Plan](#test-plan)
  - [Differential and unit tests](#differential-and-unit-tests)
  - [Lifecycle and recovery tests](#lifecycle-and-recovery-tests)
  - [Consumer parity tests](#consumer-parity-tests)
  - [Fault and upgrade tests](#fault-and-upgrade-tests)
  - [Size and performance tests](#size-and-performance-tests)
  - [KIND tests](#kind-tests)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** for OMENative—OME's direct Pod-management mode—keep each
InferenceReplica's crash-recovery journal, stop copying three current Pod
observations into every replica row, and prove old/new decision parity before
the compact writer is allowed to ship.

One `InferenceReplica` (IR) owns one `InferenceService` (ISVC)
Component—engine, decoder, or router—and one Instance is a single replica made
of either one Pod or an atomic leader/worker gang.
OEP-0007.3 records an approximately 505 KiB IR status at 1,000 Instances and an
approximately 1 MiB status at 2,000 Instances; the exact measurement fixture is
not checked in and must be reproduced by this OEP's first implementation PR.
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
3. The first compact representation stops serializing three fields whose
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
7. Compaction is opt-in through `inferenceservice-config`, and its writer is
   blocked until the compatibility release, differential tests, downgrade
   procedure, and raw-status API contract are approved.
8. A later lossless columnar format may range-encode the retained logical rows
   and store only exceptional operations or failures per index. That can save
   substantially more, but it is a separate representation proof rather than a
   reason to infer durable or prior-snapshot state from current Pods.

The intended invariant is narrow: lifecycle decisions, rollout ordering,
placement, autoscaling, Conditions, Kubernetes Event emission, revision
retention, and recovery must remain identical, including which observation
epoch feeds a decision. The intentional API differences are the raw GET shape
and representation marker, plus raw IR watch timing when only an omitted field
changes.

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
   first compact format targets the three proven fields; an exact byte gate
   is established by the measurement PR described below.
2. **Preserve every current control-plane decision.** Create, Update, Restart,
   Migrate, Delete, rollout coordination, placement, autoscaling, and revision
   retention must receive semantically identical inputs.
3. **Preserve crash recovery through zero-Pod windows.** Operation ownership,
   incarnation, canonical ordinal, revision identity, failure evidence, retry
   authority, and migration state remain durable on the IR.
4. **Preserve current read authority.** Cached observations remain acceptable
   for counters; destructive confirmation continues to use live API reads.
5. **Avoid a new workload layer.** The hierarchy remains
   `InferenceService -> InferenceReplica -> Pod`.
6. **Avoid moving etcd bytes from one object to another without benefit.** No
   counter or lifecycle-history annotations are copied onto Pods.
7. **Make upgrade and rollback explicit.** Compact writes begin only after all
   readers understand both representations. Release A is the guaranteed
   rollback floor; rolling back farther is allowed only if rehydration succeeds.
8. **Keep configuration outside the binary.** Storage mode comes from the
   existing `inferenceservice-config` ConfigMap; absent configuration retains
   the current representation.

### Non-Goals

1. **Sparse or columnar lifecycle rows in the first version.** Eliding repeated
   healthy-row values or range-encoding them could save much more, but changes
   the persisted representation of index, incarnation, revision-retention, and
   total-loss recovery state. Dense compact rows are the conservative boundary.
   A lossless columnar candidate is evaluated below and in the measurement PR.
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
- **InlineV1 representation:** the current legacy format, where live
  per-Instance fields are copied inline into IR status.
- **DerivedV1 representation:** the first compact format, where
  `readyPodCount`, `scheduledPodCount`, and `nodesOccupied` are
  omitted from each Instance row. Prior-snapshot decision fields remain inline.
- **Columnar representation:** a possible later format that losslessly groups
  equal durable and prior-snapshot values by canonical index ranges. This is a
  design candidate, not a selected schema or representation marker.
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

The repository's existing 2,000-entry unit test is not a sufficient size
baseline. It serializes a Creating shape and asserts only that the complete
object is below 1.5 MiB. That value is a test constant, not a verified limit for
every deployment. The applicable API-server/etcd request budget and required
headroom are **TBD** in the measurement PR. The current test also omits steady
gang placement, failure messages, mixed revisions, and migrations.

A provisional JSON row model gives the likely first-order benefit. It uses
representative revision/node strings and excludes Kubernetes object metadata,
and the representation marker. It is **not** a
complete IR size measurement or graduation evidence; the implementation must
replace it with Go fixtures using the real types and full update request.

| Modeled 2,000-Instance row set | InlineV1 row bytes | DerivedV1 row bytes; admission and prior-snapshot counters retained | Estimated row-byte reduction |
| --- | ---: | ---: | ---: |
| Single-Pod Instances | 463,430 B | approximately 323,430 B | approximately 140,000 B (30.2%) |
| Eight-Pod gangs on eight distinct nodes | 631,430 B | approximately 323,430 B | approximately 308,000 B (48.8%) |

The percentage changes with name lengths, optional fields, topology, and
failures. The useful conclusion is bounded: the three fields are a material
steady-state fraction, especially `nodesOccupied` for multi-node gangs. The
audit does **not** support a claim that the first safe singleton cut removes a
majority of the complete object.

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
| `instanceStatuses` | Only in steady state | Keep dense compact lifecycle rows in DerivedV1. |
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

| Field | Current source and consumer | Zero-Pod risk | DerivedV1 disposition |
| --- | --- | --- | --- |
| `index` | Pod label in steady state; planner key, sparse migration identity | Operations and relocated indices outlive Pods | Keep on every dense row. |
| `incarnation` | Pod label in steady state; identity fencing and restart | Increment is committed before old Pods disappear | Keep. |
| `phase` | Lifecycle state machine | Pod health cannot identify operation ownership | Keep. |
| `runningRevision` | Pod revision hash in steady state; rollout and revision GC | In-place labels advance before promotion; old CR must survive zero Pods | Keep. |
| `targetRevision` | Update intent and identity guards | Exists before replacement Pods | Keep; already omitted in steady state. |
| `podCount` | Current Pod list; aggregate classifiers | Previous value proves the last published gang view was complete | **Keep in DerivedV1.** |
| `readyPodCount` | `ContainersReady=True`; publication rollups, raw diagnostics, and the empty surge-slot integrity guard read it | No readiness decision needs its prior value; the integrity guard rejects nonzero legacy residue | Derive for internal rollups; stop serializing only after the raw diagnostic contract is replaced or deprecated. |
| `servingPodCount` | `ContainersReady=True` and `ome.io/serving=True`; prior value drives failed-update exemption and RatioBalanced gang-peak accounting | Re-deriving current state advances decisions relative to today's deferred publisher | **Keep in DerivedV1.** |
| `availablePodCount` | Ready, nonterminating EndpointSlice target; prior value gates migration tail | Re-deriving current EndpointSlices can advance migration by one reconcile | **Keep in DerivedV1.** |
| `scheduledPodCount` | `Pod.spec.nodeName != ""`; the empty surge-slot integrity guard checks zero | No historical meaning outside that legacy-row integrity check | Derive; stop serializing. |
| `admitted` | Pods exist and none carries a scheduling gate; placement counts the current row set | Row removal/teardown may bypass publication, so a scalar aggregate can remain stale | **Keep in DerivedV1.** |
| `nodesOccupied` | Sorted unique `Pod.spec.nodeName` values; raw diagnostics and the empty surge-slot integrity guard read it | Current node observation only; migration history lives elsewhere | Derive; stop serializing only after the raw migration/debug query is replaced or deprecated. |
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
identity guard. DerivedV1 therefore preserves all three. They are normally
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

No additional Pod metadata is required for the three DerivedV1 fields.

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
| Create batch rollback | Whole-row equality indirectly includes all three publication-only fields | Compare every retained transition-ownership field; preserve current InlineV1 observations while restoring prior lifecycle state. |
| IR aggregate and conditions | Per-Instance counters, phases, revisions, failures | Publication-epoch observation plus durable rows; persist identical top-level values. |
| ISVC IR projector | Top-level counts, revisions, conditions | Unchanged. It intentionally does not mirror per-Instance rows. |
| RatioBalanced / surge gates | Top-level serving count, Operation steps, prior per-Instance serving peak | Unchanged persisted serving values in DerivedV1. A later format needs an equivalent persisted predicate before removing them. |
| Sequential coordination | Generation/revision skew, dense Instance shape, Ready transition time | Durable rows and unchanged top-level condition. |
| Canary | Current and update revisions | Unchanged. |
| HPA/KEDA `/scale` | `replicas` and `labelSelector` | Unchanged. |
| Multi-cluster placement | Per-Instance `admitted`, failed phase, and top-level `readyReplicas` for Split accounting and endpoint weight | Unchanged durable rows and top-level summaries. No remote Pod informer or aggregate migration. |
| ControllerRevision GC | Current/update and every Instance running/target revision | Unchanged durable union. |
| Internal performance invariant harness | Per-Instance `readyPodCount` to avoid comparing two independently lagging watches | Must receive the approved diagnostic replacement; this is part of the raw-status blocker, not “just a test” to delete. |
| KIND and operator-driven migration | KIND reads `Pod.spec.nodeName`; external clients may use `nodesOccupied[0]` to supply `fromNode` | Keep the Pod query and define the external diagnostic contract. Migration's existing live validation remains authoritative. |
| Test admitters and integration fixtures | Direct per-Instance `admitted` reads/writes and assertions on the three removable fields | Admission fixtures stay unchanged; assert live Pod facts where epoch coupling is not required. Keep explicit InlineV1/DerivedV1 coverage. |
| Operators using raw IR YAML | All serialized fields | Preserve mode remains available; DerivedV1 intentionally omits three fields. |

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
4. **`minReadySeconds` is not enforced by availability.** DerivedV1 reproduces
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

When DerivedV1 is enabled:

1. The IR controller MUST keep durable lifecycle state separate from live
   Pod/EndpointSlice facts and MUST preserve the current decision and
   publication observation epochs.
2. Every lifecycle, rollout, placement, retention, and aggregate decision MUST
   be equivalent to InlineV1 for the same supported
   sequence of status, cache, and live-read states—not merely for a perfectly
   synchronized fixture. Conversion MUST reject a legacy row whose compacted
   fields violate the aggregation invariants defined below.
3. The IR MUST continue persisting dense Instance rows with `index`,
   `incarnation`, `phase`, `runningRevision`, `targetRevision`, `podCount`,
   `servingPodCount`, `availablePodCount`, `admitted`, `operation`,
   `activeOrdinal`, `lastFailure`, per-Instance Conditions, and any other
   non-compacted legacy fields already present.
4. The IR MUST NOT persist new values for `readyPodCount`,
   `scheduledPodCount`, or `nodesOccupied` in DerivedV1.
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
   Every status write family MUST preserve or normalize the object's declared
   representation so a non-aggregate write cannot regrow compacted fields.
   Compaction adds no Pod-to-status controller and no writer owned by the ISVC
   controller.
10. Pods MUST NOT receive copied counter, operation, or history annotations for
    this design.
11. A compact writer MUST NOT ship until the compatibility and operator API
    questions in this OEP are closed.
12. Empty-slot and terminal-finalization predicates MUST be representation
    aware. InlineV1 objects retain the current field-by-field predicate.
    DerivedV1 keeps every existing durable, `podCount`, serving, availability,
    Condition, admission, and failure check; only the three omitted checks
    disappear. The
    retained prior-published `podCount` remains the occupancy guard because
    conversion first proves that any omitted nonzero observation implies a
    nonzero `podCount`, as current aggregation guarantees.

### Persist the lifecycle journal; derive the live observation

The Component observation is a process-local read model, not a second source of
truth. Conceptually:

```text
ComponentObservation
  durable[index] -> reconcile-entry IR InstanceStatus row
  live[index]    -> Pods, counts, serving/admission/node facts
  availablePods  -> EndpointSlice target set
```

Consumers must ask for the class of fact they need. For example:

- Restart asks the durable row whether the last publication saw a complete gang and
  compares it with the current live Pod set.
- Publication asks the live view how many desired Pods are ContainersReady;
  pre-effect decisions continue reading the prior persisted serving and
  availability values where they do today.
- Revision GC asks the durable rows which full revision names are protected.
- Delete asks the durable Operation who owns work, then performs its existing
  live absence confirmation before removing the row.

This explicit split prevents a current count from overwriting a prior-snapshot
decision input in memory.

### First compact representation

DerivedV1 deliberately keeps the list dense. A steady row remains roughly:

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
ordinal state exactly as today. The three DerivedV1 fields are absent.

DerivedV1 is a storage representation, not a signal to replace live facts with
zero in memory. The publication path computes the complete transient row,
derives top-level summaries, then clears the three fields only on the
persistence candidate. It compares that normalized candidate—not
the transient full row—with the status re-read by the existing conflict-retry
closure before deciding to write. Otherwise Ready and UpdatedReady would
collapse to zero, or every steady
reconcile would see the temporary fields as a false status delta.

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
- The member carrying a marker can be the member that disappears.
- A surge cannot atomically flip a “canonical” label between two Pods.

Copying state to Pods would add writes, move bytes rather than remove them, and
still lose the recovery record during deletion.

### Configuration and representation marker

The existing `inferenceservice-config` ConfigMap gains an `omenativeStatus`
block with one field:

```json
{
  "instanceObservationMigration": "Preserve"
}
```

Allowed values are:

- `Preserve`: preserve each IR's current representation and do not initiate a
  conversion. A new IR starts InlineV1.
- `DerivedV1`: preserve existing DerivedV1 objects and convert InlineV1 objects
  to DerivedV1 as they reconcile.
- `Rehydrate`: preserve existing InlineV1 objects and convert DerivedV1 objects
  back to InlineV1 as they reconcile.

Release A accepts Preserve and Rehydrate but rejects DerivedV1 activation;
Release B adds permission to initiate compact conversion. In either release,
ordinary status writes preserve the object's committed representation.

An absent ConfigMap block means Preserve; the binary does not invent a storage
conversion. The initial Helm value explicitly renders `Preserve`. Invalid
values prevent manager startup rather than silently selecting a mode.

IR status gains `status.instanceObservationStorage`, which records the format
actually committed on that object. The first marker value is `DerivedV1`;
absence means InlineV1, and rehydration removes the marker. Changing the marker
and clearing or rehydrating the three fields happens in one conflict-retried
status update, so readers never infer format from a partially converted row.

Conversion is a publication-epoch operation, not a side effect of an unrelated
lifecycle mutation. Before clearing a legacy row, it verifies the normal
aggregation invariants: `podCount` is nonnegative; ready and scheduled counts
are nonnegative and no greater than `podCount`; and a nonempty `nodesOccupied`
implies `podCount>0` and contains no more entries than the Pod count. An
anomalous row fails conversion and stays InlineV1 for diagnosis. This keeps the
DerivedV1 empty-slot predicate equivalent to the legacy predicate for
controller-produced rows instead of silently normalizing possibly meaningful
corrupt state.

The mode is loaded once at manager startup. The Helm ConfigMap checksum rolls
manager Pods when the value changes; a direct ConfigMap edit takes effect only
after a manager restart. This startup-only contract avoids some replicas
dynamically changing mode while others keep a stale value. Preserve is
non-reversing, so old-configuration and new-configuration leaders cannot
oscillate an object during a rolling activation.

Preserve is also a mandatory barrier between the two converting modes. The
supported reversal is `DerivedV1 -> Preserve` followed by a completed manager
rollout, then `Preserve -> Rehydrate` and another rollout. A direct
DerivedV1-to-Rehydrate rollout is unsupported because an old compacting leader
could reconvert an object just rehydrated by a new leader. The reverse direction
uses the same barrier.

A DerivedV1 or Rehydrate startup MUST preflight the served IR CRD schema and
verify that `status.instanceObservationStorage` is declared with the expected
schema. If not, startup fails before any IR status write. Structural-schema
pruning can otherwise accept an update, erase the marker, and leave a compact
object that looks InlineV1.

Concretely, the manager reads the `inferencereplicas.ome.io` CRD through the
apiextensions client and walks the served `v1beta1` OpenAPI status properties.
It requires a string marker whose accepted values include `DerivedV1`; checking
the property name alone is insufficient. The marker must be optional—not in the
schema's `required` set—because absence means InlineV1. The preflight also
verifies that the three legacy
row fields remain optional: Ready and Scheduled are `int32`, and NodesOccupied
is an array of strings. Rehydrate must not silently prune them. Failure to read
the CRD, a
missing served version, or an incompatible property is a startup error in
DerivedV1/Rehydrate mode. The chart grants the manager read-only `get` access to
that CRD. Preserve
performs the same check best-effort; if the schema or permission is absent
during an emergency binary-first start, it remains legacy-only and writes
no marker. It may reconcile marker-absent InlineV1 objects, but any object that
already carries `DerivedV1` or an unknown nonempty marker fails closed before a
status write or external effect until the schema can be verified. It never
treats an observed compact marker as InlineV1.

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

Every path that can call `Status().Update` MUST pass through one representation
normalizer, or an audited equivalent with the same tests. On a DerivedV1 object
it clears the three compacted fields before write; on an InlineV1 object it
preserves them. An ordinary lifecycle write never changes the marker;
conversion or rehydration occurs only in the dedicated publication boundary
requested by the startup mode. This prevents a RetryBlock or migration-only
write from accidentally regrowing compacted data.

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
   counters, persists the fields retained by the object's representation, and
   publishes unchanged top-level summaries.

The epochs share counter formulas and indexing helpers, not necessarily object
snapshots. The IR reconciler and ISVC coordination reconciler are separate
passes and cannot share process-local memory. DerivedV1 therefore retains
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

DerivedV1 therefore retains `podCount` with its current write ordering. A later
format may replace the accidental dual use with an explicit per-Incarnation
“materialized” latch, but only after:

1. the latch is committed when a complete set is first observed;
2. Incarnation changes clear it atomically;
3. partial and total loss are distinguished exactly as today; and
4. the byte and complexity benefit justifies another schema field.

That later change is not required to deliver the first material saving.

### Future columnar representation

A range-encoded representation is a promising way to remove the repeated JSON
keys and values that remain after DerivedV1. In a 500-Instance steady state,
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
7. full decision, Event, recovery, upgrade, and downgrade parity, plus raw API
   compatibility and approved-contract gates equivalent to DerivedV1; and
8. measured best- and worst-case sizes. Randomly distributed values can defeat
   range compression, so the format needs a deterministic fallback rather than
   assuming the steady-state example is representative.

The first measurement PR will compare this candidate with InlineV1 and
DerivedV1 using the same real fixtures. If the additional reduction justifies
the representation complexity, a follow-up OEP may supersede DerivedV1 before
its writer ships instead of forcing operators through two storage migrations.
This OEP does not assign a marker name or authorize columnar writes.

### Consumer migration

Internal migration follows these rules:

1. **Workload core:** accept durable and live observations separately. No
   operation reads a compacted raw status field directly except the audited
   empty-slot predicate described below.
2. **Aggregation:** compute the same top-level counters from the live view plus
   durable revision/phase state. In DerivedV1 it still persists serving and
   availability, but not the three compacted fields.
3. **Coordination:** continue using top-level aggregates, durable Operations,
   and persisted per-Instance serving values. DerivedV1 adds no Pod LIST to the
   ISVC reconciliation path.
4. **Placement:** continue counting per-Instance `admitted` from the current row
   set. No fallback, remote Pod informer, or observation-epoch change is added.
5. **Revision retention:** continue unioning top-level current/update and every
   durable running/target revision. No live-only revision may be swept while an
   operation can resume against it.
6. **ISVC projection, canary, and autoscaling:** continue reading the same
   top-level fields.

Raw access to one of the three fields outside the observation builder is a
review blocker after migration, with one allowlisted exception:
`emptyGangSurgeTargetSlotForRepresentation` evaluates the fresh status inside
the conflict-retried mutation. It checks all three fields for InlineV1 and the
validated retained-`podCount` implication for DerivedV1. A repository check or
focused static test SHOULD make any other direct reader visible.

The mutation adapter exposes the freshly read top-level representation through
`InstanceMutationSnapshot` on every conflict attempt. Empty-slot validation is
a batch precondition that reads that marker and the indexed row from the same
snapshot; it does not capture the reconcile-entry marker in a closure. The
adapter must rebuild both after a conflict before it evaluates the precondition
again.

### Upgrade, downgrade, and mixed-version safety

Compact writes require a bridge release. The safe sequence is:

1. **Release A—reader bridge.** Install the additive CRD schema, then roll every
   workload-cluster manager and control-plane placement manager to a binary
   that reads both formats. In Preserve mode, every write keeps the object's
   existing format, and A cannot initiate compact conversion. A can explicitly
   rehydrate a DerivedV1 object at the publication boundary for rollback. It
   compares decisions from the old and new read paths while objects remain
   InlineV1. The legacy path remains authoritative; shadow evaluation emits no
   status write, Kubernetes Event, or external action.
2. **Prove parity.** Run the differential, KIND, performance, and fleet
   observation gates. Any decision mismatch blocks compaction.
3. **Close the raw-status contract.** API owners approve either deprecation of
   the three observation fields with documented replacement queries or a
   reconstructed diagnostic surface. Until then, no compact writer ships.
4. **Release B—compact writer.** Roll every manager to a compact-capable binary
   while configuration remains Preserve. Only after every possible leader in
   the control plane and every participating workload cluster can read
   DerivedV1 may the operator select DerivedV1 on a workload cluster.
5. **Enable deliberately.** The controller converts each IR atomically and
   reports remaining InlineV1/DerivedV1 object counts.

Release A's shadow comparison is broader than `workload.Plan`. `Plan` does not
own execute-time coordination gates or every per-Instance Create and Update
admission decision. The bridge therefore introduces a side-effect-free decision
trace at each existing effect-authorization branch. The normalized trace
contains the action family, Instance identity and revision, allow or deny
result, and stable reason category; it contains no Pod payload or unbounded
diagnostic text.

At each branch, the InlineV1 and DerivedV1 evaluators receive the same
reconcile-entry status, the same cached or authoritative object snapshot, and
the same resolved external-gate context—not the same final gate decision. A
lister or `ResolveGateContext` call runs once at its current point in the
execute loop. Pure gate checks then evaluate that immutable context separately
with each trace's strategy and in-flight counts; only the InlineV1 evaluation
may record gate metrics or Events. Release A refactors the current `UpdateGate`
adapter to expose that separation. Shadow evaluation does not repeat reads or
reorder earlier effects. The InlineV1 trace alone authorizes the effect. A
difference is recorded before that effect and the legacy execution continues.
Tests map every Pod, PodGroup, status, placement, and revision effect site to a
trace point and cover early divergence where a later gate is never reached. A
zero mismatch result is not a valid enablement signal unless this effect-site
coverage check passes.

Rollback rules:

- Before rolling the binary from Release B to A, complete a Release B rollout
  in Preserve mode. Release A rejects DerivedV1 activation but safely reads and
  preserves already-compact objects while configured Preserve.
- Rolling back to a pre-A binary is unsafe while any DerivedV1 IR exists.
  From Release A Preserve, roll A in Rehydrate mode and allow it to convert
  objects. A larger InlineV1 status can receive the same 413/size rejection that
  motivated compaction. If any IR cannot rehydrate, return A to Preserve and
  stop there. If all succeed, complete another A Preserve rollout before the
  fleet-wide verification and downgrade. Pre-A rollback is conditional, not
  guaranteed.
- One representation target is attempted per IR reconcile, with the existing
  bounded conflict retry around its atomic status update. Fleet conversion is
  bounded by the controller's configured worker concurrency. Manager startup
  naturally enqueues existing IRs through informer Add events; no unbounded
  per-reconcile fleet sweep is added.

CRD ordering is also explicit. The additive schema lands before a binary writes
the representation marker. A Preserve-only binary-first
start may observe legacy objects but MUST NOT enable compaction; a DerivedV1 or
Rehydrate start against the old schema fails the preflight. Real-apiserver tests
must cover old structural-schema pruning rather than relying on fake clients.

DerivedV1 and Rehydrate modes require leader election on every participating
manager and fail startup when it is disabled. Leader election prevents
simultaneous active reconcilers, but it does not make a mixed-version manager
set safe: an older Pod can become leader after a newer one dies. Therefore the
“every possible leader is bridge-capable” prerequisite is load-bearing.

The additive CRD schema is part of the rollback floor. Operators MUST NOT
downgrade it while any DerivedV1 object exists: pruning the marker would make a
compact row look InlineV1. Pre-A binary or CRD downgrade requires a fleet-wide
zero-DerivedV1 proof across every cluster that can contain an IR. Release A
provides an operator-invoked, one-shot downgrade verifier; it is release
tooling, not a controller reconcile path. In a standalone installation its
target is the current cluster. In a multi-cluster installation it checks the
current cluster and every registered workload cluster, using the placement
control plane's registered credentials for remote targets.

For each target, the verifier first performs an authoritative GET of the served
IR CRD and applies the same optional-marker schema checks as manager startup. A
missing or incompatible marker schema blocks verification because structural
pruning could otherwise hide a stored marker. Only then does it perform a
paginated, authoritative IR LIST; every object must have an absent marker.
Release A extends the remote multicluster-access role with read-only `get` and
`list` on IRs and resource-scoped `get` on the IR CRD, and verifies those
credentials in upgrade tests; no remote write verb is added. Cached counts and
metrics are observability only and cannot authorize downgrade. A CRD or LIST
failure, unreachable cluster, unknown marker, or authorization failure blocks
the downgrade.

### Failure semantics

| Failure | Required response |
| --- | --- |
| Cached Pod or EndpointSlice LIST fails | Return an error; retain the prior published observation and start no decision that depends on fabricated zeros. |
| Live destructive LIST fails | Fail closed exactly as today; perform no external effect. |
| Pod has missing/malformed identity labels | Preserve existing orphan/fencing behavior; do not bucket it into a guessed Instance. |
| Status conversion conflicts | Re-read, rebuild against the fresh durable status, and retry the complete conversion. |
| Legacy row violates conversion invariants | Leave the object InlineV1, report the invariant, and continue lifecycle reconciliation with InlineV1 semantics. |
| Unknown representation marker | Fail closed and surface an unsupported-format error; never interpret it as InlineV1 or DerivedV1. |
| Manager crashes during InlineV1 -> DerivedV1 conversion | The single committed status update leaves either complete format, never a mixed marker/row set. |
| Manager crashes after compact status but before an effect | Durable Operation and identity fields resume the same work. |
| EndpointSlice lags Pod readiness | Publish the same temporarily lower availability as the current implementation. |
| Remote placement cannot read an IR | Fail/retry according to its current semantics; do not infer admission from missing data. |
| Status update is rejected for size | Surface the error and preserve durable work. DerivedV1 reduces the common shape but does not claim to bound unbounded diagnostics or migrations. |

### API and operator compatibility

There is an unavoidable limit to the “zero behavior change” claim: removing
serialized bytes changes a raw API GET. An operator who reads
`instanceStatuses[42].nodesOccupied` will no longer find that field in
DerivedV1, even though the same current node remains on the Pod. A change only
to an omitted field may also stop producing an IR status write and watch event
when no retained aggregate changes. Internal controllers must be proven
independent of that event; external watch behavior belongs to the API decision.

This OEP therefore makes two narrower guarantees:

1. **Workload and control-plane behavior is identical.** No create, delete,
   drain, rollout, placement, autoscaling, condition transition, Kubernetes
   Event emission, or recovery decision changes.
2. **Existing installations do not compact implicitly.** Preserve remains the
   initial configured mode; it writes no marker and removes no row field.

The CRD fields remain optional and accepted during the compatibility window.
DerivedV1 stops writing them; it does not immediately remove them from OpenAPI
or generated clients. That preserves decoding of old objects and allows
rehydration, but callers must still handle their absence on a compact object.
Generated Go, Rust, and Python/OpenAPI client compatibility therefore belongs
in the API test matrix rather than being assumed from successful decoding alone.

Before a DerivedV1 writer ships, reviewers must choose one of two operator
contracts:

- declare the three per-Instance fields an internal, deprecated cache and provide
  documented Pod queries; or
- provide a read-only diagnostic surface that reconstructs the same view.

The chosen contract must cover both GET shape and watch-event expectations.
Claiming that a smaller stored object has a byte-identical raw API would be
false, so provisional approval of this OEP authorizes the bridge and
measurement work only. It does not authorize compact writes while this decision
is open.

### Implementation sequence

The work is split so each PR has one reviewable responsibility:

1. **Measurement and fixtures.** Add named, reproducible size attribution for
   steady singletons, gangs, creates, updates, deletes, failures, and
   migrations using real API types and complete status-update request bodies.
   Compare the lossless columnar candidate against InlineV1 and DerivedV1 before
   committing to a production writer. No behavior change.
2. **Observation seam.** Make the durable-plus-live inputs explicit while
   preserving separate decision/publication epochs and each path's source and
   owner-filtering scope. Keep InlineV1 writes and differential-test old/new
   decisions.
3. **Consumer migration.** Move every production reader of the three removable
   fields to the correct epoch-scoped view. Retained prior-snapshot and
   admission fields, placement behavior, and InlineV1 writes stay unchanged.
4. **Release A compatibility bridge.** Add the schema, marker, startup-loaded
   configuration, dual-format readers, writer-family normalizer, explicit
   rehydration, and low-cardinality telemetry. Preserve remains configured and
   A rejects DerivedV1 activation.
5. **API approval.** Close the raw GET/watch contract and document its
   replacement before compact writes are reviewable.
6. **Release B DerivedV1 writer.** Stop serializing the three fields only after
   all prior gates pass. This PR changes persistence mechanics, not lifecycle
   policy.
7. **Optional later proposals.** Decide sparse rows, explicit materialization,
   diagnostic bounds, and migration queue bounds independently.

Each production-code PR must keep its diff focused. Large differential test
matrices are expected; reviewers should be able to separate them from the
small persistence-mechanics change in the PR description.

### Observability

Metrics use low-cardinality labels such as representation, observation epoch,
and component type; they do not label by IR name or namespace:

- `ome_omenative_ir_status_bytes`: a histogram of complete serialized
  status-update request bytes, split by representation. It MUST reuse bytes
  already produced by the write path or transport instrumentation rather than
  add a second full serialization on the reconcile hot path.
- `ome_omenative_ir_status_derived_mismatch_total`: shadow comparison mismatch,
  labeled by decision family. Any nonzero value blocks enablement.
- `ome_omenative_ir_observation_duration_seconds`: time to build an
  epoch-scoped observation.
- `ome_omenative_ir_observation_errors_total`: failed Pod or EndpointSlice
  observations by source.
- `ome_omenative_ir_status_representation_objects`: cluster-level object count
  by representation, with no object identity label.

Conversion and rehydration errors must be visible in existing reconcile error
telemetry and Events. The representation count lets operators observe migration
progress without creating per-object metric cardinality; only the authoritative
one-shot verifier can authorize a downgrade.

## Test Plan

### Differential and unit tests

Build paired fixtures at both observation epochs:

1. a full prior InlineV1 status plus current Pods/EndpointSlices; and
2. the equivalent DerivedV1 durable status plus the same current objects.

The persisted counters MUST intentionally lead and lag the current objects in
both directions. In particular, include prior-serving/current-not-serving,
prior-not-serving/current-serving, prior-available/current-unavailable, and the
reverse. Fixtures in which status always matches Pods would not test the
decision/publication boundary that keeps `podCount`, `servingPodCount`, and
`availablePodCount` persisted.

The three removable fields also lead and lag their current Pod facts in both
directions, including a changed node set. For conversion-eligible,
controller-produced rows, their old values must influence no control decision
other than the explicitly equivalent empty-slot predicate, while the
publication result remains identical. Anomalous rows stay InlineV1 and are
covered by rejection tests instead of this equivalence claim.

For each pair, assert identical:

- Component plan and ordered actions;
- operation admission and resume decisions;
- update, restart, migration, delete, and scale-down gates;
- aggregate status, Conditions, and Kubernetes Events;
- ISVC projection and rollout-coordination observations;
- ControllerRevision retention set.

Decision-trace tests enumerate every effect-authorization site, including the
execute-time coordination gate and per-Instance Create and Update admission.
They prove that shadowing performs no second LIST, gate call, Event, status
write, or external effect, and that a mismatch is reported before the legacy
effect proceeds. `workload.Plan` coverage alone is not sufficient.

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

Conversion tests cover every valid boundary value and reject anomalous legacy
rows such as `podCount=0` with Ready, Scheduled, or Nodes nonzero.
Focused tests pin equivalent empty gang-surge-slot selection before and after a
valid conversion.

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
objects. The DerivedV1 and InlineV1 outcomes must match.

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
  approved diagnostic contract;
- performance test admitters and real-Kueue multi-cluster fixtures;
- revision retention during mixed-revision zero-Pod transitions.

Placement tests continue counting current per-Instance `admitted` values. Pin
the admitted-row removal, teardown, and failed-publication cases that make a
separate scalar count unsafe; InlineV1 and DerivedV1 must return the same Single,
All, and Split decisions.

Event-liveness tests also isolate changes that affect only an omitted field:

- a partial gang member becoming ContainersReady below the aggregate Ready
  threshold while serving remains false;
- a NodeName-only bind after the Instance is already admitted; and
- a scheduled or node change while every retained status field is fixed.

For each case, prove that suppressing the omitted-only IR status update does not
change any parent ISVC control decision, traffic decision, placement result,
Condition, or Event. This turns the static consumer audit into a regression gate
for the IR-to-ISVC watch edge.

### Fault and upgrade tests

Inject:

- cache lag and watch gaps;
- Pod, EndpointSlice, IR, and ControllerRevision read errors;
- 409 conflicts, 413 rejections, 429 throttling, 5xx responses, and context
  cancellation;
- malformed labels and changed owner UID;
- crash during representation conversion and rehydration.

Upgrade tests exercise:

- old full object -> Release A -> Release B DerivedV1;
- DerivedV1 -> Release B Preserve rollout -> Release A Preserve rollback;
- DerivedV1 -> successful rehydration -> pre-A rollback for a fixture that fits;
- rehydration rejected with 413, which leaves DerivedV1 intact and stops the
  rollback at Release A;
- leader handoff during Preserve-to-DerivedV1 and Preserve-to-Rehydrate
  rollouts, with a completed Preserve barrier before reversal;
- DerivedV1 and Rehydrate startup with leader election disabled, which must
  fail before any status write;
- control-plane placement and workload-cluster manager version skew;
- CRD-first startup and Preserve-only binary-first startup with compaction
  disabled;
- DerivedV1/Rehydrate startup against the old CRD, which must fail preflight;
- missing CRD-read RBAC and wrong marker schemas (required, wrong type, or
  missing enum value), including Preserve allowing InlineV1 but blocking an
  observed compact object;
- real-apiserver structural-schema pruning behavior, not only fake clients;
- attempted CRD downgrade with any DerivedV1 object, plus a fleet-wide
  authoritative-LIST zero-DerivedV1 proof that ignores cached/metric counts and
  blocks on an unreachable workload cluster or any LIST page failure;
- the one-shot verifier in standalone and multi-cluster target sets, including
  current-schema validation, remote IR `get,list` and resource-scoped CRD `get`
  credentials, pagination, unknown markers, authorization failure, and the
  zero-object cluster case;
- old stored optional fields and legacy operation fields.

Every status writer family gets a representation-preservation test: single and
batch Instance mutation, standalone RetryBlock mutation, migration accept,
phase mutation, import/prune sync, and automatic-relocation outcome, collision
count, current-revision promotion, aggregate publication, the dedicated gang
Condition writer, terminal row removal, and teardown. A
DerivedV1 object must never regrow the three fields; an InlineV1 object must not
lose them; only the publication conversion path may change the marker. Repeated
reconciles after conversion must be idempotent and must not create a status
write storm.

Every case asserts no unintended Pod create, drain, patch, delete, PodGroup
delete, placement win, or revision sweep.

### Size and performance tests

Measure serialized bytes by field family at 0, 1, 100, 1,000, and 2,000
Instances for:

- steady singleton;
- steady eight-Pod gang with realistic node placement;
- Creating with Operation;
- in-place and surge update;
- deleting wave;
- Kueue-gated Pods;
- realistic and intentionally large LastFailure;
- migration-heavy status.

Record current versus DerivedV1 bytes for the complete serialized IR
status-update request, including realistic spec, metadata, and managed fields;
also record bytes per write, write count, Pod/EndpointSlice LIST count, decoded
object count, reconcile CPU, allocation, and peak memory. The measurement PR
sets the exact acceptance threshold from named fixtures; the controller binary
does not contain an object-size magic value.

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
RatioBalanced behavior under both representations.

The repository should also run a 2,000-Instance synthetic/fake-client size test.
Before Beta, run a real scale trial with 2,000 single-Pod Instances and a gang
shape whose total Pod count is at least comparable. The exact larger gang trial
is recorded by the measurement milestone rather than invented here.

## Graduation Criteria

### Alpha

- The measurement fixtures and field attribution are checked in.
- Release A reads both representations and preserves each object's committed
  representation on ordinary writes; it cannot initiate DerivedV1.
- Every internal consumer of a removable field uses the correct epoch-scoped
  observation; admission and placement remain on the durable row set.
- Differential tests report zero decision mismatches across the complete unit
  matrix.
- Focused KIND lifecycle tests pass in InlineV1 mode.
- Admitted-row removal and teardown prove identical placement behavior.
- The raw GET/watch contract is closed before Release B is approved.

### Beta

- DerivedV1 is opt-in and has passed the full required test suite.
- Focused KIND tests pass in both modes.
- A 2,000-Instance scale trial demonstrates the approved size reduction and
  headroom for the named bounded singleton, gang, transition, failure, and
  migration fixtures. Unbounded diagnostics and migration queues are reported
  separately rather than covered by this claim.
- No reconcile path performs more Pod or EndpointSlice LISTs than its current
  counterpart, and placement opens no remote Pod informer.
- Upgrade and Release A rollback are automated and tested. Rehydration is
  proven for fixtures that fit; a 413 is proven to stop pre-A rollback safely
  at Release A.
- Shadow mismatch metrics remain zero during the agreed observation window.
- The approved operator/debug API replacement is documented and tested.

### Stable

- Fleet soak shows no lifecycle, rollout, placement, autoscaling, or revision
  retention regression.
- The team decides whether DerivedV1 becomes the rendered chart mode; no binary
  default is added.
- Remaining growth risks—unbounded diagnostics and nonterminal migrations—have
  explicit accepted follow-ups or documented bounds.
- Any proposal to make Instance rows sparse has its own recovery proof and is
  not implied by this OEP's completion.

## Drawbacks

1. **The code carries two views of one Instance.** This is deliberate but adds
   type and test complexity. The current code already has two meanings in one
   struct; making them explicit trades hidden coupling for visible structure.
2. **Raw compact status is less convenient for debugging.** Operators must read
   Pods or an approved derived diagnostic surface for the three
   fields.
3. **Dense lifecycle rows remain O(N).** DerivedV1 materially reduces the
   coefficient but does not make IR status O(1).
4. **Top-level status still rewrites the complete IR object.** Kubernetes status
   update semantics still carry dense durable rows. This proposal reduces bytes,
   not the fundamental whole-object update model.
5. **The first format does not bound every shape.** Large LastFailure text and
   unbounded nonterminal migrations can still exceed the object limit.
6. **Rollout takes two releases.** That delay is the cost of a real downgrade
   bridge; skipping it would make a leader rollback capable of destructive
   misreads.
7. **Raw watch timing changes in DerivedV1.** A node or scheduling observation
   that changes no retained aggregate may no longer cause an IR status event.
   The API contract must resolve this before compact writes ship.

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
6. **Remove `podCount` with the other counters.** Rejected for DerivedV1. It
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

1. **Is the raw per-Instance observation a supported operator API?** Before
   any DerivedV1 writer ships, reviewers must choose deprecation plus documented
   queries or a reconstructed diagnostic surface, including watch-event
   expectations. Owner: API reviewers; Release A bridge work may proceed while
   this is open, Release B may not.
2. **What exact byte gate should Beta enforce?** The measurement PR must record
   named, reproducible InlineV1 and DerivedV1 full-request fixtures, then
   propose a reduction and headroom threshold. Owner: implementation author;
   required before the compact-writer PR.
3. **Should `podCount` be replaced later by an explicit materialization latch?**
   Only if the clearer invariant and any extra savings justify schema and
   migration complexity. Owner: OMENative maintainers; not required for
   DerivedV1.
4. **How should full failure diagnostics be bounded without losing operator
   access or changing retry reasons?** Options include a configured length
   bound, a bounded audit ledger, or a separate sharded diagnostic resource.
   This OEP does not pick one.
5. **How should queued and stuck nonterminal migrations be bounded?** Terminal
   retention does not solve that queue. Any cap, rejection, or expiry is a
   lifecycle policy and must be config-driven.
6. **What observation window is sufficient before opt-in enablement?** Define
   the cluster count, workload mix, and duration for a zero-mismatch Release A
   soak. Owner: approvers; record it in `latest-milestone` before Beta.
7. **Should a lossless columnar format supersede DerivedV1 before Release B?**
   Use the common measurement fixtures to compare reduction, worst-case growth,
   CPU/allocation cost, operability, and migration complexity. Avoid shipping
   two persisted formats merely because DerivedV1 was designed first. Owner:
   OMENative and API maintainers; requires a follow-up OEP or an approved
   revision of this proposal before any columnar writer.

The next action is the measurement-and-fixtures PR. It changes no behavior and
turns the provisional size estimates into the acceptance gates for the reader
bridge; this OEP applies only to OMENative InferenceReplicas.
