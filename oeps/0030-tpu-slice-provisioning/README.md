# OEP-0030: Slice provisioning for OME TPU workloads

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Terminology](#terminology)
- [Proposal](#proposal)
  - [What must be true](#what-must-be-true)
  - [Option 1: ome-controller provisions, ome-scheduler places](#option-1-ome-controller-provisions-ome-scheduler-places)
  - [Option 2: slice-scheduler treats OME objects as first-class](#option-2-slice-scheduler-treats-ome-objects-as-first-class)
  - [Comparison and recommendation](#comparison-and-recommendation)
- [Design Details](#design-details)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** On a pre-provisioned TPU cluster, a workload can only start if a
partition of *exactly* its shape already exists. OME has no way to make one, so
an OME service whose shape nobody pre-carved does not start — it fails at
`START_SESSION`. This OEP picks who creates that partition: `ome-controller`, leaving
`ome-scheduler` to place onto it (Option 1), or `slice-scheduler` doing both
(Option 2). I recommend Option 1.

OEP-0022 gives `ome-scheduler` topology-aware placement onto slices that already
exist. It does **not** create them. That is the gap here — and the fix belongs in
the controller, not the scheduler, because slice creation is a slow control-plane
call and the scheduler binds pods.

## Motivation

On a node carrying `cloud.google.com/gke-accelerator-topology-mode=PROVISION_ONLY`,
libtpu validates every session against the provisioned partition, and the
**total** topology a session requests must **equal** the partition's. Any
mismatch is `START_SESSION failed ... Max # of tries(=3) exhausted`.

The following controlled tests compare requested session shapes with provisioned
partitions:

| workload shape | partition | result |
|---|---|---|
| 4 engines × 1 chip (`PROCESS_BOUNDS=1,1,1`) | 2x2x1 | **fails**, 4/4 chips |
| 4 engines × 1 chip, group `2,2,1` | 2x2x1 | **works**, 4/4 `Init worker` |
| same, 4 chips | 2x2x2 | **fails**, 4/4 chips |
| 8 engines × 1 chip, group `2,2,2`, 2 hosts | 2x2x2 | **works**, 8/8 `Init worker` |

Two consequences the reader should not have to infer:

1. **The shape is a hard match, not a fit.** A 4-chip pod cannot take half of a
   2x2x2 partition — measured above, not argued. So "enough free chips" is not a
   schedulable condition; "a free partition of this exact shape" is.
2. **The floor is one host.** The `PartitionTree` for a 4x4x4 cube offers only
   `2x2x1`(16), `2x2x2`(8), `2x2x4`(4), `2x4x4`(2), `4x4x4`(1). No carve yields a
   single chip, so per-chip partitioning is not an option at any layer.

Today the only producer of slices for OME is NPC (`TPUNodePool.spec.slices`),
decided per cube ahead of time by capacity config. That is workable when the
demand profile is known and uniform. With mixed demand, a 4-chip OME service
cannot use free capacity carved exclusively into `2x2x2` partitions. It can
enter `CrashLoopBackOff` if placed there despite having enough free chips.

Converting a static pool to pre-provisioned slices therefore requires matching
the shapes of its existing workloads. Enabling `PROVISION_ONLY` across a pool
without those matching partitions can prevent those workloads from starting.

### Goals

- An OME TPU service starts on a pre-provisioned cluster without a human having
  pre-carved its exact shape.
- One owner for the slice lifecycle, so a slice is never leaked or freed while a
  pod still holds it.
- Provisioning stays out of the pod-binding path: a slow or failed slice create
  must degrade one workload's reconcile, not every pending pod's scheduling.
- No change for static pools: OME on a non-`PROVISION_ONLY` pool behaves exactly
  as today.

### Non-Goals

- **Defragmentation or re-carving.** Reshaping a cube whose partitions are in use
  is out of scope; see [Open Questions](#open-questions).
- **The libtpu session shape itself.** Getting the process group to match the
  partition is a runtime concern outside this proposal.
- **Non-TPU accelerators.** GPU fabrics have no partition object to create.

## Terminology

- **Partition** — a hardware-level TPU subdivision the control plane can grant,
  identified by `cloud.google.com/gke-tpu-partition-<shape>-id` on the node.
  Granting is all-or-nothing: a session gets the whole partition or fails.
- **Slice CR** — `slices.accelerator.gke.io`, the Kubernetes object that binds a
  partition and labels its nodes. Creating one is how a partition becomes
  usable; deleting one releases it.
- **Slice producer** — a controller that creates Slice CRs. Two exist today: NPC
  (owner = `TPUNodePool`, static) and `slice-scheduler` (owner = the workload,
  on demand).

## Proposal

### What must be true

Independent of which option we pick, the contract is:

1. **Shape is derived, not declared twice.** The slice shape must come from the
   same source as the pod's `nodeSelector` and chip request. *Rationale:* they
   are already coupled — a runtime pinned to `2x2x1` with a `2x2x2` selector
   fails at `START_SESSION`, which is how this class of bug reaches production.
2. **Create before admit.** The Slice CR must be `Ready`/`ACTIVE` before the
   gang's pods are bound. *Rationale:* a pod bound to a node whose partition is
   not yet provisioned burns a ~90 s libtpu retry cycle and then crash-loops.
3. **Lifecycle is tied to the owner.** The slice is released when the owning OME
   object goes away, and **not** before. *Rationale:* deleting a slice under a
   running pod is a hardware-level yank; leaking one strands a partition that no
   other workload can claim.
4. **Static pools are untouched.** Absence of `PROVISION_ONLY` means no slice is
   created and nothing changes.

### Option 1: ome-controller provisions, ome-scheduler places

Split the two jobs along the line they already fall on:

- **`ome-controller`** creates and deletes Slice CRs while reconciling the
  workload, exactly as it already creates pods and `InferenceReplica`s.
- **`ome-scheduler`** does what OEP-0022 already says it does — gang placement
  and bin-packing onto whatever slices exist. It is unchanged.

*Rationale for the split:* the scheduler sits in the pod-binding hot path, and
creating a slice is a slow cloud-control-plane operation — the Slice CR must
reach `ACTIVE` before a pod can use it. Blocking a scheduler plugin on that call
couples binding latency for **every** pending pod to one cloud API. The
controller is already the component that owns this workload's lifecycle, and
lifecycle side effects belong with the owner — the same argument OEP-0008 makes
for delegating eviction to the controller that owns the group.

**Provisioning granularity is the controller's decision, and it is not the
workload's size.** A service needing 8 chips is satisfied by a `2x2x2`, one
needing 64 by a `4x4x4`. Beyond that exact-fit choice, the controller may create
*several* slices of one shape at once, so scaling a replica does not cost a
control-plane round trip each time and the scheduler always has somewhere to
pack the next one.

> **Constraint, measured — do not design around the opposite.** A Slice CR is
> exactly **one** partition: a tested `4x4x4` slice carries a
> `partitionIds` list of length 1 and labels 16 nodes. A partition is granted
> whole, so a gang **cannot** take part of one — an 8-chip session on a `4x4x4`
> partition fails exactly as a 4-chip session on a `2x2x2` does (both measured
> above). Therefore "provision coarser" means *create N slices of shape S*, not
> *create one larger slice and subdivide it later*. There is no subdividing.

- **Pro:** keeps every OEP-0022 placement feature, because the scheduler does not
  change. Slice lifetime is tied to an object OME already finalizes, so
  create-before-admit and release-after-drain fall out of ordinary reconciliation
  instead of needing new cross-component coordination.
- **Tradeoff:** a third slice producer in the fleet (NPC, `slice-scheduler`,
  OME), and OME takes on a GKE-specific object it did not previously touch.

### Option 2: slice-scheduler treats OME objects as first-class

OME workloads set `schedulerName: slice-scheduler`, and `slice-scheduler` learns
to own a slice for an OME object (`InferenceReplica` / OMENative Instance) the
way it already does for a `StatefulSet`.

The capability has been verified with StatefulSet workloads:
`slice-scheduler-controller-manager` holds `create`/`delete` on
`slices.accelerator.gke.io`, and creates workload-owned `2x2x1` slices on demand
for pods using `schedulerName: slice-scheduler`. A representative slice carries
the following ownership metadata:

```yaml
labels:
  cloud.google.com/slice-owner-kind: StatefulSet
  cloud.google.com/slice-owner-name: example-workload
annotations:
  cloud.google.com/managed-by: slice-scheduler
```

- **Pro:** no new provisioning code, and one slice-lifecycle implementation for
  the whole fleet rather than an OME-specific fork.
- **Tradeoff:** OME gives up `ome-scheduler` for these workloads, so every
  OEP-0022 placement feature (gang `Permit` gating, replica spreading, later
  preemption) must either already exist in `slice-scheduler` or be lost. It also
  merges provisioning and placement back into one component — the coupling
  Option 1 exists to avoid.

### Comparison and recommendation

| | Option 1 (controller provisions, scheduler places) | Option 2 (slice-scheduler owns both) |
|---|---|---|
| new provisioning code | yes | no |
| keeps OEP-0022 placement | **yes, by construction** | unknown — see Open Question 1 |
| slice creation in the pod-binding hot path | no | yes |
| slice-lifecycle implementations in fleet | 3 | 2 |
| owner model change | none | must accept a non-StatefulSet owner |
| blast radius | OME only | every `slice-scheduler` tenant |

**I recommend Option 1.** The decisive property is not code reuse — it is that
provisioning and placement have different latencies, different failure modes and
different owners, and Option 1 is the only arrangement that keeps them apart. A
slow or failed slice create degrades one reconcile loop; under Option 2 the same
failure sits in the path that binds every pod that scheduler is responsible for.

Option 2 stays attractive on one axis — two slice producers instead of three —
and that axis gets heavier if `slice-scheduler` already does gang placement to
OEP-0022's standard. I do not think it outweighs the coupling, but that is a
judgment rather than a measurement, and Open Question 1 is what would change it.

## Design Details

Common to both options (under Option 1 all of this lives in `ome-controller`):

- **Shape derivation.** The slice shape comes from the engine's
  `nodeSelector["cloud.google.com/gke-tpu-topology"]` plus the pod count, and
  admission rejects a service whose chip request cannot tile that shape.
  *Failure mode if skipped:* the mismatch surfaces ~90 s later as
  `START_SESSION failed`, with no indication which of the two fields is wrong.
- **Ownership label.** The Slice CR carries owner kind/name pointing at the OME
  object, mirroring `slice-scheduler`'s existing
  `cloud.google.com/slice-owner-{kind,name}`.
- **Release.** On owner deletion the slice is deleted; a finalizer on the OME
  object prevents release while pods still hold the partition. A slice is also
  kept while pods of another workload hold chips on its hosts: deleting it
  deactivates the partition under them.
- **Static-pool short-circuit.** No `PROVISION_ONLY` on the candidate nodes → no
  slice work at all.
- **Host health.** A `Ready` slice that no pod holds, with a host its pods
  cannot be scheduled on (cordoned or not ready, after the pods' tolerations), is
  released and provisioned again. Its pods are withheld meanwhile, with a
  `SliceHostUnavailable` warning. A slice a pod still holds is left to the
  scheduler. The pods also wait until every host the slice needs is visible,
  since hosts are found by a label that can lag the slice's state, and a slice
  whose readiness the provider reports as unknown is not ready. With
  `slice.readyTimeout` set, a slice that has partitions but stays out of a ready
  state longer than that, while no pod holds it, is released and provisioned
  again with a `SliceReadyTimeout` warning. *Failure mode if skipped:* a node
  that goes bad after the slice is `ACTIVE` leaves the pods `Pending`, a
  recreated pod lands on the same slice, and a slice that never activates
  parks its Instance with no deadline.
- **Lost slice.** A slice deleted from outside while pods run on it (gone,
  being deleted, or replaced by a newer slice of the same name) has its pods
  deleted with a `SliceLost` warning, counted by `ome_tpu_slice_lost_total`.
  Their Instance is rebuilt on a new slice of the same name. *Failure mode if
  skipped:* the pods stay bound and `Ready` on chips that are no longer
  theirs, and nothing reacts.

Deferred deliberately: retry/backoff policy on a failed create, and behavior when
the cube has free chips but no free *partition* of the requested shape. Both need
the fragmentation answer first.

## Test Plan

- **Unit:** shape derivation from `nodeSelector` + pod count; admission rejects
  mismatched chip requests.
- **Integration (envtest):** slice created before pods bind; slice deleted on
  owner deletion; finalizer blocks release while a pod holds it; no slice created
  on a static pool.
- **e2e on a test cluster:** a 4-chip OME service starts on a cube with **no**
  pre-carved `2x2x1`, reaching 4/4 `Init worker` and no `START_SESSION` failures.
  Verify both matching and mismatched session shapes against the table above.

## Graduation Criteria

- **Alpha:** one shape (`2x2x1`), single-host, opt-in per ISVC on a test cluster.
- **Beta:** multi-host shapes (`2x2x2`+) with gang placement preserved; slice
  leak rate measured at 0 over a week of rollouts; metric for created/released/
  leaked slices.
- **Stable:** default for `PROVISION_ONLY` pools; a documented answer to
  fragmentation.

## Drawbacks

OME becomes coupled to a GKE-specific object. `slices.accelerator.gke.io` has no
analogue on other clouds, so this is the first place OME's accelerator model
stops being portable. Option 2 confines that coupling to `slice-scheduler`,
whereas Option 1 pulls it into `ome-controller`. That is a real cost of the
option I am recommending, and the honest mitigation is only that the coupling is
one CRD behind an interface the controller can stub per-provider — not that it
goes away.

## Alternatives

**Static carve only (no OEP).** Pre-carve cubes `{"2x2x1": 16}` via NPC and
change nothing in OME. This has been verified for a fixed 4-chip workload shape:
it packs 16 independent workloads per cube, the same as 16 static hosts, and
needs no new OME code.

I recommend this as a **near-term** option for known, uniform shapes. Dynamic
provisioning is useful when shapes are heterogeneous and unpredictable. Carving
requires a full `4x4x4` cube: a `2x2x2` pool reports
`SLICES: unsupported on 2x2x2` and must be rebuilt before it can provide
`2x2x1` slices.

## Open Questions

1. **Does `slice-scheduler` do gang placement to OEP-0022's standard?** This is
   the only fact that would move me off Option 1: if the answer is yes, Option 2
   costs one fewer slice producer for no placement regression. Worth an hour of
   reading its source before we commit. *Owner: TBD.*
2. **Can `slice-scheduler` own a slice for a non-StatefulSet?** The verified
   examples are StatefulSets. If the owner model is hardcoded, Option 2 is more
   than a `schedulerName` change. *Owner: TBD.*
3. **Fragmentation.** Creating a `2x2x1` needs a cube not already carved
   incompatibly; a cube carved `{"2x2x2": 8}` with slices in use cannot yield
   one. Dynamic creation moves the planning problem rather than removing it.
   Mixed carves are legal (the CEL rule caps only total chips at 64), which may
   be enough — **unverified.** *Owner: TBD.*
4. **Should the admission policy land first, regardless?** A Warn-mode policy
   could flag single-host pods on multi-host
   partitions. It is cheap and catches the mistake at submit time instead of at
   `START_SESSION`. I think yes, independent of this OEP.

**Scope:** this applies to OME TPU workloads on `PROVISION_ONLY` pools. Static
pools and non-TPU accelerators are unaffected.

**See also:** [OEP-0022](../0022-ome-scheduler/) (`ome-scheduler` placement — the
layer this builds on).
