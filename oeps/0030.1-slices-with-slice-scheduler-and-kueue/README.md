# OEP-0030.1: TPU slices with slice-scheduler and Kueue

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Terminology](#terminology)
  - [What was verified](#what-was-verified)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Parts 1 and 2: merged](#parts-1-and-2-merged)
  - [Part 3: Provision after Kueue reserves quota](#part-3-provision-after-kueue-reserves-quota)
  - [Part 4: One Kueue Workload per Instance](#part-4-one-kueue-workload-per-instance)
  - [Part 5: A configured priority class name](#part-5-a-configured-priority-class-name)
  - [Part 6: slice-scheduler waits for OME's pods](#part-6-slice-scheduler-waits-for-omes-pods)
- [Design Details](#design-details)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** OME creates a TPU slice before Kueue decides whether the Instance
may run. So an Instance waiting on quota holds a slice it cannot use, and every
chip in it. I propose that OME provision a queued Instance's slice as a Kueue
admission check, after Kueue has reserved the quota.

OEP-0030 made `ome-controller` provision the slice each Instance runs on. Those
slices meet two schedulers OEP-0030 did not design for: `slice-scheduler`, which
moves and preempts slices, and Kueue, which gates pods on quota. Checking both
found six problems. Two are fixed (#2224, #2225); this OEP proposes fixes for
the other four.

This applies to OMENative workloads on provision-only TPU pools. Static pools,
and pods without a Kueue queue label, are unaffected.

## Motivation

### Terminology

- **Slice**: a GKE `Slice` object that binds a TPU topology to partitions of a
  provision-only node pool. Its hosts carry `cloud.google.com/gke-tpu-slice`
  set to its name, and a pod that selects that label is *confined* to the
  slice. OME derives the name from the Instance and slot, so a pod can name its
  slice before the slice exists.
- **`slice-scheduler`**: the provider's controller that assigns partitions to
  slices, preempts lower-priority slices (`cloud.google.com/slice-priority`),
  and reassigns failed ones. It binds only pods whose `schedulerName` is
  `slice-scheduler`; `ome-scheduler` binds OME's pods.
- **Gated**: a pod with `kueue.x-k8s.io/queue-name` is created with a scheduling
  gate, which keeps it unschedulable. Kueue tracks it as a *Workload* and
  removes the gate, i.e. *admits* it, once it fits its ClusterQueue's quota.
- **Kueue preemption**: Kueue deleting an admitted Workload's pods to admit a
  higher-priority one. It is unrelated to slice preemption by
  `slice-scheduler`; the two read different priorities.

### What was verified

Each finding was checked against the code. The Kueue findings were also run
hermetically: envtest, a stub `Slice` CRD, the InferenceReplica controller with
slice provisioning, and Kueue v0.19.3 (controllers, scheduler and pod webhook)
in one manager. No kubelet ran, so "admitted" means Kueue removed the gate.
Findings 3 to 6 were rechecked on main at `e2e17b02e` (2026-10-09); the code
they rest on has not changed.

1. *Fixed by #2224.* `slice-scheduler` cleared an OME slice's partitions while
   OME's pods still ran on the old hosts. OME noticed only a deleted slice.
2. *Fixed by #2225.* Every OME slice carried the same priority.
3. **A queued Instance holds a slice Kueue has not admitted.** With the queue's
   quota used, a second Instance's slice was created 3.5 s after the Instance,
   before any pod existed for Kueue to judge. OME then created the pod and
   Kueue gated it. The slice stayed ready, its chips idle, for as long as the
   pod waited (30 s and more in the run).
4. **Kueue preemption frees quota, not chips.** Kueue deleted the low-priority
   pod 0.5 s after the high-priority pod appeared. OME recreated it 1 s later
   and Kueue gated it. 45 s later the low Instance still held the same ready
   slice: a gated pod counts as holding its slice, so nothing releases it.
5. **A multi-pod Instance is admitted pod by pod.** OME sets no Kueue pod-group
   labels, so each pod is its own Workload. With quota for one of two pods,
   Kueue admitted the leader and gated the worker. The whole 8-chip slice stayed
   held for an Instance that cannot start on half its pods.
6. **A missing WorkloadPriorityClass gates a pod forever.** The
   dedicated-cluster path stamps `kueue-scheduling-high-priority`, a Go
   constant. On a cluster without that class Kueue creates no Workload, so the
   pod stays gated and, per finding 3, holds its slice.

### Goals

1. A queued Instance holds no slice until Kueue has reserved its quota (Part 3).
2. A Kueue preemption frees the victim's chips, not only its quota (Part 3).
3. Kueue admits a multi-pod Instance whole or not at all (Part 4).
4. No Kueue priority class name is a literal in the binary (Part 5).
5. `slice-scheduler` reassigns a slice's partitions only after OME's pods have
   left them (Part 6).

### Non-Goals

- Changing Kueue.
- Fleet-wide quota, which OEP-0024 covers.
- Static pools, which OME neither provisions nor releases.

## Proposal

### Parts 1 and 2: merged

- **Part 1 (#2224).** A slice is *lost* when it has no partitions, or a bound
  pod's node no longer carries its label. OME then deletes the slice's bound
  pods and rebuilds the Instance once the slice is ready on its new hosts.
- **Part 2 (#2225).** `tpuSliceProvisioning.slice.podAnnotations` lists
  annotation keys copied from the pod template onto each slice, e.g.
  `cloud.google.com/slice-priority`. A slice keeps the values it was created
  with.

### Part 3: Provision after Kueue reserves quota

**What must be true.**

1. A queued Instance, one whose pods carry a queue label, holds no slice until
   Kueue has reserved its quota (findings 3 and 5).
2. When Kueue evicts a queued Instance, OME releases its slice once no pod runs
   on it (finding 4).
3. A pod becomes schedulable only once its slice is ready and its hosts are
   visible. This is OEP-0030's guarantee, restated.
4. An Instance without a queue label provisions exactly as it does today.

**Options.**

a) **Provision the slice as a Kueue admission check.** An operator lists OME's
   `AdmissionCheck` on a ClusterQueue. OME creates a queued Instance's pods
   first, confined to the slot's slice by name. Kueue gates them, reserves the
   quota, and waits on the check. OME then provisions the slice and marks the
   check `Ready` once the slice is ready and its hosts visible, and Kueue
   removes the gate. On eviction Kueue resets the check and deletes the pods,
   and OME releases the slice. *Tradeoff:* OME depends on Kueue's `Workload`
   and `AdmissionCheck` APIs.
b) **A second scheduling gate that OME owns.** OME provisions after Kueue
   removes its gate, then removes its own. *Tradeoff:* Kueue counts the
   Instance admitted for the minutes the slice takes, and a slice that cannot
   be provisioned has no way to hand the quota back.
c) **Release a gated Instance's slice after a timeout.** *Tradeoff:* a
   mitigation, not a fix. The slice is still created before Kueue looks, held
   for the timeout, and churned for an Instance that waits longer than it.

**I recommend (a).** Admission checks exist for provisioning that must follow a
quota reservation; Kueue's `ProvisioningRequest` check does the same for node
autoscaling. With (a), Kueue owns retries and backoff (a check in `Retry`
returns the quota and requeues), the Workload shows what it waits on, and
eviction resets the check, which is the signal requirement 2 needs. (c) is worth
shipping only as a stopgap while (a) is built.

Q. *"Creating pods before their slice is ready breaks OEP-0030's rule."*
A. That rule stops a pod from waiting `Pending` on a slice that may never
activate, and from binding to a half-activated one. A gated pod cannot be
scheduled, and `Ready` requires the same readiness OEP-0030 checks. The
guarantee holds; it now attaches to when the pod becomes schedulable, not when
it is created.

Q. *"Why not size quota under provisionable capacity instead?"* A. That caps
how many Instances hold slices. It does not stop a queued Instance from holding
a slice it got before Kueue looked. It narrows the gap; it does not close it.

### Part 4: One Kueue Workload per Instance

Each pod of a multi-pod Instance carries `kueue.x-k8s.io/pod-group-name`, unique
to the Instance's current incarnation, and the
`kueue.x-k8s.io/pod-group-total-count` annotation. Kueue then admits all of an
Instance's pods or none, and with Part 3 one Workload maps to one slice.

OME's own gang, the scheduler-plugins `PodGroup`, sees no pod until Kueue
ungates all of them, so I expect no deadlock between the two. That is OEP-0018's
open question, and the suite below must show it before Beta.

*Failure mode if skipped:* finding 5.

### Part 5: A configured priority class name

The dedicated-cluster path reads the WorkloadPriorityClass name from config,
with no in-code default; unset, it stamps no priority-class label. OEP-0024
already flags the constant as a hardcoded value.

*Failure mode if skipped:* finding 6.

### Part 6: slice-scheduler waits for OME's pods

`slice-scheduler` drains a preempted or failed slice before it clears the
partitions: it lists the pods labeled with the slice's name and evicts those
whose `schedulerName` is `slice-scheduler` (its pod reconciler, vendored by
#2555). OME's pods carry no such label, and the `schedulerName` filter would
drop them if they did, so it clears the partitions at once. Part 1 recovers
afterwards, but until it does, the preemptor's pods can be bound to hosts whose
chips OME's pods still hold.

The fix has two halves, and neither works alone:

1. OME labels its pods with their slice's name. `slice-scheduler` waits for
   every pod with that label before it clears the partitions, and still evicts
   only its own.
2. OME deletes a slice's bound pods when the slice is marked for eviction
   (`cloud.google.com/evict-pods: "true"`) or reports a failed state, then
   rebuilds the Instance as Part 1 does.

*Failure mode with only the first half:* the two wait on each other forever,
`slice-scheduler` for OME's pods and OME for the partitions to change. The first
half also patches the vendored `slice-scheduler`, so it belongs upstream.

## Design Details

Mechanism for Part 3 (a); none of it is built.

- **Which Instances.** An Instance is queued when its pod template carries the
  queue label. Only queued Instances change order.
- **Pods first.** For a queued Instance the create path skips the wait for the
  slice and creates the pods, each confined by name to its slot's slice. Kueue
  v0.19.3 would also apply a node selector from the check to a plain pod before
  removing its gate, but slice names are known up front, so the check need not.
- **The check controller.** It answers to a controller name read from
  `tpuSliceProvisioning`, with no in-code default. It maps a Workload to its
  Instance's slot through the pods' labels and sets the check from the slice:
  `Ready` once the slice is ready and its hosts visible; `Retry` when a create
  fails; `Rejected` when another owner holds the slot's name; `Pending`
  otherwise, with the slice's state as the message.
- **Release.** A slot whose pods are all gated holds its slice only for the
  check. When Kueue resets the check on eviction, OME releases the slice once no
  pod runs on it.
- **Lost slices.** A gated pod never ran, so a slice released and created again
  under it is not lost. Part 1's rules apply only to pods Kueue has ungated.

## Test Plan

- **Parts 3 to 5:** a hermetic real-Kueue envtest suite, the setup described
  under [What was verified](#what-was-verified), running findings 3 to 6 as
  scenarios. Today it confirms them; with Parts 3 to 5 the expectations flip: no
  slice before quota, a released slice after a preemption, a gang admitted
  whole, and no stranded pod for a missing class. The suite is not in the tree
  yet; it lands with Part 3.
- **Part 6:** an envtest case where a slice is marked for eviction under an
  Instance's pods, and the slice is not reassigned until they are gone.
- **e2e on a test cluster** with quota below demand: no slice held by a gated
  Instance beyond the check's wait, and a preemption that frees the victim's
  partitions within one slice release (TBD: a number once measured).

## Graduation Criteria

- **Alpha:** Parts 1, 2 and 5 merged.
- **Beta:** Part 3 (a) available to a ClusterQueue that lists the check, Part 4
  behind config, and the hermetic suite passing with the flipped expectations.
- **Stable:** Part 3 the default for queued TPU workloads on provision-only
  pools, with a measured preemption-to-free time.

## Drawbacks

OME takes a dependency on Kueue's `Workload` and `AdmissionCheck` APIs, and one
Instance's admission is split across two controllers. Debugging a stuck Instance
means reading its InferenceReplica status and its Workload's check. The only
mitigation is that the check's message carries the slice's state.

## Alternatives

1. **Static slices for queued workloads.** Pre-carve the shapes a queue needs
   and leave quota as the only gate. Proven for uniform shapes, but it gives up
   on-demand shapes, which is why OEP-0030 exists.
2. **`slice-scheduler` as the check controller.** Fewer moving parts in OME,
   but `slice-scheduler` would have to understand Instances and slots, the
   coupling OEP-0030 rejected.
3. **Kueue's `ProvisioningRequest` check.** Built for node autoscaling. Nothing
   implements it for GKE slices, so OME would write that implementation anyway.

## Open Questions

1. How does a Kueue pod group treat a replaced pod under OME's restart
   policies, and must a replacement start a new group? TBD.
2. Should slice priority (Part 2) follow the Workload's priority class, so an
   operator sets one priority instead of two? TBD.
3. Will upstream `slice-scheduler` accept Part 6's first half? Owner: TBD.
