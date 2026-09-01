# OEP-0008: Alfred GPU Cluster Caretaker

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [The GPU fragmentation problem](#the-gpu-fragmentation-problem)
  - [Why pure eviction is insufficient](#why-pure-eviction-is-insufficient)
  - [Why one caretaker, not N controllers](#why-one-caretaker-not-n-controllers)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [What Alfred is](#what-alfred-is)
  - [Architectural posture: observer, recommender, narrow actuator](#architectural-posture-observer-recommender-narrow-actuator)
  - [The engine: snapshot → policies → arbiter → dispatcher](#the-engine-snapshot-policies-arbiter-dispatcher)
  - [Core concepts](#core-concepts)
  - [User stories](#user-stories)
    - [Story 1: Operator observes fragmentation, then opts into action](#story-1-operator-observes-fragmentation-then-opts-into-action)
    - [Story 2: Alfred unblocks a pending large workload](#story-2-alfred-unblocks-a-pending-large-workload)
    - [Story 3: A node goes GpuUnhealthy — Alfred evacuates and signals for repair](#story-3-a-node-goes-gpuunhealthy-alfred-evacuates-and-signals-for-repair)
    - [Story 4: Maintenance-window honor](#story-4-maintenance-window-honor)
    - [Story 5: Opt-out for a critical workload](#story-5-opt-out-for-a-critical-workload)
    - [Story 6: LWS legacy workload — recommendation only](#story-6-lws-legacy-workload-recommendation-only)
    - [Story 7: Coexistence with cluster-autoscaler](#story-7-coexistence-with-cluster-autoscaler)
  - [Risks and mitigations](#risks-and-mitigations)
- [Design details](#design-details)
  - [Policy #1: Defragmentation](#policy-1-defragmentation)
    - [Observation layer](#observation-layer)
    - [Fragmentation scoring](#fragmentation-scoring)
    - [Candidate selection](#candidate-selection)
    - [Placement-hint computation](#placement-hint-computation)
    - [Execution](#execution)
  - [Policy #2: Node-health evacuation](#policy-2-node-health-evacuation)
    - [Goal and context](#goal-and-context)
    - [What must be true](#what-must-be-true)
    - [Approach (how)](#approach-how)
    - [The narrow-contracts property (no new RBAC, no cloud credentials)](#the-narrow-contracts-property-no-new-rbac-no-cloud-credentials)
    - [Anticipated objections](#anticipated-objections)
  - [Policy #3: Descheduling (future)](#policy-3-descheduling-future)
  - [The arbiter (arbitration-lite)](#the-arbiter-arbitration-lite)
    - [Why an arbiter at all](#why-an-arbiter-at-all)
    - [The four rules](#the-four-rules)
    - [Why lite, and what a heavier version would add](#why-lite-and-what-a-heavier-version-would-add)
  - [Coexisting with the autoscaler (OEP-0013)](#coexisting-with-the-autoscaler-oep-0013)
  - [Policy and configuration model](#policy-and-configuration-model)
    - [Opt-in / opt-out semantics](#opt-in-opt-out-semantics)
  - [Safety bounds](#safety-bounds)
  - [Concurrent-operation awareness](#concurrent-operation-awareness)
  - [Spot and preemptible nodes](#spot-and-preemptible-nodes)
  - [Multi-tenancy](#multi-tenancy)
  - [Model-download coordination](#model-download-coordination)
  - [Degraded mode](#degraded-mode)
  - [Deployment model](#deployment-model)
  - [Leader election](#leader-election)
  - [RBAC](#rbac)
  - [Observability](#observability)
- [Test plan](#test-plan)
  - [Unit tests](#unit-tests)
  - [Integration tests](#integration-tests)
  - [Chaos / robustness](#chaos-robustness)
  - [Simulation](#simulation)
- [Graduation criteria](#graduation-criteria)
  - [Alpha](#alpha)
  - [Beta](#beta)
  - [GA](#ga)
- [Implementation history](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
  - [Alternative 1: Alfred as a scheduler plugin](#alternative-1-alfred-as-a-scheduler-plugin)
  - [Alternative 2: Embed Alfred in the OME manager](#alternative-2-embed-alfred-in-the-ome-manager)
  - [Alternative 3: Alfred as a CRD-driven system](#alternative-3-alfred-as-a-crd-driven-system)
  - [Alternative 4: Don't build Alfred; manual operator intervention](#alternative-4-dont-build-alfred-manual-operator-intervention)
  - [Alternative 5: Extend cluster-autoscaler](#alternative-5-extend-cluster-autoscaler)
  - [Alternative 6: Keep Alfred a single-purpose defragmenter; put node-health and descheduling in separate controllers](#alternative-6-keep-alfred-a-single-purpose-defragmenter-put-node-health-and-descheduling-in-separate-controllers)
- [Open questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** Alfred is the GPU **Cluster Caretaker** — a cluster-level control loop that watches the *physical* GPU layer (nodes, accelerators, workload placement) and, when it drifts into a bad state, arbitrates a safe corrective action and actuates it through the existing narrow write contracts. It does not schedule pods, scale replicas, or touch node lifecycle.

The caretaker is a single observe → arbitrate → actuate engine, not a grab-bag of one-off scripts. One shared, read-only **ClusterSnapshot** (the physical GPU state for a reconcile pass) is fed to a set of **Policies**; each Policy is a pure function `Evaluate(snapshot) → []Candidate`, where a *Candidate* is a proposed action plus its justification. An **Arbiter** selects and sequences across all Policies' candidates under global safety bounds (rate limits, cooldowns, concurrency caps), and a **Dispatcher** actuates the winners through the same narrow contracts the original design established: OMENative's migration annotation for multi-pod Instances, and the K8s Eviction API for single-pod `RawDeployment` workloads.

Three policies, staged: **Policy #1 — Defragmentation** is the existing logic from this OEP's first scope (detect GPU fragmentation, recommend/execute consolidating migrations). **Policy #2 — Node-Health Evacuation** is new: on a bad node condition, evacuate OME workloads off the node *and emit a remediation signal* — it does **not** cordon, drain, terminate, or reboot the node. **Policy #3 — Descheduling** (rebalance against drifted constraints) is future, and named here only to show the engine generalizes. Alfred introduces **no new CRDs**: configuration lives in a `ConfigMap`, per-workload gating in annotations, and output in Prometheus metrics, K8s Events, and an optional recommendations `ConfigMap`.

## Motivation

### The GPU fragmentation problem

In GPU-dense clusters running LLM inference, the default K8s scheduler's spread behavior produces a predictable failure mode. Consider 10 nodes × 8 H100s = 80 GPUs. Deploy 10 × 1-GPU workloads spread one-per-node: each node has 7 free GPUs, 70 free in total. Now deploy a workload needing 8 GPUs on one node — the scheduler cannot place it despite 70 GPUs being "free," because no single node has 8 *contiguous* free GPUs. The cluster is not out of capacity; it is fragmented.

With bin-packing, those 10 small workloads occupy 1–2 nodes (e.g., Node1 full at 8 workloads, Node2 holds 2), leaving 8 empty nodes — each of which can accept the 8-GPU workload. So fragmentation splits into two sub-problems:

1. **Initial placement (bin-packing).** At deployment time. Solved at the scheduler level: deploy Volcano, Yunikorn, or scheduler-plugins with `NodeResourcesFit: MostAllocated`. OME supports this today via `PodSpec.SchedulerName`. **Not Alfred's concern.**
2. **Runtime defragmentation.** As workloads come and go, placement drifts from optimal. Scale-downs, deletions, node additions, failed-then-rescheduled pods all leave fragmentation behind. Even with a bin-packing scheduler at placement time, cluster state degrades over its lifetime. **This is Alfred's problem.**

The analog is cluster-autoscaler (CA): CA doesn't handle initial resource requests; it handles dynamic node-utilization state *after* workloads evolve. Alfred is to GPU fragmentation what CA is to node utilization — a control loop that corrects the steady-state drift the scheduler does not revisit.

### Why pure eviction is insufficient

A naive caretaker would use the K8s Eviction API universally, trusting the scheduler to re-place evicted pods more densely. For single-pod workloads this works: brief disruption, the pod lands wherever the scheduler puts it.

For **multi-pod Instances** (OMENative's multi-node engine/decoder grouping — see [OEP-0007](../0007-omenative-workload-strategy/README.md)) and legacy **LWS groups**, eviction has sharp edges. An *atomic multi-pod group* is a set of pods that must start, stop, and migrate together; evicting one without the others leaves a half-broken workload — which is exactly why blind eviction is unsafe here.

- **LWS with `RecreateGroupOnPodRestart`** (OME's current default for LWS) tears down the *entire* group on any pod deletion. No surge, no rollback: if the scheduler can't place all `N` replacement pods simultaneously, the service stays down.
- **OMENative Instances** have atomic group semantics too — evicting one pod triggers a per-Instance restart, cleaner than LWS's whole-group teardown but still not surge-protected. So pure eviction is unsafe for OMENative multi-pod Instances as well.

The answer is **not** "smarter eviction." The answer is to narrow the action surface: for multi-pod Instances, the Dispatcher delegates execution to OMENative's first-class migration verb (surge-then-swap or drain-then-rebuild) via the published annotation contract; eviction stays available **only** for single-pod `RawDeployment` workloads, where it is safe. This narrow-actuation rule is load-bearing for every policy, not just defragmentation: it is the property that lets the Arbiter reason about safety uniformly (next section).

### Why one caretaker, not N controllers

Tl;dr: defragmentation, node-health evacuation, and descheduling are *different policies over the same physical state*, and the hard parts they share — building a consistent snapshot, enforcing safety bounds, and actuating without corrupting workloads — should be written once, not three times.

The reader's reasonable objection: "These are three unrelated jobs. Why not three small controllers?" Because the genuinely difficult, genuinely dangerous machinery is identical across all three:

1. **One ClusterSnapshot.** Every policy reasons about the same physical GPU layer — node GPU inventory, per-pod GPU consumption, accelerator health, pending-pod pressure. Building that view *consistently* (so two policies don't act on disagreeing state in the same pass) is the same work whether the trigger is fragmentation or a `GpuUnhealthy` condition. Three controllers means three snapshots that can disagree, and three reconcile loops racing each other on the same pods.
2. **One safety layer.** Rate limits, per-workload cooldowns, concurrency caps, and "reject high-uncertainty actions" (*primum non nocere*) are not policy-specific — they bound *actuation*, regardless of which policy proposed it. Centralizing them in the Arbiter means a defrag migration and a health evacuation can't both move the same workload, or together exceed the cluster's churn budget. N independent controllers cannot enforce a shared budget without inventing a coordination protocol between them — i.e., reinventing the Arbiter, badly.
3. **One narrow actuation contract.** Both moving a fragmented workload and evacuating a workload off a bad node reduce to the *same* primitive: migrate this workload safely via OMENative's annotation, or evict it via the K8s API if it's a single pod. The Dispatcher already encodes "how to move an OME workload without breaking it." A second controller would either duplicate that contract or, worse, take a wider, more dangerous surface.

This is also where the **archived `ome-operator` Alfred** re-enters the design. That component (in the retired, pre-open-source `ome-operator` repository) carried four pieces, audited as:

- **Auto-patching** (`pkg/alfred/autopatching/autopatching.go`) — cordons nodes, triggers GPU node image rotation via the OCI Compute API, drains single-replica InferenceServices.
- **Auto-repair** (`pkg/alfred/autorepair/autorepair.go`) — watches for a `GpuUnhealthy` node condition, then cordons, drains, and triggers a soft reset via the OCI Compute API.
- **Rescheduling** (`pkg/alfred/rescheduling/rescheduling.go`) — a 6-line stub; its one function `gpuEfficiencyScore` was never called.
- **Capacity check** (`pkg/alfred/application/capacityCheck.go`) — a stub returning hardcoded `true` with a `// will add real logic later` comment.

The rescheduling and bin-packing logic never shipped; **Policy #1 (Defragmentation)** is the realization of that original, never-built vision. The old auto-repair is **partially** realized here as **Policy #2 (Node-Health Evacuation)** — but only the safe half. On a bad node condition, Policy #2 evacuates OME workloads off the node through the *same narrow Dispatcher contract every other policy uses*, and emits a remediation signal (a Prometheus metric and a K8s Event) for whatever system actually owns hardware remediation. It explicitly does **not** cordon, drain, terminate, or reboot the node, and it does **not** call any cloud Compute API. That is the line between the archived design and this one: we keep "react to a `GpuUnhealthy` node," we drop "patch/repair the hardware." (Refined non-goal below.)

Generalizing to a caretaker engine has a cost, and it is worth naming: a single process now carries three policies' blast radius, and a snapshot bug or a Dispatcher bug is a bug for *all* policies at once. The mitigation is the same property that makes the engine tractable — every policy actuates only through the narrow contract and only under the Arbiter's bounds — so a misbehaving policy is bounded to "proposes a bad Candidate," which the Arbiter can reject, not "directly does damage." This is a mitigation, not a full fix: a wrong *snapshot* fools the Arbiter too. TBD: the snapshot-consistency guarantees and how a policy declares a Candidate's confidence are Design Details, not settled here.

### Goals

1. **Run one observe → arbitrate → actuate engine** over the physical GPU layer: a shared read-only ClusterSnapshot, a set of Policies (`Evaluate(snapshot) → []Candidate`), an Arbiter that selects and sequences under global safety bounds, and a Dispatcher that actuates through the existing narrow contracts. The engine is the deliverable; policies are pluggable on top of it.
2. **Ship Policy #1 — Defragmentation.** Continuously observe GPU utilization, workload placement, and pending-pod pressure; compute a fragmentation metric; produce candidates identifying which workloads, if relocated, would reduce fragmentation most; and (optionally, per opt-in) execute those migrations.
3. **Ship Policy #2 — Node-Health Evacuation.** On a bad node condition (consumed from existing signals — node conditions, AcceleratorClass status — not newly detected by Alfred), produce candidates that evacuate OME workloads off the affected node through the narrow contract, **and** emit a remediation signal (metric + Event) for the system that owns remediation. Evacuate and signal; never cordon, drain, terminate, or reboot.
4. **Reserve Policy #3 — Descheduling** as a future policy on the same engine (rebalancing workloads whose placement has drifted from current constraints). Named here only to validate that the engine generalizes; **no** behavior is promised in this OEP.
5. **Actuate only through narrow contracts.** OMENative's migration annotation (the published OEP-0007 contract) for multi-pod Instances; the K8s Eviction API for single-pod `RawDeployment` workloads. Never modify OME-owned reconciliation state by any other path.
6. **Operate safely by default.** Conservative rate limits, per-workload cooldowns, concurrency caps, and explicit rejection of high-uncertainty actions, enforced centrally in the Arbiter across all policies. First principle: *primum non nocere.*
7. **Introduce no new CRDs.** Configuration via `ConfigMap`; per-workload gating via annotations on `InferenceService`; output via Prometheus metrics, K8s Events, and an optional recommendations `ConfigMap`.
8. **Be useful without OMENative** for `RawDeployment` single-pod workloads (a non-trivial fraction of real clusters), where eviction is a safe actuation path.
9. **Scale to large clusters** (tested design target: 1000 nodes, 10k pods, 100+ InferenceServices).

### Non-Goals

1. **Not a scheduler, and not in the scheduling hot path.** Alfred does not make initial pod-placement decisions and is not a K8s scheduler extender or plugin. The K8s scheduler (default or custom) places pods; Alfred is a separate, out-of-band controller. *Rationale:* placement is already owned, and competing with the scheduler in-band is how you get oscillation.
2. **No node lifecycle management — Alfred does not cordon, drain, terminate, or reboot nodes.** This stays **true** even with Policy #2: node-health *evacuation* moves OME *workloads* off a node through the narrow Dispatcher contract and emits a remediation signal; it never touches the *node* object's lifecycle. Cordon/drain/terminate/reboot belong to the operator or cluster autoscaler. *Rationale:* node lifecycle is destructive and cluster-wide; keeping Alfred to workload-level actuation is what keeps its blast radius bounded.
3. **No replica scaling — that is [OEP-0013](../0013-autoscaling/README.md)'s domain.** Alfred relocates existing workloads; it never changes how many replicas a workload has. *Rationale:* scaling and defragmentation are separate control loops with separate triggers; merging them couples two independent decisions and double-counts churn budget.
4. **No new CRDs.** Hard constraint. Configuration is a `ConfigMap`, gating is annotations, output is metrics/Events/ConfigMap. *Rationale:* a caretaker should be deployable into an existing OME cluster without an API surface change or a migration.
5. **Node-health *evacuation* is in scope (Policy #2); hardware *remediation* stays deferred.** This **refines** the archived Alfred's blanket "no auto-repair." We keep reacting to a bad node condition by evacuating workloads and signaling; we do **not** patch, repair, soft-reset, image-rotate, or call any cloud Compute API. Remediation — the part that actually fixes or recycles hardware — remains out of scope and is left to the operator/cloud, with an optional remediation plugin a possible *future* extension (not promised here). *Rationale:* evacuation is reversible and lives entirely within Alfred's existing narrow contract; remediation is irreversible, cloud-specific, and a different trust boundary.
6. **Not solving GPU health detection.** Alfred *consumes* existing signals (AcceleratorClass status, node conditions) to trigger Policy #2; it adds no new telemetry and runs no health probes of its own. *Rationale:* detection is a separate concern with its own ownership; conflating it would expand scope past a caretaker.
7. **Not an optimization engine in the mathematical sense.** Policies use heuristic candidate selection under safety bounds. "Good enough to reduce fragmentation" is the standard; global optimality is **not** promised. *Rationale:* optimal bin-packing is NP-hard and the cluster state moves under you anyway; a safe heuristic that converges beats an optimum that's stale on arrival.
8. **Not managing workloads beyond InferenceService.** Non-OME GPU workloads are *observed* (they count toward the fragmentation picture in the ClusterSnapshot) but **never** migrated or evacuated. *Rationale:* Alfred only has a safe actuation contract for OME-owned workloads; acting on anything else would exceed that contract.
9. **Not causing preemption.** Users set `priorityClass` on `InferenceService` and the scheduler enforces it; Alfred does not preempt. *Rationale:* preemption is the scheduler's decision, and an out-of-band preemptor would race it.
10. **Not integrated with DRA in v1.** Forward-looking; revisit when Dynamic Resource Allocation matures. *Rationale:* DRA changes how accelerators are modeled, which would reshape the ClusterSnapshot — premature to bind to it now. (TBD: revisit trigger.)

## Proposal

Tl;dr: Alfred is a GPU **Cluster Caretaker** — a leader-elected controller that
runs one loop, observe → arbitrate → actuate, on top of a shared read-only
**ClusterSnapshot**. Independent **Policies** each propose work against that
snapshot; a single **Arbiter** selects and sequences across them; a
**Dispatcher** actuates only through the narrow write contracts OEP-0008 already
owns. The caretaker never orchestrates pod lifecycle itself, so its blast radius
is bounded by that surface — and adding the new node-health policy needs **no**
new node-write RBAC and **no** cloud credentials.

### What Alfred is

Alfred is a Kubernetes controller deployed as a leader-elected `Deployment`
(typically 3 replicas, one active) in OME's namespace or a dedicated namespace.
"Caretaker" is the precise word, not branding: Alfred *tends* the GPU cluster —
it watches for conditions that degrade GPU utilization or workload health, and it
nudges the cluster back toward a better state without ever owning the workloads
it nudges. It does not schedule, it does not provision, it does not repair
hardware. It observes, decides, and hands work to whoever already owns the
execution.

Alfred runs two loops:

- **Observation loop** (default 30s): refresh the `ClusterSnapshot` from
  informers, compute fragmentation and health signals, update Prometheus gauges.
  This loop only reads and only emits metrics — it never actuates.
- **Decision loop** (default 5m): hand the latest snapshot to every Policy,
  collect their Candidates, run the Arbiter to pick and order a set of
  Recommendations, then let the Dispatcher actuate the executable ones (subject
  to mode, cooldowns, and rate limits).

The split matters for a concrete failure mode: if the decision loop is wedged or
disabled (`mode: recommend-only`), the observation loop keeps the metrics and
events flowing, so an operator still *sees* fragmentation and health degrade even
when Alfred is choosing not to act. Observability is not coupled to actuation.

Alfred reads:
- Nodes (GPU capacity, allocation, conditions including `GpuUnhealthy`).
- Pods (labeled by OME conventions).
- InferenceServices.
- BaseModels / ClusterBaseModels.
- AcceleratorClasses.
- Alfred's own ConfigMap for policy configuration.

Alfred writes — and this list is exhaustive (see [the engine
table](#the-engine-snapshot--policies--arbiter--dispatcher)):
- Prometheus metrics (continuous).
- K8s Events on InferenceServices and Nodes (recommendations, migrations,
  evacuation signals, skip reasons).
- Optional `alfred-recommendations` ConfigMap in its own namespace.
- Migration-request annotations on InferenceServices (only in execute mode, only
  for OMENative-managed workloads).
- Pod eviction subresource (only for single-pod `RawDeployment` workloads).

Everything else is read-only.

### Architectural posture: observer, recommender, narrow actuator

Tl;dr: Alfred observes and recommends; it does not move pods itself. It hands
execution to whoever already owns the workload's lifecycle — because pod eviction
is destructive, and getting it wrong on a multi-pod group breaks the workload.
Alfred should never be the thing that does the breaking.

**Definition.** An *atomic multi-pod group* is a set of pods that must start,
stop, and migrate together (an OMENative Instance, or a legacy LWS group);
evicting one without the others leaves a half-broken workload. This is precisely
why eviction is unsafe for these groups and why their migration must be delegated.

**The posture, stated as three roles.**

1. **Observer.** Alfred reads cluster state into the snapshot. It mutates
   nothing while observing.
2. **Recommender.** Alfred reasons over the snapshot and emits Recommendations —
   metrics, events, and an optional ConfigMap. A recommendation is a *statement*,
   not an action; in `recommend-only` mode the caretaker never gets past this
   role.
3. **Narrow actuator.** When configured to act, Alfred executes a Recommendation
   only by *delegating*:
   - Multi-pod workloads → **OMENative**, the controller that owns their
     lifecycle, via the OEP-0007 migration-request annotation.
   - Single-pod `RawDeployment` workloads → the **K8s Eviction API**.

Why not let Alfred orchestrate lifecycle directly? Eviction races with other
cluster activity, and for an atomic group it is *unsafe*: evicting one pod out
from under the group corrupts the workload, and a plain eviction is not
surge-protected — there is no replacement pod waiting. Safe migration of a group
requires the controller that owns it. So that is where the work belongs.

**Failure modes are bounded — by the narrow surface, by design.**

- A bad *recommendation* (e.g. the scorer proposes a counterproductive move) is
  rejected or simply not executed by OMENative: nothing happens to the workload.
- A bad *eviction* (e.g. evicting a single-pod workload that turns out to be
  busy) triggers a normal K8s pod restart: the existing recovery path handles it,
  the same way it handles any node drain or preemption.

Note the boundary condition explicitly: **this bound holds *because* Alfred never
orchestrates pod lifecycle directly.** The worst Alfred can do through its write
contracts is request a migration (which a healthy OMENative can refuse) or evict
a single pod (which K8s already knows how to recover). The day Alfred starts
driving pod lifecycle itself — staging surge pods, deleting group members in
sequence — the guarantee is gone and the blast radius becomes unbounded. The
narrow surface is not an accident of the current implementation; it is the load-
bearing safety property of the whole design.

### The engine: snapshot → policies → arbiter → dispatcher

Tl;dr: One read-only snapshot feeds many independent policies; one arbiter is the
*only* component that reasons across policies; one dispatcher is the *only*
component that writes. Policies never actuate.

The engine is a four-stage pipeline, and the separation between stages is the
design, not an implementation detail:

```
ClusterSnapshot ──▶ [ Policy₁ Defragmentation ]──┐
   (read-only)      [ Policy₂ Node-Health Evac ]──┼──▶ Arbiter ──▶ Dispatcher ──▶ narrow
                    [ Policy₃ Descheduling (fut)]──┘  (select+    (the only        write
                                                       sequence)   writer)        contracts
```

**The Policy interface.** Every policy implements exactly one method:

```go
// A Policy reads the shared snapshot and proposes work. It never writes.
type Policy interface {
    Evaluate(snapshot *ClusterSnapshot) []Candidate
}
```

The contract is deliberately tiny, and the consequence is the point:

- A policy is a **pure function of the snapshot**: given a snapshot, it returns
  Candidates and mutates nothing. That makes each policy **independently
  testable** against a synthetic snapshot fixture — no fake clients, no informer
  plumbing, no apiserver. The defragmentation scorer can be exercised on an
  adversarial cluster state in a table-driven unit test, and the node-health
  policy can be handed a snapshot with a `GpuUnhealthy` node and asserted to
  return exactly the evacuation Candidates expected.
- A policy **cannot actuate**, because the interface gives it no client and no
  write path. Even a buggy or malicious policy can, at worst, propose bad
  Candidates — which the Arbiter and Dispatcher are positioned to filter. This is
  the same safety lever as the narrow posture above, pushed one layer inward.
- Adding a policy is additive: register a new `Evaluate` implementation. The
  Arbiter and Dispatcher are unchanged. **Descheduling (Policy #3)** is named
  here precisely to show the seam — it is future work, not in this OEP's scope,
  and it slots in as one more `Evaluate` with zero change to the actuation path.

**The three policies.**

1. **Defragmentation (Policy #1).** Detects GPU fragmentation (free GPUs
   scattered across nodes so that a large contiguous request cannot schedule) and
   proposes consolidating migrations. This is the original OEP-0008 workload.
2. **Node-Health Evacuation (Policy #2).** Detects a node that has gone
   `GpuUnhealthy` (node condition) and proposes evacuating the OME workloads off
   it, plus *signalling* that the node needs repair (an Event on the node;
   optionally a label). It **evacuates and signals — it does not repair.** The
   actual hardware repair (cordon-and-reset via a cloud Compute API) is left to
   whatever node-lifecycle controller the operator already runs. This is the key
   reframing of the old `ome-operator` auto-repair: the evacuation reuses the
   exact same delegated migration/eviction path as defragmentation, so Policy #2
   needs **no new node-write RBAC and no cloud credentials.**
3. **Descheduling (Policy #3, future).** Reserved seam for policy-driven eviction
   of workloads that violate placement rules over time. Out of scope here; named
   only to validate the interface.

**The Arbiter** is the *only* cross-policy reasoner. Each policy is myopic by
design — it sees the whole snapshot but only its own concern. The Arbiter takes
the union of all Candidates and resolves them globally: it deduplicates moves
that two policies both proposed, orders by priority (a `GpuUnhealthy` evacuation
should usually preempt a routine defrag consolidation), enforces cluster-wide
caps and per-node/per-workload cooldowns, and runs the regression check
(simulate the post-move snapshot, reject moves that make fragmentation worse).
Its output is an ordered list of **Recommendations**. Centralizing cross-policy
reasoning in one place means thrashing prevention and rate limiting are written
once, not re-implemented per policy.

**The Dispatcher** is the *only* component that writes to the wider cluster. It
takes the Arbiter's ordered Recommendations and actuates the executable ones
through the narrow write contracts — by workload type, exactly as the posture
section describes. Concentrating all writes in one component is what makes the
write-contract table below an *exhaustive* and *auditable* surface rather than a
hope.

**The narrow write contracts** (everything the Dispatcher — and therefore Alfred
— writes to the wider cluster):

| Target | Resource | Write | Why | New RBAC for Policy #2? |
|--------|----------|-------|-----|-------------------------|
| Alfred namespace | ConfigMap (`alfred-recommendations`) | Full read/write | Alfred's own output | No |
| Alfred namespace | Lease | Full read/write | Leader election | No |
| Any namespace | Events (on ISVC **and** Node) | `create`, `patch` | Surface recommendations, migrations, evacuation + repair signals | No — `events` already granted |
| Any namespace | Pod eviction subresource | `create` | RawDeployment single-pod migration **and** single-pod evacuation | No — same verb both policies |
| Any namespace | InferenceService annotations | `patch` (narrow to `ome.io/migration-request-v1-*`) | OEP-0007 migration verb, for defrag **and** multi-pod evacuation | No — same annotation both policies |

The load-bearing property: **Policy #2 (Node-Health Evacuation) introduces no new
write contract.** It evacuates by reusing the migration annotation (multi-pod)
and the eviction subresource (single-pod) the Dispatcher already holds, and it
signals repair by writing an Event on the node — a verb Alfred already has. It
does **not** cordon nodes via the node-write API, and it does **not** call any
cloud Compute API. Contrast the old `ome-operator` auto-repair, which cordoned
nodes and triggered OCI Compute resets and therefore needed both node-write RBAC
and cloud credentials. Folding node-health into the caretaker as a policy keeps
the entire surface read-only-plus-five-narrow-writes — the same surface OEP-0008
already justified for defragmentation alone.

### Core concepts

Each term defined before use, with the consequence baked in.

- **ClusterSnapshot.** An immutable, in-memory model of cluster state at one
  instant — nodes (GPU capacity, allocation, largest-contiguous-free, health,
  cordon/scale-down/preemptible flags, occupants), workloads (broken into
  Components and Instances), model availability, and pending pods. *Consequence:*
  it is built once per loop and shared read-only across all policies, so every
  policy reasons over the **same** view — two policies can never disagree about
  what the cluster looked like, and a policy can never mutate state another policy
  is reading.
- **Policy.** A pluggable decision module implementing `Evaluate(snapshot) →
  []Candidate`. *Consequence:* a policy is a pure, side-effect-free function of
  the snapshot — independently unit-testable and structurally incapable of
  actuating (it has no client).
- **Candidate.** A single proposed action — e.g. "migrate Component X's Instance
  Y off Node Z." Each Candidate carries a **benefit** (expected improvement: GPUs
  freed, fragmentation reduced, or a node drained for repair) and a **cost**
  (disruption risk: pods restarted, model re-pull, in-flight requests). *Consequence:*
  benefit and cost are explicit and comparable, so the Arbiter can rank across
  policies on a common axis instead of trusting each policy's self-assessment.
- **Recommendation.** A Candidate that has survived the Arbiter — passed
  opt-in/eligibility, cooldown, rate-limit, capacity, and regression checks — and
  is ready to emit or dispatch. *Consequence:* a Recommendation is the only thing
  the Dispatcher will act on; an un-arbitrated Candidate never reaches a write.
- **Arbiter.** The single cross-policy reasoner: it takes the union of all
  policies' Candidates and selects + sequences them into ordered Recommendations,
  resolving conflicts, deduplicating, prioritizing (health evac usually over
  routine defrag), and enforcing global caps and cooldowns. *Consequence:* there
  is exactly **one** place where cross-policy tradeoffs and rate limits live, so
  thrashing prevention is not re-derived per policy.
- **Dispatcher.** The single actuator: it executes Recommendations through the
  narrow write contracts, choosing migration-annotation (multi-pod via OMENative)
  or eviction (single-pod via K8s) by workload type. *Consequence:* all writes
  funnel through one component, making the write surface exhaustively auditable.
- **Fragmentation Score.** A cluster-level number in [0, 1] summarizing GPU
  fragmentation — 0 = perfectly packed, 1 = maximally fragmented — composed of
  weighted signals (mechanism in Design Details). *Consequence:* it is the
  scalar the Defragmentation policy thresholds on and the metric an operator
  watches; it is a *signal*, not an action, so a rising score in
  `recommend-only` mode produces visibility without disruption.
- **Workload.** The unit Alfred reasons about: typically one InferenceService,
  broken down into its **Components** and **Instances**. Alfred reasons at the
  Component/Instance level, not the bare-pod level. *Consequence:* a multi-pod
  Instance is treated as one atomic unit to be moved together, which is exactly
  why its migration is delegated to OMENative rather than evicted pod-by-pod.
- **Atomic multi-pod group.** A set of pods that must start, stop, and migrate
  together (an OMENative Instance, or a legacy LWS group). *Consequence:*
  evicting one member without the others leaves a half-broken workload — so for
  these groups, migration must go through the lifecycle owner (OMENative), never
  through Alfred-driven eviction.

### User stories

#### Story 1: Operator observes fragmentation, then opts into action

An operator deploys OME with Alfred enabled and `mode: recommend-only`. Over a
week, workloads come and go; the `alfred_cluster_fragmentation_score` gauge rises
from 0.2 to 0.6. The operator reviews the Defragmentation policy's
Recommendations in `kubectl get events` and the `alfred-recommendations`
ConfigMap, gains confidence, and switches to `mode: execute`. Alfred's Dispatcher
begins writing OEP-0007 migration-request annotations to OMENative-managed
workloads. Fragmentation drops over the next hour. The observe-before-act split
is deliberate: the operator saw the recommendations the engine *would* have
executed before granting it permission to execute them.

#### Story 2: Alfred unblocks a pending large workload

A new InferenceService for Llama4 needs 8 GPUs on a single node. No node has 8
contiguous free GPUs despite 70 GPUs free cluster-wide. The Defragmentation
policy sees the Pending pod in the snapshot and proposes Candidates whose
migration would free 8 contiguous GPUs on one node. The Arbiter ranks them by
benefit (capacity unblocked) against cost (small workloads disrupted) and emits
Recommendations; the Dispatcher migrates the small OMENative workloads via the
migration verb, consolidating Node1. Llama4 schedules on Node3 within minutes.

#### Story 3: A node goes GpuUnhealthy — Alfred evacuates and signals for repair

Node7's node condition flips to `GpuUnhealthy` (a GPU has failed). The
Node-Health Evacuation policy sees the condition in the snapshot and returns
Candidates to evacuate every OME workload off Node7 — high benefit (workloads are
running on degraded hardware), and the cost is bounded because the moves go
through the same delegated path as defrag. The Arbiter prioritizes these over any
routine defrag Candidates (a sick node should empty first) and emits ordered
Recommendations. The Dispatcher evacuates: multi-pod Instances via the OEP-0007
migration annotation, any single-pod RawDeployment workloads via the Eviction
API. Once the node is drained of OME workloads, Alfred *signals* — it writes an
Event on Node7 (and optionally a label) saying the node needs repair — and stops
there. **Alfred does not cordon the node and does not call any cloud Compute API
to reset it.** Repair is delegated to whatever node-lifecycle controller the
operator already runs. The whole story uses only the existing narrow write
contracts: migration annotation, eviction subresource, node Event. No new RBAC,
no cloud credentials.

#### Story 4: Maintenance-window honor

An operator sets a weekday business-hours maintenance window. During the window,
high fragmentation produces Recommendations (metrics + events) but the Arbiter
withholds them from the Dispatcher — Alfred observes and recommends, but does not
act. After the window, dispatch resumes under the standard rate limit. An
`emergencyPendingAgeMinutes` override lets the Arbiter release a Recommendation
during the window if a Pending pod has waited past the threshold.

#### Story 5: Opt-out for a critical workload

A business-critical InferenceService is annotated `alfred.ome.io/movable:
"false"`. Every policy's `Evaluate` excludes it from Candidate generation
entirely (the eligibility filter reads the annotation off the snapshot). An Event
on the ISVC explains the exclusion — useful when an operator wonders why no
Recommendation was produced despite obvious fragmentation.

#### Story 6: LWS legacy workload — recommendation only

An InferenceService using `deploymentMode: MultiNode` (LWS-backed) is highly
fragmented. A policy produces a Candidate, but the Dispatcher will **not**
execute it, because Alfred has no safe delegated path for an LWS group (LWS is
not OMENative; there is no migration verb to delegate to). The event explains:

> LWS-backed workload cannot be migrated safely by alfred. Migrate the workload
> to OMENative strategy for automatic defragmentation, or handle manually.

The operator migrates the workload to OMENative (per OEP-0007 §Strategy Migration
Procedure) or handles the defragmentation manually. This is the posture in
action: where Alfred has no safe delegate, it degrades to recommend-only rather
than reaching for an unsafe direct action.

#### Story 7: Coexistence with cluster-autoscaler

Cluster-autoscaler (CA) is enabled and scales down underutilized nodes. The
Defragmentation policy's consolidation feeds this: as Alfred packs workloads onto
fewer nodes, CA notices the emptied nodes and removes them. The policy reads each
node's `cluster-autoscaler.kubernetes.io/scale-down-disabled` annotation off the
snapshot and excludes such nodes from placement hints, so the two controllers do
not fight over the same node.

### Risks and mitigations

Each risk in failure-mode → mitigation form. "Mitigation, not full fix" is noted
where true.

**Risk: a policy proposes a counterproductive move (scorer bug).** A
Defragmentation Candidate that, if executed, would *increase* fragmentation.
*Mitigation:* the Arbiter runs a regression check — simulate the post-move
snapshot and reject any Recommendation that worsens the Fragmentation Score.
Because policies are pure functions of the snapshot, the same scorer is exercised
in table-driven unit tests over adversarial synthetic snapshots before it ever
ships. `recommend-only` is the default for early deployments, so a bad scorer
produces visible-but-inert Recommendations, not disruption.

**Risk: thrashing.** Alfred migrates workload A, creating fragmentation
elsewhere, triggering a migration of workload B that undoes A.
*Mitigation:* the Arbiter owns this — per-workload cooldown (no migration within
30 minutes of the last), per-node cooldown (10 minutes after a migration touches
the node), and trend-based gating (act only when a signal is consistently above
threshold, not on a spike). Centralizing cooldowns in the Arbiter means a new
policy inherits anti-thrash for free; it cannot forget to implement it.

**Risk: two policies propose conflicting actions on the same workload.** The
Defragmentation policy wants to keep a workload where it packs best; the
Node-Health policy wants it evacuated off a sick node.
*Mitigation:* the Arbiter is the *only* cross-policy reasoner and resolves the
conflict by priority — health evacuation usually preempts routine defrag — and
deduplicates moves both policies proposed. No policy sees another policy's
Candidates, so conflicts cannot be resolved (or mishandled) inside a policy.

**Risk: migration storm.** Many Candidates approved at once; OMENative is
overwhelmed.
*Mitigation:* the Arbiter enforces a cluster-wide cap (default 3 in-flight, 10
per hour). The Dispatcher observes OMENative `RateLimited` rejections and backs
off. A circuit breaker trips if the failure rate exceeds 50% of the recent 10
dispatched actions — pause for 1 hour, emit a critical event.

**Risk: broken GPU discovered only after migration.** Alfred migrates onto Node5,
which has a broken NVLink; NCCL fails.
*Mitigation:* the snapshot carries node health (including `GpuUnhealthy`), so
unhealthy nodes are excluded from placement hints during Candidate generation.
This is also exactly why **Node-Health Evacuation is a first-class policy** rather
than an afterthought — the same health signal that excludes a node as a *target*
also makes it a *source* to evacuate. Post-migration, if a migrated workload fails
within a window, Alfred marks the node suspect and backs off from it. Mitigation,
not full fix: a fault that is invisible to the node condition can still bite.

**Risk: conflict with HPA / scheduler / other controllers.**
*Mitigation:* the Defragmentation policy checks for active HPA scaling on the
target ISVC in the snapshot and the Arbiter defers while it is in progress.
Alfred respects `scale-down-disabled` on nodes. OMENative enforces a per-Instance
migration-in-progress lock (OEP-0007 Q-004), so concurrent migration requests are
serialized on the workload-owner side regardless of what Alfred sends.

**Risk: migration-request annotation write lost or delayed.**
*Mitigation:* the OEP-0007 annotations are UUID-keyed and idempotent (Q-021). If
OMENative does not clear within 5 minutes, the Dispatcher retries with the same
UUID. If still unacknowledged at 1 hour, OMENative clears it as stale and emits
`MigrationRequestStale`; Alfred restarts from observation on the next loop.

**Risk: Alfred / OMENative version skew on the migration wire contract.**
*Mitigation:* the Dispatcher writes only the OEP-0007 v1 request shape
(`ome.io/migration-request-v1-*`, `schemaVersion: "v1"`). OMENative rejects
unknown `schemaVersion` values explicitly; additive unknown fields within a
version are ignored, behavior-changing fields require a new version. On
`UnsupportedSchemaVersion`, Alfred degrades that workload to recommend-only and
emits an event.

**Risk: Alfred is a cluster-wide SPOF.**
*Mitigation:* leader-elected with 3 replicas; a crashed leader fails over in
~15s. Alfred is **not** on the critical path — workloads run fine if Alfred is
down for hours, because Alfred only *optimizes*, it does not *serve*. The
caretaker going dark degrades GPU efficiency over time; it never takes a workload
offline.

**Risk: snapshot staleness produces a bad decision.** The snapshot is a point-in-
time view; the cluster can change between snapshot and dispatch.
*Mitigation:* the snapshot is rebuilt every observation-loop period (default
30s), bounding staleness to the loop frequency, and the Dispatcher does a
pre-flight re-check against the latest snapshot before each actuation. Mitigation,
not full fix: a change inside the pre-flight window is still possible — caught
downstream by OMENative's own lock and by the bounded blast radius of the narrow
posture.

**Risk: a third-party or future policy misbehaves.** Policy #3 (Descheduling), or
any later addition, has a bug.
*Mitigation:* the Policy interface gives a policy **no** write path — the worst it
can do is emit bad Candidates, which the Arbiter filters and which, even if
dispatched, hit only the bounded narrow-write surface. The structural guarantee
of the engine is that *blast radius is independent of how many policies exist.*

## Design details

The engine — shared `ClusterSnapshot` → `Policies` → `Arbiter` → `Dispatcher` — is fixed; what varies is the set of policies plugged into it. This section specifies each policy as a self-contained implementation of the `Policy` interface, then the cross-cutting concerns (safety, RBAC, tests) that the engine enforces around all of them.

### Policy #1: Defragmentation

Tl;dr: Policy #1 implements `Evaluate(snapshot) → []Candidate`. It reads the shared `ClusterSnapshot`, scores how badly the cluster's GPUs are fragmented, and — only when that score crosses a threshold — returns a ranked slice of candidate migrations to the Arbiter. It does not move pods. Execution belongs to the shared Dispatcher, which delegates to whoever owns the workload's lifecycle.

This is the first policy, and its scoring and selection logic is unchanged from the original defragmenter design: the only structural change is that it now returns `[]Candidate` to the Arbiter instead of dispatching directly. Keep that boundary in mind while reading — every "emit", "score", and "rank" below produces a `Candidate`, never a side effect.

**Definitions (used throughout this section).**

- *Candidate*: a proposed move of one Component Instance — "Component X's Instance Y off Node Z" — carrying a *benefit score* (expected fragmentation improvement), a *cost score* (disruption risk), an `Executable` flag, and a ranked list of `HintTargetNodes`. A Candidate is a value, not an action; the Arbiter decides whether any Candidate is dispatched.
- *Instance*: per the OEP-0007 hierarchy, a Component (router/engine/decoder) is a set of Instances; an Instance is the atomic unit Alfred reasons about and migrates. An OMENative Instance is multi-pod and must move together; a RawDeployment single-pod workload is one pod.
- *Movable*: a workload Alfred is permitted to migrate. Default true; set false by the `alfred.ome.io/movable: "false"` annotation. A non-movable workload is excluded from candidate enumeration entirely — Policy #1 never produces a Candidate for it.

#### Observation layer

Policy #1 is a pure function of the shared `ClusterSnapshot`: it reads, it does not watch. The engine owns the informer wiring and refresh cadence; the policy only consumes the snapshot the engine hands it on each evaluation tick. Defining the read surface up front matters because every score below is computed against exactly these fields, and a field the snapshot does not carry is a signal the policy cannot use.

The snapshot the policy consumes:

```go
type ClusterSnapshot struct {
    Timestamp          time.Time
    Nodes              map[string]*NodeState
    Workloads          map[types.NamespacedName]*WorkloadState
    Models             map[string]*ModelAvailability
    PendingPods        []*PendingPodInfo
    OMENativeAvailable bool                      // API discovery result
}

type NodeState struct {
    Name                  string
    AcceleratorClass      string
    TotalGPUs             int
    AllocatedGPUs         int
    FreeGPUs              int
    LargestContiguousFree int                    // topology-aware
    Unhealthy             bool
    Cordoned              bool
    ScaleDownDisabled     bool                   // CA annotation observed
    Preemptible           bool                   // spot / preemptible label observed
    OMEManagedPods        []OMEPodInfo
    OtherOccupants        []OtherPodInfo         // non-OME GPU workloads
}

type WorkloadState struct {
    ISVC            *omev1beta1.InferenceService
    Components      map[string]*ComponentState   // router, engine, decoder
    Movable         bool                         // from annotation
    Priority        float64                      // from annotation or default
    LastMigration   time.Time                    // per-workload cooldown
    ActiveMigration *MigrationInFlight
}

type ComponentState struct {
    DeploymentMode string                        // OMENative, RawDeployment, MultiNode (LWS), etc.
    Instances      []*InstanceState              // per OEP-0007 hierarchy
}

type InstanceState struct {
    InstanceIndex int32
    NodesSet      map[string]int                 // node -> pod count
    TotalGPUs     int32
    PodCount      int32
    ReadyPods     int32
    DesiredPods   int32
}

type PendingPodInfo struct {
    Namespace    string
    Name         string
    ISVC         types.NamespacedName
    GPUsNeeded   int32
    PendingSince time.Time
}

type ModelAvailability struct {
    Name        string
    NodesReady  []string                         // from BaseModel.Status.NodesReady
    NodesFailed []string
}
```

Two properties of the read surface are load-bearing for later sections:

- **Non-OME GPU workloads count against capacity but are never candidates.** Kubeflow Notebook pods, generic Jobs, and any other non-OME GPU consumer appear in `NodeState.OtherOccupants` and are folded into `AllocatedGPUs` / `FreeGPUs` so the fragmentation score reflects reality. But Policy #1 enumerates candidates only from `WorkloadState` (i.e. OME InferenceServices). *Failure mode if we got this wrong:* if non-OME pods were invisible to scoring, Alfred would underestimate fragmentation and sit quiescent on a genuinely packed cluster; if they were eligible for migration, Alfred would try to evict a notebook it does not own. The snapshot threads the needle by counting them for scoring and excluding them from selection.
- **`OMENativeAvailable` is an API-discovery result, not an assumption.** The execution path for multi-pod migration depends on the OMENative migration verb existing on the cluster. The snapshot records whether it does, so the policy can downgrade multi-pod candidates to non-executable rather than emit a Candidate the Dispatcher cannot honor.

#### Fragmentation scoring

The score answers one question: how much would the cluster benefit from defragmentation right now? It is a cluster-level number in `[0, 1]` — 0 = perfectly packed, 1 = maximally fragmented — and it gates the entire policy. Below the threshold, `Evaluate` returns an empty slice and Alfred stays quiescent; above it, the policy proceeds to candidate selection. We compute it from four signals because no single signal captures the failure we care about: a cluster can have plenty of free GPUs (good bin-packing) and still be unable to schedule a Llama4-Maverick that needs 8 contiguous GPUs on one node (bad contiguity). The signals below are deliberately complementary.

**Signal 1: Node contiguity.** For each node with GPUs, the largest contiguous free GPU block as a fraction of total. This is the signal that catches the "70 free GPUs, none of them schedulable as a block" trap.

```
NodeContiguityScore(n) = LargestContiguousFree(n) / TotalGPUs(n)
ClusterContiguityScore = mean(NodeContiguityScore(n) for n with GPUs)
```

Higher is better — more room for a large workload to land whole.

**Signal 2: Bin-packing efficiency.** For each non-empty node, how full it is. Rewards a cluster whose nodes trend toward "either empty or full" rather than every node half-occupied.

```
NodeBinPackScore(n)  = AllocatedGPUs(n) / TotalGPUs(n)
ClusterBinPackScore  = mean(NodeBinPackScore(n) for n with AllocatedGPUs(n) > 0)
```

Higher is better.

**Signal 3: Schedulability.** For a set of reference workload sizes `[1, 2, 4, 8, 16, 32]` GPU, what fraction of GPU nodes could schedule each size right now. This converts the abstract "fragmentation" into the concrete "can the cluster still place the workloads we actually run."

```
SchedulabilityScore(size) = NumberOfNodesWithAtLeast(size, free) / TotalNodesWithGPUs
ClusterSchedulabilityScore = weighted_sum(SchedulabilityScore(s) * w(s))
```

The per-size weights `w(s)` default to favoring larger sizes — small-workload schedulability is usually fine, and it is the 8/16/32-GPU placements that fragmentation actually blocks. (The exact default per-size weight vector is a tunable; see TBD below.)

**Signal 4: Pending-workload penalty.** A penalty proportional to the GPU demand and age of every pending pod. This forces urgency into the loop: a workload that has been Pending for an hour should drag the score up even if the static packing looks tolerable.

```
PendingPenalty = sum(GPUsNeeded(p) * age(p)) / TotalClusterGPUs
                 for p in PendingPods
```

Older and larger pending pods contribute more.

**Combined score.** The three "higher is better" signals are subtracted from 1 (so that good packing lowers the fragmentation score), and the penalty is added:

```
FragmentationScore =
      1 - ( w1 * ClusterContiguityScore
          + w2 * ClusterBinPackScore
          + w3 * ClusterSchedulabilityScore )
        + w4 * PendingPenalty

clamped to [0, 1]
```

Default weights: `w1=0.3, w2=0.2, w3=0.3, w4=0.2`. All four are tunable via policy. The score is published as the gauge `alfred_cluster_fragmentation_score`, with per-node detail in `alfred_node_fragmentation_score{node}`, so operators can watch it climb before deciding to switch from `recommend-only` to `execute`.

**Threshold gate.** If `FragmentationScore > policy.fragmentationThreshold` (default `0.5`), candidate selection proceeds. At or below the threshold, `Evaluate` returns an empty `[]Candidate` and Alfred is quiescent. *Rationale for a threshold rather than always-on:* migration is disruptive; we do not want Alfred churning pods to shave a fragmentation score from 0.2 to 0.15 when nothing is actually blocked.

#### Candidate selection

When the score is above threshold, the policy turns the snapshot into a ranked `[]Candidate`. The steps below run on each evaluation tick:

1. **Enumerate.** For every movable workload (`Movable=true`, not in per-workload cooldown via `LastMigration`, no `ActiveMigration` in flight), enumerate each Component Instance as one prospective candidate.
2. **Classify by deployment mode** — this sets the `Executable` flag, it does not drop the candidate:
   - **OMENative**: executable. Migration via the OMENative migration verb.
   - **RawDeployment single-pod**: executable. Migration via the K8s Eviction API.
   - **LWS-backed multi-pod**: `Executable=false`, but *still emitted* as a Candidate so the recommendation surfaces to operators. *Why include something we won't execute:* LWS's `RecreateGroupOnPodRestart` tears down the whole group on eviction with no surge protection — migrating it automatically is unsafe — but an operator still needs to see that this workload is a defrag opportunity and that the safe fix is to move it to OMENative.
   - **Knative**: not managed by Alfred; not enumerated.
3. **Simulate.** For each candidate, predict the post-migration cluster state: virtually free the source node's GPUs and virtually place the Instance on the best-available target node, then recompute the fragmentation score on that hypothetical snapshot. This gives `FragmentationScore_after`.
4. **Score** each candidate with a benefit-minus-cost rule:

   ```
   BenefitScore = FragmentationScore_before - FragmentationScore_after   // positive = improvement
   CostScore    = f(disruption_risk, pods_impacted, migration_mode)
   FinalScore   = BenefitScore - CostWeight * CostScore
   ```

   Cost is keyed off how disruptive the migration mode is:
   - **RawDeployment single-pod**: very low — one pod, brief disruption.
   - **OMENative rolling** (multi-replica, Service preserved): low.
   - **OMENative surge** (single-replica, needs ~2× capacity briefly): medium.
   - **LWS-backed**: effectively infinite — not executable, so it can never out-rank an executable candidate and is only ever surfaced as advisory.
5. **Boost emergencies.** If a candidate's migration would unblock a pod that has been Pending longer than `emergencyPendingAgeMinutes`, multiply its `FinalScore` by a boost factor. This is what lets Story 2 — a Llama4 stuck Pending despite 70 free GPUs — jump the queue ahead of routine consolidation.
6. **Rank** by `FinalScore` descending.
7. **Apply policy filters** before returning: drop cooldown violations, maintenance-window exclusions, rate-limited excess, and tenant-boundary violations (see the multi-tenancy section). These filters belong in the policy because they shape the candidate set; the Arbiter applies its own cross-policy arbitration on top.
8. **Return** the surviving ranked slice as `[]Candidate` to the Arbiter. As a side effect for observability, the policy emits a `FragmentationRecommendationProduced` K8s Event on each target InferenceService, increments `alfred_recommendations_produced_total`, and (if enabled) writes an entry to the `alfred-recommendations` ConfigMap. Note: emitting the event and metric is observability, not execution — whether a returned Candidate is acted on is the Arbiter's call, and dispatch is the Dispatcher's.

#### Placement-hint computation

Each Candidate carries `HintTargetNodes`: a ranked, *advisory* list of nodes the migration should prefer. Advisory is the key word — Alfred computes a good target, but the K8s scheduler makes the final placement, and Alfred re-evaluates from the observed outcome rather than insisting. Computing a hint:

1. **Enumerate** nodes that can physically accommodate the Instance's footprint (GPU count and `AcceleratorClass`).
2. **Filter** out nodes that would make the migration pointless or unsafe:
   - **Model not available**: the target must have the model ready, per `BaseModel.Status.NodesReady` or the node label `models.ome.io/{ns}.basemodel.{name}=Ready` (OEP-0007 Q-017). Migrating to a node that must first pull a multi-hundred-GB model defeats the purpose.
   - **Unhealthy or cordoned**: excluded. Excluding unhealthy nodes from placement is existing defragmentation behavior — and it is the seam Policy #2 builds on: a node Policy #1 already refuses as a *target* is exactly the kind of node Policy #2 will reason about as a *source* to drain. (Mechanism for Policy #2 in its own section.)
   - **CA scale-down in progress**: a node with `scale-down-disabled` being processed is excluded, so Alfred and the cluster-autoscaler do not fight over it.
   - **Spot / preemptible**: excluded from targets by default (see the spot/preemptible section) — moving a workload onto a node that can vanish is a poor trade.
3. **Rank** by a bin-packing heuristic whose direction depends on the goal: prefer partially-filled nodes when the goal is to consolidate, or prefer empty nodes when the goal is to free contiguous capacity elsewhere.
4. **Return** the top N (default `3`).

#### Execution

Policy #1 does not execute. It returns `[]Candidate`; the Arbiter selects which Candidate(s) to act on across all policies; the shared **Dispatcher** carries out the move. This separation is the safety boundary of the whole design: a bad *recommendation* is just a value the Arbiter can decline, and the policy never holds an eviction client. The Dispatcher delegates by workload type — the same mechanics the original defragmenter used, now factored out of the policy so every policy executes through one path:

**RawDeployment single-pod → K8s Eviction API.**

```go
eviction := &policyv1.Eviction{
    ObjectMeta: metav1.ObjectMeta{
        Name:      pod.Name,
        Namespace: pod.Namespace,
    },
    DeleteOptions: &metav1.DeleteOptions{
        GracePeriodSeconds: ptr.Int64(30),
    },
}
client.CoreV1().Pods(ns).EvictV1(ctx, eviction)
```

The Deployment controller recreates the pod and the scheduler places it wherever it fits. Disruption is brief and bounded to a single pod — which is exactly why eviction is acceptable here and unsafe for a multi-pod group.

**OMENative-managed → migration-request annotation (OEP-0007 Q-004).** The Dispatcher does not evict multi-pod groups; it asks the controller that owns the group's lifecycle to migrate it, by PATCHing a request annotation onto the ISVC:

```go
uuid := generateUUID()
payload := MigrationRequest{
    SchemaVersion:   "v1",
    Component:       "engine",
    Instance:        0,
    Reason:          "fragmentation",
    FromNode:        "node1",
    HintTargetNodes: []string{"node3", "node7"},
    RequestedAt:     time.Now(),
    RequestedBy:     "alfred-controller",
}
patch := fmt.Sprintf(`{
    "metadata": {
        "annotations": {
            "ome.io/migration-request-v1-%s": %q
        }
    }
}`, uuid, marshalJSON(payload))
client.Patch(ctx, isvc, types.MergePatchType, []byte(patch))
```

OMENative observes the annotation, validates it, accepts or rejects, and records the outcome in `Status.MigrationHistory`. Alfred watches that status to learn what happened — it does not assume success.

Retry semantics (OEP-0007 Q-021):
- If OMENative has not cleared the annotation within 5 minutes, the Dispatcher re-PATCHes the **same** UUID — the request is idempotent, so a retry is a no-op if the original is still being processed.
- If after 1 hour no clear has happened, OMENative auto-clears the request with `MigrationRequestStale`; Alfred abandons it and emits an event. *Failure mode this guards against:* a request that hangs forever would pin the workload in `ActiveMigration` and silently exclude it from future candidate sets.

**Legacy LWS-backed → no execution, advisory only.** The Dispatcher refuses to execute an LWS-backed Candidate (which arrives flagged `Executable=false`). It emits a recommendation event with a message directing the operator to migrate the workload to OMENative, and increments `alfred_lws_recommendations_total{isvc, action: manual}` so a dashboard can alert that manual defragmentation is needed. *Why refuse rather than try:* LWS's `RecreateGroupOnPodRestart` tears down the entire group on a single eviction with no surge protection — Alfred would cause the very outage it exists to prevent.

**Failure handling at dispatch:**
- **Eviction blocked** (PDB denies, API error): emit an event and retry on the next evaluation tick — the candidate will simply be re-derived from the next snapshot.
- **Migration rejected by OMENative** (`InsufficientCapacity`, `RateLimited`, `MigrationInProgress`): log the reason, put the candidate in cooldown, and let the Arbiter try the next-ranked Candidate.
- **Migration accepted but `Status.MigrationHistory` reports `Failed`**: increment the failure metric, emit an event, and put that workload in cooldown so Alfred does not immediately re-attempt a move that just failed.

### Policy #2: Node-health evacuation

Tl;dr: when a node's GPU goes bad, get the OME workloads off it and tell the
operator the node needs repair — but do not touch the node. Policy #2 is
"evacuate + signal," and that two-word actuation depth is the whole point: it
stays inside the exact write contracts Policy #1 already uses, so it needs no
new RBAC and no cloud credentials.

**Definition.** *Policy #1* is defragmentation (the bin-packing relocation of
the rest of this document): its trigger is a fragmentation score crossing a
threshold. *Policy #2* is node-health evacuation: same machinery, different
trigger. A *bad node condition* is a node-level signal the snapshot already
carries — `GpuUnhealthy` is the canonical one — that marks a node's
accelerators as unusable for OME workloads. *Evacuate + signal* means Alfred
produces evacuation Candidates for the workloads on that node and emits a
remediation signal for whoever owns the hardware; it does **not** repair,
replace, cordon, drain, terminate, or reboot the node.

#### Goal and context

The failure mode Policy #2 addresses: a node reports `GpuUnhealthy` (failed GPU,
bad NVLink, ECC errors past threshold) and the OME workloads pinned to it are
now running on — or scheduled onto — hardware that will fail their NCCL
collectives or crash their pods. Defrag already *avoids* such a node: unhealthy
nodes are excluded from placement hints, so Policy #1 never migrates a workload
*toward* one. But avoidance is not evacuation. Nothing in Policy #1 moves the
workloads that are *already* stranded on the bad node, and nothing tells the
operator the node is bad and needs repair. The pods sit there degrading until a
human notices.

In scope: detect the bad condition (already in the snapshot), emit evacuation
Candidates for the OME workloads on that node, and emit a remediation signal so
the node gets repaired or replaced. Out of scope: deciding *how* to repair the
hardware, and performing any node-level write. That belongs to the operator, the
cloud provider, or the cluster autoscaler — see the anticipated objection below.

#### What must be true

1. The trigger is a node condition already present in the snapshot. *Rationale:*
   Non-Goal #6 stands — Alfred adds no new GPU-health telemetry. Policy #2
   consumes `GpuUnhealthy` (and the equivalent `NodeState.Unhealthy` flag the
   observation layer already computes); it does not probe GPUs itself.
2. Evacuation uses the same execution surfaces as defrag, unchanged. An
   evacuation Candidate for an OMENative Instance goes out as the OEP-0007
   migration-request annotation; for a single-pod `RawDeployment` it goes out as
   a K8s Eviction; for an LWS-backed group it is recommend-only (Alfred never
   executes LWS). *Rationale:* the safety argument that makes eviction tolerable
   — multi-pod groups are migrated by their owning controller, never evicted out
   from under themselves — must hold identically for health-evacuation, because
   the destructiveness of the action does not depend on what triggered it.
3. The remediation signal is observable by the operator/cloud/cluster-autoscaler
   without Alfred holding any node-write permission. *Rationale:* this is the
   property that keeps Non-Goal #3 ("Alfred does not cordon, drain, or terminate
   nodes") literally true while still making the bad node *actionable*.

#### Approach (how)

On a bad condition for node N, Alfred does two things, both within its existing
contracts:

a) **Evacuate.** Produce evacuation Candidates for every OME workload occupying
   N, scored and dispatched through the identical Candidate pipeline Policy #1
   uses (eligibility check, simulation against the *healthy* remainder of the
   cluster, capacity check, dispatch via migration-annotation or Eviction).
   Health-evacuation Candidates carry a distinct reason (`NodeUnhealthy`, vs
   `Fragmentation`) so operators can tell defrag moves from forced evacuations in
   metrics and Events — but they flow through the same code path.

b) **Signal.** Emit a remediation signal in two places: a Kubernetes **Event on
   the Node object** (reason `GpuUnhealthyEvacuating`, type `Warning`), and an
   entry in Alfred's **own `alfred-recommendations` ConfigMap** keyed by node
   name with the condition, the evacuated workloads, and a timestamp. *Rationale:*
   the Event is what cluster-autoscaler / node-problem-detector / a cloud
   remediation controller already watch for; the ConfigMap entry is the durable
   record Alfred itself owns, surviving across decision loops so a flapping
   condition doesn't spam fresh signals.

The only genuinely new behavior versus Policy #1 is the **trigger** (a node
condition, not a fragmentation score) and the **remediation Event**. Everything
downstream — Candidate scoring, simulation, the global safety bounds, dispatch
— is reused verbatim. That reuse is deliberate: it is what bounds the blast
radius of adding a second policy to roughly zero new write surface.

#### The narrow-contracts property (no new RBAC, no cloud credentials)

State it plainly, because it is the load-bearing reason Policy #2 is cheap and
safe to add: **evacuate + signal stays entirely within the four write contracts
Alfred already holds.** Those four, and nothing more:

| Write surface | Used by | Node-level? | New for Policy #2? |
|---|---|---|---|
| OEP-0007 migration-request annotation on the ISVC | defrag + evacuation | no | no |
| K8s Eviction subresource (single-pod RawDeployment) | defrag + evacuation | no | no |
| Kubernetes Events | recommendations + remediation signal | Event *targets* a Node; it is **not** a write to the Node's spec/status | the Node-targeted Event is new; the verb (`create events`) is not |
| Alfred's own `alfred-recommendations` ConfigMap | recommendations + node remediation record | no | no |

There is **no** `patch nodes`, **no** `update nodes/status`, **no** cordon (which
is a node spec write), **no** drain, **no** cloud-API call, and therefore **no**
cloud credential anywhere in Policy #2. Emitting an Event whose `involvedObject`
is a Node requires only the `create events` permission Alfred already has for its
recommendations — it does not require write access to the Node itself.

Why this matters as a failure mode, not just a tidiness claim: the day Policy #2
acquires `patch nodes` or a cloud credential, Alfred becomes a thing that can
cordon the wrong node or terminate a healthy instance, and the bounded-blast-
radius guarantee from the architectural posture section evaporates. Keeping the
write set frozen at four contracts is what *makes* "Alfred does not manage node
lifecycle" an enforceable invariant rather than an aspiration — an auditor can
verify it from the RBAC manifest alone.

#### Anticipated objections

Q. The old ome-operator Alfred watched for `GpuUnhealthy`, cordoned the node,
drained it, and triggered a soft reset via the OCI Compute API. Why is the new
Policy #2 strictly weaker — why not just cordon and repair the node like before?

A. Because hardware remediation is cloud-specific and high-blast-radius, and OME
core is cloud-neutral. The old auto-repair path (`pkg/alfred/autorepair`) was
welded to the OCI Compute API: it only worked on OCI, it held credentials that
could reboot any instance, and a bug in it could take down a healthy node. We are
not reintroducing that into OME core. Cordon is itself a node spec write —
exactly the permission we are refusing to take. The split we are choosing: Alfred
makes the bad node *actionable* (workloads off, Event emitted, ConfigMap record
written) using only its narrow contracts, and the actual repair-or-replace is the
operator's / cloud's / cluster-autoscaler's job. A provider that *wants*
OCI-style soft-reset can add it as an **optional, pluggable remediation provider
(deferred to Phase 3)** that consumes Alfred's remediation signal — out-of-tree,
credentialed separately, with its own blast radius — never as a built-in that
forces every OME deployment to grant node-write and cloud creds.

Q. If Alfred only signals and never repairs, what closes the loop — couldn't a
bad node sit forever with workloads refusing to land on it?

A. The loop is closed by whoever already owns nodes, not by Alfred. The Node
Event and ConfigMap record are the handoff; cluster-autoscaler replacing the node
or an operator repairing it is the completion. If nobody acts, the node stays
excluded from placement hints (defrag avoidance) and the evacuation already moved
the live workloads off — so the cluster is *degraded by one node*, not broken.
That is the deliberate trade: Policy #2 guarantees the workloads are safe, not
that the hardware gets fixed. Fixing hardware is explicitly someone else's
contract.

### Policy #3: Descheduling (future)

Tl;dr: a reserved interface slot, not a design. Listed now only so the Policy
abstraction is shaped to hold more than two triggers.

*Policy #3* is generic descheduling: "this placement now violates a constraint it
satisfied when the pod was scheduled" — anti-affinity drift (two pods that should
be spread ended up co-located after unrelated churn), priority inversion (a
low-priority workload squatting on capacity a high-priority one now needs), or a
topology constraint that newer labels invalidated. The trigger is a *constraint
violation discovered after scheduling*, distinct from fragmentation (Policy #1)
and node health (Policy #2). The actuation depth would again be evacuate-style
relocation through the same Candidate pipeline — but the *detection* (which
constraints to evaluate, how to score a violation's severity, how to avoid
fighting the scheduler that placed the pod) is not designed here. Policy #3 is
**Phase 2** and is named only to confirm that the Policy interface — trigger →
Candidates → Arbiter → dispatch — generalizes cleanly past defrag, so we don't
have to reshape it later. Anything beyond this paragraph about Policy #3 is out
of scope for this OEP.

### The arbiter (arbitration-lite)

Tl;dr: with more than one policy producing Candidates, something has to decide
which Candidate wins when they conflict. The arbiter does that with four
deterministic rules and **no** prediction — it sequences actions, it does not
forecast them.

**Definition.** The *arbiter* is the stage between policy Candidate-production and
dispatch. *Arbitration-lite* means it resolves conflicts by fixed
priority/exclusion/safety rules that are fully determined by the current
snapshot — given the same snapshot it always makes the same choice — as opposed
to a predictive planner that models future cluster state. We are explicitly
choosing the lite version for v1.

#### Why an arbiter at all

The failure mode it prevents: two policies, run independently, issue
contradictory or unsafe-in-aggregate actions in the same cycle. Concretely —
Policy #1 (defrag) wants to migrate workload A *onto* node N to improve packing,
while Policy #2 (health) is simultaneously evacuating node N because it just went
`GpuUnhealthy`. Without arbitration, Alfred migrates A directly into a node it is
in the middle of evacuating: the worst possible move. Or: two policies each emit
a Candidate for the same workload, and the workload gets two concurrent
migration requests, violating the single-in-flight invariant. The arbiter exists
to make those impossible *before* dispatch, deterministically.

#### The four rules

a) **Priority — health preempts defrag.** A health-evacuation Candidate
   (Policy #2) always outranks a defrag Candidate (Policy #1) that touches the
   same workload or node. *And* the arbiter never lets defrag migrate a workload
   *toward* a node that any health Candidate is evacuating this cycle. *Rationale:*
   getting a workload off failing hardware is strictly more urgent than improving
   bin-packing, and migrating toward a node under evacuation is self-defeating.

b) **Mutual exclusion — at most one in-flight action per workload and per node
   per cycle.** If two Candidates target the same workload, only the
   higher-priority one survives arbitration; the other is dropped (it will be
   re-proposed next cycle if still valid). Likewise at most one action per node
   per cycle. *Rationale:* concurrent actions on one workload race the OEP-0007
   single-migration-in-flight lock and waste rate-limit budget; serializing per
   workload and per node keeps every downstream contract's "one at a time"
   assumption intact.

c) **Global safety bounds — the existing cooldown, rate-limit, and capacity-check
   apply across all policies, not per policy.** The cluster-wide caps
   (`maxInFlightMigrations`, `maxMigrationsPerHour`), the per-workload and
   per-node cooldowns, and the pre-flight capacity check are evaluated on the
   *combined* stream of surviving Candidates after priority and exclusion — never
   reset or duplicated per policy. *Rationale:* if each policy got its own budget,
   adding Policy #2 would silently double the migration rate the cluster
   tolerates, and the thrashing/migration-storm mitigations from the risks
   section would be defeated. The safety envelope is a property of Alfred as a
   whole, so it is enforced once, globally, at the arbiter.

d) **OEP-0013 awareness — defer to the autoscaler.** Before a Candidate clears
   the arbiter, Alfred reads the autoscaler's intent (read-only; see the next
   section) and skips any workload the autoscaler is *actively scaling*.
   *Rationale:* migrating a Component while OEP-0013 is adding or removing its
   replicas races two controllers over the same pods. *Failure-mode default:* if
   the autoscaler-intent signal is unavailable (CRD absent, read error, stale
   beyond the snapshot window), the arbiter treats the workload as **busy** and
   defers it. We choose the safe default — skip on uncertainty — because acting
   on a workload that might be mid-scale is the costlier error; a deferred
   Candidate simply reappears next cycle.

#### Why lite, and what a heavier version would add

Options for resolving multi-policy conflict:

- a) **Arbitration-lite (deterministic sequencing).** Tradeoff: it never plans
  ahead — it can pick a locally-correct action this cycle that a forecasting
  planner would skip because of where the cluster is heading.
- b) **Predictive planner (forecast post-action cluster state across policies,
  optimize globally).** Tradeoff: needs a cluster-state model and a forecasting
  loop, is far harder to test adversarially, and its failure modes (a confidently
  wrong forecast) are exactly the kind of high-uncertainty action Goal #6 tells us
  to reject.

We choose (a). v1 is sequencing only — priority, exclusion, global bounds,
autoscaler-defer — and that is enough to make multi-policy operation safe and
predictable. A **Phase 3 predictive planner** could sit on top, consuming the
same Candidate stream and adding forecasting before the arbiter's deterministic
rules; nothing in arbitration-lite forecloses it. But forecasting is not in v1,
and we will not pretend the lite arbiter does more than order actions.

### Coexisting with the autoscaler (OEP-0013)

Tl;dr: read-only, one direction. Alfred consults the autoscaler's intent before
acting and **never** writes into OEP-0013's surface. Two controllers, two jobs,
one boundary.

The division of labor, as a one-liner: **Alfred decides *where* pods live and
when to move or evacuate them; the autoscaler (OEP-0013) decides *how many*; the
scheduler decides *initial placement*; the operator/cloud repairs hardware.**
Each owns a different verb, and the verbs don't overlap.

The seam is deliberately asymmetric and minimal: Alfred *reads* OEP-0013's intent
(is this Component being actively scaled right now?) as the input to arbiter rule
(d), and that is the entire interaction. Alfred does not create, patch, or delete
ScaledObjects, HorizontalPodAutoscalers, or any OEP-0013-owned object; it does
not set replica counts; it does not write back any "I'm migrating this" flag into
the autoscaler's surface. *Rationale:* a read-only, one-directional seam has no
write-write race to reason about — the only failure mode is a stale read, and the
arbiter's safe default (treat unavailable/stale intent as "busy, defer") already
bounds that. The moment the seam became bidirectional — Alfred writing into the
autoscaler's state, or the autoscaler asking Alfred to move pods — we would own a
two-controller write contract with all the version-skew and ordering hazards that
implies. We are not building that. The autoscaler scales; Alfred places and
evacuates; they meet only at a single read.

See also: OEP-0013 (Component-Scoped Autoscaling) for the autoscaler-intent
surface Alfred reads. Open question (TBD): the exact shape of the
"actively-scaling" read — whether Alfred infers it from observed ScaledObject /
HPA status conditions and recent replica churn (no OEP-0013 cooperation needed,
works today) or OEP-0013 publishes an explicit intent marker (cleaner, requires
coordination). v1 uses the inference path because it needs nothing from OEP-0013;
an explicit marker is a Phase 2 refinement if the inference proves too noisy.

### Policy and configuration model

Tl;dr: one ConfigMap drives the whole caretaker; each Policy is enabled
independently; each workload can be gated independently; aggressiveness is a
scoring knob, not an on/off switch.

**Definitions.** A *Policy* is a pluggable producer of recommendations that
reads the shared `ClusterSnapshot` and emits ranked candidates — Policy #1 is
defragmentation, Policy #2 is node-health evacuation. A *recommendation* is a
proposed action (migrate this Instance, evacuate this node) carrying a
benefit/cost score and a target hint. Policies never act; they only propose.

Why a single ConfigMap rather than a CRD or per-Policy config objects: the
caretaker is an opt-in operational tool, and operators tune it the way they tune
cluster-autoscaler — edit one config, watch it reload, no API surface to
version. The cost: config is untyped YAML, so a schema-version field and a
last-known-good fallback do the validation work a CRD would get for free
(mechanism below).

Configured via the `alfred-config` ConfigMap. The schema extends the existing
`config.yaml` with a per-Policy block; the global scoring, rate-limiting, and
window keys stay where they were (they now feed the Arbiter — see
[Safety bounds](#safety-bounds)):

```yaml
config.yaml: |
  schemaVersion: 1

  # Overall mode
  mode: recommend-only    # recommend-only | execute

  # Loop cadence
  decisionLoopInterval: 5m
  observationLoopInterval: 30s

  # Per-policy enable + tuning
  policies:
    defragmentation:
      enabled: true
      fragmentationThreshold: 0.5
      aggressiveness: balanced     # conservative | balanced | aggressive
      scoring:
        contiguityWeight: 0.3
        binPackWeight: 0.2
        schedulabilityWeight: 0.3
        pendingPenaltyWeight: 0.2
        referenceWorkloadSizes: [1, 2, 4, 8, 16, 32]
    nodeHealth:
      enabled: true
      aggressiveness: balanced
      # which node conditions trigger evacuation; consumes existing signals,
      # does not detect health itself (see Non-Goals).
      triggerConditions: [AcceleratorUnhealthy, NodeNotReady]
      signalOnly: false            # true = signal the owning controller, never evict

  # Per-workload defaults
  defaultMovable: true

  # Execution surfaces
  rawDeploymentEvictionEnabled: true
  omenativeMigrationEnabled: true
  lwsRecommendationsEnabled: true    # produce recommendations for LWS (never execute)

  # Output
  recommendationsConfigMapEnabled: true
  recommendationsConfigMapName: alfred-recommendations

  # Logging
  logLevel: info
  structuredLogging: true
```

The global safety, spot, multi-tenancy, and maintenance-window keys are
unchanged and documented in their own sections below; they are deliberately
*not* nested under `policies` because they are applied once, by the Arbiter,
across every Policy's output.

**Aggressiveness is a scoring knob, not a kill switch.** `conservative` raises
the benefit/cost threshold a candidate must clear before the Arbiter will admit
it; `aggressive` lowers it. It does not bypass cooldowns, rate limits, or the
capacity check — those are global and non-negotiable (see
[Safety bounds](#safety-bounds)). Naming the failure mode: if aggressiveness
*could* relax safety bounds, an operator chasing utilization would set
`aggressive` and silently disable the very gates that keep the caretaker from
churning the cluster. So it can't.

**Per-Policy enable + per-workload gating are orthogonal axes.** Disabling
`policies.nodeHealth` stops the caretaker from proposing evacuations for anyone;
annotating one workload `alfred.ome.io/movable: "false"` opts that workload out
of *all* Policies. An operator needs both: cluster-wide "don't run node-health
yet" and per-workload "never touch this one."

Per-workload overrides via InferenceService annotations:

```yaml
metadata:
  annotations:
    alfred.ome.io/movable: "false"                       # opt out of all policies
    alfred.ome.io/priority: "0.3"                        # lower = more protected
    alfred.ome.io/cooldown-minutes: "60"                 # per-workload override
    alfred.ome.io/opt-out-reason: "critical production"  # operator note
    alfred.ome.io/tenant-group: "team-alpha"             # for cross-tenant opt-in
```

Annotations win over ConfigMap defaults: the per-workload signal is closer to
the workload owner than the cluster-wide default, so it takes precedence.

**Reload semantics.** Alfred watches the ConfigMap and reloads on change. An
invalid `schemaVersion`, or a config that fails schema validation, triggers
fallback to last-known-good with an event on the ConfigMap and
`alfred_policy_reload_total{outcome="failure"}`. Failure mode being mitigated: a
fat-fingered edit must never leave the caretaker running with a half-parsed
policy — better to keep serving the previous known-good config and shout about
it than to act on garbage.

#### Opt-in / opt-out semantics

Default: opt-in per `defaultMovable: true`. A workload is eligible for *any*
Policy unless one of these holds:

- Annotation `alfred.ome.io/movable: "false"`.
- The workload is in cooldown (per-workload or per-node).
- The workload is in active migration.
- Its strategy is unsupported for execution — LWS is recommendation-only,
  Serverless is not managed.

Operators preferring opt-in-only set `defaultMovable: false`; then a workload
needs `alfred.ome.io/movable: "true"` explicitly. Rationale: a conservative
cluster wants the caretaker silent until each workload owner has signed off,
even at the cost of the caretaker doing nothing on day one.

### Safety bounds

Tl;dr: the safety bounds are *global*, not per-Policy. Every recommendation —
defragmentation or node-health — passes through the same Arbiter gate before any
Dispatcher acts. Primum non nocere: the caretaker must never be the thing that
destabilizes the cluster.

**Why global, not per-Policy.** If each Policy enforced its own rate limit, two
Policies firing in the same loop could each stay under their own cap while
together exceeding the cluster's safe migration rate — node-health evacuating
three Instances while defragmentation migrates three more, six concurrent moves
when the cluster can absorb three. The failure mode is real: a node-health event
and a fragmentation threshold can trip at once. So the bounds live in the
Arbiter, which sees *all* Policies' output and enforces one budget across them.

**Definition.** The *Arbiter* is the single component that receives every
Policy's ranked recommendations, applies the global gates below in order, and
admits or rejects each candidate. A rejected candidate produces an event and a
metric; it does not retry inside the loop.

The gates, applied by the Arbiter to the merged candidate stream:

- **In-flight cap (cluster-wide)**: default 3. The Arbiter tracks in-flight
  actions across *both* surfaces (OMENative migration and eviction) and across
  *both* Policies, and refuses to admit more.
- **Per-hour cap (cluster-wide)**: default 10 actions per rolling hour, summed
  over all Policies.
- **Per-workload cooldown**: default 30 minutes after the last action touching a
  workload, inclusive of failures, regardless of which Policy proposed it. A
  workload defragmentation moved 10 minutes ago is *not* eligible for a
  node-health evacuation 10 minutes later — the cooldown is per-workload, not
  per-Policy.
- **Per-node cooldown**: default 10 minutes after an action touches a node
  (source or target).
- **Capacity check**: before admitting a candidate, the Arbiter re-checks the
  `ClusterSnapshot` for a feasible target with enough contiguous free GPUs and a
  ready model copy. A candidate scored against a stale snapshot whose target has
  since filled is rejected here, not dispatched into a guaranteed-pending pod.
- **Circuit breaker**: if the recent-10-actions failure rate exceeds 50%, the
  Arbiter pauses *all* execution for 60 minutes and emits a critical event.
  Failure mode: a systematically bad target (a node that looks free but rejects
  scheduling) would otherwise let the caretaker thrash; the breaker stops the
  bleeding.
- **Dry-run mode**: `mode: recommend-only` disables execution entirely — Policies
  still produce recommendations and the Arbiter still scores and gates them, but
  no Dispatcher fires. Useful during rollout to watch what the caretaker *would*
  do.

These defaults are deliberately low. The caretaker optimizes a steady-state
property (fragmentation) and reacts to a rare event (node health); neither
justifies moving more than a handful of workloads per hour. An operator who
wants faster convergence raises the caps explicitly and owns the consequence.

One bound that is *not* the Arbiter's: maintenance windows. Defragmentation runs
only inside configured `maintenanceWindows` (default business hours UTC,
weekdays), because a steady-state optimization can wait. Node-health evacuation
overrides the window — a failing accelerator is not a "wait until Monday"
problem. `emergencyPendingAgeMinutes` (default 10) likewise overrides the window
for defragmentation when a pending pod has starved past the threshold. The
window is a per-Policy urgency policy; the rate limits, cooldowns, and capacity
check above are global.

### Concurrent-operation awareness

The caretaker is one of several actors moving pods around. Acting blind to the
others is how it picks a target the Cluster Autoscaler is about to delete, or
migrates an ISVC the HPA is mid-scale on. Each is a concrete defer-or-exclude
rule, applied by the Arbiter regardless of which Policy proposed the candidate:

- **HPA**: the Arbiter checks HPA status conditions / scale history for the
  target InferenceService. If the HPA scaled within the last 2 minutes, that
  ISVC is deferred — migrating a workload mid-scale fights the autoscaler and
  produces churn neither actor wanted.
- **Cluster Autoscaler**: nodes annotated
  `cluster-autoscaler.kubernetes.io/scale-down-disabled: "true"` are excluded
  from placement hints. Nodes in active CA scale-down (detected via CA's
  deletion-candidate label) are also excluded — placing a workload on a node the
  CA is about to drain guarantees a second move.
- **In-flight migrations (OMENative)**: if
  `InferenceService.Status.MigrationHistory` has an entry with `phase:
  InProgress`, the Arbiter defers new actions for that ISVC. OMENative is the
  authoritative record of what is already moving.
- **Node maintenance**: nodes carrying common maintenance taints
  (`node.kubernetes.io/unschedulable`, custom `ome.io/maintenance`) are excluded
  as targets.

Note that node-health evacuation is itself a node-maintenance-adjacent action;
the awareness rules apply to its *targets*, not its source — the whole point of
Policy #2 is to leave the unhealthy source.

### Spot and preemptible nodes

A spot/preemptible node can vanish on the cloud provider's schedule, so the
caretaker should never *land* a stable workload there and should *prefer* to
evacuate one already there before the provider preempts it. Policy-driven:

```yaml
spotPolicy:
  avoidAsTarget: true
  preferAsSource: true
  preemptibleLabels: [node.kubernetes.io/preemptible, cloud.google.com/gke-preemptible]
```

**Detection**: the caretaker matches each node's labels against
`spotPolicy.preemptibleLabels` (and, optionally, the standard K8s
`node.kubernetes.io/preemptible` label).

**Behavior**:

- `avoidAsTarget: true` excludes preemptible nodes from `hint_target_nodes`.
  Stable workloads land on non-preemptible nodes.
- `preferAsSource: true` boosts the priority of workloads on preemptible nodes
  so they evacuate before a preemption event, not after.
- Per-workload override via `alfred.ome.io/spot-policy: avoid|migrate|ignore`.

This is a scoring input shared by both Policies: defragmentation prefers spot
nodes as evacuation sources, and node-health treats a failing spot node the same
as any failing node — it just gets there first because the spot boost already
raised its priority.

### Multi-tenancy

Tenant boundary = namespace by default. The caretaker's recommendations and
actions are scoped to the ISVC's own namespace: it does not migrate workload A
in `ns1` to benefit workload B in `ns2`. Rationale: cross-tenant moves let one
team's optimization disrupt another team's workload, which no operator wants as
a silent default.

Operators with cross-tenant intent set `alfred.ome.io/tenant-group: <name>` on
ISVCs across namespaces; the caretaker treats same-group ISVCs as
cross-migratable (still subject to every other filter). The hard override
`allowCrossTenantOptimization: false` makes the namespace boundary absolute even
when tenant-group annotations are present — a cluster admin can veto cross-tenant
moves regardless of what individual teams annotate.

### Model-download coordination

The caretaker must not migrate a workload to a node that lacks the model — the
pod would land and immediately block on a multi-gigabyte download, turning a
migration into an outage. So placement hints filter by
`BaseModel.Status.NodesReady` / `ClusterBaseModel.Status.NodesReady` (OEP-0007
Q-017). If no ready node is feasible as a target:

- Skip the candidate.
- Emit event `NoFeasibleTarget` on the ISVC with the reason.
- Do *not* trigger a pre-download in v1. The operator pre-provisions the model
  (via BaseModel / ClusterBaseModel distribution) before expecting migrations to
  that node.

This applies to both Policies: a node-health evacuation that has nowhere with a
ready model copy is skipped and surfaced, not forced. Anticipated objection: "a
failing node with no migration target leaves the workload stranded." Honest
answer for v1 — yes; the caretaker signals `NoFeasibleTarget` and the owning
controller's normal recovery applies. Auto-triggered pre-download (the caretaker
asking model-agent to distribute the model to a target before retrying) is a
future iteration, out of scope for v1.

### Degraded mode

The caretaker can execute multi-pod migrations *only* through OMENative; without
it, the only safe surface left is single-pod RawDeployment eviction. So at
startup it queries API discovery for OMENative's registration — the
`WorkloadStrategy` feature-gate status and the ability to read the
`InferenceService.Status.MigrationHistory` field schema.

**If OMENative is not installed** (feature gate off or strategy not registered):

- The caretaker runs in degraded recommend-only mode for all workloads, even
  those annotated `movable: "true"` — there is no safe executor for them.
- RawDeployment eviction stays enabled (it does not need OMENative).
- `alfred_omenative_unavailable` gauge is set to 1.
- Event `OMENativeUnavailable` is emitted on the *transition* into degraded
  mode, not on every decision loop — otherwise the event stream becomes noise.
- ConfigMap `omenativeMigrationEnabled: true` is a no-op while degraded.

Both Policies degrade the same way: node-health evacuation of a multi-pod
Instance also requires OMENative, so in degraded mode it falls back to
recommend-only and signals the operator rather than acting.

### Deployment model

Ships as:

- Binary: `cmd/alfred/main.go` → `alfred` container image.
- Helm chart: `charts/ome-alfred/` (or a sub-chart of `ome-resources`).
- Deployment: 3 replicas, leader election enabled.
- ServiceAccount + ClusterRole + ClusterRoleBinding per [RBAC](#rbac) below.
- ConfigMap `alfred-config` with default values.

The caretaker installs alongside OME (same Helm release) or independently; it is
a separate Deployment, not embedded in the main OME manager, so it has an
independent release cadence, failure domain, and resource footprint. Hard
dependencies:

- OME `InferenceService` CRD installed.
- OMENative strategy registered (otherwise degraded mode — see above).

### Leader election

`client-go/tools/leaderelection` with a Lease resource:

- Lease duration: 15s
- Renew deadline: 10s
- Retry period: 2s

Only the leader runs the decision loop — Policies, Arbiter, and Dispatchers — so
exactly one replica is acting on the cluster at a time. All replicas run the
observation loop, so Prometheus can scrape any replica for snapshot-derived
gauges. This mirrors cluster-autoscaler's pattern.

On leader loss, in-flight action tracking is transient state; the new leader
rebuilds it from `InferenceService.Status.MigrationHistory` (OMENative is the
authoritative source) plus the `alfred-recommendations` ConfigMap. Failure mode
being mitigated: a leader handoff must not double-dispatch an in-flight
migration, so the global in-flight cap is reconstructed from durable cluster
state, not from the dead leader's memory.

### RBAC

Tl;dr: the caretaker re-scope adds *no* new write permissions. Because Policy #2
is **evacuate + signal** — it migrates via the existing OMENative annotation and
evicts via the existing Eviction subresource, and otherwise only *signals* the
owning controller — the narrow write contracts below are unchanged. In
particular: **no node-write permission.** The caretaker never cordons, drains,
or patches a Node; it reads node state and acts on workloads.

The full ClusterRole, unchanged from the defragmenter scope:

```yaml
# Nodes (read-only) — no write, no cordon, no drain
- apiGroups: [""]
  resources: [nodes]
  verbs: [get, list, watch]

# Pods (read) + eviction (create on subresource only)
- apiGroups: [""]
  resources: [pods]
  verbs: [get, list, watch]
- apiGroups: [""]
  resources: [pods/eviction]
  verbs: [create]

# ConfigMaps (read for policy loading, write only to pre-created Alfred-owned ConfigMaps)
- apiGroups: [""]
  resources: [configmaps]
  verbs: [get, list, watch]
- apiGroups: [""]
  resources: [configmaps]
  verbs: [update, patch, delete]
  resourceNames:
    - alfred-config
    - alfred-recommendations

# Events
- apiGroups: [""]
  resources: [events]
  verbs: [create, patch]

# Leader election
- apiGroups: [coordination.k8s.io]
  resources: [leases]
  verbs: [create, get, update]

# OME CRDs (read)
- apiGroups: [ome.io]
  resources:
    - inferenceservices
    - inferenceservices/status
    - servingruntimes
    - clusterservingruntimes
    - basemodels
    - clusterbasemodels
    - acceleratorclasses
  verbs: [get, list, watch]

# OME InferenceService annotation patching (narrow contract)
- apiGroups: [ome.io]
  resources: [inferenceservices]
  verbs: [patch]
```

The write contracts in detail — each is the *only* mutation the caretaker can
perform on that resource:

| Resource | Verbs | What the write can do | Why no broader grant |
|---|---|---|---|
| `nodes` | get, list, watch | nothing — read-only | Policy #2 evacuates *workloads*, it never touches the Node object; node cordon/drain belongs to maintenance tooling, not the caretaker |
| `pods` | get, list, watch | nothing — read-only | candidate selection reads pods; it never patches or deletes them directly |
| `pods/eviction` | create | request a graceful eviction (RawDeployment single-pod only) | eviction is the K8s-sanctioned destructive path; it respects PodDisruptionBudgets, so the blast radius is bounded by the cluster's own policy |
| `inferenceservices` | patch | write/delete `ome.io/migration-request-v1-*` annotations only | a migration request is one annotation; the patch must not touch spec, status, labels, finalizers, or other annotations (enforced cluster-side, below) |
| `configmaps` (named) | update, patch, delete | mutate only `alfred-config` / `alfred-recommendations` | the caretaker does not create ConfigMaps at runtime; Helm pre-creates them |
| `events` | create, patch | emit observability events | events are the audit trail; no other side effect |
| `leases` | create, get, update | leader-election Lease | standard controller pattern |

**Why the re-scope needs nothing new.** Node-health evacuation could, in a naive
design, demand `nodes` write (to cordon the failing node) and `pods` delete (to
force the workload off). It demands neither: evacuation reuses the OMENative
migration annotation for multi-pod Instances and the Eviction subresource for
single-pod RawDeployment, and for everything else it *signals* — emits an event
and a `migration-request` annotation that the owning controller acts on. The
narrow surface that bounded the defragmenter's blast radius bounds the
caretaker's identically. The RBAC table above is the security contract; adding
node-write to it would be the moment the caretaker stops being safe-by-design.

**Authorization boundary.** The dedicated Alfred service account is the only
principal allowed to trigger OMENative migration in v1. In production, no other
principal should hold generic `patch` on `InferenceService`, and cluster-side
admission must reject migration-annotation mutation by non-Alfred callers even
if such RBAC exists.

**RBAC invariant.** The caretaker's only allowed `patch` effect on
`InferenceService` is to write or delete `ome.io/migration-request-v1-*`
annotations. An internal patch-gateway helper guards this at runtime, but it is
*not* the primary security boundary.

**Mandatory cluster-side enforcement.** Execute mode requires a
`ValidatingAdmissionPolicy` (K8s 1.30+) installed by the Helm chart. If the
policy or binding is absent, the caretaker must refuse to start in
`mode: execute` and fall back to recommend-only. The reference policy object
belongs in the Helm chart template
(`charts/ome-alfred/templates/admission-policy.yaml`, or the equivalent subtree
under `charts/ome-resources/` if packaged there). The policy must enforce two
invariants:

1. Only Alfred's service account may add, update, or delete
   `ome.io/migration-request-v1-*` annotations.
2. Even Alfred's service account may not change `spec`, `status`, labels,
   finalizers, owner references, or non-migration annotations as part of that
   patch.

The exact CEL expression is implementation detail and must be covered by
negative integration tests; the design contract is the enforcement behavior
above, not a sample snippet. PATCH-type caveat: the implementation must be
validated against both JSON merge patch and JSON Patch semantics. If the chosen
admission checks cannot soundly prove the invariants above for a patch type the
caretaker might send, execute mode must reject that patch type rather than rely
on ambiguous CEL behavior.

**ConfigMap write boundary.** The caretaker does not create ConfigMaps at
runtime. Helm pre-creates `alfred-config` and, if recommendation snapshots are
enabled, `alfred-recommendations`; the caretaker only updates/patches/deletes
those named objects.

### Observability

Tl;dr: every recommendation metric now carries a `policy` label so operators can
tell defragmentation churn from node-health churn at a glance; the fragmentation
and migration metrics are unchanged; two node-health counters are added. Events
and the audit ConfigMap stay as the human-readable surfaces.

**Why the `policy` label.** With two Policies feeding one Arbiter, an
undifferentiated `alfred_recommendations_produced_total` can't answer "is the
caretaker moving pods because of fragmentation or because nodes are failing?" —
the single most important operational question. So the recommendation and
outcome counters gain `policy="defragmentation"|"nodehealth"`. The
snapshot-derived gauges (fragmentation scores, capacity) are inherently
Policy-agnostic and keep their existing labels.

**Prometheus metrics**:

Snapshot / fragmentation gauges (unchanged):

- `alfred_cluster_fragmentation_score` (gauge, 0-1)
- `alfred_node_fragmentation_score{node}` (gauge)
- `alfred_gpu_capacity{node,status}` (gauge; status: total/allocated/free/contiguous_max)
- `alfred_schedulability_score{size}` (gauge)
- `alfred_pending_pod_count` (gauge)
- `alfred_pending_pod_gpu_requirements{size}` (gauge)

Recommendation / migration counters (now `policy`-labeled):

- `alfred_recommendations_produced_total{policy,workload,component,reason,executable}` (counter)
- `alfred_recommendations_accepted_total{policy,workload,component}` (counter)
- `alfred_recommendations_rejected_total{policy,workload,component,reason}` (counter)
- `alfred_migration_calls_total{policy,workload,mode,surface}` (counter; surface: eviction/omenative)
- `alfred_migration_outcome_total{policy,workload,mode,outcome}` (counter; outcome: completed/failed/timeout)
- `alfred_lws_recommendations_total{isvc,action}` (counter; action: manual)

Node-health counters (new):

- `alfred_nodehealth_evacuations_total{node,workload,surface,outcome}` (counter; the count of evacuation actions Policy #2 actually dispatched)
- `alfred_nodehealth_signals_total{node,reason}` (counter; the count of signal-only outcomes — degraded mode, no feasible target, or `signalOnly: true` — where the caretaker emitted a signal instead of acting)

The split between `_evacuations_total` and `_signals_total` is the load-bearing
distinction: an evacuation moved a workload; a signal punted to the owning
controller. An operator watching `_signals_total` climb while
`_evacuations_total` stays flat knows the caretaker *wants* to act but can't —
no target, or degraded mode — which is exactly when a human should look.

Loop / operational metrics (unchanged):

- `alfred_observation_loop_duration_seconds` (histogram)
- `alfred_decision_loop_duration_seconds` (histogram)
- `alfred_leader_status{pod}` (gauge; 0/1)
- `alfred_policy_reload_total{outcome}` (counter; outcome: success/failure)
- `alfred_circuit_breaker_state` (gauge; 0: closed/1: open)
- `alfred_omenative_unavailable` (gauge; 0/1)

**Events** on InferenceService:

- `FragmentationRecommendationProduced`
- `MigrationRequested`, `MigrationCompleted`, `MigrationFailed`, `MigrationRejected`
- `MigrationSkippedOptOut`
- `MigrationSkippedMaintenanceWindow`
- `MigrationSkippedCooldown`
- `MigrationSkippedRateLimit`
- `LWSMigrationUnsupported`
- `NoFeasibleTarget`
- `NodeHealthEvacuationRequested` (Policy #2 dispatched an evacuation for this workload)

**Events** on Alfred's own ConfigMap:

- `PolicyReloadFailed`
- `CircuitBreakerOpened` / `CircuitBreakerClosed`

**Events** cluster-wide:

- `OMENativeUnavailable`

**Logs**: structured JSON. Every multi-step operation carries a correlation ID
(recommendation UUID) so a single migration can be traced from Policy through
Arbiter to Dispatcher.

**Audit ConfigMap (optional)**: if `recommendationsConfigMapEnabled: true`, the
caretaker maintains the `alfred-recommendations` ConfigMap in its own namespace —
a single structured snapshot operators can read without scraping events. Each
recommendation now records which Policy produced it:

```yaml
data:
  recommendations.json: |
    {
      "generated_at": "2026-04-16T12:00:00Z",
      "cluster_fragmentation_score": 0.62,
      "threshold": 0.5,
      "recommendations": [
        {
          "id": "rec-001",
          "policy": "defragmentation",
          "workload": "prod/llama-70b",
          "component": "engine",
          "instance": 0,
          "from_node": "node1",
          "hint_target_nodes": ["node3", "node7"],
          "benefit_score": 0.18,
          "cost_score": 0.1,
          "final_score": 0.08,
          "reason": "fragmentation",
          "executable": true,
          "strategy": "OMENative",
          "status": "accepted"
        }
      ]
    }
```

## Test plan

Tl;dr: the old suite covered one policy (Defrag) end-to-end; the engine
refactor adds three things that can break independently — the **Arbiter**
(cross-policy conflict resolution), **Node-Health Evacuation** (a second
policy that actuates), and the **OEP-0013 read-only seam** (Alfred must not
fight Component-Scoped Autoscaling). Each gets its own coverage below, on top
of the existing Defrag tests, which are preserved verbatim because Defrag is
now Policy #1, not a special case.

Terminology used throughout this section:

- **Policy** : a pluggable unit implementing the `Policy` interface
  (`Evaluate(snapshot) -> []Candidate`). Defrag is Policy #1, Node-Health
  Evacuation is Policy #2, Descheduling is Policy #3.
- **Candidate** : a policy's request to act on a (workload, target) — the
  rename of the old single-policy "Recommendation", generalized so the
  Arbiter can compare candidates from different policies.
- **Arbiter** : the engine stage that takes all candidates for one cycle,
  resolves conflicts, enforces mutual exclusion, and emits at most one action
  per (workload, node).

### Unit tests

Target coverage per package (engine refactor splits the old monolith; the
new packages carry their own thresholds):

- `engine/arbiter`: ≥ 90% — new; the conflict-resolution logic is the highest-risk addition.
- `policy/defrag`: ≥ 90% — the old `scorer`/`recommender` logic, ported behind the `Policy` interface.
- `policy/nodehealth`: ≥ 90% — new.
- `policy/descheduling`: ≥ 90% — new, lands in Phase 2.
- `observer`: ≥ 90%
- `executor`: ≥ 85%
- `metrics`: ≥ 80%
- `controller`: ≥ 80%

Synthetic cluster snapshot harness for unit testing without real K8s. The
harness is shared across all policies and the arbiter, because they all
consume the same immutable snapshot — see Alternative 6 for why that sharing
is load-bearing.

New unit coverage for the engine refactor:

1. **Arbiter priority ordering.** Two policies propose actions on disjoint
   workloads; verify the Arbiter emits both, ordered by the configured policy
   priority (Node-Health > Defrag > Descheduling). *Failure mode if absent:* a
   low-priority cosmetic defrag could be scheduled ahead of an urgent
   node-health evacuation, delaying evacuation off a failing GPU.
2. **Arbiter mutual exclusion on a workload.** Defrag and Node-Health both
   propose an action on the **same workload** in one cycle; verify exactly one
   action is emitted (the higher-priority one) and the loser is dropped with a
   recorded reason. *Failure mode if absent:* two policies move the same
   workload at once — double migration, corrupted lifecycle.
3. **Arbiter mutual exclusion on a node.** Two policies target the **same
   node** as a migration destination such that the node would be over-committed
   if both ran; verify the Arbiter serializes or drops to keep the node within
   capacity.
4. **OEP-0013 skip.** A workload's Component is mid-scaling per the OEP-0013
   read-only seam (autoscaler is actively changing replica count); verify every
   policy's candidate against that workload is filtered before it reaches the
   Arbiter. *Failure mode if absent:* Alfred migrates a pod the autoscaler is
   simultaneously scaling — the two controllers thrash.
5. **Node-health candidate selection.** Given a snapshot with one node
   carrying a `GpuUnhealthy` condition (per the remediation-signal schema, TBD
   — see Open Questions), verify Node-Health Evacuation selects exactly the OME
   workloads on that node as candidates, and never selects workloads on healthy
   nodes. Verify a node with a transient/cleared condition produces no
   candidate.
6. **Node-health emits evacuate + signal.** Verify the policy produces both an
   evacuation candidate **and** a remediation signal (an Event in Phase 1; the
   pluggable cloud node-remediation provider is Phase 3). The two are coupled:
   evacuating without signalling leaves a bad node in service for new pods.

The existing single-policy unit expectations (fragmentation scoring,
threshold gating, placement-hint computation, cooldown, rate limiting) move
under `policy/defrag` unchanged.

### Integration tests

The existing 23 integration tests are preserved (deploy in a kind cluster,
mock InferenceServices, verify behavior end-to-end):

1. **End-to-end observation**: deploy Alfred in a kind cluster, deploy mock
   InferenceServices (via test CRDs), verify
   `alfred_cluster_fragmentation_score` changes with workload deployments.
2. **Recommendation generation**: construct a 3-single-GPU-on-3-nodes
   scenario. Verify Alfred (Defrag policy) produces 2 consolidation candidates.
3. **Pending-pod prioritization**: create a Pending 8-GPU pod; verify Alfred
   boosts candidates that would unblock it.
4. **RawDeployment eviction**: `mode: execute`; verify Alfred calls the
   Eviction API for a RawDeployment workload.
5. **OMENative migration invocation**: OMENative workload; verify Alfred
   writes `ome.io/migration-request-v1-<uuid>` on the InferenceService. Mock
   OMENative acks (clears annotation, emits status).
6. **LWS recommendation-only**: LWS-backed workload; verify recommendation
   emitted but no migration request; event reflects `LWSMigrationUnsupported`.
7. **Opt-out**: `alfred.ome.io/movable: "false"` excludes the workload from
   candidates; event explains.
8. **Per-workload cooldown**: execute a migration, verify no new attempt for
   the same workload within cooldown.
9. **Maintenance window**: active window; Alfred continues recommending, does
   not execute.
10. **Rate limiting**: 10 candidates; only 3 migrations in-flight.
11. **Policy hot reload**: update ConfigMap; Alfred reloads without restart.
12. **Leader election**: 3 replicas; kill leader; new leader elected within 20s.
13. **Post-migration health monitoring**: migration succeeds but target GPU
    fails within window; Alfred marks node suspect, backs off.
14. **HPA coexistence**: HPA scaling the target ISVC; Alfred defers.
15. **CA coexistence**: node marked `scale-down-disabled`; excluded from hints.
16. **OMENative-unavailable degraded mode**: feature gate off; Alfred enters
    degraded mode, only RawDeployment evictions occur.
17. **Circuit breaker**: simulate 6+ failed migrations; verify circuit opens,
    pauses execution.
18. **Non-OME workload**: Kubeflow Notebook Pod on a node; Alfred observes but
    does not migrate; fragmentation score accounts for its GPU consumption.
19. **Multi-tenancy default**: workload in namespace A cannot be migrated to
    benefit workload in namespace B without explicit tenant-group annotation.
20. **Spot node avoidance**: preemptible-labeled node excluded from targets;
    preemptible source prioritized.
21. **Admission hardening**: non-Alfred caller attempts to modify
    `ome.io/migration-request-v1-*`; request rejected by mandatory admission policy.
22. **Alfred narrow patch enforcement**: Alfred caller attempts to modify
    `spec` or non-migration annotations in the same patch; request rejected by
    admission policy.
23. **Unsupported schema version**: Alfred writes a request with unsupported
    `schemaVersion`; OMENative rejects it and Alfred marks the workload
    recommend-only for OMENative execution until versions are compatible.

New integration coverage for the multi-policy engine:

24. **Node-health end-to-end (the headline Phase 1 test).** A node transitions
    to `GpuUnhealthy`. Verify, in order: (a) Alfred (Node-Health policy)
    selects the OME workloads on that node, (b) they are evacuated via the same
    actuation path as Defrag (OMENative migration request for OMENative
    workloads, Eviction API for RawDeployment), (c) a remediation Event is
    emitted naming the node, and (d) the bad node receives **no re-placement** —
    Alfred excludes it from every policy's target set for as long as the
    condition holds. *Failure mode if absent:* Alfred evacuates a node and then
    immediately migrates a different workload back onto it.
25. **Arbiter cross-policy in a live cluster.** Stage a cluster where Defrag
    wants to consolidate workload W onto node N while Node-Health wants to
    evacuate a workload off node N in the same cycle; verify the Arbiter lets
    Node-Health win and Defrag's candidate is dropped for that cycle with a
    recorded reason, then re-evaluated next cycle.
26. **OEP-0013 seam in a live cluster.** A Component is actively scaling
    (replica count changing); verify no Alfred policy actuates against its
    workload until scaling settles. This is the integration-level twin of unit
    test 4.
27. **Descheduling (Phase 2).** A workload violating its placement
    preference (e.g., a soft anti-affinity that drifted) is selected by the
    Descheduling policy and migrated; verify it does not fire on workloads that
    still satisfy their constraints.

### Chaos / robustness

Existing:

- Kill Alfred during in-flight migration; verify recovery.
- API server slow response; verify backoff.
- OMENative rejects all requests; verify circuit breaker triggers.
- InferenceService deleted mid-migration; verify Alfred cleans up gracefully.

New, targeting the multi-policy failure surface:

- **Flapping node conditions don't thrash.** A node's `GpuUnhealthy` condition
  flaps on/off faster than the cooldown window. Verify Alfred evacuates **at
  most once** per cooldown and does not oscillate between evacuate and
  re-place. The cooldown holds across the flap — the condition clearing does
  not reset it. *Failure mode if absent:* a node with intermittently-reported
  health flaps Alfred into an evacuate/re-place loop, churning workloads.
- **Defrag and Node-Health never act on the same workload or node in one
  cycle.** Run a chaos scenario with both policies enabled, randomized node
  health, and randomized fragmentation; over many cycles, assert the invariant
  that no (workload, node) pair is touched by two policies in the same cycle.
  This is the live, randomized version of unit tests 2 and 3 — the Arbiter is
  the only thing standing between two policies and a double-move, so it is
  fuzzed, not just unit-tested.

### Simulation

- 10 random cluster states; verify post-Alfred state has lower or equal
  fragmentation.
- Adversarial states designed to trick the Defrag scorer; verify safe
  rejection or correct handling.
- Long-running simulation (100 decision cycles) — verify no thrashing (bounded
  migration count per workload), now with all enabled policies active so the
  thrashing bound is asserted against the **combined** action stream, not just
  Defrag's.

## Graduation criteria

The maturity ladder maps onto the phasing: Alpha ships the engine refactor
plus the first two policies; Beta adds Descheduling and production soak; GA
optionally adds the predictive planner and a cloud remediation provider. The
phasing is deliberate — each phase is independently useful, and a phase can
slip without blocking the one below it.

### Alpha

Scope: the engine refactor (`Policy` interface + `Arbiter`), Defrag ported to
**Policy #1**, **Policy #2 Node-Health Evacuation** (evacuate + remediation
signal), **arbitration-lite** (priority ordering + mutual exclusion, no
forecasting), and the **OEP-0013 read-only seam**.

- Unit tests ≥ 80% coverage, including the new `engine/arbiter`,
  `policy/defrag`, and `policy/nodehealth` packages.
- Integration tests 1–10 passing, plus the node-health end-to-end test (24),
  the cross-policy arbiter test (25), and the OEP-0013 seam test (26).
- The two new chaos tests passing: flapping-node cooldown, and the
  no-same-cycle-collision invariant.
- Deployment via Helm documented.
- Feature gate `AlfredGPUDefragmenter` disabled by default in the OME Helm chart.
- Policy schema documented, including how to enable/disable each policy and set
  the Arbiter priority order.
- `recommend-only` is the default mode; `execute` is opt-in. This applies to
  **every** policy, Node-Health included: an alpha operator can run Node-Health
  in recommend-only and watch the Events before granting it the eviction verb.

### Beta

Scope adds **Policy #3 Descheduling**.

- Integration tests 1–27 (Descheduling included) + chaos + simulation passing.
- 2+ production users running Alfred for ≥ 60 days without high-severity incidents
  attributed to Alfred — across **at least Defrag and Node-Health enabled
  together**, so arbitration is exercised in production, not just in CI.
- Dashboard + runbook published, covering per-policy metrics and the Arbiter's
  drop-reason counters.
- RBAC policy review by a security reviewer.
- OMENative at Beta maturity (execute path validated end-to-end).
- The remediation-signal schema (see Open Questions) is settled at least for
  the Event-based Phase 1 form; the pluggable-provider shape may still be TBD.

### GA

Scope optionally adds, and is not blocked on, a **predictive planner**
(forecasting layer) and a **pluggable cloud node-remediation provider**. Both
are Phase 3 / "GA-and-beyond": GA does not require them to ship, but the
engine must not preclude them.

- 6 months of Beta.
- Test coverage ≥ 85%.
- Policy schema and the `Policy`/`Arbiter` interfaces stabilized — adding a
  fourth policy must not require an interface change.
- OMENative at GA.
- Deprecation story documented (if Alfred is ever replaced).
- If the predictive planner ships: its forecasts feed the Arbiter as
  advisory-only input, and an operator can disable it without losing the
  reactive policies. If it does not ship by GA, that is acceptable — see Open
  Questions on whether it is worth building at all.

## Implementation history

- 2026-04-16: OEP-0008 initial draft (single-purpose defragmenter).
- 2026-04-16: Design questions consolidated; 37 decisions locked.
- 2026-04-16: OEP-0008 updated to reflect locked decisions.
- 2026-06-04: Reframed from "Alfred GPU Defragmenter" to "Alfred GPU Cluster
  Caretaker": Defrag becomes one policy behind a `Policy` interface, an
  `Arbiter` resolves cross-policy conflicts, Node-Health Evacuation added as
  Policy #2, Descheduling reserved as Policy #3, and the predictive planner and
  cloud remediation provider deferred to Phase 3. Phasing mapped onto
  Alpha/Beta/GA.
- TBD: Alpha implementation (engine refactor + Policies #1 and #2).
- TBD: First Beta user.
- TBD: Beta (Policy #3 Descheduling).
- TBD: GA.

## Drawbacks

1. **Cluster-wide logical controller.** Alfred is one actor making
   cluster-wide decisions. Bugs have broad blast radius. Similar risk profile
   to cluster-autoscaler. Multi-policy raises the stakes: the Arbiter is now a
   single point through which every policy's actions flow, so an Arbiter bug
   can mis-order or drop actions from **all** policies at once.
2. **Requires OMENative for full value on multi-pod workloads.** Until users
   adopt OMENative, Alfred is limited to RawDeployment + recommendations-only
   for others — this is true for Node-Health evacuation too, not just Defrag.
3. **Heuristic, not optimal.** Greedy candidate selection may miss non-obvious
   consolidations. Trade: simplicity and speed over global optimality. The
   predictive planner (Phase 3) is the proposed answer, but it is not yet
   justified — see Open Questions.
4. **Operational overhead.** Operators learn Alfred's metrics, policy schema,
   and failure modes — now multiplied across three policies and an Arbiter
   priority order they must configure.
5. **Policy complexity.** The ConfigMap schema has many knobs, and the
   multi-policy engine adds per-policy enablement plus the Arbiter priority
   list. Good defaults mitigate, but the failure surface is larger than the
   single-policy design.
6. **Cross-controller interaction space.** Alfred, OMENative, HPA, CA,
   OEP-0013 Component-Scoped Autoscaling, and the scheduler form a complex
   interaction graph. Subtle bugs possible. Mitigated by conservative defaults,
   the OEP-0013 read-only seam, and extensive observability.
7. **Narrow write contract is brittle.** The annotation contract with OMENative
   is the primary coupling point. Race conditions possible during annotation
   write/clear; mitigated by idempotent UUID semantics. Node-Health reuses this
   same contract, so it inherits both the safety property and the brittleness.

## Alternatives

### Alternative 1: Alfred as a scheduler plugin

Run Alfred inside a custom K8s scheduler, intercepting scheduling events to
apply bin-packing at placement time.

- **Pros:** Scheduling is the right place for placement logic.
- **Cons:** Only affects new pods; does not handle runtime defragmentation —
  and does nothing for node-health evacuation, which is inherently about
  workloads already running. Would need combination with a separate actor for
  migration anyway.

Rejected — the core problem is runtime caretaking (defrag, evacuation,
descheduling), not initial placement.

### Alternative 2: Embed Alfred in the OME manager

Run Alfred's logic inside the main OME controller manager.

- **Pros:** Single deployment, shared informer cache.
- **Cons:** Alfred's failure takes down OME core. Operational profiles differ.
  Less flexible release cadence.

Rejected — keeping them separate is cleaner.

### Alternative 3: Alfred as a CRD-driven system

Alfred's policy = CRD (`DefragmentationPolicy`); recommendations = CRD
(`RecommendationHistory`).

- **Pros:** K8s-idiomatic configuration.
- **Cons:** Violates the "no new CRDs" hard constraint; adds API surface;
  high-frequency data (recommendations/candidates) in etcd is expensive.

Rejected — no new CRDs.

### Alternative 4: Don't build Alfred; manual operator intervention

Operators manually trigger migrations and evacuations during scheduled
maintenance.

- **Pros:** Zero new components.
- **Cons:** Does not scale; reactive, not preventive; fragmentation
  continuously drifts, and node-health evacuation cannot wait for a maintenance
  window — a failing GPU degrades a running workload now.

Rejected — the problem scales with cluster size and workload churn, and
node-health is time-sensitive.

### Alternative 5: Extend cluster-autoscaler

Fork / contribute to CA to add GPU-aware defragmentation.

- **Pros:** Leverages CA's existing architecture.
- **Cons:** CA is node-focused (scale nodes), not pod-focused (move pods).
  Different problem. Fork is high-cost.

Rejected — architectural mismatch.

### Alternative 6: Keep Alfred a single-purpose defragmenter; put node-health and descheduling in separate controllers

Leave Alfred as the defragmenter it was, and ship Node-Health Evacuation and
Descheduling as their own standalone controllers, each with its own
deployment, snapshot, and actuation path.

- **Pros:** Smaller blast radius per controller; each can fail or be disabled
  in isolation; no Arbiter to build.
- **Cons:** All three policies need the **same** machinery — the cluster
  snapshot (observe nodes/GPUs/workloads), the safety bounds (cooldown, rate
  limiting, circuit breaker, maintenance windows, opt-out), and the **same
  narrow actuation surface** (OMENative migration request / Eviction API).
  N controllers means N copies of that machinery to build, test, and keep in
  sync. Worse: with no shared engine, there is **no place to do cross-policy
  arbitration** — two independent controllers can evacuate and consolidate the
  same workload in the same instant, exactly the double-move the Arbiter
  exists to prevent. The coordination problem doesn't disappear; it just moves
  out of one process and into an unowned gap between three.

Rejected — the policies share the snapshot, the safety machinery, and the
narrow actuation, so splitting them duplicates the engine three ways and
forfeits the one thing only a shared engine can provide: arbitration across
policies.

## Open questions

Some of these are the long-standing research-level questions carried from the
single-policy design; the rest are new, surfaced by the multi-policy reframing.
None block design completeness; they bound what is **not** yet decided.

New, from the Caretaker reframing:

1. **Is the predictive planner worth Phase 3 at all?** The greedy heuristic is
   reactive: it defrags and evacuates after fragmentation or failure is
   observed. A forecasting layer would let the Arbiter pre-empt — e.g.,
   evacuate ahead of a predicted GPU failure. Open question: does the
   prediction accuracy justify the complexity and the false-positive cost
   (evacuating a node that would have been fine)? The engine is built to accept
   forecasts as advisory Arbiter input, but whether we ever build the forecaster
   is genuinely undecided. GA does not depend on it.

2. **The exact remediation-signal schema.** Node-Health Evacuation emits a
   signal that "this node needs remediation". In Phase 1 that is a Kubernetes
   Event. The open questions: what is the structured payload (node, condition,
   evacuation outcome, suggested action)? Is it an Event, a node annotation, or
   both? And what contract does the Phase 3 pluggable cloud node-remediation
   provider consume — the same schema, or a richer one? Settle the Event form
   for Beta; the provider form can lag.

3. **DRA (Dynamic Resource Allocation) interaction.** K8s 1.31+
   `ResourceClaims` reshape GPU allocation, which affects **both** how Alfred
   reads "GPU allocation" for fragmentation scoring **and** how Node-Health
   reasons about which workloads hold a claim on a failing GPU. Revisit as DRA
   adoption progresses; this is now a cross-policy concern, not just a Defrag one.

4. **OEP-0013 seam depth.** The read-only seam (Alfred skips workloads whose
   Component is mid-scaling) is the minimum to avoid thrashing with
   Component-Scoped Autoscaling. Open question: is "skip while scaling" enough,
   or should Alfred read OEP-0013's desired-replica intent to plan around
   it (e.g., not consolidate onto a node a Component is about to scale into)?
   Phase 1 ships the minimal seam; deeper coupling is deferred.

Carried from the single-policy design (research-level, not blocking):

5. **Fragmentation score formula validation** (Q-049). Defaults ship with the
   proposed weights; alpha-user feedback will refine them. What's the
   evaluation methodology? TBD during alpha.

6. **GPU topology awareness within a node (NVLink, PCIe)** (Q-040). v1 treats
   intra-node GPUs as fungible. v2+ considers NVLink neighborhoods. Source of
   topology data still open (gpu-feature-discovery, DCGM, custom?).

7. **RDMA fabric topology across nodes** (Q-039). Multi-node migration targets
   need a full RDMA mesh. Data source unclear. Research.

8. **Historical learning from past migration outcomes** (Q-042). Adjust scoring
   weights or cost model based on real-world outcomes. v2+ feature; data
   collection starts in v1. Note: this overlaps with the predictive-planner
   question above — if we collect outcome data in v1, it is the training signal
   the forecaster would need.

9. **Cost accounting — runtime vs static** (Q-043). v1 uses static
   per-workload-shape cost constants. v2 measures actual disruption cost from
   past migrations.

10. **Dedicated UI beyond metrics + events** (Q-044). v1 = metrics + events +
    ConfigMap. Dashboards operator-owned. No dedicated UI.

11. **Unified "OME Ops" controller** (Q-045). The Caretaker reframing is a step
    toward this: Defrag, Node-Health, and Descheduling now share one engine.
    The open question becomes whether auto-patch / auto-repair / maintenance
    tooling also fold in as further policies, or stay separate. Current design
    is compatible — the `Policy` interface is the obvious extension point.

12. **Auto-patch / auto-repair re-introduction** (Q-046, Q-022 on plugin
    architecture). Could become Policy #4/#5 under the new engine rather than
    separate OEPs. Alfred is designed not to preclude this.

13. **Model pre-download coordination** (Q-032 extension). v2 may add
    auto-triggered pre-download to target nodes before retrying migrations
    blocked by model unavailability — relevant to Node-Health too, where an
    evacuation's target may lack the model.

14. **Final name** (Q-048). "Alfred" is the working name, inherited from
    ome-operator. The Caretaker reframing reopens this: candidates now include
    `GPUCaretaker`, `WorkloadConsolidator`, `OMEScheduler`. Finalize before GA.

15. **Kueue interaction detail** (Q-027). Alfred respects `SuspendedLabel`.
    Full integration with queueing and quotas depends on OEP-0003
    AcceleratorClass maturity. Revisit then.

16. **Priority-class boost semantics** (add as Q-053 if this matures).
    Operator-set `priorityClass` on an InferenceService influences scheduler
    placement; should Alfred also weight candidate-selection by `priorityClass`?
    Current design: no. Priority is for placement/preemption, not caretaking.
    May revisit — and if it does, it is an Arbiter-level concern (which
    policy's candidate wins), not a per-policy one.

