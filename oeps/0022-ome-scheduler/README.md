# OEP-0022: ome-scheduler — topology-aware gang placement for accelerator fleets

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Primer: how Kubernetes scheduling works](#primer-how-kubernetes-scheduling-works)
  - [Architecture](#architecture)
  - [How OME describes work to the scheduler](#how-ome-describes-work-to-the-scheduler)
  - [Placement: how a gang gets scheduled](#placement-how-a-gang-gets-scheduled)
  - [Additional placement features](#additional-placement-features)
  - [Phase B: quota — adopt Kueue](#phase-b-quota--adopt-kueue)
  - [Phase C: topology-aware preemption and reclaim](#phase-c-topology-aware-preemption-and-reclaim)
  - [Phase D: batch backfill](#phase-d-batch-backfill)
  - [Protecting running services](#protecting-running-services)
- [Design Details](#design-details)
  - [The scheduler binary and how it is deployed](#the-scheduler-binary-and-how-it-is-deployed)
  - [What can go wrong, and what happens then](#what-can-go-wrong-and-what-happens-then)
  - [Working alongside other systems](#working-alongside-other-systems)
  - [Review findings: holes found and how we close them](#review-findings-holes-found-and-how-we-close-them)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [Appendix A: a worked example](#appendix-a-a-worked-example)
- [Appendix B: glossary](#appendix-b-glossary)
<!-- /toc -->

> **Audience.** Engineers who may not have a Kubernetes scheduling background. A glossary is in Appendix B; every load-bearing term is defined on first use.

## Summary

We are building `ome-scheduler`: a Kubernetes scheduler for OME's accelerator workloads — GPU and TPU alike. It solves problems the default Kubernetes scheduler cannot solve. It places groups of pods together on connected accelerator hardware. It packs work tightly so capacity is not wasted. In later phases, it will also enforce quotas between teams and evict lower-priority work to make room for higher-priority work.

We do not write a scheduler from scratch. We reuse the official Kubernetes scheduler code as a library and add our own logic through its built-in plugin system. The result is our own scheduler program, deployed alongside the default one. Only pods that ask for it use it.

OME itself changes very little. It keeps managing pods directly, as it does today, and simply tells the scheduler which pods belong together and how they should be treated.

Delivery happens in four phases, each useful on its own: **Phase A** is placement, **Phase B** brings quotas and queues by adopting Kueue, **Phase C** adds topology-aware preemption, and **Phase D** lets batch jobs use idle capacity.

**Current alpha implementation boundary.** The code currently implements topology-aware placement, `Permit` gating for one PodGroup at a time, standalone-pod domain scoring, and replica spreading (the per-service Preferred/Required policy below). Partner-gang placement (prefill/decode co-location), custom queue ordering, topology-aware preemption, and the protection layer remain design targets in this provisional OEP. Until partner reservations are implemented, the scheduler deliberately fails closed when it sees the configured placement-group label; producers must omit that label rather than assuming co-location is active.

**Scope: any accelerator.** The model is deliberately fabric-agnostic. A *domain* is whatever the configured topology label groups — an NVLink clique on GB200, a multi-host TPU slice on GKE, an RDMA island or rack elsewhere, or nothing at all for plain CPU work (which then carries no co-location constraint and is the trivial case). A pool can set one topology label in plugin configuration; a PodGroup may override it through a configurable annotation key when one scheduler serves several fabrics. Neither the metadata key nor the label value is baked into the plugin binary, so one build serves GPU, TPU, and CPU pools. **TPU is in v1**: forcing every gang member into a single slice-domain is exactly what stops a multi-host TPU gang from splitting across two slices — a failure mode seen on real multi-host TPU pools. On a GKE 2x2x2 pool, `cloud.google.com/gke-tpu-partition-2x2x2-id=<slice-id>` is the unique slice label. The key is topology-shaped, so each fleet and TPU shape must verify its own unique-per-slice label instead of assuming that 2x2x2 key. The one real fabric difference lives *within* a domain (see [Placement](#placement-how-a-gang-gets-scheduled)): an NVLink clique is all-to-all, so any free node in it is interchangeable, whereas a TPU slice is a torus, so a gang that uses less than a full slice needs a contiguous sub-slice. Version 1 places whole-domain gangs — the common serving case for GPU and TPU alike — and treats finer, shape-aware sub-domain placement as another level of the topology hierarchy (see [Multi-level topology](#additional-placement-features)).

## Motivation

OME manages inference pods directly. It does not use Deployments, StatefulSets, LWS, or Ray. We learned this the hard way: those tools brought their own opinions about restarts, updates, and pod identity, and those opinions repeatedly did not fit multi-node inference. Each mismatch forced a fork or a rewrite. What survived are boring, mature building blocks used as libraries.

Placement is now the gap, because of how modern accelerator fleets are built. Nodes come in groups wired together with very fast networking. We call such a group a **domain**. One GB200 NVL72 rack is a domain: 18 nodes that behave almost like one giant GPU. A job that spans several nodes must stay inside one domain to get that speed. On other fleets, the equivalent grouping is a rack, an RDMA island, or a multi-host TPU slice; the design treats all of these the same way — a domain is whatever the fabric wires tightly together, named by a configurable label. (Accelerators differ inside a domain, not in this framing: an NVLink clique is all-to-all, a TPU slice is a torus. That distinction only matters for a gang smaller than a domain — see [Placement](#placement-how-a-gang-gets-scheduled).)

Three properties of our workloads make placement hard:

First, multi-node inference is a **gang**: a set of pods that only works if every member runs. If 17 of 18 pods are placed and the last one cannot fit, the 17 do nothing except hold accelerators.

Second, modern LLM serving splits one service into two cooperating groups, **prefill** and **decode**, which exchange data constantly. They should sit close together, ideally in the same domain.

Third, fragmentation is expensive. If small jobs are scattered across many domains, no domain has enough free nodes left for a large gang, even when the fleet as a whole has plenty of free capacity.

The default Kubernetes scheduler cannot handle this. It places one pod at a time and has no concept of a group, so it can strand a gang half-placed. Its scoring spreads pods out by design, which is exactly what fragments domains. And on managed platforms such as GKE, EKS, and OKE, its configuration cannot be changed. That is why a second scheduler is needed.

This is not hypothetical: on real production fleets, large multi-node gangs have stranded tens of pods `Pending` because no domain had enough contiguous free nodes — while OME had already created the PodGroup that says "these go together." The default scheduler ignores the group and places pods one at a time until the domain runs out. Goals R1 and R3 below are that failure, named.

### Goals

| # | Requirement | Plain statement | Phase |
|---|---|---|---|
| R1 | Bin packing | Two parts: keep each gang within the fewest domains, and — the harder, fleet-level part — fill already-used domains first so empty domains stay whole for large jobs, including steering standalone pods. | A |
| R2 | Group closeness | Prefill and decode for one service land in the same domain when possible. | A |
| R3 | Gang placement | All members of a gang land in one domain, and either the whole gang runs or none of it does. | A |
| R4 | Eviction safety | Never remove part of a gang. Gangs move as a whole or not at all. | A |
| R5 | Ratio safety | When pods must be removed or are lost, keep the prefill/decode balance as close to target as possible. | A |
| R6 | Quotas and fairness | Teams get guaranteed shares of the fleet, can borrow idle capacity, and are treated fairly when they compete. | B (Kueue, adopted) |
| R7 | Preemption and reclaim | Higher-priority work can take capacity from lower-priority work — always whole gangs, never leaving a broken group behind. Borrowed capacity can be called back. | B + C |
| R8 | Batch backfill | Batch jobs (evals, weight conversion) can run on idle capacity and give it back when inference needs it. | D (mostly Kueue configuration) |

R1 through R3 are placement problems and belong to the scheduler. One nuance on R1: Kueue's topology-aware scheduling (TAS) already packs a single gang into the fewest domains, so that half overlaps it; the fleet-level half — filling busy domains first and steering standalone pods so empty domains stay whole — is what no admission-time system delivers, and it stays scheduler-only (see [Phase B](#phase-b-quota--adopt-kueue)). R4 and R5 are about how pods die, and most of that does not go through any scheduler, so they are handled by a separate protection layer (see [Protecting running services](#protecting-running-services)). R6 through R8 were originally out of scope. The decision now: quota is adopted, not built — Kueue owns it. Preemption splits in two — Kueue already evicts whole workloads for quota and priority reasons, and we build only the topology-aware remainder. Batch backfill then becomes mostly configuration on top of both.

### Non-Goals

`ome-scheduler` will never be a from-scratch scheduling core; the reasons in [Alternatives](#alternatives) do not change with scope. It will never do node-level tricks like oversubscription or performance isolation between pods sharing a node — that is a node agent's job, a different kind of component. It will never share a node pool with another placement decision-maker. And it will never move a running pod on its own; moving services is OME's job, placing them is ours.

## Proposal

### Primer: how Kubernetes scheduling works

A scheduler in Kubernetes is just a program. It watches for pods that have no node assigned, picks a node for each, and writes that assignment (called a **binding**) to the API server. The kubelet on the chosen node then starts the pod.

A cluster can run several schedulers at the same time. Every pod names the scheduler it wants using a field called `schedulerName`. The default scheduler handles everything that does not name another one. This is why we can add our own scheduler on GKE or EKS without touching anything the platform manages.

The official Kubernetes scheduler exposes a plugin system: named hooks inside its decision cycle where custom code can run. Plugins are compiled into the program, so "our plugin" ships as our own scheduler binary that contains the entire official scheduler plus our additions. The hooks we use:

| Hook | What it does, in plain terms |
|---|---|
| QueueSort | Decides which pending pod the scheduler looks at next. |
| PreEnqueue | Decides whether a pod is even ready to be considered. |
| PreFilter | Runs once per pod, before nodes are checked. Can restrict which nodes are considered at all. |
| Filter | A yes/no check for each candidate node. |
| Score | Ranks the nodes that passed Filter. |
| Reserve | Bookmarks resources for the pod before the final commit. Has an undo (Unreserve). |
| Permit | Can hold a pod at the very last step. We use it as the gate where a gang waits for all its members. |
| PostFilter | Runs when no node fits. This is where preemption logic lives. |
| PostBind | Runs after the assignment is written. Used for bookkeeping. |

One more mechanism matters: a **scheduling gate** is a flag on a pod that says "do not schedule me yet." Whoever removes the flag approves the pod to run. We use this as the plug-in point for quotas later. A gated pod is not merely deprioritized — it is kept out of the scheduler's active queue entirely until the gate is removed, so every hook above (including PreEnqueue) only ever sees already-admitted pods.

### Architecture

```
┌──────────────────────────────────────────────────────────────┐
│ OME controller                                               │
│   Creates pods and PodGroups. Owns naming, restarts,         │
│   rollouts, and prefill/decode ratio repair.                 │
│   Never reads nodes. Needs no new permissions.               │
└──────────────┬───────────────────────────────────────────────┘
               │ Creates gangs with a hold flag:
               │ "these pods are one gang, place together"
               ▼
┌──────────────────────────────────────────────────────────────┐
│ Kueue (adopted — Phase B)                                    │
│   Team quotas, queues, borrowing, fair sharing.              │
│   Admits work by removing the hold flag. Evicts whole        │
│   workloads when quota or priority demands it.               │
└──────────────┬───────────────────────────────────────────────┘
               │ Admitted work
               ▼
┌──────────────────────────────────────────────────────────────┐
│ ome-scheduler (this document)                                │
│   Phase A: placement plugin — domains, packing, gang gate    │
│   Phase C: topology preemption engine                        │
│   Watches nodes and pods. Decides placement. Writes bindings.│
└──────────────┬───────────────────────────────────────────────┘
               │ Placed pods
               ▼
┌──────────────────────────────────────────────────────────────┐
│ Protection layer                                             │
│   Eviction webhook: blocks removals that would break a gang  │
│   Per-role PDBs: minimum pod counts for prefill and decode   │
│   OME repair loop: restores gangs and ratio after any loss   │
└──────────────────────────────────────────────────────────────┘
```

OME declares what belongs together. Kueue decides whose work may start. The scheduler decides where it goes. The protection layer controls how things are allowed to die. This is the standard Kubernetes division of labor, extended with a vocabulary for groups.

**The hub.** Several later sections refer to a management hub: a control-plane component that holds a cross-cluster capacity ledger, moves capacity between pools by flipping whole domains, and makes multi-cluster dispatch decisions. The hub is specified separately — it is the OEP-0018 management plane, not part of this document. ome-scheduler depends on it at exactly three interfaces: it *exports* a per-domain "largest gang that still fits" metric the hub reads; it *honors* a pool label the hub may flip; and in multi-cluster mode the hub, not stock Kueue, makes the admit decision through the hold flag. Nothing in Phase A requires the hub to exist.

### How OME describes work to the scheduler

The whole interface is data on objects OME already creates. No new API calls, no direct connection between OME and the scheduler.

**PodGroup.** A small object that says "these pods belong together, and at least this many must be placed as a unit." We reuse the existing PodGroup type from the upstream scheduler-plugins project, which OME already creates today:

```yaml
apiVersion: scheduling.x-k8s.io/v1alpha1
kind: PodGroup
metadata:
  name: svc-a-prefill
  namespace: team-inference
  annotations:
    ome.io/topology-key: network.example.com/domain
  labels:
    ome.io/role: prefill
spec:
  minMember: 18                        # the gang: at least 18 placed together
  scheduleTimeoutSeconds: 600
```

**Pod labels and fields.**

```yaml
metadata:
  labels:
    scheduling.x-k8s.io/pod-group: svc-a-prefill   # which gang this pod belongs to
    ome.io/service: svc-a                          # for spreading replicas
    ome.io/role: prefill
spec:
  schedulerName: ome-scheduler
  schedulingGates:
  - name: ome.io/admission                         # the hold flag; see below
```

**The hold flag.** Gang pods are created with a scheduling gate. In Phase A, OME removes it immediately, so it changes nothing in practice. In Phase B, Kueue removes it when the team's quota allows. This one field is how quota, and any alternative admission system, plugs in without touching anything else.

**Ordering.** OME creates the PodGroup first, then all gang pods. This ensures the scheduler can resolve the group before it evaluates a member, but creation order alone is not an atomicity guarantee. Atomic admission comes from selecting a gang-aware scheduler through `schedulerName`; the default scheduler ignores PodGroup and may place members independently.

**Policies.** Two per-service knobs, both with values `Required` or `Preferred`. Co-location says whether prefill and decode must share a domain or merely should. Spreading says whether replicas of the same service must land in different domains or merely should. Both matter on NVL72, where a domain is only 18 nodes, so they are per-service settings, not global ones.

Those policy knobs and their placement-group metadata are not implemented in the current alpha. The chart configures the metadata key as `unsupportedPlacementGroupLabel`; seeing it makes the scheduler hold the gang Pending. This is intentional fail-closed behavior until the reservation described under [Review findings](#review-findings-holes-found-and-how-we-close-them) exists.

### Placement: how a gang gets scheduled

**Domains and counting.** Nodes carry a domain label written by the accelerator software stack. All ready nodes sharing a label value form one domain. In version 1, every gang pod uses all the accelerators of its node, so measuring free capacity is simple: count free nodes per domain. For an all-to-all fabric like NVLink this also means the exact node inside a domain does not matter — any free node is as good as another — which keeps the design simple. A TPU slice is a torus, not all-to-all, so this node-interchangeability holds only when a gang uses the whole slice (the common case); a gang smaller than a slice needs a contiguous sub-slice, which is the shape-aware case deferred to [Multi-level topology](#additional-placement-features). Whole-domain gangs on both fabrics reduce to the same count.

**Choosing a domain.** When the first pod of a gang arrives, the plugin checks which domains have enough free nodes for the entire gang. If co-location is Required, it counts prefill and decode together. Among the domains that fit, it picks the fullest one. This "best fit" rule is the packing strategy: it concentrates work into domains that are already in use and preserves empty domains as large contiguous blocks for future big gangs. If no domain fits, the whole gang waits — no member is placed alone.

**Pinning.** The chosen domain is remembered for the service group. Every other member of the gang, and the partner gang (decode for prefill, or vice versa), is steered to that domain: the PreFilter hook narrows the candidate nodes to that domain, and the Filter hook rejects everything else. Because the scheduler considers one pod at a time, this remembered decision is what turns per-pod scheduling into a group decision.

**The group gate.** Each placed member waits at the Permit hook instead of starting immediately. When the count of waiting members reaches `minMember`, all of them are released at the same moment. If the gang does not complete within the timeout, every waiting member is released back, all bookkeeping is undone, and the gang retries from scratch — possibly picking a different domain. Because OME creates all pods at once, the healthy wait is under a second; the timeout is a safety valve for a genuinely stuck member, such as a node going bad mid-flight.

Two correctness details make this safe under concurrency. First, members waiting at the gate hold their reserved nodes, so a half-formed gang temporarily pins capacity. To stop two gangs each half-filling the same domain and deadlocking until timeout, a gang claims its **entire** domain footprint on its first pod — the pin above is a real hold, not a hint — and later members consume that claim instead of competing for it. Second, Bind can still fail after the gate opens (a node dying in the gap between release and bind); if any member fails to bind, the whole gang is unreserved and retried, never left partially bound.

**Nodes within the domain.** For whole-node pods any free node works. Scoring is still set to prefer fuller nodes, which becomes important later when pods that use only part of a node are supported.

**Standalone pods (design target; not current alpha).** Pods that are not part of any gang will be scored toward domains that are already busy. This stops small services from nibbling one node out of each empty domain and quietly destroying the fleet's ability to host large gangs. This packing only holds if **every** accelerator pod in the pool is scheduled by ome-scheduler: a pod the default scheduler places is invisible to our counting, so it can nibble an empty domain and we never see it. On any pool where R1 matters, per-pod opt-in is therefore not enough — the pool must be exclusively ome-scheduled (see [What can go wrong](#what-can-go-wrong-and-what-happens-then)). Per-pod opt-in is for mixed pools where R1 is best-effort.

**Retrying.** When a gang cannot fit, the scheduler subscribes to the events that could change the answer — a pod freeing capacity, a node appearing, a label changing — and retries the gang when one happens. The retry checks are deliberately cheap and precise, so heavy churn elsewhere in the cluster does not cause pointless retries.

### Additional placement features

These come from studying Volcano, KAI, Koordinator, YuniKorn, and Kueue. Each is stated as the problem it solves.

#### Protecting big gangs from starving (version 1)

The problem: best-fit packing plus standalone-pod steering means every node that frees up in a nearly-ready domain gets taken by smaller work before a big gang can accumulate enough. The big gang can wait forever. Queue order does not help, because a small gang that fits schedules regardless of who is ahead of it.

The fix: a gang that has waited past a threshold (default: twice the gang timeout) becomes protected, oldest first. The plugin then reserves the domain closest to being able to host it and blocks other pods from eating into the reserved headroom. The reservation ends when the gang places or when it expires. Safety rules: one reservation per domain, a fleet-wide cap on active reservations, and strict oldest-first ordering so two big gangs cannot deadlock the fleet against each other.

#### Spreading replicas of a service (version 1 — implemented, scheduler-free)

The problem: a domain is a single failure zone. One NVSwitch or power-shelf failure takes out an entire NVL72 rack. If all replicas of a service are packed into one domain — which is exactly what best-fit does — one hardware failure takes the whole service down.

The fix needs no scheduler feature at all. OME renders the per-component `topologySpread: Preferred|Required` policy (next to `topologyKey`; ISVC overrides runtime) as a standard `topologySpreadConstraint` on each instance's ANCHOR pod — the leader, or the sole pod of a single-pod instance — with maxSkew 1 and a selector matching sibling anchors, over `topologySpreadKey` (defaults to the co-location key; set coarser where the failure zone contains many gang domains, e.g. co-locate by TPU partition, spread across `cloud.google.com/gce-topology-subblock`). One anchor per instance makes matching-pod counts equal instance counts; workers never race ahead because their generated worker→leader required affinity holds them until the leader is placed. `Required` (DoNotSchedule) is enforced by every scheduler that runs PodTopologySpread's Filter — including this profile, which disables only its Score: when best-fit pins a domain the constraint vetoes, PostFilter releases the pin, remembers the domain, and the retry plans a compatible one (integration-tested end to end, including the coarser-key TPU shape). maxSkew 1 means BALANCED spreading — three instances on two racks land 2/1, never Pending on balance alone. `Preferred` (ScheduleAnyway) is advisory: honored by schedulers that score spreading, currently without effect under this packing profile (Score disabled, and gang candidates are pre-narrowed to one domain before scoring); a plugin-side domain preference remains the designed extension if soft spreading under this profile ever earns its cost. The constraint is deliberately outside the revision hash, so toggling the policy shapes future placements without rolling a healthy fleet.

#### Flexible gang size (version 1)

A gang can declare a minimum and a desired size — for example, a decode pool that needs at least 4 workers but wants 8. The gate opens at the minimum. The domain choice prefers domains that fit the desired size, and the extra members trickle in after release.

#### Clear answers to "why is my gang pending" (version 1)

A lesson from operating Volcano is that pending jobs are opaque. The intended design writes the arithmetic onto the PodGroup status: how many nodes the gang needs, which domain came closest and how many nodes it had free, and what blocked it (not enough capacity, spreading rule, or a reservation held by another gang). `kubectl describe podgroup` would then answer the question without reading scheduler logs, and OME could surface the same message on the service. **This is not implemented in the current alpha:** failed placement is visible in scheduler logs and framework status only; the scheduler does not write PodGroup status or an OME service condition yet.

#### Multi-level topology (version 2, shaped for now)

Today the design has one level of grouping. A gang larger than one domain simply never schedules. The fix, later, is a hierarchy — island, then domain, then node — where a gang is placed at the smallest level that can hold it, using as few groups as possible at each level. Version 1 ships one level, but the capacity-tracking code is written as a list of levels from day one so this becomes an addition, not a rewrite. The same list-of-levels structure absorbs finer topology *within* a domain — a TPU slice's sub-slice torus shape, for instance — so shape-aware placement of a gang that uses less than a whole slice is an added level, not a redesign. Until then, a sub-slice TPU gang is served by giving it a whole slice, exactly as OME does today.

#### Reserving capacity ahead of pods (version 2)

Rolling updates of a gang need the new copy to have a guaranteed landing spot before the old copy is torn down — start-new-before-stop-old. Safe defragmentation moves need the same guarantee. The mechanism is a reservation object: a claim on capacity that future pods then consume, an approach proven by the Koordinator project. It builds directly on the starvation reservations above. Until it exists, defragmentation stays opportunistic: the domain choice is recomputed on every rollout, so every rolling update naturally re-packs the service, and the management hub can trigger service moves when fleet fragmentation metrics degrade.

#### Two facts worth writing down

The cluster autoscaler cannot understand our placement rules — it simulates with the default scheduler's logic — so autoscaling decisions for gang pods would be wrong. The fleets this scheduler targets are statically provisioned, so this is a documented limitation, not a gap. And the scheduler never moves a running pod; OME moves services, the scheduler only places what OME creates.

### Phase B: quota — adopt Kueue

"Capacity management" hides four different functions, and Kueue is the right owner of exactly one of them. Splitting them cleanly is the decision record for this phase.

**Who may run: Kueue, adopted.** Kueue gives teams ClusterQueues with guaranteed quota, ceilings, and a hierarchy of cohorts; queues borrow idle capacity from each other subject to lending limits; competing queues are shared fairly; workloads carry priority. Mechanically, it admits work by removing the hold flag — exactly the contract this design already defines — so adoption requires no change to OME and no change to the placement plugin. Two facts tip build-versus-adopt decisively. First, Kueue's preemption evicts whole workloads by construction, which satisfies the never-break-a-gang rule at the quota level for free; adopting it deletes the in-house arbiter entirely and the largest part of the planned preemption engine. Second, its risk profile is not the LWS/Ray category: it is not a behavioral controller wrapping our pods, it is not a scheduler, it is owned by kubernetes-sigs, and it is productized inside GKE. The accepted costs: its Workload object model enters our stack, we track its release cadence, and our quota policy must be expressible through its queue and admission-check APIs. The escape stays open by design — the hold flag is the interface, so any pool can swap Kueue for an in-house arbiter (the original Phase B plan) without touching anything else.

**What exists and fits: never Kueue.** Kueue's quota is declared numbers, and quota is fungible while domains are not. A queue can be 200 GPUs under its quota and still unable to place one 18-node gang, because no domain has 18 contiguous free nodes. The measured, topology-shaped truth lives in the scheduler's per-domain metrics — the "largest gang that still fits" number — and in the hub's capacity ledger. Reconciling declared quota against placeable reality is exactly the gap this scheduler closes.

**Where it goes: never Kueue — and run it without TAS.** Kueue's topology-aware scheduling (TAS) assigns a workload to topology domains at admission time, and it genuinely does pack a single gang into the fewest domains — that half of R1 overlaps it, and a pool that needs only that could use TAS and skip ome-scheduler. It cannot be the placement engine here, though, for reasons that are concrete, not territorial. It decides at admission against a snapshot and then hands binding to whatever scheduler is named on the pod — the default one on GKE/EKS/OKE, which we cannot reconfigure — so nodes still spread within the chosen domain and the assignment can go stale before it binds. It is per-workload, so it has no way to express prefill/decode co-location across two separate gangs, replica spreading across domains, or starvation reservations held over time. And it never sees standalone or non-Kueue pods — the main fragmenters — so the fleet-level half of R1 is beyond it. Only once those gaps push us to own placement does the "second brain" rule bite: with ome-scheduler assigning domains at bind time, TAS must be off so two systems don't assign domains and disagree — one brain per function, the same rule as one brain per pool.

**Which cluster: the hub.** Kueue's multi-cluster mode, in stock form, races clusters — the first to admit wins — which is the opposite of packing. If multi-cluster Kueue enters the stack, its external-dispatcher hook becomes the hub's enforcement arm, and the hub makes the packing decision from the per-domain metrics.

In one line: quota in Kueue, truth in the scheduler and the hub, placement in ome-scheduler, dispatch in the hub.

### Phase C: topology-aware preemption and reclaim

With Kueue adopted, preemption splits in two. Quota-level preemption — a higher-priority workload needs quota, or a lending queue calls back borrowed capacity — is Kueue's job, and Kueue already evicts whole workloads only. That covers most of R7 with no code from us.

What Kueue cannot see is the case its quota arithmetic calls healthy: a gang has quota, is admitted, and still starves because no domain has enough contiguous free nodes. Freeing a whole domain is a placement-level act, so this remainder is ours. When policy allows it, the engine picks a victim: whole service groups only, lowest tier first, least damage — fewest GPUs displaced, and the prefill/decode balance preserved in what remains. Before evicting anyone, it reserves the domain being freed for the waiting gang, using the starvation-reservation machinery, so the capacity cannot be stolen while victims shut down. Then it evicts the victim group whole, carrying the token our protection webhook recognizes.

The victim must not fight its way straight back. Instead of letting its recreated pods re-enter scheduling, the engine requeues the victim's workload in Kueue: Kueue marks it evicted, the recreated pods arrive with the hold flag on, and the workload waits its turn in its queue like any other pending work. This is the Phase B dependency in its new form — Phase C requires the Kueue integration, not an in-house arbiter.

The safety rules are unchanged from the original plan: never between equals, only across tiers or against over-quota queues; a preemption reservation is itself never preempted; a fleet-wide cap on preemptions in flight; deterministic victim choice so every eviction can be explained afterward; and heavy testing of the races — slow-terminating victims, scheduler failover mid-preemption, expiring reservations. Even narrowed, this is the hardest code in the system and is budgeted that way.

Why the built-in machinery cannot do this: the framework's own preemption frees room for one pod on one node, and capacity freed across many nodes is normally taken by other work before a gang can gather it. The reservation is the missing piece. Multi-pod eviction from a plugin has upstream precedent in the CapacityScheduling plugin; we extend that pattern with group atomicity and the reserved landing.

### Phase D: batch backfill

Mostly configuration once B and C exist. Batch work — evals, weight-conversion jobs — gets low-priority queues in Kueue, which admits it into idle quota and preempts it, whole workloads at a time, when higher-priority inference arrives. Our Phase C engine covers the domain-level half: clearing contiguous space when an inference gang needs a domain that batch work is fragmenting. No job-runtime estimates are needed, because batch is evictable by definition; Slurm-style estimate-based backfill remains optional polish that nothing depends on.

### Protecting running services

Pods die in four ways, and only one of them ever consults a scheduler. This is why R4 and R5 cannot be solved by scheduler features alone, in any scheduler, including Volcano.

| How pods die | Asks a scheduler first? | Our control |
|---|---|---|
| Scheduler preemption | Yes — the only one | Does not exist until Phase C, and Phase C only ever evicts whole groups. |
| The eviction API: node drains, upgrades, autoscaler consolidation | No | The eviction webhook plus per-role PDBs, below. |
| Kubelet pressure: a node running out of memory kills pods | No | Gang pods request exactly what they use, on whole nodes, which makes them the last candidates for this. |
| Hardware failure | No — nobody asks | OME's repair loop recreates the gang; the scheduler re-places it. |

**The eviction webhook.** A small admission service that sees every eviction request for pods carrying a gang label. It rejects any request that would leave a gang partially alive. Whole-gang moves happen through OME, which tears down and recreates the group as a unit; a routine node drain is therefore forced to either skip gang pods or trigger a proper OME-managed migration. For evictions that are allowed, the webhook prefers removing pods from whichever role is currently over its target ratio. The webhook is configured to fail closed — if it is down, evictions of gang pods are refused — and it is scoped tightly to OME-labeled pods so it cannot interfere with anything else.

**PDBs.** OME creates a PodDisruptionBudget per role per service: a floor on how many prefill and how many decode pods must stay up, sized to the worst tolerable ratio. The floors use absolute numbers, not percentages, because percentage budgets require a standard controller behind the pods and OME's pods have none.

**OME's repair loop.** Hardware does not ask permission. After any loss, OME recreates pods to restore the gang size and the target ratio; the scheduler places them back into the pinned domain if it still has room, or re-plans if not. Repair, not eviction policy, is the real mechanism behind R5.

## Design Details

### The scheduler binary and how it is deployed

**Code.** One Go module in the OME project. The entry point builds the real Kubernetes scheduler with our plugin registered next to the built-in ones:

```go
package main

import (
	"os"

	"k8s.io/component-base/cli"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	"github.com/ome-projects/ome-scheduler/pkg/plugins/gangpack"
)

func main() {
	cmd := app.NewSchedulerCommand(
		app.WithPlugin(gangpack.Name, gangpack.New),
	)
	os.Exit(cli.Run(cmd))
}
```

Pulling in the Kubernetes code requires a known block of Go module redirects; we copy it from the upstream `scheduler-plugins` project rather than writing it by hand. The Kubernetes version we build against is pinned to the version the target clusters run (see [Graduation Criteria](#graduation-criteria)).

**Image and deployment.** A single static binary in a minimal container. It runs as an ordinary Deployment in the `ome-system` namespace with 2 replicas. Only one replica is active at a time; the second takes over automatically if the first dies. This failover uses a mechanism called leader election, on our own dedicated lock so we never interfere with the platform's scheduler. Two replicas are for availability, not speed — one active scheduler is far more than fast enough for our load (see [Test Plan](#test-plan)).

**Permissions.** The scheduler gets its own service account. Rather than copying a version-sensitive scheduler permission list, the Helm chart binds it to the API server's auto-reconciled `system:kube-scheduler` and `system:volume-scheduler` ClusterRoles. A chart-owned role adds only access to its dedicated leader Lease and read-only PodGroup access. The upstream scheduler role includes the pod deletion permission used by kube-scheduler internals; it does not grant arbitrary workload-pod creation. Because the built-in roles are reconciled for the cluster release, they also acquire that release's DRA permissions when DRA is enabled. This does not relax the binary version requirement: the scheduler executable must still be built for the supported cluster minor.

**Opt-in.** OME sets `schedulerName: ome-scheduler` on the pods it creates for services that enable this. Nothing else in the cluster is affected. This works identically on GKE Standard, OKE, EKS, and internal clusters. GKE Autopilot is not supported, because Autopilot forbids custom schedulers.

**Configuration.** A standard scheduler configuration file, delivered as a ConfigMap. The current alpha enables OMEGangPack through `multiPoint`, which expands it only onto the hooks it actually implements: PreFilter, Filter, PostFilter, Reserve, Permit, and PostBind. Queue ordering remains the upstream `PrioritySort`; custom QueueSort and PreEnqueue behavior are not implemented. PostBind only clears successful-cycle retry bookkeeping; it does not alter the gang's domain commitment. OMEGangPack does not implement Score in the current alpha. The separate built-in soft topology-spread score is disabled because spreading is the opposite of packing, while its hard Filter behavior remains active. The built-in NodeResourcesFit plugin uses "most allocated" for node-level packing; its weights are fleet-specific operational tuning, not part of OMEGangPack's contract. Plugin settings include an optional pool-wide domain label, a configurable PodGroup annotation name for per-gang overrides, configured informer/GC/Permit timeouts, and the placement-group label that currently fails closed. The OME chart supplies the `ome.io/*` metadata names; the generic plugin does not know that API prefix.

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
leaderElection:
  leaderElect: true
  resourceName: ome-scheduler
  resourceNamespace: ome-system
profiles:
- schedulerName: ome-scheduler
  plugins:
    multiPoint:
      enabled:
      - {name: OMEGangPack}
      disabled:
      - {name: DefaultPreemption}
    score:
      enabled:
      - {name: NodeResourcesFit, weight: 10}
      disabled:
      - {name: PodTopologySpread}
  pluginConfig:
  - name: OMEGangPack
    args:
      topologyKey: nvidia.com/gpu.clique
      podGroupTopologyKeyAnnotation: ome.io/topology-key
      unsupportedPlacementGroupLabel: ome.io/placement-group
      defaultPermitTimeoutSeconds: 600
      podGroupSyncTimeoutSeconds: 30
      gcIntervalSeconds: 60
  - name: NodeResourcesFit
    args:
      scoringStrategy:
        type: MostAllocated
        resources:
        - {name: nvidia.com/gpu, weight: 10}
        - {name: cpu, weight: 1}
        - {name: memory, weight: 1}
```

### What can go wrong, and what happens then

**Two gangs want the same domain.** Cannot conflict: the scheduler makes decisions one pod at a time on a single thread, so the first gang claims the domain and the second sees reduced capacity and picks elsewhere or waits.

**The plugin's bookkeeping disagrees with reality.** The built-in capacity check runs on every node anyway, as an independent second opinion. A disagreement leaves the pod pending — a retryable state — never broken. A background audit compares the plugin's counts against the scheduler's own view and raises an alarm on any mismatch, because a mismatch is a bug, not something to paper over.

**A node dies between the decision and the placement.** For whole-node pods, any other free node in the same domain works, so this usually self-heals. If the loss makes the whole gang not fit, the gate timeout unwinds everything and the gang re-plans.

**The scheduler restarts or fails over.** The new leader rebuilds its memory from reality: for every gang with placed pods, the domain is simply where those pods are. Gangs that were mid-flight lose their bookings and retry from scratch — safe by construction, because the gate never opened and nothing was running.

**The default scheduler races us onto the same nodes.** Both schedulers can, in theory, promise the same node to different pods; the kubelet accepts one and rejects the other, and the loser retries. With whole-node accelerator pods this is rare in practice. Marking node pools so only ome-scheduler's pods land there does two things: it removes this race entirely, and — more importantly — it is what makes bin-packing (R1) actually hold, since a pod the default scheduler places is invisible to our packing (see [Placement](#placement-how-a-gang-gets-scheduled)). For race-avoidance alone, exclusivity is an optimization; for R1 on a contended pool, it is required.

**A gang that can never fit.** It retries with increasing backoff, its status explains exactly what is missing, a metric counts repeated timeouts, and OME surfaces the condition on the service.

### Working alongside other systems

**Slurm.** The realistic need is training on Slurm and inference on OME sharing accelerator capacity. Three patterns, in order of preference.

Pattern A, a split fleet with a movable boundary: nodes carry a pool label; Slurm runs its pool through SUNK, Soperator, or Slinky's operator (all of which run Slurm's node agents on Kubernetes — this is CoreWeave's native mode), and ome-scheduler serves the inference pool via its pool selector. The management hub moves capacity by flipping *whole domains* between pools — never splitting a domain between two decision-makers, which would both fragment it and break the fabric grouping. When a domain flips toward Slurm, our protection layer is exactly what makes the handoff safe: gang-aware draining and, later, reservations for start-new-before-stop-old moves. When one flips toward us, waiting gangs notice the label change and retry automatically.

Pattern B, Slurm as the only decision-maker: SchedMD's slurm-bridge lets Slurm schedule Kubernetes pods directly. The rule is one decision-maker per node pool, so ome-scheduler is simply not deployed on such a pool. OME changes one field, `schedulerName`, and everything else it declares — the groups, labels, and hold flags — still works. This is deliberate: OME depends on the contract, not on our binary. Slurm's own topology-aware placement covers domain packing there.

Pattern C, a broker above both: the hub holds the capacity ledger across Slurm and Kubernetes fleets and moves capacity using pattern A's domain flips, or admits work through the hold flags. This is the management-plane direction, and it asks nothing more of ome-scheduler than pattern A does.

**Alternative admission systems.** Kueue is the default, but the hold flag is the interface: a pool can run an in-house arbiter or another admission system instead, and nothing else changes.

**Multiple clusters.** Cross-cluster placement is the hub's decision. Each cluster's ome-scheduler exports, per domain, the size of the largest gang that would still fit — the fragmentation number that actually matters. The hub uses it to pick a cluster; ome-scheduler picks the domain and nodes within it.

### Review findings: holes found and how we close them

A deliberate attempt to break this design found the following. Each item states the hole and the required change; the changes are considered part of the design.

**Required co-location did not hold the partner's space.** The fit check counted prefill and decode together, but only placed pods consumed capacity, so between the first gang landing and the second gang's pods arriving, other work could eat the partner's headroom — especially now that Kueue may admit the two gangs at different moments. Change: when co-location is Required, pinning a domain also places a real hold on the partner gang's node count, using the starvation-reservation machinery, with an expiry in case the partner never arrives.

**Two timers could fight: Kueue requeue versus starvation protection.** Kueue charges quota at admission, so a gang that cannot find a domain blocks its queue's quota while pending; the natural answer is Kueue's not-ready-in-time requeue. But that timer could evict a gang exactly when our starvation reservation was about to pay off. Change: the timers are ordered by policy — starvation threshold, then reservation lifetime, then Kueue's requeue timeout — so protection always gets to finish before requeue fires.

**Phase C was not failover-safe.** Placement pins rebuild from where bound pods sit, but reservations for pending gangs lived only in memory. A leader failure in the middle of a preemption would leave victims evicted, the reservation gone, and the freed domain stolen. Change: preemption and starvation reservations are persisted (as a small object or an annotation on the PodGroup) and rebuilt on failover. Related ordering rule: the victim's workload is deactivated in Kueue before its pods are evicted, otherwise recreated pods arrive still-admitted and slip past the hold flag.

**The protection webhook is narrower than the protection section implied, and its trust model needed fixing.** It intercepts the polite eviction API only; a direct pod delete bypasses it and bypasses PDBs. That is deliberate — Kueue preemption, Phase C, and OME rollouts all delete — but it means R4's guarantee covers voluntary disruption only, with direct deletes controlled by permissions and audit rather than the webhook. Trust change: the webhook authorizes whole-gang evictions by the requester's identity (the scheduler's service account), never by an annotation, which anyone with pod-edit rights could forge. Operational change: fail-closed means a dead webhook blocks every drain of gang pods fleet-wide, so the webhook gets two replicas, its own disruption budget, monitoring, and a documented break-glass path.

**Two group abstractions must be kept in lockstep.** Kueue groups plain pods into its Workload using its own annotations; the scheduler uses PodGroup. OME stamps both, and any disagreement in counts or membership produces gangs that are admitted but unschedulable, or half gated. Change: OME derives both from one source of truth and a validation check refuses pods where the two disagree. On flexible gangs, note the accounting consequence plainly: Kueue charges quota for the desired count even when only the minimum places. That is accepted policy, not an accident.

**Smaller closures.** Gang pods are validated to be whole-node in version 1, because the counting math silently breaks otherwise; fractional gangs wait for version 2. Whether standalone pods go through Kueue is a per-fleet policy decision that must be made explicitly, because mixed accounting misleads both systems. Once Kueue priorities exist, starvation protection orders by tier first and age second, so a low-priority elder cannot reserve a domain ahead of a high-priority gang. The per-domain "largest gang that fits" metric subtracts active reservations, or the hub double-counts capacity. Binding the scheduler ServiceAccount to the release-managed `system:kube-scheduler` role supplies the matching DRA permissions when that feature is enabled; the scheduler binary and tests must move to the same Kubernetes minor before those APIs are used. And the test plan gains a Kueue integration tier, exercising the full loop — admission, placement, requeue, preemption, victim re-admission — because that loop is exactly where these bugs live.

**What the review confirmed unchanged.** The interface choices (hold flags, PodGroup, pool labels) absorbed every scenario thrown at them, including Kueue adoption, without modification. Single-threaded decision-making makes domain claims race-free. Rebuilding placement memory from bound pods is sound. And the four-way split of how pods die remains the correct frame for R4 and R5.

## Test Plan

**The load is small.** Placement here is rare and chunky: gangs of a few dozen pods on rollouts and scale events. Even redeploying a few hundred services at once is a few thousand pods, against a scheduler core that sustains hundreds of placements per second. Speed is not the risk; correctness under constant change is.

**How fast one gang goes through.** An 18-pod gang is 18 quick decisions plus one gate release. The first pod pays the domain choice, a scan over a few hundred counters. Every later pod is a lookup plus a check over the 18 nodes of the chosen domain. Total compute is a few tens of milliseconds; the wall-clock time is dominated by API round-trips. Targets: the gate wait under one second in the healthy case, a full gang placed within five seconds, and every plugin hook under ten milliseconds at the 99th percentile.

**Three implementation rules keep it that way.** Domain counters are updated incrementally from events, never recomputed by walking all nodes on each decision. Retry hints are precise, so unrelated churn does not trigger pointless work. And because a waiting gang holds its booked capacity until the gate times out, the timeout doubles as the upper bound on how long capacity can be held by a gang that never completes — it is sized with that in mind.

**Testing, in three tiers.** First, unit benchmarks and the Go race detector on the domain-choice logic and its bookkeeping (the pure functions — this is where test-first development applies). Second, the upstream scheduler performance harness, which runs a real API server, extended with gang scenarios, with pass/fail thresholds on the per-hook latency metrics the framework already exports; integration correctness tests for each requirement (gang-into-one-domain, no-domain-fits, best-fit, co-location, spreading, gate-timeout unwind) mirror the shape of scheduler-plugins' coscheduling integration tests. Third, large-scale simulation with kwok, a tool that fakes thousands of nodes without any hardware: about 5,000 fake nodes forming about 280 domains, running a burst of 50 gangs at once, hours-long churn where the bookkeeping-drift alarm must stay silent, deliberate domain contention, leader kills in the middle of a gang, and mass-deletion storms to prove retries stay bounded. The Phase 1 canary then confirms the targets on real hardware. From Phase B on, a Kueue integration tier exercises the full loop — admission, placement, requeue, preemption, victim re-admission — because that loop is exactly where the [review-finding](#review-findings-holes-found-and-how-we-close-them) bugs live.

## Graduation Criteria

**Rollout plan.** Phase 0, shadow: deploy to one cluster with no workloads routed to it; verify failover, permissions, metrics, and that the bookkeeping audit stays clean against live fleet state. Phase 1, opt-in: one non-critical service switches its `schedulerName`; verify gang placement, co-location, spreading, and a deliberately forced gate timeout. Phase 2: gang-scheduled services default to ome-scheduler, then standalone services follow. Phase 3: the eviction webhook goes to log-only mode, then to enforcing; OME starts stamping the per-role PDBs. Phase 4: per-fleet configuration (domain label per fleet) and hub metrics integration. Phases B through D follow as their own projects on this base.

**One cutover cost to sequence.** Switching an existing service to ome-scheduler changes `schedulerName`, which is effectively immutable on a running pod — so the cutover recreates the service's pods. For an OMENative service still on `RecreatePod` with no rollout budget, that recreation is a full-fleet wipe, exactly like any other pod-template change. The schedulerName cutover must therefore follow the rollout-budget work (a bounded `updateStrategy` on every OMENative service) and be paced through it. On a fresh pool with no running services this cost is zero — which is the preferred way to adopt.

Rollback at every step is one field: `schedulerName` back to the default. No infrastructure to tear down.

**Version policy.** Our binary is built against a specific Kubernetes version and must stay in step with the clusters: the same minor version, or at most one behind, never ahead. The plugin interfaces genuinely change between Kubernetes versions, so version bumps are deliberate, reviewed changes with the full test suite behind them — not routine dependency updates. CI runs the test tiers above against every supported Kubernetes version, and two versions are supported at a time to cover fleet upgrade windows.

## Drawbacks

- **Another deployable to operate.** A second scheduler is a control-plane component with its own leader election, RBAC, config, version lockstep, and failure modes. The [failure-mode section](#what-can-go-wrong-and-what-happens-then) and the two-replica + audit design price this, but it is real operational surface that did not exist before.
- **Kubernetes-version lockstep.** Building the scheduler as a library means the binary is pinned to a Kubernetes minor version and must move with the fleet; a bump is a reviewed change, not a dependency update. This is the cost of inheriting the framework's hardening for free.
- **Kueue's object model enters our stack (Phase B).** Adopting Kueue brings its Workload objects, release cadence, and API surface; our quota policy must be expressible through its queue and admission-check APIs. The hold-flag interface keeps the escape open (swap in an in-house arbiter per pool), but by default we take the dependency.
- **R1 needs pool exclusivity.** Bin-packing only holds if every accelerator pod in the pool is ome-scheduled; a stray default-scheduled pod is invisible to our counting. On mixed pools R1 is best-effort only.
- **Fail-closed webhook blast radius.** The eviction webhook fails closed, so a dead webhook blocks every drain of gang pods fleet-wide. Mitigated with two replicas, its own PDB, monitoring, and a break-glass path — but it is a fleet-wide dependency for voluntary disruption.
- **Phase C is the hardest code in the system.** Topology-aware preemption with reservations, failover persistence, and race handling is genuinely complex and is budgeted as such; it is deliberately deferred behind Phases A and B.

## Alternatives

**Write a scheduler from scratch, the way Volcano and KAI did.** Rejected. Those projects wrote their own cores around 2017 to 2019, when the plugin system did not exist yet, so they had no choice. Anyone starting today does. Owning a core means owning years of hardening: caching correctness, races with the kubelet, retry and backoff behavior, and every new Kubernetes feature — the next big one being DRA, the new way Kubernetes allocates devices like GPUs. By reusing the official code, we inherit all of that for free, including DRA support, which matters for GB200.

**Put placement logic inside OME itself.** Rejected. To place pods, a component must watch every node, track all free capacity, and have authority over allocation. Giving OME that would turn a workload controller into an over-powered monolith and would effectively lock the accelerator pool to OME alone. Kubernetes already defines the right home for that power: a scheduler. In our design OME never reads a node object at all.

**Adopt Volcano or KAI instead of building.** Rejected, by the same rule that removed LWS and Ray: they are full schedulers, large opinionated systems in the critical path whose opinions become our problem. We keep contract compatibility with them on purpose (see [Working alongside other systems](#working-alongside-other-systems)), so one of them could still be adopted for a specific node pool later without changing OME.

**Adopt Kueue — for quota only.** Accepted; [Phase B](#phase-b-quota--adopt-kueue) is the decision record. Kueue is a different risk class from the systems above: it is not a scheduler and never touches placement, it attaches at exactly the hold-flag contract this design already defines, it is owned by kubernetes-sigs, and it is productized inside GKE. Adopting it deletes the two heaviest components we would otherwise build. The working rule that falls out: adopt controllers that attach at contracts we control; do not adopt schedulers.

**Use Kueue TAS as the placement engine (no custom scheduler).** Rejected for OME's pools, accepted where the need is narrow. TAS genuinely packs a single gang into fewest domains, but it decides at admission against a snapshot and hands binding to the unconfigurable default scheduler; it is per-workload (no cross-workload co-location, no cross-replica spreading, no reservations); and it never sees standalone/non-Kueue pods. A pool that needs only "gang into one domain" can use TAS and skip ome-scheduler — the hold-flag interface allows it. OME's pools need the rest, so they build. Full analysis in [Phase B](#phase-b-quota--adopt-kueue).

**What we chose.** The official scheduler code as a library, plus our plugins, shipped as our own binary, with Kueue adopted alongside it for quota. We first scoped this minimally — placement only — then expanded: quota arrives by adoption, and the topology-aware preemption that no external system provides is built as an extra component around the same core. At no scope do we fork the scheduler core.

## Open Questions

- **Pool exclusivity policy per fleet.** R1 requires the pool to be exclusively ome-scheduled; which fleets enforce this (taint/label so only our pods land) versus run mixed with best-effort R1 is a per-fleet decision that must be made explicitly.
- **Standalone pods through Kueue.** Whether non-gang accelerator pods are admitted through Kueue is a per-fleet policy call; mixed accounting misleads both Kueue and the scheduler, so it must be decided, not defaulted.

## Appendix A: a worked example

Service `svc-a` runs prefill as a gang of 18 whole-node pods and decode as a gang of 6, on a fleet of NVL72 racks, where each domain has 18 nodes.

First, a shape check: with co-location Required, the pair needs 24 nodes in one domain, which can never fit an 18-node domain. So OME validation either rejects Required for this shape or the service uses Preferred. Assume Preferred.

OME creates the two PodGroups and all 24 pods at once. The first prefill pod triggers the domain choice: among domains with at least 18 free nodes, the fullest is picked and pinned. All 18 prefill pods are steered there, wait at the gate, and are released together. Decode's first pod prefers the pinned domain, finds no room, and spills — as Preferred allows — to the next-fullest domain with 6 free nodes.

A month later, a routine node drain tries to evict one prefill pod. The webhook refuses: that would leave 17 of 18. The operator instead triggers an OME migration, which tears down and recreates the gang as a whole; the scheduler re-plans it, and the old domain's "largest gang that fits" metric jumps back up by 18.

## Appendix B: glossary

- **Arbiter** — a component that decides whose work may start; this role is filled by Kueue (adopted), with an in-house build kept as the reversible fallback at the same hold-flag interface.
- **Best fit** — choosing the fullest domain that still fits the job, so empty domains stay whole.
- **Binding** — the API write that assigns a pod to a node.
- **ClusterQueue / cohort** — Kueue's objects for a team's quota and for groups of queues that may borrow from each other.
- **Domain** — a set of nodes wired together with fast fabric (one NVL72 NVLink clique, a multi-host TPU slice, or a labeled rack/island on other fleets); called a clique in NVIDIA's labels.
- **DRA** — Kubernetes' new mechanism for allocating devices such as GPUs; needed for GB200 fabric features.
- **Eviction** — the polite, API-driven way to remove a pod, used by drains and upgrades; can be refused by budgets and webhooks.
- **Gang** — a group of pods that must all run together or not at all.
- **Hold flag / scheduling gate** — a field on a pod meaning "do not schedule yet"; removing it is how work is admitted.
- **Kueue** — a Kubernetes-native quota and queueing system (a kubernetes-sigs project) that admits work by removing hold flags and evicts whole workloads only; owns Phase B here, run without its topology mode (TAS).
- **kwok** — a test tool that simulates thousands of fake nodes without hardware.
- **Leader election** — running two copies where one is active and the other takes over on failure.
- **minMember** — the number of gang pods that must be placed before any may start.
- **PDB (PodDisruptionBudget)** — a declared floor on how many pods of a set must remain up during voluntary disruptions.
- **Placement group** — the prefill and decode gangs of one service, treated as neighbors.
- **PodGroup** — the small object declaring gang membership and minMember.
- **Preemption** — evicting lower-priority work to make room for higher-priority work.
- **Prefill / decode** — the two cooperating halves of disaggregated LLM inference.
- **Reclaim** — taking back capacity a queue had lent out.
- **Reservation** — a claim on capacity held for a specific gang before its pods land.
- **schedulerName** — the pod field that selects which scheduler handles it.
- **Starvation** — a big job waiting forever because smaller jobs keep taking each freed node.
- **TAS** — Kueue's Topology-Aware Scheduling: admission-time topology assignment; deliberately off here (see [Phase B](#phase-b-quota--adopt-kueue)).
- **Whole-node pod** — a pod that uses every accelerator on its node, so capacity math is just counting nodes.
