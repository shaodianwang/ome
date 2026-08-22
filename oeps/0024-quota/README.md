# OEP-0024: Fleet Quota — global GPU/TPU capacity budgets

**Status:** provisional. Single-cluster is the self-contained first slice;
multi-cluster (the global budget) builds on it. Open questions marked inline and
collected at the end.

<!-- toc -->
<!-- /toc -->

**Tl;dr:** Today OME owns *no* quota. Capacity is whatever `ResourceFlavor` /
`ClusterQueue` / `LocalQueue` a cluster operator hand-provisions out of band
(OEP-0018: "the cluster operator owns this; OME assumes it"), tenancy is an
opaque `ome.io/dedicated-ai-cluster` string used verbatim as a Kueue queue name,
priority is one hard-coded class, and multi-cluster is a race with no notion of a
fleet-wide budget. A capacity admin cannot express the one sentence they most
need to say: **"team-a gets 40 `tpu7x` chips across the fleet."** This OEP adds
**one** control-plane API type — `AcceleratorQuota` — modeled directly on Kueue's own
hierarchy: **each CR is one node in a quota tree** (an internal `Cohort` grouping
or a leaf budget), linked to its parent by `spec.parentRef` and all descending from
a single reserved `root` node that holds the fleet-wide total. A fleet is a handful
of these CRs; a validating webhook **assembles and verifies the whole tree at
admission** (every non-root node has a resolvable parent, no cycle, within depth,
node-kind consistent, parent ≥ Σ children, namespaces unique within the tree it can
see) so a CR that would break the tree is rejected up front, not silently
mis-materialized. Delivery
is **two-level**: the management plane splits each node's budget into per-cluster
shares and projects `AcceleratorQuota` CRs onto each workload cluster, where a
local controller renders them into **stock Kueue** objects. The management plane
therefore never holds a Kueue credential on a member, and a member can refuse a
budget it cannot honor. Everything below the budget is native Kueue, so borrowing,
preemption, and fair-share are Kueue's job, not ours. Kueue is the **first** backend
behind `AcceleratorQuota`; the materialization layer may evolve, but the
`AcceleratorQuota` contract stays put.

> [!IMPORTANT]
> An internal node's budget is an **authoring guardrail, not an enforced cap**.
> Kueue's cohort quota is additive, so materializing it would *raise* the ceiling
> rather than lower it. See *Budget → Kueue mapping*.

## Summary

`AcceleratorQuota` is a CRD on the OME control plane, modeled on Kueue's own tree:
**one CR is one node**. `spec.role` declares which kind: a `Cohort` node groups
other nodes, and a `ClusterQueue` node is a leaf that binds serving namespaces and
carries a **per-`(resource, flavor)` budget** (a `ClusterQueue` plus a `LocalQueue` per
namespace). `spec.parentRef` names its parent CR; the fleet's whole quota tree
is the graph of those edges, rooted at a single reserved `root` node whose budget
is the fleet-wide total. A leaf also carries a **priority tier** and a
**distribution policy**. Because every node is a first-class API object, a
**validating webhook can assemble the entire tree at admission** — it lists the
existing `AcceleratorQuota`s, splices in the incoming one, and rejects it unless the
result is a well-formed tree (parent resolves, no cycle, within depth, node-kind
consistent, parent ≥ Σ children, namespaces unique within the plane's visible set —
see Open Question #6 for the residual cross-plane hole). A
**management-mode** controller then reconciles the admitted tree by (1) computing
each leaf's per-cluster share of every `(resource, flavor)` pair and (2)
server-side-applying **one `AcceleratorQuota` per matched cluster** — the leaf plus
its ancestor chain, carrying that cluster's resolved share — over OME's existing
`WorkloadCluster` connection subsystem. A **workload-mode** controller on each
member renders those projections locally: a `Cohort` per internal node, a
`ClusterQueue` per leaf, a `LocalQueue` per serving namespace. Both controllers
re-run the tree checks, but they are authoritative for different things: the hub
owns fleet containment and cross-source namespace uniqueness; the member owns
per-cluster materialization and a capacity veto. Anything that slips past a
concurrent admission surfaces as a `Degraded` condition and a **frozen** last-good
materialization — never a deletion. No OME CRD exists below the budget — the
materialized objects *are* Kueue objects. Single-cluster is the degenerate case:
one workload-mode controller, a locally-authored tree, and no management plane.
OME's InferenceService reconciler gains one **resolver** that maps an ISVC to its
leaf's `LocalQueue` + `WorkloadPriorityClass` labels, replacing today's two
divergent label stampers. Because a fleet is now many small CRs, OME also ships
**authoring tooling** (a CLI / kubectl plugin) that renders and validates the
assembled tree offline before it is committed. Capacity admins view the tree and
live usage in MUSE, which is read-only — rolled up into `AcceleratorQuota.status`
on the management plane, and drillable against `ClusterQueue.status` on a member.
Budget spec edits flow through GitOps, and status is not synced through GitOps.

**Kueue is the v1 enforcement backend, and it is optional.** Quota *management* —
declaring fleet budgets and distributing them across clusters — is the feature;
the current design *enforces* those budgets by materializing them to Kueue. Fleet
Quota is an **opt-in** OME feature, off by default and toggled on at the OME side
(a chart value / flag). Turning it on makes **Kueue a dependency**: it must be
installed on every workload cluster and OME must hold write RBAC for Kueue objects
there. Left off, OME behaves exactly as it does today. The `AcceleratorQuota` API and
the distribute→materialize split are kept deliberately separable so the backend is
not baked into the user-facing contract.

## Motivation

Five concrete failure modes, each anchored to current code:

1. **Nobody provisions quota.** No OME controller, chart, or hack script creates
   a Kueue `ResourceFlavor`/`ClusterQueue`/`LocalQueue`/`Cohort` — OME's whole Kueue
   surface is reading and stamping: the label keys at
   `pkg/constants/constants.go:544-546`, one hard-coded priority-class name at
   `:231`, and a scheme registration (`cmd/manager/main.go:40,108`) used to read
   admission state.
   OEP-0018 makes this explicit and deliberate: "Kueue installed on every
   workload cluster … the cluster operator owns this; OME assumes it"
   (`oeps/0018-multi-cluster-placement/README.md:234`). The cost: quota lives in
   hand-maintained YAML with no OME-level source of truth, no validation, and no
   view — it drifts, and a mis-sized `ClusterQueue` silently wedges a launch at
   the 30-minute admission deadline.

2. **Tenancy is an opaque string.** `SetPodLabelsFromAnnotations`
   (`pkg/controller/v1beta1/inferenceservice/utils/annotations.go:67-82`) copies the
   `ome.io/dedicated-ai-cluster` (DAC) annotation into a queue-name label with no
   interpretation — `kueue.x-k8s.io/queue-name` when the `kueue-enabled` key is also
   present, `volcano.sh/queue-name` otherwise, with a `volcano-queue` annotation
   taking precedence over both. There is no tenant object, no namespace↔queue
   binding, no hierarchy — the "tenant" is whatever `LocalQueue` the string
   happens to name. Priority is a single hard-coded class,
   `DedicatedAiClusterPreemptionWorkloadPriorityClass = "kueue-scheduling-high-priority"`
   (`constants.go:231`), applied uniformly — which also violates the repo's
   no-magic-values rule.

3. **No global budget in multi-cluster.** Placement fans an ISVC out to
   capability-matched clusters and keeps the first whose Kueue admits it
   (`pkg/controller/v1beta1/placement/{derive,admission}.go`); `WorkloadCluster`
   carries *health only, no capacity* (`pkg/apis/ome/v1beta1/workloadcluster_types.go:8-12`).
   There is nowhere to say "team-a gets 40 chips across the fleet" and nothing to
   split or rebalance that number — each cluster's quota is an independent island
   (OEP-0018 Open Question, `README.md:274`).

4. **No admin surface.** MUSE is read-only (`muse/README.md:3`) and does not even
   watch Kueue (`informers.rs` covers OME kinds, Node/Pod, and apps/v1 workload kinds
   — no Kueue types), so an admin can
   neither see quota-vs-usage nor change a budget.

5. **A budget would have no denominator.** No controller in the OME manager reads
   `Node.status.allocatable`. ome-scheduler's gangpack plugin reads allocatable through the scheduler
   framework's `NodeInfo` snapshot (`scheduler/pkg/plugins/gangpack/nodes.go:36,51`),
   but only per-node inside a scheduling cycle; the only fleet-level rollup in the tree is MUSE's, in Rust and
   read-only (`muse/crates/muse-server/src/rollup.rs`). So a fleet total is a number
   an admin types and nothing checks — over-subscribe by 3× and the tree is
   perfectly valid while the workloads never schedule.

### Goals

- **G1.** One declarative, control-plane source of truth for fleet capacity
  budgets: `AcceleratorQuota`, per tenant, per `(resource, ResourceFlavor)` pair.
- **G2.** Deterministic materialization of a budget into **native Kueue** objects
  on each workload cluster; OME owns the budget, Kueue owns admission/preemption/
  fair-share/borrowing. The management plane never holds a Kueue credential on a
  member, and a member can refuse a budget it cannot honor.
- **G2a.** Give a budget a denominator: derive each cluster's schedulable
  accelerator capacity from its own nodes and report it, so over-subscription is
  visible rather than latent.
- **G2b.** State honestly which invariants are *enforced* and which are only
  *checked at authoring time*, so nobody builds on a guarantee that is not there.
- **G3.** A single ISVC→(queue, priority) **resolver** replacing the two
  divergent stampers (DAC path + placement `stampQueue`), with the DAC annotation
  kept working as a back-compat alias.
- **G4.** Single-cluster and multi-cluster share the **same API and the same
  materialization path**: workload mode renders a tree identically whether that tree
  was locally authored or projected. Multi-cluster adds a management-mode
  distribution layer on top; it does not fork materialization.
- **G5.** A capacity-admin view in MUSE: read-only fleet capacity vs quota vs
  usage vs borrowed, per tenant and per cluster. Budget edits are GitOps, not a
  MUSE write.
- **G6.** Authoring ergonomics for a many-CR tree: a client-side tool that
  assembles the per-node CRs into the full tree, validates it (the same checks the
  webhook runs) before a GitOps commit, and can scaffold a child under a parent —
  so operators reason about the *tree*, not a scatter of `parentRef` strings.

### Non-Goals

- **Live cross-cluster borrowing.** Kueue cohorts are per-cluster; a global
  budget is enforced as the *sum of per-cluster shares*. Rebalancing across
  clusters is a reconcile cycle, not instantaneous borrowing (see *Drawbacks*).
- **A traffic/perf-aware optimizing solver** for distribution in v1. The
  distribution policy is pluggable; v1 ships only `Explicit` and `Proportional`.
- **Fungible "accelerator-equivalent" units** in v1 — budgets are raw per-flavor
  counts (Open Question #1).
- **Budgeting CPU/memory (or any non-accelerator resource).** The fleet budget is
  per-`ResourceFlavor` accelerator counts only. Each extra resource dimension
  multiplies both the containment invariant (`parent ≥ Σ children` would have to
  hold *per resource*, per flavor) and the distribution split, and in accelerated
  serving the accelerator dominates cost while CPU/memory ride the node's fixed
  accelerator ratio — so one dimension keeps the tree tractable. CPU/memory are
  still *covered* on the materialized `ClusterQueue` so admission works (see
  *Budget → Kueue mapping*); they are simply not something an admin budgets.
- **Node/capacity provisioning** (turning cloud reservations into nodes). That is
  a layer below quota — capacity-management territory, not OME.
- **Replacing Kueue's admission, preemption, or fair-share.** We generate Kueue
  objects and read Kueue status; we do not reimplement the scheduler.

## Proposal

### Terms (defined before use)

- **ResourceFlavor** — a Kueue `ResourceFlavor` (e.g. `tpu7x`, `gb300`): a hardware class
  identified by node labels + tolerations. OME references flavors; it does not
  own node labeling (Open Question #4).
- **Quota node** — one `AcceleratorQuota` CR, one position in the tree, its kind
  declared by `spec.role`. Called just "node" where the context is the tree;
  a bare `Node` in code font always means the Kubernetes object. A **`Cohort` node** groups other nodes (maps to a Kueue `Cohort`); a
  **`ClusterQueue` node** is a leaf that binds one or more serving namespaces, carries the
  per-`(resource, flavor)` budget, and is the unit a workload charges against (maps to a Kueue
  `ClusterQueue` plus a `LocalQueue` per namespace). `spec.parentRef` names the
  parent CR; the single reserved `root` node is the only one with no `parentRef`.
- **Global budget** — a leaf's fleet-wide allowance of one `(resource, flavor)`
  pair (`team-a: 40 google.com/tpu on tpu7x`): the sum it may consume across all
  clusters.
- **Share** — the per-cluster slice of a global budget the management plane assigns
  to one cluster; becomes that cluster's `ClusterQueue` `nominalQuota` for the pair.
- **Matched cluster** — a registered `WorkloadCluster` a leaf may run on. It is
  **derived, never selected**: under `Explicit` the matched set is exactly the
  clusters named in `perCluster[]`; under `Proportional` it is the clusters with
  non-zero capacity for the budgeted `(resource, flavor)` pair. Only matched
  clusters receive a projection and only they are apportioned a share, so an
  unmatched cluster is not "zero" — it is absent. There is deliberately no
  cluster-selector field; see *Why the matched set is derived*.
- **Management plane** — the OME manager running `--multicluster-role=control-plane`
  *and* quota-management mode. It holds the authored tree and the `WorkloadCluster`
  connections, computes shares, and projects them. It writes **no** Kueue object
  anywhere.
- **Workload plane** — the OME manager on a serving cluster running quota-workload
  mode. It is the **only** thing that writes Kueue objects, and only on its own
  cluster.
- **Source** — an `AcceleratorQuota` a human authored: the fleet tree on the
  management plane, or the local tree on a single-cluster install.
- **Projection** — an `AcceleratorQuota` the management plane wrote onto a workload
  cluster, carrying that cluster's already-resolved share. A projection is *never*
  a source: no controller derives anything further from it, which is what makes the
  two-level design terminate.
- **Derived root** — the reserved root node on a workload cluster, whose
  `status.capacity` the local controller computes from that cluster's own nodes.
  Derived means **status-only**: the local controller never writes root `spec`, so
  a human's fleet total and a machine's observation can never overwrite each other.
- **Freeze** — stop writing a node's output and leave the last-good version
  serving. Distinct from *unmaterialize*, which deletes it. The difference is a
  tenant outage.
- **Resolver** — the ISVC-reconciler behavior that stamps `queue-name` +
  `priority-class` labels from a workload's leaf node.

### The one API type: `AcceleratorQuota`

One CRD *type* is the entire new API surface, and **one CR is one node** of the
quota tree — the same shape as Kueue's own `Cohort`/`ClusterQueue` split, lifted to
the fleet. A node points at its parent with `spec.parentRef`; the tree is the graph
of those edges, not a path string. `spec.role` declares what the node *is* — a
`Cohort` grouping or a `ClusterQueue` leaf — rather than leaving it to be inferred from
structure. A fleet is a handful of these CRs — one per grouping and one per leaf
budget — which is what lets a validating webhook treat the whole tree as data and
verify it at admission (see *Assembling the tree*). Sketch (full schema in Design
Details):

```yaml
# an internal node: a grouping Cohort holding the number its children must fit in
apiVersion: ome.io/v1beta1
kind: AcceleratorQuota
metadata: {name: org}                      # this node's identity; children target it by name
spec:
  role: Cohort                             # grouping node
  parentRef: {name: root}                  # top-tier grouping under the reserved fleet root
  budgets:                                 # authoring guardrail; never materialized
    - resourceName: nvidia.com/gpu
      resourceFlavor: gb300
      nominal: 100
---
# a leaf node: binds namespaces, carries the budget (ClusterQueue)
apiVersion: ome.io/v1beta1
kind: AcceleratorQuota
metadata: {name: team-a}
spec:
  role: ClusterQueue                       # leaf node
  parentRef: {name: org}                   # edge to the parent node
  namespaces: [team-a-prod]                # leaf → one LocalQueue per namespace, per cluster
  priorityTier: production                 # default WorkloadPriorityClass; workloads may override
  distribution:
    policy: Proportional                   # this node's default; Explicit = admin sets perCluster
  budgets:
    - resourceName: nvidia.com/gpu         # the resource the count applies to
      resourceFlavor: gb300                # the hardware class it applies on
      policy: Explicit                     # optional; overrides this node's default for one entry
      nominal: 60                          # global allowance; split follows the policy
      borrowingLimit: 16                   # optional; burst up (Kueue borrowingLimit)
      lendingLimit: 20                     # optional; release when idle (Kueue lendingLimit)
      perCluster:                          # required iff effective policy == Explicit
        - {cluster: cluster-a, nominal: 36}
        - {cluster: cluster-b, nominal: 24}
status:
  parent: org
  budgets:
    - {resourceName: nvidia.com/gpu, resourceFlavor: gb300, nominal: 60,
       admitted: 44, borrowed: 4,
       perCluster: [{cluster: cluster-a, nominal: 36, admitted: 30}, ...]}
```

Below this object there is **no OME CRD** — the workload-mode controller writes stock Kueue
`Cohort`, `ClusterQueue`, and `LocalQueue`.

### Assembling the tree from `parentRef`

The tree is the graph of `parentRef` edges across all `AcceleratorQuota`s — a node in
one CR, its parent in another. `parentRef` is single-valued and **exactly one node
is parent-less: the reserved `root`**, which every other node descends from — so the
fleet is a **single tree**, not a forest. Because every node is a cluster-scoped API
object, this graph is fully visible **at admission time**: the validating webhook lists the
existing `AcceleratorQuota`s, splices in the object under review, and verifies the
result is a well-formed tree before admitting it. A CR that would break the tree is
**rejected**, not silently mis-materialized:

- **Dangling parent** — `parentRef` names a node that does not exist. Every node
  except the reserved `root` carries one, and parents are created before children
  (top-down); the tool in *Authoring the tree* orders this.
- **Cycle** — following `parentRef` from the node returns to itself.
- **Depth** — the node's distance from `root` exceeds `maxTreeDepth`.
- **Node-kind violation** — the structure contradicts the declared `spec.role`: a
  `parentRef` targets a `role: ClusterQueue` node (leaves take no children), or a
  `role: Cohort` node carries `namespaces`/`priorityTier`. A node is *either* a
  `Cohort` *or* a `ClusterQueue`, never both; `role` is declared, and a
  `ClusterQueue`→`Cohort` change is admitted only on a drained leaf (see *Node kind*).
- **Containment** — a parent's `nominal` for some `(resource, flavor)` pair is less than the sum of its
  children's (including the incoming one).
- **Namespace collision** — a namespace bound by this leaf is already bound by
  another leaf in the tree this plane can see. This is *not* a fleet-wide
  guarantee; see Open Question #6.

Admission is fast feedback but not a serialization point: two children admitted
concurrently can each pass containment yet together bust the parent, and a parent
can be deleted out from under a child. So the **controller re-runs every one of
these checks at reconcile** — a violation it finds (a race, an out-of-order edit, a
deleted parent) sets a `Degraded` condition + event on the offending node and
**freezes** that subtree at its last-good materialization, rather than writing a
broken Kueue tree. Freezing, not deleting: see *Freeze, and the deletion paths* for
why the literal alternative turns a cordoned rack into a fleet-wide outage. The
webhook keeps the common mistake out; the controller keeps the invariant from being
acted on once broken.

The two planes are authoritative for **different** invariants, and it is worth
being precise since their inputs differ: the management plane owns fleet
containment and cross-source namespace uniqueness, because only it sees the whole
authored tree; the member owns per-cluster materialization and the capacity veto,
because only it sees its own silicon. "The same checks on both planes" is not quite
true once a member's numbers are shares rather than fleet totals.

### Single-cluster flow (the must-have slice)

1. Admin declares the `AcceleratorQuota` nodes — the reserved `root`, groupings, and
   leaves — directly on the serving cluster. There is no management plane, no
   registry, and no projection: workload mode alone. A locally-authored source
   resolves to itself, so `nominal` **is** this cluster's share, and
   `spec.distribution` and `budgets[].perCluster` are rejected by
   the webhook (there is no fleet to split across, and silently ignoring them would
   let an admin believe a split is happening). The root's `spec.budgets` is
   admin-authored here — it is the only containment top available — while
   `status.capacity` stays controller-written.
2. The workload-mode controller materializes, on that cluster: a `Cohort` tree
   mirroring the `parentRef` edges among internal nodes (no `resourceGroups` — see
   *Budget → Kueue mapping*); one `ClusterQueue` per leaf node (`nominal` →
   `nominalQuota`; `borrowingLimit`/`lendingLimit` passed through); one
   `LocalQueue` per listed namespace. `WorkloadPriorityClass`es referenced by
   `priorityTier` are referenced, not created.
3. The resolver stamps each ISVC's engine pods with the tenant's `LocalQueue`
   name and priority class.
4. Kueue admits/gates as usual; OME reads `InstanceStatus.Admitted` (unchanged).

That is a complete quota system on one cluster with **only** the `AcceleratorQuota`
object added — everything else is native Kueue.

### Multi-cluster flow (the global budget)

Same object; the registry now has N clusters. Delivery is **two-level**: the
management plane projects `AcceleratorQuota` CRs — its own CRD, nothing else — and
each member renders them into Kueue locally.

Options considered:

- **(a) A control-plane distributor writes Kueue objects on each member directly.**
  *Tradeoff:* the simplest data path — one controller, no new objects on the member
  — but it needs cluster-wide `kueue.x-k8s.io` **write** on every member, and the
  member gets no say in what lands.
- **(b) The management plane projects `AcceleratorQuota` CRs; the member renders
  them.** *Tradeoff:* two controllers and a second copy of every node on each
  member, plus an eventual-consistency window (a) does not have. In exchange the
  hub holds no Kueue credential and the member can refuse a budget through its own
  webhook.
- **(c) The member pulls the tree from the hub.** *Tradeoff:* inverts the
  connection direction, which the `WorkloadCluster` transport does not support;
  needs a new inbound API and per-member credentials pointing the other way.

We choose **(b)**. The extra copy is the price of the member's veto, and it is a
cheap copy: a fleet is a handful of nodes, not thousands.

The management plane, for each leaf node and `(resource, flavor)` pair:

1. Computes per-cluster **shares** from the entry's effective policy:
   - `Explicit` — take `perCluster[]` verbatim (must sum to `nominal`).
   - `Proportional` — split `nominal` in proportion to a **snapshot** of each
     cluster's allocatable capacity of that pair, read from that cluster's derived
     root (see *Derived capacity*). The snapshot is deliberately static: shares
     change only when the budget is reconciled, not continuously as capacity
     drifts (Open Question #2).
2. Server-side-applies **one `AcceleratorQuota` per matched cluster** — the leaf
   plus its full ancestor chain — over the existing `WorkloadCluster` connection
   subsystem (`pkg/controller/v1beta1/workloadcluster/remoteclient.go`). Each
   projection carries *that cluster's* resolved `nominal`.
3. The member's workload-mode controller validates the projection with its own
   webhook and renders the local Kueue tree.
4. Usage flows back: the member rolls `ClusterQueue.status.flavorsUsage` up its
   local tree; the management plane reads it into `status.budgets[].perCluster[]`
   on the source node.

Placement is unchanged in mechanism (still fan-out + first-Kueue-to-admit-wins),
but the per-cluster quota it races against is now OME-managed and provably sums
to the global budget.

#### Residual authority: what two levels does *not* buy

The hub does not become harmless, and the design should not be sold as if it does.

Materialized Kueue object names come from the node's `metadata.name`. A management
plane holding cluster-scoped `AcceleratorQuota` write on a member can therefore
cause that member's controller — which *does* hold cluster-wide Kueue write — to
create a `ClusterQueue` or `Cohort` of **any name**, including that of a
hand-provisioned queue OME does not own. That is a confused deputy, and it survives
the split. Two things bound it, and both must be built:

- The materializer MUST refuse to **adopt** a pre-existing Kueue object that does
  not already carry the ownership labels; it sets `Degraded`, reason
  `NameConflict`, and writes nothing. "Never touches unlabeled objects" (see
  *Ownership & lifecycle*) is a filter on *reads*; this is the stronger rule needed
  on *creates*.
- The member's webhook applies the veto below, so the hub cannot demand an
  arbitrary number.

The accurate accounting: the hub loses the Kueue credential and gains a
name-shaped influence the member can refuse. A real reduction, and a smaller one
than "it only writes one CRD" would suggest.

#### What the member rejects

A veto is worth nothing without a predicate. The member's webhook denies a
projection when:

1. **Over-capacity.** Σ of projected leaf shares for a `(resource, flavor)` exceeds
   the derived root's capacity for that pair by more than a configured
   `overcommitPercent`. Absent that config the check does not run — deliberate
   oversubscription is legitimate, so this is opt-in.
2. **Unknown flavor.** The `resourceFlavor` names no `ResourceFlavor` on this
   cluster. Rendering anyway produces an *inactive* `ClusterQueue`
   (`pkg/cache/clusterqueue.go:281-283`), which is worse than a rejection: the
   tenant silently cannot admit anything.
3. **Foreign projector.** See *Projection contract*.

Note the asymmetry with freezing: capacity *shrinking* below an already-rendered
budget is absorbed by hysteresis and revokes nothing, but capacity that is
*insufficient for a new projection* is refused at admission. Rejecting a write is
cheap; revoking a running tenant's queue is not.

The management plane records the member's denial as a per-cluster condition on the
source node, so the fleet admin sees the refusal without reading member logs.

#### Apportionment, with freezing folded in

A naive "largest-remainder over all matched clusters" contradicts "never rebalance
a frozen cluster's share". The rule that satisfies both:

1. Partition clusters into **pinned** and **live**. Pinned = frozen, unreachable,
   **or newly unmatched but still serving** — a cluster that dropped out of the
   selector keeps its `ClusterQueue` under the freeze-don't-reap rule, so its share
   must stay accounted for or the fleet double-books. Live = matched and reporting.
2. Pinned clusters keep their existing share verbatim. Subtract those shares from
   the fleet `nominal`, and remove their capacity from the `Proportional`
   denominator.
3. Apportion the remainder over live clusters by largest-remainder with a
   deterministic tiebreak, so `Σ shares == nominal` holds **exactly** rather than
   to within rounding. Naive per-cluster rounding loses or invents chips; at fleet
   scale that is a real number.

If pinned shares alone meet or exceed `nominal`, live clusters apportion to zero
and the source node goes `Degraded` with reason `SharesExhausted`. That state is
reachable during a long partition and must not be silent.

**Bootstrap.** The first `Proportional` computation is the normal case, not an edge
case, and has no capacity to work from: shares derive from each member's derived
root, which exists only once that member's controller has started. A matched
cluster that has never published capacity is **`Pending`** — excluded from the
denominator, given zero share, and reported as such.

That forces a precision on the freeze rule. Stated exactly: **a pinned cluster's
share is never reduced.** Live clusters' shares move freely — granting a first share
to a newly-reporting cluster necessarily shrinks its peers', and that is the tree
converging, not a partitioned cluster being robbed. What is forbidden is
redistributing the share of a cluster that cannot currently be told about it.

### Derived capacity: giving a budget a denominator

An admin authoring "team-a gets 40 `tpu7x` chips" has no in-cluster answer to "out
of how many?" No controller in the OME manager reads `Node.status.allocatable`;
ome-scheduler's gangpack plugin reads it through the scheduler framework's
`NodeInfo` snapshot (`scheduler/pkg/plugins/gangpack/nodes.go:36,51`), but only
per-node inside a scheduling cycle, and the only fleet-level rollup in the
tree is MUSE's, in Rust and read-only. So `root`'s total is a number an admin types
and nothing checks: over-subscribe by 3× and the tree is perfectly valid while the
workloads never schedule.

Each workload cluster's controller therefore maintains its reserved root locally
and writes `status.capacity` from its own `Node` objects. Availability follows
MUSE's predicate — `Ready=True && !spec.unschedulable`
(`muse/crates/muse-server/src/rollup.rs:280-300`) — deliberately **not** the
scheduler's predicate (`scheduler/pkg/plugins/gangpack/nodes.go:150-158`), which is
pod-relative: it matches taints against a *specific pod's* tolerations and has no
meaning for a pod-free capacity sum. Applied with an empty toleration set it would
report zero on a fully-tainted TPU cluster.

Listing nodes is nearly free in workload mode: the manager's cache is unfiltered for
Nodes (only Pods are label-scoped, `cmd/manager/main.go:335-343`), the BaseModel
reconcilers already register a `Node` informer
(`pkg/controller/v1beta1/basemodel/controller.go:156-162`), and `nodes:
get/list/watch` is already granted (`config/rbac/role.yaml:36-44`). The event wiring
is **not** free: that existing watch carries a delete-only predicate
(`basemodel/backends/pernode/watches.go:20-26`), so capacity derivation needs its own
Update-passing watch or a resync tick. Those controllers are also **off** under the
control-plane role — a second reason capacity derivation belongs on the member.

**Status-only is load-bearing.** On the management cluster `root.spec` is the
admin's fleet total; on a member `root.status.capacity` is that cluster's silicon.
If the local controller could write `root.spec`, a projection and a local
observation would fight over one object forever. They cannot, because they write
different halves. The fleet capacity view lives on the root `AcceleratorQuota`'s
`status.capacity[].perCluster[]`, read back from each member — **never** on
`WorkloadClusterStatus`, which is health-only by explicit design
(`pkg/apis/ome/v1beta1/workloadcluster_types.go:8-12`). `TODO:` that comment's next
clause says a placer "reads capacity live from the cluster's own Kueue", which this
design replaces — reword `workloadcluster_types.go:10-11` when derived capacity
lands. That is a judgment call
rather than an obvious win: the hub does now know about capacity, just via a
different object. The reason to prefer it is that `WorkloadCluster` is the
connection registry the placer reads on every decision, and a frequently-changing
number does not belong there.

**A projected member's root carries no budget; a single-cluster root does.** The
distinction is who authored the tree, not which mode is running:

- *Member receiving projections.* The local controller creates the root with
  `role: Cohort` and **no** `spec.budgets`, and containment is a hub-side invariant
  — the member's numbers are shares, so comparing them against a fleet total would
  be meaningless.
- *Single-cluster install.* The admin authors the root's `spec.budgets`; it is the
  only containment top available, and the local webhook checks containment against
  it exactly as a hub would. This is the one case where a workload-mode webhook runs
  the containment check.

In both cases `status.capacity` stays controller-written. A
node carrying no budget entry for a `(resource, flavor)` is *unconstrained* for
containment on that pair — otherwise the re-check on every member would compare
projected shares against an empty parent and freeze the fleet on first reconcile.
On a member the meaningful comparison is `Σ leaf share ≤ root.status.capacity`,
which is not containment: it is the veto above at admission, and advisory
(`Degraded` + hysteresis) at reconcile.

**Two writers must be prevented at cluster scope, not process scope.** A process
may not run both quota modes — a hard startup reject in the style of
`cmd/manager/main.go:457-461` — which rejects an invalid `--multicluster-role` value,
so it is the precedent for fatal-on-bad-flag, not for mode mutual exclusion, which is
new. That check is process-scoped while the hazard is
cluster-scoped: two Deployments in one cluster, or a cluster registered as its own
`WorkloadCluster`, reach the same last-writer-wins outcome on `root.status`. So
additionally, the root's status writer holds a named lease, and a `WorkloadCluster`
endpoint resolving to the local apiserver is rejected at reconcile.

### Freeze, and the deletion paths

A node that violates a **computed** invariant must not have its subtree deleted.
Trace the failure: an operator cordons a rack → the derived root's allocatable
drops → the authored budget now exceeds observed capacity → containment "fails" →
the subtree is unmaterialized → **every tenant's `ClusterQueue` is deleted and
admission stops fleet-wide**, mid-maintenance, because someone drained a rack.

So the controller **freezes**: stops writing that node's output, keeps the
last-good version serving, records what it froze at, sets `Degraded`, emits an
Event. And a shrinking *derived* capacity does not trip the check at all —
comparison is against a **high-water mark** with a configurable hysteresis band, so
a drain, a cordon, or a device-plugin restart (which transiently reports
`allocatable: 0`) is absorbed. The API carries `highWaterMark` and the band already, but no hysteresis *helper* is
implemented — the closest is the traffic-specific `TrafficWeightDeadbandPercent` — so
the logic is new code, and per the repo's no-magic-values rule its band is a config field.

**Freeze propagates down, but only to new output.** Existing descendants keep their
last-good materialization; no *new* descendant materializes under a frozen
ancestor; status and usage observation continue while output writes stop. Usage
must keep flowing or MUSE shows stale numbers for a very much alive tenant.

**A missing ancestor `Cohort` is a silent trap, not an error.** If a leaf's
`spec.cohort` names a `Cohort` object that is not there, Kueue does not complain —
it substitutes an implicit default cohort with no parent and no borrow/lend limits
(`apis/kueue/v1beta1/clusterqueue_types.go:63-65`, implemented at
`pkg/hierarchy/manager.go:94-96`). The topology quietly flattens and the borrowing
scope changes with no signal.

Top-down materialization keeps this out of the *new*-subtree path. The way in is
re-parenting: moving a live leaf under an ancestor that is frozen and never
materialized is an in-place update of an existing `ClusterQueue`, not new output, so
the "no new descendant under a frozen ancestor" rule does not catch it. The
materializer MUST therefore check the ancestor `Cohort` exists on every write, not
only on create, and surface its absence as `Degraded`, reason `CohortMissing`.

**Deletion paths, enumerated.** "Only via the finalizer" is too glib; there are
four, each with its own rule:

| Path | Rule |
|---|---|
| Explicit deletion of a **source** | The hub's finalizer deletes that node's projection on every matched member; each member's GC then reaps its Kueue objects. The hub itself reaps no Kueue object — it holds no credential to. The intended path. |
| Explicit deletion of a **projection** | The member's OwnerReference GC (or its finalizer) reaps that node's Kueue objects. The hub re-creates the projection unless the source is gone too. |
| Projection whose `origin-uid` no longer matches the live source | Reap the projection — a leftover from a deleted-and-recreated source. Scoped by origin. |
| A cluster stops matching (selector change, `WorkloadCluster` deregistered) | **Freeze, do not reap.** Deregistering a cluster is an operator action about *connectivity*; silently stopping a tenant's admission there is not implied by it. `TBD:` an explicit reap verb for the deliberate case. |

The same rule covers an unreachable member: **freeze the projection, never
rebalance it away.** Rebalancing a partitioned cluster's share onto its peers
double-books the fleet the moment the partition heals.

### Muse: the capacity-admin view

MUSE is **read-only** in v1, and which plane it runs on decides what it can see.

On the **management plane** there are no Kueue objects at all, so the fleet panel
reads entirely from the source `AcceleratorQuota.status` the hub has already rolled
up — `capacity[]`, `budgets[].nominal/admitted/borrowed`, and their `perCluster[]`
breakdowns. That is the "Quota" panel: **capacity vs quota vs used vs borrowed**,
per tenant and per cluster, with no new accounting and no cross-cluster join.

On a **member**, MUSE may additionally watch Kueue `ClusterQueue`/`Cohort`/
`LocalQueue` (gated on CRD presence, like the existing OME informers) and join them
in `rollup.rs` against the per-namespace usage it already computes, for drill-down
into one cluster. Usage/borrowing is read directly from
`ClusterQueue.status.flavorsUsage`.

Editing a budget is **not** a MUSE write: `AcceleratorQuota` is the declarative source
of truth, so changes flow through GitOps like any other cluster config. The exact
write mechanism (which repo/path, review flow) is out of scope for this OEP.
Having `AcceleratorQuota` as the declarative source of truth is what keeps that clean:
admins edit a handful of OME nodes, not a scatter of raw `ClusterQueue` YAML across
N clusters.

### Authoring the tree

One-CR-per-node buys admission-time validation and per-node RBAC/status, but it
shifts the failure mode from "one giant document is hard to read" to "the tree is
easy to get wrong one `parentRef` at a time." OME ships a client-side **authoring
tool** (a CLI / `kubectl` plugin) so operators reason about the *tree*, not the
edges:

- **Render** — assemble the per-node CRs (from a directory or a live cluster) into
  the full tree and print it: parent/child structure, per-`(resource, flavor)` `nominal` vs. Σ
  children, depth, and each leaf's namespaces.
- **Validate (dry-run)** — run the *same* checks as the admission webhook offline,
  against a proposed change, before it reaches the cluster or a GitOps PR — so a
  containment or node-kind violation is caught at author time, not at `apply`.
- **Scaffold** — generate a child CR under a named parent (wiring `parentRef` and
  the parent's flavor set), and optionally expand a single compact "whole-tree"
  description into the individual per-node CRs a GitOps repo stores.

The tool is pure client-side validation and code-gen: it holds no privilege and is
not on the admission path — the webhook and the controllers remain the authority. This
keeps MUSE read-only while still giving admins a way to build and check the whole
tree without hand-tracking every edge. The exact surface (standalone binary vs.
`kubectl ome quota …` plugin) is an implementation detail; the requirement is that
the tree is authorable and checkable as a whole.

## Design Details

### `AcceleratorQuota` schema (v1)

- `spec.parentRef {name}` — the parent node's CR name; omitted **only** by the
  single reserved `root`. The tree is the graph of these edges (mirrors Kueue's
  `Cohort.spec.parent`).
- `spec.role` ∈ `{Cohort, ClusterQueue}` — what the node **is**, declared not
  inferred. `Cohort` = an internal grouping node; `ClusterQueue` = a leaf that
  carries the budget and binds namespaces. Structure is validated *against* this
  field, which is **mutable only on a drained leaf** (see *Node kind*).
- There is **no** cluster-selector field; a leaf's matched set is derived from
  what it already declares (see *Why the matched set is derived*).
- `spec.namespaces[]` — `ClusterQueue` nodes only; each becomes a `LocalQueue` in that
  namespace, on every **matched** cluster, bound to this node's `ClusterQueue`.
- `spec.priorityTier` — `ClusterQueue` nodes only; the **default**
  `WorkloadPriorityClass` the resolver stamps on the leaf's workloads. It is a
  default, not a partition: a workload may declare its own priority, and the leaf's
  `ClusterQueue` admits mixed-priority workloads (Kueue orders and preempts within
  the queue by priority), so one budget serves many priorities.
- `spec.distribution.policy` ∈ `{Explicit, Proportional}` (pluggable; `Solver` is a
  future value) — this node's **default** policy. `Proportional` reads allocatable
  capacity per `(resource, flavor)` from each member's derived root
  `AcceleratorQuota.status.capacity` (a snapshot — see *Derived capacity* and Open
  Question #2). There
  is no single fleet-wide default object; each node states its policy. An unset
  value falls back to the operator-level default in the `quota` config
  (`defaultDistributionPolicy`), and absent that too the node reports `Degraded`
  rather than guessing a split — consistent with absent-config-disables.
- `spec.budgets[] {resourceName, resourceFlavor, policy?, nominal, borrowingLimit?,
  lendingLimit?, perCluster?}` — the allowance for one `(resource, flavor)` pair,
  which is how Kueue keys quota. `resourceName` is the Kubernetes resource the
  count applies to (e.g. `google.com/tpu`); `resourceFlavor` references an existing
  Kueue `ResourceFlavor` by name (OME does not own node labeling, Open Question
  #4). An optional per-entry `policy` overrides the node default at
  **`(node, resource, flavor)`** granularity — the same granularity as
  `perCluster` — so a leaf can be `Explicit` on a reserved flavor and
  `Proportional` on a fungible one. `perCluster[] {cluster, nominal}` is required
  iff the entry's effective policy is `Explicit`, and rejected otherwise.
- `status` reports:
  - `parent` and `path` — the resolved edge and the computed root-to-node path,
    for humans.
  - `budgets[] {resourceName, resourceFlavor, nominal, admitted, borrowed,
    effectiveCeiling?, perCluster[]}` — the resolved allowance and the usage rolled
    up from each cluster's `ClusterQueue.status`. `perCluster[]` is the single
    per-cluster rollup and is populated only where the tree is authored.
    `effectiveCeiling` appears only for a bursting leaf (see *Burstable tenants*).
  - `capacity[] {resourceName, resourceFlavor, allocatable, highWaterMark,
    observedAt, perCluster[]}` — **root node only**. Written by the local
    controller on a member from its own nodes; aggregated per cluster on the
    management plane (see *Derived capacity*).
  - `sourceGeneration` — projections only; the source revision this materialization
    reflects (see *Projection contract*).
  - `materialization {frozen, frozenAt, reason, lastAppliedGeneration}` — the
    freeze bookkeeping that makes last-good behavior survive a restart.
  - `clusters[] {cluster, appliedGeneration, appliedTime, materializedGeneration,
    message}` — management plane only; per-cluster projection progress, and where a
    member's veto message surfaces.
  - `observedGeneration` and conditions (`Ready`/`Degraded`/`Materialized`); a node
    that violates a cross-CR invariant a concurrent admission missed is marked
    `Degraded`.

  A node's `metadata.name` is additionally capped at 63 characters by a root CEL
  rule, because it is copied into a label value on every Kueue object the node
  materializes and a label value may not exceed that. The cap is at the schema root
  deliberately: `removecrdvalidation` strips the spec and status schemas from the
  minimal CRD variant, so a root rule is the one structural check that survives
  there.

**Invariants.** Because every node is a cluster-scoped object, the validating
webhook sees its plane's whole tree and enforces these at admission; the controller
re-checks the ones it is authoritative for at reconcile, for the races and
out-of-order edits
admission cannot serialize (see *Assembling the tree from `parentRef`*).

- **Tree (single-rooted).** Exactly one node is parent-less — the reserved
  `root` — and every other node MUST set `parentRef` to an existing node, forming no
  cycle; the fleet is therefore **one tree**, not a forest. A second parent-less
  node, or any node other than `root` created without a `parentRef`, is rejected.
  `root` is authored like any other node and its per-`(resource, flavor)` `nominal` is the
  fleet-wide total, so containment chains top to bottom: `root` ≥ Σ top-tier ≥ … ≥
  the leaves (a parent's `nominal` per `(resource, flavor)` MUST be ≥ the sum of
  its children's — the standard hierarchical-quota invariant). This is an
  **authoring** invariant: it is rejected at admission and re-checked at
  reconcile, but nothing below the budget enforces it at runtime, because Kueue
  cohort quota is additive (see *Budget → Kueue mapping*).
- **Identity.** A node is identified by its CR `metadata.name`, which is globally
  unique because `AcceleratorQuota` is cluster-scoped — and that name is what the
  materializer uses for the Kueue object. Because that name is fleet-unique,
  reusing a short label under two parents requires two distinct CR names
  (`org1-team-b`, `org2-team-b`) — there is no per-parent namespace in which a bare
  `team-b` could appear twice, and the second `apply` would simply collide.
  `status.path` reports ancestry for humans and carries **no** uniqueness semantics.
  Kueue object names are deliberately **not** derived from
  ancestry: a path-derived name would rename — and therefore delete and recreate —
  a live `ClusterQueue` whenever a node is re-parented (see *Restructuring the
  tree*).
- **Disjoint subtrees.** Sibling subtrees MUST NOT overlap: no node and no
  namespace is shared between two of them — two similarly-labelled tenants under
  different parents are two distinct CRs with distinct names (see *Identity*), not a
  shared node. This is a
  consequence of the rules above, not a separate check — node-disjointness follows
  from each node being a distinct CR with a single parent (a subtree cannot hang
  under two parents; a DAG is impossible), and namespace-disjointness from the
  fleet-wide *Namespace binding* invariant. Note this is **structural** overlap
  only: two top-tier subtrees MAY target the same `ResourceFlavor` on the same
  cluster — that is how `root`'s capacity is divided among them (the materializer
  sums every leaf's share per `(resource, flavor)` per cluster), not an overlap.
- **Depth.** A node's distance from `root` MUST NOT exceed a configurable
  maximum, `maxTreeDepth`, supplied via the `quota` block of
  `inferenceservice-config` (the OME chart ships 5; there is no in-code default, and
  an unset value disables the check — see *Where it runs*). Operators
  who need deeper hierarchies can raise it (e.g. 10), with a cost: the materializer
  builds the `Cohort` tree **top-down** — a parent `Cohort` must exist before its
  children reference it, or Kueue silently substitutes its implicit default cohort
  (see *Freeze, and the deletion paths*) — so convergence takes one pass per level. The cap is a guardrail against accidental sprawl, not a hard Kueue
  limit.
- **Node kind.** A node is *either* a grouping *or* a leaf, never both — Kueue
  `ClusterQueue`s cannot have child `ClusterQueue`s. `spec.role` **declares** which,
  and the webhook validates the structure against it rather than inferring kind
  from whether children happen to exist:
  - `role: Cohort` MAY carry `budgets` — the parent number containment is checked
    against, an authoring guardrail that never materializes (see *Budget → Kueue
    mapping*) — but MUST NOT carry `namespaces`/`priorityTier`, since no workload
    binds to a `Cohort`; nor `distribution`, since only a leaf's budget is split
    across clusters; nor `borrowingLimit`/`lendingLimit`, which have no meaning on a
    quota that is never materialized.
  - `role: ClusterQueue` MUST carry ≥1 `(resource, flavor)` budget entry and MUST NOT be named as any other
    node's `parentRef`. **Adding a child under a `ClusterQueue` node is rejected**; to
    place workloads at a grouping level, add an explicit `ClusterQueue` leaf child under
    it (a sibling of the other children) instead.
  - `role` is **declared, not inferred**, so the bootstrap case is unambiguous — a
    grouping created before its first child is still a `Cohort`, not an empty leaf —
    and no third party can silently convert a leaf into a grouping by adding a child.
    It is **mutable only under a guard**: a `ClusterQueue`→`Cohort` change (a leaf
    growing sub-projects) is admitted only when the leaf is **drained** (its
    `ClusterQueue` has no admitted workloads and binds no namespaces), because the
    conversion deletes the underlying `ClusterQueue`; a change on a live leaf is
    rejected, and the zero-downtime path is the grouping-above pattern in
    *Restructuring the tree*. `Cohort`→`ClusterQueue` is likewise rejected while the
    node still has children.
- **Namespace binding.** Each namespace maps to **exactly one** `ClusterQueue` node (a
  leaf may list many namespaces, but a namespace belongs to one leaf), or the
  resolver cannot decide which queue a workload in that namespace charges. Each
  plane rejects a collision **within the set it can see**; the cross-plane case — a
  projected leaf on one cluster, a locally-authored leaf on another — is caught only
  at reconcile, as `Degraded`. Open Question #6. A
  consequence: `LocalQueue`s exist **only at leaves**, so no namespace is ever
  bound at both a grouping and its descendant; sibling leaves under one grouping
  each get their own `LocalQueue`s in their own namespaces.

### Why the matched set is derived

Every leaf needs a matched set — the clusters the management plane projects to and
apportions over. The obvious way to express it is a label selector over
`WorkloadCluster`, mirroring `spec.placement.clusterSelector` on an
InferenceService. We deliberately do not, because under both policies the set is
already implied, and a second independent source could disagree with it.

- Under **`Explicit`**, `perCluster[]` names the clusters *and* their shares. A
  selector would be pure duplication, and the two could contradict each other.
- Under **`Proportional`**, shares split in proportion to each cluster's
  allocatable capacity for the budgeted `(resource, flavor)` pair. A cluster
  without that flavor has zero capacity for it and therefore takes a zero share —
  the arithmetic excludes incapable clusters without being told to.

Deriving is also *safer* than selecting. A selector saying `region=eu` on a leaf
budgeting `tpu7x` can match a cluster with no TPUs at all. That cluster is then
apportioned a share it can never use, and because shares must sum to `nominal`,
those chips are subtracted from the fleet total and stranded — a quiet loss of
budget with no failure surfaced anywhere. A capacity-derived set cannot reach that
state.

The cost is real and worth stating: **`Proportional` across a deliberate *subset*
of capable clusters is not expressible.** An admin wanting "split team-a across the
EU clusters only, proportionally" has two routes — switch that leaf to `Explicit`
and state the split, losing the automatic proportioning; or model the restriction
as a `ResourceFlavor` dimension, which is arguably its right home, since a flavor
is already "a hardware class identified by node labels" and residency is a node
label. `TBD:` revisit if the `Explicit` workaround proves onerous.

Note this is a *quota* concern only. Restricting where a workload runs is
placement's job and `spec.placement` already expresses it; the matched set decides
only where a budget is held.

### Budget → Kueue mapping

The concrete translation:

| AcceleratorQuota | Kueue object |
|---|---|
| `role: Cohort` node | `Cohort` (`v1alpha1`; `spec.parent` = `parentRef.name`), **no `resourceGroups`** |
| `role: ClusterQueue` node | `ClusterQueue` (`spec.cohort` = `parentRef.name`) |
| per-`(node, resource, flavor)` per-cluster `share.nominal` | `ClusterQueue` `resourceGroups[].flavors[].resources[].nominalQuota` |
| node `borrowingLimit` | `ClusterQueue` resource `borrowingLimit` (burst up, within-cluster cohort borrow) |
| node `lendingLimit` | `ClusterQueue` resource `lendingLimit` (share released to siblings when idle) |
| internal-node budget | **nothing** — an authoring guardrail only, see below |
| leaf `namespaces[]` | `LocalQueue` per namespace, `spec.clusterQueue` = leaf CQ |
| `priorityTier` | referenced `WorkloadPriorityClass` |

**Internal-node budgets do not materialize, and containment is not enforced by
Kueue.** Kueue's cohort quota is *additive*, not a ceiling: `SubtreeQuota` is
"the sum of the node's quota, as well as resources available from its children,
constrained by LendingLimits" (`pkg/cache/resource_node.go:33-35`) — and since
`lendingLimit` defaults to null, meaning lend everything, that constraint is
usually absent. Giving a `Cohort` its own `nominalQuota` would
therefore *add* to what its children already contribute — an `org` of 100 over
leaves of 60 + 40 would admit 200, the opposite of the cap an admin authored. A
`Cohort` node's `budgets` are consequently an **authoring guardrail** — the number
`parent ≥ Σ children` is checked against at admission — with **no runtime
enforcement**, and internal nodes materialize as `Cohort` objects carrying no
`resourceGroups` (pure topology, which still pools what children lend). The only
enforced ceiling is the sum of leaf `nominalQuota` + `borrowingLimit`.

Quota is keyed by **`(resource, flavor)`**, not by flavor alone: a
`ResourceFlavor` names a hardware class by node labels and says nothing about
which resource a count applies to, while Kueue's `ClusterQueue` pairs
`coveredResources` with per-flavor `resources[].name`. A budget entry therefore
carries both `resourceName` (e.g. `google.com/tpu`) and `resourceFlavor`.

Materialized object names come from the node's CR `metadata.name` (globally unique,
and stable across re-parenting), never from its position in the tree.

**Non-accelerator resources on the materialized `ClusterQueue`.** Kueue admits a
`Workload` only if **every** resource it requests is covered by the `ClusterQueue`'s
`resourceGroups`; a serving pod always requests `cpu` and `memory`, so a
`ClusterQueue` that quota'd the accelerator alone would render every workload
inadmissible — quota would wedge the whole tenant, not cap its chips. The
materializer therefore also covers `cpu`/`memory` in the resourceGroup, set high
enough never to gate: either effectively unbounded (a large `nominalQuota` with open
borrowing) or derived from the flavor's per-node cpu:mem:accelerator ratio. These
are **not** authored in `AcceleratorQuota.spec` and are **not** part of the budget —
the accelerator is the only fleet-budgeted resource (see *Non-Goals*); covering the
rest is pure materialization plumbing so Kueue admission never blocks on them.

Within a cluster, borrowing/lending/fair-share are entirely Kueue's (the cohort
tree plus Fair Sharing) — OME sets the numbers and delegates the mechanism.

### Burstable tenants (floor vs. cap)

The polarity here is the opposite of what it looks like, and getting it backwards is
how a "hard cap" tenant silently doubles. In Kueue, **null means unlimited**:

> borrowingLimit … If null, it means that there is no borrowing limit.
> lendingLimit … If null, it means that there is no lending limit, meaning that all
> the nominalQuota can be borrowed by other clusterQueues in the cohort.
> — `apis/kueue/v1beta1/clusterqueue_types.go:235-252`

Every leaf we materialize sets `spec.cohort` (see *Budget → Kueue mapping*), and the
two limits are only *permitted* when a cohort is set. So leaving both unset does
**not** produce a hard cap — it produces **unlimited borrowing within the cohort**.
Bursting is the default; capping is the opt-in.

- **Bursting tenant (the default).** With both limits unset, a leaf may consume its
  own share plus every idle chip its cohort siblings are lending on that cluster.
  `nominal` is a **guaranteed floor + opportunistic overage**, not a fleet ceiling,
  and the tenant can exceed its global `nominal` — borrowing on cluster A while
  using its full share on cluster B. `status` surfaces the relaxed effective ceiling
  so the shift is explicit, never silent.
- **Hard-capped tenant (opt-in, `borrowingLimit: 0`).** Setting `borrowingLimit: 0`
  restores the strict guarantee: per-cluster shares provably **sum to the global
  `nominal`** and no workload of that tenant is admitted beyond it. Cost and billing
  tenants want this, and they must ask for it. Pair it with `lendingLimit: 0` if the
  tenant must also never have its idle capacity lent out from under it — otherwise
  its unused share is borrowable, and reclaim preempts the borrower.

`TBD:` whether the *operator-level* default should be `borrowingLimit: 0` — safe,
but surprising to anyone who knows Kueue — or Kueue's own null. Either way it is a
config value with no in-code default, and `status` must show the effective ceiling.

Two consequences: (1) borrowed capacity is reclaimable, and reclaim means
**preempting an admitted replica** — so a burst tier should ride a *lower*
`priorityTier` than the guaranteed floor (Kueue preempts it first), and burst is
far safer for batch than for long-running serving. (2) This is the
*within-cluster* form of elasticity only; live *cross-cluster* reassignment of
unused share is the strictly harder, deferred problem (Open Question #3), and the
reclaim churn it shares with `Proportional` rebalancing needs the same hysteresis
(Open Question #2).

### Projection contract

**Marks.** A projection carries label `ome.io/quota-origin` — value: the management
plane's configured identity, config-driven with no in-code default, mirroring
`PlacementControlPlaneLabel` (`pkg/controller/v1beta1/placement/constants.go:39-47`)
— plus annotations `ome.io/quota-origin-uid` (staleness detection),
`ome.io/quota-source-generation` (progress reporting), and `ome.io/quota-cluster`,
which records which `WorkloadCluster` the share was computed for so a projection
landing on the wrong member after a kubeconfig swap is detectable rather than
silently rendered. A source carries none of them.

**What differs on a projection.** Same `metadata.name` (so the Kueue object name is
stable), same `role`, `parentRef`, `namespaces`, `priorityTier`, `borrowingLimit`,
`lendingLimit`. `nominal` is replaced by that cluster's resolved share.
`distribution`, `budgets[].policy`, and `budgets[].perCluster` are **omitted** — a
projection carries a resolved number, never an unresolved split.

**The ancestor chain is projected too.** A leaf's `parentRef` must resolve on the
member, so the hub projects every ancestor of every matched leaf up to the top
tier. Projected internal nodes carry **no** `budgets`: a fleet `nominal` is
meaningless on one cluster, and a budget-less node is unconstrained for
containment. Their only job on the member is to reproduce the `Cohort` topology.

**Progress reporting.** `metadata.generation` is object-local; a projection's
generation has no relation to the source's, so comparing them reports nonsense —
typically "permanently behind". The member echoes the source generation it rendered
into `status.sourceGeneration`, and the hub compares that against what it applied.
Both count *source* revisions, so the pair is comparable.

**Four loop guards.** No control loop may turn its output back into its input, and
no single guard is sufficient.

1. **Mode exclusivity.** Workload mode holds no `WorkloadCluster` transport, so a
   projection has nothing to be re-projected *by*. The structural guard.
2. **Origin-label source filter.** The hub's source lister uses
   `!ome.io/quota-origin`, so a projection that lands on the management cluster — a
   self-registered `WorkloadCluster`, a misdirected kubeconfig — is invisible as a
   source.
3. **Webhook provenance gate.** The member denies writes to an origin-labeled CR by
   anyone other than the projector ServiceAccount, and denies adding *or* removing
   the label on an existing object. Without this a human could promote a projection
   to a source by deleting one label. Note the contrast with OME's existing
   provenance gate (`pkg/webhook/admission/inferencereplica/validator.go:129-134`),
   which keys on an *annotation* and is therefore only a discouragement — an
   annotation is forgeable by anyone who can write the object. `req.UserInfo` is
   apiserver-supplied and unforgeable, so an identity-comparing gate is an
   enforceable boundary, not merely a deterrent. RBAC on the member remains the
   coarse control; this is the fine one.
4. **Origin-scoped reaping, one projector per member.** Two management planes
   projecting same-named nodes onto one member would each see the other's
   projections as stale and reap them, forever — oscillating `nominalQuota` and
   Kueue preemption churn rather than a clean failure. So the reaper considers
   **only** projections whose origin equals this plane's identity, and only then
   compares `origin-uid`. The member additionally pins a single expected origin and
   denies any other with reason `ForeignProjector`. This is exactly the rule
   `PlacementControlPlaneLabel` exists to express; the label was borrowed from
   placement, and the rule must be too.

### Ownership & lifecycle

Materialized Kueue objects carry **ownership labels** — `ome.io/quota-managed-by`
(the plane's identity) and `ome.io/accelerator-quota` (the node's CR name). The
materializer never touches an object lacking them, so hand-provisioned queues
coexist during migration, and never *adopts* an existing object that lacks them
(see *Residual authority*).

Because rendering now happens on the member against a **local** projection, the
owner and the dependent are on the same cluster. Kubernetes permits a
cluster-scoped dependent to be owned by a cluster-scoped owner, so `ClusterQueue`
and `Cohort` — and the namespaced `LocalQueue` — CAN carry a real OwnerReference to
the local `AcceleratorQuota`, and kube GC handles uninstall. That was impossible in
a direct-SSA design only because the owner lived on a different cluster; two-level
projection removes the obstacle. *Lean:* OwnerReferences as the primary reaper,
with a label sweep and a finalizer as belt-and-braces. The sweep is not redundant:
removing an entry from `namespaces[]` orphans that namespace's `LocalQueue` while its
owner still exists, so OwnerReference GC will **not** reap it. (A `metadata.name`
change cannot orphan anything — names are immutable in Kubernetes.) `TBD:` whether
SSA field-manager conflicts on
`metadata.ownerReferences` make this awkward in practice — needs a spike.

Deleting a leaf reaps its `ClusterQueue`/`LocalQueue` **on the member that holds
them** — via the cascade in *Freeze, and the deletion paths*, since the management
plane writes no Kueue object. Deleting an internal node that still has children is
rejected by the webhook (it would orphan the subtree).

### Restructuring the tree

A leaf that grows sub-projects is the common evolution: `team-a` is a `ClusterQueue`
leaf today, and tomorrow it needs `proj-x` under it. Converting `team-a` in place
(`ClusterQueue`→`Cohort`) is admitted **only if it is drained first** — the guard in
*Node kind* — because that deletes its `ClusterQueue` and everything admitted against
it. So for a **live** tenant the zero-downtime path introduces a grouping *above* the
existing leaf, which keeps it and its admitted workloads intact:

```
before                            after
org-1                             org-1
└─ team-a (ClusterQueue, 60)      └─ team-a-group (Cohort, 60)
                                     ├─ team-a (ClusterQueue, 40)  # same CR, re-parented
                                     └─ proj-x (ClusterQueue, 20)  # new leaf
```

The order matters, because containment is checked at every step:

1. **Create the grouping** `team-a-group` (`role: Cohort`) under `org-1` with **no
   budget**. Sizing it to 60 up front would be rejected: `team-a` is still `org-1`'s
   child at that moment, so `org-1` would see children summing to 120. A
   budget-less node is unconstrained for containment, which is exactly what makes
   this step admissible.
2. **Re-parent the existing leaf** — change `team-a`'s `parentRef` to
   `team-a-group`. `parentRef` is freely mutable (`role` changes only under the drain guard in
   *Node kind*), and since Kueue
   object names come from `metadata.name`, this is an **in-place update** of the
   existing `ClusterQueue`'s `spec.cohort`: the queue object, its namespaces,
   its `LocalQueue`s, and its admitted workloads all survive. Only the node's
   `status.path` changes.
3. **Size the grouping, shrink the incumbent, then add the sibling** — set
   `team-a-group` to 60 (now that `team-a` is its child, not `org-1`'s), lower
   `team-a` to 40, then create `proj-x` at 20. Every apply keeps parent ≥ Σ children
   true; adding `proj-x` first, or sizing the grouping in step 1, would be
   rejected.

Two variants worth calling out:

- **Moving namespaces between leaves** (e.g. `proj-x` takes over a namespace from
  `team-a`) is workload-visible: the resolver stamps a different `queue-name`, so
  *newly created* pods charge the new leaf while already-admitted pods keep running
  against the old one until they are recreated. It is a rollout, not an instant cut.
- **Forcing the old name to become the grouping** (keeping the path literally
  `org-1.team-a` as a `Cohort`) is the guarded in-place `role` change: drain the
  leaf, then flip `role` to `Cohort`. It deletes the underlying `ClusterQueue`, so it
  disrupts anything still admitted — the drain is mandatory and the webhook rejects
  the flip otherwise. The grouping-above pattern exists precisely to avoid the drain,
  at the cost of one extra name in the tree.

The authoring tool renders the before/after tree and dry-run validates each step,
so the ordering constraint above is caught before a GitOps commit rather than as a
rejected apply.

### Where it runs

One binary, two modes, selected by `--quota-mode={workload,management}` (empty
means off — a flag rather than ConfigMap data, following the rule at
`pkg/controller/v1beta1/controllerconfig/multicluster.go:20-23`: topology decides
which controllers run, so it is a restart-time decision).

- **Management mode** additionally requires `--multicluster-role=control-plane`.
  It reuses the `WorkloadCluster` connection subsystem for remote SSA, and its
  remote grant is **`acceleratorquotas` read + write, and nothing else** — no
  `kueue.x-k8s.io` at all. It needs no `/status` subresource rule: reading a
  projection's status comes with `get`/`list` on the main resource, and the hub
  never writes a projection's status.
- **Workload mode** writes Kueue on its own cluster. Running both modes in one
  process is a hard startup reject.

**The RBAC delta is two-sided.** This removes cluster-wide `kueue.x-k8s.io` write
from the hub and adds it to every member. On the member that grant would otherwise
land on the ServiceAccount already used by the ISVC reconcilers, the pod mutating
webhook, and the model controllers. It should not: quota-workload mode SHOULD run
as its own Deployment with its own ServiceAccount, so the Kueue write is scoped to
the process that needs it. Doing otherwise concedes most of the argument for
two-level delivery.

Everything else is a `quota` block in the `inferenceservice-config` ConfigMap with
**no in-code default** — `rootName`, `maxTreeDepth`, `acceleratorResources`,
`coverResources`, `budgetedNodeSelector`, `capacityHysteresisPercent`,
`overcommitPercent`, `defaultDistributionPolicy`, `fieldManager`, `originIdentity`
(hub-side, stamped on projections), `projectorServiceAccount` and `expectedOrigin`
(member-side pins), `resyncInterval`.

Two of those decide what "capacity" even means and deserve naming here.
`acceleratorResources` is the list of resource names that count as accelerators
(`nvidia.com/gpu`, `google.com/tpu`, …) — absent, capacity derivation is off, since
OME should not guess which of a node's scalar resources is the expensive one.
`budgetedNodeSelector` narrows which nodes OME budgets at all, so nodes reserved for
another tenant or another scheduler do not inflate the denominator. `TBD:` how a
node's allocatable is attributed to a `ResourceFlavor` — matching the flavor's
`spec.nodeLabels` is the obvious rule and costs nothing given the reference-only
stance in Open Question #4, but it is unspecified today.
Absent config **disables** rather than assumes: no `maxTreeDepth` means the depth
check does not run, not that 5 is assumed.

The reserved root's **name** is deliberately **not** in that list, and not
configurable at all. It is a wire-contract identity: every `AcceleratorQuota` in
the fleet writes `parentRef: {name: root}`, so a per-cluster name would make those
CRs non-portable between planes and hand one cluster a tree the next cannot read.
Configurability would also brick the feature when unset — with no root, the rules
above (exactly one parent-less node; a dangling `parentRef` rejected at admission)
reject *every* authored node. So it is a compiled-in constant,
`v1beta1.AcceleratorQuotaRootName`, and the no-magic-values rule is satisfied by
the value being part of the API contract rather than a behavioral default.

### The resolver and a hard prerequisite

The resolver unifies `SetPodLabelsFromAnnotations` and placement's `stampQueue`
into one function: given a workload's leaf node (from namespace binding or the DAC
annotation alias), stamp `kueue.x-k8s.io/queue-name` = the leaf's `LocalQueue`
and `kueue.x-k8s.io/priority-class` = the leaf's default tier, left intact if the
workload already declares its own `WorkloadPriorityClass`. It runs in the ISVC
reconciler
for both `RawDeployment` and `OMENative` (the labels already flow to pods via
`irprojector`/render — no webhook needed).

**Not a prerequisite — already satisfied.** The concern was that
`InstanceReadyTimeout` measured from operation start would fail a
legitimately-queued pod at ~30 minutes, and that quota only *increases* how often
pods sit gated. For `OMENative` the deadline is already parked while a pod is
gated: `workload.PodAdmissionGated` (`pkg/controller/v1beta1/workload/deadline.go`)
zeroes it for a scheduling-gated pod, wired in at
`pkg/controller/v1beta1/inferencereplica/status.go`. `RawDeployment` has no
deadline mechanism at all, so nothing can fire early there either.

What remains is hygiene rather than a dependency: the 30m default is a literal in
two places (`workload/plan.go`, `isvc/inference_service_defaults.go`), which the
no-magic-values rule says belongs in config. Worth fixing, but it blocks nothing
here.

### Interplay with placement and ome-scheduler

- **Placement (OEP-0018.3 `Split`).** `Split` apportions `minReplicas` (the
  guaranteed floor) across clusters. If a cluster's share does not cover its slice
  of that floor, the floor cannot be admitted. The management plane cannot enforce
  this today: shares are computed per tenant leaf from capacity or admin input, with
  no ISVC input at the hub (Open Question #9). So for now it is a **property an
  admin must maintain**, surfaced rather than guaranteed — `Split` reports
  unsatisfiable floors on the ISVC, it does not resize the quota tree. `maxReplicas`
  remains a per-cluster autoscaling ceiling and may exceed the share (the
  workload then borrows within-cluster or stays gated — OEP-0013's "no safety
  ceiling" stance is unchanged).
- **ome-scheduler (OEP-0022).** Quota is exactly the "hold flag" seam: Kueue
  removes the `ome.io/admission` gate when quota allows (Phase B), then
  ome-scheduler places the gang. This OEP provides the quota half of that
  contract; TAS stays off so the two don't both assign domains.

## Test Plan

- **Unit:** tree assembly and every invariant; apportionment as a property test
  asserting `Σ shares == nominal` **exactly** across random splits, including
  pinned clusters; `Explicit` sum check; capacity summation with the
  Ready/uncordoned predicate; hysteresis on a synthetic capacity series that dips
  and recovers.
- **Tree validation, per plane** — the two webhooks reject different things:
  - *hub:* dangling parent, second root, cycle, over-depth, node-kind clash, `role`
    flip on a non-drained leaf, parent < Σ children, namespace collision **within
    the authored tree** (the residual cross-plane hole is Open Question #6).
  - *member:* over-capacity projection, unknown flavor, foreign projector,
    hand-edit of an origin-labelled CR, and `distribution`/`perCluster` on a
    locally-authored source.

  The controller then **re-detects** the race cases admission cannot serialize (two
  children busting a parent concurrently, a parent deleted under a child) and sets
  `Degraded` + **freezes** that subtree — deleting nothing.
- **Materialization (envtest, real Kueue):** using `framework.Kueue` (the CRD bundle) plus
  `framework.StartKueue` (the in-process Kueue stack), as the opt-in `realkueue`
  multicluster suite already does — no single-cluster envtest suite is wired to Kueue
  today, so standing one up is part of this work — a leaf becomes a `ClusterQueue` with a
  cpu/mem cover; an internal node becomes a `Cohort` with **no** `resourceGroups`;
  usage rolls up from `ClusterQueue.status.flavorsUsage`; a pre-existing *unlabeled*
  `ClusterQueue` of the same name is refused rather than adopted.
- **Single-cluster integration:** on a KWOK cluster with Kueue installed from the
  pinned module's chart, apply an `AcceleratorQuota` tree, deploy ISVCs in two
  tenants, assert each pod lands in its tenant's `LocalQueue`, that over-budget
  pods gate, that `status` usage matches `ClusterQueue.status`, and — the
  regression that matters — that **cordoning a node deletes no `ClusterQueue` and
  changes no `nominalQuota`.**
- **Multi-cluster:** one management cluster + 2 members. Assert projections carry
  resolved shares *and the ancestor chain*, shares sum to the budget exactly, usage
  flows back, a tenant delete reaps only its labeled objects, a hand-edit of a
  projection is rejected, a never-reported member is `Pending` at zero share, and
  **stopping a member freezes rather than rebalances.**

## Graduation Criteria

- **Alpha — workload mode only, no management plane.** A locally-authored
  `AcceleratorQuota` tree materializes to Kueue on one cluster; derived root
  capacity; freeze-on-violation; the ISVC→(queue, priority) resolver; the authoring
  tool's render + dry-run validate.
- **Beta — management mode.** Projection of each matched leaf and its ancestor
  chain; `Explicit` then `Proportional` (snapshot) apportionment with `Σ shares ==
  nominal` exactly, including pinned clusters; the member veto with `Pending` and
  `SharesExhausted` reporting; OwnerReference GC with the label sweep as
  belt-and-braces; MUSE read panel for capacity vs quota vs usage; DAC-annotation
  migration path documented.
- **Stable:** the authoring-time containment check and the projection contract
  battle-tested; priority tiers proven; usage rollup in `status` proven against
  `ClusterQueue.status`; runbooks for over-subscription, starvation, and a long
  member partition.

## Drawbacks

- **Two controllers and a duplicated object set.** Two-level delivery costs a
  second controller and a copy of every matched node on every member, versus one
  distributor writing Kueue directly. The exchange is that the hub holds no Kueue
  credential and the member gets a veto. I should name the condition under which
  that trade is wrong: if the hub and every member are run by one team inside one
  trust boundary — which is how OEP-0018 and OEP-0019 describe OME today — the veto
  is one nobody exercises. The trigger to reconsider is simple: **if after Alpha no
  member operator has ever used the veto, the extra controller is not paying for
  itself.**
- **A new remote write grant**, `acceleratorquotas` on every member — smaller than
  the `kueue.x-k8s.io` grant a distributor would need, but still larger than
  today's label-only remote access. And the Kueue write does not vanish; it moves
  to each member, where it should be scoped to its own ServiceAccount.
- **A shrinking share is over-committed until its projection lands.** A share that
  *grows* under-shoots harmlessly (the member keeps its smaller share until the
  projection arrives); a share that *shrinks* leaves the member admitting at the
  old number for that window. Inherent to a sum-of-shares model, but real.
- **Rebalancing is not instantaneous.** Because the global budget is the sum of
  per-cluster shares and Kueue cohorts are per-cluster, a tenant idle in cluster
  A cannot instantly lend to itself in cluster B; the share moves only when the
  management plane re-apportions. In v1 that happens on a spec edit and nothing
  else — the capacity snapshot is deliberately static (Open Question #2) — so the
  honest latency is "when an admin changes something", not seconds–minutes. A
  periodic re-split with move damping would be the mechanism if we ever want the
  faster behavior. Within a cluster, borrowing is Kueue-instant.
- **Tighter Kueue coupling.** OME now depends on Kueue's `Cohort`/`ClusterQueue`
  schema (which is still evolving across Kueue minors) rather than treating Kueue
  as an opaque admission black box.

## Alternatives

1. **Pure GitOps raw Kueue, no OME object** — admins author `ClusterQueue`/
   `Cohort` YAML directly; OME only documents conventions + the resolver.
   *Rejected for this requirement:* it cannot express a global budget or split it
   — there is no fleet-wide number, only N independent per-cluster queues. (It is
   the right answer if per-cluster quota is acceptable; this OEP exists because it
   is not.)
2. **MultiKueue** (Kueue's native multi-cluster) — a management cluster dispatches
   `Workload`s to members. *Rejected:* OME already rejected MultiKueue in
   OEP-0018 in favor of fan-out + its own `WorkloadCluster` connection; adopting
   it now would fork the placement model.
3. **One jumbo CR holding the whole tenant tree** (a flat `tenants[]` list, the
   hierarchy encoded in dot-path names). *Rejected —* though note the honest
   trade: a jumbo CR makes cross-node validation **easier**, because one admission
   sees the entire tree with no concurrency window at all. What it costs is
   everything else: the structure hides in string parsing, every edit funnels
   through one document (no per-node CODEOWNERS, RBAC, or status), two teams cannot
   edit their own subtrees concurrently, and one bad field rejects the whole fleet's
   config. The chosen one-CR-per-node model trades a narrow admission race — which
   the controller re-check covers — for per-node ownership, and mirrors Kueue's own
   `Cohort`/`ClusterQueue` split.
4. **A `ConfigMap` instead of a CRD.** *Rejected:* no schema validation, no typed
   `status` for the usage rollup, and no natural webhook for the parent≥Σchildren
   invariant.
5. **A control-plane distributor writing Kueue objects on each member directly.**
   *Rejected:* it needs cluster-wide `kueue.x-k8s.io` write on every member held by
   the hub, and gives the member no veto over what lands — see *Multi-cluster flow*,
   option (a), for the full comparison. It is genuinely the simpler data path, and
   *Drawbacks* names the condition under which we would go back to it.

## Open Questions

1. **Fungible units?** v1 budgets are raw per-`ResourceFlavor` counts. Do we need
   normalized "accelerator-equivalent" budgets across heterogeneous hardware?
   That adds a contribution table and a flavor-fungibility model. *Lean:* raw
   per-flavor for v1; revisit if a single budget must span mixed hardware.
2. **`Proportional` snapshot vs. dynamic re-split, and damping.** v1 takes a
   capacity **snapshot** at reconcile time and holds it — static and controllable.
   Should we later re-snapshot dynamically (a periodic tick, or on observed
   capacity change), and if so how do we damp churn (a share that shrinks can
   force Kueue to preempt admitted work)? A dynamic mode would need hysteresis /
   move damping. *Lean:* start snapshot-only; revisit a dynamic mode once the
   static split is proven.
3. **Cross-cluster borrowing later?** Is the "sum of per-cluster shares" ceiling
   acceptable long-term, or do we eventually want a hub that reassigns unused
   share live (approaching MultiKueue)? Ties into OEP-0022's "hub" note.
4. **Who owns `ResourceFlavor` + node labels?** Does OME reference existing
   flavors only, or also reconcile them from a spec (assuming some external
   node-labeling controller)? *Lean:* reference-only in v1.
5. **Strict top-down creation vs. eventual assembly.** v1 **rejects** a CR whose
   `parentRef` does not yet resolve (parents before children), which the authoring
   tool orders automatically. The alternative — admit a dangling child and hold its
   subtree unmaterialized until the parent appears — is friendlier to unordered
   GitOps applies but weakens the "admission proves a valid tree" guarantee.
   *Lean:* strict for v1; revisit if GitOps ordering proves painful.
6. **Should locally-authored nodes be rejected on a cluster that receives
   projections?** Fleet-wide namespace uniqueness cannot actually be checked by a
   per-cluster webhook: on a member the visible set is projections plus local CRs (a
   subset), and on the hub it is the fleet tree minus each member's locally-authored
   nodes. The invariant holds strictly in **neither** place, so a namespace claimed
   by a projected leaf on one cluster and a local leaf on another is caught only at
   reconcile. Rejecting local authoring on a projection-receiving cluster would make
   the sets disjoint and close the hole, at the cost of a per-cluster escape hatch.
   *Lean:* reject — but only once the management plane is the normal deployment;
   forbidding it earlier breaks the single-cluster path.
7. **Bound the shrinking-share window?** See Drawbacks. *Lean:* start unbounded and
   measure; a projection deadline that `Degrade`s the source node is easy to add.
8. **Namespace absent on a member.** `LocalQueue` is namespaced, so a leaf binding a
   namespace that does not exist on some cluster cannot materialize there. Create
   it, or `Degrade` and skip? *Lean:* skip — OME creating serving namespaces is a
   new and surprising power.
9. **What unblocks quota-aware ISVC dispatch?** Placement is deliberately untouched
   (see *Interplay with placement*), and it must stay that way until the control
   plane can answer "does this ISVC fit". It cannot today: the ISVC defaulter
   runtime-selects against the *local* catalog, which is empty on the control plane
   (`cmd/manager/main.go:628-637`), so placement's only hub-side inputs are label
   selectors. Either the ISVC declares its accelerator demand explicitly, or the
   runtime catalog is projected to the hub. Both are their own OEP.
10. **Resolver ordering.** Two-level delivery creates a hazard for G3: a leaf's
    `LocalQueue` exists only after the projection renders, so an ISVC created in
    that namespace beforehand gets stamped with a `queue-name` that does not
    resolve. Leave the pod unstamped (it runs unquota'd — a billing decision) or
    stamp anyway (Kueue holds it until the queue appears)? A policy choice, not an
    implementation detail. `TBD`.
