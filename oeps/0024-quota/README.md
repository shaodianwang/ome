# OEP-0024: Fleet Quota — global GPU/TPU capacity budgets

**Status:** provisional. Single-cluster is the self-contained first slice;
multi-cluster (the global budget) builds on it. Open questions marked inline and
collected at the end.

<!-- toc -->

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
node-kind consistent, parent ≥ Σ children, namespaces globally unique) so a CR that
would break the tree is rejected up front, not silently mis-materialized. A **distributor** then splits
each node's budget across clusters and materializes **stock Kueue** objects on
each. Everything below the budget is native Kueue, so borrowing, preemption, and
fair-share are Kueue's job, not ours. Kueue is the **first** backend behind
`AcceleratorQuota`; the materialization layer may evolve, but the `AcceleratorQuota`
contract stays put.

## Summary

`AcceleratorQuota` is a CRD on the OME control plane, modeled on Kueue's own tree:
**one CR is one node**. `spec.role` declares which kind: a `Cohort` node groups
other nodes, and a `ClusterQueue` node is a leaf that binds serving namespaces and
carries a **per-flavor budget** (a `ClusterQueue` plus a `LocalQueue` per
namespace). `spec.parentRef` names its parent CR; the fleet's whole quota tree
is the graph of those edges, rooted at a single reserved `root` node whose budget
is the fleet-wide total. A leaf also carries a **priority tier** and a
**distribution policy**. Because every node is a first-class API object, a
**validating webhook can assemble the entire tree at admission** — it lists the
existing `AcceleratorQuota`s, splices in the incoming one, and rejects it unless the
result is a well-formed tree (parent resolves, no cycle, within depth, node-kind
consistent, parent ≥ Σ children, namespaces globally unique). A
`quota-distributor` controller then reconciles the admitted tree by (1) computing
each leaf's per-cluster share of every flavor and (2) server-side-applying native
Kueue objects to each workload cluster over OME's existing `WorkloadCluster`
connection subsystem: a `Cohort` tree (identical on every cluster) from the
internal nodes, a `ClusterQueue` per leaf (carrying *that cluster's* share as
`nominalQuota`), and a `LocalQueue` per serving namespace. The distributor re-runs
the same tree checks as the source of truth — the webhook is fast feedback, the
distributor is the authority, and anything that slips past a concurrent admission
surfaces as a `Degraded` condition rather than a bad materialization. No OME CRD
exists below the budget — the materialized objects *are* Kueue objects.
Single-cluster is the degenerate case where the cluster registry has one entry.
OME's InferenceService reconciler gains one **resolver** that maps an ISVC to its
leaf's `LocalQueue` + `WorkloadPriorityClass` labels, replacing today's two
divergent label stampers. Because a fleet is now many small CRs, OME also ships
**authoring tooling** (a CLI / kubectl plugin) that renders and validates the
assembled tree offline before it is committed. Capacity admins view the tree and
live usage (from `ClusterQueue.status`) in MUSE, which is read-only; budget spec
edits flow through GitOps, and status is not synced through GitOps.

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

Four concrete failure modes, each anchored to current code:

1. **Nobody provisions quota.** No OME controller, chart, or hack script creates
   a Kueue `ResourceFlavor`/`ClusterQueue`/`LocalQueue`/`Cohort` — the only Kueue
   identifiers in the tree are the label keys in `pkg/constants/constants.go:520-525`.
   OEP-0018 makes this explicit and deliberate: "Kueue installed on every
   workload cluster … the cluster operator owns this; OME assumes it"
   (`oeps/0018-multi-cluster-placement/README.md:234`). The cost: quota lives in
   hand-maintained YAML with no OME-level source of truth, no validation, and no
   view — it drifts, and a mis-sized `ClusterQueue` silently wedges a launch at
   the 30-minute admission deadline.

2. **Tenancy is an opaque string.** `SetPodLabelsFromAnnotations`
   (`pkg/controller/v1beta1/inferenceservice/utils/annotations.go:67-82`) copies
   the `ome.io/dedicated-ai-cluster` (DAC) annotation *verbatim* into
   `kueue.x-k8s.io/queue-name`. There is no tenant object, no namespace↔queue
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
   watch Kueue (`informers.rs` covers OME kinds + Node/Pod only), so an admin can
   neither see quota-vs-usage nor change a budget.

### Goals

- **G1.** One declarative, control-plane source of truth for fleet capacity
  budgets: `AcceleratorQuota`, per tenant, per `ResourceFlavor`.
- **G2.** Deterministic materialization of a budget into **native Kueue** objects
  on each workload cluster; OME owns the budget, Kueue owns admission/preemption/
  fair-share/borrowing.
- **G3.** A single ISVC→(queue, priority) **resolver** replacing the two
  divergent stampers (DAC path + placement `stampQueue`), with the DAC annotation
  kept working as a back-compat alias.
- **G4.** Single-cluster and multi-cluster are the **same design** (N=1 vs N>1
  cluster registry), satisfying the "both" requirement without a second code path.
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
  clusters is a distributor cycle, not instantaneous borrowing (see Drawbacks).
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
- **Node** — one `AcceleratorQuota` CR, one position in the tree, its kind declared by
  `spec.role`. A **`Cohort` node** groups other nodes (maps to a Kueue `Cohort`); a
  **`ClusterQueue` node** is a leaf that binds one or more serving namespaces, carries the
  per-flavor budget, and is the unit a workload charges against (maps to a Kueue
  `ClusterQueue` plus a `LocalQueue` per namespace). `spec.parentRef` names the
  parent CR; the single reserved `root` node is the only one with no `parentRef`.
- **Global budget** — a leaf's fleet-wide allowance of a flavor
  (`team-a: {tpu7x: 40}`): the sum it may consume across all clusters.
- **Share** — the per-cluster slice of a global budget the distributor assigns to
  one cluster; becomes that cluster's `ClusterQueue` `nominalQuota` for the flavor.
- **Distributor** — the control-plane controller that turns budgets into shares
  and materializes Kueue objects.
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

Below this object there is **no OME CRD** — the distributor writes stock Kueue
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
- **Containment** — a parent's per-flavor `nominal` is less than the sum of its
  children's (including the incoming one).
- **Namespace collision** — a namespace bound by this leaf is already bound by
  another leaf, fleet-wide.

Admission is fast feedback but not a serialization point: two children admitted
concurrently can each pass containment yet together bust the parent, and a parent
can be deleted out from under a child. So the **distributor re-runs every one of
these checks at reconcile as the source of truth** — a violation it finds (a race,
an out-of-order edit, a deleted parent) sets a `Degraded` condition + event on the
offending node and leaves that subtree unmaterialized, rather than writing a broken
Kueue tree. The webhook keeps the common mistake out; the distributor guarantees
the invariant.

### Single-cluster flow (the must-have slice)

1. Admin declares the `AcceleratorQuota` nodes (the reserved `root`, groupings, and
   leaves) with a one-cluster registry (or the implicit local cluster).
2. The distributor materializes, on that cluster: a `Cohort` tree mirroring the
   `parentRef` edges among internal nodes; one `ClusterQueue` per leaf node
   (`nominal` → `nominalQuota`; `borrowingLimit`/`lendingLimit` passed through);
   one `LocalQueue` per listed namespace; and the `WorkloadPriorityClass`es
   referenced by `priorityTier`.
3. The resolver stamps each ISVC's engine pods with the tenant's `LocalQueue`
   name and priority class.
4. Kueue admits/gates as usual; OME reads `InstanceStatus.Admitted` (unchanged).

That is a complete quota system on one cluster with **only** the `AcceleratorQuota`
object added — everything else is native Kueue.

### Multi-cluster flow (the global budget)

Same object; the registry now has N clusters. The distributor, running in the
control/management plane (OEP-0019 `--mode`), for each leaf node and flavor:

1. Computes per-cluster **shares** from the entry's effective policy:
   - `Explicit` — take `perCluster[]` verbatim (must sum to `nominal`).
   - `Proportional` — split `nominal` in proportion to a **snapshot** of each
     cluster's allocatable capacity of that flavor, read at reconcile time from
     the workload cluster (the same connection MUSE/placement use). The snapshot
     is deliberately static: shares change only when the budget is reconciled, not
     continuously as capacity drifts (Open Question #2).
2. Server-side-applies the native Kueue objects to each workload cluster over the
   **existing `WorkloadCluster` connection subsystem**
   (`pkg/controller/v1beta1/workloadcluster/remoteclient.go`) — the same remote
   client modeled on MultiKueue that placement already relies on. The `Cohort`
   tree is identical on every cluster; each cluster's `ClusterQueue` carries its
   own share.
3. Re-runs on `AcceleratorQuota` change — re-taking the `Proportional` capacity
   snapshot and updating `ClusterQueue` `nominalQuota` in place. Whether to also
   re-snapshot on a slow periodic tick or on observed capacity change is left as
   Open Question #2; v1 starts snapshot-only for a controllable, static split.

Placement is unchanged in mechanism (still fan-out + first-Kueue-to-admit-wins),
but the per-cluster quota it races against is now OME-managed and provably sums
to the global budget.

### Muse: the capacity-admin view

MUSE is **read-only** in v1. Extend its read plane to watch Kueue
`ClusterQueue`/`Cohort`/`LocalQueue` (gated on CRD presence, like the existing OME
informers) and the `AcceleratorQuota` object, and join them in `rollup.rs` against the
per-namespace usage MUSE already computes → a "Quota" panel: **capacity vs quota
vs used vs borrowed**, per tenant and per cluster. Usage/borrowing is read
directly from `ClusterQueue.status.flavorsUsage` — no new accounting.

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
  the full tree and print it: parent/child structure, per-flavor `nominal` vs. Σ
  children, depth, and each leaf's namespaces.
- **Validate (dry-run)** — run the *same* checks as the admission webhook offline,
  against a proposed change, before it reaches the cluster or a GitOps PR — so a
  containment or node-kind violation is caught at author time, not at `apply`.
- **Scaffold** — generate a child CR under a named parent (wiring `parentRef` and
  the parent's flavor set), and optionally expand a single compact "whole-tree"
  description into the individual per-node CRs a GitOps repo stores.

The tool is pure client-side validation and code-gen: it holds no privilege and is
not on the admission path — the webhook and distributor remain the authority. This
keeps MUSE read-only while still giving admins a way to build and check the whole
tree without hand-tracking every edge. The exact surface (standalone binary vs.
`kubectl ome quota …` plugin) is an implementation detail; the requirement is that
the tree is authorable and checkable as a whole.

## Design Details

### `AcceleratorQuota` schema (v1)

- `spec.parentRef {name}` — the parent node's CR name; omitted **only** by the
  single reserved `root`. The tree is the graph of these edges (mirrors Kueue's
  `Cohort.spec.parentName`).
- `spec.role` ∈ `{Cohort, ClusterQueue}` — what the node **is**, declared not
  inferred. `Cohort` = an internal grouping node; `ClusterQueue` = a leaf that
  carries the budget and binds namespaces. Structure is validated *against* this
  field, which is **mutable only on a drained leaf** (see *Node kind*).
- `spec.namespaces[]` — `ClusterQueue` nodes only; each becomes a `LocalQueue` in that
  namespace, on every cluster, bound to this node's `ClusterQueue`.
- `spec.priorityTier` — `ClusterQueue` nodes only; the **default**
  `WorkloadPriorityClass` the resolver stamps on the leaf's workloads. It is a
  default, not a partition: a workload may declare its own priority, and the leaf's
  `ClusterQueue` admits mixed-priority workloads (Kueue orders and preempts within
  the queue by priority), so one budget serves many priorities.
- `spec.distribution.policy` ∈ `{Explicit, Proportional}` (pluggable; `Solver` is a
  future value) — this node's **default** policy. `Proportional` reads allocatable
  capacity per flavor from each `WorkloadCluster` (a snapshot — see below). There
  is no single fleet-wide default object; each node states its policy and the
  webhook defaults an unset value.
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
- `status` reports `parent`, `path` (the computed root-to-node path, for humans),
  observed `admitted`/`borrowed` per flavor and per
  cluster (rolled up from each cluster's `ClusterQueue.status`), and conditions
  (`Ready`/`Degraded`); a node that violates a cross-CR invariant a concurrent
  admission missed is marked `Degraded`. For a bursting leaf it also reports the
  relaxed effective ceiling (see *Burstable tenants*).

**Invariants.** Because every node is a cluster-scoped object, the validating
webhook sees the whole tree and enforces all of these at admission; the distributor
re-checks them at reconcile as the authority, for the races and out-of-order edits
admission cannot serialize (see *Assembling the tree from `parentRef`*).

- **Tree (single-rooted).** Exactly one node is parent-less — the reserved
  `root` — and every other node MUST set `parentRef` to an existing node, forming no
  cycle; the fleet is therefore **one tree**, not a forest. A second parent-less
  node, or any node other than `root` created without a `parentRef`, is rejected.
  `root` is authored like any other node and its per-flavor `nominal` is the
  fleet-wide total, so containment chains top to bottom: `root` ≥ Σ top-tier ≥ … ≥
  the leaves (a parent's `nominal` per `(resource, flavor)` MUST be ≥ the sum of
  its children's — the standard hierarchical-quota invariant). This is an
  **authoring** invariant: it is rejected at admission and re-checked at
  reconcile, but nothing below the budget enforces it at runtime, because Kueue
  cohort quota is additive (see *Budget → Kueue mapping*).
- **Identity.** A node is identified by its CR `metadata.name`, which is globally
  unique because `AcceleratorQuota` is cluster-scoped — and that name is what the
  distributor uses for the materialized Kueue object. Its **path from the root** is
  the human-readable identity, reported in `status.path`: the same *local* name
  under different parents is a **different node** (under `root`, `org1.tenant-a.team-b`
  and `org2.tenant-a.team-b` are distinct Kueue objects, never colliding), because
  they are necessarily distinct CRs. A bare tenant name is meaningful only relative
  to its parent. Kueue object names are deliberately **not** derived from
  ancestry: a path-derived name would rename — and therefore delete and recreate —
  a live `ClusterQueue` whenever a node is re-parented (see *Restructuring the
  tree*).
- **Disjoint subtrees.** Sibling subtrees MUST NOT overlap: no node and no
  namespace is shared between two of them — the same *local* name under two parents
  is two distinct path-qualified nodes (see *Identity*), not a shared node. This is a
  consequence of the rules above, not a separate check — node-disjointness follows
  from each node being a distinct CR with a single parent (a subtree cannot hang
  under two parents; a DAG is impossible), and namespace-disjointness from the
  fleet-wide *Namespace binding* invariant. Note this is **structural** overlap
  only: two top-tier subtrees MAY target the same `ResourceFlavor` on the same
  cluster — that is how `root`'s capacity is divided among them (the distributor
  sums every leaf's share per flavor per cluster), not an overlap.
- **Depth.** A node's distance from `root` MUST NOT exceed a configurable
  maximum, `maxTreeDepth` (Helm value on the OME chart, **default 5**). Operators
  who need deeper hierarchies can raise it (e.g. 10), with a cost: the distributor
  builds and reconciles the `Cohort` tree bottom-up, so construction time grows
  with depth. The cap is a guardrail against accidental sprawl, not a hard Kueue
  limit.
- **Node kind.** A node is *either* a grouping *or* a leaf, never both — Kueue
  `ClusterQueue`s cannot have child `ClusterQueue`s. `spec.role` **declares** which,
  and the webhook validates the structure against it rather than inferring kind
  from whether children happen to exist:
  - `role: Cohort` MAY carry `budgets` — the parent number containment is checked
    against, an authoring guardrail that never materializes (see *Budget → Kueue
    mapping*) — but MUST NOT carry `namespaces`/`priorityTier`, since no workload
    binds to a `Cohort`, nor `borrowingLimit`/`lendingLimit`, which have no
    meaning on a quota that is never materialized.
  - `role: ClusterQueue` MUST carry ≥1 resourceFlavor budget and MUST NOT be named as any other
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
  resolver cannot decide which queue a workload in that namespace charges. This
  holds **fleet-wide** — a namespace claimed by two leaves is rejected. A
  consequence: `LocalQueue`s exist **only at leaves**, so no namespace is ever
  bound at both a grouping and its descendant; sibling leaves under one grouping
  each get their own `LocalQueue`s in their own namespaces.

### Budget → Kueue mapping

The concrete translation:

| AcceleratorQuota | Kueue object |
|---|---|
| `role: Cohort` node | `Cohort` (`v1alpha1`; `spec.parent` = `parentRef.name`), **no `resourceGroups`** |
| `role: ClusterQueue` node | `ClusterQueue` (`spec.cohort` = `parentRef.name`) |
| per-`(node, resource, flavor)` per-cluster `share.nominal` | `ClusterQueue` `resourceGroups[].flavors[].resources[].nominalQuota` |
| node `borrowingLimit` | `ClusterQueue` resource `borrowingLimit` (burst up, within-cluster cohort borrow) |
| node `lendingLimit` | `ClusterQueue` resource `lendingLimit` (share released to siblings when idle) |
| internal-node per-flavor budget | **nothing** — an authoring guardrail only, see below |
| leaf `namespaces[]` | `LocalQueue` per namespace, `spec.clusterQueue` = leaf CQ |
| `priorityTier` | referenced `WorkloadPriorityClass` |

**Internal-node budgets do not materialize, and containment is not enforced by
Kueue.** Kueue's cohort quota is *additive*, not a ceiling: `SubtreeQuota` is
"the sum of the node's quota, as well as resources available from its children"
(`pkg/cache/resource_node.go`). Giving a `Cohort` its own `nominalQuota` would
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
distributor therefore also covers `cpu`/`memory` in the resourceGroup, set high
enough never to gate: either effectively unbounded (a large `nominalQuota` with open
borrowing) or derived from the flavor's per-node cpu:mem:accelerator ratio. These
are **not** authored in `AcceleratorQuota.spec` and are **not** part of the budget —
the accelerator is the only fleet-budgeted resource (see *Non-Goals*); covering the
rest is pure materialization plumbing so Kueue admission never blocks on them.

Within a cluster, borrowing/lending/fair-share are entirely Kueue's (the cohort
tree plus Fair Sharing) — OME sets the numbers and delegates the mechanism.

### Burstable tenants (floor vs. cap)

`borrowingLimit`/`lendingLimit` are opt-in and pass straight through to Kueue's
fields of the same name, which live on the **per-cluster** cohort. Enabling them
changes what a tenant's global number *means*:

- **Nominal-only tenant (default).** No burst knobs → the strict guarantee holds:
  a tenant's per-cluster shares provably **sum to its global `nominal`**, and no
  workload of that tenant is ever admitted beyond it. This is the hard-cap case
  (cost/billing tenants keep it).
- **Bursting tenant (opt-in).** Because cohort borrow/lend is cluster-local, a
  tenant can exceed its global `nominal` — e.g. borrow an idle sibling's share on
  cluster A (up to `borrowingLimit`) while using its full share on cluster B. So
  `nominal` becomes a **guaranteed floor + opportunistic overage**, not a fleet
  ceiling; the true ceiling is `Σ_clusters (share + borrowingLimit)`, reclaimed on
  contention. `status` surfaces this relaxed effective ceiling so the shift is
  explicit, never silent.

Two consequences: (1) borrowed capacity is reclaimable, and reclaim means
**preempting an admitted replica** — so a burst tier should ride a *lower*
`priorityTier` than the guaranteed floor (Kueue preempts it first), and burst is
far safer for batch than for long-running serving. (2) This is the
*within-cluster* form of elasticity only; live *cross-cluster* reassignment of
unused share is the strictly harder, deferred problem (Open Question #3), and the
reclaim churn it shares with `Proportional` rebalancing needs the same hysteresis
(Open Question #2).

### Ownership & lifecycle

Materialized Kueue objects are cluster-scoped, so OwnerReferences cannot bind
them to the (control-plane) `AcceleratorQuota` across clusters. Use **label-based
ownership** (`ome.io/quota-managed-by`, `ome.io/accelerator-quota` = the node's CR
name) + a **finalizer** on `AcceleratorQuota` that garbage-collects the labeled Kueue
objects on delete. Deleting a leaf reaps its `ClusterQueue`/`LocalQueue`; deleting
an internal node that still has children is rejected by the webhook (it would
orphan the subtree). The distributor never touches unlabeled Kueue objects (so
hand-provisioned queues coexist during migration).

### Restructuring the tree

A leaf that grows sub-projects is the common evolution: `team-a` is a `ClusterQueue`
leaf today, and tomorrow it needs `proj-x` under it. Converting `team-a` in place
(`ClusterQueue`→`Cohort`) is admitted **only if it is drained first** — the guard in
*Node kind* — because that deletes its `ClusterQueue` and everything admitted against
it. So for a **live** tenant the zero-downtime path introduces a grouping *above* the
existing leaf, which keeps it and its admitted workloads intact:

```
before                          after
org-1                           org-1
└─ team-a  (ClusterQueue, 60)         └─ team-a-group (Cohort, 60)
                                   ├─ team-a  (ClusterQueue, 40)   # same CR, re-parented
                                   └─ proj-x  (ClusterQueue, 20)   # new leaf
```

The order matters, because containment is checked at every step:

1. **Create the grouping** `team-a-group` (`role: Cohort`) under `org-1`, sized to
   cover the subtree (60).
2. **Re-parent the existing leaf** — change `team-a`'s `parentRef` to
   `team-a-group`. `parentRef` is mutable (only `role` is not), and since Kueue
   object names come from `metadata.name`, this is an **in-place update** of the
   existing `ClusterQueue`'s `spec.cohortName`: the queue object, its namespaces,
   its `LocalQueue`s, and its admitted workloads all survive. Only the node's
   `status.path` changes.
3. **Shrink the incumbent, then add the sibling** — lower `team-a` to 40, then
   create `proj-x` at 20. Doing it in this order keeps parent ≥ Σ children true at
   each apply; adding `proj-x` first would be rejected.

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

The distributor is a control-plane controller, gated by OEP-0019's
`--mode`/`--multicluster-role` (it belongs with the placement/WorkloadCluster
controllers in the control plane, not the per-cluster serving plane). It reuses
the `WorkloadCluster` connection subsystem for remote SSA; it needs **write**
RBAC for `kueue.x-k8s.io` on each workload cluster (a new grant — today OME's
remote RBAC is read + label-stamp only).

### The resolver and a hard prerequisite

The resolver unifies `SetPodLabelsFromAnnotations` and placement's `stampQueue`
into one function: given a workload's leaf node (from namespace binding or the DAC
annotation alias), stamp `kueue.x-k8s.io/queue-name` = the leaf's `LocalQueue`
and `kueue.x-k8s.io/priority-class` = the leaf's default tier, left intact if the
workload already declares its own `WorkloadPriorityClass`. It runs in the ISVC
reconciler
for both `RawDeployment` and `OMENative` (the labels already flow to pods via
`irprojector`/render — no webhook needed).

**Prerequisite, single- and multi-cluster:** `InstanceReadyTimeout` MUST be
measured from Kueue admission, not operation start, or a legitimately-queued pod
fails at ~30 minutes (OEP-0018 `README.md:114,233`). Quota only *increases* how
often pods sit gated, so this is a hard dependency, not an optimization.

### Interplay with placement and ome-scheduler

- **Placement (OEP-0018.3 `Split`).** `Split` apportions `minReplicas` (the
  guaranteed floor) across clusters; the distributor MUST size each cluster's
  share to cover that floor, or the floor cannot be admitted. `maxReplicas`
  remains a per-cluster autoscaling ceiling and may exceed the share (the
  workload then borrows within-cluster or stays gated — OEP-0013's "no safety
  ceiling" stance is unchanged).
- **ome-scheduler (OEP-0022).** Quota is exactly the "hold flag" seam: Kueue
  removes the `ome.io/admission` gate when quota allows (Phase B), then
  ome-scheduler places the gang. This OEP provides the quota half of that
  contract; TAS stays off so the two don't both assign domains.

## Test Plan

- **Unit / envtest:** the distributor turns the node set into the expected
  `Cohort`/`ClusterQueue`/`LocalQueue` tree; `Explicit` sum check; `Proportional`
  split math; finalizer GC.
- **Tree validation:** the webhook assembles the tree at admission and **rejects**
  each broken case (dangling parent, second root, cycle, over-depth, node-kind
  clash, `role` flip on a non-drained leaf, parent < Σ children, fleet-wide namespace
  collision, undefined flavor); the distributor
  **re-detects** the race cases admission cannot serialize (two children busting a
  parent concurrently, a parent deleted under a child) and sets `Degraded` +
  leaves that subtree unmaterialized.
- **Single-cluster integration:** on a KWOK cluster (the `ome-kueue-0` harness
  already used for Kueue + TPU sims), apply a `AcceleratorQuota`, deploy ISVCs in two
  tenants, assert each pod lands in its tenant's `LocalQueue`, that over-budget
  pods gate, and that `status` usage matches `ClusterQueue.status`.
- **Multi-cluster:** the N-envtest-in-one-process pattern from OEP-0018's
  `realkueue` suite — one control plane + 2 members; assert shares sum to the
  budget, a `Proportional` rebalance on a capacity change updates both
  `ClusterQueue`s, and a tenant delete reaps only its labeled objects.

## Graduation Criteria

- **Alpha:** single-cluster `AcceleratorQuota` → Kueue materialization + resolver;
  `Explicit` distribution across a static registry; MUSE read panel.
- **Beta:** `Proportional` distribution (snapshot on reconcile); finalizer GC
  hardened; MUSE read panel for capacity vs quota vs usage; DAC-annotation
  migration path documented.
- **Stable:** parent/child invariant + priority tiers battle-tested; usage
  rollup in `status` proven against `ClusterQueue.status`; runbook for
  over-subscription and starvation.

## Drawbacks

- **A new control-plane component** (the distributor) with **write** RBAC to
  Kueue on every workload cluster — a strictly larger blast radius than today's
  label-only remote access.
- **Rebalancing is not instantaneous.** Because the global budget is the sum of
  per-cluster shares and Kueue cohorts are per-cluster, a tenant idle in cluster
  A cannot instantly lend to itself in cluster B; the distributor must re-run
  (seconds–minutes) to move the share. This is inherent to any sum-of-shares
  model; a periodic re-split with move damping is the mechanism. Within a cluster,
  borrowing is Kueue-instant.
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
   hierarchy encoded in dot-path names). *Rejected:* it hides the tree structure in
   string parsing, forces every edit through one document (no per-node CODEOWNERS/
   RBAC or status), and defers all cross-node conflict detection to reconcile
   because no single admission ever validates against a *different* CR. The chosen
   one-CR-per-node model makes each edge an explicit `parentRef` the webhook can
   check against the whole tree at admission, mirroring Kueue's own
   `Cohort`/`ClusterQueue` split.
4. **A `ConfigMap` instead of a CRD.** *Rejected:* no schema validation, no typed
   `status` for the usage rollup, and no natural webhook for the parent≥Σchildren
   invariant.

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
