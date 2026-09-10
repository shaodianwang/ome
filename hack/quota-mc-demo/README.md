# Fleet quota + ISVC distribution, on the real three-cluster fleet

A demo runbook for OEP-0024 quota distribution and OEP-0018 placement against
live clusters, not `kwok`. It budgets **32 `google.com/tpu` chips fleet-wide**
and shows both halves of what a fleet does: a tree authored once on the hub
becoming Kueue objects on each member, and an InferenceService authored once on
the hub becoming pods on each member — admitted when the budget covers them,
gated when it does not.

`hack/kwok-sim/` is the simulated equivalent, and the two are deliberately
shaped alike. What this adds is everything the simulator fakes: a real Kueue
admitting real pods onto real accelerators, capacity derived from hardware
nobody wrote down, and a hub reaching members over credentials the members
issued.

| | hub | member | member |
|---|---|---|---|
| kubeconfig | `~/.kube/msl-mks-ome-lco-0-rc` | `~/.kube/msl-gcp-usc1-rc-1` | `~/.kube/msl-gcp-usc1-dev-1` |
| `WorkloadCluster` name | — | `msl-gcp-usc1-rc-1-tpu` | `msl-gcp-usc1-dev-1-tpu` |
| `ome-quota-manager` | management | workload | workload |
| `ome-controller-manager` | `control-plane` role | default | default |
| Kueue | — | v0.19.1 | v0.19.1 |
| `tpu7x` allocatable | — | thousands | 256 |
| budgeted share | — | 16 | 16 |

## Prerequisites

None of these are fixtures — they are properties of how the fleet is deployed.
Steps 1–3 are one-time setup; step 4 is a check on something the members already
carry. `status.sh` shows which are missing.

### 1. Label the WorkloadClusters

`spec.placement.requirements` is matched against `WorkloadCluster` labels and
**nothing else**, and an ISVC that declares no requirement matches *no* cluster.
A registered cluster carries no labels of its own, so without this step nothing
is ever placed and the failure is silent.

```sh
kubectl --kubeconfig ~/.kube/msl-mks-ome-lco-0-rc label workloadcluster \
  msl-gcp-usc1-rc-1-tpu  accelerator=tpu7x env=rc  --overwrite
kubectl --kubeconfig ~/.kube/msl-mks-ome-lco-0-rc label workloadcluster \
  msl-gcp-usc1-dev-1-tpu accelerator=tpu7x env=dev --overwrite
```

`accelerator` is what every fixture here requires; `env` tells the two apart so
one can be singled out with `clusterSelector`. The quota plane never reads
either.

> [!WARNING]
> These objects are ArgoCD-managed (`ome-workloadclusters-msl-mks-ome-lco-0-rc`),
> and Argo runs outside the fleet, so its sync policy is not readable from the
> hub. A hand-applied label shows as drift and is reverted on the next sync if
> self-heal is on — after which nothing is placed and, again, nothing reports an
> error. Re-read the first block of `status.sh` immediately before demoing, and
> put the labels in the GitOps source for anything past a demo window.

### 2. Create the namespace on all three clusters

```sh
for kc in msl-mks-ome-lco-0-rc msl-gcp-usc1-rc-1 msl-gcp-usc1-dev-1; do
  kubectl --kubeconfig ~/.kube/$kc apply -f hack/quota-mc-demo/fixtures/00-namespace.yaml
done
```

`ome-mc-test` already exists on all three; what matters is the
`kueue.x-k8s.io/managed=true` label, which is what puts it inside Kueue's
webhook scope. Every Kueue webhook on these members is scoped by that selector,
so an unlabelled namespace is invisible to Kueue and its pods run ungated
whatever queue they name. It is also the blast-radius fence for step 4.

> [!IMPORTANT]
> Each member's `ome-quota-manager` must also **enroll** the namespace —
> `--enrolled-namespaces=ome-mc-test`, from
> `quotaManager.materialize.enrolledNamespaces` on the chart. That is where the
> set lives: a leaf is a tenant and names no namespace, so which namespaces a
> cluster serves from is a property of the install. Without it the member writes
> no Kueue objects at all — materialization is disabled rather than partial,
> because a ClusterQueue selecting no namespace admits nothing and reports the
> reason in no object. `status.sh` shows an empty LocalQueue section when this
> is missing.

### 3. Install the runtime on all three clusters

```sh
for kc in msl-mks-ome-lco-0-rc msl-gcp-usc1-rc-1 msl-gcp-usc1-dev-1; do
  kubectl --kubeconfig ~/.kube/$kc apply -f hack/quota-mc-demo/fixtures/01-runtime.yaml
done
```

One pod per replica, four chips per pod. Inert on the hub (the `control-plane`
role switches the local ServingRuntime and InferenceService controllers off),
but installed there anyway so the hub's admission webhook sees the same world
the members do.

### 4. Confirm Kueue's `pod` integration is on

Both members carry it, from their Kueue deployment. It is a check rather than a
step — but check it, because it is the single setting the admission half of the
demo rests on and its absence is silent.

```sh
for kc in msl-gcp-usc1-rc-1 msl-gcp-usc1-dev-1; do
  kubectl --kubeconfig ~/.kube/$kc -n kueue-system get cm kueue-manager-config \
    -o jsonpath='{.data.controller_manager_config\.yaml}' | sed -n '/^integrations:/,/^[a-z]/p'
done
```

```
integrations:
  frameworks:
  - batch/job
  - pod
```

> [!IMPORTANT]
> Without `pod` in that list, **quota distribution demos fine and ISVC admission
> cannot be demoed at all** — and nothing reports an error.

OME's placer stamps exactly one Kueue mark on a derived ISVC: the
`kueue.x-k8s.io/queue-name` label on each component's pod metadata, taken from
the source ISVC's `ome.io/local-queue` annotation — which every `2*-isvc-*`
fixture sets to `mc-demo.serving`. The operator-wide fallback
(`placement.localQueue`) is empty by default: a LocalQueue is named for its
leaf, so no one fleet-wide name addresses more than one tenant. Kueue acts
on that label only when `pod` is in `integrations.frameworks`, and the stock
Kueue chart enables `batch/job` alone. With the list at that default the label
is inert: pods are never gated, never become `Workload`s, and consume chips
entirely outside quota. Every leaf's `ADMITTED` reads `0` however many chips are
held, and every candidate reads as instantly admitted to the placer — because
"admitted" means "carries no scheduling gate". Part 3 of the demo would show
five pods Running and a budget of 16 apparently untouched.

Blast radius is bounded by step 2: Kueue touches a pod only when its namespace
carries `kueue.x-k8s.io/managed=true` **and** the pod names a queue
(`manageJobsWithoutQueueName` is false), so `ome-mc-test` is the only place in
either cluster where the integration can gate anything.

Kueue reads its configuration once, at start. If the list has just changed, the
controller keeps serving the old one until it is restarted — so check the pod is
younger than the edit, not just that the ConfigMap looks right.

> [!NOTE]
> `kueue-manager-config` is ArgoCD-managed on these clusters. Change it in the
> GitOps source; a hand patch shows as drift and self-heal reverts it.

## The demo

### Part 1 — quota distribution

Author the tree on the hub. Nothing else moves.

```sh
kubectl --kubeconfig ~/.kube/msl-mks-ome-lco-0-rc \
  apply -f hack/quota-mc-demo/fixtures/10-tree-explicit.yaml
hack/quota-mc-demo/status.sh
```

```
root                     Cohort        32          the fleet total, authored here
└── mc-demo              Cohort        32          grouping, travels budget-less
    └── mc-demo.serving  ClusterQueue  32          Explicit: rc-1 16, dev-1 16
```

What to point at, in order:

1. **The hub holds no silicon and measures nothing.** Its root's
   `status.capacity` is a fleet view assembled by *reading each member's own
   root* — capacity the members derived from their own hardware, not a number
   any fixture wrote.
2. **The tree is one document, authored once.** Each member receives a copy
   whose `nominal` is already resolved to *that member's* share. `get aq
   mc-demo.serving -o yaml` on a member says `16`, with no trace of the split
   and no `perCluster`.
3. **The leaf became stock Kueue.** A `ClusterQueue` named for the node, a
   `LocalQueue` of the SAME name in every namespace the member enrolls, and the
   grouping above it a `Cohort` with no `resourceGroups` — pure topology that
   pools what its children lend but contributes no quota of its own. The queue
   is named for the leaf because the name is the tenant: it is what a workload
   puts in `kueue.x-k8s.io/queue-name` to reach one budget out of many sharing
   a namespace.
4. **The hub authenticates as `ome-quota-access`**, an identity the *members*
   issued, whose grant is `acceleratorquotas` plus the placement role and
   nothing else. A projector reaching for a `Node` or a `ResourceFlavor` is
   refused here and passes everywhere it is tested with admin credentials.

Then swap the policy to show the capacity read is live rather than fixtured:

```sh
kubectl --kubeconfig ~/.kube/msl-mks-ome-lco-0-rc \
  apply -f hack/quota-mc-demo/fixtures/11-tree-proportional.yaml
```

Same shape, no policy named, so the hub's configured `Proportional` default
applies and the 32 splits against what each member actually reported —
overwhelmingly toward rc-1. **Put `10-tree-explicit.yaml` back before Part 2**:
dev-1's proportional share cannot fund even one 4-chip replica, so every
placement would land on one member.

Worth two minutes: `12-tree-badsum.yaml` is an `Explicit` split summing to 24
against a nominal of 32. **The apply succeeds** — share arithmetic is not an
admission check, and the object is created at generation 1 — and then no
`AcceleratorQuota` reaches either member at all. The leaf resolves to no shares,
and a leaf's ancestors are rendered only when that leaf has one, so the
`mc-demo` cohort is skipped too. Both members keep only their own bare root.

The reason to show it is that `kubectl get aq` looks entirely healthy while this
is true — every node `Ready=True`, `Degraded=False`, budget columns identical to
a working tree, because the `ShareUnresolved` reason the API documents is never
actually raised for this. The whole signal is `status.clusters[].message`, which
no column carries, which is why `status.sh` prints a projection-state section:

```
-- projection state per cluster (a message here means it did NOT land)
  mc-demo.serving -> msl-gcp-usc1-rc-1-tpu: STALLED: budget google.com/tpu/tpu7x is 32 but its per-cluster shares sum to 24
```

> [!WARNING]
> On a tree that has **already projected**, this does not merely stall. The
> unresolved leaf is absent from what the pass wrote and, not being frozen, is
> absent from the keep-set the sweep spares — so the projector *deletes* the
> copies already on each member, cohort included. A serving tenant is torn down
> fleet-wide by an edit the hub accepted without complaint.

That is worth separating from the capacity guardrail, which behaves the way this
one does not: a budget overshooting what a member holds **is** projected, and
refused there as `Degraded`/`CapacityExceeded` with the last-good materialization
frozen rather than replaced. Whether a number fits is knowable only on the
member; arithmetic that contradicts itself is knowable on the hub and never
leaves it.

Re-apply `10-tree-explicit.yaml` to recover; the node names are shared, so it
fixes the leaf in place and the next pass projects normally.

### Part 2 — ISVC distribution, admitted

```sh
kubectl --kubeconfig ~/.kube/msl-mks-ome-lco-0-rc \
  apply -f hack/quota-mc-demo/fixtures/20-isvc-fits.yaml
hack/quota-mc-demo/status.sh
```

2 replicas × 4 chips = **8 chips per member** against a 16-chip share. `All`
mode places on every candidate; there is no winner and no sweep.

Expect on each member: a derived InferenceService in `ome-mc-test`, 2 pods
`Running`, `mc-demo.serving` at `ADMITTED 8` of `16`.

### Part 3 — ISVC distribution, not admitted

```sh
kubectl --kubeconfig ~/.kube/msl-mks-ome-lco-0-rc \
  apply -f hack/quota-mc-demo/fixtures/21-isvc-exceeds.yaml
```

5 replicas × 4 chips = **20 chips per member** against the same 16. All five
pods are created on each member; Kueue funds four and holds the fifth.

```sh
kubectl --kubeconfig ~/.kube/msl-gcp-usc1-dev-1 -n ome-mc-test \
  get pods -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,GATES:.spec.schedulingGates[*].name
kubectl --kubeconfig ~/.kube/msl-gcp-usc1-dev-1 get aq mc-demo.serving -o wide
```

`ADMITTED` pins at exactly 16 and stays there. The point worth landing: **from
the hub this looks healthy.** The derived objects exist, both candidates are
listed, nothing is `Degraded` — only `readyReplicas` is short. A quota refusal
is not an error condition anywhere up the stack; it is a workload waiting, and
the only place it is visible is the pod's scheduling gate.

Run it *after* Part 2 and it is starker: `mc-demo-fits` already holds 8 of the
16, so only two of the five fit and three stay gated. Delete `mc-demo-fits` and
they admit with nothing re-applied.

### Part 4 — one service across the whole fleet

```sh
kubectl --kubeconfig ~/.kube/msl-mks-ome-lco-0-rc -n ome-mc-test \
  delete isvc mc-demo-fits mc-demo-exceeds --ignore-not-found
kubectl --kubeconfig ~/.kube/msl-mks-ome-lco-0-rc \
  apply -f hack/quota-mc-demo/fixtures/22-isvc-split.yaml
```

8 replicas is 32 chips — the entire budgeted fleet — and no member has room for
more than 4. A converged 4/4 is evidence of real apportionment rather than an
even division chosen up front.

Give it a couple of reconcile cycles. `Packed` does not know either member's
budget, so it over-requests the whole deficit on both, lets each Kueue arbitrate,
and trims to what actually admitted — a `get pods -w` during convergence shows 8
pods per member with half gated before settling to 4. That transient is the
mechanism, not a fault.

## Optional — a runtime that actually uses the TPU

`01-runtime.yaml` is a sleep container: it claims chips so quota has something to
count, and never touches the accelerator. `40-runtime-tpu-jax.yaml` is the real
thing — JAX on live `tpu7x` silicon behind an OpenAI-shaped API. Verified on
dev-1.

```sh
kubectl --kubeconfig ~/.kube/msl-gcp-usc1-dev-1 apply -f hack/quota-mc-demo/fixtures/40-runtime-tpu-jax.yaml
kubectl --kubeconfig ~/.kube/msl-gcp-usc1-dev-1 apply -f hack/quota-mc-demo/fixtures/41-isvc-tpu-jax.yaml
kubectl --kubeconfig ~/.kube/msl-gcp-usc1-dev-1 -n ome-mc-test port-forward svc/mc-demo-jax 8080:8080
```

```console
$ curl -s localhost:8080/health
{"status": "ok", "jax": "0.9.2", "global_devices": 16, "local_devices": 8,
 "device": "TPU_8(process=1,(0,0,1,0))", "probe_ms": 1.19}
```

`/v1/models` and `/v1/chat/completions` answer in OpenAI shape, computing a
matmul on a TPU device per request. **There is no model** — the response text
describes the mesh it ran on. Real chips, real JAX, no weights.

Serving an actual LLM is deliberately out of scope. The production path on this
fleet is the internal `msl-tpu-kernel` vLLM fork at `tensor-parallel-size=16`
with 500–800Gi of memory, a Ray leader/worker bootstrap, an XLA warm-cache
tarball and a checkpoint mounted from a per-cluster GCS bucket over the gcsfuse
CSI driver. One replica is 16 chips — an entire member's share — so it cannot
demonstrate a quota ceiling, and it is neither minimal nor portable between the
two members.

Three things this fixture pair pins down, each of which fails quietly if dropped:

- **`2x2x2`, not a single pod.** A lone 4-chip pod works on a `2x2x1`
  single-host pool, which only one member has; the other carries `2x2x2` and
  larger, where one pod half-claims a two-host slice. 1 leader + 1 worker = 2
  pods = 8 chips is the smallest shape both members can schedule.
- **`topologyKey: cloud.google.com/gke-tpu-partition-2x2x2-id`** co-locates the
  gang in one slice. Confirmed: both pods landed on
  partition `12f1a232…`, pool `b2p1-blk1-sblk134-2x2x2-00`. Without it they can
  split across slices and libtpu never forms a mesh.
- **`global=16` is cores, not chips.** tpu7x is two cores per chip, so 16 cores
  = 8 chips = 2 hosts × 4. A host that failed to join reports `global=8` rather
  than an error.

> [!NOTE]
> This fixture names no leaf, so its pods carry no `kueue.x-k8s.io/queue-name`
> label and hold 8 real chips while charging no budget. Placement stamps that
> label from the ISVC's own `ome.io/local-queue` annotation and nothing else —
> the operator-wide fallback is empty, since a LocalQueue is named for its leaf.
> Add the annotation and place it from the hub (see the comment in
> `41-isvc-tpu-jax.yaml`) to have it charge a leaf. At 8 chips a replica, a
> 16-chip share funds two.

## Cleanup

```sh
kubectl --kubeconfig ~/.kube/msl-mks-ome-lco-0-rc -n ome-mc-test delete isvc --all
kubectl --kubeconfig ~/.kube/msl-mks-ome-lco-0-rc delete acceleratorquota mc-demo.serving mc-demo root

# only if the real-TPU pair was applied; it holds 8 chips until removed
kubectl --kubeconfig ~/.kube/msl-gcp-usc1-dev-1 -n ome-mc-test delete isvc mc-demo-jax --ignore-not-found
kubectl --kubeconfig ~/.kube/msl-gcp-usc1-dev-1 -n ome-mc-test delete servingruntime mc-demo-tpu-jax --ignore-not-found
```

Delete the leaf before its ancestors, and note that the order is not advice: the
validating webhook covers `DELETE`, and a `Cohort` removed while its children are
still there is **refused**, because that delete orphans a subtree and the damage
is invisible from the deleted object alone. A node stays in the tree until its
finalizer releases, so a parent is deniable for as long as its child is
`Terminating`.

Deleting the leaf is already enough to take every Kueue object down. A leaf's
ancestors are projected only in service of that leaf, so `mc-demo` stops being
projected the moment `mc-demo-serving` leaves the tree, and the sweep removes the
`LocalQueue`, the `ClusterQueue` and the `Cohort` together. Each member keeps only
its own bare root, which the hub never projects and never reaps — capacity is a
property of the cluster rather than of anything an admin authored, and the
high-water marks every split is sized against live on that object.

> [!IMPORTANT]
> Delete the ISVCs first, as the block above does. Kueue holds
> `kueue.x-k8s.io/resource-in-use` on a `ClusterQueue` until nothing is reserving
> quota against it, so a queue swept while workloads are still admitted sits
> `Terminating` / `Active=Terminating` — by which point the `AcceleratorQuota`
> above it is gone from both the member and the hub.

Everything the prerequisites set up is harmless to leave — the runtime, the
namespace label, the WorkloadCluster labels, and Kueue's frameworks list, which
belongs to the members rather than to this demo.

## Design notes for anyone editing the fixtures

**Why no `spec.model`.** The runtime is pinned by name, which is the supported
lean path: the controller skips model fetch and runtime auto-selection. It also
skips injecting the `models.ome.io/clusterbasemodel.<name>=Ready` nodeSelector
that a model-agent would satisfy — and no node in either member carries such a
label, so a model reference would leave every pod `Pending` on node affinity with
the quota story invisible behind it.

**Why `spec.deploymentMode: OMENative` on every ISVC, and not the annotation.**
The members default to `RawDeployment`, and only OMENative dispatches through
`InferenceReplica`s — which is where the per-instance admitted flag the placer
reads lives. Under `RawDeployment` the placement never converges because nothing
reports admission.

`DetermineEngineDeploymentMode` resolves the mode from four things in order:
`spec.engine.annotations`, `spec.deploymentMode`, leader/worker shape, then
`RawDeployment`. **`metadata.annotations` is not among them**, and nothing copies
it into the field — so an ISVC carrying only the top-level
`ome.io/deploymentMode: OMENative` annotation claims OMENative and renders a
Deployment. A leader+worker engine picks up OMENative from the shape rung, which
is why multi-node fixtures appear to work on the annotation alone; a single-pod
engine does not.

> [!WARNING]
> On a **member** the object reads as though it contradicts itself. Measured with
> a server-side dry-run: the hub leaves annotations alone, but the member's
> defaulting webhook back-fills `ome.io/deploymentMode: RawDeployment` from its
> own configured default, against the `spec.deploymentMode: OMENative` on the
> same object. The field wins and the behaviour is OMENative — only the
> annotation lies. Check `{.spec.deploymentMode}`, or just look for
> `InferenceReplica`s.

**Why single-pod and not a multi-host gang.** A gang drags in `PodGroup`s, a
topology key and `ome-scheduler`'s co-location plugin, none of which the quota
plane touches. The admission story is identical and the failure surface is far
smaller. It also sidesteps the one real gap here: OME stamps the queue-name
label but no pod-group label, so Kueue funds a service **pod by pod** rather than
all-or-nothing. For a single-pod runtime that is the natural unit; a multi-host
gang would want group semantics and does not get them today.

**Why the runtime's nodeSelector is exactly the flavor's node labels.** Kueue
assigns a workload to a flavor only if the pod's own node selection is compatible
with that flavor's labels. Narrower is fine; contradictory makes the workload
unassignable, and it then sits pending with no quota charged and no obvious
reason. The TPU topology label is deliberately not selected — the two members
carve their fleets differently and the shapes they have free are not the same.

**Why 16/16 rather than a proportional split.** See
`fixtures/10-tree-explicit.yaml`. In short: the members are three orders of
magnitude apart, so proportional over 32 chips starves the small one below one
replica. 16/16 is also self-evidently not what the hardware would produce, which
is what makes the numbers on the members prove which policy ran.
