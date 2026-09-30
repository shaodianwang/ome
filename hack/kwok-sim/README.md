# OME on kind + kwok, with simulated TPU nodes

A local cluster that behaves like the reference TPU cluster for anything you can
answer by looking at where pods land: a real OME control plane — manager,
webhooks, `ome-scheduler` and `ome-quota-manager` — driving
[kwok](https://kwok.sigs.k8s.io)-simulated `tpu7x` nodes that carry that
cluster's real labels, taints and capacity.

There is no kubelet behind those nodes. Placement, topology, gang co-location,
packing and saturation are faithful. Anything that needs a container to actually
run is not — see [What this cannot tell you](#what-this-cannot-tell-you).

## Prerequisites

`docker` (running), `kind`, `kubectl`, `helm`, `jq`, and a Go toolchain for the
one-time image build. Helm 3 and 4 both work.

## Quick start

```sh
hack/kwok-sim/up.sh --with-isvcs             # first run: builds three images (slow)
hack/kwok-sim/up.sh --with-isvcs --skip-build  # ~95s once they exist
```

`--with-isvcs` applies `fixtures/2*-isvc-*.yaml`, waits for the pods, and prints
`status.sh`. Leave it off and `up.sh` stops after the model and runtimes so you
can pick a topology yourself:

```sh
hack/kwok-sim/up.sh --skip-build
kubectl apply -n kwok-sim -f hack/kwok-sim/fixtures/20-isvc-2x2x2.yaml
hack/kwok-sim/status.sh
```

```
== fleet ==
cube 1  4x4x4  nodes=16  chips=64
cube 2  2x2x2  nodes=16  chips=64

== inference services ==
NAME        URL                                                READY   BASEMODEL   RUNTIME
sim-2x2x2   http://sim-2x2x2.kwok-sim.svc.cluster.local:8080   True    demo-noop   tpu7x-2x2x2
sim-4x4x4   http://sim-4x4x4.kwok-sim.svc.cluster.local:8080   True    demo-noop   tpu7x-4x4x4

== gang placement (pods per node pool) ==
  2x  sim-2x2x2      b3p1-blk1-sblk02-2x2x2-0
  16x sim-4x4x4      b3p1-blk1-sblk01-4x4x4-0

== chip utilisation ==
  72 / 128 chips
```

Tear down with `hack/kwok-sim/down.sh`, which reads the same `--cluster` and
`OME_KWOK_CLUSTER` the bring-up does — so it deletes the cluster you actually
started, not whichever one the default names.

## Verifying gang spreading

`topologySpread` places replicas of one component in different fault domains,
rendered as a standard `topologySpreadConstraint` on each instance's leader —
the scheduler carries no spread configuration. On tpu7x the co-location
partition is gang-sized, so the fault domain is the cube
(`cloud.google.com/gce-topology-subblock`), named via `topologySpreadKey`. A
two-cube 2x2x2 layout demonstrates both shapes:

```sh
hack/kwok-sim/up.sh --cubes 2x2x2,2x2x2 --skip-build
kubectl apply -n kwok-sim -f hack/kwok-sim/fixtures/70-spread-required.yaml
hack/kwok-sim/verify-spread.sh sim-spread --distinct 2

kubectl apply -n kwok-sim -f hack/kwok-sim/fixtures/71-spread-balanced.yaml
hack/kwok-sim/verify-spread.sh sim-spread-balanced --distinct 2
```

The first fixture's two instances land in different cubes (packing alone
would put both in one). The second over-subscribes on purpose: three
instances, two cubes — maxSkew 1 is BALANCED spreading, so they place 2/1
and nothing stays Pending on balance alone. `Preferred` renders
ScheduleAnyway and is advisory only under this packing profile (the
spreading Score is disabled); use `Required` when spreading must hold.

## Components

`up.sh` builds and deploys three OME components. `--component` narrows that to
the ones you are working on; repeat it for more than one.

| component | image | chart | brings with it |
|---|---|---|---|
| `manager` | `ome-manager` | `ome-resources` | cert-manager, the model and runtime fixtures |
| `scheduler` | `ome-scheduler` | `ome-scheduler` | the scheduler-plugins `PodGroup` CRD |
| `quota-manager` | `ome-quota-manager` | `ome-quota-manager` | the Kueue CRDs, the `ResourceFlavor` fixture; Kueue itself with `--with-kueue` |

```sh
hack/kwok-sim/up.sh                                    # all three
hack/kwok-sim/up.sh --component quota-manager          # rebuild and re-roll one
hack/kwok-sim/up.sh --component manager --component scheduler
```

The simulator itself is ensured on every run whatever is selected — the kind
cluster, kwok and its Stages, the fake nodes, and the OME CRDs — so a single
component can be re-rolled against a cluster that is already up. Everything else
is gated on the selection: `--component quota-manager` installs no cert-manager
(the quota webhook manages its own cert) and touches neither the manager nor the
runtime fixtures.

`--no-scheduler` still means what it did: it subtracts `scheduler` from the
default set and switches `--topology` to `strict`. Combining it with an explicit
`--component scheduler` is an error rather than a silent winner.

The OME CRDs are the one exception to "gated on the selection". `ome-crd` is
installed unconditionally, because `charts/ome-quota-manager` can also install
the `AcceleratorQuota` CRD (`quotaManager.crd.install`) and two charts claiming
one cluster-scoped object fails whichever release claims it second — which one
that is would otherwise depend on the order of past runs.

## Node shapes

The unit is a **cube**: one tpu7x sub-block, 64 chips, 16 nodes of
`tpu7x-standard-4t` at 4 chips each. The reference TPU cluster carves each cube
into node pools by slice topology, and every node in a pool advertises that
pool's topology:

| carve   | chips/slice | nodes/pool | pools/cube |
|---------|------------:|-----------:|-----------:|
| `2x2x1` |           4 |          1 |         16 |
| `2x2x2` |           8 |          2 |          8 |
| `2x2x4` |          16 |          4 |          4 |
| `2x4x4` |          32 |          8 |          2 |
| `4x4x4` |          64 |         16 |          1 |

The default `--cubes 4x4x4,2x2x2` is two cubes = **32 nodes**: cube 1 kept whole
as a single 16-node `4x4x4` pool, cube 2 split into eight 2-node `2x2x2` pools.
One entry per cube, so `--cubes 4x4x4,4x4x4,2x2x2,2x2x1` is four cubes / 64
nodes. A production cluster has thousands of TPU nodes; a few hundred fake nodes
is comfortable, thousands is not (kind's IPAM hands each node a real `/24` out
of `10.244.0.0/16`).

Each node reproduces the reference cluster's shape: `google.com/tpu: 4`, 224 CPU, ~944Gi,
`node.kubernetes.io/instance-type=tpu7x-standard-4t`, the
`google.com/tpu=present:NoSchedule` taint, and the label set OME and the GKE
schedulers key on — `cloud.google.com/gke-tpu-accelerator`, `-tpu-topology`,
`-nodepool`, `gce-topology-block/-subblock/-host`, and the
`gke-tpu-partition-<topo>-id` slice id shared by every node in a pool.

Regenerate the manifest alone (no cluster) to see exactly what gets created:

```sh
hack/kwok-sim/gen-tpu-nodes.sh --cubes 4x4x4,2x2x2 --model demo-noop
```

## Making an InferenceService schedulable

Four things have to line up. All four are already wired into the fixtures; this
is what to change when you write your own.

**1. The model-ready node label.** For any non-PVC model, OME injects
`models.ome.io/clusterbasemodel.<name>=Ready` as a nodeSelector — the label the
model-agent DaemonSet normally writes. That DaemonSet is not installed here, so
`gen-tpu-nodes.sh --model <name>` bakes the label into every fake node instead.
Miss this and every pod sits Pending on "didn't match Pod's node
affinity/selector". Keep the model name ≤32 characters, the budget left inside
the 63-character label key.

**2. `hf://`, not `pvc://`.** A `pvc://` model routes to the PVC backend, which
spawns an `ome-agent` metadata-extraction Job against a real volume. A non-PVC
URI routes to the per-node backend, which needs nothing but the label above.
Nothing downloads, and the ISVC controller gates on model readiness only for
*sharded* models.

**3. Toleration plus topology nodeSelector on the runtime.** Fake nodes carry
the reference cluster's real `google.com/tpu=present:NoSchedule` taint, so an
untolerated pod sees zero capacity.

**4. A slice-co-location mechanism** — see the next section. A multi-host TPU
session spans one slice, so the whole gang has to land in one pool, and there
are two different ways to make that happen depending on which scheduler runs.

`leader` + `worker` on the engine is what selects OMENative mode; OME then
creates the gang's pods directly. No LeaderWorkerSet involved.

## ome-scheduler, and the two co-location modes

`ome-scheduler` is the upstream kube-scheduler built as a library with OME's
`OMEGangPack` placement plugin registered (OEP-0022). It installs as a *second*
scheduler into the `ome` namespace alongside the manager, and workloads opt in
per-runtime:

```yaml
kind: ClusterServingRuntime
spec:
  schedulerName: ome-scheduler     # back-filled onto engine, decoder and router
```

Nothing else claims those pods — if `ome-scheduler` is not running they stay
Pending forever rather than falling back to the default scheduler.

### Where slice co-location comes from

In production an ISVC with `leader` + `worker` sets `engineConfig.topologyKey`,
and OME does **two** things with that one field:

1. annotates the Instance's PodGroup with `ome.io/topology-key` — which is what
   `OMEGangPack` reads, and
2. injects a **required worker→leader podAffinity** on the same key.

Under the default scheduler both are right, and that is the contract OME's own
KIND suite asserts (`topology_podgroup_kind_test.go`, which pins
`SchedulerName` to the default). Under `ome-scheduler` the second one is fatal.
`--topology` picks how to resolve that:

| mode | PodGroup annotated | worker→leader affinity | schedules |
|---|---|---|---|
| `podgroup` — default with ome-scheduler | yes | overridden | yes |
| `plugin` | no | none | yes |
| `strict` — default with `--no-scheduler` | yes | yes (real) | only under the default scheduler |

`render-runtimes.sh <mode>` prints exactly what each one applies.

**`strict` + ome-scheduler is a silent, permanent wedge.** The worker's required
affinity cannot be satisfied while the leader is merely gang-reserved rather
than bound, so every candidate node is filtered:

> 16 node(s) didn't match pod affinity rules, 17 node(s) didn't satisfy
> plugin(s) [OMEGangPack] … gang reservation released after all candidate nodes
> were filtered

and after that one failure the gang is never retried — OEP-0022 lists custom
QueueSort/PreEnqueue and starvation reservations as design targets, not
implemented, so the scheduler just goes quiet. Observed: 16 pods Pending, last
scheduling decision two minutes earlier, no further attempts. Small gangs
sometimes win the race and schedule anyway, which is worse than a clean failure
— a 2-pod gang succeeded once and then failed on a restarted scheduler with the
same manifests. `up.sh --topology strict` (with the scheduler on) reproduces it.

`podgroup` is the default because it keeps the production PodGroup shape. It
gets there by declaring the worker's required term itself: OME defers to a
hand-written required term on the resolved topology key and injects nothing, and
the declared term anchors on a per-node DaemonSet so it is true in every pool
and constrains nothing. Co-location is then entirely `OMEGangPack`'s, off the
annotation. This is a workaround for the wedge above, not something a production
ISVC would carry.

`plugin` drops `engineConfig.topologyKey` altogether and relies on the chart's
fleet-wide `scheduler.plugin.topologyKey`. That works here only because
`cloud.google.com/gke-nodepool` is one slice in the reference cluster *and* is
the same key for every carve; `cloud.google.com/gke-tpu-partition-<topo>-id` is
the more literal "same slice" key but is topology-shaped, so a mixed-shape pool
would need the per-PodGroup annotation. In this mode the PodGroup carries no
annotation, which is the divergence from production.

`OMEGangPack` is fail-closed on topology: with neither the annotation nor the
plugin setting it refuses to place anything —

> PodGroup kwok-sim/sim-4x4x4-engine-0 must declare ome.io/topology-key or the
> scheduler must configure topologyKey

Expect `GangSplitRisk` events in `podgroup` and `plugin` modes. OME cannot see
the scheduler-side co-location, so from its point of view the workers have no
co-location affinity. Check `status.sh` placement, not the event.

The chart is written for a real fleet, so `up.sh` overrides it for one kind
node: 1 replica (the default 2 have a required hostname anti-affinity and a PDB),
no leader election, and 256Mi/1Gi instead of 4Gi/12Gi. It also forces
`percentageOfNodesToScore=100`, because `OMEGangPack` builds its per-domain
free-node counts from whatever upstream sampled and a partial view can rank a
filled slice above an empty one.

## Fleet quota

`ome-quota-manager` is the OEP-0024 quota plane: a separate binary and
Deployment that assembles the `AcceleratorQuota` tree, reports each node's
position and condition, and derives this cluster's accelerator capacity onto the
reserved root. `up.sh` runs it in **workload** mode — the half that owns a
serving cluster's own silicon — because that is what this simulates. Management
mode projects per-cluster shares onto members, and there are none here.

**`up.sh` creates no `AcceleratorQuota`.** A quota tree is the thing you are
here to write, so it is left to you exactly as the ISVCs are. Nothing is lost by
that: the controller creates the reserved `root` itself — bare, `Cohort`, no
parentRef, no budgets — as soon as capacity derivation is on, because capacity
is a property of the cluster rather than of anything an admin authored. So a
freshly-built cluster already shows the component working:

```
== accelerator quota ==
NAME   ROLE     PARENT   RESOURCE   FLAVOR   NOMINAL   ADMITTED   READY   DEGRADED   AGE
root   Cohort                                                     True    False      30s

== derived capacity (root) ==
  google.com/tpu on tpu7x: allocatable=128 highWater=128
```

The budget columns are blank because this root carries no budget, which is the
one shape they cannot describe: they report `status.budgets`, and a node with
nothing authored has none. Capacity has no column at all — it is the root's
alone, so `kubectl get aq` would print it blank on every other row. `status.sh`
reads it off the root, as above, and `kubectl qt -o wide` carries it as
`ALLOCATABLE` and `PEAK`.

Delete that root and it comes straight back. What an admin adds to it is the
*budget* — the fleet total, and the one number the controller will not invent.

The one fixture `up.sh` does apply is `fixtures/30-resourceflavor.yaml`: a Kueue
`ResourceFlavor` `tpu7x` selecting every fake node by
`cloud.google.com/gke-tpu-accelerator`. That is not a tree, it describes the
hardware — which this simulator owns — and without it the root reports no
capacity at all (see below).

`fixtures/31-acceleratorquota.yaml` is a tree to start from when you want one:
`root` → `sim-tenants` → two leaves, budgeting one cube between a serving and a
batch tenant, with two levels of grouping so containment is exercised at both.
It carries its own `root`, so it applies standalone — parents are admitted
before children, and a dangling `parentRef` is refused rather than held, so a
tree that did not bring one would be rejected node by node on a cluster that has
none. The enrolled namespaces it does *not* carry; `up.sh` creates those:

```sh
kubectl apply -f hack/kwok-sim/fixtures/31-acceleratorquota.yaml
hack/kwok-sim/status.sh
```

That apply warns that `acceleratorquotas/root` has no
`last-applied-configuration` annotation. Expected, not a fault: the controller
created it imperatively, and `kubectl` patches the annotation on as it goes.

```
== accelerator quota ==
NAME                  ROLE           PARENT        RESOURCE         FLAVOR   NOMINAL   ADMITTED   READY   DEGRADED   AGE
root                  Cohort                                                                      True    False      2m
sim-tenants           Cohort         root          google.com/tpu   tpu7x    64        0          True    False      5s
sim-tenants.batch     ClusterQueue   sim-tenants   google.com/tpu   tpu7x    16        0          True    False      5s
sim-tenants.serving   ClusterQueue   sim-tenants   google.com/tpu   tpu7x    48        0          True    False      5s
```

`NOMINAL` is what the tree authored, so it appears whether or not anything can
admit. `ADMITTED` is zero here and stays zero without `--with-kueue`: with only
the Kueue CRDs installed there is nothing to admit a workload, and an
InferenceService would not charge one of these queues anyway. See
[`--with-kueue`](#--with-kueue) for the Job that moves it.

**A ResourceFlavor is what makes capacity appear.** A flavor names a hardware
class by node labels, and OME reads flavors rather than authoring them — so
without one, every fake node's chips come back *unattributed* and the root's
`status.capacity` stays empty. That is deliberate: an unclaimed accelerator is
either a missing flavor or a labelling mistake, and both look identical to a
shrinking fleet unless they are named.

By default only the Kueue **CRDs** are installed, not Kueue itself. Nothing then
admits a workload, and the types are all the quota plane reads to derive
capacity. They are vendored by `make kueue-crds` from the version in
`.kueue-crd-version` — the one the fleet runs, which is ahead of the
`sigs.k8s.io/kueue` pin in `go.mod` — into the same gitignored
`tests/integration/dep-crds/` tree as the `PodGroup` CRD. When that version
cannot be fetched the target vendors nothing rather than an older schema.

### `--with-kueue`

`--with-kueue` installs Kueue itself — controller, webhooks and CRDs — so a
rendered `ClusterQueue` is judged by the thing that judges it in production
rather than by our reading of its API. It goes `Active`, gates pods, admits
them, and reports the usage the quota plane reads back.

It is worth understanding what this covers that nothing else does. Every other
Kueue-touching test links Kueue **in process from `go.mod`**, so they all run the
version OME *compiles* against. This runs the version the fleet *deploys*, taken
from `.kueue-crd-version`, so the controller and the CRDs move together. The two
differ deliberately and permanently: the pin is held back by the Kubernetes and
controller-runtime versions this repo is on.

Opt-in rather than the default, because it pulls an image and needs the
`quota-manager` component. Three details it has to get right, each of which
fails in a way that is easy to misread:

- **The image is saved platform-scoped and loaded as an archive**, unlike the
  images this script builds. Two reasons, and each fails in a way that reads as
  something else: a pulled multi-arch ref carries attestation manifests, so
  `kind load docker-image` fails with `content digest ... not found` — the digest
  it wants is an attestation, not a layer; and the platform is taken from the
  docker daemon rather than assumed, because exporting a platform the local
  image does not have fails with `no suitable export target found`, which looks
  like a registry problem rather than an architecture one. Override the ref with
  `KUEUE_IMAGE` if you have it somewhere reachable.
- **A ready pod is not a serving webhook**, and `helm --wait` only knows about
  the pod. Kueue generates its own certificate in process and its webhooks are
  `failurePolicy: Fail`, so until the listener accepts TLS every
  `ResourceFlavor` and `ClusterQueue` write is rejected with `connection
  refused`. `up.sh` probes it by attempting a server-side dry-run apply, not by
  watching endpoints: the pod passes its readiness probe before the webhook
  server has bound, so a ready `EndpointSlice` returns while the very next apply
  still fails.

The webhook needs no cert-manager: `quotaManager.webhook.internalCertManagement`
is on by default, so the component generates its own serving cert and writes its
own `caBundle`. It is `failurePolicy: Fail`, which is why `up.sh` waits for the
`caBundle` and for ready endpoints before applying the tree, and retries the
apply — the cert write and the webhook's first successful TLS accept are
independent events.

#### Making a tenant consume something

A tree with Kueue behind it still reports `admitted=0` until a workload charges
one of its queues, and nothing here does: an InferenceService takes OME's own
path and never passes through a LocalQueue. `fixtures/40-job-batch.yaml` is a
kwok Job that does — three pods of four chips against `sim-tenants.batch`'s 16.

```sh
kubectl apply -f hack/kwok-sim/fixtures/40-job-batch.yaml
kubectl get aq -o wide
```

```
NAME                  ROLE           PARENT        RESOURCE         FLAVOR   NOMINAL   ADMITTED   BORROWED   READY   DEGRADED   AGE
root                  Cohort                                                                                 True    False      4m
sim-tenants           Cohort         root          google.com/tpu   tpu7x    64        12         0          True    False      2m
sim-tenants.batch     ClusterQueue   sim-tenants   google.com/tpu   tpu7x    16        12         0          True    False      2m
sim-tenants.serving   ClusterQueue   sim-tenants   google.com/tpu   tpu7x    48        0          0          True    False      2m
```

The 12 appears on `sim-tenants` as well as on the leaf, because usage rolls up
the tree — a grouping reports what its subtree holds, not a budget of its own.
`kubectl qt` shows the same numbers with the shape around them, and carries the
root's capacity alongside:

```
google.com/tpu on flavor tpu7x

NAME                      ROLE          NOMINAL  ADMITTED  READY  AGE
root                      Cohort        -        -                4m
└─ sim-tenants            Cohort        64       12               2m
   ├─ sim-tenants.batch   ClusterQueue  16       12               2m
   └─ sim-tenants.serving ClusterQueue  48       0                2m
```

The root's dashes are the same fact the flat list shows as blanks: it carries no
budget. The `(resource, flavor)` columns collapse into the caption because the
whole tree uses one pair.

The Job's pods must be held Running to keep holding chips. `stage-fast` ships
`pod-complete` with no delay at all, so a Job's pod succeeds the moment it is
Running; `up.sh` patches the stage to read
`pod-complete.stage.kwok.x-k8s.io/delay` off the pod, and the fixture sets it to
`60m`. Without that patch the annotation is inert and the usage is gone before
the next read. The stage selects Job-owned pods only, so no ISVC is affected.

Apply a second copy under another name and the 24 does not fit. Kueue funds
neither of the two pods it cannot pay for, that Job stays suspended, and
`ADMITTED` holds at 12:

```sh
sed 's/name: kwok-batch/name: kwok-batch-2/' hack/kwok-sim/fixtures/40-job-batch.yaml | kubectl apply -f -
kubectl -n kwok-sim-batch get jobs,workloads
```

Capacity moves with the fleet, and the two numbers move differently.
`--cubes 4x4x4` alone reports 64 chips; cordoning one node of the default 32
takes `allocatable` to 124 and leaves `highWater` at 128:

```sh
kubectl cordon gke-tpu-<pool>-00
kubectl get aq root -o jsonpath='{.status.capacity}' | jq .
```

`allocatable` is what is schedulable now. The mark tracks *installed* capacity —
allocatable plus whatever is parked on cordoned or NotReady nodes — so a cordon
does not move it at all, because the hardware is still in the rack. Hardware
genuinely leaving does move it, judged against the hysteresis band. Budgets are
checked against the mark, which is why a drain never marks a tenant Degraded.

That distinction is easy to get backwards, and this simulator is a good place to
see it: cordon the whole fleet and the mark holds at 128 while `allocatable`
goes to 0; delete the nodes and the mark follows them down.

## Three clusters: `fleet-up.sh`

`up.sh` is one cluster. `fleet-up.sh` is a **hub and two workload members**,
wired the way a real deployment is — which is the only way to exercise the
transport, a member-issued credential, and a split taken against capacity the
members derived from their own hardware rather than numbers a fixture wrote.

```sh
export KUBECONFIG="$(mktemp -t ome-kwok-fleet-XXXXXX)"
hack/kwok-sim/fleet-up.sh                                   # quota plane only
hack/kwok-sim/fleet-up.sh --with-kueue --with-placement      # placement + admission
hack/kwok-sim/fleet-up.sh --with-kueue --with-routing        # also publish TrafficMaps
hack/kwok-sim/fleet-status.sh                               # what landed where
hack/kwok-sim/fleet-down.sh
```

Keep the dedicated kubeconfig for subsequent fleet commands and rebuilds. If
the clusters already exist, populate that file before running the scripts:

```sh
for cluster in ome-quota-hub ome-quota-a ome-quota-b; do
  kind export kubeconfig --name "$cluster" --kubeconfig "$KUBECONFIG"
done
```

The two members are deliberately unequal — two cubes against one, 128 chips
against 64 — so a proportional split has to produce different numbers on each,
and a bug that handed both the fleet total is visible rather than plausible.

### What goes where

Component selection reaches the members only; the hub's component set is
fixed, because a management plane holds no silicon and schedules nothing.
`--with-routing` configures publication on the hub.

| | hub `ome-quota-hub` | member `ome-quota-a` | member `ome-quota-b` |
|---|---|---|---|
| kind cluster, OME CRDs | ✅ | ✅ | ✅ |
| kwok + fake TPU nodes | — `--no-nodes` | ✅ 2 cubes / 128 chips | ✅ 1 cube / 64 chips |
| cert-manager | ✅ | ✅ | ✅ |
| `ome-manager` | ✅ `control-plane` role | ✅ | ✅ |
| `PodGroup` CRD | ✅ | ✅ | ✅ |
| model + runtimes, ns `kwok-sim` | ✅ (inert) | ✅ | ✅ |
| the enrolled namespaces | ✅ | ✅ | ✅ |
| `ome-scheduler` | — | 🅿️ | 🅿️ |
| `ome-quota-manager` | ✅ **management** mode | ✅ **workload** mode | ✅ **workload** mode |
| Kueue CRDs + `ResourceFlavor` | — | ✅ | ✅ |
| Kueue controller | — | 🚦 | 🚦 |
| `ome-quota-access` SA + token | — | ✅ | ✅ |
| placement grant on the SA | — | 🅿️ | 🅿️ |
| `WorkloadCluster` CRs | ✅ `member-a`, `member-b` | — | — |
| placement member namespace + capacity config | ✅ | — | — |
| HTTPRoute CRD + TrafficMap publication | `--with-routing` | — | — |

🅿️ `--with-placement`  🚦 `--with-kueue`

Three asymmetries are worth internalising:

- **The hub runs `ome-manager`, and must.** The `WorkloadCluster` registry
  reconciler lives there and is the single writer of the `Ready` condition every
  remote client waits on. The `control-plane` role simultaneously switches *off*
  the local InferenceService, BaseModel and ServingRuntime controllers, which is
  why the model and runtime objects it installs sit inert.
- **The quota manager runs in a different mode on each side.** Members derive
  capacity from local hardware and materialize Kueue objects; the hub only
  projects shares, never touching a Kueue object — which is why it gets no Kueue
  and no `ResourceFlavor`.
- **The hub carries no silicon and no scheduler.** Nothing to schedule, so kwok,
  the fake nodes and `ome-scheduler` would all be dead weight.

### `--with-placement`

The quota plane is only half of what a fleet does. `--with-placement` adds the
other half — OEP-0018 fan-out — by doing three things the flag exists to make
unforgettable:

1. Adds `manager` and `scheduler` to the member component set. The manager
   reconciles a derived ISVC into pods; the scheduler is needed because the
   runtimes render with `schedulerName: ome-scheduler` under the default
   topology mode, so without it every gang sits Pending.
2. Widens the members' grant. One `WorkloadCluster` carries one credential and
   the placer shares it with the projector, so placing as well as budgeting
   means binding the `ome-resources` chart's own `ome-multicluster-access`
   ClusterRole onto the `ome-quota-access` ServiceAccount the members already
   issued. The rules are the chart's, not a copy — the fleet exercises the grant
   a real control plane is given.
3. Labels each `WorkloadCluster` `accelerator=tpu7x` and `zone=a` / `zone=b`.
   `spec.placement.clusterAffinity` matches those labels and can also constrain
   the registration's actual `metadata.name`. Omitted affinity matches every
   registration; an ISVC without `spec.placement` remains local.

Every `6*-placement-*.yaml` sets `spec.placement.policy: ClusterAffinity`, the
OEP-0018.4 contract. Without it the placement is Legacy, which rejects
`clusterAffinity`: the ISVC holds `Pending` with `PlacementInputValid=False`,
reason `InvalidPlacementIntent`, and nothing fans out.

The hub is also configured for `SplitByCapacity`. `up.sh --multicluster-role
control-plane` points `placement.memberOperatorNamespace` at `ome`, where this
script installs every member, and sets a `placement.capacity` block that reads
the fleet report the management-mode quota manager keeps on the hub's `root`.
Without that block the mode holds with `PlacementCapacityFresh=False`, reason
`CapacityUnknown`.

`--topology` becomes forwardable under this flag, since it is what puts a
scheduler on the members for the mode to mean anything.

**Apply `50-fleet-proportional.yaml` first.** Each `6*-placement-*.yaml` names
`sim-tenants.serving` in its `ome.io/local-queue` annotation, and a workload
naming a `LocalQueue` that does not exist is never queued — its pods are created,
gated, and stay there. Every `5*-fleet-*.yaml` provides that leaf, but only 50
funds a whole cube of it on both members: 51 gives member A 32 chips and 52
gives member B 32, half a replica, so that home stays gated. `67` is the
exception and needs `52-fleet-mixed.yaml` for its second leaf.

The workload names the leaf because nothing else can. `placement.localQueue` —
one operator-wide string stamped on every derived workload — is empty by default,
since a `LocalQueue` is named for the quota leaf it belongs to and no single name
addresses more than one tenant. Placement resolves the annotation first and that
default second, so until the ISVC→(queue, priority) resolver lands, naming the
leaf on the workload is the mechanism. `67-placement-two-tenants.yaml` is the
case that makes it visible: two tenants in one namespace, told apart only by the
leaf each names.

Drop the annotation to place *without* quota — the derived pods then carry no
`queue-name` label at all, Kueue leaves them alone, and they run ungoverned.

```sh
hack/kwok-sim/fleet-up.sh --with-kueue --with-placement
kubectl --context kind-ome-quota-hub apply -f hack/kwok-sim/fixtures/50-fleet-proportional.yaml
kubectl --context kind-ome-quota-hub apply -f hack/kwok-sim/fixtures/63-placement-split-spread.yaml
```

| fixture | what it shows | expect |
|---|---|---|
| `61-placement-all.yaml` | every member serves | 16 pods each, no winner |
| `62-placement-single.yaml` | race, then sweep the loser | one winner, read it |
| `63-placement-split-spread.yaml` | 3 replicas = 192 chips = the whole fleet | 2 on a, 1 on b |
| `64-placement-split-capped.yaml` | equal shares with a per-cluster ceiling | 1 each |
| `65-placement-pinned.yaml` | `clusterAffinity.matchFields` selects `metadata.name=member-b` | member-b only |
| `66-placement-nomatch.yaml` | a requirement nothing carries | never placed |
| `67-placement-two-tenants.yaml` | two leaves in one namespace, on `52` | 8 admitted per leaf per member; Pending, no 2x2x2 slice |
| `68-placement-misrouted-queue.yaml` | a queue no tree provisions | gated on both, `AllocationShortfall` on the hub |
| `69-placement-split-by-capacity.yaml` | 3 replicas weighted by each member's hardware | 2 on a, 1 on b, each with a capacity sample |

One replica is one whole cube — the `tpu7x-4x4x4` runtime is 1 leader + 15
workers = 16 pods = 16 nodes = 64 chips — so `50-fleet-proportional`'s 128/64
split is exactly two replicas and one. They all charge the same leaf, so run one
at a time.

Which member wins `62` is not fixed and no fixture can make it so: the race is
decided by whichever member's Kueue admits first. Read the winner rather than
assuming one.

```sh
kubectl --context kind-ome-quota-hub -n kwok-sim get isvc \
  -o custom-columns=NAME:.metadata.name,PHASE:.status.placement.phase,WINNER:.status.placement.cluster
```

### `--with-routing`

`--with-routing` implies `--with-placement` and enables the hub's TrafficMap
controller and Gateway API publisher. It installs the HTTPRoute CRD from the
Gateway API version pinned in `go.mod` before starting the manager. Keep this
flag on subsequent `fleet-up.sh` invocations, including `--skip-build` runs;
omitting it restores the chart's routing-disabled default.

```sh
# Use the dedicated fleet KUBECONFIG from above.
hack/kwok-sim/fleet-up.sh --with-kueue --with-routing --skip-build
kubectl --context kind-ome-quota-hub -n kwok-sim get trafficmaps,httproutes
```

[routing-values.yaml](routing-values.yaml) supplies the Helm settings under
`ome.multicluster.config`: `routing.enabled=true`, publisher `gatewayapi`,
global hostname template `{{.Name}}.{{.Namespace}}.routing.example.com`, Gateway
reference `kwok-sim/local-gateway`, and backend port `8080`. The chart writes
them into the hub's `ome/inferenceservice-config` ConfigMap, under
`data.multicluster`, and rolls the manager when that configuration changes.
Edit the values file to configure these settings for a different simulation.

For a placed `place-all` ISVC, inspect the corresponding objects:

```sh
kubectl --context kind-ome-quota-hub -n kwok-sim get trafficmap place-all -o yaml
kubectl --context kind-ome-quota-hub -n kwok-sim get httproute place-all-global -o yaml
```

The TrafficMap records each member's weight. Check `Published=True`,
`status.observedTrafficMapGeneration == metadata.generation`, and that
`spec.placementPlanID` matches the source ISVC's `status.placement.plan.id`
before treating that plan as published. A ready member can carry positive
weight while an unready member has weight zero. This exercises the publication
acknowledgement used by placement transitions.

This setup tests publication only: the Gateway reference is a test parent,
and no Gateway controller, xDS publisher, or HTTP data plane is installed.
`Published=True` confirms the routing objects were written, not that a gateway
accepted them or that inference requests can be served. KWOK simulates pod
readiness, so active HTTP probes and capacity polling stay disabled.

## What it is good for

Saturation and packing behave correctly, which is the main reason to have this.
Nine `2x2x2` gangs into cube 2's eight slices:

```
  2x  sim-2x2x2-1..8   b3p1-blk1-sblk02-2x2x2-0 .. -7     # 8 gangs, one per slice
  0/1 nodes            sim-2x2x2-9                        # Pending
```

```
0/33 nodes are available: 16 Insufficient google.com/tpu,
17 node(s) didn't match Pod's node affinity/selector.
```

— 16 `2x2x2` nodes full, 17 rejected on topology (the 16 `4x4x4` nodes plus the
real kind node). Fast enough to iterate on: a full teardown-to-first-pod cycle
is about two minutes.

## What this cannot tell you

There is no kubelet, so kwok fakes pod status outright:

- **Probes never run and images never pull.** A bogus image reaches `1/1
  Running`; a `readinessProbe` with a one-hour delay is ignored. ImagePullBackOff
  and CrashLoopBackOff escalation cannot be reproduced. For those, use the real
  KIND suite (`make test-kind`).
- **Termination is instant.** `stage-fast`'s `pod-delete` force-deletes on
  `deletionTimestamp`, so `terminationGracePeriodSeconds` is effectively 0 and
  drain races, preStop windows and slow-terminate ordering are invisible.
- **`kubectl top` and CPU/memory HPA never work.** An HPA is created regardless
  (the default autoscaler class is HPA), and reports `<unknown>` forever. Noise,
  not a fault.
- **Top-level ISVC `Ready` is not a placement signal.** An ISVC whose gang is
  entirely Pending still reports `Ready=True`, because the engine's readiness
  floor resolves to `min 0`. The per-Component condition is accurate — read that,
  or read pod phase:

  ```sh
  kubectl -n kwok-sim get isvc <name> \
    -o jsonpath='{range .status.components.engine.lifecycle.conditions[*]}{.type}={.status} ({.reason}) {.message}{"\n"}{end}'
  ```

- **Only whole-slice gangs are modelled.** Sub-slice placement — a gang smaller
  than a slice needing a contiguous sub-torus — is not something OME does today
  (OEP-0022 defers it), so the simulator cannot show it either.

Also expect kube-proxy and kindnet pods faked onto all 32 nodes: both DaemonSets
tolerate everything. Harmless.

## Two traps worth knowing

Both are handled by the scripts; they will bite you if you build this by hand.

**Fake nodes must carry their own InternalIP.** kwok stamps `--node-ip`, which
the chart wires to the kwok controller pod's own address. Leave `.status`
unset and all 32 nodes report the real kind node's IP. kindnetd identifies "the
node I am running on" by matching that IP, picks a fake node, and rewrites the
*real* node's CNI conflist with the *fake* node's podCIDR — after which real pods
get addresses nothing routes. The addresses must also sit inside the kind bridge
subnet; an off-link range leaves kindnetd in a permanent route-reconcile failure
loop. `gen-tpu-nodes.sh` derives the subnet from the real node and assigns
`<prefix>.100+.x`.

That is also why nodes are created with `kubectl create` and a full `.status`:
the apiserver honours status on POST but drops it on apply, and a node that
reaches kwok without capacity gets node-initialize's default of 1k CPU / 1Ti /
no accelerators — a node that accepts any pod, quietly voiding every scheduling
result you then measure.

**`stage-fast` is required, and `pod-complete` is not what you want.** The kwok
chart alone creates zero Stages and nothing ever goes Ready. Install both, under
*distinct* release names — the common recipe installs each as `kwok`, which
collides in one namespace. `stage-fast`'s own `pod-complete` Stage is safe: it
selects `ownerReferences[].kind == Job`, so serving pods stay Running. A
hand-written `pod-complete` without that selector — the one in the Kueue recipe —
completes every Running pod in the cluster, serving pods included. Do not apply
it here.

## Layout

| path | what |
|---|---|
| `up.sh` | one-shot: kind → kwok → nodes → dependency CRDs → cert-manager → OME CRDs → the selected components → fixtures |
| `fleet-up.sh` | three clusters: a hub plus two unequal members, registered and wired |
| `fleet-status.sh` / `fleet-down.sh` | what landed where across the fleet; tear it down |
| `render-runtimes.sh` | prints the runtimes adjusted for one `--topology` mode |
| `gen-tpu-nodes.sh` | emits the node manifest; no cluster contact |
| `apply-tpu-nodes.sh` | creates them (`--delete` to remove) |
| `status.sh` | fleet, gang placement, chip utilisation, pending pods, quota tree |
| `fixtures/00-model.yaml` | `ClusterBaseModel` needing no bytes on disk |
| `fixtures/10-runtime-tpu.yaml` | multi-node `ClusterServingRuntime`, one per carve |
| `fixtures/2*-isvc-*.yaml` | one slice, and one whole cube |
| `fixtures/30-resourceflavor.yaml` | the Kueue `ResourceFlavor` the fleet's chips attribute to |
| `fixtures/31-acceleratorquota.yaml` | a four-node quota tree over one cube; not applied by `up.sh` |
| `fixtures/40-job-batch.yaml` | a kwok Job charging the `sim-tenants.batch` leaf, so the tree reports usage |
| `fixtures/5*-fleet-*.yaml` | fleet quota trees for `fleet-up.sh`; not applied by it |
| `fixtures/60-placement-rbac.yaml` | binds the chart's placement ClusterRole onto the members' quota SA |
| `fixtures/6[1-9]-placement-*.yaml` | one OEP-0018.4 placement scenario each; needs `--with-placement` |

## Install notes

Things `up.sh` does that are not obvious, if you are reproducing it by hand:

- **kind inherits your shell's proxy variables.** A proxy on `localhost` is the
  *host's* loopback, not the node's, so every in-cluster pull dies with
  `proxyconnect … connection refused`. `up.sh` clears the proxy vars for
  `kind create` only; set `KWOK_SIM_KEEP_PROXY=1` if your node genuinely needs
  one.
- **Every image must be built locally.** The charts default to an internal
  registry, so `up.sh` builds `ome-manager:kind-local`,
  `ome-scheduler:kind-local` and `ome-quota-manager:kind-local` and `kind load`s
  them, with `--set global.hub=""` so the charts render the bare tags that
  resolve to them. `--component` builds only what it selects.
- **A rebuild alone does not restart anything.** The tag is reused, so the
  Deployment's image ref is byte-identical: `docker build` succeeds, `kind load`
  succeeds, `helm upgrade` reports no change, and the pod goes on serving the
  previous binary. `up.sh` stamps the image ID into a pod annotation so a
  rebuild is visible to the Deployment — and only a real one, so re-running with
  an unchanged image still rolls nothing. Without that, `--component` would
  quietly redeploy stale code, which is the one thing it exists not to do.
- **Helm runs twice, and pass 1 is expected to fail.** The chart registers
  `failurePolicy: Fail` webhooks and, in the same apply, a `ClusterServingRuntime`
  those webhooks intercept — impossible before the controller serves and
  cert-manager's cainjector has replaced the placeholder caBundle. Pass 2 is
  retried, because a Deployment reporting Available can still refuse admission
  connections: the readiness probe answers on the health port before the webhook
  server binds 9443.
- **Helm 4 needs `--force-conflicts`.** It applies server-side, so its
  placeholder caBundle collides with the field cainjector owns. Helm 3 does not.
- **The release namespace must be `ome`.** The `inferenceservice-config`
  ConfigMap name, every webhook's `inject-ca-from`, and the manager's
  leader-election namespace all assume it.
- **`acceleratorResources` names `google.com/tpu`.** OME counts accelerators
  only by the names listed there, and the chart lists `nvidia.com/gpu` alone,
  so a TPU gang would measure zero accelerator demand and capacity placement
  could not size it. Listing it also puts `PARALLELISM_SIZE` on multi-node pods.
  The quota manager's `capacity.resources` is narrowed the same way: every
  listed resource gets a row per flavor, so the chart's `nvidia.com/gpu` would
  otherwise show on the root as an explicit zero pool under `tpu7x`.
- **`modelAgent`, `prometheus` and `multiclusterAccess` are switched off**, and
  the controller is dropped to one replica — the chart default of 3 × (2 CPU /
  4Gi) does not fit one kind node.
- **kind runs 1.35, not 1.34.** `charts/ome-scheduler` guards
  `kubeVersion: ">=1.35.0-0 <1.36.0-0"` and will not render otherwise: the
  scheduler is kube-scheduler-as-a-library, so its plugin API is pinned to one
  minor (`scheduler/go.mod` tracks k8s 1.35.4) and the node has to match.
  `--no-scheduler` frees you to run any version.
- **There are no committed PodGroup or Kueue CRDs to point at.** Both are
  vendored into a gitignored directory
  (`tests/integration/dep-crds/{scheduler-plugins,kueue}/`). Only the
  hand-vendored bundles there — `envoy-gateway`, `prometheus-operator` — are
  tracked, each with a `SOURCE` file. `up.sh` runs `make scheduler-plugins-crds`
  or `make kueue-crds` when one is missing. `scheduler-plugins` copies from the
  `go.mod` module in the local module cache and needs no network; `kueue` reads
  `.kueue-crd-version` and vendors nothing when it cannot fetch that version.
  `PodGroup` is required for `ome-scheduler` and merely wanted otherwise
  (without it OME stamps `GangSchedulingUnavailable=True` and a gang can land
  partially); the Kueue CRDs are required for `ome-quota-manager`, which cannot
  attribute capacity to a flavor it cannot read.

## Refreshing the node shape

The capacity and label set in `gen-tpu-nodes.sh` are transcribed from a live
node of the reference TPU cluster. To re-derive them:

```sh
export KUBECONFIG=<path to a tpu7x cluster kubeconfig>
NODE=$(kubectl get nodes -l cloud.google.com/gke-tpu-topology=4x4x4 \
        -o jsonpath='{.items[0].metadata.name}')
kubectl get node "$NODE" -o json | jq '{labels: .metadata.labels, taints: .spec.taints,
                                        capacity: .status.capacity, allocatable: .status.allocatable}'
```
