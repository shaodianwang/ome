# OME on kind + kwok, with simulated rc-1 TPU nodes

A local cluster that behaves like `msl-gcp-usc1-rc-1` for anything you can answer
by looking at where pods land: a real OME control plane — manager, webhooks,
`ome-scheduler` and `ome-quota-manager` — driving
[kwok](https://kwok.sigs.k8s.io)-simulated `tpu7x` nodes that carry rc-1's real
labels, taints and capacity.

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

## The fleet

The unit is a **cube**: one tpu7x sub-block, 64 chips, 16 nodes of
`tpu7x-standard-4t` at 4 chips each. rc-1 carves each cube into node pools by
slice topology, and every node in a pool advertises that pool's topology:

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
nodes. rc-1 itself is 2044 TPU nodes across 32 `4x4x4`, 80 `2x2x2` and 16
`2x2x1` sub-blocks; a few hundred fake nodes is comfortable, thousands is not
(kind's IPAM hands each node a real `/24` out of `10.244.0.0/16`).

Each node reproduces rc-1's shape: `google.com/tpu: 4`, 224 CPU, ~944Gi,
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
rc-1's real `google.com/tpu=present:NoSchedule` taint, so an untolerated pod
sees zero capacity.

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
`cloud.google.com/gke-nodepool` is one slice in rc-1 *and* is the same key for
every carve; `cloud.google.com/gke-tpu-partition-<topo>-id` is the more literal
"same slice" key but is topology-shaped, so a mixed-shape pool would need the
per-PodGroup annotation. In this mode the PodGroup carries no annotation, which
is the divergence from production.

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
It carries its own tenant namespaces, so it applies standalone:

```sh
kubectl apply -f hack/kwok-sim/fixtures/31-acceleratorquota.yaml
hack/kwok-sim/status.sh
```

That apply warns that `acceleratorquotas/root` has no
`last-applied-configuration` annotation. Expected, not a fault: the controller
created it imperatively, and `kubectl` patches the annotation on as it goes.

```
== accelerator quota ==
NAME          ROLE           PARENT        RESOURCE         FLAVOR   NOMINAL   ADMITTED   READY   DEGRADED   AGE
root          Cohort                                                                      True    False      2m
sim-batch     ClusterQueue   sim-tenants   google.com/tpu   tpu7x    16        0          True    False      5s
sim-serving   ClusterQueue   sim-tenants   google.com/tpu   tpu7x    48        0          True    False      5s
sim-tenants   Cohort         root          google.com/tpu   tpu7x    64        0          True    False      5s
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
kwok Job that does — three pods of four chips against `sim-batch`'s 16.

```sh
kubectl apply -f hack/kwok-sim/fixtures/40-job-batch.yaml
kubectl get aq -o wide
```

```
NAME          ROLE           PARENT        RESOURCE         FLAVOR   NOMINAL   ADMITTED   BORROWED   READY   DEGRADED   AGE   PATH
root          Cohort                                                                                 True    False      4m    /root
sim-batch     ClusterQueue   sim-tenants   google.com/tpu   tpu7x    16        12         0          True    False      2m    /root/sim-tenants/sim-batch
sim-serving   ClusterQueue   sim-tenants   google.com/tpu   tpu7x    48        0          0          True    False      2m    /root/sim-tenants/sim-serving
sim-tenants   Cohort         root          google.com/tpu   tpu7x    64        12         0          True    False      2m    /root/sim-tenants
```

The 12 appears on `sim-tenants` as well as on the leaf, because usage rolls up
the tree — a grouping reports what its subtree holds, not a budget of its own.
`kubectl qt` shows the same numbers with the shape around them, and carries the
root's capacity alongside:

```
google.com/tpu on flavor tpu7x

NAME               ROLE          NOMINAL  ADMITTED  READY  AGE
root               Cohort        -        -                4m
└─ sim-tenants     Cohort        64       12               2m
   ├─ sim-batch    ClusterQueue  16       12               2m
   └─ sim-serving  ClusterQueue  48       0                2m
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
| `render-runtimes.sh` | prints the runtimes adjusted for one `--topology` mode |
| `gen-tpu-nodes.sh` | emits the node manifest; no cluster contact |
| `apply-tpu-nodes.sh` | creates them (`--delete` to remove) |
| `status.sh` | fleet, gang placement, chip utilisation, pending pods, quota tree |
| `fixtures/00-model.yaml` | `ClusterBaseModel` needing no bytes on disk |
| `fixtures/10-runtime-tpu.yaml` | multi-node `ClusterServingRuntime`, one per carve |
| `fixtures/2*-isvc-*.yaml` | one slice, and one whole cube |
| `fixtures/30-resourceflavor.yaml` | the Kueue `ResourceFlavor` the fleet's chips attribute to |
| `fixtures/31-acceleratorquota.yaml` | a four-node quota tree over one cube; not applied by `up.sh` |
| `fixtures/40-job-batch.yaml` | a kwok Job charging the `sim-batch` leaf, so the tree reports usage |

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

## Refreshing the node shape from rc-1

The capacity and label set in `gen-tpu-nodes.sh` are transcribed from a live
rc-1 node. To re-derive them:

```sh
export KUBECONFIG=~/.kube/msl-gcp-usc1-rc-1
NODE=$(kubectl get nodes -l cloud.google.com/gke-tpu-topology=4x4x4 \
        -o jsonpath='{.items[0].metadata.name}')
kubectl get node "$NODE" -o json | jq '{labels: .metadata.labels, taints: .spec.taints,
                                        capacity: .status.capacity, allocatable: .status.allocatable}'
```
