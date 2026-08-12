# OME on kind + kwok, with simulated rc-1 TPU nodes

A local cluster that behaves like `msl-gcp-usc1-rc-1` for anything you can answer
by looking at where pods land: a real OME control plane — manager, webhooks, and
`ome-scheduler` — driving [kwok](https://kwok.sigs.k8s.io)-simulated `tpu7x`
nodes that carry rc-1's real labels, taints and capacity.

There is no kubelet behind those nodes. Placement, topology, gang co-location,
packing and saturation are faithful. Anything that needs a container to actually
run is not — see [What this cannot tell you](#what-this-cannot-tell-you).

## Prerequisites

`docker` (running), `kind`, `kubectl`, `helm`, `jq`, and a Go toolchain for the
one-time image build. Helm 3 and 4 both work.

## Quick start

```sh
hack/kwok-sim/up.sh --with-isvcs             # first run: builds two images (slow)
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

Tear down with `kind delete cluster --name ome-kwok`.

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
| `up.sh` | one-shot: kind → kwok → nodes → PodGroup CRD → cert-manager → OME → ome-scheduler → fixtures |
| `render-runtimes.sh` | prints the runtimes adjusted for one `--topology` mode |
| `gen-tpu-nodes.sh` | emits the node manifest; no cluster contact |
| `apply-tpu-nodes.sh` | creates them (`--delete` to remove) |
| `status.sh` | fleet, gang placement, chip utilisation, pending pods |
| `fixtures/00-model.yaml` | `ClusterBaseModel` needing no bytes on disk |
| `fixtures/10-runtime-tpu.yaml` | multi-node `ClusterServingRuntime`, one per carve |
| `fixtures/2*-isvc-*.yaml` | one slice, and one whole cube |

## Install notes

Things `up.sh` does that are not obvious, if you are reproducing it by hand:

- **kind inherits your shell's proxy variables.** A proxy on `localhost` is the
  *host's* loopback, not the node's, so every in-cluster pull dies with
  `proxyconnect … connection refused`. `up.sh` clears the proxy vars for
  `kind create` only; set `KWOK_SIM_KEEP_PROXY=1` if your node genuinely needs
  one.
- **Both images must be built locally.** The charts default to an internal
  registry, so `up.sh` builds `ome-manager:kind-local` and
  `ome-scheduler:kind-local` and `kind load`s them, with `--set global.hub=""`
  so the charts render the bare tags that resolve to them.
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
- **There is no committed PodGroup CRD to point at.** It is vendored out of the
  `sigs.k8s.io/scheduler-plugins` module already in `go.mod` into a gitignored
  directory (`tests/integration/dep-crds/scheduler-plugins/`). Only the
  hand-vendored bundles there — `envoy-gateway`, `prometheus-operator` — are
  tracked, each with a `SOURCE` file. `up.sh` runs `make scheduler-plugins-crds`
  when the file is missing; that target copies from the local module cache, so
  it needs no network. The CRD is required for `ome-scheduler` and merely wanted
  otherwise (without it OME stamps `GangSchedulingUnavailable=True` and a gang
  can land partially).

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
