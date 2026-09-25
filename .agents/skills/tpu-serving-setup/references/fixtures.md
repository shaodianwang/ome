# The in-tree TPU fixtures and how to edit them

All paths are relative to the repository root. Read the comment block at the
top of each file before changing a field; the notes below say only what you
need in order to make a personal copy safely.

## The multi-host pair (real silicon)

| File | Kind | Scope | What it is |
|---|---|---|---|
| `hack/quota-mc-demo/fixtures/40-runtime-tpu-jax.yaml` | `ServingRuntime` `mc-demo-tpu-jax` | namespaced (`ome-mc-test`) | leader + worker on `tpu7x` / `2x2x2`, `schedulerName: ome-scheduler`, `google.com/tpu: "4"` per pod, JAX liveness API on 8080 with an HTTP readiness probe |
| `hack/quota-mc-demo/fixtures/41-isvc-tpu-jax.yaml` | `InferenceService` `mc-demo-jax` | namespaced (`ome-mc-test`) | `deploymentMode: OMENative`, `runtime.kind: ServingRuntime`, `engine.topologyKey: cloud.google.com/gke-tpu-partition-2x2x2-id`, slice annotation, `leader` + `worker.size: 1`, one replica |

Fields that are load-bearing (dropping any one fails quietly):

- **`nodeSelector` on both roles** - `gke-tpu-accelerator: tpu7x` and
  `gke-tpu-topology: 2x2x2`. The topology must be a shape the cluster has
  free; a shape it lacks leaves the pods `Pending` forever.
- **toleration `google.com/tpu` / `NoSchedule`** - the TPU nodes are tainted;
  without it the pods see zero capacity.
- **`schedulerName: ome-scheduler` per role** - the gang is placed together or
  not at all. Both roles must name it; a top-level runtime `schedulerName` is
  not what OMENative pods run under.
- **`topologyKey` on the ISVC** - co-locates leader and worker in one slice;
  OME also generates the worker-to-leader affinity from it. Without it the
  two pods can land in different slices, the mesh never forms, and the leader
  hangs in JAX initialisation instead of failing.
- **the same image on both roles** - a mixed leader/worker image can run for
  a long time before failing in a misleading way.
- **`/dev/shm` `emptyDir` medium `Memory`** on both roles.
- **the readiness probe on the leader only** - the worker serves nothing and
  is never probed; it initialises JAX and holds the mesh up.
- **the container commands trap `TERM`** - a bare `sleep` as PID 1 ignores
  SIGTERM and the pod holds its chips for the whole grace period on delete.
- **TPU environment is injected, not set by hand** - GKE and OME supply
  `TPU_WORKER_ID`, `TPU_PROCESS_ADDRESSES` and friends; do not call
  `jax.distributed.initialize()` or set `TPU_*` yourself.

The only edits for a personal copy are the two names and the namespace (the
`sed` block in `SKILL.md`). Names are DNS labels; the ISVC name becomes the
Service name and part of the routed hostname, so keep it short.

## Image mirror

The fixture names `docker.io/vllm/vllm-tpu:<tag>`. On a cluster that pulls only
through the ECR mirror, rewrite Docker Hub references on the way in:

```sh
source .buildkite/eval_image_mirror.sh     # exports the mirror prefix; AWS_REGION selects the replica
mirror_images < runtime.yaml > runtime.mirrored.yaml
```

`mirror_images` rewrites `image: docker.io/<ns>/<repo>` to
`<prod-ecr>/pt_dockerio/<ns>/<repo>`; nothing else in the file changes.

## Single-pod variant (holds chips, no mesh)

For scheduling, quota or teardown checks that do not need a TPU mesh:

| File | Kind | Edits |
|---|---|---|
| `hack/quota-mc-demo/fixtures/01-runtime.yaml` | `ClusterServingRuntime` `mc-demo-tpu`, one pod, `google.com/tpu: "4"`, hold-loop container | cluster-scoped: rename `metadata.name` to your own name so you never share an object with someone else's demo; the image is `busybox` (mirror it the same way) |
| `hack/quota-mc-demo/fixtures/20-isvc-fits.yaml` | `InferenceService` referencing that runtime by name | remove `spec.placement` and the `ome.io/local-queue` annotation (both are multi-cluster/quota inputs), set `metadata.namespace`, `metadata.name` and `spec.runtime.name`, set `minReplicas`/`maxReplicas` to 1 |

A lone 4-chip pod half-claims a two-host `2x2x2` slice; that is fine only
because this container never touches libtpu. Anything that initialises JAX
must take the whole slice (the multi-host pair).

## Deployment mode

`spec.deploymentMode: OMENative` is a **field**. The top-level
`ome.io/deploymentMode` annotation is not in the mode-resolution chain, and the
defaulting webhook may even back-fill it with the install default so the
object looks self-contradictory. Read `{.spec.deploymentMode}` or look for
`InferenceReplica`s; ignore the annotation. A leader+worker engine resolves to
OMENative from its shape; a single-pod engine needs the field.

## ClusterBaseModel

The fixtures pin the runtime by name and set **no `spec.model`**. That is the
lean path: the controller skips model fetch and runtime auto-selection, and
skips injecting the `models.ome.io/clusterbasemodel.<name>=Ready` node
selector that a model agent would satisfy. The TPU install overlay runs with
`modelAgent.enabled: false` (`charts/ome-resources/values-usc1-1-tpu.yaml`),
so on those clusters an ISVC that references a model gates on a node label
nothing applies and stays `Pending` on node affinity. Check before relying on
`spec.model` anywhere:

```sh
kubectl --kubeconfig "$KC" --context "$CTX" get nodes -l "models.ome.io/clusterbasemodel.<name>=Ready" -o name | head -3
```

When you do need the object (a runtime that resolves the weights itself, or a
cluster with a model agent), the minimal shape is the harness fixture
`hack/omenative-testing/perm/runtime/cbm.yaml`: `modelArchitecture`,
`modelFormat`, `modelFramework`, `modelParameterSize`, `modelType`, and
`storage.storageUri` (there a `pvc://<namespace>:<claim>/<path>` the harness
templates through `runtime_fixtures.py`; HuggingFace, OCI, S3, Azure, GCS and
GitHub URIs are also accepted). `charts/ome-serving` renders ClusterBaseModels
from a model catalogue (`hfModelId` + local `path`) together with a GPU
sglang runtime; there is no TPU catalogue entry, so on tpu7x a ClusterBaseModel
is something you write for a runtime you also bring.

## Namespace

`hack/quota-mc-demo/fixtures/00-namespace.yaml` creates `ome-mc-test` with
`kueue.x-k8s.io/managed: "true"`. That label is what puts a namespace inside
Kueue's webhook scope: in such a namespace a pod that names a queue is gated
until quota admits it. The TPU pair names no queue, so it runs outside quota
even there; a namespace without the label is never gated at all. Do not create
namespaces on the TPU clusters ad hoc - use one you already hold the admin
action for.
