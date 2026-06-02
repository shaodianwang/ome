# Runtimes

*OME user guide — page 3 of 4. Prev: [OMENative](guide-omenative.md) · Next:
[Models & InferenceService](guide-models-and-inferenceservice.md) ·
[To OME or not](guide-when-to-use-ome.md)*

A runtime describes how a model is served: the container image, the launch
command and flags, the hardware it needs, and the probes that decide when it's
healthy. An InferenceService points at a runtime by name.

The one idea to take from this page: split the runtime in two. A **base**
runtime holds the things that rarely change and aren't yours to change (the
hardware). A **tenant** runtime holds the things you change all the time (your
image and flags). The tenant inherits the base.

On this page:
- Why split base from tenant
- How inheritance merges (read this before moving `command`)
- The container name is required
- A base runtime per cloud (CoreWeave, GCP, OCI)
- Cluster scope vs namespace scope
- autoSync and revision: tracking vs pinning
- Rules of thumb
- FAQ

## Why split base from tenant

Put each setting on the side that owns it.

- **Base runtime** — *where and how it runs.* GPUs and RDMA, volumes, probes,
  ports, networking, infrastructure env (NCCL, CUDA). The platform team owns
  it. It changes rarely. It carries no image and no model flags.
- **Tenant runtime** — *what runs.* The container image (your sglang build),
  the launch command and flags (tensor-parallel size, quantization, context
  length, prefill/decode wiring), and your model and cache env. You own it. It
  changes per model and per build. It inherits everything else from the base.

The payoff: changing your model or bumping your image touches only the small
tenant file. The shared hardware profile stays put, and one platform team can
maintain it for everyone.

The tenant declares its parent with one annotation:
`ome.io/inherit-from: <base-runtime-name>`.

## How inheritance merges (read this before moving `command`)

OME combines the tenant onto the base with a Kubernetes *strategic merge*.
This matters, because different kinds of fields merge differently:

| Field kind | How it merges | Example |
|------------|---------------|---------|
| a single value (`image`, `name`) | the tenant's value **replaces** the base's | tenant image wins |
| a named list (`env`, keyed by `name`) | merged **by name** — additive; tenant wins per key | base infra env + your workload env |
| a plain list (`command`, `args`) | **replaced as a whole** — not appended | the side that sets it wins entirely |

That last row is the one that surprises people. **`command` and `args` are
replaced wholesale.** You cannot put a shared prefix of the command in the base
and "add a flag" in the tenant — the tenant's list replaces the base's
completely. So the full launch command lives in the tenant, written out in
full, once per component.

If you want to factor out a per-component difference (say, prefill vs decode),
the tool is variable substitution, not list merging: write `$(VAR)` in the args
and set `VAR` in env. Kubernetes expands `$(ENV)` in command and args at pod
start. The example here already does this for `$(CC_MODEL_PATH)` and
`$(NAMESPACE)`.

## The container name is required

The container's `name` must be present on every runtime — the CRD schema
requires it — so it appears on both the base and the tenant. Use `ome-container`
for engine and decoder, and `router` for the router. OME finds the main
container by that exact name; don't rename it. (This is the one field that
"belongs" in the tenant by your instinct but has to be in both because the
schema demands it.)

## A base runtime per cloud (CoreWeave, GCP, OCI)

"Where it runs" is exactly what differs between clouds. So publish one base
runtime per cloud, and let tenants inherit whichever matches their cluster. The
tenant — your image and flags — stays the same everywhere.

| Concern | CoreWeave (CKS / gb300) | GCP (GKE) | OCI (OKE) |
|---------|-------------------------|-----------|-----------|
| accelerator resource | `nvidia.com/gpu` + `rdma/ib` | `nvidia.com/gpu` or `google.com/tpu` | `nvidia.com/gpu` |
| image registry | CW ECR mirror | Artifact Registry | OCIR |
| node selectors | arch + CW node labels | GKE nodepool / TPU topology | OKE node-pool labels |
| accelerator network | IB macvlan via multus, IPv6 NCCL | gVNIC / GPUDirect-TCPX | RDMA/RoCE via OCI cluster network |
| infra env | IB / AF_INET6 NCCL settings | TCPX NCCL settings | OCI RDMA settings |
| storage | CW PVC / cluster cache | Filestore / GCS-fuse / cluster cache | OCI FSS / object storage / cluster cache |

This is the idea behind the `ome-isvc` Helm chart's "vendor profiles"
(`cks-gb300-sglang-pd`, `gke-tpu-sglang`): each is a base runtime for one
place, and your deployment just names which one to inherit.

## Cluster scope vs namespace scope

There are two runtime kinds. The difference is reach.

| | `ClusterServingRuntime` | `ServingRuntime` |
|--|--------------------------|-------------------|
| scope | the whole cluster | one namespace |
| usual owner | platform team | a model team |
| who can use it | any namespace | only InferenceServices in that namespace |

How to choose:

- The **base** profiles are usually **cluster-scoped** — published once by the
  platform team, one per cloud, shared by everyone.
- A **tenant** runtime is usually **namespace-scoped** — it lives in your team's
  namespace, is isolated by RBAC, and can't collide with another team's. A
  namespace-scoped tenant can still inherit a cluster-scoped base; OME looks in
  your namespace first, then falls back to cluster scope.
- Make a tenant cluster-scoped only if it's genuinely shared across namespaces.

The InferenceService picks a runtime with `spec.runtime.name` and
`spec.runtime.kind` (default `ClusterServingRuntime`).

## autoSync and revision: tracking vs pinning

`spec.runtime.autoSync` decides whether your InferenceService tracks the *live*
runtime or pins to a snapshot of it.

- `autoSync: true` (the default) — every reconcile, OME re-renders the pods from
  the current runtime. Edit the runtime and your service picks it up. Simple,
  always current.
- `autoSync: false` — OME pins the service to a snapshot (a ControllerRevision)
  of the runtime. Later edits to the runtime do *not* touch your pods until you
  deliberately move forward.

Why you'd turn it off: change control. In production you often don't want an
unrelated edit to someone's shared runtime to silently restart your pods. Pin
it, and roll forward on purpose.

`spec.runtime.revision` is how you pin to a *specific* named snapshot — and
therefore how you roll **back**: set it to a previous revision. It's ignored
while `autoSync` is true.

These are separate from the rollout knobs on page 2. `autoSync`/`revision`
decide *which* runtime spec you're targeting; lifecycle and coordination decide
*how* the pods move to it.

## Rules of thumb

- *Hardware in the base, workload in the tenant.* If you're editing GPU counts
  to ship a model, something is in the wrong file.
- *Write the whole command in the tenant.* Don't try to split it across base and
  tenant — the merge won't append it.
- *Pin production.* Default `autoSync` is convenient for dev; pin (`autoSync:
  false` + a `revision`) where a surprise restart is unacceptable.

## FAQ

**Can I just use one big runtime and skip the split?**
Yes — nothing forces the split. But then every team copies the hardware
settings, and a hardware change means editing every copy. The split exists so
that doesn't happen.

**How deep can inheritance go?**
A tenant inherits one base. Chains resolve up to a bounded depth, but in
practice keep it to base → tenant.

**If I bump my image, do I edit the base?**
No. The image is in the tenant. Bump it there. The base doesn't carry an image
at all.

**How do I roll back a bad runtime change?**
Set `autoSync: false` and point `spec.runtime.revision` at the last good
snapshot.
