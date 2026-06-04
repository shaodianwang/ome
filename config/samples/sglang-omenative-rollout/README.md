
# OME docs + a working PD sample

This folder is both a runnable prefill-decode example and the OME doc set.

**Just want to ship? Start with the task recipes (how-to):**

1. [Launch a model](howto-launch-a-model.md) — get one model serving, ~10 min
2. [Write a ServingRuntime](howto-write-a-runtime.md)
3. [Update & roll safely](howto-update-and-roll.md) — blue/green, canary, sequential
4. [Multi-node & PD](howto-multi-node-and-pd.md)

**Want the concepts behind it?** Read the reference guides:
[when to use OME](guide-when-to-use-ome.md) · [OMENative](guide-omenative.md) ·
[runtimes](guide-runtimes.md) ·
[models & InferenceService](guide-models-and-inferenceservice.md).

The rest of this file documents the YAML in this folder — the PD rollout sample
the how-tos point back to.

## Who edits what

| File | Kind | Who owns it |
|------|------|-------------|
| `base-clusterservingruntime.yaml` | `ClusterServingRuntime` | **Platform team.** Hardware/platform base — GPUs, RDMA, volumes, probes, networking, infra env. Set once; you rarely touch it. |
| `tenant-servingruntime.yaml` | `ServingRuntime` | **You.** Your sglang image, launch flags, model path, cache env. Inherits the base. |
| `clusterbasemodel-pvc.yaml` | `ClusterBaseModel` | **You — temporary.** PVC fallback for the weights. Optional; goes away once cluster cache is stable (see "Model loading"). |
| `inferenceservice.yaml` | `InferenceService` | **You.** Replica counts + rollout behavior. |

Apply order: **base runtime → tenant runtime → model → ISVC**. All four
validate with `kubectl apply --dry-run=server`.

## The two runtimes: base vs tenant

Instead of one giant runtime, the spec is split in two and joined by the
annotation `ome.io/inherit-from` on the tenant:

- **Base** = *where + how it runs*: hardware/platform bits that almost never
  change. No image, no sglang flags.
- **Tenant** = *what runs*: your image version, the sglang launch command and
  flags, your model path and cache env — the things that change per model or
  per build.

Change your model or bump your sglang build → you edit only the small tenant
file. The shared platform base stays put.

### How inheritance merges (important for `command`/`args`)

OME merges the tenant onto the base with a Kubernetes **strategic merge**.
Different field types merge differently:

| Field type | Merge rule | Example |
|------------|-----------|---------|
| scalars (`image`, `name`) | tenant **overrides** | tenant image wins |
| named lists (`env`) | **merge by `name`** (additive; tenant wins per key) | base infra env + your workload env |
| plain lists (`command`, `args`) | **replace wholesale** — NOT append | the side that sets it wins entirely |

That last row is the gotcha: **`command`/`args` are replaced as a whole, never
appended.** So you can't put a shared command prefix in the base and "add a
flag" in the tenant — the tenant's list replaces the base's completely. That's
why the full sglang command lives in the tenant, once per component. Engine and
decoder share it via a YAML anchor and differ only by `--disaggregation-mode`.
Per-component differences you *do* want to factor out can use `$(VAR)`
substitution (already used for `$(CC_MODEL_PATH)`, `$(MODEL_PATH)`,
`$(NAMESPACE)` — Kubernetes expands `$(ENV)` in command/args).

## Model loading: cluster cache (primary) + PVC (temporary fallback)

The model is **fetched at runtime by cluster cache (CC)**: the runtime loads
`--model-path $(CC_MODEL_PATH)` — a `manifold://` path — using the
`CLUSTER_CACHE_*` env in the tenant runtime. Because CC supplies the weights,
the ISVC's `spec.model` is **optional** and researchers don't have to manage a
model resource at all.

The `ClusterBaseModel` (PVC) in this sample is **only today's fallback**: it
mounts the weights so the runtime's `--fallback-model-path $(MODEL_PATH)` has
somewhere to fall back to while CC is still being hardened.

> **The PVC fallback is temporary and MUST be removed once CC is fully
> stable.** Cleanup:
> 1. delete `clusterbasemodel-pvc.yaml`
> 2. drop `spec.model` from the ISVC
> 3. drop `--fallback-model-path` and `--fallback-speculative-draft-model-path`
>    from the tenant runtime

## Rollout behavior — two layers

When you change the spec (e.g. a new image), OMENative rolls the pods. You
control it at two layers in the ISVC.

### Layer A — group `rollout.coordination`: how components roll *together*

| Policy | What happens | Best for |
|--------|--------------|----------|
| **RollingUpdate** *(this sample)* | prefill + decode roll **in parallel**, in small paced steps; mixed old/new revisions are tolerated mid-roll | steady, capacity-efficient PD upgrades |
| **Sequential** | one component fully rolls to the new version before the next starts (`order` required); optional `soak` dwell to validate under live traffic in between | staged, validate-then-proceed |
| **BlueGreen** | bring up a full new copy of every component, then flip traffic atomically | atomic cutover + instant rollback (needs ~2× GPU) |
| **Independent** | each component rolls on its own (no coordination) | when components don't need to move together |

### Layer B — per-component `lifecycle`: how *one* component's pods roll

`updateStrategy.type`:

| Type | Behavior | Cost |
|------|----------|------|
| `SurgeThenDrain` *(default, this sample)* | start a new pod, wait until it serves, then drain the old one | +1 pod, no traffic gap |
| `RecreatePod` | delete old, then recreate | brief capacity dip |
| `InPlaceIfPossible` *(router here)* | change the pod in place; fall back to recreate if needed | none |
| `InPlaceOnly` | in place, or reject the change | none |

…with `rollingUpdate.{maxSurge, maxUnavailable}` (ints or `%`) as the budget.

> The group budget caps the per-component one: `pacing.maxSurge` is the ceiling
> over each component's `lifecycle…maxSurge`.

## Why this sample uses RollingUpdate + RatioBalanced

Prefill and decode serve one model together at a fixed ratio (1:1 here) and
talk to each other over NIXL. **RatioBalanced** pacing rolls them in lockstep —
it never lets the live prefill:decode ratio drift past `ratioTolerance` (5%),
so new-revision prefill and decode come up in proportion and never end up
talking across a lopsided version skew. Sequential would roll one pool fully
before the other (maximizing that skew window); BlueGreen would need double the
GPUs. Hence RollingUpdate + RatioBalanced for PD.

> GPU-constrained? Use `lifecycle.updateStrategy.type: RecreatePod` with
> `maxUnavailable: 1` and `pacing.maxSurge: 0` — rolls in place of capacity
> instead of above it.

## Two rules that are easy to miss

1. **Set `spec.deploymentMode: OMENative` once** — it propagates to every
   component (engine/decoder/router) and the ingress. The per-component
   `ome.io/deploymentMode` annotation is an optional override for mixed-mode
   (e.g. one component on RawDeployment); when set, it wins over the field for
   that component. Miss the field entirely → components silently run as plain
   Deployments.
2. **The engine/decoder container must be named `ome-container`** (and the
   router `router`). OME keys off those names for status and pod injection;
   renaming breaks them.

## Watching a rollout

- `status.rolloutCoordination.groups[].phase` —
  `Idle → Surging → Waiting → Shifting → Draining → ScalingDown → Idle`
  (or `Failed` / `RollingBack` / `Paused`).
- `status.rolloutCoordination.groups[].observedRatio` — prefill:decode pod
  counts (RatioBalanced only).
- `status.components.<c>.lifecycle` — `replicas`, `readyReplicas`,
  `servingReplicas`, `updatedReadyReplicas`.
