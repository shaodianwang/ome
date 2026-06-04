# How-to: write a ServingRuntime

*Task recipe — page 2 of 4. Prev: [Launch a model](howto-launch-a-model.md) ·
Next: [Update & roll](howto-update-and-roll.md) ·
[Multi-node & PD](howto-multi-node-and-pd.md)*

A **runtime** is the reusable half of a deployment: the container image, the
launch command, and the list of models it knows how to serve. You write it once;
many InferenceServices point at it.

Use a `ServingRuntime` (one namespace) or a `ClusterServingRuntime` (cluster-wide,
shared by every namespace). They have the identical spec.

## The smallest working runtime

```yaml
apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: my-runtime
  namespace: <your-namespace>
spec:
  # --- 1. Which models this runtime can serve (the match block) ---
  supportedModelFormats:
    - modelFormat:
        name: safetensors          # the on-disk weight format
      modelFramework:
        name: transformers         # the producing library
      modelArchitecture: LlamaForCausalLM   # must equal your model's architecture
      autoSelect: true             # eligible for auto-selection when ISVC omits runtime
      priority: 1                  # higher wins when several runtimes match
  protocolVersions:
    - openAI

  # --- 2. How to run it (the serving container) ---
  engineConfig:
    runner:
      name: ome-container          # REQUIRED exact name — do not rename (see below)
      image: <your-registry>/sglang:<tag>
      command:
        - python3
        - -m
        - sglang.launch_server
        - --host
        - "0.0.0.0"
        - --port
        - "30000"
        - --model-path
        - $(CC_MODEL_PATH)        # cluster cache fetches the weights at runtime
      env:
        # CC_MODEL_PATH points at the checkpoint; the CLUSTER_CACHE_* cache
        # wiring is usually inherited from the platform base runtime — see
        # tenant-servingruntime.yaml for the full env set.
        - name: CC_MODEL_PATH
          value: "manifold://<your-checkpoint-path>"
      ports:
        - containerPort: 30000
          name: http
      resources:
        requests: { nvidia.com/gpu: "1" }
        limits:   { nvidia.com/gpu: "1" }
      readinessProbe:
        httpGet: { path: /health, port: 30000 }
```

Two blocks: **what it matches** (`supportedModelFormats`) and **how it runs**
(`engineConfig`). An InferenceService that names this runtime inherits the whole
`engineConfig` and only overrides replica counts (and anything else it sets).

## The rules you can't skip

- **The serving container must be named `ome-container`.** (A router container,
  if you add one, must be named `router`.) OME keys status reporting and config
  injection off these names. Rename them and the pod runs but status/injection
  break silently.
- **The runtime supplies the weights.** Internally they're fetched at runtime via
  cluster cache: `--model-path $(CC_MODEL_PATH)` plus a `CC_MODEL_PATH` env
  pointing at the checkpoint (`manifold://…`), with the `CLUSTER_CACHE_*` cache
  wiring (usually inherited from the platform base runtime — see
  `tenant-servingruntime.yaml`). OME also injects `$(NAMESPACE)` and
  `$(INFERENCESERVICE_NAME)`. (If instead you use a managed `spec.model`, OME
  injects `$(MODEL_PATH)` from its `storageUri` — see
  [Models & InferenceService](guide-models-and-inferenceservice.md).)
- **The match block must actually match your model.** `modelArchitecture`,
  `modelFormat.name`, and `quantization` (if set) are compared against the
  model's metadata. One mismatch and this runtime is silently skipped.

## Auto-select vs named

- `autoSelect: true` → this runtime is a candidate when an ISVC omits
  `spec.runtime`. Among candidates, higher `priority` wins, then a tighter
  `modelSizeRange` fit, then **namespaced beats cluster-wide**, then name order.
- `autoSelect: false` → only used when an ISVC names it explicitly. Good for
  vendor/experimental runtimes you don't want auto-picked.

## Share hardware setup across runtimes (base + tenant)

Don't copy GPU counts, volumes, and probes into every runtime. Put the
**hardware/platform** bits in a base `ClusterServingRuntime` (no image, no
flags) and inherit it from a small **tenant** runtime that adds the image and
command:

```yaml
apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: my-runtime
  namespace: <your-namespace>
  annotations:
    ome.io/inherit-from: <base-clusterservingruntime-name>   # the join key
spec:
  engineConfig:
    runner:
      name: ome-container
      image: <your-registry>/sglang:<tag>
      command: [ ... full command ... ]
```

See `base-clusterservingruntime.yaml` (platform-owned) and
`tenant-servingruntime.yaml` (you own) in this folder for the real split.

> **The inheritance gotcha that wastes an afternoon:** merge is a Kubernetes
> *strategic merge*. Scalars (image) override; named lists (`env`) merge by name;
> but **`command` and `args` are replaced *wholesale*, never appended.** You
> cannot put a command prefix in the base and "add a flag" in the tenant — the
> side that sets `command` wins entirely. So write the *full* command in the
> tenant, once. Factor per-component differences with `$(VAR)` substitution, not
> by splitting the command across base and tenant.

## Why isn't my runtime selected?

If an ISVC fails with "no supporting runtime" or "model size outside supported
range," walk the match contract — **all** of these must hold:

| Check | Field |
|---|---|
| Format matches | `modelFormat.name` == model's format (+ `version` via `operator`) |
| Architecture matches | `modelArchitecture` == model's architecture |
| Quantization matches | `quantization` == model's (if either is set) |
| Framework matches | `modelFramework` name/version (if both present) |
| Size fits | model params within `modelSizeRange.min..max` |
| Not disabled | `spec.disabled` is false |
| (auto only) | some format has `autoSelect: true` |

> **Two field-naming traps to know about (these are real API warts, not yours):**
> - `modelFormat` is the *on-disk encoding* (safetensors, gguf, onnx);
>   `modelFramework` is the *producing library* (transformers, pytorch). The
>   field docs unhelpfully list `"PyTorch"` as an example for **both** — when in
>   doubt, copy a working runtime for your model family rather than guessing.
> - In the InferenceService these same blocks are called `engine`/`decoder`/
>   `router`, but in the runtime they're `engineConfig`/`decoderConfig`/
>   `routerConfig`. Same content, different key. Copy-pasting a block between the
>   two CRDs means renaming it.

## Next

- Deploy a model on this runtime → [Launch a model](howto-launch-a-model.md)
- Roll a new image of this runtime safely → [Update & roll](howto-update-and-roll.md)
- Prefill-decode or multi-node runtime → [Multi-node & PD](howto-multi-node-and-pd.md)
