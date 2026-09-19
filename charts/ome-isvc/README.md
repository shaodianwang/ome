# ome-isvc

A scientist-facing Helm chart for deploying one or more InferenceService
(ISVC) stacks on top of an already-running OME installation. The values
surface is intentionally limited to the knobs a model deployer cares
about (image, replicas, args, context window, coordination); GPU
counts, RDMA / multus wiring, tolerations, security contexts, and
probes live in pre-tailored vendor ClusterServingRuntime (CSR) profiles
bundled under `templates/vendors/`.

## 1. Quick start

```bash
helm install llama4x-prod charts/ome-isvc/ \
  --values charts/ome-isvc/examples/values-cks-gb300-sglang-pd.yaml
```

Prerequisites: OME CRDs (`charts/ome-crd`), OME controller + webhooks
(`charts/ome-resources`), and an OME version that supports the
`ome.io/inherit-from` runtime inheritance annotation.

## 2. What this chart deploys

Each Helm release of `ome-isvc` produces one or more model-serving
stacks. Per stack, the chart emits:

- A vendor ClusterServingRuntime (CSR) — the pre-tailored image / GPU /
  RDMA / NCCL / probe profile, auto-deployed when referenced (or pinned
  via `vendors.deployAll: true`). Bundled under `templates/vendors/`.
- A tenant child ClusterServingRuntime (or ServingRuntime, scope-
  dependent) — thin override that inherits the vendor CSR via the
  `ome.io/inherit-from` annotation and carries only the scientist's
  per-component knobs (image override, extra args, replicas-shaped
  runner spec).
- An InferenceService (ISVC) — the consumer resource that points at the
  tenant runtime and an existing (or chart-emitted) ClusterBaseModel
  (CBM), with replica counts, autoscaling, and a synthesised
  RolloutCoordinationGroup.
- Optionally one or more `(Cluster)BaseModel`(s) declared inline, for
  releases that also want to provision the model resource.

## 3. Values reference

| Field | Type | Default | Description |
|---|---|---|---|
| `nameOverride` | string | `""` | Override the chart name component of release-derived names. |
| `fullnameOverride` | string | `""` | Pin the release-name prefix; bypasses release-name composition. |
| `namespace.name` | string | `default` | Required. Namespace where the InferenceService(s) (and any BaseModels with `scope=namespace`) land. Must already exist; the chart does not create namespaces. |
| `scope` | enum | `namespace` | `cluster` or `namespace` — controls tenant child runtime scope and the default scope for inline BaseModels. |
| `vendor` | string | `cks-gb300-sglang-pd` | Default vendor profile inherited by every deployment that omits `vendor:`. Must match a profile bundled under `templates/vendors/`. |
| `deployments[].name` | string | (required) | Short identifier; becomes `<release>-<name>` for the InferenceService and `<release>-<name>-csr` for the tenant child ClusterServingRuntime. |
| `deployments[].vendor` | string | `.Values.vendor` | Optional override of the top-level vendor for this one deployment. |
| `deployments[].servingMode` | enum | (required) | `standalone` | `pd` | `multinode`. See "Serving modes" below. |
| `deployments[].prefill.image` | string | `""` | `pd` only. Image override; empty = use vendor default. |
| `deployments[].prefill.replicas.min` | int |  | Min replica count. |
| `deployments[].prefill.replicas.max` | int |  | Max replica count. |
| `deployments[].prefill.contextLength` | int |  | Synthesised as `--context-length <N>` on the runner args. |
| `deployments[].prefill.tensorParallelSize` | int |  | Synthesised as `--tensor-parallel-size <N>` (sglang). |
| `deployments[].prefill.gpuMemoryUtilization` | number |  | Fraction (0.0–1.0); synthesised as `--mem-fraction-static <X>` (sglang). |
| `deployments[].prefill.maxConcurrentRequests` | int |  | Synthesised as `--max-running-requests <N>` (sglang). |
| `deployments[].prefill.dtype` | string | `""` | Model compute dtype. Enum: `""` / `bfloat16` / `fp16` / `fp8` / `auto`. Empty string omits the flag (sglang default takes effect); a set value synthesises `--dtype <value>`. |
| `deployments[].prefill.quantization` | string | `""` | Model quantization scheme (e.g. `modelopt_fp4`, `awq`, `gptq`, `fp8`). Free-form string; empty omits the flag; a set value synthesises `--quantization <value>`. |
| `deployments[].prefill.kvCacheDtype` | string | `""` | KV cache dtype. Enum: `""` / `bfloat16` / `fp16` / `fp8` / `auto`. Empty omits the flag; a set value synthesises `--kv-cache-dtype <value>`. |
| `deployments[].prefill.moeRunnerBackend` | string | `""` | MoE-only runner backend (e.g. `flashinfer_trtllm`). Free-form string; empty omits the flag; a set value synthesises `--moe-runner-backend <value>`. |
| `deployments[].prefill.speculative.algorithm` | string | `""` | Speculative decoding algorithm (e.g. `EAGLE`, `EAGLE3`). Empty disables the WHOLE speculative block (gating field). A set value synthesises `--speculative-algorithm <value>`. |
| `deployments[].prefill.speculative.draftModelPath` | string | `""` | Path to the draft model (e.g. `$(MODEL_PATH)/draft_hf`). Synthesises `--speculative-draft-model <value>` only when `algorithm` is set; otherwise silently ignored. |
| `deployments[].prefill.speculative.draftModelQuantization` | string | `""` | Draft model quantization (e.g. `unquant`). Synthesises `--speculative-draft-model-quantization <value>` only when `algorithm` is set. |
| `deployments[].prefill.args` | array | `[]` | Extra CLI flags appended after the runtime base + synthesised typed flags. User entries WIN on duplicate `--flag` via dedup-by-flag-name. Chart-owned args list — see "Args layering" below. |
| `deployments[].prefill.env` | array | `[]` | Extra container env vars. Each entry is a corev1.EnvVar (`{name, value}` or `{name, valueFrom: ...}`). Forwarded to the tenant CSR's `runner.env`, where they merge with the vendor's env list **by `name` key** (per-key strategic merge — predictable, additive). See "Env merge" below. |
| `deployments[].prefill.autoscaling.enabled` | bool | `false` | Placeholder. KEDA wiring is reserved for a follow-up. |
| `deployments[].prefill.autoscaling.metrics` | array | `[]` | Placeholder. Shape reserved; v0.1 templates ignore it. |
| `deployments[].prefill.lifecycle.*` | object |  | OMENative lifecycle override. STRICT pass-through, defaults come from the controller defaulter. See "Lifecycle overrides" below. |
| `deployments[].decode.*` | object |  | Same shape as `prefill.*` (including the 4 typed model knobs, `speculative.*`, `env: []`, `args: []`, and `lifecycle.*`). `pd` only. |
| `deployments[].engine.*` | object |  | Same shape as `prefill.*` (including the 4 typed model knobs, `speculative.*`, `env: []`, `args: []`, and `lifecycle.*`), plus `workerSize`. `standalone` or `multinode` only. |
| `deployments[].engine.workerSize` | int |  | Multinode only: number of worker pods per leader gang. |
| `deployments[].router.enabled` | bool | `true` | `pd` only. Whether to emit the router Component. |
| `deployments[].router.image` | string | `""` | Router image override. |
| `deployments[].router.replicas.{min,max}` | int |  | Router replica counts. |
| `deployments[].router.args` | array | `[]` | Extra router CLI flags appended after the synthesised flags; user overrides win on duplicate flags. Chart-owned list. |
| `deployments[].router.env` | array | `[]` | Extra router container env vars (same shape + merge semantics as `prefill.env`). |
| `deployments[].router.lifecycle.*` | object |  | Router OMENative lifecycle override (same schema as `prefill.lifecycle.*`). |
| `deployments[].router.timeoutSeconds` | int | `30` | Request-completion deadline; synthesised as `--backend-timeout <N>` on the router runner. |
| `deployments[].router.streamingTimeoutSeconds` | int | `600` | SSE/token-stream deadline (so long generations don't time out mid-stream); synthesised as `--streaming-timeout-secs <N>`. The exact sglang router flag name may differ between versions; adjust in `_helpers.tpl::ome-isvc.renderRouter` if upstream renames it. |
| `deployments[].router.loadBalancingAlgorithm` | string |  | Synthesised as `--load-balancer <value>` (placeholder; see open work items). |
| `deployments[].coordination.policy` | enum |  | `BlueGreen` | `Sequential` | `RollingUpdate` | `Independent`. |
| `deployments[].coordination.order` | array |  | Required for `Sequential`. Scientist names (`prefill`/`decode`/`engine`/`router`); chart translates to OME Components. |
| `deployments[].coordination.soak` | string |  | Wait between Sequential steps; Go-style duration (e.g. `"10m"`). |
| `deployments[].coordination.pacing.type` | enum |  | `PerComponent` (default) or `RatioBalanced`. |
| `deployments[].coordination.pacing.maxSurge` | int / string |  | Forwarded onto the synthesised RolloutCoordinationGroup. |
| `deployments[].coordination.pacing.maxUnavailable` | int / string |  | Forwarded onto the synthesised RolloutCoordinationGroup. |
| `deployments[].coordination.pacing.ratioTolerancePercent` | int |  | RatioBalanced only. |
| `deployments[].modelRef.name` | string | (required) | Name of the (Cluster)BaseModel to serve. |
| `deployments[].modelRef.scope` | enum | `cluster` | `cluster` -> ClusterBaseModel, `namespace` -> BaseModel. |
| `vendors.deployAll` | bool | `false` | When `true`, emit every bundled vendor runtime. When `false`, emit only referenced ones. |
| `basemodels` | array | `[]` | Optional inline (Cluster)BaseModel(s). Most releases skip this and use existing models. |
| `basemodels[].name` | string | (required) | BaseModel name. |
| `basemodels[].storageUri` | string | (required) | Source URI of the model bytes (`oci://`, `hf://`, `s3://`, `gs://`, `az://`, `github://`, ...). |
| `basemodels[].scope` | enum | `cluster` | `cluster` -> ClusterBaseModel; `namespace` -> BaseModel in `namespace.name`. |
| `basemodels[].vendor` | string |  | Optional vendor label; omitted from the rendered resource when unset. |
| `basemodels[].distribution` | string |  | Optional distribution mode (e.g. `Sharded`). Omitted when unset; controller chooses based on size. |
| `basemodels[].displayName` | string |  | Optional human-friendly display name. Omitted when unset; controller derives from `name`. |
| `commonLabels` | map | `{}` | Labels stamped on every chart-emitted resource. |
| `commonAnnotations` | map | `{}` | Annotations stamped on every chart-emitted resource. |

Schema is enforced via `values.schema.json` (hand-written for v0.1),
including a `oneOf` per `deployments[]` entry that requires `prefill +
decode` when `servingMode: pd` and `engine` for `standalone` /
`multinode`.

## 4. Serving modes

`deployments[].servingMode` controls which component blocks the
deployment fills in and which spec fields the chart renders on the
InferenceService and tenant ClusterServingRuntime.

- **standalone** — single engine Component. One container per replica,
  no prefill/decode split, no router. Fill in `engine:` (image,
  replicas, args, context length). No `prefill:` / `decode:` / `router:`
  blocks.
- **pd** — prefill+decode disaggregated. Two pod populations
  (`prefill:` synthesises to `spec.engine`, `decode:` synthesises to
  `spec.decoder`) plus an optional `router:` that load-balances across
  prefill and decode pods. Required block pair: `prefill` + `decode`.
- **multinode** — single engine Component, but with a leader/worker
  gang. Fill in `engine:` plus `engine.workerSize: N` (N worker pods
  per leader). Useful for sharded models that need leader/worker
  collectives.

Component name translation happens inside the templates:

| values.yaml block | InferenceService field      | Active in                              |
|-------------------|------------------------------|----------------------------------------|
| `prefill`         | `spec.engine`                | `servingMode: pd`                      |
| `decode`          | `spec.decoder`               | `servingMode: pd`                      |
| `engine`          | `spec.engine`                | `servingMode: standalone` / `multinode`|
| `router`          | `spec.router`                | optional; `pd` only                    |

The same translation applies inside `spec.rollout.coordination.groups[].components`
and `groups[].order` — scientists write `[decode, prefill]`; the chart
emits `[decoder, engine]`.

## 5. Vendor profiles + inheritance

Vendor profiles are pre-tailored `ClusterServingRuntime` resources
bundled under `templates/vendors/`. They are deliberately a "where it
runs" description: container image, GPU / TPU / RDMA / IB resource
requests, node selectors, tolerations, multus / macvlan annotations,
infrastructure-grade env vars (NCCL networking, CUDA coredump, host
identity), volumes, security contexts, and probes. They do **not**
own the workload args, the workload-tuning env vars, or the
process-launch `command`.

### What's IN the vendor vs the chart

The split matters because Kubernetes strategic-merge handles named
lists (like `env`) very differently from plain `[]string` lists (like
`args`):

| List | Patch strategy | Effect on inheritance |
|---|---|---|
| `runner.env` (named — `name` key) | per-key merge | Vendor env + child env land in the final container, deduplicated by `name`. Predictable, additive. |
| `runner.args` (plain `[]string`) | wholesale-replace | Whichever side last sets `args` wins; the other side's list is silently dropped. |
| `runner.command` (plain `[]string`) | wholesale-replace | Same as `args`. |

So if the vendor shipped a full `args:` list and a scientist appended
a single `--my-flag`, the resulting tenant CSR would carry **only**
that single flag — strategic-merge would have wiped the rest. The
chart works around this by stripping `args`/`command` from the vendor
profile entirely and having the chart's tenant CSR own the complete
list, so there is nothing to lose.

| Concern | Vendor (`templates/vendors/*.yaml`) | Chart-side tenant CSR (`templates/servingruntime.yaml`) |
|---|---|---|
| Container image | ✅ owns | overridable via `deployments[].<component>.image` |
| GPU / RDMA / TPU resources | ✅ owns | — |
| nodeSelector / tolerations | ✅ owns | — |
| Volumes + volumeMounts | ✅ owns | — |
| Multus / macvlan / IB annotations | ✅ owns | — |
| Infrastructure env (NCCL_*, CUDA_*, host IP, PYTHONUNBUFFERED, ...) | ✅ owns | — |
| Security context + probes + ports | ✅ owns | — |
| Container `command` | ❌ stripped | container `ENTRYPOINT` from the image takes effect |
| Container `args` | ❌ stripped | ✅ owns full list (runtime base + synthesised typed flags + user `args:`, deduped by flag name; see "Args layering") |
| Workload-tuning env (SGLANG_*, model-package knobs) | ❌ stripped | ✅ owns via `deployments[].<component>.env: []` |

The vendor file's leading comment block (`IN scope` / `OUT of scope`)
spells out the same rule for that profile specifically.

### Inheritance model

Tenants inherit a vendor CSR via the `ome.io/inherit-from` annotation
on their child runtime. The chart renders an intentionally thin tenant
runtime that carries:

```yaml
metadata:
  annotations:
    ome.io/inherit-from: <vendor-id>
spec:
  engineConfig:            # or decoderConfig / routerConfig
    runner:
      name: ome-container
      image: <override>    # only if you set deployments[].prefill.image
      args:                # full list owned by the chart; replaces the
                           # vendor's (empty) args wholesale
        - --host                     # runtime base (ome-isvc.engineBaseArgs)
        - 0.0.0.0
        - --port
        - "30000"
        - --enable-prefix-cache
        - --schedule-policy
        - lpm
        - --context-length           # synthesised from contextLength
        - "1048576"
        - --tensor-parallel-size     # synthesised from tensorParallelSize
        - "4"
        - --dtype                    # synthesised from dtype
        - bfloat16
        - --quantization             # synthesised from quantization
        - modelopt_fp4
        - --speculative-algorithm    # synthesised from speculative.algorithm (gates the block)
        - EAGLE3
        # ...followed by any user-supplied .args[] (with dedup-by-flag-name)
      env:                 # per-name merge against vendor env at consume time
        - name: SGLANG_EXTERNAL_MODEL_PACKAGE
          value: sglang_meta.srt.models
```

The OME controller resolves the full spec at consume time via
strategic-merge inheritance. Child wins on collision; lists keyed by
`name` (e.g. `env`) merge per-name; plain lists like `args` are
wholesale-replaced — that's the trap. The chart sidesteps it by
having the vendor carry no `args` at all.

### Args layering

For each engine/prefill/decode component the chart-side tenant CSR's
`runner.args` is built by merging three layers, in order, with
**dedup-by-flag-name** (user wins on duplicate `--flag`):

| Layer | What | Source | Defined in |
|---|---|---|---|
| 1. **Vendor profile** | hardware / infra env only — no args | vendor CSR `runner.env`, resources, probes, volumes | `templates/vendors/<vendor>.yaml` (NOT in args) |
| 2. **Runtime base** | port/host/server defaults universally true for sglang | `--host 0.0.0.0`, `--port 30000`, `--enable-prefix-cache`, `--schedule-policy lpm` | `_helpers.tpl::ome-isvc.engineBaseArgs` |
| 3. **Synthesised flags** | typed knobs, only when set | `--context-length`, `--tensor-parallel-size`, `--mem-fraction-static`, `--max-running-requests`, `--dtype`, `--quantization`, `--kv-cache-dtype`, `--moe-runner-backend`, `--speculative-*` | `_helpers.tpl::ome-isvc.componentSynthesizedArgs` (driven by values.yaml typed fields) |
| 4. **User `args:`** | escape hatch — appended last; OVERRIDES on duplicate `--flag` | scientist's `deployments[].<component>.args` list | `values.yaml` |

The merge happens in `_helpers.tpl::ome-isvc.mergeArgs`. It walks each
list in pairs (recognising `--flag` items and pulling the following
non-`--` item as the value, or treating bare flags as booleans),
builds an ordered map of flag-name → value, and emits the final list.
User entries OVERWRITE base entries on duplicate flag rather than
producing two copies in the result.

#### Dedup-by-flag-name semantics

If the chart hardcodes `--port 30000` in the runtime base and a
scientist sets:

```yaml
deployments[0].prefill.args:
  - --port
  - "31000"
```

the merged tenant CSR's `args:` contains `--port 31000` (single
instance) — the user value wins, the base value is dropped, and there
is NO duplicate `--port` in the result. See
`examples/values-args-override.yaml` for a full worked example.

The router uses the same 3-layer pipeline via
`ome-isvc.routerBaseArgs` + `ome-isvc.routerSynthesizedArgs` + user
`args:`; `routerBaseArgs` is empty by default since no flag has been
deemed universally true for the router (extend in `_helpers.tpl` if
that changes).

### Env merge

For each component the chart-side tenant CSR's `runner.env` carries the
scientist's `env: []` entries verbatim. When OME resolves the runtime
at consume time, strategic-merge unions them with the vendor's `env`
list using `name` as the merge key — i.e. vendor entries are
preserved, child entries are added, and matching names are overridden
by the child. There is no wholesale-replace risk on `env`.

### Synthesised CLI flags

Each scientist-facing knob below maps to one sglang flag, emitted only
when you explicitly set the knob. The synthesised block is layer 3 of
the args merge — runtime base flags from layer 2 (host/port/etc.) come
first, then synthesised, then user `args:`.

| Values key                            | sglang flag                              |
|---------------------------------------|------------------------------------------|
| `contextLength`                       | `--context-length`                       |
| `tensorParallelSize`                  | `--tensor-parallel-size`                 |
| `gpuMemoryUtilization`                | `--mem-fraction-static`                  |
| `maxConcurrentRequests`               | `--max-running-requests`                 |
| `dtype`                               | `--dtype`                                |
| `quantization`                        | `--quantization`                         |
| `kvCacheDtype`                        | `--kv-cache-dtype`                       |
| `moeRunnerBackend`                    | `--moe-runner-backend`                   |
| `speculative.algorithm`               | `--speculative-algorithm` *(gates the block)* |
| `speculative.draftModelPath`          | `--speculative-draft-model`              |
| `speculative.draftModelQuantization`  | `--speculative-draft-model-quantization` |

Unset knobs (empty string or absent key) are omitted — the runtime
base / sglang defaults take effect. The speculative block is GATED by
`algorithm`: if it is empty, none of the speculative-* flags emit,
even if `draftModelPath` is set.

The `args:` list you supply is appended last and dedup-by-flag-name
ensures it wins on any duplicate `--flag`.

### Vendor defaulting

The chart accepts a top-level `vendor:` (default `cks-gb300-sglang-pd`)
that every deployment inherits unless it sets its own
`deployments[].vendor`. Concretely, the lookup order for a deployment's
effective vendor is:

1. `deployments[].vendor` (if set);
2. `.Values.vendor` (the chart-wide default).

If neither is set, the chart fails the install with an explicit error.

### Bundled vendor profiles

| Vendor                   | Status        | Notes                                                              |
|--------------------------|---------------|--------------------------------------------------------------------|
| `cks-gb300-sglang-pd`    | Production    | CW gb300 + sglang prefill+decode disaggregated for Llama4x nvfp4.  |
| `gke-tpu-sglang`         | Illustrative  | Placeholder GKE TPU sglang runtime; replace image + args before use.|

### Catalog emission

By default (`vendors.deployAll: false`), the chart auto-deploys ONLY
the vendor ClusterServingRuntimes referenced by some entry in
`deployments[].vendor` (resolved through the top-level vendor default).
A self-contained tenant install ships exactly the vendor profile it
needs, no more.

Operator scenario (`vendors.deployAll: true`): emit every bundled
vendor regardless of whether any deployment references it. Use this
when you publish the full catalog as a separate release, then tenants
install their own `ome-isvc` releases with the default
`vendors.deployAll: false` so they do not fight on the cluster-scoped
vendor names.

## 6. Examples

| File | Description |
|---|---|
| `examples/values-cks-gb300-sglang-pd.yaml` | Single PD-disaggregated Llama4x stack on CW gb300 inheriting the chart-level default vendor; demonstrates the typed model knobs (`dtype`, `quantization`, `kvCacheDtype`, `moeRunnerBackend`, full `speculative:` block) mirroring the production launch; creates ClusterBaseModel inline. |
| `examples/values-multi-deployment.yaml`    | Two stacks (prod + shadow) from one release, both inheriting the default vendor. |
| `examples/values-gke-tpu-sglang.yaml`      | Standalone single-engine stack on GKE TPU; overrides `vendor:` to the illustrative GKE profile. |
| `examples/values-existing-basemodel.yaml`  | PD-disaggregated stack pointing at a pre-existing ClusterBaseModel; no inline `basemodels`. |
| `examples/values-args-override.yaml`       | Demonstrates scientist override of a chart-hardcoded base flag via `args:`. The merge helper deduplicates by flag name (user wins) so the final tenant CSR has a single `--port 31000` rather than two `--port` entries. |

## 7. Coordination knobs

`deployments[].coordination` synthesises one `RolloutCoordinationGroup`
under `spec.rollout.coordination.groups` that spans every Component the
deployment renders (prefill+decode plus optional router for `pd`; engine
for `standalone` / `multinode`).

### Policies

- **BlueGreen** — bring up the new revision in full; cut traffic over
  once it is healthy; tear down the old revision.
- **Sequential** — walk the Components through their state machine in
  the order set under `coordination.order`. Decode drains before
  prefill cycles, so for `pd` typical order is `[decode, prefill]`.
  Honors `coordination.soak` for wait-between-steps.
- **RollingUpdate** — interleave pod replacements within each Component
  using `pacing.maxSurge` / `pacing.maxUnavailable`.
- **Independent** — each Component rolls on its own schedule; no
  cross-Component gating.

### Pacing types

- **PerComponent** (default) — each Component honors `maxSurge` /
  `maxUnavailable` independently.
- **RatioBalanced** — gates per-Component churn against the ratio of
  prefill:decode pods (use with `pacing.ratioTolerancePercent` to bound how
  far the ratio may drift mid-rollout).

### Defaults

When a `pacing:` block is declared but a sub-key is omitted, the
controller defaulter stamps:

| Field             | Default              |
| ----------------- | -------------------- |
| `type`            | `PerComponent`       |
| `maxSurge`        | `"25%"`              |
| `maxUnavailable`  | `0`                  |
| `ratioTolerancePercent`  | `5` (RatioBalanced only) |

`soak` is `0` (no inter-Component wait) unless explicitly set; only
honored for `policy: Sequential`.

## 8. Lifecycle overrides

Each Component (`prefill`, `decode`, `engine`, `router`) accepts an
optional `lifecycle:` block that maps 1:1 to OMENative's
`LifecycleSpec` (see `pkg/apis/ome/v1beta1/lifecycle_types.go`). This
governs **per-Component instance rollout** (how Instances within ONE
Component roll). Cross-Component coordination (`Sequential`,
`RatioBalanced`, etc.) lives at `deployments[].coordination` — see §7.

The chart is a **strict pass-through**: it never defaults a field, and
only emits a key when the scientist has explicitly set it. Leave the
block off entirely (or omit individual sub-keys) to inherit controller
defaults.

### Schema

```yaml
deployments:
  - name: <name>
    prefill:        # or decode / engine / router
      # ... other knobs ...
      lifecycle:
        restartPolicy: None  # None | RecreateInstanceOnPodRestart
        updateStrategy:
          type: SurgeThenDrain  # SurgeThenDrain | RecreatePod | InPlaceIfPossible | InPlaceOnly
          rollingUpdate:
            partition: 0           # canary hold: skip indices < partition
            maxUnavailable: 1      # max Instances allowed non-Ready during rollout
          inPlaceUpdateStrategy:
            markNotReadyDuringLifecycle: true  # drain via EndpointSlice before mutating
        readyPolicy: None         # None (single-pod default) | AllPodReady (multi-pod default)
        instanceReadyTimeout: 30m # Go-style duration string
        migrationPolicy:
          mode: auto              # auto | surge | never
```

### Defaults

When `lifecycle:` is omitted, the controller defaulter stamps:

| Field                                              | Single-pod              | Multi-pod (Leader+Worker)         |
| -------------------------------------------------- | ----------------------- | --------------------------------- |
| `restartPolicy`                                    | `None`                  | `RecreateInstanceOnPodRestart`    |
| `updateStrategy.type`                              | `SurgeThenDrain`        | `SurgeThenDrain`                  |
| `updateStrategy.inPlaceUpdateStrategy.markNotReadyDuringLifecycle` | `true`  | `true`                            |
| `readyPolicy`                                      | `None`                  | `AllPodReady`                     |
| `instanceReadyTimeout`                             | `30m`                   | `30m`                             |
| `migrationPolicy.mode`                             | `auto`                  | `auto`                            |

`rollingUpdate.partition` and `rollingUpdate.maxUnavailable` are
not defaulted — leave unset for no canary hold / no concurrent
non-Ready Instances.

### Emission rules

Every key is optional and emitted only when set. Specifically:

- The outer `lifecycle:` line emits only if at least one leaf is set.
- `updateStrategy:` emits only if `type`, `rollingUpdate.*`, or
  `inPlaceUpdateStrategy.*` is set.
- `rollingUpdate:` emits only if `partition` or `maxUnavailable` is set.
- `inPlaceUpdateStrategy:` emits only if `markNotReadyDuringLifecycle` is set.
- `migrationPolicy:` emits only if `mode` is set.

This avoids producing empty `lifecycle: {}` / `updateStrategy: {}` maps
that would override the controller defaulter with meaningless empties.

## 9. Autoscaling

The per-component `autoscaling:` block (`enabled`, `metrics`) is
reserved for KEDA wiring and is **not yet plumbed** into the rendered
InferenceService. v0.1 templates ignore it; the shape is reserved so
adding the body in v0.2 does not change values. Use the
`InferenceService.spec.<component>.minReplicas` / `maxReplicas` derived
from `replicas: { min, max }` for v0.1 scaling bounds.

## 10. Open work items

The current chart explicitly defers the following:

1. **KEDA autoscaling metrics body.**
   `deployments[].<component>.autoscaling.metrics` is reserved as a
   passthrough surface for the KEDA scaler config but is not yet wired
   into the rendered InferenceService.
2. **Router load-balancer enum.**
   `deployments[].router.loadBalancingAlgorithm` is typed as a
   free-form string and emitted as `--load-balancer <value>` on the
   router runner. A typed enum (and the corresponding router CLI flag
   agreement) is a follow-up.
3. **Annotation passthrough allowlist** for tenant InferenceServices
   (the chart currently emits a fixed `ome.io/deploymentMode:
   OMENative`). See `TODO-ANNOTATION-ALLOWLIST.md`.
4. **Pre-install / pre-upgrade hooks** that verify a referenced vendor
   ClusterServingRuntime exists when `vendors.deployAll: false` and no
   in-release vendor template matches.
5. **`values.schema.json` auto-generation** from OME CRD OpenAPI
   schemas (the schema is hand-written for v0.1).
6. **Per-flag drop for chart base defaults.** The 3-layer args merge
   lets scientists OVERRIDE a chart-hardcoded flag value via `args:`
   (e.g. `--port 31000` replaces base `--port 30000` via
   dedup-by-flag-name), but there is no mechanism to fully DROP a base
   flag (e.g. remove `--enable-prefix-cache` entirely). A future
   `removeArgs:` (or similar) field could express that.
