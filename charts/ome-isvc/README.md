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
| `deployments[].prefill.args` | array | `[]` | Extra CLI flags appended after the chart production defaults and synthesised flags; user overrides win on duplicate flags. Chart-owned args list — see "Args ownership" below. |
| `deployments[].prefill.env` | array | `[]` | Extra container env vars. Each entry is a corev1.EnvVar (`{name, value}` or `{name, valueFrom: ...}`). Forwarded to the tenant CSR's `runner.env`, where they merge with the vendor's env list **by `name` key** (per-key strategic merge — predictable, additive). See "Env merge" below. |
| `deployments[].prefill.autoscaling.enabled` | bool | `false` | Placeholder. KEDA wiring is reserved for a follow-up. |
| `deployments[].prefill.autoscaling.metrics` | array | `[]` | Placeholder. Shape reserved; v0.1 templates ignore it. |
| `deployments[].decode.*` | object |  | Same shape as `prefill.*` (including `env: []`). `pd` only. |
| `deployments[].engine.*` | object |  | Same shape as `prefill.*` (including `env: []`), plus `workerSize`. `standalone` or `multinode` only. |
| `deployments[].engine.workerSize` | int |  | Multinode only: number of worker pods per leader gang. |
| `deployments[].router.enabled` | bool | `true` | `pd` only. Whether to emit the router Component. |
| `deployments[].router.image` | string | `""` | Router image override. |
| `deployments[].router.replicas.{min,max}` | int |  | Router replica counts. |
| `deployments[].router.args` | array | `[]` | Extra router CLI flags appended after the synthesised flags; user overrides win on duplicate flags. Chart-owned list. |
| `deployments[].router.env` | array | `[]` | Extra router container env vars (same shape + merge semantics as `prefill.env`). |
| `deployments[].router.timeoutSeconds` | int | `30` | Request-completion deadline; synthesised as `--backend-timeout <N>` on the router runner. |
| `deployments[].router.streamingTimeoutSeconds` | int | `600` | SSE/token-stream deadline (so long generations don't time out mid-stream); synthesised as `--streaming-timeout-secs <N>`. The exact sglang router flag name may differ between versions; adjust in `_helpers.tpl::ome-isvc.renderRouter` if upstream renames it. |
| `deployments[].router.loadBalancingAlgorithm` | string |  | Synthesised as `--load-balancer <value>` (placeholder; see open work items). |
| `deployments[].coordination.policy` | enum |  | `BlueGreen` | `Sequential` | `RollingUpdate` | `Independent`. |
| `deployments[].coordination.order` | array |  | Required for `Sequential`. Scientist names (`prefill`/`decode`/`engine`/`router`); chart translates to OME Components. |
| `deployments[].coordination.soak` | string |  | Wait between Sequential steps; Go-style duration (e.g. `"10m"`). |
| `deployments[].coordination.pacing.type` | enum |  | `PerComponent` (default) or `RatioBalanced`. |
| `deployments[].coordination.pacing.maxSurge` | int / string |  | Forwarded onto the synthesised RolloutCoordinationGroup. |
| `deployments[].coordination.pacing.maxUnavailable` | int / string |  | Forwarded onto the synthesised RolloutCoordinationGroup. |
| `deployments[].coordination.pacing.ratioTolerance` | int |  | RatioBalanced only. |
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

The same translation applies inside `rolloutCoordination.groups[].components`
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
| Container `args` | ❌ stripped | ✅ owns full list (production defaults + synthesised typed flags + user `args:`) |
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
        - --enable-prefix-cache      # ome-isvc.engineDefaultArgs
        - --schedule-policy
        - lpm
        - --context-length           # synthesised from contextLength
        - "1048576"
        - --tensor-parallel-size     # synthesised from tensorParallelSize
        - "4"
        - --mem-fraction-static      # synthesised from gpuMemoryUtilization
        - "0.92"
        - --max-running-requests     # synthesised from maxConcurrentRequests
        - "256"
        # ...followed by any user-supplied .args[]
      env:                 # per-name merge against vendor env at consume time
        - name: SGLANG_EXTERNAL_MODEL_PACKAGE
          value: sglang_meta.srt.models
```

The OME controller resolves the full spec at consume time via
strategic-merge inheritance. Child wins on collision; lists keyed by
`name` (e.g. `env`) merge per-name; plain lists like `args` are
wholesale-replaced — that's the trap. The chart sidesteps it by
having the vendor carry no `args` at all.

### Args ownership

For each engine/prefill/decode component the chart-side tenant CSR's
`runner.args` is constructed as the concatenation of:

1. **Chart production defaults** — `ome-isvc.engineDefaultArgs` in
   `templates/_helpers.tpl`. Hard-coded sglang flags every tenant
   gets unless they override the entire list (e.g. `--enable-prefix-cache`,
   `--schedule-policy lpm`). Keep this list minimal — anything that
   varies by workload belongs in a typed knob or the scientist's
   `args:`.
2. **Synthesised typed flags** — emitted from the per-component knobs
   listed below, only when explicitly set.
3. **User-supplied `args:`** — appended last so they win on duplicate
   flags.

Scientists who need to **suppress** a chart production default must
override the entire list via `args:`, or ask for the default to be
removed from the chart. The chart does **not** offer a per-flag
removal mechanism in v0.1.

### Env merge

For each component the chart-side tenant CSR's `runner.env` carries the
scientist's `env: []` entries verbatim. When OME resolves the runtime
at consume time, strategic-merge unions them with the vendor's `env`
list using `name` as the merge key — i.e. vendor entries are
preserved, child entries are added, and matching names are overridden
by the child. There is no wholesale-replace risk on `env`.

### Synthesised CLI flags

Each scientist-facing knob below maps to one sglang flag, emitted only
when you explicitly set the knob:

| Values key                | sglang flag              |
|---------------------------|--------------------------|
| `contextLength`           | `--context-length`       |
| `tensorParallelSize`      | `--tensor-parallel-size` |
| `gpuMemoryUtilization`    | `--mem-fraction-static`  |
| `maxConcurrentRequests`   | `--max-running-requests` |

Unset knobs are omitted — the chart production defaults / sglang
defaults take effect. The `args:` list you supply is appended last so
it wins on duplicate flags.

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
| `examples/values-cks-gb300-sglang-pd.yaml` | Single PD-disaggregated Llama4x stack on CW gb300 inheriting the chart-level default vendor; creates ClusterBaseModel inline. |
| `examples/values-multi-deployment.yaml`    | Two stacks (prod + shadow) from one release, both inheriting the default vendor. |
| `examples/values-gke-tpu-sglang.yaml`      | Standalone single-engine stack on GKE TPU; overrides `vendor:` to the illustrative GKE profile. |
| `examples/values-existing-basemodel.yaml`  | PD-disaggregated stack pointing at a pre-existing ClusterBaseModel; no inline `basemodels`. |

## 7. Coordination knobs

`deployments[].coordination` synthesises one `RolloutCoordinationGroup`
that spans every Component the deployment renders (prefill+decode plus
optional router for `pd`; engine for `standalone` / `multinode`).

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
  prefill:decode pods (use with `pacing.ratioTolerance` to bound how
  far the ratio may drift mid-rollout).

## 8. Autoscaling

The per-component `autoscaling:` block (`enabled`, `metrics`) is
reserved for KEDA wiring and is **not yet plumbed** into the rendered
InferenceService. v0.1 templates ignore it; the shape is reserved so
adding the body in v0.2 does not change values. Use the
`InferenceService.spec.<component>.minReplicas` / `maxReplicas` derived
from `replicas: { min, max }` for v0.1 scaling bounds.

## 9. Open work items

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
6. **Per-flag suppression for chart production defaults.** Scientists
   today must override the entire `args:` list to drop a single
   chart-default flag (e.g. `--enable-prefix-cache`). A future
   `removeArg:` (or similar) mechanism could let scientists drop a
   single default without retyping the whole list.
