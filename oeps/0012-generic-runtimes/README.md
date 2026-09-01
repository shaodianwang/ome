# OEP-0012: Generic ServingRuntimes + ISVC-driven tuning

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Part A: Authoring convention — runtimes are engine-level, tuning is ISVC-level](#part-a-authoring-convention)
  - [Part B: Inheritance — validation webhook + runtime controller](#part-b-inheritance--validation-webhook--runtime-controller)
  - [Part C: Engine preset — small mutating webhook in Go for built-in engines](#part-c-engine-preset--small-mutating-webhook-in-go-for-built-in-engines)
  - [User Stories](#user-stories)
  - [Notes/Constraints/Caveats](#notesconstraintscaveats)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Env var substitution semantics](#env-var-substitution-semantics)
  - [Inheritance annotations + merge contract](#inheritance-annotations--merge-contract)
  - [Scope rules](#scope-rules)
  - [Webhook validation](#webhook-validation)
  - [Migration path](#migration-path)
  - [Test Plan](#test-plan)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
<!-- /toc -->

## Summary

This OEP addresses ServingRuntime YAML bloat (currently 100-500 lines
per runtime, with the same boilerplate repeated across ~10 runtimes)
through three complementary changes:

1. **Authoring convention (no code change)**: write *engine-level*
   runtimes — one per (engine, deployment mode) combination — whose
   commands use `$(VAR)` placeholders for model-specific tuning. ISVCs
   supply the env vars. Kubernetes substitutes `$(VAR)` in
   `command`/`args` at pod-create time; nothing new for OME to
   implement.
2. **Inheritance via validating webhook + runtime controller**: two
   annotations on ServingRuntime (`ome.io/runtime-profile` +
   `ome.io/inherit-from`) plus a small webhook (validation only) and
   a runtime controller that resolves inheritance chains into
   `Status.EffectiveSpec`. Lets the remaining infra boilerplate
   (multus, NCCL envs, GPU/RDMA resources, dshm volume) live in a
   single cluster-wide *profile* runtime that the small set of engine
   runtimes inherit. Multi-level chains supported.
3. **Engine preset mutating webhook (small Go code)**: one annotation
   (`ome.io/engine: <name>`) triggers OME-shipped Go presets to inject
   image + command + ports + env defaults for well-known engines
   (sglang first, vLLM / TGI as follow-ups). Researchers write ~10
   lines of YAML and get a working runtime. Deliberately mutating —
   the "referenced thing" is OME's binary, so the staleness window
   that ruled out mutation in Part B doesn't apply.

Together, the org goes from ~10 model-specific runtimes (each 400-500
lines) to ~3 engine runtimes (each ~150 lines after inheritance) +
1-N infra profiles. Per-model ISVCs grow by ~15 lines of tuning env
vars in exchange. Researchers using the engine preset write ~10
lines of runtime + ~10 lines of ISVC tuning.

Net change in YAML across the org for a typical 10-model fleet:
**~5500 → ~1500 lines**, with the per-model surface concentrated in
ISVCs where it belongs (model teams own ISVCs; platform owns runtimes
and profiles).

## Implementation status

Part B has shipped. Two deviations from the design below
landed during implementation; the rest of this document reflects the
original design intent.

- **`Status.EffectiveSpec` was dropped.** Consumers no longer read a
  controller-resolved spec field on the runtime. They call client-side
  helpers (`runtimeinheritance.ResolveClusterRuntime` /
  `ResolveNamespacedRuntime`) or `runtimeselector.GetRuntime`, which
  returns the resolved spec inline. `ServingRuntimeStatus` now carries
  only `InheritanceChain` and an `InheritanceReady` condition.
- **The runtime controller still drives status.** Chain + condition
  reflect resolvability so admission webhooks and operators can see
  why a chain is broken without re-walking it. The controller no
  longer materializes a merged spec.

Anywhere this document says "writes Status.EffectiveSpec" or "reads
Status.EffectiveSpec", read it as "client-side resolve via the
helpers above." The merge contract (fill-only-nil, root-first walk,
cycle detection, depth limit) is unchanged.

Part C below composes with Part B as designed: Part C mutates the
stored runtime spec at admission, and Part B's resolver runs on the
already-engine-populated spec at consumer call sites.

## Motivation

The ServingRuntime CRD exists so that many InferenceServices can share
one engine configuration. In practice, every new model variant gets its
own ServingRuntime (`srt-llama4-nvfp4-pd`, `srt-mistral-7b-instruct`,
`srt-kimi-k2-pd`, `srt-deepseek-rdma-pd`, …) because **model-specific
tuning is hardcoded directly into `runner.command`**:

```yaml
command:
  - python3
  - -m
  - sglang.launch_server
  - --tp-size=4               # model-specific
  - --ep-size=4               # model-specific
  - --quantization=modelopt_fp4  # model-specific
  - --context-length=1048576  # model-specific
  - --mem-fraction-static=0.8 # model-specific
  - --moe-runner-backend=flashinfer_trtllm  # model-specific
  - --speculative-algorithm=EAGLE3  # model-specific
  - --speculative-draft-model=$(MODEL_PATH)/draft_hf  # already parameterized — proof the pattern works
  ...
```

A new model variant ⇒ new ServingRuntime. The CRD's purpose (one
runtime, many ISVCs) is defeated.

A second source of bloat is **infrastructure boilerplate** that
repeats across every runtime regardless of model:

- 16 multus macvlan annotations
- `arm64` node selector + `nvidia.com/gpu` toleration
- `dshm` (940 GiB Memory) + `tmp-storage` volumes + matching mounts
- ~15 `NCCL_*` / `CUDA_*` env vars
- `nvidia.com/gpu: 4` + `rdma/ib: 4` resources, `IPC_LOCK` capability
- Prometheus scrape annotations, probe shapes

For a typical PD runtime, infra is ~75% of the YAML.

### Goals

- ServingRuntime authoring becomes engine-level: one runtime per
  (engine, deployment mode) — e.g. `srt-sglang-rt`, `srt-sglang-pd`,
  `srt-vllm-rt` — not one per model.
- Per-model tuning lives on the ISVC via env vars that the runtime
  command consumes via `$(VAR)` substitution.
- Infra-level fields (multus, node selector, tolerations, NCCL envs,
  GPU/RDMA resources, dshm volumes) live in a single infra profile
  per cluster shape; engine runtimes inherit via annotation.
- No new CRD. The two annotations + a small mutating webhook are the
  entire API addition.
- Controller stays declarative — sees fully-merged runtimes,
  unchanged behaviour.
- Operator decoupled from engine: profiles describe **cluster
  infrastructure**, engine runtimes describe **engine wrappers**;
  neither knows about specific models.

### Non-Goals

- **No new CRD.** The "profile" concept is just a ServingRuntime with
  `ome.io/runtime-profile: "true"` + `disabled: true`. Scope (cluster
  vs namespace) reuses ServingRuntime's existing scoping.
- **No engine-specific field validation** in OME. The webhook merges
  generic Pod/Container fragments; it does not know what
  `--tp-size` means.
- **No deep merge of `command` / `args` lists.** Inherited commands
  are taken verbatim; overriders replace the whole list. Per-flag
  patching is explicitly out of scope (see [Alternatives](#alternatives)
  for why).
- **No automatic re-propagation when a profile updates.** Profile
  changes apply on the next runtime Create/Update (Phase 1). A
  controller-driven re-trigger is a Phase 2 nice-to-have, not
  required for the design to work.

## Proposal

### Part A: Authoring convention — runtimes are engine-level, tuning is ISVC-level

This part is **documentation + migration**, no code.

A ServingRuntime defines an engine wrapper. Its command uses `$(VAR)`
placeholders for every value that varies between models. ISVCs supply
those values via env vars on `spec.engine.runner.env` (or
`spec.decoder.runner.env`).

```yaml
# ONE runtime per engine + deployment mode
kind: ClusterServingRuntime
metadata:
  name: srt-sglang-pd
spec:
  supportedModelFormats:
    - {modelArchitecture: Llama4ForCausalLM, ...}
    - {modelArchitecture: MistralForCausalLM, ...}
    - {modelArchitecture: MixtralForCausalLM, ...}
    - {modelArchitecture: DeepSeekV3ForCausalLM, ...}
    # … all architectures sglang-PD supports
  engineConfig:
    runner:
      image: ghcr.io/.../sglang:gb300-stable
      command:
        - python3
        - -m
        - sglang.launch_server
        - --model-path=$(MODEL_PATH)
        - --tp-size=$(TP_SIZE)
        - --ep-size=$(EP_SIZE)
        - --dtype=$(DTYPE)
        - --quantization=$(QUANTIZATION)
        - --kv-cache-dtype=$(KV_CACHE_DTYPE)
        - --mem-fraction-static=$(MEM_FRACTION)
        - --context-length=$(CONTEXT_LENGTH)
        - --moe-runner-backend=$(MOE_BACKEND)
        - --speculative-algorithm=$(SPEC_ALGORITHM)
        - --speculative-draft-model=$(MODEL_PATH)/$(SPEC_DRAFT_SUFFIX)
        - --speculative-draft-model-quantization=$(SPEC_DRAFT_QUANT)
        - --disaggregation-mode=prefill
        - --disaggregation-bootstrap-port=8998
        - --disaggregation-transfer-backend=$(DISAGG_BACKEND)
```

```yaml
# Every model variant is a short ISVC
kind: InferenceService
metadata: {name: llama4-nvfp4, namespace: playground}
spec:
  model: {name: llama4-nvfp4}
  runtime: {name: srt-sglang-pd}
  engine:
    runner:
      env:
        - {name: TP_SIZE,           value: "4"}
        - {name: EP_SIZE,           value: "4"}
        - {name: DTYPE,             value: bfloat16}
        - {name: QUANTIZATION,      value: modelopt_fp4}
        - {name: KV_CACHE_DTYPE,    value: bfloat16}
        - {name: MEM_FRACTION,      value: "0.8"}
        - {name: CONTEXT_LENGTH,    value: "1048576"}
        - {name: MOE_BACKEND,       value: flashinfer_trtllm}
        - {name: SPEC_ALGORITHM,    value: EAGLE3}
        - {name: SPEC_DRAFT_SUFFIX, value: draft_hf}
        - {name: SPEC_DRAFT_QUANT,  value: unquant}
        - {name: DISAGG_BACKEND,    value: nixl}
```

Kubernetes substitutes `$(VAR)` in `command`/`args` at pod-create time
from the container's own env list. This is built-in container behaviour
(see [Env var substitution semantics](#env-var-substitution-semantics))
and requires **zero OME changes**.

The existing OME merge of ISVC.spec.engine.runner into the runtime's
runner already appends the ISVC's env entries onto the runner
container, which is exactly what `$(VAR)` substitution needs.

### Part B: Inheritance — validation webhook + runtime controller

The infra layer (75% of every runtime's YAML) is the same across all
engine runtimes that target the same cluster. Express it once as a
profile; engine runtimes inherit.

Inheritance is **multi-level** — a runtime can inherit from another
runtime that itself inherits from a profile, etc. Resolving an N-deep
chain at every ISVC reconcile would mean N fetches per ISVC per
reconcile (multiplied across hundreds of ISVCs that share the same
runtime). Wrong place to do the work.

Instead, a small **runtime controller** owns the resolution:

- **On any runtime change** (Create / Update / Delete), the controller
  recomputes the fully-merged spec for that runtime — walking
  `ome.io/inherit-from` chain to its root — and stores the result on
  the runtime's own `Status.EffectiveSpec`.
- **For each runtime whose chain transitively includes the changed
  one**, the controller re-resolves and updates their status too.
  Dependents are found by scanning runtimes' annotations (small N;
  realistic deployments have well under 100 runtimes).
- **A condition `InheritanceReady`** reflects whether the chain
  resolves cleanly. Reasons cover missing parent, cycle, scope
  violation.

The ISVC controller becomes a **reader** of resolved state:

```
ISVC reconcile (every reconcile):
  fetch runtime B
  if B.Status.Conditions[InheritanceReady] != True → block + surface on ISVC
  use B.Status.EffectiveSpec (already merged) as the runtime spec
  MergeRuntimeSpecs(isvc, B.Status.EffectiveSpec)   # existing
  generate pod spec
```

No recursion on the ISVC hot path. Resolution happens **once per
runtime change**, not once per ISVC reconcile.

A validating-only webhook on `ServingRuntime` / `ClusterServingRuntime`
provides fast CRUD-time feedback before the runtime controller picks
it up:

- `inherit-from` target exists
- Scope rules satisfied (cluster→cluster only; namespace→cluster or same-ns)
- Profile invariants (`runtime-profile: "true"` ⇒ `disabled: true`)
- Cycle detection (chain walk; webhook bails on first repeat)
- Depth limit (configurable, default 5 — guard against pathological
  graphs)

The webhook **does not mutate the runtime**. `kubectl get csr -o yaml`
shows exactly what the operator wrote, alongside the resolved
`Status.EffectiveSpec` for visibility into what's actually used.

A "profile" is structurally a ServingRuntime carrying only infra
fields, marked with one annotation and disabled so it can't be
selected:

```yaml
kind: ClusterServingRuntime
metadata:
  name: profile-gb300-rdma-16
  annotations:
    ome.io/runtime-profile: "true"
spec:
  disabled: true
  engineConfig:
    nodeSelector: {kubernetes.io/arch: arm64}
    tolerations: [{key: nvidia.com/gpu, operator: Exists, effect: NoSchedule}]
    annotations:
      k8s.v1.cni.cncf.io/networks: '[…16 multus entries…]'
      prometheus.io/scrape: "true"
      prometheus.io/port: "30000"
    volumes:
      - {name: dshm, emptyDir: {medium: Memory, sizeLimit: 940Gi}}
      - {name: tmp-storage, emptyDir: {}}
    runner:
      env:
        - {name: NCCL_MNNVL_ENABLE,        value: "1"}
        - {name: NCCL_CUMEM_ENABLE,        value: "1"}
        - {name: NCCL_IB_ADDR_FAMILY,      value: AF_INET6}
        - {name: NCCL_SOCKET_IFNAME,       value: eth0}
        - {name: NCCL_SOCKET_FAMILY,       value: AF_INET6}
        - {name: CUDA_ENABLE_COREDUMP_ON_EXCEPTION, value: "1"}
        - {name: SGLANG_HOST_IP, valueFrom: {fieldRef: {fieldPath: status.podIP}}}
        # … etc
      resources:
        requests: {nvidia.com/gpu: "4", rdma/ib: "4"}
        limits:   {nvidia.com/gpu: "4", rdma/ib: "4"}
      volumeMounts:
        - {name: dshm, mountPath: /dev/shm}
        - {name: tmp-storage, mountPath: /tmp}
      securityContext:
        capabilities: {add: [IPC_LOCK, SYS_PTRACE]}
        privileged: true
```

An engine runtime inherits via annotation:

```yaml
kind: ClusterServingRuntime
metadata:
  name: srt-sglang-pd
  annotations:
    ome.io/inherit-from: profile-gb300-rdma-16
spec:
  supportedModelFormats: [...]
  engineConfig:
    runner:
      image: ghcr.io/.../sglang:gb300-stable
      command: [python3, -m, sglang.launch_server, --tp-size=$(TP_SIZE), ...]
      # NO env block, NO resources, NO volumeMounts, NO securityContext —
      # inherited from profile
  decoderConfig:
    runner:
      image: ghcr.io/.../sglang:gb300-stable
      command: [python3, -m, sglang.launch_server, …, --disaggregation-mode=decode]
```

At ISVC reconcile time, the controller:

1. Fetches the referenced runtime B (existing behaviour).
2. **New:** if B carries `ome.io/inherit-from: <profile>`, fetches
   the profile, then calls a new `MergeRuntimeFromProfile` helper
   (sibling to the existing `MergeRuntimeSpecs`) that fills profile
   fields into B's unset positions, in memory.
3. Continues with `MergeRuntimeSpecs(isvc, mergedB)` — existing
   behaviour.
4. Generates the pod spec from the result.

The validating webhook (above) gives operators fast feedback on
referential mistakes (missing profile, scope violation) at CRUD time,
but does **not** rewrite the runtime. Stored state stays sparse, and
profile updates flow through to existing ISVCs on the next reconcile.

### Part C: Engine preset — small mutating webhook in Go for built-in engines

Parts A and B cover the operator-authored layers. Researchers want
even less — pick an engine by name, get a working runtime with no
YAML beyond `supportedModelFormats`. This part adds a small,
*deliberately opinionated* mutating webhook that recognizes
`ome.io/engine: <name>` and injects engine-specific defaults from
Go-defined presets.

The annotation:

```yaml
kind: ServingRuntime
metadata:
  name: my-llama
  namespace: my-team
  annotations: {ome.io/engine: sglang-pd}
spec:
  supportedModelFormats: [{modelArchitecture: LlamaForCausalLM, ...}]
```

The webhook injects `engineConfig.runner.{image, command, env, ports}`
+ matching `decoderConfig` + `routerConfig` from a hardcoded
`sglang-pd` preset. Researcher writes ~10 lines of YAML; the pod
spec that materializes is identical to what a hand-written 200-line
runtime would produce.

#### Why a mutating webhook here, after rejecting it for Part B

The staleness concern that drove Part B to controller-side merge
**does not apply to engine presets**. The "referenced thing" for an
inheritance annotation is a `ClusterServingRuntime` that operators
edit independently — drift between admission and runtime is real. The
"referenced thing" for an engine preset is **OME's binary**, which
changes only on operator-driven upgrade. The drift window matches the
upgrade cadence, and operators expect re-reconciliation when they
upgrade.

So mutation at CRUD is the right shape: stored expanded means
`kubectl get csr -o yaml` shows exactly what the pod will use, with
no controller indirection.

#### Code shape

New package `pkg/webhook/admission/runtimepreset/` with three files:

- **`sglang.go`** (~100 lines) — Go functions returning
  `*v1beta1.ServingRuntimeSpec` for each known sglang shape:
  `sglangRTPreset()`, `sglangPDPreset()`. Container.command uses
  `$(VAR)` placeholders for everything that varies per model
  (composes with Part A). One file per engine family.

- **`webhook.go`** (~80 lines) — `Mutator` handler. Reads
  `ome.io/engine` annotation; rejects unknown values; otherwise
  applies the preset via the merge helper and returns the JSON patch.

- **`merge.go`** (~100 lines) — fill-only-nil merger (same
  contract as Part B's resolver). Preset fills unset positions on
  the runtime; runtime always wins on collision.

A registry maps annotation value → preset function:

```go
var presets = map[string]func() *v1beta1.ServingRuntimeSpec{
    "sglang-rt": sglangRTPreset,
    "sglang-pd": sglangPDPreset,
    // future engines: one map entry + one preset function per addition
}
```

Adding vLLM or TGI is one new file + one map entry. No webhook
changes, no merge changes, no other code touched.

#### How Part C composes with A and B

The three parts are independent and stack cleanly. A researcher can
use any combination:

| Researcher writes | Part A (`$(VAR)`) | Part B (inherit-from) | Part C (engine) |
|---|---|---|---|
| Fully hand-written runtime | optional | no | no |
| Inherits from operator profile | optional | yes | no |
| Picks built-in engine | applied by preset | no | yes |
| Picks built-in engine **AND** inherits operator infra profile | applied by preset | yes | yes |

The last row is the "researcher happy + platform consistent" case:

```yaml
metadata:
  annotations:
    ome.io/engine: sglang-pd                       # OME-shipped engine bundle (Part C)
    ome.io/inherit-from: profile-gb300-rdma-16     # operator infra profile (Part B)
```

Ordering: Part C's webhook runs at admission (mutates stored runtime
with engine defaults). Part B's resolver runs on demand at consumer
call sites, sees the now-engine-populated runtime, walks the
inherit-from chain, and fills remaining gaps from the operator infra
profile. The ISVC controller calls the resolver to get the merged
spec. Both merges are fill-only-nil, so order is commutative for any
field set by exactly one source.

#### Scope of the first PR for Part C

- sglang-pd preset only (the immediate need).
- ~300 lines of Go in `pkg/webhook/admission/runtimepreset/` +
  webhook registration in `cmd/manager/main.go`.
- One smoke test that creates a runtime with `ome.io/engine: sglang-pd`
  and asserts the resulting runtime has expected image + command
  shape.

vLLM, TGI, sglang-rt, etc. follow as one-file additions when
requested.

#### Trade-off acknowledged

Part C puts engine-specific knowledge into OME's binary. The OME team
becomes the gatekeeper for "what's a built-in engine." Operators who
want full control author their own profiles via Part B; the engine
annotation is for the lane that values short YAML over flexibility.

### User Stories

#### Story 1: New model variant

Operator wants to serve a new Llama variant. Today: clones an existing
~500-line runtime, edits 5 lines for the new model's tuning, sends a
PR. After: writes a 50-line ISVC. Zero new runtimes.

#### Story 2: Cluster hardware upgrade

Platform team rolls a new node class (e.g. switches from RDMA-16 to
RDMA-8). Today: touches every model-specific runtime to update
resources + multus annotations. After: updates **one** profile
(`profile-gb300-rdma-16` → `profile-gb300-rdma-8`). Engine
runtimes and ISVCs unchanged. ISVCs that reference an engine runtime
inheriting from the profile pick up the new infra on their next
reconcile (≤30s). New pods spawned after that point have the new
infra; existing pods keep running until they're naturally rolled.

#### Story 3: Engine version bump

Platform team upgrades the sglang image. Today: updates the image
field on 10 runtimes. After: updates 1 engine runtime
(`srt-sglang-pd`), 1 ISVC roll per model.

#### Story 4: Researcher spins up a new engine end-to-end (Part C)

Researcher wants to serve a Mistral model with sglang. Today: copies
an existing 500-line sglang runtime, edits 5 lines. After:

```yaml
kind: ServingRuntime
metadata: {name: my-mistral, namespace: my-team, annotations: {ome.io/engine: sglang-rt}}
spec:
  supportedModelFormats: [{modelArchitecture: MistralForCausalLM, ...}]
```

Plus a 15-line ISVC with model ref + tuning env vars. Total
researcher-authored YAML: ~25 lines. Image, command, ports, sglang
env defaults — all from the Go-defined preset.

### Notes / Constraints / Caveats

- **`$(VAR)` substitution is per-container.** If the runtime's
  command references `$(TP_SIZE)`, that env var must exist on the
  same container's env list at pod-create time. The ISVC's
  `spec.engine.runner.env` entries get merged onto the runner
  container by OME's existing merge logic.
- **Substitution happens at pod-create time, not at ISVC reconcile.**
  Operators looking at the merged Deployment will see the raw
  `$(VAR)` strings in the spec; the actual substitution happens in
  kubelet when the container starts. `kubectl describe pod` shows the
  resolved env values.
- **`disabled: true` on profiles** prevents runtime selection from
  picking them. Webhook validates that any runtime with
  `runtime-profile: "true"` is also disabled.
- **Profile updates propagate automatically.** Merging at ISVC
  reconcile means a profile edit reaches affected ISVCs on their
  next reconcile (≤30s with the existing requeue). No re-apply
  dance, no separate controller required for correctness.

### Risks and Mitigations

- **Engine that doesn't support `$(VAR)` in args.** Some engines
  parse args strictly. Mitigation: this is the standard Kubernetes
  substitution that already works on every runtime in the cluster
  (existing runtimes already use `$(MODEL_PATH)`); no new failure
  mode.
- **ISVC author forgets to set a required tuning env var.**
  `$(VAR)` substitution leaves the literal string `$(VAR)` if the
  var is unset, which the engine will reject. Mitigation: clear
  errors at engine startup; ISVC author corrects + redeploys. Future
  enhancement: a defaulting webhook can warn on missing vars (out of
  scope for this OEP).
- **Profile deleted while runtimes still inherit from it.** ISVC
  reconcile fails to fetch the profile. Mitigation: surface as an
  `InheritanceUnresolved` condition on the ISVC; reconciler does
  *not* fall back to the un-inherited runtime spec (would be a
  silent regression). Operator either restores the profile or
  edits the runtime to drop the inherit-from annotation.
- **Inheritance cycles.** A → B → A. Mitigation: validating webhook
  walks the chain at CRUD time and rejects on first repeat.
  Defensive cycle check in the runtime controller's resolver as
  belt-and-braces (catches cycles introduced by out-of-order
  creates).
- **Deep chains slow down resolution.** Mitigation: webhook caps
  depth at 5 by default. Controller does the work once per change,
  not per ISVC.
- **Operators confused by `kubectl get csr` showing profiles
  alongside real runtimes.** Mitigation: kubectl printer column for
  `runtime-profile` annotation; recommend label selector convention
  for filtering.
- **Per-reconcile merge cost.** Each reconcile fetches one extra
  object (the profile) and runs one structural merge. The profile
  is cached by the controller-runtime client, so the fetch is local.
  The merge is small (~one Pod/Container fragment). Negligible
  compared to the rest of reconcile.

## Design Details

### Env var substitution semantics

Kubernetes substitutes `$(VAR_NAME)` in `Container.command` and
`Container.args` from the container's `env` list at pod-creation
time. See the [Kubernetes documentation on dependent environment
variables](https://kubernetes.io/docs/tasks/inject-data-application/define-interdependent-environment-variables/).

OME's existing logic (`pkg/controller/v1beta1/inferenceservice/utils/reconciliation.go`,
`MergeRuntimeSpecs`) merges the ISVC's `spec.engine.runner.env` onto
the runtime's runner container env. The ISVC's env wins on duplicate
keys. After merge, both sets are present on the runner container.

This OEP adds **no substitution logic to OME**. The existing K8s
behaviour does the work.

### Inheritance annotations + merge contract

Two annotations on `ServingRuntime` / `ClusterServingRuntime`:

- `ome.io/runtime-profile: "true"` — marks this runtime as a profile.
  Profiles cannot be auto-selected by the runtime selector and must
  also set `spec.disabled: true` (webhook-enforced).
- `ome.io/inherit-from: <name>` — names the parent runtime to merge
  from. Resolved by the runtime controller; result stored in
  `Status.EffectiveSpec`; never overwrites explicitly-set fields on
  the inheriting runtime.

**Merge contract:**

| Field type | Merge behaviour |
|---|---|
| Scalar (string, int, bool) | Inheritor wins if non-zero; otherwise take parent value |
| Map (annotations, labels, nodeSelector) | Per-key: inheritor wins on collision; union of keys |
| List of named items (env vars, volumes, volume mounts, ports, tolerations) | Per-name: inheritor wins on collision; union of names |
| Plain list (**command, args**) | Inheritor's value replaces parent's entirely (no per-element merge) |
| Nested struct (runner, podSpec) | Recurse into the field-level merge above |

The resolver walks `engineConfig`, `decoderConfig`, `routerConfig`
independently. A parent carrying only `engineConfig` does not
contribute fields to a runtime that only declares `decoderConfig`.

#### Why command/args are wholesale-replace (and how to override one flag)

Container args have too many shapes for OME to merge intelligently
without coupling to engine conventions:

- `--port 8080` (space-separated)
- `--port=8080` (equals-separated)
- `--log-requests` (boolean toggle, no value)
- `--include foo --include bar` (repeated flag)
- `python -m mod --port 8080` (positional + flag)
- `cmd subcommand --opt value` (subcommand-style)

Per-flag merging would need a parser that knows which shape an engine
uses — exactly the engine-coupling this OEP avoids.

The override pattern is to **parameterize in the parent with
`$(VAR)`** and override the env var in the child:

```yaml
# Parent
runner:
  command: [python, -m, sgl, --port=$(PORT)]
  env: [{name: PORT, value: "8080"}]
```

```yaml
# Child — overrides ONE flag, no command duplication
runner:
  env: [{name: PORT, value: "9090"}]
  # command unset → inherited verbatim from parent
```

After merge: `command` from parent, `env` union with child winning on
`PORT`. K8s substitutes `$(PORT)=9090` at pod-create. The engine
never sees that OME was involved.

If the parent didn't parameterize what the child wants to change, the
child must copy the entire `command` list. Loses dedup for that
runtime; always works as an escape hatch.

### Scope rules

| Inheriting runtime | Allowed profile targets |
|---|---|
| `ClusterServingRuntime` | `ClusterServingRuntime` only (no namespace-scoped data leaks into cluster scope) |
| `ServingRuntime` in namespace X | `ClusterServingRuntime` OR `ServingRuntime` in namespace X (same scope or strictly broader) |

Webhook rejects anything else with a clear error citing the offending
scope combination.

### Webhook validation (no mutation)

In `pkg/webhook/admission/servingruntime/` (existing validator,
extended). The webhook **does not mutate** the runtime — it only
rejects invalid combinations at CRUD time for fast operator feedback:

1. **Parent must exist + be in scope**: `inherit-from` target resolves
   to an existing runtime within the allowed scope (see
   [Scope rules](#scope-rules)).
2. **Profile must be disabled**: `runtime-profile: "true"` ⇒
   `spec.disabled: true` required. Prevents an under-specified
   profile from being auto-selected for serving.
3. **No cycles**: the webhook walks `inherit-from` to its root; on
   repeat, rejects.
4. **Depth limit**: chain length capped at `OME_RUNTIME_INHERIT_MAX_DEPTH`
   (default 5) to bound resolver work and force sane composition
   patterns.

Multi-level inheritance IS supported — e.g. `srt-sglang-pd-llama4` ⇒
`profile-sglang-pd` ⇒ `profile-gb300-rdma-16` ⇒ `profile-base`
is a four-level chain that resolves cleanly. The webhook's job is
just to keep it bounded and acyclic.

### Runtime controller

New controller in `pkg/controller/v1beta1/servingruntime/`. Reconciles
both `ServingRuntime` and `ClusterServingRuntime` (separate
Reconciler instances sharing a resolver helper).

**Reconcile logic** for a runtime X:

1. Walk X's inheritance chain via `ome.io/inherit-from` to its root,
   collecting `[X, X.parent, X.grandparent, ...]` (max depth from
   webhook).
2. If any parent is missing / cycle detected / scope violation: set
   `Status.Conditions[InheritanceReady] = False` with reason; do
   not update `Status.EffectiveSpec`.
3. Merge bottom-up (root → X) with the fill-only-nil contract:
   `eff = root.Spec; for each child in chain[:-1]: merge child.Spec onto eff`.
4. Set `Status.EffectiveSpec = eff`,
   `Status.InheritanceChain = [names...]`,
   `Status.Conditions[InheritanceReady] = True`.

**Watch setup:**

- `For(ServingRuntime{})` — direct watch on the type the reconciler owns.
- `Watches(ClusterServingRuntime{}, mapFunc)` — when a cluster-scoped
  runtime changes, enqueue all runtimes whose chain includes it.
  Mapper scans cached runtimes (all-cluster-cached via the existing
  informer the ISVC controller already uses) and returns reconcile
  requests for dependents.
- Symmetric watcher for ServingRuntime→dependents (within-namespace
  and cluster-scope inheritors).

Dependent lookup is O(N) per parent change; N is the total runtime
count cluster-wide. Acceptable for realistic deployments (<100
runtimes). If N grows beyond that, add an in-memory `parent → set of
children` index.

**Cycle defence in the controller** (belt and braces with the webhook):
the chain walk tracks visited names; on repeat, returns a typed error
and sets `InheritanceReady=False` with `Reason=InheritanceCycle`.

### Status fields on ServingRuntime / ClusterServingRuntime

Two new fields on `ServingRuntimeStatus`:

```go
type ServingRuntimeStatus struct {
    // EffectiveSpec is the fully-resolved spec the ISVC controller
    // uses for pod-spec generation. Populated by the runtime
    // controller. Equals Spec when no inheritance is declared.
    // +optional
    EffectiveSpec *ServingRuntimeSpec `json:"effectiveSpec,omitempty"`

    // InheritanceChain lists the runtime names walked to compute
    // EffectiveSpec, in root-first order. Single-entry list ([self])
    // when no inheritance.
    // +optional
    // +listType=atomic
    InheritanceChain []string `json:"inheritanceChain,omitempty"`

    // Conditions: InheritanceReady (True / False) — informational.
    // +optional
    Conditions []metav1.Condition `json:"conditions,omitempty"`
}
```

### Migration path

This is the largest part of the work, and it is **operator-driven**,
not code-driven.

**Phase 0 (immediate, no code)**: document the recommended pattern.
New runtime PRs are reviewed against the "engine-level, not
model-level" guideline; PRs that hardcode `--tp-size=N` in `command`
get pushed back with a suggested ISVC env block.

**Phase 1 (~1-2 weeks of work)**:
- Ship the validating webhook + two annotations.
- Ship the runtime controller that populates `Status.EffectiveSpec`.
- Update the ISVC controller to read `Status.EffectiveSpec` instead
  of `Spec` when it's populated (backward-compatible: runtimes
  without inheritance have `EffectiveSpec == Spec`, so the read is
  uniform either way).
- Migrate one cluster's worth of infra boilerplate into a profile
  (`profile-gb300-rdma-16`). Migrate one runtime
  (`srt-sglang-pd`) to inherit from it. Verify the resulting pod
  spec (after ISVC reconcile) is byte-identical to the pre-migration
  one.

**Phase 2 (gradual)**: refactor existing model-specific runtimes
into one engine runtime per (engine, mode). For each retired
runtime, the ISVCs that referenced it switch to the generic runtime
and add their tuning env vars. Coordinate with model teams; aim for
one engine family per week.

**Phase 3 (cleanup)**: delete the old model-specific runtimes once
no ISVC references them. Update OEP-0007 (if it mentions runtime
patterns) to reflect the new convention.

**Phase 4 (Part C, independent)**: ship the engine preset webhook
with `sglang-pd` as the only preset. Researchers begin using
`ome.io/engine: sglang-pd` for new model serving. Add `sglang-rt`,
`vllm-*`, `tgi-*` etc. as separate one-file PRs as the org needs
them. This phase is independent of Phases 1-3 and can ship in
parallel.

### Test Plan

#### Unit Tests

- Resolver: each merge-behaviour row in the table above exercised
  with a 2-line fixture; bottom-up chain merge of a 4-deep chain
  produces the expected union.
- Scope rules: matrix of (inheritor scope, parent scope) →
  allowed/rejected.
- Cycle detection: webhook rejects A → B → A.
- Depth limit: webhook rejects 6-deep chain when default is 5.

#### Integration Tests

Add to `tests/integration/singlecluster/webhook/servingruntime/`
(validation only):

- Namespace runtime trying to inherit from cross-namespace profile:
  rejected with clear message.
- Cluster runtime trying to inherit from namespace profile:
  rejected.
- Profile with `runtime-profile: "true"` but `disabled: false`:
  rejected.
- Chain exceeding max depth: rejected.

Add to `tests/integration/singlecluster/controller/servingruntime/`
(new suite — runtime controller is new):

- **Two-level chain resolves**: profile → runtime; runtime's
  `Status.EffectiveSpec` carries the union; `InheritanceReady=True`.
- **Four-level chain resolves**: leaf runtime's `Status.EffectiveSpec`
  reflects all four levels merged bottom-up.
- **Parent update propagates**: edit the root profile; within one
  reconcile interval, dependent runtimes' `Status.EffectiveSpec`
  reflects the new value.
- **Parent deletion**: delete a mid-chain runtime; dependents'
  `InheritanceReady=False` with `Reason=ParentMissing`;
  `Status.EffectiveSpec` retained (not wiped) so the ISVC controller
  can continue serving with the last good state.
- **Cycle created out-of-order**: create A → B, then patch B → A;
  controller catches the cycle, sets `InheritanceReady=False` with
  `Reason=InheritanceCycle`.

Add to `tests/integration/singlecluster/controller/inferenceservice/`
(reads resolved state):

- ISVC referencing an inherited runtime: pod spec reflects the
  resolved `Status.EffectiveSpec` (infra envs from root profile +
  engine command from runtime + tuning envs from ISVC).
- ISVC referencing a runtime with `InheritanceReady=False`: ISVC
  surfaces `RuntimeInheritanceUnresolved` on its own conditions;
  Deployment unchanged from last good reconcile.
- `$(VAR)` substitution end-to-end: the resulting pod's container
  spec contains the raw `$(VAR)` string (substitution happens at
  kubelet, not OME).

Add to `tests/integration/singlecluster/webhook/runtimepreset/`
(new suite — Part C):

- **Unknown engine value**: `ome.io/engine: not-a-thing` → admission
  rejects with the list of known presets in the error message.
- **Known engine, no other fields**: bare runtime with only
  `supportedModelFormats` + the annotation → stored runtime has the
  full preset's engineConfig (and decoderConfig + routerConfig for
  PD presets) merged in. Webhook DID mutate; stored form is expanded.
- **Researcher override wins**: runtime explicitly sets
  `engineConfig.runner.image = my-fork:dev` → preset's image is NOT
  applied for that field; preset still fills other unset positions.
- **Composition with Part B**: runtime carries both
  `ome.io/engine: sglang-pd` and `ome.io/inherit-from:
  profile-gb300-rdma-16` → final `Status.EffectiveSpec` has
  engine bits from preset + infra bits from inherited profile.

#### Manual smoke

Migrate the end-to-end PD sample's `cluster-serving-runtime.yaml`
(under `config/samples/`) to inherit from a profile; verify the
resulting Deployment pod spec is byte-identical to the
pre-migration one. Also create a fresh runtime
with `ome.io/engine: sglang-pd` and assert the resulting
`Deployment` pod spec equals the migrated runtime's.

## Implementation History

- 2026-05-18: OEP drafted.
- 2026-05-18: Switched Part B merge from mutating webhook to in-memory
  resolution at ISVC reconcile to avoid stale-profile drift.
- 2026-05-18: Promoted Part B in-memory resolution to a dedicated
  runtime controller writing `Status.EffectiveSpec`. ISVC reconciler
  reads the resolved status. Reasons: multi-level inheritance
  shouldn't pay N fetches per ISVC reconcile; the work belongs once
  per runtime change, not once per ISVC reconcile.
- 2026-05-18: Added Part C — small Go mutating webhook for built-in
  engine presets triggered by `ome.io/engine: <name>` annotation.
  Mutating-at-CRUD is acceptable here because the "referenced thing"
  is OME's binary, which only changes on operator-driven upgrade.

## Drawbacks

- **`kubectl get csr` shows profiles alongside runnable runtimes.**
  Cosmetic. Mitigated by printer column + label-selector
  convention. Could be hidden via a discriminator field on the CRD
  if it becomes a real complaint.
- **Migration is human work.** Refactoring 10 runtimes into 3 +
  profile is mechanical but takes per-model coordination. Not
  technical risk, but project risk.
- **The merge contract is "conservative, fill-only-nil".** This is
  the safest semantics, but means an inheriting runtime can't
  *subtract* a field from the profile. Adding "explicit nil =
  unset" support is a Phase 2 if it comes up; deferred for now
  because it's a sharp edge in the API.
- **No deep merge of `command` / `args` lists.** An inheriting
  runtime that wants to add one flag to the profile's command must
  copy the whole command. See [Alternatives](#alternatives) for
  why we did not add per-flag patching.

## Alternatives

### Mutating webhook on runtime CRUD (merge-and-store)

Considered and rejected. The webhook would resolve the parent at
admission and persist the merged form to apiserver.

Rejected because of **staleness**: a parent update after the child
was created does not re-trigger the webhook, so existing children
keep their old merged form until operators re-apply each one. Fixing
that requires a separate controller bumping dependents when parents
change — i.e., a controller anyway, plus the webhook complexity.

### In-memory merge at every ISVC reconcile

Briefly considered. The ISVC reconciler would walk the inheritance
chain and merge in-memory each reconcile, no runtime status field.

Rejected because of **multi-level inheritance cost at scale**: a
4-deep chain means 4 fetches per ISVC per reconcile, multiplied by
every ISVC referencing a runtime in that chain. With 100 ISVCs and
30s reconciles that's ~13 fetches/sec just for inheritance
resolution. The runtime controller in this OEP does the same work
once per runtime change (rare) and caches the result on
`Status.EffectiveSpec` (read-only for ISVCs).

### A new `ClusterRuntimeProfile` CRD

Rejected. ServingRuntime already has both cluster and namespace
variants with worked-out scoping rules. A new CRD doubles the API
surface for no functional gain; reusing ServingRuntime with a marker
annotation costs zero new types.

### Per-flag patching of `command` / `args` (`argsMap`, `argsRemove`)

Tempting for the within-runtime PD case (engine + decoder differ
only by `--disaggregation-mode`). Rejected for v1 because:

- It introduces engine-coupling (the patch semantics assume
  `--flag=value` form, which isn't universal).
- Most of the same goal is achieved by **two profiles** —
  `profile-sglang-pd-prefill` carrying the engine command,
  `profile-sglang-pd-decode` carrying the decoder command — at the
  cost of one extra profile per engine.
- If real demand emerges (operators wanting to override one flag
  from a profile from their runtime), Phase 2 can add it without
  breaking the v1 design. Easier to add later than to remove.

### Deeper merge semantics (smart list merging, JSONPath patches)

Rejected. Kubernetes' own strategic merge patch is the gold
standard, and even that has known sharp edges. The conservative
"fill-only-nil" contract is easy to explain, easy to debug, and
sufficient for the 80% case. Operators who need more can fall back
to fully-specified runtimes (no inheritance).

### Templating via Helm / kustomize at the GitOps layer

Works today; doesn't require any OME change. Rejected as the
*primary* recommendation because:

- It pushes the abstraction outside OME, so `kubectl apply -f
  some-runtime.yaml` to a non-templated apiserver doesn't get the
  same defaults.
- The merged YAML in git is the source of truth, so diff reviews
  are noisy (you see the expanded fragment).
- Doesn't compose with OME-native features (validation, status,
  controller).

Operators are still free to use Helm/kustomize on top of this OEP's
output if they want a higher-level interface.

### In-memory `parent → children` index in the runtime controller

The runtime controller currently does an O(N) scan of cached
runtimes to find dependents when a parent changes. For very large N
(thousands of runtimes) this scan becomes a hotspot.

An optimization: maintain an in-memory inverted index keyed by
parent name. Updated as runtimes are added/removed; lookups are
O(1).

Deferred. Realistic deployments have N < 100 and the scan is sub-ms.
Add only if a real cluster hits the limit.
