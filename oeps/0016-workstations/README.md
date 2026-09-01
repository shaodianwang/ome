# OEP-0016: Workstations — on-demand GPU developer boxes

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Today's gap](#todays-gap)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Terminology](#terminology)
  - [The spec surface: shorthand + runtime](#the-spec-surface-shorthand--runtime)
  - [Storage: created or referenced, no wrapper CRD](#storage-created-or-referenced-no-wrapper-crd)
- [Design Details](#design-details)
  - [The shape comes from the runtime — and needs no model](#the-shape-comes-from-the-runtime--and-needs-no-model)
  - [Stop is not scale-to-zero](#stop-is-not-scale-to-zero)
  - [Failure: unschedulable pods are ours to escalate](#failure-unschedulable-pods-are-ours-to-escalate)
  - [Single / multi-node / PD on one substrate — and why no LeaderWorkerSet](#single--multi-node--pd-on-one-substrate--and-why-no-leaderworkerset)
  - [Status and connection](#status-and-connection)
  - [Validation inventory](#validation-inventory)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [Implementation History](#implementation-history)
<!-- /toc -->

## Summary

OME serves models well and develops them badly. A developer who wants to iterate
on a model — or just wants a GPU box shaped like production — has no first-class
object: they hand-roll a bare Pod or Deployment that *approximates* the serving
topology, drifts from it, and burns GPU while idle because there is no "stop the
compute, keep the disk, resume later."

This OEP adds a **`Workstation`** (short name `ws`): a namespaced CRD for an
on-demand, GPU-accelerated developer box. It is **standalone** — drive it with
`kubectl apply` and attach with `kubectl exec`; no ingress, no traffic, no
autoscaling. Two reuse decisions carry the design:

- **Shape comes from a `ServingRuntime`, not from hand-rolled fields.** A
  Workstation optionally names a runtime and inherits its image, resources, and
  topology — single-node, multi-node, or PD (prefill/decode disaggregated). This
  rides the model-optional **"lean path"** OME already ships (OEP-0012): an object
  that names a runtime and omits the model builds its pods from the runtime alone.
- **Storage is a PVC the box creates or references — there is NO storage CRD.** A
  PVC is already a first-class, independently-lifecycled object; a wrapper would be
  ceremony (see [Alternatives](#alternatives)).

The controller drives OME's per-Instance **workload engine** (OEP-0007) directly —
the same engine the standalone `InferenceReplica` controller drives — so a
Workstation gets in-place image updates, restart-on-loss, drain/migrate, and
revision history without new lifecycle code.

This is sub-project #1 of three. #2 is a web console; #3 is a DevPod driver that
maps onto this CRD. Both are **out of scope here** — this OEP only ensures the
status is sufficient for #2 and the shape maps cleanly for #3.

## Motivation

### Today's gap

1. **No develop-on-the-serving-substrate path.** OME's substrate (accelerator-aware
   scheduling, the workload engine, ServingRuntimes) exists only for serving. A
   developer reproducing a PD topology to debug it hand-writes engine + decoder +
   router pods — i.e. re-derives a `ServingRuntime` by hand, and it rots out of
   sync with the real one.
2. **Idle GPU is unrecoverable.** A hand-rolled dev pod has no "stopped but exists"
   state: you either pay for an idle H100 or delete the pod and lose the home disk
   (dotfiles, venvs, checkpoints). The cost-saving workflow that matters —
   **stop the pods, keep the disk, resume onto it** — has no first-class shape.
3. **Lifecycle is re-implemented per developer.** Restart-on-crash, image bumps
   without losing the session, eviction off a draining node — every team rebuilds
   these badly on top of bare pods. OME already has them in the workload engine
   (OEP-0007); nothing exposes them to a dev box.

### Goals

- A single resource for a GPU dev box: **single-node, multi-node, and PD**, all on
  one substrate (the workload engine).
- **Stop/start that keeps the home disk** — the cost lever. Stop drops the pods
  (GPU cost → 0); the disk survives; start resumes onto it.
- **Inherit topology from a `ServingRuntime`** so the common case is "name a
  runtime" — no hand-rolled images/worker-counts/PD layout.
- **Model-optional:** a bare GPU box needs no model; a develop-on-model box mounts
  a pre-staged model and gets `MODEL_PATH` for free.
- Exec-based attach (`kubectl exec`); standalone usability with `kubectl apply`.
- A status surface rich enough for a future console and a DevPod driver to consume
  with **zero** further controller work.

### Non-Goals

- **The DevPod driver (#3) and the web console (#2).** This OEP ensures the shape
  maps and the status suffices; it builds neither.
- **Auth / multi-tenancy / ownership.** v1 relies on namespace RBAC and a
  `serviceAccountName` passthrough; *who* may create a Workstation and *whose* it
  is is a separate sub-project.
- **Autoscaling / scale-to-zero of the box.** A Workstation is one Instance
  (replicas 1); idle-stop replaces scale-to-zero. (And the engine cannot scale to
  zero anyway — see [Stop is not scale-to-zero](#stop-is-not-scale-to-zero).)
- **Serving traffic, HTTPRoutes, runtime auto-selection** (a Workstation names its
  runtime, it does not match a model against one).
- **On-demand model download, snapshotting, image building, a storage CRD.**
- **Inline PD definition.** PD is available *via a runtime* only; there are no
  `engine`/`decoder`/`router` blocks on the Workstation (see
  [Alternatives](#alternatives)).

## Proposal

### Terminology

- **Workstation** : a namespaced CRD (`kind: Workstation`, short name `ws`)
  describing one developer's GPU box. It is **not** an `InferenceService` — it
  serves no traffic and has no model-to-runtime matching — but it reuses the same
  controller substrate underneath.
- **The workload engine** : OME's source-agnostic, per-Instance pod reconciler at
  `pkg/controller/v1beta1/workload/` (OEP-0007). It takes plain data in and reports
  plain status back; it imports no `v1beta1` types outside its conversion shim, and
  the standalone `InferenceReplica` controller already drives it
  (`inferencereplica/reconciler.go` → `workload.Reconcile`). The Workstation
  controller is a third caller.
- **Shape-from-runtime** : reading a `ServingRuntime`/`ClusterServingRuntime`'s
  `engineConfig`/`decoderConfig`/`routerConfig` to learn the box's image,
  resources, and topology — the same sub-specs an `InferenceService` uses
  (`servingruntime_types.go:163`).
- **The lean path** : OME's existing model-optional flow (OEP-0012). An object that
  names a runtime and omits `spec.model` builds pods from the runtime alone, with
  every model-specific step nil-guarded (`controller.go:266`, "Lean path: no
  spec.model"). A Workstation rides this path.

### The spec surface: shorthand + runtime

A Workstation describes **three things**: storage, runtime (optional), and image
(the inline fallback). Topology is *shorthand for the simple cases, runtime for the
rest*. There are deliberately **no** `engine`/`decoder`/`router` blocks on the
Workstation — single/multi-node are inline; PD comes from a runtime (rationale in
[Alternatives](#alternatives)).

Condensed:

```go
type WorkstationSpec struct {
    DesiredState WorkstationState   `json:"desiredState,omitempty"` // Running | Stopped (default Running)
    Runtime      *ServingRuntimeRef `json:"runtime,omitempty"`      // shape: image/resources/topology (+ PD)
    Model        *ModelRef          `json:"model,omitempty"`        // optional; mounts a pre-staged model
    Image        string             `json:"image,omitempty"`        // inline path when no runtime
    Command      []string           `json:"command,omitempty"`      // default keep-alive injected if empty
    Args         []string           `json:"args,omitempty"`
    Resources    v1.ResourceRequirements `json:"resources,omitempty"` // GPUs here; overrides runtime
    Env          []v1.EnvVar        `json:"env,omitempty"`
    Workers      *WorkstationWorkers `json:"workers,omitempty"`     // size N -> multi-node (leader + N)
    Storage      *WorkstationStorage `json:"storage,omitempty"`     // created OR referenced PVC
    IdleTimeout  *metav1.Duration   `json:"idleTimeout,omitempty"`  // auto-stop after idle
    ServiceAccountName string       `json:"serviceAccountName,omitempty"`
    PodSpec                         `json:",inline"`                // escape hatch (affinity, nodeSelector, …)
    Labels       map[string]string  `json:"labels,omitempty"`
    Annotations  map[string]string  `json:"annotations,omitempty"`
}
```

`ServingRuntimeRef` and `ModelRef` are **reused verbatim** from
`inference_service.go` — so a Workstation references a runtime/model exactly as an
ISVC does, and inherits `ServingRuntimeRef`'s `autoSync`/`revision` pinning
(OEP-0014) for a reproducible box for free.

The common case is small:

```yaml
apiVersion: ome.io/v1beta1
kind: Workstation
metadata: { name: pd-dev, namespace: dev }
spec:
  desiredState: Running
  runtime: { name: srt-llama4-nvfp4-pd }   # PD topology inherited from the runtime
  storage: { size: 100Gi, mountPath: /home/dev }
  idleTimeout: 2h
```

### Storage: created or referenced, no wrapper CRD

The home is a PVC, gotten one of two ways:

- **Created** — `storage.size` set: the controller creates and *owns* a PVC. It
  survives Stop and is removed on Delete unless `retainStorageOnDelete`.
- **Referenced** — `storage.claimName` set: the controller mounts an existing PVC
  it does **not** own (never creates, never deletes). The disk's lifecycle is then
  fully independent of the box — keep it across deletes, attach a new box (a
  different GPU shape) to the same home tomorrow.

Exactly one of `size`/`claimName` is set. We did **not** add a storage CRD;
rationale is in [Alternatives](#alternatives).

## Design Details

### The shape comes from the runtime — and needs no model

A `ServingRuntime` is, structurally, a pod-shape template: its
`engineConfig`/`decoderConfig`/`routerConfig` are the same `EngineSpec`/
`DecoderSpec`/`RouterSpec` an ISVC uses (`servingruntime_types.go:163`). Topology
is expressed structurally, with **no** reference to a model:

- **single vs multi-node** = whether a component declares a leader/worker block,
  with `worker.size` the worker count (`deployment.go:34-35`);
- **PD vs non-PD** = whether both an engine and a decoder are present
  (`controller.go:379-383`).

So the Workstation controller reads the runtime and reuses OME's existing,
model-free merge to resolve "runtime defaults + inline overrides → component spec"
(`MergeRuntimeSpecs`, `reconciliation.go:111`) and the topology resolver
(`DetermineDeploymentModes`, `deployment.go:44`). These are pure functions — no
model, no cluster client — so the controller calls them and then builds the dev
pods itself.

**Why no model is required:** the model-coupled runtime fields
(`SupportedModelFormats`, `ModelSizeRange`) are read **only** by the runtime
selector for auto-selection — a Workstation names its runtime, so it never touches
them. The one model tie at pod-build time is the `MODEL_PATH` env var, injected
from the model's storage URI (`base.go:196`); with no model, `modelPathEnvValue`
returns `("", false)` and `MODEL_PATH` is simply unset. Generalizing: shape-from-
runtime is fully model-free, and that is *by reuse, not by new code* — OME already
ships the lean path (`controller.go:266`); admission already allows model omission
when a runtime is named ("at least one of spec.model or spec.runtime",
`inference_service_validation.go:214-222`).

**Develop-on-a-model**, then, is additive and has one hard precondition: the
weights must already be staged on a node or PVC. This path **mounts** a pre-staged
model; it does **not** download on demand (model staging is the model-agent's job,
out of scope here). Set `spec.model` to an un-staged model and the pod pins to
nodes that never become ready and never schedules — so "develop on a model" means
"the model is already in the cluster."

### Stop is not scale-to-zero

The obvious design — "Stop ⇒ tell the engine to scale to zero pods" — **does not
work**, and getting this wrong silently leaves a paid-for GPU running. The workload
engine is built for always-on Instances and **clamps any replica count ≤ 0 up to
1** (`BuildPlan`, `workload/plan.go:24-27`): there is no scale-to-zero path. Driving
the engine toward zero keeps exactly one Instance desired.

So Stop is defined differently, and more simply:

> **Stop (`desiredState: Stopped`):** the controller stops driving the engine *and*
> deletes the box's pod(s) itself, leaving the home PVC untouched.

This is safe because the controller owns the pods, the engine only acts when
*called*, and the engine never touches storage — the renderer deep-copies the
caller's PodSpec (`workload/ops/render.go:135`) and no workload op references a PVC,
so the home survives regardless of how Stop is achieved. **Start** is the inverse:
the controller drives the engine again, and the new pod(s) mount the intact home.

### Failure: unschedulable pods are ours to escalate

The engine already escalates a **stuck** pod — image/container-runtime wedges like
`ImagePullBackOff` or `CrashLoopBackOff` — to `Failed` after a fixed 60s grace and
fires `WarnInstanceFailed` (`workload/status_aggregate.go:45-69,:25`;
`workload/types/input.go:108`). The Workstation controller wires those callbacks
and surfaces the result.

But it escalates **only** those reasons — **not** an unschedulable pod. And the
unschedulable pod is precisely the common failure for a GPU box: request an H100
when none is free and the pod sits `Pending` forever, with no error. So the
controller adds its own rule: **if the box cannot schedule within a deadline,
surface a clear condition (escalating to `Failed`), rather than hanging silently.**
This is ours to build; the engine will not do it. (Deadline default: see
[Open Questions](#open-questions).)

### Single / multi-node / PD on one substrate — and why no LeaderWorkerSet

All three topologies ride the workload engine:

- **Single node** — one pod.
- **Multi-node** — a leader (the attach target, with the home) plus `workers.size`
  worker pods on a shared headless-Service network, for distributed training.
- **PD** — engine + decoder (+ router) from the runtime; each is its own component
  on the engine, started/stopped together. A dev box does **not** need the
  zero-downtime cross-Component rollout coordination production serving uses (it is
  opt-in, only meaningful for a live rollout), so we leave it off.

A reader will object: "OEP-0007 says multi-node serving uses a `LeaderWorkerSet` —
doesn't a Workstation drag in the LWS CRD?" No — and the distinction is load-
bearing. LWS is chosen by the **InferenceService dispatch path** for its MultiNode
mode; the **workload engine itself does multi-pod natively** as one Instance with
leader+worker Runners (`workload/plan.go:79-87`). A Workstation drives the engine
*directly*, so multi-node (and PD's per-component fan-out) ride the engine with **no
LWS object and no LWS CRD dependency** in either case.

### Status and connection

`status` is plain `metav1.Condition` + a `phase` enum, modeled on OME's OMENative
lifecycle types (`lifecycle_types.go`), **not** the knative `duckv1.Status` an ISVC
uses — a dev box has no URL, address, or traffic, so the lighter precedent fits.

`status.phase` ∈ {Pending, Provisioning, Running, Stopping, Stopped, Failed};
conditions carry the detail (StorageReady, Scheduled, Ready, WorkersReady, Stopped,
Failed). `status.connection` advertises **exec-only** (`namespace`/`podName`/
`container`); `status.leaderPod` is the attach target (the single pod, the
multi-node leader, or — for PD — the engine pod); `status.components[]` reports each
component's pods (one `default` entry for a plain box; engine/decoder/router for
PD). This is deliberately enough for the console (#2) and the DevPod driver (#3) to
consume without further controller surface.

### Validation inventory

Webhook (validating + defaulting):

- **At least one of `spec.runtime` or `spec.image`** must be set — else reject (the
  controller has nothing to build a container from).
- **`spec.workers.size >= 1`** when `workers` is set (omit `workers` for single-node).
- **`spec.storage` has exactly one of `claimName` or `size`** — reject both/neither.
- **`spec.idleTimeout >= 1m`** when set, to avoid stop/start thrash.
- Defaulting: `desiredState=Running`, `storage.accessMode=ReadWriteOnce`,
  `storage.mountPath=/home/dev`, keep-alive command when `command`/`args` are empty.

## Test Plan

- **Unit:** spec→pod-shape resolution (inline single-node, inline multi-node,
  runtime single/multi/PD, runtime+inline override, model vs no-model); webhook
  accept/reject matrix above; status projection from a mocked engine Instance.
- **envtest:** create → Provisioning → Running phase walk; Stop deletes pods and
  retains the PVC; Start resumes onto the same PVC; Delete honors
  `retainStorageOnDelete`; `claimName` PVC is mounted and never deleted.
- **KIND** (real kubelet — where the OMENative suites live): the Stop
  hand-off (engine does not recreate the pod after Stop; box reads `Stopped`);
  multi-node leader+worker convergence on the engine (no LWS object created);
  PD three-component bring-up; unschedulable→Failed deadline; in-place image update
  preserving the home.
- **Regen gate:** `make manifests && make generate`; `dep-crds`/manifest drift and
  the spec-gen → Python-SDK/API-reference chain.

## Graduation Criteria

- **Alpha:** CRD + controller for single-node and multi-node; Stop/Start/Delete with
  PVC retention; runtime + inline + model-optional; exec status; unschedulable
  deadline. PD behind the same path, KIND-verified.
- **Beta:** the Stop hand-off and PD bring-up proven on the KIND suite at scale;
  idle-stop with a real activity signal (see Open Questions); the DevPod driver (#3)
  consuming the status unchanged.
- **Stable:** console (#2) in production use; auth/tenancy sub-project landed.

## Drawbacks

- **A new CRD + controller** to maintain. Mitigated by reuse: the controller is an
  adapter over the workload engine (the `InferenceReplica` controller is the
  template), not a fresh lifecycle implementation.
- **Stop bypasses the engine** (controller deletes pods directly). This is a second
  pod-deletion path beside the engine's drain; it must not race the engine, which
  is why Stop also stops *driving* the engine. Flagged as the top open question.
- **Develop-on-model requires pre-staged weights** — no on-demand fetch. A model
  selector for an un-staged model strands the pod. Mitigated by the unschedulable
  deadline surfacing it instead of hanging.
- **PD makes a Workstation multi-component**, reopening (a little of) what a single-
  component box avoided: per-component status aggregation and the router's pod-
  discovery wiring (Open Questions).

## Alternatives

1. **A "dev mode" flag on `InferenceService`** (`spec.mode: Develop`) — maximum
   reuse, zero new CRD. Rejected: it overloads ISVC (a serving/traffic object) with
   dev affordances and forces webhook/status special-cases throughout — the exact
   "ignore this when it's actually a dev box" sprawl a dedicated, smaller resource
   avoids. OME already separates concerns into distinct kinds; a Workstation fits
   that grain.
2. **`engine`/`decoder`/`router` blocks on the Workstation** (full inline PD) — full
   symmetry with ISVC, inline PD with no runtime. Rejected for v1: it duplicates the
   entire component surface on a second CRD, and defining PD inline *is* writing a
   `ServingRuntime` by hand — which is reusable and matches production, so PD belongs
   in a runtime. Shorthand + runtime keeps the API small; the cost is no inline-PD
   override (acceptable — PD users have a runtime).
3. **A dedicated `WorkstationStorage`/`Workspace` CRD** for the home. Rejected as a
   thin wrapper: a PVC is *already* a first-class, independently-lifecycled object,
   so `claimName` gives durable, reusable, detach/reattach storage for free. A
   storage CRD only earns its keep with OME-specific semantics a bare PVC cannot
   express — snapshot/restore, seeding a home from a git repo or image, per-user
   quota, or a console object for "my workspaces." That is a later step, not v1.
4. **Reuse the ISVC `omenative` reconciler layer instead of the workload engine** —
   rejected: that layer is ISVC-typed (its params carry `*v1beta1.InferenceService`
   and it writes ISVC-shaped status), so reuse would mean threading a second CRD
   through ISVC code. The workload engine is already source-agnostic; the
   `InferenceReplica` controller proves a non-ISVC caller works.
5. **Status quo** — keep hand-rolling dev pods. Rejected: drift from the serving
   topology, no stop/keep-disk, lifecycle re-implemented per team (the Motivation).

## Open Questions

1. **The Stop hand-off needs a prototype.** "Controller deletes pods, stops driving
   the engine" is sound in principle but unbuilt; the thing to verify on KIND is
   that the engine's create bookkeeping (Expectations) does not recreate the pod the
   instant the box is driven again, and that a stopped box reads cleanly as
   `Stopped`. *Owner: TBD — build first.*
2. **PD router pod-discovery.** The stock PD router finds prefill/decode pods by a
   label selector keyed (in a runtime) on the InferenceService name. A Workstation
   has none, so the controller must stamp the engine/decoder pods with the labels
   the router expects and inject the matching selector/env from the Workstation's
   own identity, so the same runtime's router image works unchanged. Mechanically
   small; the gating piece for PD routing — prototype early.
3. **Unschedulable deadline default.** A few minutes? Should "no GPU yet" be `Failed`
   or a long-lived "waiting for capacity" the user can leave running? *Recommended:
   surface a condition immediately, escalate to Failed after a configurable
   deadline (default TBD).*
4. **Idle "activity" for v1.** Heartbeat from clients (console/DevPod refresh a
   `lastActivity` timestamp; bare-`kubectl exec` users won't be detected) versus
   GPU-utilization from day one (adds a metrics dependency). *Recommended:
   heartbeat for v1; the field is forward-compatible with utilization later.*
5. **Storage default on Delete.** Reclaim the disk by default (cheaper; a fat-
   fingered delete loses work) or retain by default (safer; quietly costs money)?
   Stop always keeps it; this governs Delete only.
6. **Accelerator selection.** v1 uses the raw `nodeSelector` escape hatch — OME has
   no generic GPU-class→node-label catalog (node labeling is model-placement-driven
   and short-circuits without a model). Add a friendly `accelerator` class knob
   later, or build the catalog first? *Recommended: raw path for v1.*

## Implementation History

- **This OEP (provisional, 2026-06-04)** — proposes the `Workstation` CRD,
  controller-over-workload-engine, shape-from-runtime, model-optional, and
  create-or-reference storage.
