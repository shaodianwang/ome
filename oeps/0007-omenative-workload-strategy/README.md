# OEP-0007: OMENative Workload Strategy

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Why a New Workload Type](#why-a-new-workload-type)
  - [Why Not LWS or RBG](#why-not-lws-or-rbg)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Core Concepts](#core-concepts)
  - [How Operators Opt In](#how-operators-opt-in)
  - [User Stories](#user-stories)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Architecture Overview](#architecture-overview)
  - [Pod Topology](#pod-topology)
  - [Restart Policy](#restart-policy)
  - [Update Strategy](#update-strategy)
  - [Service Discovery](#service-discovery)
  - [Readiness and Warmup](#readiness-and-warmup)
  - [Port Allocation](#port-allocation)
  - [Migration](#migration)
  - [Strategy Migration from LWS](#strategy-migration-from-lws)
  - [Model Availability](#model-availability)
  - [Gang Scheduling](#gang-scheduling)
  - [Webhook Interaction](#webhook-interaction)
  - [Status Propagation](#status-propagation)
  - [Coordinated Rollout](#coordinated-rollout)
  - [RBAC](#rbac)
  - [Observability](#observability)
- [Coexistence with LWS-Backed Modes](#coexistence-with-lws-backed-modes)
- [Feature Gating and Rollout](#feature-gating-and-rollout)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Future Work](#future-work)
- [Open Issues](#open-issues)
- [Open Questions](#open-questions)
- [Implementation History](#implementation-history)
<!-- /toc -->

## Summary

OMENative is a workload strategy for OME's `InferenceService` that
manages inference pods directly using native Kubernetes primitives —
`Pod`, `Service`, `PodDisruptionBudget`, `ControllerRevision`, and an
optional `PodGroup` from scheduler-plugins — without depending on
`sigs.k8s.io/lws` (LeaderWorkerSet) or `sigs.k8s.io/rbgs`
(RoleBasedGroup).

The strategy promises three things the existing LWS-backed paths
cannot deliver:

1. **An Instance is the atomic unit of identity.** Every replica
   (called an Instance) has a stable name and survives pod recreation.
   Multi-pod Instances come up as a gang and die as a gang. The
   controller never has to reason about partial groups.
2. **In-place updates preserve model weights.** Image-only changes
   restart the container without recreating the pod, so EmptyDir-backed
   model weights survive. Operators avoid 10-minute reload cycles when
   bumping a runtime image.
3. **Migration is a first-class verb.** External controllers (notably
   the Alfred defragmenter, OEP-0008) request migration via an
   annotation; OMENative surges a replacement Instance, waits for it
   to be ready, drains the old one, and deletes it. Service capacity
   never drops below the Component minimum.

OMENative introduces **no new user-visible CRDs**. `InferenceService`
remains the only workload-facing API. Every internal concept —
Instance, Runner, migration request, coordination policy — surfaces on
the existing `InferenceService` spec/status or materializes as
standard Kubernetes resources operators already recognize.

OMENative coexists with the existing LWS-backed `MultiNode` and
`PDDisaggregated` modes through a six-release deprecation window.
Users opt in per-Component at their own pace. Eventually the LWS-backed
paths are removed and `sigs.k8s.io/lws` leaves OME's module graph.

`RawDeployment` remains the default for simple single-pod components.
OMENative is the successor to the LWS-backed multi-pod paths, and an
opt-in for single-pod workloads only when Alfred needs OMENative's
migration and drain lifecycle.

A related design, [OEP-0008 (Alfred GPU
Defragmenter)](../0008-alfred-gpu-defragmenter/README.md), depends on
the migration verb. The two designs ship independently.
Cross-Component rollout coordination is the topic of
[OEP-0007.1](../0007.1-omenative-coordinated-rollout/README.md),
landing as an additive layer once the single-Component kernel is
proven.

## Motivation

### Why a New Workload Type

LWS's restart semantics drop traffic when a single pod is lost. OME
configures every LWS with `RestartPolicy=RecreateGroupOnPodRestart`,
which deletes the leader pod with foreground propagation whenever any
group member dies. The leader deletion cascades through the worker
StatefulSet: the group goes to zero replicas before coming back. LWS's
`RollingUpdate` with `MaxSurge=1, MaxUnavailable=1` only fires on
spec change, not pod deletion — an eviction flows through restart
handling, not the rollout controller, so no surge replica is ever
created during eviction.

The consequence for defragmentation: a defragmenter wanting to
migrate a workload by evicting a pod sees the entire group torn down
with no surge. The scheduler attempts to place `N` replacement pods
simultaneously; if any fails, the LWS replica is not Ready and the
service is down; no rollback is possible because the evicted pods and
their GPU allocations are gone.

LWS 0.7 and 0.8 do not change this. Their gang-scheduling work
(KEP#407) addresses initial placement, not post-loss restart.

LLM inference also needs primitives general-purpose workload
controllers don't prioritize: in-place image updates that preserve
model weights, migration with explicit surge semantics and model
availability preconditions, and (in OEP-0007.1) cross-Component
rollout coordination for prefill-decode topologies.

### Why Not LWS or RBG

`sigs.k8s.io/rbgs` (RoleBasedGroup) is a sibling project in the
sgl-project org that addresses many of the same concerns. RBG's
KEP-30 (InstanceSet) explicitly proposes replacing LWS inside RBG for
the same dependency-drift reasons cited here.

Adopting RBG directly in OME would, however, install four user-visible
CRDs between `InferenceService` and a `Pod`:

- `RoleBasedGroup` (`workloads.sgl.ai/v1alpha2`)
- `RoleInstanceSet` (`workloads.sgl.ai/v1alpha2`)
- `RoleInstance` / `Instance` (`workloads.sgl.ai/v1alpha1|v1alpha2`)
- `CoordinatedPolicy`, `RoleBasedGroupScalingAdapter`

Each layer is observable to operators via `kubectl api-resources` and
`kubectl describe`. For an operator debugging a failing workload the
cognitive load grows quickly.

OMENative borrows RBG's **design patterns** — Instance-as-atomic-unit,
predeclared readiness gates, revision history, gang adapters — while
emitting **only native Kubernetes resources**. Users see
`InferenceService` plus vanilla `Pod`, `Service`,
`PodDisruptionBudget`, `ControllerRevision`, and the optional
`PodGroup`.

LWS and RBG also evolve on schedules independent of OME's release
cadence. OME is already three minor versions behind LWS upstream;
keeping current would mean continuously validating new features,
integrating controller changes, and coordinating CRD upgrades in user
clusters. OMENative replaces that ongoing cost with one fixed
implementation cost.

### Goals

1. **Replace LWS** as the underlying primitive for OME's multi-node
   and prefill-decode paths. Users opt in per-Component; eventually
   LWS-backed paths are removed and `sigs.k8s.io/lws` is dropped from
   `go.mod`.
2. **Zero new user-visible CRDs.** `InferenceService` remains the
   sole workload API.
3. **Unified handling** of single-pod, single-Component multi-node,
   and multi-Component prefill-decode workloads under one strategy
   implementation.
4. **A migration verb** on `InferenceService`, surge-only in v1.
5. **In-place updates by default.** Image and metadata changes
   preserve EmptyDir-backed model state and avoid full model-weight
   reload when the runtime tolerates a container restart.
6. **A narrow, implementable controller kernel.** Single-Component
   correctness ships first; richer cross-Component coordination layers
   on top (OEP-0007.1).
7. **Coexist with LWS-backed modes** during a six-release deprecation
   window. No breaking changes; users migrate at their own pace.

### Non-Goals

1. Not a general-purpose workload controller. OMENative is
   specialized for LLM inference.
2. Not a scheduler. OMENative supplies hints and gates; the
   Kubernetes scheduler still places pods.
3. Not shipping an OMENative-managed HPA in v1. Existing modes keep
   their scaling paths.
4. Not owning cluster-wide defragmentation decisions. That is
   Alfred's job (OEP-0008).
5. Not solving GPU health observation.
6. Not exposing any new CRDs.
7. Not supporting cross-cluster `InferenceService`.
8. Not shipping a user-facing rollback verb in v1. Internal
   `ControllerRevision`s are emitted; the rollback API is deferred to
   a follow-up.

## Proposal

### Core Concepts

The following concepts live inside OMENative's controller or on
existing `InferenceService` fields. None are expressed as CRDs.

- **Component** (existing OME term). A top-level role within an
  `InferenceService`: `router`, `engine`, or `decoder`.
- **Instance** (new). One replica of a Component — the atomic unit
  for gang scheduling, restart policy, migration, and readiness
  aggregation. A Component with `MinReplicas=3` has three Instances,
  indexed 0, 1, 2.
- **Runner** (extends existing OME concept). A kind of pod within an
  Instance. Named `leader`, `worker`, or `default` (for single-pod
  Instances). A Runner has a size (pod count) and a template.
- **Pod.** Standard Kubernetes pod running a Runner's container spec.

Example — a prefill-decode `InferenceService` `llama-70b` with router
(2 replicas), engine (2 Instances of 4 pods each), decoder (1
Instance of 2 pods):

```
InferenceService llama-70b
├── Component router (2 Instances)
│   ├── Instance 0: 1 default Runner (size 1)
│   └── Instance 1: 1 default Runner (size 1)
├── Component engine (2 Instances)
│   ├── Instance 0: 1 leader Runner + 1 worker Runner (size 3)
│   └── Instance 1: same shape
└── Component decoder (1 Instance)
    └── Instance 0: 1 leader Runner + 1 worker Runner (size 1)

Total: 5 Instances, 12 Pods
```

### How Operators Opt In

Operators select OMENative per-Component via the existing
`ome.io/deploymentMode` annotation on each Component's metadata:

```yaml
spec:
  router:
    metadata:
      annotations:
        ome.io/deploymentMode: RawDeployment
  engine:
    metadata:
      annotations:
        ome.io/deploymentMode: OMENative
  decoder:
    metadata:
      annotations:
        ome.io/deploymentMode: OMENative
```

OMENative reuses the existing per-Component annotation rather than
introducing a typed `deploymentMode` field on EngineSpec / DecoderSpec
/ RouterSpec. The annotation already works today: `DetermineEngineDeploymentMode`
reads `engine.Annotations` first and falls back to inference. Adding
`OMENative` is a one-line change to the resolution table. Promoting
the annotation to a typed field is an additive CRD change that can
land at beta or GA once the surface has stabilized; going the other
way would be a breaking change.

Engine and decoder deployment modes **must match** when both are
present — they are coupled through coordinated rollout and shared
migration semantics. Router's mode is independent (it is stateless,
CPU-only, decoupled via Service discovery).

OMENative does not ship behind a Kubernetes feature gate. The trade-off
is no single cluster-wide kill switch; the only rollback during alpha
is annotation removal or controller revert. A real feature gate may
be introduced at beta or GA based on operational experience.

### User Stories

**Story 1 — Upgrade to multi-node tensor parallelism.** An operator
has a 7B model on `RawDeployment` and wants to move to a 70B model
needing tensor parallelism across two nodes (1 leader, 1 worker).
They add `leader` and `worker` sections under `spec.engine` with
`worker.size: 1`, plus `ome.io/deploymentMode: OMENative` on
`spec.engine.metadata.annotations`. OMENative creates a headless
Service, two pods (`-engine-0-leader-0`, `-engine-0-worker-0`), and
one PodGroup. Pods come up; runtime readiness and the controller-owned
serving gate make the Instance Ready only when both pods are Ready.
`kubectl get pods,services,podgroups` shows everything — no LWS CRD
vocabulary to learn.

**Story 2 — Alfred migrates an Instance for defragmentation.** Alfred
sees that engine Instance 0 sits alone on Node1, contributing to GPU
fragmentation. It writes a migration-request annotation on the
InferenceService with `{component: engine, instance: 0, from_node:
node1, hint_target_nodes: [node3, node7]}`. OMENative reads the
annotation, clears it within 30 seconds, creates a surge Instance
with a fresh index whose affinity excludes Node1 and prefers Node3
and Node7, waits for it to be Ready, drains the old Instance, and
deletes it. The other Instance handles traffic throughout; the
Component never drops below its minimum availability.

**Story 3 — In-place runtime upgrade.** An operator bumps a
`ServingRuntime` image tag for a prefill-decode workload.
`InferenceService` generation increments. OMENative compares old and
new `ControllerRevision`s per Component:

- Router: only the image changed; in-place eligible. Pods get the
  serving gate flipped False (traffic drains after endpoint
  convergence), the image is patched, kubelet restarts the container,
  the pod returns to Ready. Router has no model, so reload is zero.
- Engine, decoder: same. The 70B model weights sit in EmptyDir and
  survive the container restart. Warmup is seconds rather than ten
  minutes of re-download.

Each Component rolls independently in v1. OEP-0007.1 covers
cross-Component coordination.

**Story 4 — Node evacuation.** An operator drains Node5 for a GPU
driver update. For each `InferenceService` with an Instance on Node5,
they call Alfred's evacuate endpoint (or write migration-request
annotations directly). OMENative executes surge migration. Simple
`RawDeployment` workloads continue to be evicted as before.

**Story 5 — Single-pod engine.** An operator runs an engine-only
`InferenceService` where one pod is the entire serving unit. The
default remains `RawDeployment`, because Alfred can safely evict and
reschedule that pod without group semantics. The operator opts the
Component into OMENative explicitly only if they later need
controller-owned drain or surge migration.

### Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Reimplementing LWS-equivalent semantics introduces bugs LWS already fixed | Greenfield implementation informed by LWS and RBG source — patterns studied, not code-copied. Beta graduation gates on chaos tests covering known group-lifecycle edge cases. |
| Divergence from the sgl-project ecosystem (RBG) | Accepted trade-off. RBG's CRD surface is a non-starter for OME's UX. Shared patterns (not code) are sufficient; a Go helper library can be extracted later if it emerges. |
| Feature gaps versus LWS | Per-feature classification: subgroup policies out of scope; exclusive placement via standard `topologySpreadConstraints`; partition rollouts adopted; mutable `size` out of scope. Coexistence with LWS is long enough for users needing unsupported features. |
| Migration-induced service disruption | Cluster-wide rate limits (default 3 in-flight, 10/hour). Every migration emits events and status updates. `spec.<component>.lifecycle.migrationPolicy.mode: never` opts specific workloads out. Alfred has its own rate limiting. |
| Annotation-based migration-request surface is fragile | Annotation is idempotent via UUID. Lifecycle semantics (30s clear, 5m retry, 1h stale) are precise. OMENative's clear is best-effort; Alfred's retry handles contention. A subresource shim is straightforward if operator UX demands one later. |
| Operators confuse OMENative errors with native Kubernetes errors | Owner references flow from `InferenceService` to pods. OMENative emits its own event stream on the `InferenceService` summarizing lower-level events. Errors clearly distinguish OMENative-issued from native. |
| Engine/decoder coupling rule surprises users | Admission webhook rejects mismatched `ome.io/deploymentMode` between engine and decoder with `InvalidDeploymentModeCombination`. Users see the rejection at `kubectl apply` time. |

## Design Details

### Architecture Overview

OMENative is not a parallel controller. It plugs into the existing
`InferenceService` controller as a new deployment backend. The
addition is:

- a new `DeploymentModeType` value, `OMENative`
- a new backend package
  `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/`
- one new `case constants.OMENative` branch in
  `components/engine.go`, `decoder.go`, and `router.go`
- additional watches in `SetupWithManager()` for `Pod`,
  `ControllerRevision`, `EndpointSlice`, and the optional `PodGroup`

OMENative reuses OME's existing shared reconcilers for ClusterIP and
external Services, `PodDisruptionBudget`, RBAC, the model ConfigMap,
ingress and gateway integration, and the top-level
`InferenceService.Status` aggregation.

OMENative does not use the current HPA reconciler in v1. That code
targets one `Deployment`; OMENative manages Instance-scoped direct
pod sets.

Detailed reconcile-loop mechanics, file layout, and watch mapping
live in [controller-mechanics.md](./controller-mechanics.md). This
README stays at the design-contract level.

The plan computed each reconcile from `InferenceService` +
`ServingRuntime` + `BaseModel` is in-memory only — never persisted,
never a CRD. Persistent state lives on `InferenceService.Status`
(top-level conditions, per-Component status, per-Instance status,
migration history), the emitted Kubernetes objects, and
`ControllerRevision` objects per Component.

### Pod Topology

OMENative maps each `(Component, Instance, Runner, ordinal)` tuple to
exactly one directly managed pod. Names are derived deterministically:

```
Pod name:   <isvc>-<component>-<instance-idx>-<runner-name>-<ordinal>
Hostname:   same as pod name
Subdomain:  <isvc>-<component>-headless
```

For the prefill-decode workload above:

```
llama-70b-router-0-default-0
llama-70b-router-1-default-0
llama-70b-engine-0-leader-0
llama-70b-engine-0-worker-0
llama-70b-engine-0-worker-1
llama-70b-engine-0-worker-2
llama-70b-engine-1-leader-0
...
llama-70b-decoder-0-leader-0
llama-70b-decoder-0-worker-0
```

Twelve pods total. The user sees `InferenceService` plus vanilla
Kubernetes objects.

**Why direct pods, not one StatefulSet per `(Instance, Runner)`:** v1
surge migration needs one full replacement Instance without relying
on StatefulSet naming tricks or unsupported `MaxSurge`. OMENative owns
Instance atomicity directly, so the full pod set is created, drained,
and deleted together. Stable pod names plus explicit owner references
keep debugging and garbage collection straightforward. Headless-Service
DNS still works because OMENative sets `hostname`/`subdomain`
directly on each pod.

Every OMENative-managed pod carries common labels for the pod watcher,
status aggregation, and Alfred observation:

```
ome.io/inferenceservice:   <isvc>
ome.io/component:          <router|engine|decoder>
ome.io/instance-index:     <idx>
ome.io/runner:             <leader|worker|default>
ome.io/managed-by:         OMENative
```

**Single-pod Instances** still use one Runner named `default`. For
simple single-pod workloads, `RawDeployment` remains the default mode;
single-pod OMENative is opt-in only when Alfred needs controller-owned
drain or surge migration.

**Multi-pod Instances.** When the rendered Component has both leader
and worker sections, the planner emits the canonical multi-pod layout:

```
Runners: [
  {Name: "leader", Size: 1},
  {Name: "worker", Size: <WorkerSize>},
]
```

Pod naming follows the same scheme
(`<isvc>-<component>-<inst>-leader-0`,
`<isvc>-<component>-<inst>-worker-0..N-1`). Pods carry the
`ome.io/runner` label set accordingly, and a single shared subdomain
per Component so any pod's FQDN resolves under the Component's headless
Service. The revision-hash computation folds in the worker `PodSpec`
so worker-only changes correctly trigger a new `ControllerRevision`.

### Restart Policy

A typed enum nested under each Component's `lifecycle` sub-block:

```yaml
spec:
  engine:
    lifecycle:
      restartPolicy: RecreateInstanceOnPodRestart
```

OMENative spec fields nest under `lifecycle` because `EngineSpec` /
`DecoderSpec` / `RouterSpec` already inline `corev1.PodSpec` via
`json:",inline"`, exposing the kubelet-facing `restartPolicy`
(`Always`/`OnFailure`/`Never`) at the same level. Adding the
Instance-group restart policy as a flat sibling would shadow that
field. The two policies have different scopes (container restart vs.
Instance-group restart) and legitimately coexist on the same spec, so
all OMENative-specific fields live under a single sub-block. The
status side keeps its historical `omenative` key
(`status.components.<component>.omenative.{...}`) so existing status
consumers don't break; only the spec side is renamed.

Three values:

- **`None`** — a pod failure restarts only that pod.
- **`RecreateInstanceOnPodRestart`** — if any pod in an Instance
  fails, all pods in that Instance are drained, deleted, and
  recreated. Default for multi-pod Instances; the OMENative analog of
  LWS's `RecreateGroupOnPodRestart`, scoped to the Instance.

Defaults: `RecreateInstanceOnPodRestart` when the Component has any
multi-pod Runner; `None` when all Runners have `size == 1`.

The pod watcher watches pods with `ome.io/managed-by: OMENative`. On
a pod failure it reads the Component's restart policy and applies the
configured cascade (none, Instance-wide, or `InferenceService`-wide).
During an in-flight migration the watcher suppresses Instance
recreation for the migrating Instance; the migrator owns pod
lifecycle in that window.

### Update Strategy

Per-Component field nested under `lifecycle`. Default
`InPlaceIfPossible`.

```yaml
spec:
  engine:
    lifecycle:
      updateStrategy:
        type: InPlaceIfPossible
        rollingUpdate:
          partition: 0
          maxUnavailable: 1
        inPlaceUpdateStrategy:
          gracePeriodSeconds: 30
          markNotReadyDuringLifecycle: true
```

Update strategy is about operator risk appetite — what to do when a
spec change requires bringing pods down:

| Strategy | When to use |
|---|---|
| `RecreatePod` | Cheap, brief outage. Use when in-place is untrusted. |
| `InPlaceIfPossible` | Preserve sessions when safe; fall back to recreate if the change isn't in-place-capable. Default. |
| `InPlaceOnly` | Sessions are precious; prefer to fail the update than break them. Safety net. |
| `SurgeThenDrain` | Always seamless, at the cost of an extra pod's worth of capacity during the window. Default for single-pod Components. |

**SurgeThenDrain.** Each single-pod Instance carries an
`ActiveOrdinal` field on its status (`0` or `1`) identifying which of
two naming slots holds the canonical pod. The update walks three
phases:

1. **Surge** — render a pod at the alternate slot (`1 - ActiveOrdinal`)
   with the new revision; create it.
2. **Drain** — once the surge pod is Ready, flip the old pod's
   serving gate to False and wait for the per-revision routed
   Service's EndpointSlice to drop it.
3. **Promote** — delete the old pod; advance `ActiveOrdinal` to point
   at the surge slot.

`InPlace` and `RecreatePod` leave `ActiveOrdinal` at `0`; they reuse
the same pod name. The pod-name computation for single-pod Runners
consults `ActiveOrdinal`, so the dispatcher does not resurrect a pod
at the drained slot.

Status counters classify an Instance as Serving / Ready / Available
based on `observed >= desired` rather than equality, because
SurgeThenDrain transiently runs one extra pod and the equality check
wrongly marked mid-surge Instances as not-Serving.

The status writer mirrors every successful per-Instance commit back
onto the caller's in-memory `InferenceService` snapshot. The
dispatcher drives multiple per-Instance operations off one snapshot
per reconcile pass; without the mirror, the second operation reads
pre-write state for fields the first just mutated. The same pattern
applies for any per-Instance status writer.

**Multi-pod fallback.** Multi-pod Instances under `SurgeThenDrain`,
`InPlaceIfPossible`, or `InPlaceOnly` currently route to recreate.
Per-gang-member ordinal allocation for multi-pod surge is deferred
(see [Future Work](#future-work)). A one-time `MultiPodSurgeFallback`
event is logged so operators see the gap between intent and behavior.

**Revision-hash filtering.** Lifecycle annotations the controller
mutates do not enter the `ControllerRevision` spec hash, so toggling
them does not produce a phantom rollout. The current filter covers
`ome.io/rollout-paused` and the `ome.io/migration-request-v1-*`
prefix. The filter list lives in `revision/revision.go::isLifecycleAnnotation`;
extend it when adding new controller-mutated annotations.

**In-place-capable fields** — the template fields OMENative treats as
in-place eligible:

- `spec.containers[].image`
- `spec.containers[].imagePullPolicy`
- `metadata.labels`, `metadata.annotations`

**Not in-place-capable** — forces recreate under `InPlaceIfPossible`,
rejects under `InPlaceOnly`:

- `spec.containers[].command`, `args`, `env`, `envFrom`,
  `volumeMounts`, `volumeDevices`, `ports`, `securityContext`,
  `lifecycle`
- `spec.initContainers[]` (any change)
- `spec.volumes[]`
- `spec.nodeSelector`, `spec.affinity`, `spec.tolerations`
- `spec.serviceAccountName`, `spec.imagePullSecrets`
- `spec.runtimeClassName`, `spec.schedulerName`

Any future field that would change pod identity or storage wiring
falls in the second list by default.

Decision logic, in pseudocode:

```
func decideUpdate(oldRev, newRev, policy):
    if policy.Type == RecreatePod:
        return Recreate
    diffs := compareTemplate(oldRev, newRev)
    inPlaceEligible := allDiffsIn(diffs, inPlaceCapableFields)
    if policy.Type == InPlaceIfPossible:
        return InPlace if inPlaceEligible else Recreate
    if policy.Type == InPlaceOnly:
        return InPlace if inPlaceEligible else Reject
```

In-place execution on an Instance: emit an `InPlaceUpdateStarted`
event; if `markNotReadyDuringLifecycle` is true, flip the serving
gate to False and wait for EndpointSlice convergence; patch the
container image (and eligible metadata); kubelet restarts the
container in place and the pod re-runs its readiness probe; flip the
gate back to True and wait for ready endpoints; emit
`InPlaceUpdateCompleted`. The serving gate is added at pod creation
time, never mutated on a running pod's `readinessGates` list.

Rollout pacing across Instances respects
`rollingUpdate.partition` and `rollingUpdate.maxUnavailable`.
Migration (movement between nodes) always triggers recreate because
node-affinity change is not in-place capable, regardless of
`updateStrategy.type`.

**minReadySeconds.** `lifecycle.minReadySeconds` separates the health
signal from the pacing signal. A pod is *Ready* when kubelet reports
`PodReady=True`; it is *Available* when it is Ready and the `PodReady`
condition's `lastTransitionTime` plus `minReadySeconds` is not after
the current time, evaluated per pod — the same rule as
`Deployment.spec.minReadySeconds`. Unset or `0` makes Available equal
to Ready, which is the behavior every strategy has without the field.
The ISVC controller projects the value onto
`InferenceReplica.spec.minReadySeconds`; the workload engine reads it
from the plan and applies it at the point where each strategy would
otherwise release its budget slot:

| Strategy | Where Available is required |
|---|---|
| `SurgeThenDrain` (single-pod) | After the surge pod is `PodReady` and before the old pod is marked not-serving. The surge slot stays in flight, so `maxSurge` holds for the whole window. |
| `SurgeThenDrain` (gang) | Same point: after every replacement-gang pod is `PodReady`, before the source gang leaves rotation. |
| `RecreatePod` | At promotion (`Phase=Ready`, `Operation` cleared), so the `maxUnavailable` slot is held until the recreated pods have been Ready for the window. |
| `InPlaceIfPossible` / `InPlaceOnly` | At promotion, after the patched pod is back in rotation. A pod already relabeled to the target revision is not drained again while promotion waits, so the window is not restarted every pass. |

The engine polls at its in-flight Update cadence while a window is
open; the wait resolves within one poll interval of the deadline.
`status.availableReplicas` and each Instance's `availablePodCount`
count a pod only when it is in rotation on the Component's headless
Service **and** Available under this rule, so a migration that waits
on the surge Instance's `availablePodCount` inherits the window.
Ready-based signals stay on Ready: `readyReplicas`, traffic weights,
the coordination phase machine, and the Sequential handoff (which
reads the previous Component's `Ready` condition; that condition
flips only after every Instance promotes, so it observes the window
through promotion rather than through a separate check).

Clusters may stamp a default through the `inferenceservice-config`
ConfigMap (`deploy.minReadySeconds`): the ISVC defaulter copies it
onto every OMENative Component whose lifecycle leaves the field unset.
There is no in-code default; an authored value always wins.

A retention limit (default 10 revisions per Component) caps
`ControllerRevision` object count.

### Service Discovery

Three Services per Component, each with a distinct name and purpose:

| Service | Name | Type | Purpose |
|---|---|---|---|
| External | `<isvc>` | ClusterIP / LoadBalancer | Top-level external entry |
| Component routing | `<isvc>-<component>` | ClusterIP | Component-level entry |
| Peer DNS | `<isvc>-<component>-headless` | Headless (`ClusterIP: None`) | Stable pod subdomain for peer DNS |

The external and component-routing Services are the same Services
OME's controllers create today, so ingress and VirtualService
consumers keep working. OMENative also reconciles its own per-Component
stable Service, the ingress factory branch, and the PodMonitor — at
parity with `RawDeployment` and `MultiNode`.

The peer-DNS headless Service is new for OMENative. OMENative sets
each pod's `hostname` and `subdomain` directly, giving each pod a
stable DNS name:

```
<pod-name>.<isvc>-<component>-headless.<ns>.svc.<cluster-domain>
```

For example:

```
llama-70b-engine-0-worker-1.llama-70b-engine-headless.prod.svc.cluster.local
```

The headless Service has `PublishNotReadyAddresses=true` so peer DNS
resolves before pods are Ready. As a side effect, drain checks must
**not** use this Service — it would report a pod as ready forever.
Drain checks instead consult the per-revision routed Service created
by the coordination layer (`<isvc>-<component>-rev-<hash>`, with
`PublishNotReadyAddresses=false`), which correctly reflects the
controller-owned serving gate. When a pod has no `ome.io/revision-hash`
label (a pre-revision pod, rare), the drain gate is skipped and the
controller proceeds straight to delete.

**Pod-level environment variables** injected into every
OMENative-managed pod at pod-template render time:

| Variable | Example | Meaning |
|---|---|---|
| `OME_INFERENCESERVICE_NAME` | `llama-70b` | parent `InferenceService` |
| `OME_COMPONENT` | `engine` | router / engine / decoder |
| `OME_COMPONENT_REPLICAS` | `2` | total Instances of this Component |
| `OME_INSTANCE_INDEX` | `0` | this Instance's ordinal |
| `OME_RUNNER` | `worker` | leader / worker / default |
| `OME_RUNNER_SIZE` | `3` | pod count in this Runner |
| `OME_RUNNER_INDEX` | `1` | ordinal within this Runner |
| `OME_LEADER_ADDRESS` | `llama-70b-engine-0-leader-0.llama-70b-engine-headless` | DNS name of this Instance's leader pod (multi-pod Instances only) |
| `OME_INSTANCE_SUBDOMAIN` | `llama-70b-engine-0` | prefix for building peer DNS |

Runtime-declared env via `RunnerSpec.Env` is merged with these.

`OME_LEADER_ADDRESS` ships in **short form** — without the trailing
`<ns>.svc.<cluster-domain>` suffix. The controller has no reliable way
to discover the cluster's actual DNS suffix; the Kubernetes search-path
resolution makes the short form work everywhere `getaddrinfo` semantics
are used. The variable is set only on multi-pod Instances (skipped
when the Runner is `default`); the leader pod sees its own DNS name
so the leader code path can self-identify.

A controller-manager flag (`--cluster-domain`) for runtimes needing
strict-FQDN lookups (NCCL, Ray) is plausible if a real-world runtime
trips on the short form; see [Open Questions](#open-questions).

### Readiness and Warmup

Per-Component fields nested under `omenative`:

```yaml
spec:
  engine:
    lifecycle:
      readyPolicy: AllPodReady       # AllPodReady | None
      instanceReadyTimeout: 30m
```

The inference runtime's readiness probe (declared in the
ServingRuntime) is authoritative. When it passes, the pod's `Ready`
condition is true. OMENative observes via `Pod.Status.Conditions`.

`readyPolicy: AllPodReady` (default for multi-pod Instances) makes an
Instance Ready only when every pod in the Instance has `Ready=True`.
`readyPolicy: None` reports pods individually with no Instance-level
aggregation.

`instanceReadyTimeout` (default 30 minutes; large enough for big-model
loading) bounds OMENative's wait on a newly-created Instance during
migration, rollout, or initial creation. On timeout the operation
fails with reason `InstanceReadyTimeout`; surge migrations roll back.

**The controller-owned readiness gate.** OMENative appends one
readiness gate, `ome.io/serving`, to every managed pod at creation
time. The gate uses a multi-writer protocol ported from RBG: the
condition's `Message` field carries a JSON list of `{userAgent, key}`
entries, one per writer wanting the pod NotReady. The condition is
True only when the list is empty AND `ContainersReady=True`.

Writers and their keys:

- `Migrate-source-drain` / key=migration UUID
- `Update-in-place` / key=`<instance-idx>-<incarnation>`
- `Update-recreate-drain` / key=`<instance-idx>-<incarnation>`
- `Restart-drain` / key=restart operation ID
- `Delete-drain` / key=instance index
- `Lifecycle-instance-gang` / key=`<instance-idx>` — added on each
  pod that is `ContainersReady` but whose Instance siblings are not;
  removed once every sibling reaches `ContainersReady`. This enforces
  Instance-level gang readiness without making the gate itself
  Instance-scoped.

Each writer adds or removes only its own key; multiple writers can
overlap (e.g., a migration drain and an in-place update drain on the
same pod) without overwriting each other. Every write uses
`retry.RetryOnConflict` and patches only the single condition, so it
does not clobber kubelet's concurrent writes to `PodScheduled`,
`ContainersReady`, etc.

PodSpec-level `readinessGates` from the runtime or user spec are
preserved. OMENative appends its gate; it does not replace or remove
existing gates. If a user template already declares `ome.io/serving`,
OMENative reuses it rather than duplicating.

Migration surge pods must reach Ready before OMENative proceeds to
terminate the old pods, bounded by `instanceReadyTimeout`.

### Port Allocation

For workloads using `hostNetwork: true` (common with RDMA), OMENative
provides annotation-driven port allocation:

```yaml
metadata:
  annotations:
    ome.io/port-allocations: |
      {
        "allocations": [
          {"name": "grpc",  "env": "GRPC_PORT", "scope": "PodScoped"},
          {"name": "nccl",  "env": "NCCL_PORT", "scope": "InstanceScoped"},
          {"name": "prom",  "env": "PROM_PORT", "scope": "ComponentScoped"}
        ],
        "references": [
          {"env": "LEADER_PORT_REF", "from": "engine.leader.grpc"}
        ]
      }
```

Three scopes:

- **`PodScoped`** — unique port per pod.
- **`InstanceScoped`** — shared port across pods within the same
  Instance (e.g., NCCL within a tensor-parallel group).
- **`ComponentScoped`** — shared port across all pods of a Component
  (well-known service ports).

The `references` block pulls another Component/Runner/port-name's
allocated value into the current pod's env. Used by routers needing
the leader's gRPC port.

A default random allocator chooses unallocated ports in a configurable
range (default 30000–65000). Allocation state must persist in
controller-owned status or revision metadata; it must not depend on
StatefulSet annotations, because OMENative manages pods directly.

### Migration

Migration is requested via an annotation on the `InferenceService`.
Alfred (or any authorized caller) writes:

```yaml
metadata:
  annotations:
    ome.io/migration-request-v1-<uuid>: |
      {
        "schemaVersion": "v1",
        "component": "engine",
        "instance": 0,
        "reason": "fragmentation",
        "from_node": "node1",
        "hint_target_nodes": ["node3", "node7"],
        "requested_at": "2026-04-16T12:00:00Z",
        "requested_by": "alfred-controller"
      }
```

Request lifecycle:

1. OMENative observes the annotation. Within 30 seconds, it validates
   and either accepts (clearing the annotation) or rejects.
2. On accept, an entry is added to `Status.MigrationHistory` with
   `phase: Pending`. The request UUID is recorded for idempotency.
3. The caller observes the status update and knows the request is
   accepted.
4. If OMENative has not cleared the annotation after 5 minutes,
   Alfred may retry by writing the same UUID. OMENative dedupes via
   the UUID.
5. Annotations older than 1 hour without acknowledgment are stale;
   OMENative clears them with event `MigrationRequestStale`.

These 30s / 5m / 1h values are the **Alfred-facing** request lifecycle
contract only. Controller-internal step deadlines and retry policy
are defined in
[controller-mechanics.md](./controller-mechanics.md#54-operation-timeouts-and-backoff).

**Status.MigrationHistory** is a rolling window (20 entries per
`InferenceService` by default). When the window is full and a new
entry is added, the oldest is dropped. If durable archiving is
enabled, dropped entries are appended to an OMENative-owned audit
ConfigMap (one per `InferenceService`). The
`omenative_migration_history_truncated_total` metric counts every
truncation event regardless of whether it was archived.

```yaml
status:
  migrationHistory:
    - id: <uuid>
      component: engine
      instance: 0
      replacementInstance: 2
      mode: surge
      phase: InProgress
      requestedAt: ...
      startedAt: ...
      completedAt: null
      reason: fragmentation
      requestedBy: alfred-controller
      events:
        - at: ...
          message: "surge Instance created on node3"
      outcomeReason: ""
```

Version skew rule: an unsupported `schemaVersion` is rejected with
`UnsupportedSchemaVersion`; a supported `schemaVersion` with unknown
additive fields silently ignores them; behavioral changes require a
new schema version and annotation key prefix.

**Mode selection** in v1 is surge or rejection — there is no rolling
per-pod migration path:

```
if cluster_has_capacity_for_one_extra_instance:
    use Surge
else:
    reject (Reason: InsufficientCapacity)
```

Override via `InferenceService.Spec.<component>.omenative.migrationPolicy.mode`:
`auto` (default), `surge`, or `never`.

**Surge migration mechanics:**

1. Validate cluster has capacity for one extra Instance of this
   Component.
2. Allocate a fresh surge Instance index. Create a full replacement
   pod set with stable names
   (`<isvc>-<component>-<new-instance>-<runner>-<ordinal>`). The pod
   template has hard anti-affinity excluding `from_node`, plus
   optional preferred affinity toward `hint_target_nodes`.
3. Wait for all surge pods to reach runtime Ready (bounded by
   `instanceReadyTimeout`).
4. Flip the surge Instance's serving gate to True and wait until
   EndpointSlices show it ready and `minReadySeconds` is satisfied.
5. Drain the old Instance by flipping its serving gate to False and
   waiting for EndpointSlice convergence.
6. Delete the old Instance pods and old PodGroup.
7. Remove the old `InstanceStatus`, keep the surge `InstanceStatus`,
   and mark the migration Completed.

V1 accepts sparse live instance indices after migration. Migrating
Instance 0 may leave the Component with live indices `{1, 2}`.
OMENative does not attempt in-place renumbering.

**Multi-pod migration is deferred.** When the source Instance is
multi-pod (any Runner has `Size > 1`), the migrator rejects the
request up-front with reason `MultiPodMigrationNotYetSupported` and
writes a `Failed` audit row. A future release will specify the
per-gang-member surge slot layout; see [Future Work](#future-work).
Without this guard the migrator would silently allocate a single-pod
surge that the multi-pod readiness gate would never accept.

**Concurrency:** one migration in progress per Instance at a time; a
second request for the same Instance is rejected with
`MigrationInProgress`. Cluster-wide cap is
`omeNativeConfig.inFlightMigrationCap` (default 3); excess rejected
with `RateLimited`. Per-hour cap is `omeNativeConfig.maxMigrationsPerHour`
(default 10). These defaults align with
[controller-mechanics.md](./controller-mechanics.md#54-operation-timeouts-and-backoff).

**Timeout and rollback:** the per-migration timeout is
`instanceReadyTimeout + 5m`. On timeout in surge mode the surge
Instance pod set is deleted (best-effort rollback) and the migration
is marked Failed.

### Strategy Migration from LWS

Users with running `InferenceService`s on the LWS-backed `MultiNode`
or `PDDisaggregated` modes may migrate to OMENative, but **zero-downtime
cutover is not universal**. Procedure:

1. The user annotates the `InferenceService` with
   `ome.io/migrate-strategy-to: OMENative` (or changes the per-Component
   `ome.io/deploymentMode` annotation values directly — the
   migrate-strategy annotation is a declarative hint to the controller
   for intermediate phased migration).
2. OMENative's controller classifies the target Component's cutover
   as overlap-safe or not.
3. **Safety boundary.** For stateless or explicitly overlap-safe
   runtimes, the controller may create OMENative-managed resources in
   parallel with the existing LWS-backed ones, consuming roughly 2×
   capacity briefly, and may widen the Component Service selector
   briefly so both populations serve during cutover. For stateful
   engine and decoder runtimes, OMENative must not assume that old
   and new populations can safely co-serve behind one selector —
   split-brain, duplicate membership, and cache incoherence are real
   risks.
4. Zero-downtime strategy migration is therefore supported only for
   Components whose runtime contract explicitly declares overlap-safe
   cutover. For engine and decoder Components without that proof, the
   safe v1 path is controlled drain-to-zero of the LWS population,
   then recreate under OMENative, which may require a maintenance
   window.
5. After the old LWS path is drained and removed (or an overlap-safe
   cutover completes), the controller removes the old LWS-managed
   resources, Services, and PDB no longer needed.
6. The annotation is cleared. The per-Component `ome.io/deploymentMode`
   annotation settles at `OMENative`.
7. A migration history entry is recorded.

Requirements: roughly 2× the Component's capacity during the migration
window; a ServingRuntime whose pod template works under both
strategies; for overlap-safe cutover, a runtime that explicitly
tolerates simultaneous old+new populations behind one Service
selector.

Limitations: if the ServingRuntime requires LWS-specific labels or
features, the automatic procedure fails — the user must adjust the
runtime first. Single-replica `InferenceService`s without 2× capacity
must either accept a managed-recreate restart or wait for capacity.

**Operational runbooks.**

*Sparse instance index inspection.* When migration leaves live indices
non-dense, inspect both the live set and the replacement history:

```bash
kubectl get isvc <name> -o jsonpath='{range .status.components.engine.omenative.instanceStatuses[*]}{.index}{"\t"}{.phase}{"\t"}{.runningRevision}{"\n"}{end}'
kubectl get isvc <name> -o jsonpath='{range .status.migrationHistory[*]}{.instance}{"->"}{.replacementInstance}{"\t"}{.phase}{"\n"}{end}'
```

`instanceStatuses[*].index` shows the current live Instances;
`migrationHistory[*].replacementInstance` records which live index
replaced which original during surge migration.

*LWS → OMENative cutover abort.* There is no dedicated rollback verb
in v1. Operator guidance, in order of how far the cutover has
progressed:

- Before the old LWS population has begun draining: clear
  `ome.io/migrate-strategy-to`, remove any OMENative candidate
  resources (`ome.io/managed-by=OMENative`), keep the existing LWS
  population serving.
- After overlap-safe selector widening but before old LWS drain:
  restore the selector to the old LWS-serving population only, clear
  the annotation, remove OMENative candidate resources.
- After old LWS drain has begun: rollback is not supported. Do not
  re-widen the selector to include both populations again. Force the
  cutover forward to one surviving population, then reattempt
  migration later if needed.

### Model Availability

OME's existing mechanism is reused unchanged: `pkg/constants/constants.go`
defines label constructors that produce labels like
`models.ome.io/{ns}.basemodel.{name}` or
`models.ome.io/clusterbasemodel.{name}` with value `Ready`. The
model-agent sets these labels on nodes once the model is ready. OME's
existing util layer injects `NodeSelector` / `NodeAffinity` on pod
specs based on referenced models, and OMENative-emitted pods inherit
this injection.

The consequence for migration: when OMENative creates a surge pod,
the pod already has node affinity requiring the model-ready label on
the target node. If no feasible node matches (e.g., the model has not
been distributed there), the pod stays Pending and the migration
times out per `instanceReadyTimeout`. Alfred consumes the same labels
when filtering `hint_target_nodes` (see OEP-0008 §Execution).

No new API surface. No auto-download in v1 — operators pre-provision
the model via `BaseModel` / `ClusterBaseModel`.

### Gang Scheduling

Multi-pod Instances need gang scheduling: all pods of the Instance
schedule together or none. The supported scheduler in v1 is
Kubernetes `scheduler-plugins` Coscheduling.

For each multi-pod Instance, OMENative creates one
`scheduling.x-k8s.io/v1alpha1 PodGroup` with
`MinMember == TotalPodsForInstance(inst)` and
`ScheduleTimeoutSeconds == plan.InstanceReadyTimeout`. Each pod in
the Instance carries `scheduling.x-k8s.io/pod-group=<podgroup-name>`.
The PodGroup is owner-ref'd to the parent `InferenceService` so
scale-down and delete reap for free. Single-pod Instances do not
carry the pod-group label and do not need a PodGroup.

**Soft-fail when the CRD is missing.** The controller manager probes
for the PodGroup CRD once at startup and caches the result as
`GangSchedulingAvailable`, threaded through every reconcile. When the
CRD is absent and the Component has any multi-pod Instance, OMENative
still creates the pods (gang scheduling is a soft requirement) and
stamps a `GangSchedulingUnavailable=True` Component condition with
reason `PodGroupCRDNotInstalled`. Pods may schedule partially and the
runtime may hang waiting for missing peers; the symptom surfaces
through `instanceReadyTimeout`.

| Multi-pod Instance? | CRD installed? | `GangSchedulingUnavailable` |
|---|---|---|
| No | either | `False` (not required) |
| Yes | Yes | `False` (available) |
| Yes | No | `True` (`PodGroupCRDNotInstalled`) |

Volcano and Yunikorn are explicit deferrals.

**Operator-side requirement: `schedulerName`**. Creating PodGroup
objects is not enough on its own — the pods themselves must be
scheduled by a scheduler that reads
`scheduling.x-k8s.io/v1alpha1`. The operator MUST set
`runtime.spec.schedulerName` (on the ServingRuntime /
ClusterServingRuntime, where it is propagated to every Component's
pod template) or `podSpec.schedulerName` (on the InferenceService,
overriding per-Component) to a gang-aware scheduler name. Supported
values OME documents:

- `scheduler-plugins-scheduler` — the standard name for the
  scheduler-plugins controller deployed as a *secondary* scheduler
  alongside `default-scheduler` (this is the recommended path; see
  the [scheduler-plugins install
  guide](https://github.com/kubernetes-sigs/scheduler-plugins/tree/master/manifests/install)).
- A custom name when scheduler-plugins is deployed under a non-stock
  binary name in the cluster.
- A Volcano scheduler name (deferred — listed for completeness; not
  enforced in v1).

**Controller-side soft warning: `MaybeNoGangScheduler`**. When the
OMENative reconciler creates a PodGroup for a multi-pod Instance and
the rendered pod template's effective `spec.schedulerName` is empty
or `default-scheduler`, the controller emits a one-shot Warning event
(`reason=MaybeNoGangScheduler`) on the parent InferenceService. The
event is heuristic: an operator who has installed scheduler-plugins
as an *in-tree plugin* on the default scheduler (single-scheduler
deployment) will see the warning even though their setup is correct
— from inside the controller we cannot distinguish "stock
kube-scheduler" from "kube-scheduler with Coscheduling plugin
enabled". We err on the side of surfacing the heuristic because the
much more common misconfiguration is the operator forgetting to set
`schedulerName` entirely. The event is de-duplicated per
`(InferenceService, Component)` per controller process so reconciles
do not spam the event log; on controller restart the warning fires
once more (acceptable since operators re-scan events after a
restart).

The controller deliberately does NOT fail the reconcile or block
pod creation on the schedulerName check: gang scheduling is a soft
contract per the discussion above, and a hard check would create
false confidence in the in-tree-plugin case.

### Webhook Interaction

OME's pod admission webhook at `pkg/webhook/admission/pod/` runs
seven injectors (metrics aggregator, fine-tuned adapter, serving
sidecar, RDMA, shared memory, probe, observability). OMENative pods
do **not** go through that webhook — they are exempted via the
`ome.io/managed-by=OMENative` label.

Instead, OMENative calls the same injector functions directly during
rendering. The injector logic lives in a shared `pkg/podinjection/`
package that both the webhook (for non-OMENative modes) and OMENative's
render path invoke. Per-injector behavior is identical regardless of
caller.

Capturing the post-injection `PodSpec` in OMENative's revision hash
is necessary for accurate in-place eligibility checks and for OEP-0014
(pinned ServingRuntime revisions). With the webhook running after
`client.Create(pod)`, the controller would have no way to hash what
kubelet actually sees. Routing through direct calls eliminates the
divergence. This pattern matches RBG, which has no pod admission
webhook at all and renders entirely from the controller. OMENative
cannot remove OME's webhook outright because non-OMENative modes
still depend on it, but the shared package keeps both call sites in
sync.

**External admission webhooks (out of scope).** Cluster operators may
install other mutating webhooks (Istio, Linkerd, Vault Agent, Cilium,
custom org webhooks) that mutate pods at Create time. Their mutations
are not part of OMENative's revision payload, and changes to those
webhooks' configuration do not trigger OMENative rollouts. To pick up
such changes, operators trigger a rollout via spec edit. This is the
same limitation RBG and every other controller has.

### Status Propagation

OMENative writes to `InferenceService.Status`:

```yaml
status:
  conditions:
    - type: Ready
      status: "True"
    - type: EngineReady
      status: "True"
    - type: DecoderReady
      status: "True"
    - type: RouterReady
      status: "True"
    - type: Progressing
      status: "False"
    - type: Degraded
      status: "False"
  observedGeneration: 42
  components:
    engine:
      omenative:
        observedGeneration: 42
        currentRevision: llama-70b-engine-rev-abc123
        updateRevision: llama-70b-engine-rev-abc123
        labelSelector: "ome.io/inferenceservice=llama-70b,ome.io/component=engine"
        replicas: 4
        readyReplicas: 4
        availableReplicas: 4
        conditions:
          - type: Available
            status: "True"
          - type: GangSchedulingUnavailable
            status: "False"
            reason: GangSchedulingAvailable
        instanceStatuses:
          - index: 0
            incarnation: 3
            phase: Ready
            runningRevision: llama-70b-engine-rev-abc123
            targetRevision: llama-70b-engine-rev-abc123
            activeOrdinal: 0
            podCount: 4
            readyPodCount: 4
            availablePodCount: 4
            scheduledPodCount: 4
            nodesOccupied: [node3, node7, node9, node12]
    decoder: { ... similar ... }
    router: { ... similar ... }
  migrationHistory:
    - id: ...
      component: engine
      instance: 0
      phase: Completed
      ...
```

The nested `components[*].omenative` field is additive and optional
in CRD terms. Older clients ignore it; Components not annotated with
`ome.io/deploymentMode: OMENative` leave it `nil`.

**Top-level component-ready conditions.** `AggregateAndWriteStatus`
derives `EngineReady` / `DecoderReady` / `RouterReady` from the
OMENative counters (Ready = True iff `ReadyReplicas == Replicas > 0`)
and stamps them onto `InferenceService.Status.Conditions` — the same
contract `PropagateRawStatus` / `PropagateMultiNodeStatus` apply for
the LWS-backed modes. Without this, the ingress reconciler, the
aggregate `Ready` rollup, and `kubectl wait --for=condition=Ready`
all mis-classify OMENative `InferenceService`s as not-Ready forever.
The fresh condition is also mirrored onto the cached `InferenceService`
the outer reconciler holds, so the parent's `updateStatus` flush does
not clobber what `AggregateAndWriteStatus` just committed.

**Per-Instance failures escalate to the Component.** When the
per-Instance deadline lapses for an Instance and the Instance moves
to `Phase=Failed`, the coordination layer rolls that failure up to
`Component.Failed` with a `FailureMessage` naming the stuck Instance
indices. Without this rollup, a rollout sits at `Surging` or
`Waiting` indefinitely after a per-Instance deadline lapses. Every
coordination policy translates `Component.Failed` into the group's
own `Failed` phase.

**Lean (no-model) `InferenceService`s.** When `spec.Model == nil`,
the status writer skips the model-loading lifecycle block, so no
`modelStatus` surfaces. The `+kubebuilder:default=UpToDate` on
`TransitionStatus` was dropped in favor of `omitempty` + `+optional`.
Runtime-only `InferenceService`s are now a first-class supported
configuration; no surrogate `UpToDate` model state appears for
`InferenceService`s that never declared a model. This applies to
every deployment mode, not just OMENative.

After an upgrade, OMENative reconstructs this status from live pods,
Services, `ControllerRevision`s, PodGroups, and EndpointSlices, so no
storage migration is required. Per-Runner counters are intentionally
**not** persisted in `InstanceStatus` in v1 — runner-level debugging
comes from live pod labels (`ome.io/runner`) rather than a second
aggregated status layer.

### Coordinated Rollout

Coordinated engine/decoder rollout was deferred from OMENative v1 and
ships as a separate, additive design —
[OEP-0007.1 (OMENative Coordinated Rollout)](../0007.1-omenative-coordinated-rollout/README.md).
OEP-0007.1 introduces `spec.rolloutCoordination.groups[]` (declarative
opt-in), per-revision Services, generation-scoped peer discovery, and
the `Status.Components.<c>.Traffic[]` producer that closes the OEP-0011
Part 2 contract.

In OEP-0007 v1 (unchanged):

- Each OMENative-managed Component rolls independently when no group
  is declared (preserved for back-compat).
- Alfred migration targets one Component Instance at a time.
- No `CoordinationPaused` condition is required at this layer.

OEP-0007.1's coordination layer also reads `Component.Failed` (rolled
up from any Instance that hit `Phase=Failed`) so a stuck per-Instance
deadline lifts the group to its `Failed` phase rather than sitting at
`Surging` or `Waiting` indefinitely. Operators see a `Component.Failed`
with a precise message identifying the stuck Instance indices.

### RBAC

OMENative controller permissions (additive to existing OME
ClusterRole):

```yaml
# Core K8s resources
- apiGroups: [apps]
  resources: [controllerrevisions]
  verbs: [get, list, watch, create, update, patch, delete]
- apiGroups: [""]
  resources: [services, configmaps]
  verbs: [get, list, watch, create, update, patch, delete]
- apiGroups: [""]
  resources: [pods]
  verbs: [get, list, watch, create, update, patch, delete]
- apiGroups: [""]
  resources: [pods/status]
  verbs: [get, patch, update]
- apiGroups: [""]
  resources: [events]
  verbs: [create, patch]
- apiGroups: [discovery.k8s.io]
  resources: [endpointslices]
  verbs: [get, list, watch]
- apiGroups: [policy]
  resources: [poddisruptionbudgets]
  verbs: [get, list, watch, create, update, patch, delete]
- apiGroups: [scheduling.x-k8s.io]
  resources: [podgroups]
  verbs: [get, list, watch, create, update, patch, delete]

# OME API
- apiGroups: [ome.io]
  resources: [inferenceservices, inferenceservices/status]
  verbs: [get, list, watch, update, patch]
- apiGroups: [ome.io]
  resources: [servingruntimes, clusterservingruntimes, basemodels,
              clusterbasemodels, acceleratorclasses]
  verbs: [get, list, watch]
```

LWS-related RBAC remains until the deprecation window closes.

### Observability

**Prometheus metrics:**

- `omenative_reconcile_duration_seconds{isvc,component,outcome}` (histogram)
- `omenative_instance_count{isvc,component,state}` (gauge; state: Running, Pending, Failed, Migrating)
- `omenative_inplace_update_total{isvc,component,outcome}` (counter)
- `omenative_migration_requests_total{isvc,component,mode,outcome}` (counter)
- `omenative_migration_duration_seconds{isvc,component,mode,outcome}` (histogram)
- `omenative_migration_failures_total{isvc,component,reason}` (counter)
- `omenative_migration_history_truncated_total` (counter)
- `omenative_gang_scheduling_wait_seconds{isvc,component}` (histogram)
- `omenative_group_restart_total{isvc,component,reason}` (counter)
- `ome_deprecated_deploymentmode_total{namespace,isvc,mode}` (counter)

**Events** on `InferenceService`:

- `OMENativeReconcileSucceeded`, `OMENativeReconcileFailed`
- `InstanceCreated`, `InstanceReady`, `InstanceFailed`
- `InPlaceUpdateStarted`, `InPlaceUpdateCompleted`, `InPlaceUpdateFailed`, `InPlaceUpdateRejected`
- `MigrationRequestAccepted`, `MigrationRequestRejected`, `MigrationCompleted`, `MigrationFailed`, `MigrationRequestStale`
- `GroupRestartTriggered`
- `DeprecatedDeploymentMode` (for LWS-mode `InferenceService`s)

**Logs:** structured JSON; every multi-step operation carries a
correlation ID (request UUID for migrations, revision hash for
updates).

## Coexistence with LWS-Backed Modes

OMENative is selected per-Component by setting
`ome.io/deploymentMode: OMENative`. Other Components can use any
other strategy by setting (or not setting) the same annotation.

- **Router:** free. Typically `RawDeployment` or `OMENative`.
  Decoupled from engine/decoder via Service discovery.
- **Engine and decoder:** when both are present (prefill-decode
  mode), they must share the same `ome.io/deploymentMode` annotation
  value. The admission webhook rejects mismatches with
  `InvalidDeploymentModeCombination`. Coordinated rollout, shared
  migration semantics, and restart-policy coupling all require a
  single controller watching both.
- **Engine alone** (single-Component serving, no prefill-decode):
  picks any mode independently.

For a simple single-pod engine, the default remains `RawDeployment`.
OMENative is the better choice only when the workload needs
Instance-scoped migration or drain semantics for Alfred.

**Deprecation plan — six-release window:**

| Phase | OME version | LWS-backed modes | OMENative | Communication |
|-------|-------------|------------------|-----------|---------------|
| 0 | current | Supported (default) | Does not exist | none |
| 1 | +1 minor | Supported | Alpha, annotation-driven opt-in | none |
| 2 | +2 minor | Supported | Beta | Controller logs + K8s Events |
| 3 | +3 minor | Supported | Beta (default for new multi-node) | + Admission warnings |
| 4 | +4 minor | Frozen (bug fixes only) | GA, recommended | + Escalated warnings (multi-line, removal date) |
| 5 | +5 minor | Marked for removal | GA | + Announcement |
| 6 | +6 minor | Removed; `sigs.k8s.io/lws` removed from go.mod | GA (sole multi-node path) | Admission rejects LWS-mode |

Warning text at phase 3+ (exact URL and `vX.Y` populated when OMENative
reaches GA and the migration guide is published):

```
InferenceService Component <name> has annotation
ome.io/deploymentMode=MultiNode, which uses the deprecated LWS-backed
strategy. Migrate to ome.io/deploymentMode=OMENative. See migration
guide: <URL>. This path will be removed in OME vX.Y.
```

Supplementary signals: the `ome_deprecated_deploymentmode_total`
metric is alertable; the `DeprecatedDeploymentMode` event fires on
transition into the deprecated-mode state (not every reconcile);
controller logs emit WARN.

## Feature Gating and Rollout

OMENative does not ship behind a Kubernetes feature gate. Selection
is per-Component via the existing `ome.io/deploymentMode` annotation.
Operators opt in by adding `ome.io/deploymentMode: OMENative` to a
Component's metadata annotations; absence (or any other supported
value) means the existing mode for that Component continues to apply.

This keeps the alpha rollout minimally invasive — no controller-wide
flag, no global toggle, no admission gating beyond the enum validation
OME already performs on `DeploymentModeType`. The trade-off is no
single cluster-wide kill switch; for fleet-wide rollback during alpha,
operators remove the annotation from each Component (or revert OME
to the previous version).

A typed `deploymentMode` field on EngineSpec / DecoderSpec / RouterSpec
and/or a real Kubernetes feature gate may be introduced at beta or GA
based on operational experience. Both are additive changes that do
not break the alpha annotation contract.

When OMENative is not yet implemented for a given build, an
`InferenceService` Component annotated with `ome.io/deploymentMode:
OMENative` falls through the existing `IsValid()` enum check and is
rejected at admission with the standard "unsupported deployment mode"
error.

## Test Plan

### Unit Tests

Target coverage per package:

- `planner.go`: ≥ 85%
- `reconciler.go`: ≥ 85%
- `updater.go`: ≥ 90%
- `migrator.go`: ≥ 90%
- `pod_watcher.go`: ≥ 90%
- `drain.go`: ≥ 90%
- `expectations.go`: ≥ 85%
- `podgroup.go`: ≥ 85%
- `status.go`: ≥ 85%

### Integration Tests

Deployed in KIND clusters with scheduler-plugins Coscheduling for
gang scheduling. A fake GPU device plugin handles resource accounting.

Core scenarios:

1. **Single-pod RawDeployment-equivalent.** `InferenceService` with
   `engine.minReplicas=1` and `ome.io/deploymentMode: OMENative` on
   `spec.engine`, no leader/worker. Verify one direct pod with stable
   name, headless Service, and env vars.
2. **Single-Component multi-node.** Leader + 3 workers. Verify two
   Runner pod sets (leader=1, worker=3), DNS resolution correct, one
   PodGroup, pods gang-scheduled.
3. **Multi-Component single-Instance prefill-decode.** Router +
   engine + decoder, each with 1 Instance. Verify Service topology
   and cross-Component discovery.
4. **Multi-Component multi-Instance prefill-decode.** Engine (2) +
   decoder (3) + router (2). Verify correct pod and PodGroup count.
5. **Rollout with image change, `InPlaceIfPossible`.** Verify pods
   stay, container restarts in place, EmptyDir preserved (drop a
   sentinel file in EmptyDir before update; verify it survives).
6. **Rollout with volume change, `InPlaceIfPossible`.** Verify fall
   back to recreate.
7. **Rollout with volume change, `InPlaceOnly`.** Verify rejected
   with status condition.
8. **Scale up/down.** Change MinReplicas; verify new Instance pod
   sets created/deleted.
9. **Group restart on pod failure.** Kill a worker pod with
   `restartPolicy: RecreateInstanceOnPodRestart`. Verify all pods of
   the Instance are restarted.
10. **Migration (surge).** Verify surge Instance with fresh index is
    created, Ready, old Instance drained and terminated, sparse
    indices tolerated, and `migrationHistory` records
    `instance -> replacementInstance`.
11. **EndpointSlice drain.** Verify the serving gate False is
    observed before image patch or pod deletion.
12. **Migration rejection (insufficient capacity).** Verify rejection
    with correct reason.
13. **Migration timeout.** Verify surge rollback and Failed status.
14. **Concurrent migration protection.** Two requests for the same
    Instance; second rejected.
15. **Rate limiting.** 10 requests rapidly; only 3 in-flight.
16. **User readiness gates preserved.** Verify OMENative appends
    `ome.io/serving` without clobbering existing gates.
17. **Deprecation warnings at phase 3.** Verify webhook emits warning
    on `ome.io/deploymentMode: MultiNode` apply.
18. **Unknown deployment-mode rejection.** Verify a Component
    annotated with `ome.io/deploymentMode: OMENative` on a controller
    build that does not yet support OMENative is rejected at
    admission.
19. **Coexistence.** An OMENative `InferenceService` and an LWS-mode
    `InferenceService` in the same namespace. No interference.
20. **LWS → OMENative strategy migration.** Verify dual-write, drain,
    completion.
21. **Engine/decoder deployment-mode mismatch.** Verify admission
    rejects mismatched annotations.
22. **Port allocation.** PodScoped, InstanceScoped, ComponentScoped;
    verify correct env values.
23. **Unsupported schema version.** Write a migration request with
    unknown `schemaVersion`; verify rejection with
    `UnsupportedSchemaVersion`.

### Chaos / Robustness Tests

- Controller restart mid-reconcile during migration.
- Node failure during migration.
- API server slow response.
- etcd slow.

### Simulation / CI Environment

- Unit and planner tests use synthetic cluster snapshots (no real
  Kubernetes).
- Integration tests run on KIND with fake GPUs via a stub device
  plugin.
- End-to-end tests with real GPUs run on dedicated CI runners.

## Graduation Criteria

### Alpha

- All unit tests passing.
- Integration tests 1–9 passing.
- Opt-in is per-Component via `ome.io/deploymentMode: OMENative`;
  OMENative is invoked only on Components carrying it.
- Documentation covers: annotation-based opt-in, known limitations,
  the alpha contract that no feature gate is shipped (the only
  rollback is annotation removal or controller revert).
- No backward-compatibility promises on internal status fields.

### Beta

- Integration tests 1–22 passing, including chaos.
- 2+ production users running OMENative-managed workloads ≥ 60 days
  without high-severity incidents attributed to OMENative.
- Dashboard published.
- Migration guide published.
- Decision recorded on whether to promote the annotation contract to
  a typed `deploymentMode` field and/or to introduce a Kubernetes
  feature gate. Either, both, or neither is acceptable.
- API surface (status fields, annotation contract) frozen except for
  bug fixes.

### GA

- 3 months of Beta with no regressions.
- Deprecation plan reaches phase 3 (admission warnings).
- If a feature gate was introduced at beta, it is removed (or
  promoted to permanently-on); if no feature gate was introduced, the
  annotation contract is the GA surface.
- Test coverage ≥ 85% across all OMENative packages.
- Post-mortem process for any OMENative-attributed production
  incidents.

## Drawbacks

1. **Implementation cost.** Roughly 2000+ lines of new code plus
   extensive tests and documentation. One to two engineer-quarters
   to alpha quality; sustained ownership thereafter.
2. **Reinventing LWS patterns.** LWS has years of bug-fix history
   OMENative restarts without. Mitigated by a test plan covering
   known LWS edge cases.
3. **Divergence from the sgl-project ecosystem (RBG).** Accepted
   trade-off for the UX win.
4. **No benefit for users satisfied with LWS.** Deprecation will
   eventually force them to migrate; some may resist.
5. **The migration verb opens new failure modes.** Migration is
   privileged and disruptive; bugs could cause outages. Blast radius
   is bounded to one Instance.
6. **Documentation burden.** New concepts (Instance, Runner,
   migration modes, coordination) must be documented; operators who
   know LWS must re-learn.
7. **LWS coexistence increases cognitive load during deprecation.**
   Two code paths active for six releases.

## Alternatives

Four alternatives were considered:

- **Continue with LWS, contribute upstream.** LWS charter is
  general-purpose; LLM-specific primitives are unlikely to be accepted.
  Upstream release cadence is unaligned with OME's.
- **Adopt RBG via RBGStrategy.** Multiplies user-visible CRDs (four
  sgl-project CRDs visible). Velocity coupled to RBG's roadmap.
- **Fork RBG InstanceSet code into OME.** Forked code diverges
  quickly; `InstanceSet`'s API is designed for RBG's context.
  Effectively greenfield with legacy baggage.
- **OMENative** (chosen): greenfield inside OME, emitting only
  Kubernetes-native resources, with LLM-specific primitives as
  first-class features. One CRD for users (`InferenceService`).

## Future Work

- **`InferenceReplica` CRD extraction.** Phase 1 (CRD scaffold +
  scheme + RBAC + webhook stubs) has landed; no behavioral use yet.
  This is the long-term direction for status ownership: per-Component status
  currently aggregated by the OMENative reconciler into
  `InferenceService.status.components[<c>].omenative` moves into one
  `InferenceReplica` per `(InferenceService, Component)` tuple,
  mirroring how Deployment → ReplicaSet → Pod compose. The
  `InferenceService` controller becomes a thin orchestrator that
  watches the children it owns and aggregates only the top-level
  rollup (`EngineReady` / `DecoderReady` / `RouterReady` +
  `Status.Components.<c>.Traffic[]`). Cross-Component coordination
  stays at the `InferenceService` layer because it is inherently
  `InferenceService`-scoped. `InferenceReplica` is controller-only —
  users never write to it; a validating admission webhook enforces
  that. Tracked for a follow-up OEP once the remaining extraction
  PRs land.
- **Multi-pod SurgeThenDrain.** Per-gang-member ordinal allocation
  for multi-pod surge. Multi-pod under `SurgeThenDrain` currently
  routes to recreate; a future amendment would specify the gang-surge
  ordinal scheme (likely `<runner>-<ord>-A` vs `<runner>-<ord>-B`
  suffix) once the basic multi-node path has soaked.
- **Multi-pod migration.** Same gating story: deferred until the
  multi-pod plumbing is proven and a per-gang surge slot layout is
  agreed.

## Open Issues

Live bugs and incomplete work that the alpha has surfaced. Distinct
from the research-level Open Questions below.

1. **Bumping spec while a SurgeThenDrain rollout is in flight does
   not consistently retarget.** A new revision targeted while an
   earlier-target surge pod is still surging or draining: the latest
   target is sometimes ignored, an extra pod can land, and `Ready`
   may flip True prematurely. Likely the same family as the in-memory
   snapshot drift the status mirror closed for the single-bump case.
   A future patch may need to chain a status mirror through the
   target-revision retarget path as well.
2. **Multi-pod SurgeThenDrain routes to recreate.** Real surge for
   multi-pod gangs is deferred (see Future Work above); flagged here
   so the gap is discoverable from the body of the OEP.

## Open Questions

Research-level questions that do not block design completeness.

1. **KV cache handoff protocol during migration.** Future primitive
   for transferring in-flight KV cache between old and new Instance.
   Requires runtime cooperation.
2. **RDMA fabric topology data source.** How OMENative or Alfred
   knows which nodes have full RDMA mesh connectivity. Candidate
   sources: GPU feature discovery labels, operator config, runtime
   discovery. No clear standard yet.
3. **GPU topology within a node (NVLink, PCIe).** Selecting the right
   GPUs for an 8-GPU workload inside a node. Deferred; v1 treats
   intra-node GPUs as fungible.
4. **DRA (Dynamic Resource Allocation) integration.** Migrate from
   `Resources.Limits["nvidia.com/gpu"]` to `ResourceClaims`. Revisit
   when DRA matures.
5. **User-facing rollback mechanism.** Internal `ControllerRevision`
   is emitted; a rollback verb (`ome.io/rollback-to-revision: <N>` or
   subresource) is future work.
6. **Component dependencies.** Explicit `dependencies` field on
   Component spec for cases requiring orderly restart cascades (e.g.,
   Mooncake-style KV master). Not required in v1 because router uses
   Service discovery; add when a concrete need surfaces.
7. **Gang scheduling fallback.** OMENative-managed pod scheduling
   gates for clusters without a gang-aware scheduler. Substantial
   work; deferred to a separate OEP if demand emerges.
8. **Final name.** Candidates for consideration before GA:
   `OMENative` (default), `Native`, `NativeGroup`, `OMECore`,
   `Bundle`. Pin before any code lands — every label, env var,
   metric, and package path embeds the name.
9. **Resource resize in place (Kubernetes 1.33+).** When
   `resizePolicy: RestartNotRequired` is set on a 1.33+ cluster,
   resource requests/limits could be classified as in-place capable.
   Follow-up addendum.
10. **Historical migration learning.** Alfred-side, but OMENative
    could consume feedback. Adjust migration timeouts and retry
    policy based on past outcomes. A v2+ feature.
11. **Cluster-domain resolution for `OME_LEADER_ADDRESS`.** Today the
    env var ships in short form and relies on the DNS search path.
    Some multi-node runtimes (NCCL, Ray under certain configurations)
    perform strict-FQDN lookups; if a real-world runtime trips on the
    short form, the planned escape hatch is a controller-manager flag
    (`--cluster-domain`) that fully qualifies the address.

## Implementation History

- **2026-01-15** — OEP-0006 authored (@bcfre, approved @slin1237).
- **2026-03-27** — Workload Strategy framework PR opened.
- **2026-04-16** — OEP-0007 initial draft; 37 design questions
  consolidated and locked.
- **2026-05-15 → 2026-05-21** — Single-pod alpha core lands: status
  surface, create / update / restart / delete / migrate paths,
  revision computation, drain, Services builder, timeouts.
- **2026-05-20** — `Serverless` deployment mode removed from OME;
  OEP references updated.
- **2026-05-21** — Pre-alpha design lock:
  - Adopt the RBG multi-writer readiness gate pattern for
    `ome.io/serving`.
  - Defer Loop A fan-out post-alpha; serial reconcile is not a
    prefill-decode correctness blocker.
  - Extract OME pod webhook injectors into a shared
    `pkg/podinjection/` package; OMENative pods bypass the webhook
    and call extracted injectors directly via render.
- **2026-05-21 → 2026-05-26** — OEP-0007.1 (coordinated rollout)
  lands as a follow-up: `spec.rolloutCoordination.groups[]` API,
  Independent / RollingUpdate / BlueGreen / Sequential policies,
  per-revision Services, generation-scoped env injection,
  `Status.Components.<c>.Traffic[]` producer for OEP-0011.
- **2026-05-26 → 2026-05-27** — Stabilisation pass: top-level
  component-ready conditions, rollout-paused filter in the revision
  hash, drain checks moved to the per-revision routed Service,
  per-Component Service / ingress / PodMonitor parity, per-Instance
  failure rolls up to `Component.Failed`, real SurgeThenDrain state
  machine with `ActiveOrdinal`, status threshold widened to
  `observed >= desired`, post-commit in-memory status mirror,
  Sequential RequeueAfter preservation in outer reconcilers,
  lean-spec status writer.
- **2026-05-27** — Multi-node Phase 1 (plan + render + revision hash
  for worker `PodSpec`) and Phase 2 (PodGroup gang scheduling +
  `GangSchedulingUnavailable` Component condition).
- **2026-05-27** — `InferenceReplica` CRD scaffold lands as Phase 1
  of the per-Component-CRD extraction series; see Future Work.
- **TBD** — Workload Strategy framework PR merged; OMENative alpha
  implementation; Alpha release; Beta release; GA; LWS removal from
  `go.mod`.
