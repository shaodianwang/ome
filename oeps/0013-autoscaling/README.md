# OEP-0013: Component-Scoped Autoscaling

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
  - [Notes/Constraints/Caveats](#notesconstraintscaveats)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Object model](#object-model)
  - [API: per-component `autoscaler` block](#api-per-component-autoscaler-block)
  - [Inheritance from ServingRuntime](#inheritance-from-servingruntime)
  - [KEDA: triggers pass-through](#keda-triggers-pass-through)
  - [HPA: metrics pass-through](#hpa-metrics-pass-through)
  - [External mode: published target + `/scale` subresource](#external-mode-published-target--scale-subresource)
  - [Coordinated scaling for PD-disaggregated services](#coordinated-scaling-for-pd-disaggregated-services)
  - [Status surface](#status-surface)
  - [Webhook validation](#webhook-validation)
  - [Backwards compatibility](#backwards-compatibility)
  - [Failure modes](#failure-modes)
  - [Test Plan](#test-plan)
  - [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
<!-- /toc -->

## Summary

Per-component autoscaler block on `InferenceService` that pass-through KEDA triggers and HPA metric specs verbatim, a `/scale` subresource on the ISVC, and a `scalingPolicy` for PD-disaggregated services. Defaults can live on `ServingRuntime` (1:1 pairing pattern); the ISVC overrides per component.

`KedaConfig` is removed (zero observed usage). Other legacy fields (`MinReplicas`, `MaxReplicas`, `ScaleMetric`, `ScaleTarget`, and the `ome.io/autoscalerClass` annotation) keep working with deprecation warnings.

## Motivation

OME today scales an InferenceService through HPA on a single CPU or memory metric — no story for PD-disaggregated services where engine, decoder, and router need to scale on different signals or in coordination, and no `/scale` subresource for user-owned autoscalers or `kubectl scale`. This OEP fills those gaps.

### Goals

- Per-component `spec.<component>.autoscaler` block with class = `hpa | keda | external | none`.
- Verbatim pass-through of `[]kedav1.ScaleTriggers` and `[]autoscalingv2.MetricSpec` — operators write the same thing they would in a hand-authored ScaledObject / HPA.
- `/scale` subresource on `InferenceService` so `kubectl scale isvc/foo` works and user-owned autoscalers can target the ISVC directly.
- `spec.scalingPolicy` with `proportional` mode for PD-disaggregated coordination (engine autoscales, decoder/router follow by ratio).
- Defaults mirrored onto `ServingRuntime` (1:1 pairing pattern), overridden per-component by the ISVC via whole-block replace. Status surface reports the active autoscaler + which layer drove it.

### Non-Goals

- **Not building a prediction engine.** Time-based scaling is delivered via KEDA cron triggers (pass-through).
- **Not replacing KEDA or HPA.** OME continues to generate and manage `ScaledObject` / `HPA` resources; this OEP is about the API and lifecycle around them.
- **Not auto-tuning replica policies.** Operators declare ratios; OME does not infer them.
- **Not removing all legacy fields in Alpha.** `KedaConfig` is removed (zero observed usage); other legacy fields stay through Alpha with deprecation warnings.

## Proposal

A new `Autoscaler` block on `ComponentExtensionSpec` (so engine, decoder, and router each get their own) plus a `ScalingPolicy` on `InferenceServiceSpec` for cross-component coordination. Full Go type definitions are in [Design Details / API](#api-per-component-autoscaler-block). The minimal user-facing shape:

```yaml
spec:
  engine:
    minReplicas: 2
    maxReplicas: 50
    autoscaler:
      class: keda
      keda:
        triggers:
          - type: prometheus
            metadata: { serverAddress: "...", query: "...", threshold: "20" }
  decoder:
    minReplicas: 2
    maxReplicas: 50
    # no autoscaler block — follower under scalingPolicy
  router:
    autoscaler: { class: hpa }   # defaults to CPU=80%
  scalingPolicy:
    mode: proportional
    proportional: { anchor: engine, ratios: { decoder: "1" } }
```

### User Stories

#### Story 1: Multi-trigger production engine

Operator wants to scale on queue depth + GPU utilization + a daily traffic floor.

```yaml
spec:
  engine:
    minReplicas: 4
    maxReplicas: 80
    autoscaler:
      class: keda
      keda:
        triggers:
          - type: prometheus
            metadata: { query: "<queue-depth-query>", threshold: "20", serverAddress: "…" }
          - type: prometheus
            metadata: { query: "<gpu-util-query>", threshold: "75", serverAddress: "…" }
          - type: cron
            metadata: { timezone: UTC, start: "0 12 * * *", end: "0 22 * * *", desiredReplicas: "20" }
```

Today: not expressible. KEDA's `triggers` array supports this natively but `KedaConfig` only had fields for one prometheus trigger.

#### Story 2: User owns the ScaledObject

Operator's platform team manages a fleet-wide `ScaledObject` policy in a separate Helm chart. They want OME to stay out of the way.

```yaml
# OME ISVC
spec:
  engine:
    autoscaler:
      class: external

# Separately, applied by the platform Helm chart:
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: foo-engine-platform
spec:
  scaleTargetRef:
    apiVersion: ome.io/v1beta1   # NEW — points at the ISVC, not the Deployment
    kind: InferenceService
    name: foo
  triggers: [...]
```

OME publishes the ISVC's scale target via the new `/scale` subresource so the user-managed ScaledObject can scale-of-record through the ISVC; OME's reconciler refuses to touch any HPA/ScaledObject when class=external. `status.components.engine.scaleTargetRef` documents the underlying Deployment name for users who prefer that target.

#### Story 3: Runtime-defined defaults with ISVC override

Platform team authors a ServingRuntime that bundles the right autoscaler for a workload class. Most ISVCs binding this runtime inherit; a latency-critical tenant overrides on its ISVC.

```yaml
# ServingRuntime — carries the default
kind: ClusterServingRuntime
metadata: { name: runtime-a }
spec:
  engine:
    minReplicas: 2
    maxReplicas: 20
    autoscaler:
      class: keda
      keda:
        triggers:
          - type: prometheus
            metadata: { query: "...", threshold: "15", serverAddress: "..." }

---
# ISVC — inherits runtime's autoscaler
kind: InferenceService
metadata: { name: foo }
spec:
  runtime: { name: runtime-a }

---
# Different ISVC — overrides engine block for tighter scaling
kind: InferenceService
metadata: { name: foo-tenant-a }
spec:
  runtime: { name: runtime-a }
  engine:
    autoscaler:
      class: keda
      keda:
        triggers:
          - type: prometheus
            metadata: { query: "...", threshold: "5", serverAddress: "..." }
```

`status.components.engine.autoscaler.specSource` reports `runtime` for the first ISVC and `isvc` for the second.

### Notes/Constraints/Caveats

- **`class: none` vs `class: external`.** `none` means "OME and user are both out" — used for proportional-mode followers. `external` means "user owns the scaler" — OME publishes `status.scaleTargetRef` for them to target.
- **Resource quotas matter.** Multi-trigger configs can scale aggressively. Operators dampen via `behavior` / `cooldownPeriod`; OME imposes no safety ceiling.
- **Proportional coordinator is a separate reconciler** under `reconcilers/scaling_policy/`. Watches Deployment status, writes via `/scale`, never touches HPA/ScaledObject for followers.
- **ServingRuntime's responsibility grows** beyond container-spec into workload-shaped defaults. Appropriate for 1:1 pairing; runtime authors managing shared runtimes should know changes propagate to every non-overriding ISVC on next reconcile.

### Risks and Mitigations

1. **Multi-trigger KEDA flap.** KEDA takes the max across triggers; a noisy metric can dominate. Operators dampen via `behavior` / `cooldownPeriod`. OME does not pre-validate operator scaler choices.

2. **`/scale` semantics for multi-component ISVC.** `kubectl scale isvc/foo --replicas=N` writes to the anchor of `scalingPolicy.proportional` (engine if not set); independent-policy services write `spec.engine.minReplicas`. Documented in the field comment; per-component control means scaling the underlying Deployment directly.

3. **PD coordinator ↔ engine autoscaler race.** Engine scales up via KEDA → coordinator updates decoder → KEDA observes lower metric → both scale down → flap. Mitigation: coordinator runs only on engine status changes (no polling), writes through `/scale` so HPA/KEDA see the new floor without racing.

4. **HPA `metrics` pass-through is permissive.** OME does not validate the underlying metric source exists — same risk profile as hand-written HPAs.

5. **`external` ↔ `keda`/`hpa` switch orphans the previous object.** OME deletes its own when class changes; user objects with the same name become user-owned. ISVC deletion does not cascade to user-owned objects. The `external` contract is explicitly "OME hands-off."

6. **Adoption gradient.** Half-migrated ISVCs in the wild during deprecation. Mitigation: `status.components.<comp>.autoscaler.specSource` reports which layer drives each ISVC's scaling.

## Design Details

### Object model

```mermaid
flowchart LR
    SR[ServingRuntime]
    ISVC[InferenceService]
    SR -. defaults .-> Auto
    ISVC -. overrides .-> Auto
    SR -. defaults .-> Policy
    ISVC -. overrides .-> Policy
    Auto[ComponentAutoscaler<br/>per engine / decoder / router]
    Policy[ScalingPolicy<br/>ISVC-level]
    Auto -- class=hpa --> HPAObj[(HPA)]
    Auto -- class=keda --> SO[(ScaledObject)]
    Auto -- class=external --> Ext[no OME object<br/>status.scaleTargetRef published]
    Auto -- class=none --> Skip[no scaler<br/>used by proportional followers]
    HPAObj --> Dep[(Deployment)]
    SO --> Dep
    Policy -- mode=proportional --> Coord[Scaling-policy<br/>coordinator]
    Coord -. /scale .-> Dep
```

### API: per-component `autoscaler` block

`pkg/apis/ome/v1beta1/component.go`:

```go
type ComponentExtensionSpec struct {
    // Existing fields preserved.
    MinReplicas         *int                          `json:"minReplicas,omitempty"`
    MaxReplicas         int                           `json:"maxReplicas,omitempty"`
    ScaleTarget         *int                          `json:"scaleTarget,omitempty"`  // deprecated, see Autoscaler
    ScaleMetric         *ScaleMetric                  `json:"scaleMetric,omitempty"`  // deprecated, see Autoscaler
    ContainerConcurrency *int64                       `json:"containerConcurrency,omitempty"`
    TimeoutSeconds      *int64                        `json:"timeoutSeconds,omitempty"`
    CanaryTrafficPercent *int64                       `json:"canaryTrafficPercent,omitempty"`
    Labels              map[string]string             `json:"labels,omitempty"`
    Annotations         map[string]string             `json:"annotations,omitempty"`
    MinAvailable        *intstr.IntOrString           `json:"minAvailable,omitempty"`
    MaxUnavailable      *intstr.IntOrString           `json:"maxUnavailable,omitempty"`
    DeploymentStrategy  *appsv1.DeploymentStrategy    `json:"deploymentStrategy,omitempty"`
    OMENative           *OMENativeComponentSpec       `json:"omenative,omitempty"`
    // KedaConfig removed in OEP-0013 — replaced by Autoscaler.Keda.

    // New in OEP-0013.
    // +optional
    Autoscaler *ComponentAutoscaler `json:"autoscaler,omitempty"`
}
```

`pkg/apis/ome/v1beta1/autoscaler.go` (new file):

```go
type AutoscalerClass string

const (
    AutoscalerHPA      AutoscalerClass = "hpa"
    AutoscalerKEDA     AutoscalerClass = "keda"
    AutoscalerExternal AutoscalerClass = "external"
    AutoscalerNone     AutoscalerClass = "none"
)

type ComponentAutoscaler struct {
    // +kubebuilder:validation:Enum=hpa;keda;external;none
    Class AutoscalerClass `json:"class"`

    // +optional
    Keda *KedaAutoscaler `json:"keda,omitempty"`

    // +optional
    HPA *HPAAutoscaler `json:"hpa,omitempty"`
}

type KedaAutoscaler struct {
    // +kubebuilder:validation:MinItems=1
    Triggers []kedav1.ScaleTriggers `json:"triggers"`

    // +optional
    Advanced *kedav1.AdvancedConfig `json:"advanced,omitempty"`

    // +optional
    PollingInterval *int32 `json:"pollingInterval,omitempty"`

    // +optional
    CooldownPeriod *int32 `json:"cooldownPeriod,omitempty"`

    // +optional
    IdleReplicaCount *int32 `json:"idleReplicaCount,omitempty"`

    // +optional
    Fallback *kedav1.Fallback `json:"fallback,omitempty"`
}

type HPAAutoscaler struct {
    // +optional
    // +listType=atomic
    Metrics []autoscalingv2.MetricSpec `json:"metrics,omitempty"`

    // +optional
    Behavior *autoscalingv2.HorizontalPodAutoscalerBehavior `json:"behavior,omitempty"`
}
```

`pkg/apis/ome/v1beta1/inference_service.go`:

```go
type InferenceServiceSpec struct {
    // ... existing fields ...

    // +optional
    ScalingPolicy *ScalingPolicy `json:"scalingPolicy,omitempty"`
}

type ScalingMode string

const (
    ScalingIndependent ScalingMode = "independent"
    ScalingProportional ScalingMode = "proportional"
    ScalingPinned      ScalingMode = "pinned"
)

type ScalingPolicy struct {
    // +kubebuilder:validation:Enum=independent;proportional;pinned
    Mode ScalingMode `json:"mode"`

    // Required when Mode == "proportional".
    // +optional
    Proportional *ProportionalPolicy `json:"proportional,omitempty"`
}

type ProportionalPolicy struct {
    // +kubebuilder:validation:Enum=engine;decoder;router
    // +kubebuilder:default=engine
    Anchor ComponentType `json:"anchor"`

    // Followers map to ratios relative to anchor replicas.
    // resource.Quantity allows fractional ratios like "0.25".
    // +optional
    Ratios map[ComponentType]resource.Quantity `json:"ratios,omitempty"`
}
```

### Inheritance from ServingRuntime

In the common 1:1 pairing pattern — one ServingRuntime authored per workload class, bound to one ISVC — the runtime is the natural home for workload-shaped defaults including scaling. Mirror the new `Autoscaler` and `ScalingPolicy` types onto `ServingRuntime` so the runtime can carry defaults while the ISVC overrides per component.

`pkg/apis/ome/v1beta1/serving_runtime_types.go`:

```go
type ServingRuntimePodSpec struct {
    // ... existing fields ...

    // Autoscaler default for ISVCs binding this runtime. Replaced wholesale
    // by isvc.spec.<component>.Autoscaler when set on the ISVC.
    // +optional
    Autoscaler *ComponentAutoscaler `json:"autoscaler,omitempty"`
}

type ServingRuntimeSpec struct {
    // ... existing fields ...

    // ScalingPolicy default for ISVCs binding this runtime. Replaced wholesale
    // by isvc.spec.scalingPolicy when set on the ISVC.
    // +optional
    ScalingPolicy *ScalingPolicy `json:"scalingPolicy,omitempty"`
}
```

#### Resolution — whole-block replace

Effective autoscaler config is resolved at reconcile time:

```mermaid
flowchart TD
    Start([resolve for component])
    Start --> Q1{isvc.spec.&lt;comp&gt;.autoscaler set?}
    Q1 -->|yes| ISVCwin[use ISVC block<br/>specSource = isvc]
    Q1 -->|no| Q2{runtime.spec.&lt;comp&gt;.autoscaler set?}
    Q2 -->|yes| RUNwin[use runtime block<br/>specSource = runtime]
    Q2 -->|no| Q3{legacy ScaleMetric / ScaleTarget<br/>or annotation set?}
    Q3 -->|yes| LEGwin[build from legacy<br/>specSource = legacy]
    Q3 -->|no| DEFwin[controller default<br/>HPA CPU=80%<br/>specSource = default]
```

`ScalingPolicy` resolves the same way against `isvc.Spec.ScalingPolicy` and `runtime.Spec.ScalingPolicy`. Remaining legacy fields (`MinReplicas`, `MaxReplicas`, `ScaleMetric`, `ScaleTarget`) keep their existing per-field override semantics.

**Why whole-block replace, not deep merge:**

- **Predictable.** When the operator sees `autoscaler:` on an ISVC, it replaces the runtime's entire block. No surprises from arrays merging in non-obvious ways.
- **Mirrors Kubernetes conventions.** `PodSpec` overrides do not merge array elements; this matches that ergonomic.
- **Sidesteps the "name your triggers / metrics" tax.** Upstream `kedav1.ScaleTriggers` and `autoscalingv2.MetricSpec` carry no stable identifier suitable for keyed merge.
- **Matches the 1:1 pattern.** Overrides exist precisely because the workload needs *different* scaling than the runtime default; partial merges are rare and confusing.

Resolution lives in one helper at `pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler/resolve.go`:

```go
type SpecSource string

const (
    SpecSourceISVC     SpecSource = "isvc"
    SpecSourceRuntime  SpecSource = "runtime"
    SpecSourceLegacy   SpecSource = "legacy"
    SpecSourceDefault  SpecSource = "default"
    SpecSourceExternal SpecSource = "external"
)

func ResolveComponentAutoscaler(
    runtime *v1beta1.ServingRuntimeSpec,
    isvc    *v1beta1.InferenceServiceSpec,
    comp    v1beta1.ComponentType,
) (*v1beta1.ComponentAutoscaler, SpecSource) { ... }
```

Pure, table-testable, called from both the reconciler and the webhook validator. The returned `SpecSource` is propagated into `status.components.<comp>.autoscaler.specSource` (see "Status surface").

### KEDA + HPA pass-through

`reconcilers/keda/` and `reconcilers/hpa/` are rewritten to consume `Autoscaler.Keda` / `Autoscaler.HPA` verbatim and emit a `ScaledObject` or `HorizontalPodAutoscaler` targeted at the component Deployment. The legacy `KedaConfig` path is deleted (along with its hardcoded SGLang default query); legacy `ScaleMetric` / `ScaleTarget` continue to drive the HPA fallback (default CPU=80% when neither legacy nor new field is set).

```go
// KEDA path; HPA mirrors this shape with autoscalingv2.HPA + Metrics/Behavior.
func buildScaledObject(meta metav1.ObjectMeta, k *v1beta1.KedaAutoscaler, minR, maxR int32, deployName string) *kedav1.ScaledObject {
    return &kedav1.ScaledObject{
        ObjectMeta: metav1.ObjectMeta{Name: utils.GetScaledObjectName(deployName), Namespace: meta.Namespace, Labels: meta.Labels},
        Spec: kedav1.ScaledObjectSpec{
            ScaleTargetRef:   &kedav1.ScaleTarget{APIVersion: "apps/v1", Kind: "Deployment", Name: deployName},
            MinReplicaCount:  &minR,
            MaxReplicaCount:  &maxR,
            Triggers:         k.Triggers,
            Advanced:         k.Advanced,
            PollingInterval:  k.PollingInterval,
            CooldownPeriod:   k.CooldownPeriod,
            IdleReplicaCount: k.IdleReplicaCount,
            Fallback:         k.Fallback,
        },
    }
}
```

### External mode: published target + `/scale` subresource

**Status field.** Every ISVC publishes `status.components.<comp>.scaleTargetRef`:

```yaml
status:
  components:
    engine:
      scaleTargetRef:
        group: apps
        kind: Deployment
        name: foo-engine
      autoscaler:
        managedBy: external
```

Populated unconditionally so operators can use it for user-managed autoscalers regardless of which class is configured.

**`/scale` subresource on InferenceService.** Add a kubebuilder marker on `InferenceService`:

```go
// +kubebuilder:subresource:scale:specpath=.spec.engine.minReplicas,statuspath=.status.components.engine.replicas,selectorpath=.status.components.engine.labelSelector
```

The subresource:
- **GET** returns current/desired replicas reported on `status.components.engine.replicas` (sum-equivalent for proportional mode where decoder/router are derived; engine is the source of truth).
- **PUT** writes to `spec.engine.minReplicas`. Notes:
  - This sets a **floor**, not an absolute count. The autoscaler still tracks metrics and can scale up beyond it.
  - For ISVCs whose autoscaler class is `keda` or `hpa`, the ScaledObject/HPA's `minReplicaCount` reflects the new floor on next reconcile.
  - For `external` mode, the user's autoscaler observes the new floor through the same `spec.engine.minReplicas` field and updates accordingly. If the user-managed scaler ignores the floor (writes directly to the Deployment's replicas), the floor still applies via the engine-component reconciler's standard replica-merge logic.
- **selector** comes from the engine Deployment's label selector, surfaced via `status.components.engine.labelSelector` (string form, per `/scale` subresource convention).

Per-component `/scale` subresources are explicitly out of scope for Alpha; users who need finer control should `kubectl scale deployment/<isvc>-{decoder,router}` directly.

**RBAC.** The `--scale` verb is added to OME's manager role for InferenceService — required for the apiserver to authorize the subresource handler.

### Coordinated scaling for PD-disaggregated services

New reconciler under `pkg/controller/v1beta1/inferenceservice/reconcilers/scaling_policy/`:

```go
type ScalingPolicyReconciler struct {
    Client client.Client
    Log    logr.Logger
}

func (r *ScalingPolicyReconciler) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) error {
    p := isvc.Spec.ScalingPolicy
    if p == nil || p.Mode != v1beta1.ScalingProportional || p.Proportional == nil {
        return nil
    }

    anchor := p.Proportional.Anchor
    anchorReplicas, err := r.observedReplicas(ctx, isvc, anchor)
    if err != nil { return err }

    for follower, ratio := range p.Proportional.Ratios {
        if follower == anchor { continue }
        target := computeFollowerReplicas(anchorReplicas, ratio)
        if err := r.scaleFollower(ctx, isvc, follower, target); err != nil {
            return err
        }
    }
    return nil
}
```

`observedReplicas` reads `status.replicas` from the anchor's child Deployment. `computeFollowerReplicas` is `ceil(anchorReplicas * ratio.AsApproximateFloat64())`. `scaleFollower` writes through the follower component Deployment's `/scale` subresource (HTTP-level scale write, not a spec patch — keeps replica accounting clean).

The reconciler is wired as a watcher on Deployment status changes (only). It does not poll on a timer; it does not write status itself.

```mermaid
sequenceDiagram
    autonumber
    participant K as KEDA / HPA
    participant E as Engine Deployment
    participant Co as Scaling-policy<br/>coordinator
    participant D as Decoder Deployment
    K->>E: scale to N pods (anchor)
    E-->>Co: status.replicas = N (watch)
    Co->>Co: target = ceil(N × ratio)
    Co->>D: PUT /scale {replicas: target}
    D-->>D: spec.replicas = target
    Note over K,D: Coordinator never touches ScaledObject/HPA — followers stay autoscaler-free.
```

Validation gates the spec at the webhook:
- `Mode == proportional` requires `Proportional.Anchor` ∈ existing components.
- Followers in `Ratios` must not have `Autoscaler.Class ∈ {hpa, keda}` — they'd fight the coordinator. `none` and `external` are allowed (external users opting out of OME-managed scaling on a follower is legal; they own the outcome).

### Status surface

`ComponentStatusSpec` gains `Autoscaler *ComponentAutoscalerStatus`, `ScaleTargetRef`, `Replicas`, `LabelSelector`:

```go
type ComponentAutoscalerStatus struct {
    Class           AutoscalerClass    `json:"class"`
    ManagedBy       string             `json:"managedBy"` // "ome" or "external"
    CurrentReplicas int32              `json:"currentReplicas,omitempty"`
    DesiredReplicas int32              `json:"desiredReplicas,omitempty"`
    LastScaleTime   *metav1.Time       `json:"lastScaleTime,omitempty"`
    Conditions      []metav1.Condition `json:"conditions,omitempty"`
    // +kubebuilder:validation:Enum=isvc;runtime;legacy;default;external
    SpecSource      string             `json:"specSource"`
}
```

Populated each reconcile from the live HPA (or ScaledObject's internal HPA for KEDA). `SpecSource` records which layer of resolution won: `isvc` block / `runtime` defaults / `legacy` fields / controller `default` / `external`.

### Webhook validation

In `pkg/webhook/admission/isvc/`:

- **Class consistency**: `Autoscaler.Class == keda` requires `Autoscaler.Keda.Triggers` to be set; `Autoscaler.Class == hpa` falls back to default CPU=80% if `Autoscaler.HPA.Metrics` is empty.
- **Annotation-only `keda`**: `ome.io/autoscalerClass: keda` without an `Autoscaler.Keda` block on a component is rejected. There is no longer a default KEDA trigger — the hardcoded SGLang query was removed with `KedaConfig`.
- **`scalingPolicy.proportional` rules**: anchor must be one of the components present in the spec; followers in `Ratios` must not have `Autoscaler.Class ∈ {hpa, keda}`.
- **Bounds sanity**: `MinReplicas <= MaxReplicas` (existing); `IdleReplicaCount < MinReplicaCount` per KEDA's own rules.
- **MinItems on KEDA triggers**: empty triggers list rejected with a clear message.
- **Annotation legacy use**: when `ome.io/autoscalerClass` annotation is present AND any component has an `Autoscaler` block, reject with a "remove the annotation; per-component class wins" message. Mixing them deterministically silently is too error-prone.
- **ServingRuntime-side autoscaler validation**: every rule above that applies to an ISVC's `Autoscaler` block also applies to `ServingRuntime` / `ClusterServingRuntime` admission — class consistency, MinItems on triggers, KEDA/HPA mutual exclusion, `scalingPolicy.proportional` shape. Runs against the same validation helper. Webhook lives at `pkg/webhook/admission/servingruntime/`.

### Backwards compatibility

Two legacy shapes continue to work; one is removed:

| Legacy | Behavior |
|---|---|
| `ome.io/autoscalerClass: hpa` (or unset) + no other fields | Default HPA on CPU=80%. Identical to today. |
| `ome.io/autoscalerClass: external` | OME does not generate HPA or ScaledObject. Same as today, plus `status.scaleTargetRef` populated. |
| ~~`ome.io/autoscalerClass: keda` + `spec.<comp>.kedaConfig`~~ | **Removed in this OEP.** Migrate to `spec.<comp>.autoscaler.keda.triggers`. The `KedaConfig` Go type, the field, and the hardcoded SGLang default query are deleted. |

**Migration from `kedaConfig` → `autoscaler.keda`:**

```yaml
# Before
spec:
  engine:
    kedaConfig:
      scalingMetric: PromMetric
      customPromQuery: "avg(sglang:num_requests_waiting)"
      scalingThreshold: "20"
      promServerAddress: http://prometheus.monitoring:9090

# After
spec:
  engine:
    autoscaler:
      class: keda
      keda:
        triggers:
          - type: prometheus
            metadata:
              query: "avg(sglang:num_requests_waiting)"
              threshold: "20"
              serverAddress: http://prometheus.monitoring:9090
```

Remaining legacy fields (`ScaleMetric`, `ScaleTarget`, annotation) stay through Alpha with deprecation warnings; removal no earlier than Beta+1.

### Failure modes

| Failure | Behavior |
|---|---|
| KEDA CRDs not installed on cluster | Webhook rejects `Autoscaler.Class == keda` with a clear "KEDA not available on this cluster" message; resolver suggests `class: hpa` or `class: external`. |
| User writes an invalid KEDA trigger (e.g., references a nonexistent TriggerAuthentication) | OME passes it through to KEDA; KEDA reports `ScaledObject.status.conditions` Ready=False; OME propagates that into `status.components.<comp>.autoscaler.conditions`. |
| User writes an HPA metric for a metric that doesn't exist in metrics-server / Prometheus Adapter | HPA reports `ScalingActive=False` with the upstream reason; propagated into status. |
| `external` mode + no user-created ScaledObject/HPA | Deployment runs at `spec.<comp>.minReplicas` forever. No scaling. OME emits a warning Event after 5 minutes of no observed scaler. |
| Proportional coordinator: anchor Deployment doesn't exist yet | Coordinator no-ops with debug log; will pick up on the next anchor-Deployment status event. |
| Proportional ratio that yields 0 followers | Coordinator floors at `spec.<follower>.minReplicas` (which is itself floored at 1 by default). |
| `/scale` PUT with `replicas: 0` | Sets `minReplicas: 0`. ISVC scale-to-zero must already be supported by the autoscaler in use (KEDA via `idleReplicaCount`; HPA does not natively scale to zero). |

### Test Plan

**Prerequisite refactor**: drop the synthetic Engine-wrapped `ComponentExtensionSpec` in `common/deployment_reconciler.go` so the autoscaler reconciler reads the actual component spec.

**Unit**: deepcopy for the new API types; `ResolveComponentAutoscaler` table tests covering every (runtime, isvc, legacy) combination; KEDA + HPA reconcilers verify verbatim pass-through; scaling_policy reconciler covers ratio math (floor / ceil / parity / sub-1) + anchor-missing no-op; webhook covers every rejection rule.

**Integration** (`tests/integration/singlecluster/controller/inferenceservice/`): KEDA multi-trigger pass-through (prometheus + cron) survives a round-trip through the apiserver; HPA external-metric likewise; per-component dispatch (engine on KEDA, decoder on HPA, router on external) coexist; `external` class publishes `scaleTargetRef` and emits no scaler object; proportional policy with ratio 1.0 and 0.25; `kubectl scale isvc/foo` writes `spec.engine.minReplicas`; runtime-inherited and ISVC-overridden autoscalers each surface the right `specSource`.

**Webhook integration**: every rejection path on both ISVC and ServingRuntime admission, against the same shared validation helper.

### Graduation Criteria

#### Alpha

- All new fields land in `v1beta1` behind an "alpha" feature gate documented in the CRD descriptions.
- KEDA/HPA pass-through reconcilers wired in.
- `/scale` subresource implemented and tested.
- `status.components.<comp>.{autoscaler, scaleTargetRef}` populated.
- Backwards compatibility verified — every existing ISVC shape produces identical output as before.
- Webhook validation in place.

#### Beta

- Proportional scaling coordinator wired in and tested at integration level.
- One full release cycle of operator feedback on the new fields.
- Status fields stable (no rename / restructuring) for at least one release.
- Deprecation warnings on legacy fields visible in status conditions.

#### Stable

- Two release cycles in Beta with no critical bugs.
- Removal path for remaining legacy fields (`ScaleMetric`, `ScaleTarget`, `ome.io/autoscalerClass` annotation) documented and announced. `KedaConfig` already removed in Alpha.
- Per-component `/scale` subresources considered (for ratios that operators want to override mid-flight).

## Implementation History

- 2026-05-19: OEP drafted.
- TBD: API types added; `KedaConfig` field, type, and hardcoded SGLang query removed.
- TBD: KEDA + HPA pass-through reconcilers.
- TBD: ServingRuntime inheritance + `ResolveComponentAutoscaler` helper.
- TBD: `/scale` subresource + status fields.
- TBD: Webhook validation (ISVC + ServingRuntime).
- TBD: Proportional scaling coordinator.

## Drawbacks

- Adds three new types (`ComponentAutoscaler`, `KedaAutoscaler`, `HPAAutoscaler`) and a top-level `ScalingPolicy` — meaningful CRD growth on `InferenceService` and `ServingRuntime`-adjacent surface.
- KEDA/HPA pass-through couples OME's CRD to upstream type evolution. `kedav1.ScaleTriggers` schema changes propagate automatically when OME bumps the KEDA dep; this is mostly upside (new triggers work for free) but means OME consumers see KEDA type signatures in their CRDs.
- Two valid spec shapes (legacy `ScaleMetric`/`ScaleTarget` + new `Autoscaler` block) coexist on the HPA path through Alpha. Operators reading the API surface have to learn two patterns until removal. `KedaConfig` no longer has this problem — it's removed in this OEP.
- `/scale` subresource interpretation is opinionated — "scale the engine" is a defensible default but doesn't compose well with multi-component services. Operators who want per-component scale subresources have to wait for a follow-up.
- Proportional coordinator is a 4th reconciler on the ISVC critical path. Adds reconcile loop work; partially mitigated by the watch-status-only trigger.
- **Runtime-defined autoscaler defaults can drift silently across ISVCs.** When a ServingRuntime is reused across multiple ISVCs (less common in the 1:1 pairing pattern but legal), updating the runtime's `autoscaler` block changes scaling for every non-overriding ISVC on next reconcile. Documented behavior, not a bug; `status.components.<comp>.autoscaler.specSource == "runtime"` lets operators audit who's inheriting before they commit.

## Alternatives

### Keep the single-trigger KEDA shape; document patterns instead

Operators can write complex prometheus queries that combine signals into one number. Workable for simple cases; falls apart at scale: cron triggers can't be expressed as a query; external scalers (Kafka lag, RabbitMQ depth) can't either. The single-trigger ceiling is a real product limit, not a documentation gap.

### Generate ScaledObjects but allow user override via owner-ref handoff

OME generates a default ScaledObject; user can take ownership by setting an annotation; OME stops reconciling it. Looks clean; falls apart on conflict: who wins if user and controller both think they own it? Audit trail goes through annotations. Rejected in favor of explicit `external` mode + published `scaleTargetRef`.

### `kubectl scale` writes the Deployment directly

Skip the `/scale` subresource on InferenceService; tell users to scale the underlying Deployment. Works today. Rejected because (a) users expect the top-level CR to be the scale-of-record, (b) it leaks implementation detail (the `<isvc>-engine` Deployment naming convention is public API anyway, but it shouldn't be the *only* way to scale), (c) it precludes the proportional coordinator from being driven by an external HPA pointing at the ISVC.

### Single ISVC-level autoscaler that applies to all components

Simpler API. But every real PD-disaggregated user we've talked to wants per-component knobs: engine on KEDA queue-depth, decoder on GPU memory, router on plain CPU. Forcing a single class precludes the actual use case.
