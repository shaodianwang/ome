# OEP-0015.2: Metric-Gated Canary Promotion and Auto-Rollback

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Terminology](#terminology)
  - [The promotion decision point this plugs into](#the-promotion-decision-point-this-plugs-into)
  - [User stories](#user-stories)
- [Design details](#design-details)
  - [API: per-step gates with a shared Prometheus source](#api-per-step-gates-with-a-shared-prometheus-source)
  - [The Prometheus reader and the evaluator](#the-prometheus-reader-and-the-evaluator)
  - [The decision model: bake, sample, decide](#the-decision-model-bake-sample-decide)
  - [Bad versus can't-tell: breach, inconclusive, stall](#bad-versus-cant-tell-breach-inconclusive-stall)
  - [Sampling is the reconcile tick](#sampling-is-the-reconcile-tick)
  - [Connecting to Prometheus: address and auth](#connecting-to-prometheus-address-and-auth)
  - [Auto-rollback reuses the existing path](#auto-rollback-reuses-the-existing-path)
  - [Status, condition, and observability](#status-condition-and-observability)
  - [Edge cases](#edge-cases)
  - [Admission validation](#admission-validation)
  - [Interaction with the executor, coordination, and autoscaling](#interaction-with-the-executor-coordination-and-autoscaling)
- [Test plan](#test-plan)
- [Graduation criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** OEP-0015.1 shipped the canary executor, but its only unattended
promotion mode — `Auto` — advances on a **timer, not on health**. An unattended
`Auto` canary walks traffic to 100% on schedule even while it is serving
garbage. This OEP adds the third promotion answer the executor explicitly left a
hook for: **metric-gated promotion**, a controller-side path that queries
Prometheus on each step, advances *only while* operator-defined PromQL conditions
hold, and **auto-rolls-back** the moment they break past a budget. It is the
first controller→Prometheus *reader* in OME — today KEDA does all Prometheus
reading, and only for scaling; the OME controller forwards KEDA triggers verbatim
and otherwise just *emits* its own metrics. The feature reuses the executor's
step machine, the existing `ome.io/rollout-rollback` mechanics, and the bundled
Prometheus; it adds per-step gates with a shared canary-level Prometheus source, an
evaluation loop driven off the reconcile tick, and a handful of status fields.
Scope: **OMENative canary groups only** (`spec.rollout.groups[].canary`);
RawDeployment, coordination groups (BlueGreen/RollingUpdate/Sequential), and
Manual/Auto promotion are unaffected.

This OEP builds on the **OEP-0017** unified rollout types that are in tree today
(`spec.rollout.groups[].canary` → `GroupCanary`, `RolloutGroupStep`), not the
original OEP-0015 `CanarySpec` shape that OEP-0015.1's README still references.
The *concept* of the hook is unchanged; only the type names moved.

## Motivation

The failure mode, first and plainly: **a silent quality regression reaching 100%
of users because the only safety the executor has is pod-readiness, and the
regression does not crash the pod.** A bad checkpoint, a sampling-parameter
change, a quantization regression — these degrade output quality (wrong answers,
higher latency, KV-cache thrash) while the container stays `Ready`. OEP-0015.1's
capacity gate waits for `Ready` and then shifts traffic; `Auto` then advances on
`Pause.Duration`. Neither looks at whether the canary is actually *good*.
OEP-0015.1 says this itself, in its own Drawbacks: *"Timed Auto is not safety …
an unattended `Auto` canary will happily advance into a regression."* This OEP is
the answer it named.

The metric *source* already exists, but the *reader* does not. OME bundles a
single-replica Prometheus (`charts/ome-resources/templates/prometheus/`, off by
default, 30m retention, shipped so KEDA works out of the box) and integrates the
prometheus-operator (ServiceMonitor/PodMonitor). What is missing is entirely on
the controller side: there is **no PromQL query client anywhere in the
controller**, no Prometheus `serverAddress` constant, and no evaluate path —
`keda_reconciler.go` copies the user's triggers into the `ScaledObject` verbatim
and lets KEDA do the querying. So metric-gating is genuinely net-new work: a
condition API plus a controller-side query-and-evaluate loop. That is precisely
why OEP-0015.1 deferred it to its own design rather than bolting it on.

The hook is ready. OEP-0015.1 made promotion *"a single decision point —
'advance this step?'"* with Manual and Auto as two answers, *"so a metric-gated
mode is a third … and plugs in here without changing capacity, traffic, or
status."* This OEP fills that slot and nothing else: the capacity math, the
traffic weights, the per-revision Services, the drain/scale-down, the rollback
mechanics all stay as they are.

### Goals

1. **Gate advancement on metric health.** Under metric-gated promotion, a step
   advances only while *all* operator-defined metric conditions hold over a bake
   window. *Rationale:* this is the missing "is the canary actually good?" check
   that turns a timed shift into a guarded one.
2. **Auto-rollback on regression.** When conditions break past a failure budget,
   abort the canary automatically — reusing the existing
   `ome.io/rollout-rollback` path, not a second revert mechanism. *Rationale:* a
   gate that can only *stop* advancing still leaves bad traffic flowing at the
   current weight; the safety net has to be able to *retreat*.
3. **Take user-supplied PromQL, not fixed metric names.** *Rationale:* OME does
   not author serving metrics — they come from the model server (vLLM/SGLang:
   `vllm:time_to_first_token_seconds`, `vllm:num_requests_running`,
   `vllm:gpu_cache_usage_perc`, …) and live in out-of-tree runtime catalogs.
   Hardcoding names would couple this feature to one runtime and break on the
   next.
4. **Work against the bundled *or* an external Prometheus.** `serverAddress` is
   optional (defaults to the bundled service), overridable for a real external
   Prometheus, with bearer-token auth. *Rationale:* the bundled Prom is a
   30m-retention, single-replica, `emptyDir` store explicitly labeled "KEDA input
   only" — fine for a short canary, a shaky single point of truth for a
   safety-critical rollback in production.
5. **Distinguish "bad" from "can't tell."** A metric *breach* (Prometheus
   answered, value out of bounds) drives rollback; a *transport/no-data* error
   (Prometheus unreachable, empty result) must **not** be read as "canary is bad."
   *Rationale:* auto-reverting healthy canaries every time monitoring blips is its
   own outage.
6. **Be observable and crash-safe.** Surface a status condition, per-metric
   results, and metrics; keep all decision state in status and drive sampling off
   the reconcile tick so a controller restart resumes mid-analysis without a
   double-count. *Rationale:* a safety feature whose own state evaporates on a
   pod restart is not a safety feature.

### Non-Goals

1. **No new traffic or capacity mechanics.** This OEP changes *only* the
   promotion decision (`shouldAdvance` → `evaluateStep`). Capacity, per-revision
   Services, HTTPRoute weights, drain, and scale-down are OEP-0015.1 verbatim.
2. **Not an Analysis CRD in v1.** Argo Rollouts / Flagger model analysis as a
   separate `AnalysisTemplate`/`AnalysisRun` resource. We keep it **inline** on
   the step (`RolloutGroupStep.Analysis`) and factor the evaluator behind a Go
   interface so a CRD wrapper can be added later *without* a rewrite. Deferred,
   not rejected — the inline form matches the existing inline `steps[]` aesthetic
   and ships smaller.
3. **No expression engine.** Each metric is `query + operator + threshold`, not an
   arbitrary `successCondition: "result < 0.05 && p99 < 2"` expression. A scalar
   comparison covers the common cases without an evaluator/sandbox; richer
   expressions are a follow-up.
4. **No mTLS / full TLS client config in v1.** Bearer token (for Prom behind
   oauth2-proxy/Thanos) and plain HTTP (bundled). CA / client-cert configuration
   is deferred. *Calibration:* flag if an mTLS-only external Prometheus is a
   day-one requirement; it would pull TLS config forward.
5. **OMENative canary only.** RawDeployment has no canary executor to gate;
   coordination groups (BlueGreen/RollingUpdate/Sequential) are a different
   progression with no per-step traffic dwell to analyze.
6. **Does not change the PD same-revision pairing story.** That remains
   OEP-0015.1's tracked dependency on a router contract. Analysis gates on
   whatever PromQL the operator writes; it neither fixes nor worsens internal
   pairing.

## Proposal

metric-gated promotion activates a per-step **analysis window**. After the
executor has applied the step's capacity and traffic and the canary pods are
`Ready` (all unchanged from 0015.1), instead of holding for a manual annotation
or a bare timer, the controller **samples** the operator's metrics every
`interval`. It advances when the metrics have held for the step's bake time, and
it rolls back when failing samples accumulate past `failureLimit`. Manual
`ome.io/rollout-promote` / `ome.io/rollout-rollback` remain as overrides at all
times.

### Terminology

- **Analysis** (`RolloutAnalysis`): a step's checks + policy — *what* "healthy"
  means. Set per step; the shared metrics source (where to query) is
  `GroupCanary.Prometheus`.
- **Metric** (`AnalysisMetric`): one success condition — a templated PromQL
  `query`, a comparison `operator` (`LT|LTE|GT|GTE`), and a `threshold`. A metric
  *passes* when the query result satisfies `result <op> threshold`.
- **Sample**: one evaluation cycle — run *all* metrics once. A sample **passes**
  only if *every* metric passes (AND semantics); any failing metric makes it a
  **failing sample**.
- **Inconclusive**: a sample the controller could not complete — Prometheus
  unreachable, query error, or empty result set. Distinct from a failing sample
  (see [Bad versus can't-tell](#bad-versus-cant-tell-breach-inconclusive-stall)).
- **Bake time**: the step's `pause.duration` reinterpreted under Analysis as the
  *minimum* dwell before advancement. With no `pause`, the step still requires one
  passing sample before advancing (an analysis step is never instantaneous — that
  would defeat the gate).
- **`failureLimit`**: the count of failing samples, **per step**, that triggers
  auto-rollback. Resets to zero on each advance.
- **Stall**: a run of inconclusive samples long enough that the controller gives
  up *deciding* — it has been unable to read health for the stall timeout. Stall
  ≠ failure (see below).

### The promotion decision point this plugs into

OEP-0015.1's `canary/promotion.go` exposes `shouldAdvance() bool`, dispatched on
the promotion mode. This OEP generalizes the return into a three-valued decision
so the same seam can also *retreat*:

```
type StepDecision int
const ( Hold StepDecision = iota; Advance; Rollback )

func evaluateStep(...) StepDecision
```

Manual and Auto only ever return `Hold` or `Advance` — their behavior is
byte-for-byte unchanged. The `Analysis` branch is the only producer of
`Rollback`. The reconciler's existing `capacity → traffic → decide` loop is
otherwise untouched; `Rollback` routes into the `reconcileRollback()` the manual
annotation already calls.

### User stories

- **Story 1 — gate + auto-rollback (the headline).** An operator ships a new
  checkpoint with metric-gated promotion, an `error-rate ≤ 5%` metric, `interval:
  1m`, `failureLimit: 3`, and steps at 10% / 50% / 100% traffic with 5m/10m
  pauses. The canary holds at 10%, samples error-rate each minute; three bad
  minutes at any step abort it back to stable automatically. A clean run advances
  through each bake window to 100%.
- **Story 2 — validate at capacity, zero traffic, then gate.** First step
  `{capacity: "100%", traffic: 0}` with an analysis metric on a synthetic-probe
  query: bring the canary fully up and smoke-test it at zero exposure, advancing
  only when the probe passes, before any real traffic.
- **Story 3 — Prometheus blips mid-rollout.** The bundled Prom restarts during a
  bake window. Samples go inconclusive; the canary **holds** (does not roll back)
  and surfaces `RolloutAnalysisHealthy=Unknown`. If Prom stays down past the stall
  timeout, the step parks in `Failed` with traffic held — the operator decides.
- **Story 4 — operator overrides the gate.** Analysis is mid-bake but the operator
  is confident; `ome.io/rollout-promote=<hash>` force-advances one step. Or they
  see something the metric misses and `ome.io/rollout-rollback` aborts immediately.

## Design details

### API: per-step gates with a shared Prometheus source

In `pkg/apis/ome/v1beta1/rollout_types.go` — the gate is a PER-STEP property. Each
step resolves its own gate: `analysis` (metric-gated), a timed `pause`
(`pause.duration`), a bare `pause` (manual), or neither (immediate). The Prometheus
connection is canary-level (`prometheus`), shared by every analysis step; a step's
`analysis` carries only the checks + policy.

```go
type GroupCanary struct {
    Steps                 []RolloutGroupStep  `json:"steps,omitempty"`
    Prometheus            *AnalysisPrometheus `json:"prometheus,omitempty"`  // shared metrics SOURCE (where + auth)
    ScaleDownDelaySeconds *int32              `json:"scaleDownDelaySeconds,omitempty"`
}

type RolloutGroupStep struct {
    Capacity intstr.IntOrString `json:"capacity"`
    Traffic  int32              `json:"traffic"`
    Pause    *RolloutPause      `json:"pause,omitempty"`    // bare = manual; +duration = timed (or bake under analysis)
    Analysis *RolloutAnalysis   `json:"analysis,omitempty"` // opt in to metric-gated; the CHECKS + policy (no source)
}

// RolloutAnalysis is a step's checks + policy. The metrics source is
// GroupCanary.Prometheus (shared), not here.
type RolloutAnalysis struct {
    Interval       metav1.Duration  `json:"interval"`                 // sample cadence
    InitialDelay   *metav1.Duration `json:"initialDelay,omitempty"`
    FailureLimit   int32            `json:"failureLimit"`             // +Minimum=1; failing samples/step before rollback
    OnInconclusive *OnInconclusive  `json:"onInconclusive,omitempty"` // Hold (default) | Rollback
    Metrics        []AnalysisMetric `json:"metrics"`                  // +MinItems=1, +MaxItems=10; ALL must pass
}

type AnalysisPrometheus struct {
    ServerAddress string                    `json:"serverAddress,omitempty"` // default http://ome-prometheus.<ns>.svc:9090
    AuthRef       *corev1.SecretKeySelector `json:"authRef,omitempty"`       // bearer token for external Prom
    Headers       map[string]string         `json:"headers,omitempty"`       // e.g. X-Scope-OrgID for multi-tenant Prom
}

type AnalysisMetric struct {
    Name      string             `json:"name"`
    Query     string             `json:"query"`     // templated PromQL
    Operator  ComparisonOperator `json:"operator"`  // +Enum=LT;LTE;GT;GTE
    Threshold string             `json:"threshold"` // parsed as float64
}

type ComparisonOperator string // LT, LTE, GT, GTE
type OnInconclusive   string   // Hold (default), Rollback
```

A complete example:

```yaml
spec:
  rollout:
    groups:
      - components: [engine]
        canary:
          prometheus:                  # shared metrics SOURCE (where + auth)
            serverAddress: http://thanos.monitoring.svc:9090
          steps:
            - { capacity: "10%",  traffic: 10, pause: {} }          # manual gate (human eyeballs it)
            - { capacity: "50%",  traffic: 50,                      # metric-gated
                analysis: { interval: 1m, failureLimit: 3,
                            metrics: [ { name: error-rate, operator: LTE, threshold: "0.05",
                                         query: 'sum(rate(http_requests_total{service="{{.CanaryService}}",code=~"5.."}[1m])) / sum(rate(http_requests_total{service="{{.CanaryService}}"}[1m]))' } ] } }
            - { capacity: "80%",  traffic: 80,                      # metric-gated; stricter
                analysis: { interval: 1m, failureLimit: 1,
                            metrics: [ { name: p99, query: '...', operator: LTE, threshold: "2" } ] } }
            - { capacity: "100%", traffic: 100 }                    # advance immediately
```

**Query templating.** Each `query` is rendered with the same revision context the
executor already computes for per-revision Services: `{{.Namespace}}`,
`{{.ISVCName}}`, `{{.Component}}`, `{{.CanaryService}}` (=
`<isvc>-<component>-rev-<hash>`), `{{.StableService}}`, `{{.CanaryRevision}}`,
`{{.StableRevision}}`. A query must reduce to a single value; a multi-series
result is reduced by **max** (the most conservative reading — the worst pod).

**One deliberate simplification, called out so it is not mistaken for an
oversight.** The metric value lands in status as a **string**, not a float —
Kubernetes API convention discourages float fields; the value is display-only.

### The Prometheus reader and the evaluator

A new subpackage
`pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis/`
— the first controller→Prometheus reader in OME. Two units behind interfaces, so
reconciler tests stay hermetic with a fake:

```go
// Querier — instant PromQL, reduced to one scalar. The only thing that does HTTP.
type Querier interface {
    Query(ctx context.Context, promQL string) (float64, error) // ErrNoData on empty vector
}

// Evaluator — render queries, run them, apply operator/threshold, AND the verdicts.
type Evaluator interface {
    Evaluate(ctx context.Context, a *v1beta1.RolloutAnalysis, tc TemplateContext) (Result, error)
}
type Result struct {
    Passed  bool
    Metrics []MetricResult // {Name, Value, Threshold, Operator, Passed, Err}
}
```

The `Querier` impl wraps `github.com/prometheus/client_golang/api` +
`api/prometheus/v1` (the `api` subpackage and `common/model` are added to
`go.mod`; `client_golang` itself is already vendored for metric *export*). It runs
an instant query at `time.Now()`, then reduces `model.Value`: a `*model.Scalar` →
its value; a `model.Vector` → the max sample, or `ErrNoData` if empty; a
`model.Matrix` → an error (instant queries should not return one).

### The decision model: bake, sample, decide

Under metric-gated promotion, the step loop is unchanged through capacity and
traffic; only the decide stage differs. Let `entered = StepEnteredTime`,
`bake = pause.duration` (zero if unset):

1. **Warm up.** Before `entered + initialDelay`, return `Hold` and requeue. No
   sampling during warmup — the new pods need a moment before their metrics mean
   anything.
2. **Sample (throttled).** Sample only if `now - LastEvaluationTime >= interval`
   (see [Sampling is the reconcile tick](#sampling-is-the-reconcile-tick)).
   Run the evaluator; record `MetricResults`, stamp `LastEvaluationTime`.
3. **Decide:**
   - **Failing sample** → `AnalysisFailedChecks++`. If `>= failureLimit` →
     `Rollback`. Else `Hold`.
   - **Passing sample** → if `now - entered >= bake` → `Advance`; else `Hold`
     (keep baking). A passing sample does **not** reset `AnalysisFailedChecks`
     (the budget is cumulative *per step* — see
     [Edge cases](#edge-cases)).
   - **Inconclusive sample** → see the next section.
4. **Advance** resets `AnalysisFailedChecks = 0`, clears `MetricResults`,
   restamps `StepEnteredTime`, and increments `CurrentStep` — the existing
   `advanceStep()`, plus the analysis reset.

### Bad versus can't-tell: breach, inconclusive, stall

This is the load-bearing safety distinction, so it gets its own section. The
controller must not confuse "the canary is bad" with "I can't see the canary."

| Sample outcome | Meaning | Counts toward `failureLimit`? | Action |
|---|---|---|---|
| **Failing** | Prometheus answered; a metric is out of bounds | **Yes** | `Hold`, then `Rollback` at the limit |
| **Passing** | Prometheus answered; all metrics in bounds | No | `Hold` until bake elapses, then `Advance` |
| **Inconclusive** | Prom unreachable / query error / empty result | **No** (default) | `Hold` + requeue; `Stalled` → `Failed` past the stall timeout |

A **stall** is a run of inconclusive samples whose span exceeds the stall timeout
— reusing the existing `ome.io/rollout-ready-timeout` knob (default 15m),
measured from `LastConclusiveEvaluationTime`. On stall the step parks in
**`Failed`** with traffic **held at the current step**, a loud event, and
`RolloutAnalysisHealthy=Unknown/Stalled`. *Why `Failed`-and-hold rather than
rollback:* a monitoring outage is not evidence the canary is bad, and reflexively
reverting healthy canaries during Prom maintenance causes thrash; the operator
promotes or rolls back by annotation. This is the conservative default; the
`onInconclusive: Rollback` knob flips it to **fail-safe** (treat "can't tell" as
"assume bad," roll back) for operators who want that. Default is `Hold`.

> **Anticipated objection.** *"Holding on a Prom outage means a half-exposed
> canary sits there serving real traffic, unvalidated."* True — that is the cost
> of not trusting a blind signal. The mitigations: the hold is bounded (stall →
> `Failed`, not forever), it is loud (condition + event + metric), and operators
> who would rather retreat on uncertainty set `onInconclusive: Rollback`. We pick
> `Hold` as the default because the alternative — rolling back every healthy
> canary whenever Prometheus restarts — is the more common and more surprising
> failure.

### Sampling is the reconcile tick

There is **no goroutine and no in-memory ticker.** Holding under analysis returns
`ctrl.Result{RequeueAfter: interval}`; each reconcile while baking is one
potential sample. All state — `CurrentStep`, `AnalysisFailedChecks`,
`LastEvaluationTime`, `LastConclusiveEvaluationTime`, `MetricResults` — lives in
`status.canary`. *Failure mode guarded:* a controller restart mid-analysis would
lose an in-memory counter and silently reset the failure budget; status-resident
state resumes exactly where it left off.

The critical guard is the **throttle**: reconciles fire far more often than
`interval` for unrelated reasons (pod events, other ISVCs, resyncs). Sampling
must gate on `now - LastEvaluationTime >= interval`, or an event storm would run
the queries dozens of times a second and over-count failures into a spurious
rollback. *Anti-pattern named: the "event-storm over-count" — trusting reconcile
frequency as a clock.*

### Connecting to Prometheus: address and auth

`prometheus.serverAddress` is optional. When omitted it defaults to the bundled
service DNS, `http://ome-prometheus.<release-namespace>.svc:9090`. When the
bundled Prom is disabled (`prometheus.enabled=false`, the chart default), the
validator **requires** an explicit `serverAddress` — silently defaulting to a
service that does not exist would make every sample inconclusive and every
analysis stall.

Auth in v1 is **bearer token or none**. `authRef` points at a Secret key holding
a token; the `Querier` injects it via a custom `http.RoundTripper`
(`Authorization: Bearer …`). The bundled Prom is unauthenticated, so the common
case carries no `authRef`. This mirrors the shape (not the code) of KEDA's
`TriggerAuthentication`. TLS/mTLS is deferred (Non-Goal 4).

### Auto-rollback reuses the existing path

When `evaluateStep` returns `Rollback`, the reconciler calls the *same* internal
function `ome.io/rollout-rollback` already drives — the analysis engine is just
its second caller. The mechanics are OEP-0015.1's: set
`status.canary.RolledBackRevisionHash`, set canary `traffic → 0` (stable → 100%),
set `InferenceReplicaPacing.RollbackToRevision` to the stable hash so the IR rolls
every instance back (`EffectivePartition` already forces partition 0 when
`RolledBackRevisionHash != ""`), drain `ScaleDownDelaySeconds`, scale the canary
to zero, phase `RollingBack → RolledBack`.

The only genuinely new code is the trigger and a `cause` label on the rollback
metric. One requirement this surfaces: **capture the stable revision hash into
status when the canary begins**, so an auto-rollback that fires at the final step
still has a definite target even though the step's capacity is `100%`.

### Status, condition, and observability

New fields on `CanaryStatus` (`pkg/apis/ome/v1beta1/canary_status_types.go`), set
only under metric-gated promotion:

```go
AnalysisFailedChecks         int32                  // failing samples in current step (reset on advance)
LastEvaluationTime           *metav1.Time           // throttle anchor
LastConclusiveEvaluationTime *metav1.Time           // stall-timeout anchor
MetricResults                []AnalysisMetricResult  // +listType=map key=name, +MaxItems=10

type AnalysisMetricResult struct {
    Name, Value, Threshold string             // Value is a string, not a float (k8s API convention)
    Operator               ComparisonOperator
    Passed                 bool
    Message                string             // "no data", error text
    Time                   *metav1.Time
}
```

**No `RolloutPhase` enum change.** A baking step stays in the existing **`Paused`**
phase; analysis detail rides on a new ISVC status **Condition**
`RolloutAnalysisHealthy` (`True`/`False`/`Unknown`, reasons `Passing |
FailingChecks | Inconclusive | Stalled | RolledBack`) plus events on each
transition (failing sample, rollback triggered, stall). Terminal mappings are
unchanged: breach → `RollingBack → RolledBack`; stall → `Failed`.

Metrics extend the emit-only `canary/metrics.go`:
`ome_canary_analysis_evaluations_total{result}`,
`ome_canary_analysis_metric_value{metric}` (gauge),
`ome_canary_analysis_failed_checks` (gauge vs limit), and a `cause="analysis|manual"`
label on the existing `ome_canary_rollback_total`. Closing the loop: the bundled
Prom already scrapes the controller's `/metrics`, so the gate is itself alertable.

### Edge cases

| Case | Behavior |
|---|---|
| **New revision pushed mid-bake** | A new `UpdateRevision` supersedes the in-flight canary: restart at `CurrentStep=0`, new `CanaryRevisionHash`, and **reset all analysis status** (`AnalysisFailedChecks=0`, clear `MetricResults`, clear eval timestamps). The superseded canary is not rolled back — it is replaced. |
| **Rollback is terminal** | Once `RolledBackRevisionHash` is set, sampling stops (`evaluateStep` returns a terminal `Hold`). No double-rollback; recovery is a new spec — matches existing behavior. |
| **Final step is analyzed too** | Analysis runs on the last step (validate at 100% before declaring `Stable`); once advanced past it, the done-sentinel (`CurrentStep == len(Steps)`) makes analysis inert. |
| **Point of no return** | After the final step passes and the stable revision drains (`ScaleDownDelaySeconds`), rollback is physically impossible — the old pods are gone. The safety window is "until stable is drained"; after that, recovery is roll-*forward*. Documented loudly, not silently. |
| **Manual override precedence** | Annotations beat analysis: `paused` (freeze sampling) > `rollback` > `promote` (force-advance mid-bake) > analysis decision. |
| **Counter is cumulative per step** | `AnalysisFailedChecks` accrues within a step and resets on advance. Not consecutive (a flapping canary would never trip), not whole-rollout (late steps would inherit early flakiness). The budget is "tolerate up to `failureLimit` bad checks *at this traffic level*." |

### Admission validation

In `pkg/validation/canary.go`, additive to OEP-0015.1's rules:

- each analysis step's config is complete and well-formed: at least one metric, a
  numeric `threshold`, a valid `operator`, `interval > 0`, `failureLimit >= 1`.
- the metrics source (`prometheus`) is environmental — an unreachable or
  unconfigured source surfaces at runtime as an inconclusive sample, not at
  admission (the pure validator cannot see whether the bundled Prometheus is on).
- All OEP-0015.1 rules unchanged (final step `traffic: 100`, non-decreasing
  weights, `traffic > 0 ⇒ capacity > 0`).

### Interaction with the executor, coordination, and autoscaling

- **Executor (OEP-0015.1):** Analysis is purely the promotion decision; capacity,
  traffic, drain, scale-down, rollback mechanics are reused unchanged.
- **Coordination:** unaffected — analysis is a property of a `canary` progression;
  BlueGreen/RollingUpdate/Sequential groups have no per-step traffic dwell and
  ignore it.
- **Autoscaling (OEP-0013):** inherits OEP-0015.1's behavior — replica counts
  during a canary follow the step's `capacity`, not demand; the same
  autoscaler-suspend seam applies. Analysis does not touch scaling.

## Test plan

**Unit (hermetic, fake `Querier`):**
- Evaluator: operator/threshold per type; AND across metrics; vector→max
  reduction; `ErrNoData` → inconclusive; query template rendering with a populated
  `TemplateContext`.
- Decision table (`evaluateStep`): passing-before-bake holds; passing-after-bake
  advances; failing accrues and rolls back exactly at `failureLimit`;
  inconclusive holds and does not count; stall → `Failed`; `onInconclusive:
  Rollback` flips inconclusive to rollback; cumulative-per-step reset on advance;
  manual override precedence (paused > rollback > promote > analysis); the
  Interval throttle suppresses over-counting under rapid reconciles.

**Integration (envtest):** a canary with metric-gated promotion and a fake/stub
metric source progresses through bake windows on passing samples; auto-rolls-back
on sustained failing samples; `RolloutAnalysisHealthy` and `status.canary` reflect
each transition; a new revision mid-analysis resets cleanly.

**KIND (real kubelet) — and the honest hard part.** End-to-end metric-gating
needs a *real* metric source the test can move. The fake-serving image
(`tests/integration/kindutil`) exposes no `/metrics` today, so this OEP must
extend it with a **tunable `/metrics` endpoint** (an env/annotation knob to make
it report healthy or regressed) and stand up a minimal Prometheus (the bundled
chart Prom, or a scrape-only stub) in the KIND cluster. With that: a healthy
canary advances through its steps; a canary flipped to "regressed" auto-rolls-back
within `failureLimit × interval`; Prometheus taken down mid-bake parks the step in
`Failed` (stall), and `onInconclusive: Rollback` instead reverts it. This is more
scaffolding than any prior canary KIND spec; it is called out as real work, not
assumed free.

## Graduation criteria

- **Alpha:** metric-gated promotion for single-component canaries — the evaluator,
  the Prometheus reader (bundled + external, bearer auth), the decision model,
  auto-rollback, the breach/inconclusive/stall distinction, status + condition +
  metrics, admission validation, unit + envtest. PD supported on the same terms as
  OEP-0015.1 (capacity + external traffic; pairing caveat unchanged).
- **Beta:** TLS/mTLS auth; the tunable-metrics KIND e2e green and in CI's soft-fail
  lane; soak in a real environment with a real model server's metrics. Possibly
  the Analysis-CRD wrapper if demand appears.
- **Stable:** defaults and timeouts validated against production canaries; the
  `onInconclusive` default reaffirmed or changed based on operator experience.

## Drawbacks

- **An HTTP dependency in the reconcile path.** The controller now makes outbound
  calls to Prometheus during reconciles. Mitigated by the throttle, short query
  timeouts, and treating transport errors as inconclusive (not fatal) — but it is
  a new failure surface the controller did not have.
- **Bundled-Prom retention bounds the window.** 30m retention means analysis
  queries can only look back ~30m; long-baseline comparisons need an external
  Prometheus. Documented; the validator nudges toward `serverAddress` for serious
  use.
- **PromQL is an operator footgun.** A wrong query (typo'd label, wrong unit) can
  pass a bad canary or roll back a good one. Mitigated by surfacing every metric's
  rendered value and verdict in status, but the operator still owns query
  correctness — there is no way to validate a PromQL's *semantics* at admission.
- **Point of no return after drain.** Auto-rollback cannot recover a regression
  caught after the stable revision is gone. Inherent to any progressive rollout;
  documented, not solved.

## Alternatives

1. **Analysis CRD from the start (Argo/Flagger model).** A reusable
   `AnalysisTemplate` referenced by the canary. *Tradeoff:* more API surface and a
   second resource's lifecycle for the same v1 outcome; the inline form ships
   smaller and the evaluator interface leaves the CRD as a clean later addition.
   Deferred, not rejected.
2. **Expression engine (`successCondition`/`failureCondition`).** Arbitrary
   boolean expressions over query results. *Tradeoff:* an evaluator/sandbox to
   build and secure; `operator + threshold` covers the common cases now. Follow-up.
3. **External-driven via annotations (run Flagger alongside OME).** OME stays
   metrics-agnostic; an external analyzer drives `ome.io/rollout-promote` /
   `rollout-rollback`. *Tradeoff:* OME already owns the rollout state machine and
   bundles Prometheus, so outsourcing the decision ships a half-story ("our canary
   can't protect itself; go install another controller"). Rejected as the default;
   the annotation hook remains open for anyone who wants this.
4. **Reuse KEDA's Prometheus path.** *Tradeoff:* KEDA *scales* on a metric; it has
   no notion of gating a rollout step or triggering a revert. It cannot express
   "advance/rollback," so there is nothing to reuse beyond the dependency. Not
   viable.

## Open questions

1. **`onInconclusive` default** — `Hold` is chosen (don't revert on monitoring
   outages); revisit after real-canary experience. Some shops will want `Rollback`
   as the org default — should it be a chart-level default, not just per-analysis?
2. **Per-step analysis override** — v1 is canary-level only. Do operators need
   looser gates at 10% than at 50%? Left out as YAGNI; add `RolloutGroupStep.Analysis`
   if demand appears.
3. **Failure-budget scope** — cumulative *per step* is chosen over consecutive and
   whole-rollout. Revisit if per-step proves too lenient or too strict in practice.
4. **KIND metric injection** — bundled chart Prom vs. a scrape-only stub, and how
   the fake-serving image exposes a tunable `/metrics`. Needs a concrete choice
   before the e2e lands (TBD, owner: implementer).
5. **Baseline/comparison metrics** — gating on a canary-vs-stable *delta* (e.g.
   "canary error rate ≤ 1.1× stable") rather than an absolute threshold. The
   `{{.StableService}}` template var leaves the door open; an explicit
   comparison-metric shape is deferred.
