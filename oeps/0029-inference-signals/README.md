# OEP-0029: Runtime-declared inference signals for autoscaling

> **Status: provisional.** Nothing here is built. File and line references are
> against `main` at 2720a40b (2026-09-22) and will drift.

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
- [Terminology](#terminology)
- [Goals and Non-Goals](#goals-and-non-goals)
- [Proposal](#proposal)
- [API](#api)
- [Design Details](#design-details)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** the ServingRuntime gains one map, `spec.signals`, from a signal
name to a complete Prometheus expression with one placeholder,
`{{ .Selector }}`, that OME fills with the pod selector. AutoscalerPolicy
gains a second shape that names a signal and a target and contains no PromQL.
The service owner writes a policy reference and replica bounds, as today.
The rendered object is the KEDA Prometheus trigger OEP-0013.1 already
dispatches; nothing downstream of the render changes. The chart ships three
policies, one per conventional signal name, and they work on any runtime
that declares that name.

## Motivation

The autoscaling path is complete and empty. Complete: a policy renders into a
KEDA trigger (`pkg/autoscalerpolicy/render/render.go:139-217`), the trigger is
copied verbatim onto the ScaledObject
(`reconcilers/keda/keda_reconciler.go:128-135`), the ScaledObject targets the
InferenceReplica's scale subresource
(`reconcilers/autoscaler/component_dispatch.go:288-292`), the Prometheus
address comes from an operator-bound provider name with no in-code default
(`controllerconfig/metricproviders.go:73-75`), and a broken policy holds the
last scaler instead of falling to the CPU default
(`reconcilers/autoscaler/resolve.go:97-105`). Empty: no object in the tree says
what a model server's queue depth is *called*. The chart ships no
AutoscalerPolicy (no template under `charts/ome-resources/templates` mentions
the kind); the one sample, `config/samples/autoscalerpolicy/policy.yaml`,
scales to max on any activity, not on a signal. Runtime metric names appear
only in design documents (`oeps/0013.1-autoscaler-policy/README.md:189`,
`oeps/0015.2-canary-analysis/README.md:98-100`) and in one internal dashboard
file (`charts/muse/files/metrics-queries.json:41,89,97`), which alone knows
that SGLang calls cache use `sglang:token_usage` and vLLM calls it
`vllm:kv_cache_usage_perc`. The template variables a policy may use are
exactly six identity and bounds values (`render.go:17-25`); a metric name is
not among them.

So today every fleet that wants to scale on queue depth writes the vendor's
metric name, the aggregation, and the label matchers into a policy by hand,
once per runtime family — OEP-0013.1's motivation (24 services, ~1,157 lines
of near-identical KEDA blocks) one level up.

**The failure the obvious split produces.** The first design considered
putting the metric *name* on the runtime and the PromQL *around* it in the
policy (`{{ .Metrics.QueueDepth }}` inside a shared query). The owner
rejected it for a concrete outage shape, not taste. A name and its
aggregation are one fact: queue depth is a gauge you `sum`; time to first
token is a histogram read through `histogram_quantile` over a `_bucket`
series; a gauge exported under a different label set matches none of the
policy's matchers. Under the split, the policy author guesses the aggregation
for a metric they cannot see. When the guess is wrong the query returns no
series, `ignoreNullValues: "false"` turns that into an error, KEDA trips its
fallback, and the service sits at `maxReplicas` with a green
`AutoscalerResolved` condition — the trap OEP-0013.1 named when it rejected
per-service parameters (`README.md:478-484`). Call it **wrong series, stuck
at max**: silent, a full fleet of accelerators, and uncatchable at admission
because both halves are individually valid. One place for the whole
expression removes the guess.

## Terminology

Each term is defined once; the consequence is part of the definition.

- **Component** — an InferenceService's engine, decoder, or router. Each
  resolves its own autoscaler (OEP-0013), so a signal is always per Component.
- **InferenceReplica (IR)** — the per-Component object that owns an
  OMENative Component's pods. Its `/scale` subresource is what the
  ScaledObject targets (`component_dispatch.go:256-258`).
- **Instance** — one IR replica: a single pod (runner `default`) or a leader
  pod plus worker pods (`inferencereplica_types.go:124-127,252-264`). The
  scale subresource counts Instances, not pods.
- **Signal** — a named, per-pod quantity an autoscaler can act on, e.g. queue
  depth. A name is a convention shared between runtime authors and policy
  authors; it is not an enum in the schema.
- **Expression** — the complete PromQL that computes a signal's fleet total
  for one Component, written by the runtime author, with exactly one
  `{{ .Selector }}` placeholder. If the expression is wrong the scaler is
  wrong; OME never looks inside it.
- **Selector** — the label matchers OME substitutes for the placeholder:
  namespace, service, and component. It scopes the expression to one
  Component's pods. Which pods are *serving* is not part of the selector; see
  serving gate.
- **Serving gate** — the `ome.io/serving` pod condition OMENative stamps and
  kubelet ANDs into PodReady (`workload/podreadiness/readiness.go:75,345-355`).
  The bundled Prometheus scrapes only PodReady pods
  (`charts/ome-resources/templates/prometheus/configmap.yaml:78-80`), so a pod
  behind the gate contributes no series. An external Prometheus without that
  keep rule counts gated pods.
- **Policy** — an AutoscalerPolicy (OEP-0013.1): namespaced, rendered per
  Component at reconcile time into a `ComponentAutoscaler` block.
- **Target** — the per-serving-pod value of a signal the scaler tries to hold,
  e.g. four queued requests per pod. It is a number, never a query.
- **Provider** — a logical Prometheus name a policy references and the
  operator binds to an address in `inferenceservice-config`
  (`metricproviders.go:11-15`).
- **Scale subresource** — the `/scale` endpoint HPA and KEDA write. Desired
  replicas flow *only* through it; this OEP adds no other writer.

## Goals and Non-Goals

### Goals

1. One place for the expression: the runtime. A new signal needs a runtime
   edit, never a CRD change.
2. A policy shape with no PromQL, rendered onto the existing KEDA trigger.
3. The service owner writes a reference and bounds, nothing else.
4. Three chart-shipped policies that work across runtimes.
5. Fail closed with a status that names the missing signal.

### Non-Goals

- No custom-metrics adapter and no HPA path for these signals. There is no
  adapter in the tree, so an HPA `Pods` metric on a serving signal is not real
  today; KEDA through Prometheus is the only working path and this OEP says
  so rather than sketching a second one.
- No metrics from the serving-agent sidecar. It watches a weights file and
  exposes no endpoint (`internal/ome-agent/serving-agent/serving_agent.go:42-77`);
  the runtime's own `/metrics` is what the bundled Prometheus and the generated
  PodMonitor already scrape.
- No inference of signals or targets from images, model size, or traffic.
- No change to the coordinator (OEP-0013.2): the anchor's scaler may be a
  signal policy, followers still have none.
- No canary-analysis change in this OEP; that is the named follow-up.

## Proposal

### 1. The runtime declares the expression

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata: { name: vllm-h100 }
spec:
  signals:
    queueDepth:         'sum(vllm:num_requests_waiting{{{ .Selector }}})'
    kvCacheUtilization: 'sum(vllm:kv_cache_usage_perc{{{ .Selector }}})'
    ttftSeconds: |
      sum(histogram_quantile(0.95,
        sum by (pod, le) (rate(vllm:time_to_first_token_seconds_bucket{{{ .Selector }}}[2m]))))
```

The runtime author owns the name *and* the aggregation, because only they
know that `vllm:num_requests_waiting` is a gauge, that the histogram lives
under a `_bucket` suffix, and which of two vLLM cache metrics their image
exports. The placeholder is the matcher list without braces, so an author can
add matchers beside it (`{{{ .Selector }}, runner!="worker"}`).

**The contract on an expression.** It must return one number: the *total*
over the Component's serving pods of the per-pod quantity the target bounds.
That is what makes the arithmetic in part 2 correct. A queue gauge is summed.
A utilization fraction is also summed, not averaged: four pods at 0.9 sum to
3.6, and 3.6 / 0.8 = 4.5 rounds up to the five pods that bring the average to
0.8. An `avg()` expression under the same arithmetic yields `ceil(avg /
target)`, which is 1 or 2 regardless of fleet size — a scaler that never
moves. Admission cannot see this (both parse), so the contract is documented
next to the field and in the conventional-name table below.

**Conventional names.** These three are documented so that the chart's
policies work on any runtime that declares them; they are not an enum.

| Name | Per-pod quantity | Unit | Target means |
|---|---|---|---|
| `queueDepth` | requests accepted but not yet running | requests | queued requests per serving pod |
| `kvCacheUtilization` | fraction of attention-cache memory in use | 0..1 | cache fraction per serving pod |
| `ttftSeconds` | time from request arrival to first output token, at a quantile the runtime picks | seconds | that quantile per serving pod |

### 2. The policy names a signal and a target

```yaml
apiVersion: ome.io/v1beta1
kind: AutoscalerPolicy
metadata: { name: queue-depth, namespace: llm-serving }
spec:
  class: KEDA
  signal:
    name: queueDepth
    target: "4"
    providerRef: { name: cluster-prometheus }
    fallback: { failureThreshold: 3, replicas: { fromComponent: MaxReplicas } }
    advanced:
      horizontalPodAutoscalerConfig:
        behavior: { scaleDown: { stabilizationWindowSeconds: 600 } }
```

`signal` is a one-of with the existing `keda` template; existing template
policies keep working unchanged and nothing migrates. "Bounds" in this shape
means the fallback replica count and the stabilization window — the two
settings that bound how far and how fast the scaler moves; replica min and
max stay on the service, as OEP-0013 placed them.

The render produces one `prometheus` trigger: `query` is the runtime's
expression with the selector substituted, `threshold` is the target,
`metricType` is `AverageValue`, `ignoreNullValues` is `"false"`, and the
provider binding injects `serverAddress` exactly as today
(`render.go:185-201`). With `AverageValue`, the HPA asks KEDA for
`ceil(total / target)` replicas (OEP-0013.1 `README.md:501-504`). With
`Value` it would compute `ceil((total / target) × readyPods)`, and
`readyPods` counts every pod behind the scale target — on a leader-plus-worker
Instance that includes worker pods that export no request metrics, so `Value`
over-scales by the worker count. `AverageValue` never multiplies by pods;
the render forces it and the policy cannot choose otherwise.

A policy whose signal the Component's runtime does not declare fails the
render into the existing fail-closed hold: the last-known-good scaler stands
and `AutoscalerResolved=False` carries a reason that names the signal
(Design Details).

### 3. The service writes a reference and bounds

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata: { name: llama-a, namespace: llm-serving }
spec:
  runtime: { name: vllm-h100 }
  engine:
    minReplicas: 1
    maxReplicas: 8
    autoscalerPolicyRef: { name: queue-depth }
```

No InferenceService field names a metric. The per-service target is Open
Question 1: OEP-0013.1 decided that policies carry no per-service parameters
(`README.md:478-484`); "same signal, different threshold" is the common case;
I recommend one optional `autoscalerPolicyRef.target` and mark the reversal as
the owner's call.

### 4. The chart ships three policies

`charts/ome-resources` renders `queue-depth`, `kv-cache-utilization`, and
`ttft-seconds` — one signal policy per conventional name. They are
runtime-agnostic because the expression arrives from the runtime at render
time. AutoscalerPolicy is namespaced and `ClusterAutoscalerPolicy` is a
reserved, admission-rejected kind (`autoscalerpolicy_types.go:308-313`), so
the chart takes a namespace list and renders each policy into each listed
namespace (Design Details).

### 5. Canary analysis later uses the same vocabulary

The analysis samplers carry their own PromQL per metric
(`AnalysisMetric.Query`, `rollout_types.go:382`; rendered by
`canary/analysis/template.go:28-38` with a revision-scoped context). A
follow-up, OEP-0029.1, lets a metric say `signal: ttftSeconds` and a
comparison instead of a query, with a revision-scoped selector. This OEP
names it and does not implement it.

## API

### `ServingRuntime.spec.signals` (also ClusterServingRuntime)

| Field | Type | Writer | Rules |
|---|---|---|---|
| `spec.signals` | `map[string]string` | runtime author | optional; keys and values validated per below |
| key | string | runtime author | a Prometheus label name (`[a-zA-Z_][a-zA-Z0-9_]*`, checked with `model.LabelName.IsValid` from `prometheus/common`, already a dependency at `go.mod:21`) |
| value | string | runtime author | contains the exact token `{{ .Selector }}` once; contains no other `{{`; with a sample selector substituted, parses with `promql/parser.ParseExpr` (already used at admission, `render/validate.go:170`; pinned by the `go.mod:187` replace) and has value type vector or scalar |

The placeholder is substituted by literal text replacement, not by
`text/template`. Two reasons: a runtime author then needs no escaping
knowledge (`{{{ .Selector }}}` is legal), and the expression never enters the
template allowlist machinery that guards policy templates. Rejecting any other
`{{` keeps the token namespace free for later placeholders.

Runtime inheritance merges the field key-wise: the resolver is a strategic
merge of child over parent (`pkg/runtimeinheritance/merge.go:16-52`), so a
child runtime adds or overrides individual signals and cannot remove a
parent's (nulls are stripped before the merge, `merge.go:37-40`). The merged
spec is what admission validates and what the render reads.

### `AutoscalerPolicy.spec.signal`

| Field | Type | Writer | Rules |
|---|---|---|---|
| `spec.signal` | `*SignalPolicy` | policy author | one-of with `spec.keda` (CEL); requires `class: KEDA` |
| `signal.name` | string | policy author | label-name shape, as above |
| `signal.target` | `resource.Quantity` string | policy author | `> 0` |
| `signal.providerRef` | `MetricProviderRef` | policy author | required (endpoint trigger) |
| `signal.fallback` | `FallbackTemplate` | policy author | optional; same type and rules as the template form |
| `signal.advanced`, `pollingInterval`, `cooldownPeriod` | as `KedaPolicyTemplate` | policy author | verbatim |

The policy webhook's sample render (`validate.go:140-158`) runs with a
synthetic signals map containing `signal.name`, so a signal policy admits
without a runtime in view; whether any runtime declares the name is a
render-time fact and surfaces on the consuming service, not on the policy's
`Ready` condition.

### `autoscalerPolicyRef.target` (Open Question 1)

| Field | Type | Writer | Rules |
|---|---|---|---|
| `autoscalerPolicyRef.target` | `resource.Quantity` string, optional | service owner | `> 0`; overrides `signal.target`; on a template-form policy the render holds with `PolicyInvalid` ("target override requires a signal policy") rather than being silently ignored |

### Who writes what, under OEP-0026

OEP-0026 splits an IR's fields into *user* fields (owned by whoever created
the IR) and *rollout-control* fields (a composer may write). Signals sit on
the runtime, which neither the ISVC nor the IR owns, so they are outside that
split: an IR that renders from `runtimeRef` (0026 section 5) reads the same
`signals` as an inline one, and a referenced IR's `autoscaler` — a user field
in 0026's table (`README.md:180`) — resolves through the same render. Whether
a standalone IR gets its own `autoscalerPolicyRef` is 0026's decision; this
OEP requires only that the render receive the *effective* runtime spec, which
both callers hold (`components/base.go:71`).

## Design Details

### Render pipeline

`render.Context` gains two inputs: `Signals map[string]string` (the effective
runtime's map) and `SelectorLabels` (the three label names, from config). The
caller already holds both: `resolveComponentAutoscaling` runs with
`b.Runtime` in scope (`components/base.go:156-165`) and passes them into
`PolicyResolver.Resolve` (`reconcilers/autoscaler/policy.go:188-197`). For a
signal policy, `renderKeda` is bypassed in favor of `renderSignal`, which
builds the single trigger described in Proposal 2 and then joins the existing
provider-binding and fallback code paths unchanged. The template form never
sees `Signals`: a template that wants a runtime signal is the two-places
design this OEP rejects.

### Selector construction

The selector is `namespace="<ns>",inferenceservice="<isvc>",component="<c>"`.
Values are a namespace, an ISVC name, and the component enum — DNS-safe
strings that cannot contain quotes or backslashes, so there is no escaping
layer, the same argument OEP-0013.1 made for its variables.

The three *label names* are not literals in Go. The bundled Prometheus
produces them by relabeling (`prometheus/configmap.yaml:119-129`); an
operator-scraped cluster produces them through `podMonitor.relabelings`,
which has deliberately no in-code default
(`controllerconfig/configmap.go:117-135`). Hardcoding the names in the render
would contradict that. They live in a `signals.selectorLabels` block of
`inferenceservice-config`, supplied by the chart to match its own relabel
config; with the block absent, a signal render holds with a detail saying so.
This is the one new config key.

### One scaler for canary and stable

A canary and its stable revision are Instances of one IR held apart by a
partition (`inferencereplica_types.go:147-153,272-278`), and the ScaledObject
targets that IR. The selector therefore carries no `revision_hash` matcher:
both revisions' pods feed one total, and the scaler sizes the IR for the
Component's whole load. Scoping by revision is what canary *analysis* wants
and autoscaling must not do; it is Open Question 2.

### Multi-pod Instances

Worker pods of a tensor-parallel server usually export no request metrics, so
they produce no series and a `sum` ignores them; the scale subresource counts
Instances, so `ceil(total / target)` is already in Instance units. A runtime
whose workers *do* export the gauge must exclude them in its expression; the
`ome.io/runner` pod label (`workload/query/labels.go:13`) is not in the
bundled relabel schema today, and adding it is a one-line chart change left
out of the first cut.

### Failure modes

| Failure | What happens | How it shows |
|---|---|---|
| Runtime lacks the signal | render error; hold on last-known-good | `AutoscalerResolved=False`, new reason `SignalMissing`, detail names the signal and the runtime |
| `selectorLabels` not configured | render error; hold | `AutoscalerResolved=False / PolicyInvalid`, detail names the config key |
| Provider not bound | as today (`render.go:189-192`); hold | `PolicyInvalid`, "provider not bound on this cluster" |
| KEDA CRDs absent | as today (`policy.go:201-204`); hold | `ClassUnavailable`; no HPA fallback exists for these signals |
| Expression names a metric the image does not export | query returns no series; `ignoreNullValues: "false"` makes that an error; after `failureThreshold` failures KEDA serves the fallback | the service sits at the fallback count with `AutoscalerResolved=True`; only the ScaledObject's own condition says why. Operators must read both; the runbook says so |
| Expression averages instead of totals | scaler pins at 1 or 2 | not detectable at admission; documented contract |
| Scrape lag | 15s scrape (`values.yaml:919`) + KEDA poll (30s default) + HPA sync (15s) is about one minute per decision; a `[2m]` rate window on TTFT adds most of its width | scale-out trails a burst by roughly a minute; scale-in is bounded by the policy's stabilization window, which the chart policies set |

### Chart packaging

Under `ome.autoscalerPolicy` in `charts/ome-resources/values.yaml`, a
`signalPolicies` block: `namespaces: []`, `providerRef`, and one entry per
conventional name holding `target` plus optional fallback and stabilization
settings. A `range` over `namespaces` emits each policy once per namespace;
an empty list emits nothing, which is the honest default for a namespaced
kind. A policy is rendered only when its `target` is set: the chart carries
no default threshold, because a queue depth right for one fleet is wrong for
another. The block is inert unless `autoscalerPolicy.enabled` is true, the
same value that installs the CRD and webhook (`values.yaml:174-178`).

## Test Plan

- **Unit.** `pkg/validation/servingruntime_test.go`: a table over key shape,
  placeholder count (zero, one, two), stray `{{`, unparsable PromQL, and a
  range-vector result. `render/render_test.go`: the signal form renders the
  expected trigger against a runtime with the signal (selector text,
  `AverageValue`, `ignoreNullValues`, threshold, provider address); without
  the signal it errors naming it; with `selectorLabels` unset it errors
  naming the key; `ref.target` overrides `signal.target`; a template policy
  does not see `Signals`. `pkg/runtimeinheritance/merge_test.go`: child adds
  and overrides signals, cannot remove one.
- **envtest.** Extend
  `tests/integration/singlecluster/controller/inferenceservice/autoscal_omenative_dispatch_test.go`
  with a runtime carrying `spec.signals` and a `queue-depth` signal policy;
  assert the ScaledObject trigger's `query` is the runtime's expression with
  the selector substituted and its `metricType` is `AverageValue`; assert a
  runtime without the signal leaves the previous ScaledObject in place and
  sets `AutoscalerResolved=False / SignalMissing`. The
  `webhook/servingruntime` suite gains the admission matrix, including the
  inherited-runtime case.
- **KIND.** The fake-serving image (`tests/integration/kindutil/fake-serving/main.go`)
  serves only `/healthz` today (`:64-74`). It gains a `/metrics` handler
  emitting one gauge, `fake_queue_depth`, initialised from a `QUEUE_DEPTH`
  env and settable at runtime through `PUT /queue-depth` so a spec can raise
  load without a rollout — the same shape as
  `hack/omenative-testing/metrics-server/main.go:134-136`. The fixture needs
  a running KEDA operator and the chart's bundled Prometheus; today it
  applies only the KEDA CRDs (`tests/integration/kindfix/ome.go:96-102`). The
  spec: runtime declares `queueDepth: sum(fake_queue_depth{{{ .Selector }}})`,
  policy target `2`, service `minReplicas: 1`, `maxReplicas: 4`; set depth 6
  on the pod, expect the IR's `/scale` to reach 3; set depth 0, expect 1
  after the stabilization window. Classify the file in `KIND_SPEC_GROUPS` in
  `.buildkite/generate_pipeline.py` or pipeline generation fails.

## Graduation Criteria

- **Alpha:** runtime field with admission; signal policy form; render and
  hold reason; config key; chart block; unit and envtest green.
- **Beta:** KIND spec green in CI; Open Question 1 decided and, if accepted,
  `ref.target` shipped; OEP-0029.1 (analysis) drafted; one release without a
  field rename.
- **Stable:** two Beta releases without an incident attributed to the render.

## Drawbacks

- Runtime authors now write PromQL. That is the point — they are the only
  people who can — but it moves a skill onto the catalog maintainers.
- The expression contract (a fleet total) is a semantic rule admission cannot
  enforce; a wrong aggregation is a runbook item, not a rejection.
- One more render path. Bounded: the signal form reuses the provider and
  fallback code and adds one trigger builder.

## Alternatives

1. **Metric name on the runtime, PromQL in the policy** (`{{ .Metrics.X }}`
   as a template variable). *Tradeoff:* two authors own halves of one fact;
   the failure is wrong series, stuck at max. Rejected by the owner.
2. **PromQL per service.** *Tradeoff:* every service owner writes a query;
   this is the repetition OEP-0013.1 measured at ~1,157 lines, and it does not
   survive a runtime swap.
3. **A fixed enum of signals** on the runtime (three typed strings).
   *Tradeoff:* a fourth signal is a CRD change, a `make manifests`, and a
   controller release. A map with conventional names costs one doc table.
4. **A custom-metrics adapter** serving `ome_inference_*` to HPA `Pods`
   metrics. *Tradeoff:* a new component to run and secure, and gated pods
   would need an adapter-side filter that the bundled Prometheus gives for
   free.
5. **Relabel every runtime's series to stable names** at scrape time.
   *Tradeoff:* both scrape paths need the rename, `__name__` relabeling
   renames rather than aliases, and the per-runtime map lands in chart
   values — the same hardcode, moved.

I recommend the proposal because it is the only option in which one author
writes one fact once and the policy and chart never learn a vendor's name.

## Open Questions

1. **Per-service target.** OEP-0013.1 rejected per-service parameters
   because a partial override inherits the shared template's matchers and can
   match nothing (`README.md:478-484`). Here the service would override only
   a number; the selector and expression come from OME and the runtime, so a
   mismatched-matcher outcome is not reachable. "Same signal, different
   threshold" is the common case (a latency-sensitive service at 2 queued
   requests, a batch service at 16). **Recommendation:** add the optional
   `autoscalerPolicyRef.target`. This reverses a recorded 0013.1 decision and
   is the owner's call; without it, a namespace copies the policy per
   threshold.
2. **Revision in the selector.** Should `.Selector` also expose the revision
   hash so a policy can scale one revision? For autoscaling the answer is
   probably no (one IR, one scaler, above); for OEP-0029.1 the answer is yes.
   Leaning: keep one placeholder here and give analysis its own,
   revision-scoped selector.
3. **A per-signal annotation.** A `unit` (and possibly a total-versus-average
   marker) next to each expression would let status print `4 requests` and
   would let the render pick `Value` for an average. Costs a second map or a
   struct value. Leaning: defer until a consumer needs it; the conventional
   table carries the unit for now.

This applies to Components with a KEDA-capable cluster and a Prometheus that
scrapes runtime `/metrics`; RawDeployment renders through the same policy path
and is covered, MultiNode has no autoscaler dispatch and is not. Next action:
the owner decides Open Question 1; implementation then starts with the
runtime field and validation.
