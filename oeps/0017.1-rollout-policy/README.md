# OEP-0017.1: Reusable Rollout Policies

> **Status: provisional.** Follow-up to OEP-0017 (composable rollout groups).
> Additive: the group grammar and every admission rejection are preserved.
> Nothing here is implemented. Source references are as of `main@e5208902`
> (2026-09-02).

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Terminology](#terminology)
  - [The API at a glance](#the-api-at-a-glance)
  - [What must be true](#what-must-be-true)
  - [Anticipated objections](#anticipated-objections)
- [Design Details](#design-details)
  - [API](#api)
  - [Attachment and precedence](#attachment-and-precedence)
  - [The rollout run](#the-rollout-run)
  - [Validation matrix](#validation-matrix)
  - [Failure semantics](#failure-semantics)
  - [Metrics-source binding](#metrics-source-binding)
  - [Status and digests](#status-and-digests)
  - [Lifecycle](#lifecycle)
  - [Multi-cluster](#multi-cluster)
  - [Security and RBAC](#security-and-rbac)
  - [Migration and rollout](#migration-and-rollout)
  - [Implementation plan](#implementation-plan)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** define a rollout progression once, as data, in a namespaced
`RolloutPolicy`; attach it to a rollout group with one line
(`spec.rollout.groups[i].policyRef`); at the start of each roll, OME resolves
the effective plan — policy or inline — and **pins it for the duration of that
roll**, atomically with the step machine. Edits land on the *next* roll, never
under a live one. The inline progression stays, stays first in precedence, and
stays the escape hatch.

- **`RolloutPolicy`** (namespaced, `ome.io/v1beta1`): exactly one progression
  body — canary steps + analysis, blueGreen, or rollingUpdate budgets. Never
  components, `order`, soak, maintainRatio, or pairingProtocol: those are
  consumer shape and state, which is what makes one policy valid for every
  topology in a fleet.
- **Per-group ref**, same-namespace, alongside the group's progression one-of
  (when both are set, the inline arm wins). The ref **declares its progression
  kind**, so every shape-dependent admission rule (entrypoint, orphan,
  collapse, soak arity) still rejects at ISVC admission without dereferencing
  the policy.
- **The rollout run** is the new mechanism this OEP stands on: the effective
  plan is rendered at run open and written into `status.rollout.activeRun` in
  the *same status write* that resets the step counter — plan-vs-counter skew
  becomes impossible by construction. Pinning applies to inline
  `spec.rollout` too, which retires four live-read defects that exist today.
- **Fail closed = park, never un-gate:** a missing/invalid policy parks the
  new revision (old revision keeps serving) with a condition — it never falls
  back to the default blueGreen progression, because silently removing a
  canary gate is the failure class OEP-0017 exists to prevent.
- **Metrics sources are logical names** (`providerRef`) bound per cluster in
  `inferenceservice-config`, shared with OEP-0013.1's `metricProviders`;
  raw server addresses are admission-rejected in policy bodies.
- **In-use deletion is denied at admission** (exemptions: zero refs,
  break-glass annotation, terminating namespace) — the OEP-0013.1 pattern.
- **Multi-cluster:** the control plane resolves the ref at derive time and
  writes the rendered groups inline into each derived ISVC — members need no
  policy objects, no sync pipeline, and each member's own run pinning protects
  its in-flight rolls.

## Motivation

A production canary-with-analysis block — step ladder, pauses, PromQL
queries, failure budgets, inconclusive policy — is 40–55 expanded YAML lines,
and >95% of it is identical across services (the per-service delta is the
`components` list and, for prefill-decode (PD) disaggregated services,
`pairingProtocol`). The motivating
fleet has 24 InferenceServices: that is ~960–1,320 duplicated lines of the
most safety-critical config in the system. The duplication is concentrated:
coordination-style groups (sequential/blueGreen) are 2–6 lines with near-zero
reuse payoff; the payload is the canary body.

Duplication of a *gate* is worse than duplication of ordinary config,
because a drifted copy fails open:

1. **Fleet gate changes are 24-file sweeps.** Tightening an error-rate
   threshold, adding a 5% step, or fixing a broken query must be applied 24
   times; a half-applied sweep leaves some services baking under the old gate
   with nothing in status to say so.
2. **Copies drift silently toward weaker gates.** A service whose pasted
   analysis block lost its `failureLimit` tweak, or whose query kept a stale
   label matcher that matches nothing, still shows a green canary — the gate
   is vacuous, not failed.
3. **Mid-flight edits are live and unguarded — today, for everyone.** Every
   rollout consumer re-reads `spec.rollout` each reconcile and indexes the
   live plan with a persisted counter (`plan.Steps[cs.CurrentStep]`,
   `omenative/canary/reconciler.go`). Concretely, on `main@e5208902`:
   shrinking the step list below the current index hits the done sentinel and
   the canary *instantly completes* — every instance rolls onto the canary
   revision with the remaining gates skipped; editing the current step can
   move programmed traffic backward; deleting the canary group mid-flight
   unstamps the partition and strands `status.canary` forever; reordering
   groups re-keys soak clocks and ratio anchors, because coordination group
   identity is the list index (`coordination/groups.go:133`). A shared policy
   object *multiplies* this hazard — one edit would rewrite every in-flight
   canary's plan under its step counter — so reuse without a pinning story is
   not shippable. This OEP builds the pinning story and applies it to inline
   specs too.

Why not the existing mechanisms: **GitOps templating** (Helm/Kustomize — the
Argo Rollouts community's answer to strategy reuse) renders N inline copies
from one source, but the copies are still N stored objects that drift
independently, with no runtime provenance, no drift detection, and no
mid-flight protection. **ServingRuntime defaults** have no rollout tier at
all (`ServingRuntimeSpec` carries no rollout field), and the runtime already
injects per-component lifecycle pacing — a rollout tier there would make it a
third pacing source with unclear precedence. **Operator-config defaults**
already exist for scalar knobs (`coordination.defaultRatioTolerancePercent`)
and that is the right layer for scalars — but a whole plan in a ConfigMap is
the schema-less, status-less, review-less store OEP-0013.1 already rejected.
**Admission-time inflation** (Kargo's model — mutate the stored spec with the
rendered steps) creates permanent GitOps drift on every policy edit; OME's
defaulter deliberately never touches `spec.rollout`
(`inference_service_defaults.go`, "applied at runtime by the resolve layer"),
and this OEP keeps that for stored source objects.

### Goals

1. Define a progression once; reuse it unchanged across ISVCs, component
   topologies, and clusters.
2. Preserve OEP-0017's reject-at-admission honesty: no new path may silently
   change rollout behavior — every failure is an admission rejection or a
   loud park.
3. Pin what a roll executes for the duration of that roll — for
   policy-sourced *and* inline rollouts — with plan and step counter written
   atomically.
4. Keep policy bodies cluster-portable: percent capacities only, no
   endpoints, no credentials.
5. Fail closed: a broken policy parks the new revision; it never resolves to
   the default progression.
6. Observable provenance: source, policy identity, digest, and drift on the
   status surfaces a fleet operator already reads.
7. Retire the in-code 15m canary ready-timeout literal
   (`canary/reconciler.go:27`) into an API field with a config-driven
   default.

### Non-Goals

- Reusing group *structure*: components, group count, ordering, soak,
  maintainRatio, and pairingProtocol stay inline (rationale in
  [Attachment and precedence](#attachment-and-precedence)).
- A step-level `analysisRef` (analysis-only reuse) — reserved, not shipped;
  the progression ref subsumes its payload in v1.
- Per-ISVC template parameters — rejected, not deferred (same doctrine and
  same trap as OEP-0013.1: a partial override inherits the shared query's
  assumptions and mis-executes with green status).
- Forwarding promote/rollback verbs to placed ISVCs' members — a
  pre-existing gap ([Multi-cluster](#multi-cluster)); OEP-0018.x territory.
- Cross-home step coordination (each home runs its own state machine, as
  today), a cluster-scoped policy twin (reserved via `ref.kind`), or any
  change to the coordination engine's steady-state per-revision-Service and
  traffic production.

## Proposal

### Terminology

- **OMENative / InferenceReplica (IR)** — OMENative is the deployment mode in
  which OME's own controller owns pod lifecycle through per-Component
  *InferenceReplica* (IR) objects it projects from the ISVC; `spec.rollout`
  progressions execute only there (OEP-0017 rejects canary/coordination
  members on any other mode at admission), so everything in this OEP is
  OMENative-scoped.
- **Group / progression** — a `spec.rollout.groups[]` entry is a set of
  Components that roll together under at most one *progression*: `canary`
  (stepped capacity+traffic with optional analysis gates), `blueGreen` (surge
  all, flip atomically — also the default when none is set), or
  `rollingUpdate` (budgeted gradual replace). OEP-0017 defines the grammar.
- **Rollout run** — one traversal of the effective plan for one target
  revision set, from *open* (a new target revision appears) to *close*
  (completed, rolled back, or superseded by a retarget). The run is the unit
  this OEP pins config to; misread it as "the lifetime of the ISVC" and the
  design collapses.
- **Effective plan** — the fully resolved `groups[]` a run executes: each
  group's progression taken from its inline block or rendered from its
  referenced policy, provider names resolved. Pinned at run open.
- **Pin / repin** — *pin*: write the effective plan into
  `status.rollout.activeRun` atomically with the step-machine reset. *Repin*:
  the explicit, annotation-driven replacement of a pinned plan mid-run — the
  only sanctioned mid-run mutation.
- **Park** — the fail-closed state: the new revision's ControllerRevision
  exists but no update gates open and no traffic moves; the old revision
  keeps serving; `RolloutPlanReady=False` names the reason. Distinct from a
  canary *Hold* (an open run waiting at a step).
- **Entrypoint rule** — a canary group must include the external entrypoint
  Component (router if declared, else engine; `pkg/validation/canary.go`),
  which makes *every* canary group shape-sensitive — the reason component
  membership cannot live in a shared policy.
- **Collapse rule** — a multi-group rollout is admitted only as a pure run of
  single-Component blueGreen groups (`pkg/validation/coordination.go`); soak
  is honored only when the rollout collapses. Group-count-dependent — the
  second reason shape stays consumer-side.
- **Portable digest** — hash of the canonicalized (defaulting-first)
  RolloutPolicy spec; equal across clusters iff the specs match. Same
  construction as OEP-0013.1's.
- **Derived ISVC / home** — the per-member copy placement writes
  (OEP-0018); a *home* is a workload cluster running one.

### The API at a glance

The fleet's canary policy, once (the 40–55-line block 24 services stop
copying):

```yaml
apiVersion: ome.io/v1beta1
kind: RolloutPolicy
metadata:
  name: canary-std-v1              # versioned name: deploy everywhere, then flip refs
  namespace: llm-serving
spec:
  # Exactly one progression body. There is no place to write components,
  # order, soak, maintainRatio, or pairingProtocol — consumer shape and state
  # are structurally unrepresentable here.
  canary:
    scaleDownDelaySeconds: 120
    readyTimeout: 15m              # capacity-gate bound; replaces the in-code literal
    prometheus:
      providerRef:
        name: cluster-prometheus   # logical name; URL/auth/headers bound per cluster
      # serverAddress is admission-rejected in a policy body (cluster-local).
    steps:
    - capacity: "10%"              # policies are percent-only; absolute counts
      traffic: 5                   # are service-specific and stay inline
      analysis:
        interval: 60s
        initialDelay: 120s
        failureLimit: 3
        onInconclusive: Hold
        metrics:
        - name: error-rate-vs-stable
          operator: LTE
          threshold: "1.2"         # stable-relative ratio: fleet-uniform by construction
          query: |
            (sum(rate(http_requests_total{namespace="{{ .Namespace }}",
               service="{{ .CanaryService }}", code=~"5.."}[5m]))
             / sum(rate(http_requests_total{namespace="{{ .Namespace }}",
               service="{{ .CanaryService }}"}[5m])))
            /
            (sum(rate(http_requests_total{namespace="{{ .Namespace }}",
               service="{{ .StableService }}", code=~"5.."}[5m]))
             / sum(rate(http_requests_total{namespace="{{ .Namespace }}",
               service="{{ .StableService }}"}[5m])))
    - capacity: "50%"
      traffic: 30
      pause: {}                    # manual gate
    - capacity: "100%"
      traffic: 100
```

A consumer keeps its shape and state inline; behavior becomes one ref:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata: { name: llm-a, namespace: llm-serving }
spec:
  rollout:
    pairingProtocol: kvproto-v2    # per-service wire contract: always inline
    groups:
    - components: [router, engine, decoder]   # shape is consumer-owned
      policyRef:
        name: canary-std-v1
        progression: canary        # declared kind: admission validates every
                                   # shape rule without dereferencing the policy
  engine: { ... }
  decoder: { ... }
  router: { ... }
```

An engine-only service references the *same* policy with
`components: [engine]`. Different SLO tiers are different named policies
(`canary-tier-large`); the expected fleet-wide count is ≤3, because
stable-relative ratio queries (canary p99 / stable p99) are model-agnostic —
the door OEP-0015.2 left open, now the encouraged idiom.

What a roll pins — the provenance an operator reads mid-incident:

```yaml
status:
  rollout:
    activeRun:
      runID: llm-a-7f3c9a1e
      openedAt: "2026-09-02T12:00:00Z"
      targetRevisions: { engine: rev-9f2c, decoder: rev-9f2c, router: rev-9f2c }
      plan:                        # the EFFECTIVE groups list, frozen for this run
        groups:
        - components: [router, engine, decoder]
          source: Policy           # Inline | Policy, per group
          policyRef: { name: canary-std-v1 }
          policyGeneration: 4
          portableDigest: "rp1:9f3a1c20b7d4"
          canary: { ... rendered steps, provider resolved ... }
    lastRun:
      outcome: Completed           # Completed | RolledBack | Superseded
      openedAt: "2026-08-30T09:11:00Z"
      closedAt: "2026-08-30T10:02:00Z"
      groups:                      # per-group provenance — groups may
      - source: Policy             # reference different policies
        policyRef: { name: canary-std-v1 }
        portableDigest: "rp1:8812aa04c1de"
```

### What must be true

1. Exactly one effective progression per group, chosen deterministically:
   inline > policy, **no further fallback**. A group with a ref and no inline
   block never silently becomes default-blueGreen.
2. Every OEP-0017 admission rejection survives, enforced at ISVC admission
   (shape rules, over declared kinds) or at policy admission (body rules) —
   never demoted to a silent runtime difference.
3. A run executes exactly the plan it opened with. Plan and step counter are
   written in one status update; no consumer can observe a new plan under an
   old counter.
4. Mid-run edits — policy or inline — are inert and *visible* (a drift
   condition names the divergence). The explicit repin verb is the only
   mid-run plan mutation, and it can only hold or advance safely (clamped
   index, non-decreasing traffic), never force-complete.
5. Fail closed parks **before the first traffic write**: every deterministic
   resolution failure (missing policy, kind mismatch, unbound provider) is
   decided at run open, while the old revision still serves 100%.
6. A policy edit by itself never moves traffic: it mints no revision, opens
   no run.
7. Policy bodies are cluster-portable: percent capacities re-resolve against
   each home's derived replicas; endpoints and credentials are
   unrepresentable or admission-rejected.
8. A controller that predates this feature must not silently un-gate a
   ref-only rollout — the downgrade path is procedurally gated and the risk
   is documented, not hidden ([Failure semantics](#failure-semantics)).

### Anticipated objections

**Q. Why one progression body per policy instead of a whole
`spec.rollout` (groups included) — or an array of progressions?**

A. Because everything an array adds is consumer-specific. The moment a policy
carries `groups[]`, it must name components — and component shape is exactly
what varies per service. Every shape-dependent rule (entrypoint, orphan,
collapse, soak arity) would then need the consumer's shape at policy
admission, forcing either a declared shape on the policy (a mixed fleet then
copies the 40–55-line payload into one near-identical policy per shape × SLO
tier, re-creating the duplication with no cross-copy drift detection) or
validation demoted to render time (the silent-mis-execute class OEP-0017
eliminated). Meanwhile the array structure itself has nothing to share:
multi-group rollouts are admitted only as runs of 2–6-line single-Component
blueGreen groups. The consumer's `groups[]` *is* the array — each group takes
one ref, and different groups may reference different policies. This mirrors
the OEP-0013.1 resolution ("different behaviors = different policies;
composition happens by ref"), one level up.

**Q. Pinning changes behavior for inline users who never asked for
policies. Why is that in scope?**

A. Because shipping a shared policy on top of the live-read engine is not
safe — one policy edit would rewrite every in-flight canary's plan under its
persisted step counter — and a pinning mechanism that only guards the policy
path forks the executor into two mid-flight semantics, doubling the test
matrix while leaving the documented inline defects (instant-complete on step
shrink, backward traffic, stranded status, reorder re-keying) in place. The
replaced behaviors
are defect classes, not contracts. The one real workflow this removes —
"delete the canary group to abort" — gets an explicit replacement
(`ome.io/rollout-rollback`, or repin) and a pointed event when the removal is
detected mid-run, not just a release note. I recommend no opt-out toggle;
this is a contestable call and the sign-off gate for this OEP.

**Q. OEP-0013.1 chose live-read plus last-known-good. Why does this OEP
snapshot instead?**

A. Because the consumers differ in kind. An autoscaler is a *continuous*
behavior with no natural boundary — LKG ("keep scaling on the last good
render") is the only coherent degraded state. A rollout is a *bounded run*
with a natural pin point (run open), and LKG-across-runs would replay a stale
plan against a new revision — itself a silent mis-execute. The ecosystem
evidence is one-sided: for the snapshot models (Argo's AnalysisRun flattens
templates at run creation; Kargo inflates at admission and freezes the
Promotion) we found no reported mid-flight-drift incident class, while the
live models are where the wrecks are — Argo's own step-edit reset-to-zero is
a recorded confusion generator (argoproj/argo-rollouts#1372), and OpenKruise
preemptively *blocks* step-count edits mid-flight, its webhook comment
reading "so that the code can be simpler" (openkruise/rollouts). Same house
digest machinery as 0013.1; different pinning point, on purpose.

**Q. Under pinning, an emergency gate-tightening can no longer reach a
canary that is mid-bake. Isn't live-read strictly better in an incident?**

A. It reaches it — explicitly. `ome.io/rollout-repin` re-renders and replaces
the pinned plan mid-run, clamped so it can only hold or tighten (the step
index clamps into the new ladder, and a clamped step that would raise
exposure is not applied — the run holds at the currently-programmed
capacity and traffic pending manual promote). A documented bulk-repin flow —
annotate every ISVC with `RolloutPlanDrift=True`, CAS-safe via the
digest-valued annotation — recovers live-read's one genuine advantage on
demand, without accepting its failure mode ambiently. What live-read can do
and repin cannot is *loosen* a gate under a live run in one sweep; that
asymmetry is intended.

**Q. Why per-group refs instead of one ISVC-level `rolloutPolicyRef`?**

A. The group is where the progression one-of already lives, so the ref rides
the existing axis with no new targeting sub-language. It composes (a PD
sequence can put decoder and engine groups on different policies), and it
keeps the shape rules evaluable at admission — an ISVC-level ref would need
the policy to say which groups it populates, which is the array problem
again. Cost: a multi-group ISVC repeats a one-line ref per group; acceptable.

## Design Details

### API

Types land in `pkg/apis/ome/v1beta1`. The kind is `RolloutPolicy` — it
renders into the `spec.rollout` group family; `RolloutTemplate` was rejected
because the body is not templated (see below), and `ProgressionPolicy` names
an internal concept, not the user surface.

The policy body **reuses the existing progression types verbatim** —
`GroupCanary`, `GroupBlueGreen`, `GroupRollingUpdate` — rather than forked
policy twins. This is load-bearing: the composed plan is a literal
`RolloutGroup` fragment, so run-open re-validation runs the *same*
`pkg/validation` functions the ISVC webhook runs, and the two can never
disagree about what is admissible. Policy-only restrictions (percent-only,
no `serverAddress`) are admission rules on this kind, not type differences.

```go
type RolloutPolicySpec struct {
    // Exactly one progression body (CEL-enforced one-of, exactly one).
    // The types are the spec.rollout group progressions, reused verbatim.
    Canary        *GroupCanary        `json:"canary,omitempty"`
    BlueGreen     *GroupBlueGreen     `json:"blueGreen,omitempty"`
    RollingUpdate *GroupRollingUpdate `json:"rollingUpdate,omitempty"`
}
```

Two additive changes to existing types, useful independently of policies:

```go
// GroupCanary gains the capacity-gate bound that today is an in-code 15m
// literal. Nil falls back to the operator-configured default
// (rollout.defaultReadyTimeout, chart-supplied); the annotation override
// (ome.io/rollout-ready-timeout) is retained and still wins.
ReadyTimeout *metav1.Duration `json:"readyTimeout,omitempty"`

// AnalysisPrometheus gains the logical-provider spelling. Exactly one of
// ProviderRef / ServerAddress may be set (CEL). In a RolloutPolicy body,
// ProviderRef is the only admitted spelling; inline specs may use either.
ProviderRef *MetricProviderRef `json:"providerRef,omitempty"`
```

Consumer side — a new field on `RolloutGroup`, sibling of the inline
progression arms:

```go
// PolicyRef names a same-namespace RolloutPolicy that supplies this group's
// progression. It may coexist with one inline progression (the inline block
// wins — the preview/rollback mechanism); the existing at-most-one rule
// continues to apply to the inline arms only.
// +optional
PolicyRef *RolloutPolicyRef `json:"policyRef,omitempty"`

type RolloutPolicyRef struct {
    Name string `json:"name"`

    // Progression declares the referenced policy's kind. ISVC admission
    // validates every shape-dependent rule (entrypoint, one-canary-max,
    // collapse, soak/maintainRatio arity, lockstep, pairingProtocol grouping)
    // against this declaration WITHOUT dereferencing the policy; a
    // declaration the policy body contradicts parks at run open
    // (ProgressionMismatch) — it cannot mis-execute.
    // +kubebuilder:validation:Enum=canary;blueGreen;rollingUpdate
    Progression string `json:"progression"`

    // Reserved: v1 admission accepts only "RolloutPolicy";
    // "ClusterRolloutPolicy" is the reserved cluster-scoped twin.
    // +kubebuilder:default=RolloutPolicy
    // +optional
    Kind string `json:"kind,omitempty"`
}
```

**There is no template engine in this OEP.** The policy body is verbatim
data; the only resolution at run open is provider-name binding and
composition into the effective plan. The seven analysis template variables
(`{{.Namespace}}`, `{{.ISVCName}}`, `{{.Component}}`, `{{.CanaryService}}`,
`{{.StableService}}`, `{{.CanaryRevision}}`, `{{.StableRevision}}`) already
late-bind in the analysis sampler at evaluation time (OEP-0015.2) — the
policy inherits them for free, and they are what make fleet-uniform bodies
possible. Deliberate non-fields: no `params` on the ref, no `optOut`, no
ISVC-level ref, no selector attachment. Reserved shapes are exactly one:
`ref.kind`.

### Attachment and precedence

Per-group, evaluated at run open (and, read-only, by the status writer every
reconcile):

1. Inline progression on the group — `source: Inline`. The escape hatch and
   the preview mechanism: ref + inline coexist, inline executes, status
   publishes `shadowedPolicyRef` with the digest the policy *would* pin.
2. The group's referenced policy, verbatim — `source: Policy`.
3. **No further fallback.** A group whose ref cannot resolve parks
   ([Failure semantics](#failure-semantics)). The no-progression default
   (blueGreen) applies only to groups that declare *neither* an inline arm
   nor a ref — unchanged from OEP-0017.

What stays inline, and why each:

- `components` — the shape; the entrypoint and orphan rules read it at
  admission.
- `soak`, `maintainRatio` — one-line knobs whose *validity* depends on group
  count and arity (collapse rules); keeping them inline keeps their rejection
  at ISVC admission with zero dereferencing. No reuse payoff exists.
- `pairingProtocol` — per-service wire-contract *state* that rides revisions;
  not behavior, never reusable.
- Absolute step capacities (`capacity: "3"`) — service-specific by
  definition, and the final-step-completeness rule is only checkable for the
  percent form; policies are percent-only, absolute-count services stay
  inline.
- The operator verbs (`ome.io/rollout-promote`, `-rollback`, `-paused`,
  `-ready-timeout`) — operator inputs, always read live.

There is deliberately no ServingRuntime tier: the runtime already defaults
per-Component lifecycle pacing through its component configs, and the
coordination engine reads the partition from the projected IR precisely
because runtime-injected pacing is invisible on the raw ISVC
(`coordination/reconciler.go`). A policy tier in the runtime would be a third
pacing source; the policy slots only between inline spec and operator config.

**Conflicts are structural non-events:** one ref per group; different groups
on different policies is composition; inline + ref is precedence, reported
via `shadowedPolicyRef`. Two policies on one group is unrepresentable.

### The rollout run

The mechanism everything above depends on. Today the executor indexes the
**live** step list with a **persisted** counter; the run makes both sides of
that index come from the same frozen object.

**Open.** Evaluated where `canary.Dispatch` / coordination resolve run today,
when all of: `spec.rollout.groups` is non-empty; no `activeRun` exists; some
group member's target revision (the rendered-desired specTarget hash) differs
from its `latestRolledoutRevision`; and the target is not the sticky reject
(`status.canary.rolledBackRevisionHash` re-arm semantics unchanged). Opening
renders the effective plan (per-group inline-or-policy, providers resolved),
runs the full composed validation — the same `pkg/validation` pass the
webhook runs, over the effective spec — and, only if valid, issues **one
status update** carrying `activeRun{...}` plus the run-scoped resets
(`CurrentStep=0`, coordination ratio anchor). Both already live in ISVC
status, so one write really can carry them; a consumer on a stale informer
sees an *older* consistent pair, never a mixed one. The Sequential soak clock
needs no reset here and is deliberately outside the atomicity claim: the
dispatcher-side gate reads the *previous* Component's InferenceReplica Ready
transition time (IR status, not ISVC status —
`coordination/sequential_gate.go`), which re-arms itself on each roll.

**Close.** Any of:

- *Completion* — every grouped member has converged to the run's target:
  its `latestRolledoutRevision` equals it, **or** the group has reached the
  engine's Staged resting state (a lifecycle partition deliberately holds
  mixed revisions forever; keying completion on `latestRolledoutRevision`
  alone would pin such a run's plan — and block policy-edit pickup — until an
  unrelated retarget). The last group's soak stays ignored, as the API
  documents: completion is convergence, not a trailing wait.
- *Rollback terminal* — sticky reject recorded and surviving run close, as
  today.
- *Retarget* — a new specTarget mid-run closes the run and immediately
  reopens with a **fresh render**; this is where pending policy edits take
  effect. The reopen's resets are scoped **per group**: only groups whose
  target revisions changed reset their progress (step index, ratio anchor);
  a group mid-flight toward an unchanged target keeps its state — matching
  today's semantics, where the canary resets only on its *own* target change
  and independent groups run independently. `StableRevisionHash` is preserved
  across the close/open.
- *Repin to empty* — a repin whose re-render produces no groups.

On close, `activeRun.plan` is dropped and the bounded `lastRun` record
remains.

**What the executor reads.** The canary executor indexes
`activeRun.plan` steps; the coordination engine resolves groups from the
pinned plan while a run is active and from live spec when idle (its
steady-state per-revision-Service and traffic production is not a run and is
unchanged); per-Instance update gates already fetch the parent ISVC each tick
and read its pinned plan — no new fetch, no cross-object pointer. Group
list-index identity now indexes the *frozen* pinned list, so mid-run reorders
can no longer re-key ratio anchors or soak clocks. Partition stamping uses
the pinned current step. One ordering invariant is load-bearing: a new target
is **not projected to IRs before the run-open (or park) status write
persists** — equivalently, update gates fail closed whenever a grouped
Component's target has diverged and no matching `activeRun` exists. Without
it, the window between target divergence and the pin (they are different
loops — the dispatcher gates tick per-Instance) would resurrect exactly the
unstamped-partition defect this section retires. Components not in any group
are outside the run and roll under their lifecycle pacing, as today.

**Why status, not the alternatives.** Digest-only pinning detects drift but
cannot execute through it — the executor would still read a mutable object
mid-run, so an edit either wedges a healthy run (digest mismatch ⇒ hold) or
drifts; the plan itself must be stored. An ISVC annotation fights GitOps
(prune/sync rewrites metadata) and cannot be written atomically with the
status-resident counter. A dedicated Run CRD or ControllerRevision-adjacent
record splits the pin across objects — a crash between the record write and
the status reset recreates exactly the index-vs-plan skew this design exists
to kill, and per-Instance gates would gain a dangling read against a GC'd
object (an observer/dispatcher mismatch class this repo has been bitten by
before). Runs are ≤1 per ISVC, so the 1:1 status block is the natural shape;
Argo needs a standalone AnalysisRun only because its runs fan out N per
rollout. Cost: ~2–4 KB of status per in-flight roll (multi-line PromQL
dominates), bounded by `rollout.maxPinnedPlanBytes` in operator config
(chart-supplied, no in-code literal) — policy admission rejects bodies over
the cap, and the ISVC webhook applies the same cap to inline progression
bodies, since inline rollouts are pinned too. Status loss (subresource
replace,
backup/restore) is no *new* failure class — the step counter already lives
there — and has a stated recovery: activeRun absent while component status
shows a roll in progress ⇒ re-render from the current source, adopt-in-place
as the run's plan, one `RolloutPlanRepinned` event.

**Mid-run edits.**

- Policy edited: active runs keep executing their pinned plan; when the live
  render digest diverges from `activeRun`'s, the controller sets
  `RolloutPlanDrift=True (PolicyNewerThanRun)`. The next run picks up the
  edit — the Argo AnalysisRun contract. A days-long manual pause never blocks
  a fleet policy update.
- Inline `spec.rollout` edited: same semantics — inert + drift condition.
  One executor path, no policy-vs-inline branch.
- Ref added / removed / retargeted to another policy: inert (a ref change
  mints no revision); drift condition; applies at the next run. If the edit
  removes a mid-run group entirely, the controller additionally emits a
  pointed event naming the working abort verbs — this replaces the implicit
  "delete the group to abort" workflow.
- **Repin** — `ome.io/rollout-repin: "<expected-render-digest>"` (digest-
  valued, so it is CAS-safe against a racing policy edit; a bare `"now"` is
  accepted but not recommended). One-shot and audited by event with old→new
  digests: re-render from the current source, re-validate, replace
  `activeRun.plan` preserving run identity and progress. Clamps — a repin may
  only hold or tighten: `CurrentStep` clamps into the new ladder, and the
  clamped step is **not applied when it would raise exposure**. That case is
  real, not marginal: a shorter ladder clamps onto the final step, whose
  traffic/capacity is 100% by validation, and the executor's normal order
  applies traffic once the capacity gate passes, *before* any pause hold — so
  a bare index-clamp would be full cutover. Instead the run enters a
  **pre-step hold** (a new executor state, specified here deliberately):
  capacity and traffic stay at their currently-programmed values, pending
  `ome.io/rollout-promote`. Analysis samples re-key through the existing
  content fingerprint (`analysis_sampler.go`), so samples taken under the old
  plan are never consumed by the new one. This is OpenKruise's
  jump-don't-restart, opt-in per incident rather than ambient.
- **In-use edit-freeze — evaluated, rejected as primary.** Freezing policy
  updates while any referencing run is in flight (the OpenKruise webhook
  model) gates fleet-wide policy iteration on the slowest paused canary —
  days — and needs a runs-per-policy index on the write path. Pinning gives
  the same no-drift guarantee without blocking writers. Retained as a
  non-blocking admission **warning**: a policy UPDATE returns the count of
  active runs pinned to older digests, so the editor sees the blast radius at
  write time. One targeted freeze *is* adopted: an in-place edit that changes
  the policy's progression *kind* while any ISVC references it is rejected —
  kind changes ship as a new versioned policy name.

### Validation matrix

OEP-0017's contract is that a rollout spec the engine will not honor is
rejected, not silently adjusted. Every rule below names its enforcement
point; the only demotion relative to a fully-inline spec is marked.

| Rule | Enforcement point |
|---|---|
| Exactly one progression body | Policy CEL + webhook |
| Canary body: steps 1–20 non-empty, non-decreasing traffic, traffic>0 ⇒ capacity>0, final step traffic==100 and capacity=="100%", complete analysis (metrics 1–10, failureLimit ≥1, thresholds numeric), strict capacity parsing | Policy webhook — the existing `pkg/validation/canary.go` plan checks, refactored to take a progression body |
| Percent-only capacities; `serverAddress`/`authRef` rejected; `providerRef` required when any step has analysis; rendered size ≤ configured cap | Policy webhook (the ISVC webhook applies the same size cap to inline progression bodies — inline plans are pinned too) |
| `providerRef` name bound on *this* cluster | Policy status condition + admission **warning** only — members legitimately differ; unbound-here is not a spec error |
| Policy UPDATE while runs pin an older digest | Admitted + warning (run count); progression-kind change while referenced: **rejected** |
| Policy DELETE while any group references it | **Denied** (webhook DELETE handler). Exemptions: zero refs; `ome.io/allow-in-use-delete: "true"`; terminating namespace. Never a finalizer — a finalizer accepts the delete and parks the object in Terminating, wedging GitOps prune; a refusal is immediate and names the referencing ISVCs |
| Orphan rule; entrypoint rule; one canary group max; soak/maintainRatio-on-canary; collapse + soak-only-when-sequenced; non-empty `order`; rollingUpdate lockstep; pairingProtocol grouping | ISVC webhook, unchanged — evaluated over inline components + each ref's **declared** progression (a group with an inline arm counts as its inline kind; the ref is preview-only then) |
| Ref shape (`progression` present, `kind` = RolloutPolicy) | ISVC CEL + webhook |
| Feature-disabled cluster ⇒ any ref rejected | ISVC webhook only — a CRD CEL rule is static per schema and cannot read the chart gate |
| Ref existence / body matches declaration | *Demoted, deliberately*: not ISVC-admission-blocking (a dangling ref must not block unrelated ISVC updates, and webhook availability must not couple to policy-cache health). Best-effort: the ISVC webhook dereferences via informer and returns a **warning** on a missing policy or declared-kind mismatch, failing open on staleness. Authoritative check: run open — failure parks, never executes |
| Composed plan valid (belt-and-suspenders full `pkg/validation` pass over the effective spec) | Run open — failure ⇒ run does not open; park + condition. Narrow by construction: the body passed policy admission and the shape passed ISVC admission, so this catches only kind mismatch, deleted-policy, and cross-rule skew |

### Failure semantics

Governing rule: the forbidden failure is silently removing a gate — a canary
group that quietly becomes blueGreen un-gates a production roll. Fail closed
therefore means **park**: the new revision is minted but no gate opens and no
traffic moves; the old revision keeps serving; reconciles keep succeeding
(degraded-not-error, the OEP-0013.1 lesson — `PolicyNotFound` must not
requeue-with-backoff the whole ISVC and stall image updates or replica
edits). All park decisions are made at run open, before the first traffic
write.

| Failure | Behavior | Condition reason |
|---|---|---|
| Ref names a missing policy at run open | Park; watch-driven recovery when the policy appears | PolicyNotFound |
| Declared `progression` ≠ policy body | Park (cannot mis-execute by construction) | ProgressionMismatch |
| Policy exists, `Ready=False` (failed own validation, e.g. bypassed webhook during skew) | Park | PolicyNotReady |
| Composed re-validation fails at run open | Park | PlanInvalid |
| `providerRef` unbound in this cluster's `metricProviders` | Park at run open — a config error is deterministic; do not start a roll whose gate cannot sample | ProviderUnbound |
| Prometheus unreachable / query empty *mid-run* | Soft, unchanged: inconclusive sample → step Holds → existing stall escalation. Data-plane errors degrade; config errors park | (canary status, as today) |
| Policy deleted mid-run (break-glass, namespace teardown, skew) | Active run unaffected — the plan is pinned; next run parks | PolicyNotFound (next run) |
| Pinned plan lost (status wipe/restore) | Re-render from current source, adopt-in-place, event | — (RolloutPlanRepinned event) |
| Inline present + ref dangling | Inline executes (precedence); condition notes the dangling preview ref; non-blocking | — |
| Repin annotation digest ≠ current render | Repin rejected, event; annotation cleared | — |

**The downgrade hazard — documented, procedurally gated, not solved.** A
controller downgraded below this feature does not know `policyRef`: a group
whose only progression is the ref reads as a bare group and would execute the
**default blueGreen** — the forbidden un-gate, arriving precisely during a
platform rollback. No in-cluster actor survives the downgrade to detect it.
Mitigations: (a) the operational gate — before downgrading the controller,
inflate refs to inline (`ome-policy` renders the diff; the bulk flow is one
annotation sweep), mirroring OEP-0013.1's stage-R rule; (b) this row in the
risk register, so the release runbook carries it; (c) an open question
([Open Questions](#open-questions)) on whether a CRD/CEL posture can make a
ref-only group fail *old* validation loudly instead. Multi-cluster
inflate-at-derive shrinks the exposure to the management plane's own
controller: members only ever see inline groups.

### Metrics-source binding

The canary Prometheus source converges on OEP-0013.1's provider bindings —
one cluster binding serves both consumers. `metricProviders` moves to a
top-level key of `inferenceservice-config` (the `autoscalerPolicy` block
reads it from the new location; the old nested key is honored as an alias for
one release, then removed):

```json
{
  "metricProviders": {
    "cluster-prometheus": {
      "serverAddress": "http://prometheus.monitoring.svc:9090",
      "authSecretRef": { "name": "prom-token", "key": "token" },
      "headers": { "X-Scope-OrgID": "llm-serving" }
    }
  },
  "rollout": { "maxPinnedPlanBytes": 16384, "defaultReadyTimeout": "15m" },
  "canaryAnalysis": { "defaultProvider": "cluster-prometheus", ... }
}
```

The four deltas between the two consumers, handled:

1. **Headers** (tenant scoping): the binding gains an optional `headers` map
   — operator-authored config, not a user injection surface. An inline
   `headers` on the CR overlays it, CR key winning.
2. **Secret semantics**: the autoscaler never reads the secret value (KEDA's
   TriggerAuthentication does); the canary sampler must attach a bearer token
   itself, so it keeps its live per-sample value read — sourcing the
   SecretKeySelector from the binding. Same field, two consumption modes, no
   new API. As in the autoscaler mode, the Secret is read from the
   *consumer's* namespace (a SecretKeySelector carries no namespace); the
   binding — which only cluster admins author — fixes the secret name and
   key.
3. **The anonymous default**: `canaryAnalysis.bundledPrometheusAddress` is
   retained as the no-ref fallback for *inline* specs (deprecation-tracked),
   and `canaryAnalysis.defaultProvider` names a binding for fleets that want
   the default to be a first-class provider. No reserved magic provider name.
4. **Failure split**: an unbound provider *name* is a deterministic config
   error ⇒ park at run open (autoscaler semantics); an unreachable *server*
   mid-run stays soft-inconclusive ⇒ Hold ⇒ stall escalation (canary
   semantics). Resolution errors park; sampling errors degrade.

In policy bodies, `providerRef` is the only admitted spelling; inline specs
keep `serverAddress`/`authRef` as the escape hatch (existing surface, not
widened). SSRF posture matches 0013.1: authors name providers; only cluster
admins bind names to URLs.

### Status and digests

Policy status is bounded (counts and digests, never name lists), with the
same Argo Lua health-check note as 0013.1 (CRs are Healthy-by-default in
Argo; gate waves on `Ready`):

```yaml
status:                            # RolloutPolicy
  observedGeneration: 4
  portableDigest: "rp1:9f3a1c20b7d4"
  attachedGroups: 24
  conditions:
  - { type: Ready, status: "True", observedGeneration: 4 }
  - { type: InUse, status: "True", reason: Attached }
```

ISVC side, additive under `status.rollout` (which also becomes the natural
home for the eventual status unification OEP-0017 left open):

- `activeRun` / `lastRun` — as sketched in
  [The API at a glance](#the-api-at-a-glance). Per-group provenance
  (`source`, `policyRef`, `policyGeneration`, `portableDigest`) lives on each
  pinned group.
- `groups[]` — the always-current resolution view, written every reconcile
  independent of runs: per group `{source, policyRef, observedDigest,
  shadowedPolicyRef{name, wouldPinDigest}}`. `shadowedPolicyRef` appears when
  an inline arm outranks a ref — the preview surface: an operator diffs
  `wouldPinDigest` against expectation *before* deleting the inline block.
- Conditions: `RolloutPlanReady` (False = parked, reason above) and
  `RolloutPlanDrift` (True = live render diverges from the pinned digest;
  reason names which side moved).
- kubectl columns: ISVC gains `PLAN-SOURCE` (Inline | Policy/<name>) and
  `PLAN-DRIFT`; RolloutPolicy gains `READY`, `DIGEST` (short), `REFS`.

**One digest, one job.** The portable digest (`rp1:` prefix) hashes the
defaulted, canonicalized policy spec via the shared canonicalization package
— defaulting-first for the same reason as 0013.1 (the apiserver stores a
defaulted object; hashing raw GitOps bytes would mismatch identical files).
There is no per-home "resolved digest" here: the pinned plan itself is the
resolved artifact, and it is fully visible in status. Events carry old→new
digests on every pin, repin, and drift transition — short-horizon debugging
only, since Events expire on a ~1h default TTL; the durable record while a
run lives is `activeRun` itself, and post-close history beyond `lastRun` is
open question 1.

### Lifecycle

| Event | Behavior |
|---|---|
| Policy created | `Ready` computed (body validation + sample composition); parked consumers recover watch-driven at next run open |
| Policy updated (valid) | In-flight runs unaffected (pinned); `RolloutPlanDrift` raised where diverged; next run renders fresh. Admission warning reports pinned-run count |
| Policy updated (invalid) | Webhook rejects; if bypassed (skew), `Ready=False` and the next run open parks (PolicyNotReady) |
| Policy progression-kind changed while referenced | Rejected; ship a new versioned name |
| Delete attempted | Denied while referenced (exemptions: zero refs, break-glass, terminating namespace); through an exempt path ⇒ pinned runs finish, next run parks |
| Inline arm added beside a ref | Next run sources inline; current run unaffected; `shadowedPolicyRef` published immediately |
| Inline arm removed (ref remains) | Next run sources the policy; preview digests make the flip verifiable before it happens |
| Retarget mid-run | Close + reopen with fresh render (pending edits land); `StableRevisionHash` preserved; sticky-reject re-arm unchanged |
| Upgrade with a roll in flight | Adopt-in-place: pin the current live plan at the persisted step; no restart, no traffic movement; `RolloutPlanRepinned` event |
| Controller downgrade below the feature | The documented gate: inflate refs to inline first ([Failure semantics](#failure-semantics)) |

### Multi-cluster

Today `DeriveISVC` copies `spec.rollout` verbatim onto every derived ISVC and
strips the promote/rollback annotations as control-plane-owned — with no
forwarding mechanism, so a bare manual-Pause canary on a placed ISVC can only
be advanced by annotating each member's derived object directly. That gap
predates this OEP and stays out of scope; the policy layer must simply not
widen it.

**Distribution: inflate at derive.** The control plane resolves each group's
`policyRef` against its own policy copy (GitOps-synced to the CP, as in
0013.1) and writes the **rendered inline groups** into the derived ISVC, plus
provenance annotations (`ome.io/rollout-plan-source: <name>@<digest>` per
group) on the system-authored derived object — where annotation-clobber
concerns do not apply, because no GitOps tool owns derived objects. Members
therefore need no RolloutPolicy CRDs, no policy informers, and no sync
pipeline; the member policy-sync race is eliminated rather than preflighted.
A CP policy edit re-derives the members; each member's own run pinning
protects its in-flight rolls, and the edit lands at each member's next run.

This inverts 0013.1's worker-local-render choice, deliberately: the
autoscaler *must* render on members because its output depends on
member-local derived bounds, while a progression body is bounds-free by
construction (percent-only). Head-to-head:

| | Inflate at derive (chosen) | GitOps-distribute + preflight (0013.1 model) |
|---|---|---|
| Member prerequisites | none beyond base OEP-0017 + the `providerRef` field | policy CRD + controller + policy objects on every member |
| Sync race | none (plan rides the derived spec) | member missing the policy parks until GitOps catches up |
| Per-home percent resolution | automatic (percent re-resolves against derived replicas) | automatic |
| Policy edit propagation | CP re-derive, one writer | N member syncs, drift window per member |
| CP outage | members keep executing pinned/derived plans; no *new* rolls get fresh policy content until CP returns | members render locally, unaffected |
| Failure isolation | CP render bug affects all derivations | member render bug affects one home |

The CP-outage row is the real cost of inflation and it is acceptable: rollout
plans change rarely (versioned names), and a stale-but-valid plan is safe by
construction — unlike an autoscaler, where a stale bound is a live hazard.

**Preflight** (placement controller, read-only): candidate members must carry
the capability label `ome.io/rollout-policy: v1beta1` when the effective plan
uses `providerRef` or `readyTimeout` — an older member's CRD schema would
prune the unknown fields, silently detaching the gate's metrics source
(pruned `providerRef` ⇒ no source ⇒ inconclusive-forever), which is exactly
the silent class the label gate exists to close. For analysis-bearing plans,
a home without the named binding in *its* `metricProviders` is ineligible —
closing today's live gap where an unbound member reads inconclusive into a
Failed-park. Preflight also emits a **warning** when a placed effective plan
contains a bare manual Pause step, naming the stripped-verb dead end at plan
time instead of at a member's frozen step 3; fleets placing canaries should
prefer analysis or timed gates. Provenance survives inflation: at run open,
the member copies each group's `ome.io/rollout-plan-source` annotation into
that pinned group's provenance record (`source: Policy`, `policyRef`,
`portableDigest`), so member status reports the same identity a
locally-resolved ref would. The watch-funnel — the OEP-0018.2 channel that
streams member status back to the control plane — lifts these per-group
digests into `status.placement.candidates[]` for fleet drift dashboards
(which home is still rolling under the old plan).

Per-home rolls remain independent state machines (step indices and phases can
diverge across homes, as today); cross-home step coordination is a non-goal.

### Security and RBAC

| Actor | RolloutPolicy | ISVC | inferenceservice-config | Secrets |
|---|---|---|---|---|
| Policy author (platform team) | CRUD in namespace | read | — | — |
| ISVC author | read only | update (ref / inline) | — | — |
| Workload OME manager | get/list/watch | as today | read | read (sampler bearer token, ns-local, live reader) |
| Management-plane OME | get/list/watch (its own synced copies; no member access needed) | derived RW as today | — | — |
| GitOps (Argo) | apply in ns (+ CP copy) | apply | apply (chart) | provider token secrets |
| Cluster admin | — | — | `metricProviders` (the SSRF gate) | provider token secrets |

Hard lines: policy bodies carry no endpoints (admission-rejected) and no
credentials (unrepresentable); the analysis template variables remain the
closed seven controller-derived values — no user string enters a query
context. A compromised policy author is confined to one namespace and cannot
move traffic (a policy edit opens no run); a compromised control plane
cannot write member policies (there are none). Webhook wiring follows the
house checklist (handler package, registration, chart
ValidatingWebhookConfiguration, cert-manager CA injection, failurePolicy
Fail, DELETE-safe matchConditions).

### Migration and rollout

Each stage individually reversible. Stage 1 carries the one behavior change
(run pinning) for *all* rollout users and is the sign-off gate; stages 3–5
are per-service and revert with a PR.

- **0** — API + validation + run-model code land. The chart gate covers only
  the policy surface (CRD, webhook, policy-status controller), which stays
  disabled; run pinning is deliberately ungated and activates with the
  stage-1 controller image.
- **1** — controller upgrade: run pinning active for inline rollouts.
  In-flight rolls adopt-in-place (no restart, no traffic movement). Release
  note + event cover the abort-verb change. Rollback: downgrade the
  controller — inline specs revert to live-read semantics with no data loss
  (`activeRun` is ignored, then garbage on the next status write).
- **2** — enable the policy surface (CRD + webhook + policy-status
  controller, one Helm value; members need only their capability label);
  distribute `canary-std-v1` + provider bindings via the bootstrap
  ApplicationSet, including the CP copy; verify one identical
  `portableDigest` everywhere.
- **3 — preview, one low-risk ISVC:** add `policyRef` beside the inline
  progression. Inline wins — zero behavior change by construction; status
  shows `shadowedPolicyRef.wouldPinDigest`; CI (`ome-policy check`, extended)
  posts the rendered-progression-vs-inline diff, not just digest equality.
  Rollback: remove the ref.
- **4 — cutover that ISVC:** delete its inline progression in the same PR.
  Next roll pins `source: Policy`; assert the pinned plan equals the previous
  inline body byte-for-byte (the stage-3 diff was empty). Rollback: revert.
- **5** — sweep the fleet in batches; the absolute-capacity and bespoke
  services stay inline.
- **6** — CI ratchet: `ome-policy check --require-rollout-ref` forbids new
  inline canary bodies in the fleet directory without an exemption marker.
- **R** — rollback rules: restoring an inline arm is atomic and per-service
  (next run sources it). Controller downgrade below the feature: **inflate
  refs to inline first** — never downgrade while any ref-only group exists
  (the downgrade hazard, [Failure semantics](#failure-semantics)).

### Implementation plan

1. API + codegen: `RolloutPolicy` types, `PolicyRef` on `RolloutGroup`,
   `ReadyTimeout` on `GroupCanary`, `ProviderRef` on `AnalysisPrometheus`,
   CEL rules. No behavior.
2. `pkg/rolloutpolicy` (pure, K8s-free): canonicalization + portable digest;
   progression-body validation refactored out of `pkg/validation` so webhook,
   run open, and CLI share one implementation.
3. The run model: `status.rollout.activeRun`/`lastRun` types; run open/close;
   executor reads switch to the pinned plan (canary reconciler, coordination
   resolve, gate context, partition stamping); adopt-in-place; repin verb;
   drift condition. The largest step — it touches the executor spine and
   every KIND mid-run-edit spec flips from asserting live-apply to asserting
   inertness + repin.
4. Webhooks: policy validator (incl. in-use DELETE denial, kind-change
   freeze, size cap) + ISVC webhook additions (ref shape, declared-kind shape
   rules, best-effort deref warning); envtest suites.
5. Config: top-level `metricProviders` (+ alias), `rollout` block,
   `canaryAnalysis.defaultProvider`/`defaultReadyTimeout`; chart values;
   delete the in-code 15m literal.
6. Policy-status controller (Ready, digest, attachedGroups) + ISVC
   `status.rollout.groups[]` writer + field index for the DELETE denial and
   drift fan-out.
7. Multi-cluster: inflate-at-derive, capability-label + provider preflight,
   manual-Pause warning, funnel lift of run digests; MC integration tests.
8. KIND specs (classified in `KIND_SPEC_GROUPS`) + perf rangespecs;
   `ome-policy` CLI extension + CI wiring docs.

The OEP lands first (provisional → implementable before step 1 merges).

## Test Plan

| Layer | Suite | Cases |
|---|---|---|
| Unit | rolloutpolicy pkg | body validation parity with inline (same function, golden cases); percent-only; serverAddress rejection; digest canonicalization (defaults applied, field order); size cap |
| Unit | run model | open/close triggers per progression; atomic pin+reset; retarget close/reopen preserving StableRevisionHash; sticky-reject re-arm; repin clamps (shorter-ladder hold, non-decreasing traffic, sample re-key); adopt-in-place; status-loss recovery |
| envtest webhook | `singlecluster/webhook/rolloutpolicy` | one-of; plan rules on the body; in-use DELETE denied / zero-refs / break-glass / terminating ns; kind-change freeze; update warning; ISVC ref shape; declared-kind shape rules (entrypoint via declared canary, collapse over declared kinds, soak/maintainRatio-on-declared-canary); feature-disabled ref rejection; ref+inline coexistence admitted; deref warning fails open on missing policy |
| envtest controller | `singlecluster/controller/rolloutpolicy` | policy status; park reasons (NotFound/Mismatch/NotReady/ProviderUnbound) with old revision still serving; drift condition both directions; unrelated spec changes converge while parked (degraded-not-error); preview digests |
| KIND | new specs in `KIND_SPEC_GROUPS` | policy-sourced canary end-to-end (pin → steps → promote → lastRun); mid-run policy edit inert + drift + next-run pickup; mid-run inline step-shrink no longer force-completes; group-delete mid-run emits the abort-verb event; repin mid-run (tighten + shorter-ladder hold); upgrade adopt-in-place with a canary in flight; unbound provider parks before traffic moves |
| Multi-cluster | `tests/integration/multicluster` | inflate-at-derive (member spec carries rendered groups + provenance annotations, no policy CRs); CP policy edit re-derives, member pinned run unaffected; capability-label gate; provider-binding ineligibility; manual-Pause preflight warning; funnel lifts run digests |
| Perf (rangespec) | `tests/perf` | run open p99 (render + validate + status write) < 50ms; drift fan-out on a policy edit across 100 consumers < 30s; zero status writes on no-op reconciles with an active run |
| CI (GitOps repo) | `ome-policy check` | render every policy × ISVC pair; progression-body diff vs inline during migration; `--require-rollout-ref`; digest equality raw-file == live object |

Behavioral acceptance is asserted on pinned-plan content and traffic/partition
writes, not on Prometheus behavior — the sampler and gate mechanics are
OEP-0015.1/0015.2 surfaces with their own suites.

## Graduation Criteria

- **Alpha (this OEP):** canary + blueGreen + rollingUpdate bodies; per-group
  ref with declared kind; run pinning (inline included) + repin; park
  semantics; provider convergence; inflate-at-derive; the motivating fleet's
  canary services migrated; full test plan green.
- **Beta:** the downgrade-posture open question resolved (schema-level or
  formally procedural); `analysisRef` decided (ship or retire the
  reservation); bulk-repin tooling shipped; a second namespace adopts a
  policy; drift fan-out re-validated at ≥500 consumers.
- **Stable:** the cluster twin shipped or its reservation retired;
  `bundledPrometheusAddress` deprecation completed; status.rollout
  unification (OEP-0017's open item) absorbs `status.canary` /
  `status.rolloutCoordination` or explicitly declines to.

## Drawbacks

1. **Two things look like truth**: spec says one plan, an active run may be
   executing another. This is the inherent cost of every snapshot system
   (Argo AnalysisRun, Kargo Promotion), paid deliberately; the drift
   condition, kubectl columns, and pin/repin events are the mitigation.
2. **A behavior change for inline users** who never adopted policies. The
   replaced behaviors are defect classes, but "edit the live ladder" was a
   workflow; repin is its replacement and requires retraining.
3. Status grows ~2–4 KB per in-flight roll (bounded, dropped at close), and
   `lastRun` keeps only a digest — post-mortem of a failed run's *full* plan
   needs the policy's git history until the archive open question lands.
4. The executor refactor (live spec → pinned plan) touches the OMENative
   rollout spine; the KIND suites are the safety net, and every mid-run-edit
   spec must be rewritten, which is review-heavy.
5. A new CRD + webhook + controller, the second policy CRD after
   AutoscalerPolicy — mitigated by sharing its config surface, digest
   construction, DELETE-denial pattern, and CLI.
6. The downgrade un-gate window is documented and procedurally gated, not
   structurally closed (open question 3).

## Alternatives

Three complete designs were developed independently and judged through three
lenses (admission honesty / silent-mis-execute risk; fleet-operator UX;
correctness under concurrent change). Scores, 1–10:

| Design | Honesty | Operator UX | Correctness | Verdict |
|---|--:|--:|--:|---|
| **B — per-group progression ref + pinned run (chosen)** | **8.5** | **8.5** | **9** | Wins every lens |
| A — analysis-only ref (`analysisRef`, the Argo/Flagger precedent) | 8 | 6.5 | 8 | No fatal flaw, scope-limited by intent |
| C — whole-RolloutSpec policy + declared componentShape | 6 | 6 | 6.5 | Two blocking flaws as designed |

- **A — analysis-only:** factor out only `RolloutAnalysis` + the provider
  ref; the step ladder stays inline. Shape-free and zero new race surface —
  but it strands the ladder (24 duplicated copies; a fleet-wide "add a 5%
  step" stays a 24-file change), leaves every live-read footgun alive, and
  its audit trail rides Events (~1h TTL), which expire before a multi-day
  canary completes. Its payload is a strict subset of B's. The step-level
  `analysisRef` remains a reserved future addition for fleets with
  heterogeneous ladders but shared gates.
- **C — whole-spec policy:** maximal per-consumer DRY, and its declared
  `componentShape` genuinely makes policy admission complete. But a mixed
  fleet re-copies the 40–55-line payload into shapes × tiers near-identical
  policies with no cross-copy drift detection — the duplication relocated,
  not removed; its ControllerRevision-based pin splits plan and counter
  across objects with no atomic write (the index-vs-plan skew reappears, plus
  a dangling gate read against a GC'd record); and an in-use edit to group
  membership can retro-violate update-time rules (pairing atomicity,
  lockstep) that static re-validation cannot catch. Its one field-best idea —
  multi-cluster inflate-at-derive — is adopted here.

Rejected mechanics, independent of the surface choice:

1. **Drop-absent-component semantics** — silently flips soak/collapse/
   maintainRatio validity in both directions and can delete the canary gate
   outright (`components` MinItems=1). The silent-mis-execute class,
   rejected outright.
2. **Placeholder components** (`entrypoint`, `allBackends`) — unnecessary
   once shape stays inline; would demote the count-dependent rules to
   per-consumer render errors and break "the policy body is a literal
   admissible fragment."
3. **Per-consumer params** — the 0013.1 doctrine holds verbatim: a
   threshold-only override on a shared query invites threshold/query
   mismatch that fails green. Ratio queries + ≤3 named variants cover the
   observed variance. Flagger's `templateVariables` is the cautionary
   precedent, not the model.
4. **Live-read + LKG (the 0013.1 model)** — right for continuous behaviors,
   wrong for bounded runs; LKG-across-runs replays stale gates against new
   revisions ([Anticipated objections](#anticipated-objections)).
5. **In-use edit-freeze as primary** (OpenKruise) — blocks fleet policy
   iteration on the slowest days-paused canary. Kept only as the
   non-blocking update warning + the kind-change freeze.
6. **Admission-time inflation into stored source specs** (Kargo) — permanent
   GitOps drift; OME's defaulter never touches `spec.rollout`. Inflation is
   used only where the object is system-authored: derived ISVCs.
7. **Run CRD / ControllerRevision pin storage** — loses plan/counter
   atomicity and adds an object lifecycle for a ≤1-per-ISVC concept; C's
   scores show the cost.
8. **Digest-only pinning** — detects drift but cannot execute through it:
   either wedges a healthy run or drifts. The plan itself must be stored.
9. **GitOps-templated inline blocks (no policy object)** — today's de-facto
   answer; N stored copies still drift independently with no runtime
   provenance and no mid-flight protection. Survives as how policies
   themselves are distributed.

## Open Questions

1. **Plan history** — v1 keeps only `lastRun` (digest + outcome). Archive the
   full pinned plan of non-Completed runs in ControllerRevisions GC'd by
   `revisionHistoryLimit` (the executor never reads them, so no dangling-read
   class)? Owner: this OEP's author; decide during implementation step 3.
2. **`analysisRef`** — reserved. Trigger to ship: a real fleet with
   heterogeneous ladders but shared gates. Owner: API reviewers.
3. **Downgrade posture** — can CRD schema/CEL make a ref-only group fail an
   *old* controller's validation loudly instead of defaulting to blueGreen
   (e.g. a required sentinel the old schema rejects)? If not, the procedural
   gate stands. Owner: this OEP's author; experiment before beta.
4. **Bulk repin tooling** — annotation sweep vs a `ome-policy repin --all`
   subcommand; decide with the first fleet-wide gate-tightening drill.
   Owner: release engineering.
5. **Verb forwarding for placed ISVCs** — the pinned-run status surface gives
   a future OEP the per-member progress visibility it needs; until then,
   placed canaries should use analysis or timed gates (preflight warns on
   bare manual pauses). Owner: OEP-0018.x maintainers.
6. **Cross-variant drift lint** — with ≤3 tier policies, `ome-policy check`
   could diff variants and flag divergence beyond thresholds. Cheap; decide
   at stage 6. Owner: this OEP's author.
7. **Status unification** — `activeRun` lands under `status.rollout`;
   `status.canary` and `status.rolloutCoordination` remain. OEP-0017 already
   tracks the consolidation; this OEP adds the constraint that the pinned
   plan and the step machine must stay co-located wherever they end up.

Next action: review of this OEP — in particular sign-off on run pinning
applying to inline rollouts (the one behavior change for existing users) and
on inflate-at-derive for multi-cluster. Implementation starts at step 1 of
[Implementation plan](#implementation-plan) only after those two calls and
the API shape are agreed.
