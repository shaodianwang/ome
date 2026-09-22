# OEP-0013.2: Cross-Component Scaling Coordinator

> **Status: provisional.** Follow-up to OEP-0013; nothing here is built. Checked against `main` at 2026-09-21. Resolves #1035.

<!-- toc -->
- [The decision](#the-decision)
- [Summary](#summary)
- [Motivation](#motivation) — goals, non-goals
- [Proposal](#proposal) — terminology, what must be true
- [Design Details](#design-details) — policy resolution, mechanism, rounding,
  follower rules, projector, API, Pinned, failure modes, OEP-0026 dependency
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## The decision

**The fact.** `InferenceService.spec.scalingPolicy` (and the same field as a
default on ServingRuntime / ClusterServingRuntime) is validated and stored,
and no controller reads it. On `main` the only code outside the API types and
generated files that mentions `ScalingPolicy` is
`pkg/validation/scalingpolicy.go` and the two webhooks that call it
(`pkg/webhook/admission/isvc`, `pkg/webhook/admission/servingruntime`). Since
PR #1084 those webhooks reject a new or changed `mode: Proportional` or
`mode: Pinned` with reason `ScalingModeNotImplemented`; `Independent`, empty,
and `nil` are accepted because they describe what the code already does. An
object stored with a rejected mode before #1084 still accepts unrelated
updates and behaves exactly like `Independent`, with nothing in status saying so.

**The owner's rule.** The engine never acts on the user's behalf beyond what
the spec says. A ratio the user wrote *is* the spec, so a coordinator that
enforces `decoder: "1"` is allowed. A coordinator that guesses a ratio, tunes
one from traffic, or scales a Component the user did not list is not.

| Option | What the user gets | Cost |
|---|---|---|
| (a) Build the coordinator | The ratio in the manifest becomes true in the cluster | A new writer of follower counts; one reconciler package; the projector and admission rules below |
| (b) Keep rejecting `Proportional` and `Pinned` at admission (today's state) | No silent no-op | The field promises a feature the product lacks; PD operators hand-roll the loop against `/scale` with no status or tests |
| (c) Remove the field | An honest API | Breaks a stored alpha field; the PD ratio requirement (OEP-0013) has no home |

**Recommendation.** Keep (b) — it is already on `main` — and build (a) as
this OEP, lifting the `Proportional` rejection in the same change that lands
the coordinator, never before. Remove `Pinned`
([Pinned: remove it](#pinned-remove-it)). If the owner picks (c), this
document is withdrawn and #1035 closes with the field removal.

## Summary

**Tl;dr:** one Component (the anchor) keeps scaling through its own HPA /
KEDA ScaledObject or fixed replica count; the ISVC controller copies the
anchor's *desired* count, multiplied by each declared ratio and rounded up,
onto each follower InferenceReplica through the `/scale` subresource.
Followers may not carry their own autoscaler. The resolved policy and each
follower's target-versus-actual count appear in a new `status.scalingPolicy`
block. No new CRD, no new spec field.

## Motivation

OEP-0013 shipped the field and sketched a coordinator that never became code.
The IR projector says so in its own comment: "until that coordinator wires
in, MinReplicas is the floor"
(`pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector/projector.go`).

The gap matters most for prefill/decode (PD) disaggregated serving, where a
decoder serves a fixed number of prefill engines and that ratio belongs to
the service, not to either Component. The reproduction in #1035 — engine
`minReplicas: 2`, decoder `minReplicas: 1`, `decoder: "1"` — leaves the
decoder at 1. Before #1084 the manifest lied; after #1084 it is rejected.
Neither delivers the ratio.

### Goals

- Make every acceptance criterion in #1035 checkable against one section here.
- State who writes each replica count, the rounding rule, what happens on
  conflict, and how failure shows up in status.
- Add no CRD and no spec field.

### Non-Goals

- No new autoscaler; the anchor scales as OEP-0013 built it.
- No ratio inference (the owner's rule).
- No coordinator for RawDeployment or MultiNode: no InferenceReplica, so
  `Proportional` stays rejected there (admission rule 5).
- No multi-cluster rule and no scale-to-zero (Open Questions 1 and 3).

## Proposal

### Terminology

- **Component** — an InferenceService's engine, decoder, or router.
- **InferenceReplica (IR)** — the per-Component object that owns the pods of
  an OMENative Component. `spec.replicas` is the *desired* Instance count and
  the target of the `/scale` subresource; `status.replicas` is the *observed*
  count. HPA and KEDA already write `spec.replicas` through `/scale`
  (`inferencereplica_types.go`, `component_dispatch.go`).
- **Anchor** — the one Component whose desired count drives the others. If
  the anchor is wrong, every follower is wrong.
- **Follower** — a Component listed in `proportional.ratios`. The coordinator
  is the only writer of its desired count.
- **Resolved policy** — the policy after ISVC-over-runtime resolution; `nil`
  means Independent, i.e. today's behavior.

### What must be true

1. **No silent no-op.** An admitted policy is enforced, or its failure to
   apply is written to `status.scalingPolicy`.
2. **One writer per desired count.** Follower: the coordinator. Anchor: its
   autoscaler, or the projector when it has none. A second writer is rejected
   at admission or held with a condition — never fought at reconcile cadence.
3. **One rounding rule, integer arithmetic.** The same anchor count gives the
   same follower target on every controller build.
4. **Level-based.** After a restart or missed event, the live pair (anchor
   desired, follower desired) fully determines the next write. Status is
   output only.
5. **No hardcoded defaults.** This design adds no tunable; if one appears it
   lives in `inferenceservice-config` and the chart, not in a Go literal.

## Design Details

### Which policy applies

ISVC block wins over the runtime block, whole-block, no merge — the rule
`ResolveComponentAutoscaler` already uses for the per-Component autoscaler
(`reconcilers/autoscaler/resolve.go`) and the rule the runtime field's doc
comment states ("Replaced wholesale by isvc.spec.scalingPolicy").

```go
// ResolveScalingPolicy returns the policy that applies and where it came from:
//  1. isvc.Spec.ScalingPolicy != nil    -> (isvc copy, "isvc")
//  2. runtime.Spec.ScalingPolicy != nil -> (runtime copy, "runtime")
//  3. otherwise                          -> (nil, "")  — Independent.
```

An ISVC `mode: Independent` therefore masks a runtime `Proportional` default:
the opt-out for one tenant of a shared runtime, visible through the status
`source` field. A merge (ISVC anchor, runtime ratios) would be a policy
nobody wrote.

### How the coordinator scales followers

The coordinator is a package under
`pkg/controller/v1beta1/inferenceservice/reconcilers/`, called from the ISVC
reconcile after the IRs are projected — not a separate controller. The ISVC
controller already `Owns(&v1beta1.InferenceReplica{})`
(`inferenceservice/controller.go`), so an HPA or KEDA write to the anchor IR
already enqueues the parent ISVC.

**Input: the anchor IR's `spec.replicas`.** This revises OEP-0013's sketch,
which read `status.replicas`. Latency: when KEDA scales the engine, the
decoder must start provisioning in the same reconcile, not after the engine
pods are Ready, or the ratio is broken for the whole warm-up window.
Stability: observed counts move during every rollout; desired counts move
only when a scaler or a human acts. For an anchor with
`autoscaler: {class: None}`, `spec.replicas` is the projector's stamp of
`minReplicas`, so a fixed-count anchor takes the same path. The cost: if the
anchor cannot schedule, followers match a count that never arrives —
accepted, since in PD serving an idle decoder costs money and a missing one
blocks the pipeline.

**Output: a `/scale` update on each follower IR** whose live desired count
differs from its target (`client.SubResource("scale")`, the call the envtest
suite already uses to imitate HPA); steady state is a read and no write.
`/scale` is the path HPA, KEDA, and `kubectl scale` already use; the IR
validating webhook matches `resources=inferencereplicas` only and so does
not gate it; and the manager already holds `inferencereplicas/scale` in
`config/rbac/role.yaml`. Per reconcile: resolve, validate, compute every
target, write each follower that differs, write status. A failed write
returns the error and controller-runtime requeues; followers already written
stay written, since each is independently the correct level-based target.

### The rounding rule

```
target(follower) = clamp(ceil(anchorDesired × ratio),
                         max(1, follower.minReplicas),
                         follower.maxReplicas)
```

**Round up**, because the two directions cost different things: rounding
down under-provisions the follower (for a decoder, prefill work with nowhere
to go); rounding up over-provisions by at most one replica. With
`ratio: "0.25"` and 5 anchor replicas the target is 2, not 1. No per-follower
rounding knob.

**Integer arithmetic.** `ratios` values are `resource.Quantity`. Admission
bounds them to `(0, 1000]` with at most three decimal places; the coordinator
computes `(anchorDesired*ratio.MilliValue() + 999) / 1000`. No float
conversion, so two controller builds agree on the same manifest. A ratio of
`0` is rejected: holding a follower at zero is a bounds question.

**Clamping is visible.** When bounds truncate the target, the coordinator
writes the clamped value, sets `clamped: true` on that follower's status
entry, and emits a warning event. The `max(1, minReplicas)` floor reuses the
projector's existing floor (`replicasFromComponentExt`); that floor and
`defaultDispatchMinReplicas` are pre-existing in-code defaults, flagged here
and left to the scale-to-zero work.

### Followers cannot have their own autoscaler

Admission rules when the policy on the object being admitted is
`Proportional`; the same rules run on ServingRuntime / ClusterServingRuntime
against the runtime's own `scalingPolicy` and per-Component blocks.

1. `proportional` is required and `ratios` is non-empty. A `proportional`
   block under a non-Proportional mode is rejected: a stored block that does
   nothing is a stored lie.
2. `anchor` names a Component present in the spec and is not in `ratios`.
3. Every `ratios` key names a Component present in the spec; values are
   bounded per the rounding rule.
4. A follower with an explicit `autoscaler` block of class `HPA`, `KEDA`, or
   `External` is rejected; `None` is accepted. This tightens OEP-0013, which
   allowed `External` on followers: two writers of one `/scale` target cannot
   both win, and "someone else scales this Component" already has a
   spelling — leave it out of `ratios`.
5. The Component must be OMENative (have an IR). For RawDeployment and
   MultiNode there is no scale target the coordinator owns.

**What admission cannot see.** A follower with no autoscaler block resolves
to the default CPU-80% HPA (`SpecSourceDefault`), which would conflict.
Rather than require `autoscaler: {class: None}` on every follower, a
default-sourced block on a `ratios` Component resolves to `{Class: None}` and
dispatch generates nothing for it. An *explicit* HPA/KEDA/External block
admission could not see — a runtime edited after the ISVC was admitted — is
not overridden; the coordinator holds: no follower writes, `Ready=False /
FollowerAutoscalerConflict`, warning event. Deleting the operator's
ScaledObject as a side effect of a policy is the kind of unrequested action
the owner's rule forbids, and the dispatch layer already preserves foreign
scalers (`reconcilers/autoscaler/dispatch.go`).

### The projector must not undo the coordinator

Today `desiredReplicas` in `irprojector/projector.go` preserves the live
`spec.replicas` only when the resolved class is not `None`; for `None` it
re-stamps `minReplicas` on every update. Wired as-is, each ISVC reconcile
would reset every follower to `minReplicas` and the coordinator would raise
it again — a permanent write loop. The projector gains one case: a follower
of the resolved policy keeps its live count on update, like an
autoscaler-managed Component. The create path is unchanged (`minReplicas`
first; the coordinator raises it on the next pass).

### API: the existing field plus one status block

No new CRD, no new spec field. `ScalingPolicy`, `ScalingMode`, and
`ProportionalPolicy` in `pkg/apis/ome/v1beta1/autoscaler.go` stay as they
are, except that `Pinned` leaves the enum in Beta. One block is added to
`InferenceServiceStatus`:

```go
// ScalingPolicyStatus reports the resolved cross-Component scaling policy
// and the coordinator's last decisions. Output only; never read back.
type ScalingPolicyStatus struct {
    Mode          ScalingMode   `json:"mode,omitempty"`   // "" when nothing resolves
    Source        string        `json:"source,omitempty"` // isvc | runtime
    Anchor        ComponentType `json:"anchor,omitempty"`
    AnchorDesired int32         `json:"anchorDesired,omitempty"`
    Followers     []FollowerScalingStatus `json:"followers,omitempty"` // one per ratios key
    Conditions    []metav1.Condition      `json:"conditions,omitempty"` // Ready
}

type FollowerScalingStatus struct {
    Component ComponentType     `json:"component"`
    Ratio     resource.Quantity `json:"ratio"`
    Target    int32             `json:"target"`            // after clamping
    Current   int32             `json:"current,omitempty"` // follower IR spec.replicas
    Clamped   bool              `json:"clamped,omitempty"`
}
```

`Independent` or `nil` writes `mode` and `source` only. The block is bounded
(at most two followers today); ISVC status size has real write cost
(OEP-0007.4).

### Pinned: remove it

`Pinned` is documented in source as "Reserved for a future release; not
implemented" and rejected since #1084. Its only proposed meaning — fixed
counts, no autoscaler — is already `autoscaler: {class: None}` plus
`minReplicas`; the other reading, "all Components equal", is `Proportional`
with every ratio at `1`. **Remove it**, in two phases, because dropping a
CRD enum value blocks *every* update to a stored object still carrying it:

1. **Alpha:** `Pinned` stays in the enum and stays rejected on new or changed
   writes. The coordinator treats a stored `Pinned` as unresolved:
   `Ready=False / ModeNotImplemented`.
2. **Beta:** drop `ScalingPinned` from the Go constants and the CRD schema,
   with a one-line audit in the release notes
   (`kubectl get isvc,servingruntime,clusterservingruntime -A -o json | jq
   '..|.scalingPolicy?|select(.mode=="Pinned")'`). Stragglers decode fine
   (`ScalingMode` is a string) but cannot be updated until the policy is
   removed or set to `Independent`, which is always admitted.

### Failure modes and restart

| Failure | Behavior |
|---|---|
| Anchor IR missing or `spec.replicas` nil | Hold all follower writes; `Ready=False / AnchorUnavailable`. Followers keep their last count — dropping to a floor on a transient would manufacture an outage. |
| One follower IR missing | Write the followers that exist; `Ready=False` naming the missing one. |
| `/scale` conflict (409) or transient API error | Return the error; controller-runtime backoff. No bespoke retry loop. |
| Follower scaler found at reconcile (cross-object) | Hold; `Ready=False / FollowerAutoscalerConflict`; the scaler is never deleted. |
| Follower `maxReplicas` starves the ratio | Write the clamped value; `clamped: true`; warning event. Admission-time rejection is Open Question 2. |
| Human runs `kubectl scale` on a follower | Overwritten to the ratio target on the next reconcile; visible as `current` vs `target` until then. The policy owns the count while it stands. |
| Anchor desired goes to 0 | Follower clamps to the floor; Open Question 1. |
| Controller restart or missed event | Nothing to recover: the next reconcile recomputes from live desired counts. |

### Dependency on OEP-0026

OEP-0026 makes the IR public with two forms: *inline* (the ISVC creates and
owns the IR — today's only form) and *referenced* (`replicaRefs`; the user
owns the IR). Its section 3 lets the ISVC write a referenced IR only in the
*rollout-control* fields (partition, pacing, rollback target, pause), and its
section 4 lists `replicas` as a *user* field.

This coordinator writes `spec.replicas` on follower IRs. Inline, that is
within today's ownership. Referenced, it is a write to a user field on a
user-owned IR — the same shape as the rollout-control writes 0026 is
deciding in its Open Question 1. Therefore 0026's ownership rule must either
name the scaling-coordinator follower write as a permitted composer write,
scoped to IRs the resolved policy lists in `ratios`, or state that
`Proportional` is rejected when any follower is referenced. Either fits this
OEP; the second costs the referenced form the PD ratio. Also for 0026 to
name: its admission-by-identity matches `resources=inferencereplicas` and
does not see `/scale` writes (HPA's and KEDA's included), so enforcing
`replicas` ownership on referenced IRs must cover the scale subresource.
Until 0026 decides, this OEP targets the inline form — every IR today.

## Test Plan

- **Unit:** `ResolveScalingPolicy` table (isvc, runtime, nil), including ISVC
  `Independent` masking a runtime `Proportional`; rounding table over ratios
  `1`, `0.25`, `500m`, `1.5`, `0.001` and anchors 0/1/5/1000 with clamping,
  checked against a big-decimal oracle; follower default-to-None override;
  every admission rule; projector keeps follower counts on update; status
  construction for every `Ready=False` reason.
- **envtest** (`tests/integration/singlecluster/webhook/inferenceservice`,
  `.../webhook/servingruntime`): admission matrices on ISVC and runtime for
  every rule; the update ratchet still passes unrelated writes on objects
  stored with `Pinned`; `Pinned` rejected on new writes throughout.
- **KIND** (new spec file in the OMENative KIND suite, classified in the
  pipeline generator's spec groups): `decoder: "1"`, `kubectl scale` the
  anchor IR to 3, decoder IR reaches desired 3 and 3 Ready Instances (the
  #1035 criterion); ratio `0.25` through anchor 1, 4, 5, 8; manual follower
  scale reverted under pod churn; policy active during a RecreatePod rollout
  with no count fights; a runtime edit adding a follower KEDA block holds.

## Graduation Criteria

- **Alpha:** coordinator, resolution, projector case, admission rules, status
  block; `Proportional` rejection lifted in the same change; `Pinned` still
  in the enum and rejected; unit and envtest green.
- **Beta:** `Pinned` removed with the audit note; KIND specs green; one
  release without status renames; OEP-0026's ownership rule names the write.
- **Stable:** two Beta releases without a coordination-attributed incident.

## Drawbacks

- One more writer of IR desired counts, next to the projector, HPA, KEDA,
  and `kubectl scale`; bounded by the one-writer rule, but more to hold in mind.
- The projector learns about the scaling policy; the alternative (re-raising
  counts after every projector reset) is a permanent write loop.
- Rejecting `External` on followers tightens OEP-0013's sketch, and a
  rejected-but-present enum value lives in the API for one release.

## Alternatives

1. **Keep rejecting forever, or remove the field** — options (b) and (c) in
   [The decision](#the-decision).
2. **A standalone controller watching IRs.** Duplicates the watch the ISVC
   controller already has and adds a second writer identity.
3. **Track anchor `status.replicas` (OEP-0013's sketch).** Breaks the ratio
   for every warm-up window and flaps during rollouts.
4. **Transfer ownership of a conflicting follower scaler** (adopt or delete
   it). A policy edit that deletes an operator's ScaledObject is the kind of
   unrequested action the owner's rule forbids.
5. **Round to nearest or down.** Saves at most one replica at the cost of an
   under-provisioned decoder.
6. **Implement `Pinned`.** A third mode with no meaning not already spelled
   another way.

## Open Questions

1. **Anchor at zero.** When the anchor can scale to zero, do followers track
   to zero or hold at the floor? This OEP keeps the floor; the scale-to-zero
   work owns floor semantics.
2. **Reject unsatisfiable bounds at admission?** A follower `maxReplicas`
   below `ceil(anchor.maxReplicas × ratio)` could be rejected instead of
   clamped. Stricter, but it couples two Components' bounds and breaks the
   "tighten bounds during an incident" workflow. Current position: clamp
   visibly; revisit with Alpha feedback.
3. **Multi-cluster.** Placement copies the ISVC spec onto each member
   (`placement/derive.go`), so each member's coordinator acts on its local
   IRs. Whether the ratio must hold across members mid-migration is open.

This applies to OMENative Components with an InferenceReplica; RawDeployment
and MultiNode are unaffected. Next action: the owner picks (a), (b), or (c);
if (a), confirms desired-count input, round-up, hold-not-transfer, and
`Pinned` removal, and the dependency is filed against OEP-0026's Open
Question 1.
