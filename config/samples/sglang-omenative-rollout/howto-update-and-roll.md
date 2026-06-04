# How-to: update & roll a model safely

*Task recipe — page 3 of 4. Prev: [Write a runtime](howto-write-a-runtime.md) ·
[Launch a model](howto-launch-a-model.md) · Next:
[Multi-node & PD](howto-multi-node-and-pd.md)*

Two separate questions, don't conflate them:

- **What changed?** A new image, a new checkpoint, new launch flags.
- **How should it roll out?** All at once, surge-then-drain, blue/green, staged.

For the *why* behind any of this — the per-group state machine, the drain
mechanism, the `readyReplicas` vs `servingReplicas` distinction — read the
[OMENative guide](guide-omenative.md). This page is the task recipe.

## Read this first: rollout knobs only work in OMENative mode

A plain (RawDeployment) service rolls like any Kubernetes Deployment — edit it,
pods roll, done. **Everything below (`lifecycle`, `spec.rollout.coordination`,
blue/green) only does anything in OMENative mode.**

Opt in with the typed `spec.deploymentMode` field — set it **once** and it
propagates to every component:

```yaml
spec:
  deploymentMode: OMENative   # propagates to engine / decoder / router
  engine:
    ...
  decoder:
    ...
```

> Older samples set `ome.io/deploymentMode: OMENative` as an annotation on
> *each* component. That still works — a per-component annotation **overrides**
> the field, which is handy for mixed-mode (e.g. one component on RawDeployment) —
> but you no longer need it: `spec.deploymentMode` covers the whole service.

**Verify before you apply.** Coordination and lifecycle changes are validated by
an admission webhook (Sequential needs an `order`, zero-budget pacing is
rejected, etc. — see [Validation](#validation-what-the-webhook-rejects)). Catch
those locally:

```sh
kubectl apply --dry-run=server -f inferenceservice.yaml
```

## What triggers a rollout (and what doesn't)

OMENative tracks each component's pod template as a saved snapshot — a
Kubernetes **ControllerRevision** named `<isvc>-<component>-<hash>`. The hash is
computed from the **fully-rendered pod template** (runtime merged with your ISVC
overrides: image, command/args, env, resource requests/limits, volumes, init
containers, plus pod-template labels/annotations, and the worker template for
multi-pod components). When that hash changes, a new revision is born and the
component rolls.

**Triggers a new revision (rolls the pods):**

- container **image** or **command/args** change (most common: a new sglang build or flag)
- **env** change — including `CC_MODEL_PATH` repointing to a new checkpoint
- **resource requests/limits** change
- **volumes / init containers** change
- **model / runtime** selection that changes the rendered template
- a **worker-only** image/env/resource bump on a multi-pod component
- pod-template **label / user-annotation** changes

**No-op (does NOT create a revision, pods stay put):**

- adding/removing a **migration request** annotation
- setting/clearing `ome.io/rollout-paused`
- transient operational state the controller writes itself (drain markers,
  operation IDs, anti-affinity for an in-flight migration, the serving
  readiness gate)

So a pause annotation never "phantom-rolls" an unrelated component, and a
migration request on one instance doesn't restart the others.

### Shipping a new image or checkpoint

**New image / flags (most common):** edit the runtime's `engineConfig.runner`.
By default the ISVC has `runtime.autoSync: true`, so it re-renders pod specs from
the live runtime on each reconcile. Note, though, that a runtime edit does **not**
by itself trigger a reconcile of `autoSync: true` ISVCs — the runtime-change
watch fan-out only enqueues `autoSync: false` (pinned) ISVCs, so the new spec is
picked up on the next reconcile driven by another event.

**New checkpoint:** the checkpoint lives on the runtime — the `CC_MODEL_PATH` env
(a `manifold://` path) consumed by `--model-path`. Point it at the new checkpoint
(edit the ServingRuntime, or bump your chart values and re-apply); with
`runtime.autoSync: true` the ISVC re-renders from the live runtime on the next
reconcile (again, the runtime edit alone won't re-enqueue an `autoSync: true`
ISVC — it picks the change up on the next reconcile driven by another event).

(If you instead manage weights with a `spec.model`, swap `spec.model.name` on the
ISVC — see [Models & InferenceService](guide-models-and-inferenceservice.md).)

## How it rolls: two layers

### Layer 1 — how *one* component's pods replace themselves

Per component, `lifecycle.updateStrategy.type`:

| Type | Behavior | Cost | Pick it when |
|---|---|---|---|
| `SurgeThenDrain` *(default)* | start a new pod, wait until it serves, then drain the old | +1 pod, zero traffic gap | you have a spare accelerator slot and want no gap |
| `RecreatePod` | delete old, then create new | brief capacity dip, no extra GPU | you're GPU-constrained and a short dip is fine |
| `InPlaceIfPossible` | patch the pod in place if the diff is **regular-container-images-only**, else recreate | none (when in-place) | image-only bumps where you want to keep the pod and weights on disk |
| `InPlaceOnly` | in place if eligible, else **reject** (single-pod) / fall back to recreate (multi-pod) | none | you want a hard guarantee the change is image-only |

```yaml
  engine:
    minReplicas: 2
    maxReplicas: 2
    lifecycle:
      updateStrategy:
        type: SurgeThenDrain
        rollingUpdate: { maxSurge: 1, maxUnavailable: 0 }
```

GPU-constrained? Use `RecreatePod` with `maxUnavailable: 1` and `maxSurge: 0` —
rolls in place of capacity instead of above it.

**What "in-place capable" actually means.** A diff is in-place eligible only when
the rendered pod spec is identical **apart from regular-container images**. An
env change, an args/command change, a resource change, a volume change — or even
an **init-container** image change — is *not* in-place eligible and routes to
recreate. So `InPlaceIfPossible` quietly recreates for anything but a plain
image bump, and `InPlaceOnly` on a single-pod instance **rejects** such a change.
In-place updates keep the same pod name and incarnation (no pod-UID rotation);
recreate rotates the pod.

> Multi-pod (leader+worker) components always **downgrade any in-place mode to
> recreate** — in-place compares only the leader template, so a worker-only
> change would split-brain the gang. `InPlaceOnly` on multi-pod silently falls
> back to recreate instead of rejecting.

### Layer 2 — how components roll *relative to each other*

Only matters with ≥2 components (e.g. engine + decoder). `spec.rollout.coordination`:

| Policy | What happens | Best for |
|---|---|---|
| `RollingUpdate` | all components roll in parallel, paced; mixed old/new tolerated | steady, capacity-efficient |
| `Sequential` | one component fully rolls before the next (`order` required) | validate-then-proceed |
| `BlueGreen` | bring up a full new copy of every component, then flip traffic atomically | atomic cutover + instant rollback (~2× GPU) |
| `Independent` *(default)* | each rolls on its own | components that don't move together |

Components not listed in any group roll independently — that's the default with
no `spec.rollout.coordination` at all. A component may appear in **at most one**
group.

## maxSurge / maxUnavailable — the budget (and a deadlock to avoid)

Both layers use the same two knobs:

- **`maxSurge`** — how many *extra* pods you allow above the desired count during
  the roll. Higher = faster + more GPU headroom needed.
- **`maxUnavailable`** — how many pods you'll let drop below ready at once.

Each accepts a plain integer (`1`) or a percentage (`"25%"`); percentages resolve
to `ceil(replicas * pct / 100)` at reconcile time.

**Which knob gates depends on the strategy.** `SurgeThenDrain` never takes a pod
offline (it adds first, drains after), so it is gated by **`maxSurge`** and
side-steps `maxUnavailable` entirely. `RecreatePod` and in-place updates remove
or disrupt a pod first, so they are gated by **`maxUnavailable`**.

**The deadlock.** `maxUnavailable: 0` with a drain-first strategy
(`RecreatePod` / in-place) on a single-replica component **cannot start** — there
is no budget to take the one pod down and no surge to bring a replacement up
first. The webhook rejects an *explicit* `maxSurge: 0` AND `maxUnavailable: 0` on
the same component (and on a coordination group) as unstartable. To roll a
single replica with no spare GPU, give it `maxUnavailable: 1`.

**Two surge budgets compose — the smaller wins.** When a component is in a
coordination group, two *independent* caps apply: its per-component
`lifecycle…rollingUpdate.maxSurge` (extra pods this one component may add) and the
group's `pacing.maxSurge` (extra pods across the *whole* group, so two peers
rolling at once don't each surge to their full budget). A pod surges only when
**both** layers allow — the effective cap is `min(group, per-component)`. Leave the
per-component value unset and that layer doesn't cap, so the group budget is the
only constraint (the common case). The defaults you get if you declare a `pacing`
block but leave the values unset depend on which path applies: the admission **webhook**
defaults `maxSurge: 25%`, `maxUnavailable: 0`, `type: PerComponent`,
`ratioTolerancePercent: 5` (the last only when `type: RatioBalanced`), while the
**runtime resolver** defaults `maxUnavailable` to `25%` (a default of `0` would
deadlock single-replica rollouts). These are runtime/webhook defaults, not
apiserver/kubebuilder CRD defaults — they land only for components that resolve
to OMENative.

> There is **no** `minReady` / `minReadySeconds` knob on the `pacing` struct or
> exposed as a field on the `lifecycle` update-strategy types. The full pacing
> knob set is exactly `type`, `maxSurge`, `maxUnavailable`, `ratioTolerancePercent`. The
> group-level `soak` (Sequential only) and the in-place `gracePeriodSeconds`
> (default `30`) are wait knobs. (A `minReadySeconds` field does exist on the
> underlying replica spec, propagated from the ISVC, and gates a pod's
> availability — but it isn't a `pacing` or update-strategy knob.)

## Coordinating multiple components

`spec.rollout.coordination.groups[]` couples ≥2 components so they roll under one
policy. Each group needs `components` (≥1, but ≥2 for RollingUpdate/Sequential)
and a `policy`.

### RollingUpdate — roll in parallel, tolerate mixed revisions

Components roll together in small paced steps; any vN pod may talk to any vM pod
in the group during the window. This is the recommended shape for PD when paired
with `RatioBalanced` pacing.

```yaml
spec:
  rollout:
    coordination:
      groups:
        - components: [engine, decoder]
          policy: RollingUpdate
          pacing:
            type: RatioBalanced     # roll prefill:decode in lockstep, never lopsided
            maxSurge: 1
            maxUnavailable: 0
            ratioTolerancePercent: 5       # keep prefill:decode within 5% of the starting ratio
```

**Use this when** you want steady, capacity-efficient upgrades and your
components can interoperate across a brief version skew — i.e. the normal case
for prefill/decode. Note: on **update**, a RollingUpdate group must bump every
member's image *together* — a partial bump is rejected (lockstep).

### RatioBalanced pacing for PD

`pacing.type: RatioBalanced` surges the components *in lockstep*, holding the
cross-component ratio (e.g. prefill:decode = 1:1) within `ratioTolerancePercent` percent
of where it was at rollout start. The two pools come up in proportion, so a
new-revision prefill never ends up talking to an old-revision decode across a
lopsided skew. The anchor ratio is snapshotted once at rollout start and held
across reconciles (so scaling mid-roll doesn't move it).

`pacing.type: PerComponent` (the default) adds no cross-component ratio coupling —
each component just rolls under the `min(group, per-component)` surge and
unavailable caps.

> **RatioBalanced is bypassed for in-place strategies.** With
> `InPlaceIfPossible` / `InPlaceOnly`, each drain is paired 1:1 with the same pod
> coming back, so net capacity is ~0 and there is no ratio to skew — the ratio
> check is skipped. The remaining gates (`maxUnavailable` and Sequential
> coordination) still govern concurrency. (You'll see a `RatioGateBypassed`
> event.)
>
> **RatioBalanced is rejected on Sequential groups** — only one component rolls
> at a time there, so there is no pair to balance.
> See [Multi-node & PD](howto-multi-node-and-pd.md) for the full PD rationale.

### Sequential — one component fully rolls before the next

```yaml
spec:
  rollout:
    coordination:
      groups:
        - components: [engine, decoder]
          policy: Sequential
          order: [decoder, engine]   # REQUIRED for Sequential
          soak: 10m                  # dwell on the new revision before the next component starts
```

`order` is **required** and must cover every component in the group. The next
component cannot enter `Surging` until the previous one is back at `Idle` on its
new revision — a hard readiness gate. The optional `soak` adds a dwell *after*
the previous component reaches `Idle` before the next starts, so you can validate
one pool under live traffic before risking the next (`soak: 0`, the default,
starts the next immediately). A later-in-order component bumped while an earlier
one is still rolling is silently deferred (you'll see a
`SequentialNextSpecBumpDeferred` warning).

**Use this when** you want to check one component before risking the next.
*Avoid* it for PD interoperation — it *maximizes* the version-skew window between
prefill and decode.

### BlueGreen — atomic cutover

```yaml
spec:
  rollout:
    coordination:
      groups:
        - components: [engine, decoder]
          policy: BlueGreen
          # order: [decoder, engine]   # has no effect on surge sequencing in BlueGreen
```

**Use this when** you want an atomic cutover and can afford ~2× the accelerators
for the duration. See [Blue/green end to end](#bluegreen-end-to-end) below.

### Independent — each rolls on its own

The default behavior, made explicit. When no coordination block is declared at
all, a component is uncoordinated: no cross-component gating, no group
surge/unavail budgets. Declared as an **explicit** `Independent` group, the
component still has group membership, so it picks up the group's default pacing
(`25%`/`25%`) and its per-component `maxSurge`/`maxUnavailable` budgets *are*
enforced — but there is still no cross-component ratio/sequential gating, and
`order` is ignored. Declare it as an explicit assertion that you considered
coordination and chose against the cross-component policies.

```yaml
spec:
  rollout:
    coordination:
      groups:
        - components: [engine, decoder]
          policy: Independent
```

## Blue/green end to end

Atomic cutover: a full new copy of every component in the group comes up
alongside the old; once **every** new pod serves, traffic flips; if any component
fails to become Ready the whole group goes `Failed` and traffic never flips. The
new set is **not** automatically torn down — the group holds at `Failed` until
the operator reverts the failed component's spec.

What "switch" means under the hood: OMENative keeps **one routing Service per
`(component, revision)`** and writes per-revision **weights** into
`status.components.<c>.traffic[]`. Traffic is apportioned proportionally to live
per-revision pod counts every reconcile, so weights follow pod readiness rather
than flipping a single Service's selector. `order` has **no effect on surge
sequencing** in BlueGreen — all components surge in parallel and traffic-shifts
together once every component's new-revision pods are Ready.

**Do it:**

```sh
# 1. set the group policy to BlueGreen (YAML above), then ship your change
kubectl apply --dry-run=server -f inferenceservice.yaml   # verify first
kubectl apply -f inferenceservice.yaml

# 2. watch it surge the full new stack, then flip
kubectl get isvc <name> -n <ns> -o jsonpath='{.status.rolloutCoordination.groups[*].phase}'
# Surging -> Waiting -> Shifting -> ScalingDown -> Idle
```

**Verify the flip:**

```sh
# weights live here — after the flip the new revision's Service holds 100%
kubectl get isvc <name> -n <ns> -o jsonpath='{.status.components.engine.traffic}'
# the per-revision Services it points at:
kubectl get svc -n <ns> -l ome.io/inferenceservice=<name>
# fully-rolled-out revision once the group is Idle:
kubectl get isvc <name> -n <ns> -o jsonpath='{.status.components.engine.latestRolledoutRevision}'
```

**Abort before the flip:** the old copy is still up and still serving until
`ScalingDown` deletes it. To revert, change the spec back (or pin to the previous
revision; see [Roll back](#roll-back--when-a-rollout-is-stuck)) and re-apply —
the prior revision becomes the target again and the now-stale new pods are scaled
down through the normal `Surging` → `Waiting` → `Shifting` → `ScalingDown` machinery
(traffic again following pod counts).

## Sequential rollout in practice

`order` walks the components one at a time. The gate between them is real: the
controller will not start the next component's surge until the previous one is
`Idle` on its new revision **and** (if set) the `soak` window has elapsed,
measured from the previous component's `Available` condition flipping True. While
waiting, the group sits at composite phase `Sequential.AwaitingNextComponent`.

```sh
# which component is rolling, and where the group is parked:
kubectl get isvc <name> -n <ns> \
  -o jsonpath='{.status.rolloutCoordination.groups[0].compositePhase}'
# e.g. decoder.Surging   ... then ... Sequential.AwaitingNextComponent ... then engine.Surging
kubectl get isvc <name> -n <ns> \
  -o jsonpath='{.status.rolloutCoordination.groups[0].currentComponent}'
```

## Observing a rollout

Watch the live coordination status — this is the surface the controller actually
writes (populated only when `spec.rollout.coordination` is declared):

```sh
kubectl get isvc <name> -n <ns> -w
kubectl get isvc <name> -n <ns> -o jsonpath='{.status.rolloutCoordination.groups[*].phase}'
# Idle -> Surging -> Waiting -> Shifting -> Draining -> ScalingDown -> Idle
# (or Failed / Paused)
```

What the phases mean: `Idle` steady (no roll) · `Surging` creating new-revision
pods · `Waiting` waiting for new pods to reach Ready · `Shifting` writing new
traffic weights · `Draining` waiting for in-flight requests to drain ·
`ScalingDown` deleting old pods · `Failed` permanent error (operator action) ·
`Paused` held by you.

More status to read:

```sh
# human-readable phase + active component (e.g. "decoder.Surging"):
kubectl get isvc <name> -n <ns> -o jsonpath='{.status.rolloutCoordination.groups[*].compositePhase}'
# live prefill:decode counts when using RatioBalanced — your ratio holding:
kubectl get isvc <name> -n <ns> -o jsonpath='{.status.rolloutCoordination.groups[*].observedRatio}'
# the readiness condition (True/Idle, False/InProgress, False/Failed, Unknown/Pending):
kubectl get isvc <name> -n <ns> -o jsonpath='{.status.conditions[?(@.type=="RolloutCoordinationReady")]}'
```

Per-component health under `status.components.<c>.lifecycle`:
`replicas`, `readyReplicas`, `servingReplicas`, `updatedReadyReplicas`.

> `readyReplicas` = pods whose containers pass their probes. `servingReplicas` =
> pods *also* in the load balancer's rotation. They **diverge** during a drain or
> an in-place update, when OME holds a pod out of traffic while it's still
> technically ready — and the `maxUnavailable` budget gates against
> `servingReplicas`, not `readyReplicas`. `updatedReadyReplicas` is how far the
> roll has progressed. (Details in the [guide FAQ](guide-omenative.md).)

Events and conditions are the other half — `kubectl describe isvc <name>` shows
coordination events (`CoordinationGroupSurging` / `Shifting` / `Completed` /
`Failed`, `RatioSkewRejected`, `RatioGateBypassed`, `MixedPairingObserved`,
`SequentialAwaitingNext`, `ConsiderCoordinationGroup`).

> The per-component `status.components.<c>.rolloutPhase` field (with values like
> `Stable` / `Canarying` / `Promoting`) exists in the CRD but is **reserved and
> not populated** today. Read `status.rolloutCoordination` instead.

## Roll back / when a rollout is stuck

### How rollback works today

There is **no traffic-revert verb** in the live path. The `ome.io/rollout-rollback`
and `ome.io/rollout-promote` annotations are defined as constants and have their
values validated by the admission webhook, but they are **reserved no-ops** tied
to the unbuilt canary executor — no reconciler/controller consumes them, and the
CanarySpec they drive is admission-rejected until a step executor exists. The two
operative moves are:

- **Pin to a previous revision** — the durable rollback. Find the revision and
  pin to it:
  ```sh
  kubectl get controllerrevision -n <ome-namespace> -l ome.io/inferenceservice=<name>
  ```
  ```yaml
  spec:
    runtime:
      name: my-runtime
      autoSync: false
      revision: <controllerrevision-name>   # roll back by pointing at a previous one
  ```
  `autoSync: false` stops the ISVC re-rendering from the live runtime (so
  unrelated runtime edits don't restart your pods), and `revision:` pins the pod
  template to that saved snapshot. Pinning to the *previous* snapshot rolls the
  service back to it through the normal lifecycle/coordination path.

- **Pause** an in-flight OMENative rollout (freeze every coordination group at its
  current phase):
  ```sh
  kubectl annotate isvc <name> ome.io/rollout-paused=true
  # resume by removing it:
  kubectl annotate isvc <name> ome.io/rollout-paused-
  ```
  This is the only rollout-control annotation the controller actively **acts on**
  today (`rollout-promote` / `rollout-rollback` / `rollout-ready-timeout` are
  declared and admission-validated but inert until a step executor lands). While
  paused, every group in the OMENative coordination path reports phase `Paused`
  and authorizes no new surge.

Pause vs pin: **pause** freezes the roll exactly where it is (both revisions
coexist, nothing advances); **pin** drives the service back to a chosen snapshot
through the normal roll.

### The old revision keeps serving

A new-revision pod that can't reach Ready does **not** take down the service. The
old revision stays up and in rotation until the new one is verified — that's the
point of `SurgeThenDrain` and BlueGreen. A stuck roll shows as phase `Failed`;
`RolloutCoordinationReady` goes `False/InProgress` or `False/Failed`. Note:
`RolloutCoordinationReady` is **informational** — it is *not* part of the
aggregate `Ready` condition, so a mid-flight roll doesn't by itself flip the
ISVC's top-level Ready to False.

### Triage when a rollout is stuck

1. `kubectl describe isvc <name>` — read the conditions and events first.
2. Pods pending? `kubectl get pods -l ome.io/inferenceservice=<name>` then
   `kubectl describe pod <pod>` — usually unschedulable (GPU/quota) or
   `ImagePullBackOff`. A persistent `ImagePullBackOff` past the deadline
   escalates the group to phase `Failed`.
3. **Lifecycle/coordination config doing nothing?** You almost certainly forgot
   `spec.deploymentMode: OMENative` (see the top of this page — set it once and it
   propagates to every component; the per-component `ome.io/deploymentMode`
   annotation is an optional override for mixed-mode).
   Check: `kubectl get isvc <name> -o jsonpath='{.spec.deploymentMode}'`.
4. **Surging slower than your per-component `maxSurge` allows?** In a coordination
   group the effective cap is `min(group pacing.maxSurge, per-component maxSurge)`,
   and a peer rolling at the same time can be the binding constraint. Set surge at
   the group level for coordinated components.
5. Phase `Failed`? A new-revision pod didn't reach Ready inside the timeout. Fix
   the pod (image/flags/probe) — the old revision is still serving. The lifecycle
   instance-ready timeout, `lifecycle.instanceReadyTimeout`, defaults to 30m;
   raise it if your model is just slow to load.

## Validation: what the webhook rejects

Caught at `kubectl apply` (run `--dry-run=server` first):

- **Sequential** needs `order` covering every member, and ≥2 components — missing
  `order`, an incomplete/duplicate order, or a single-component Sequential group
  is rejected.
- **Sequential + `RatioBalanced` pacing** is rejected — only one component rolls
  at a time, so there's no ratio to balance.
- **RollingUpdate** needs ≥2 components; on **update**, all members of a
  RollingUpdate group must bump their image together (lockstep).
- **Zero-budget deadlock** — an explicit `maxSurge: 0` AND `maxUnavailable: 0`
  on the same component (or on a coordination group's pacing) is rejected as
  unstartable. Leave one knob unset to defer to the default.
- **Non-OMENative member** — a coordination group referencing a component that
  doesn't resolve to OMENative is rejected.
- **Duplicate membership** — a component in two groups is rejected.
- **`ratioTolerancePercent` > 50** is a non-blocking **warning** (it effectively
  disables ratio enforcement), not a rejection.

## Canary (gradual traffic %) — reserved, not available

Operator-authored step canary (shift 10% → 50% → 100% of *traffic* with pauses)
is reserved at `spec.rollout.canary`. The shape is fixed in the schema (a `steps`
list with per-step `newCapacity` / `oldCapacity` / `trafficWeight` / `pause`, a
`promotionPolicy` of `Manual`/`Auto`, and `scaleDownDelaySeconds`) so enabling it
later isn't an API migration — but there is **no step executor yet**.

**Setting it is rejected at admission today**, not ignored: your create/update
fails outright with

```
spec.rollout.canary is not yet implemented (RolloutCanaryReserved)
```

Don't reach for it. For safe rollouts today, use the coordination policies above:

- **BlueGreen** — atomic cutover (no partial-traffic phase).
- **RollingUpdate** / **RatioBalanced** — paced pod replacement; traffic follows
  pod readiness automatically (no operator-controlled traffic split).

## Next

- Prefill-decode or multi-node topologies → [Multi-node & PD](howto-multi-node-and-pd.md)
- The architecture behind all this (state machine, drain, `readyReplicas` vs
  `servingReplicas`) → [OMENative guide](guide-omenative.md)