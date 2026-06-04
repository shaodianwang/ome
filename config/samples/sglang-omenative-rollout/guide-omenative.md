# OMENative

*OME user guide — page 2 of 4. Prev: [To OME or not](guide-when-to-use-ome.md)
· Next: [Runtimes](guide-runtimes.md) ·
[Models & InferenceService](guide-models-and-inferenceservice.md)*

OMENative is a deployment mode. You turn it on with one field —
`spec.deploymentMode: OMENative` — set once, and it propagates to every
component (engine/decoder/router). From then on OME manages the serving pods
itself, directly, instead of handing them to a built-in Kubernetes controller.
Everything interesting about rollouts in this guide only happens in this mode.

On this page:
- The deployment modes, and who actually runs the pods
- Why it's called "native"
- The object model: Component, Instance, Runner, Pod
- Pod identity: how OMENative keeps track
- How the controller works: two loops, status as truth
- The five operations
- Lifecycle: how one component rolls
- Rollout coordination: how components roll together
- Two rules that are easy to miss
- Watching a rollout
- Further reading
- FAQ

## The deployment modes, and who actually runs the pods

An InferenceService runs in one of a few modes. The difference is simple: who
is responsible for the pods.

| Mode | Pods are run by |
|------|-----------------|
| `RawDeployment` | a standard Kubernetes Deployment + Service |
| `MultiNode` | a LeaderWorkerSet (multi-pod gangs) |
| `PDDisaggregated` | the older prefill/decode path (separate Deployments) |
| **`OMENative`** | **OME itself, pod by pod** |

## Why it's called "native"

In the first three modes, OME delegates. It writes a Deployment (or a
LeaderWorkerSet) and lets that controller create and replace pods. That
controller is general-purpose; it knows "keep N copies of one template" and
little else.

In OMENative, OME owns the pods *natively* — it creates, watches, updates, and
replaces them through its own controller. That's the whole reason the mode
exists: a Deployment can't express the things model serving actually needs, and
once OME is driving the pods directly it can.

Concretely, owning the pods lets OME do four things a Deployment cannot:

- **In-place updates** — change a pod's env or args without rescheduling it.
- **Surge-then-drain with a real serving gate** — bring a new pod up, wait
  until it's actually taking traffic, *then* drain the old one. No gap.
- **Gang restart** — when one pod of a multi-pod instance dies, restart the
  whole instance, not just that pod.
- **Coordinated rollouts** — roll prefill and decode together, in proportion.

If you don't need any of those, don't use OMENative; a Deployment is simpler.

## The object model: Component, Instance, Runner, Pod

OMENative manages four nested things. The first three are concepts inside the
controller; only the last is a real Kubernetes object.

- **Component** — a role in the service: `router`, `engine`, or `decoder`.
- **Instance** — one replica of a Component, and the *atomic unit*. Restart,
  migration, gang scheduling, and readiness all happen per Instance. A Component
  with `minReplicas: 3` has three Instances (0, 1, 2).
- **Runner** — a kind of pod inside an Instance: `leader`, `worker`, or
  `default` (single-pod). A Runner has a size and a pod template.
- **Pod** — a normal Kubernetes pod.

So a single-pod engine replica is one Instance with one `default` pod. A
multi-node engine replica is one Instance with a `leader` pod plus N `worker`
pods that come up as a gang and die as a gang. That is the key difference from a
Deployment, which only counts pods and has no idea that several of them are one
thing.

Pods get stable, predictable names:

```
<isvc>-<component>-<instance>-<runner>-<ordinal>
e.g.  llama-engine-0-leader-0,  llama-engine-0-worker-0, ...
```

The name is stable across recreation — an Instance keeps its identity even when
its pods are replaced. (OME sets `hostname`/`subdomain` on each pod directly, so
headless-Service DNS works without a StatefulSet.)

## Pod identity: how OMENative keeps track

Managing pods directly means OMENative does bookkeeping a Deployment never has
to. A Deployment treats its pods as interchangeable — any pod is as good as any
other. OMENative can't: it has to know that *this exact pod* is Instance 0's
leader, and tell an old version of that pod from a new one even though they
share a name. Four mechanisms do that.

**Labels carry the coordinates.** Every managed pod is tagged with where it sits
in the model (labels are just key/value metadata Kubernetes lets you select on):

```
ome.io/inferenceservice     = <isvc>
ome.io/component            = router | engine | decoder
ome.io/instance-index       = <idx>
ome.io/runner               = leader | worker | default
ome.io/managed-by           = OMENative
ome.io/instance-incarnation = <n>
```

The Service routes by the `component` label, so traffic keeps reaching the right
pods no matter which Instances exist at the moment.

**The index is a name, not a slot.** Instance indices don't have to be dense.
After a migration, `{0, 1}` can become `{1, 2}`, and nothing breaks — the
controller counts *live Instances*, it never assumes index `N` is "the N-th
array element." (k8s readers: this is the StatefulSet-ordinal assumption,
deliberately not made.)

**Incarnation tells old from new.** Here's the subtle part. Because the pod name
is stable, the controller can be looking at a pod called
`llama-engine-0-leader-0` and not know whether it's the one it just created or a
leftover from a previous life. So each Instance carries a monotonically
increasing **incarnation** number, stamped on the pod as the
`ome.io/instance-incarnation` label. It bumps when the Instance is fully
recreated, or restarted after a pod loss; it does **not** bump on an in-place
update (same pod, new image). Incarnation answers "is this the pod I think it
is?" — a different question from "what version is it running?", which is tracked
separately by a saved spec snapshot (a Kubernetes `ControllerRevision`).

**Name reuse is handled deliberately.** Stable names create a real hazard: the
controller could act on a stale pod that happens to carry the name it wants to
use next. So before it creates a pod, it looks for **orphans** — a live pod with
the right name but the wrong revision, the wrong incarnation, or no place in the
current plan — and removes them first. It also keeps an in-memory set of
**expectations** (the creates and deletes it has issued but not yet observed) so
it doesn't act twice on the same thing. Expectations are only a speed-up: after
a controller restart they're gone, and the controller rebuilds the truth by
listing the live pods. (k8s readers will recognize the expectations pattern from
the built-in controllers; the orphan check is the price of stable names.)

> Naming aside, for the curious: with `SurgeThenDrain` on a single-pod Instance,
> the trailing `<ordinal>` in the pod name flips between `0` and `1` across
> upgrades. The new pod comes up in the free slot and starts taking traffic
> before the old slot is drained, so for a few seconds the two coexist under
> different names. `RecreatePod` and in-place updates keep the same name.

## How the controller works: two loops, status as truth

OMENative is **not a separate controller**. It's a backend inside the existing
InferenceService controller — one more `deploymentMode`. And it emits **only
standard Kubernetes objects**: Pods, Services, a PodDisruptionBudget (a cap on
how many pods may be down at once), ControllerRevisions (saved spec snapshots,
for revision history), and an optional PodGroup (for gang scheduling —
all-or-nothing placement). There are **no new CRDs** to learn or debug — you see your
InferenceService and ordinary pods. (This is a deliberate choice over adopting
LeaderWorkerSet or RoleBasedGroup, which would put extra CRDs between your
InferenceService and a pod.)

It runs two loops:

- **The reconcile loop** is the only thing that changes anything. Each pass it
  (1) builds the desired Component → Instance → Runner → Pod plan from the
  InferenceService + runtime + model, (2) reads the live pods, (3) picks *at
  most one* action per Component, (4) records that action in status *before*
  doing it, (5) executes, and (6) requeues until it converges. One action at a
  time per Component keeps the state machine simple and stops, say, an update
  and a migration from fighting over the same Instance.
- **The watch loop** only *wakes up* the reconcile loop when a pod or
  EndpointSlice (the list Kubernetes routes a Service's traffic through)
  changes. It has no authority of its own — if it misses an event,
  the reconcile loop still recovers by comparing the plan against a live read.
  Watches are an accelerator, not the source of truth.

The source of truth is the InferenceService **status** plus the live Kubernetes
objects — never an in-memory plan (that's recomputed every pass), never a hidden
CRD. If the controller restarts, it rebuilds everything from what's actually in
the cluster.

## The five operations

Every change an Instance can undergo is one of five operations. The controller
records which one is in flight on the Instance's status, so it can resume after
a restart:

1. **Create / scale up** — bring a new Instance (gang) to ready.
2. **Update** — move an Instance to a new revision, *in place* when the change
   allows it (e.g. an image or env change that just restarts the container and
   keeps the model weights on disk), otherwise by recreating the pod.
3. **Restart on pod loss** — if a pod in a gang dies, restart the whole Instance
   (a half-gang can't serve).
4. **Delete / scale down** — drain and remove an Instance.
5. **Migrate** — move an Instance off a node by surging a replacement, waiting
   for it to be ready, draining the old one, then deleting it — so capacity
   never dips. (External tools like the GPU defragmenter request this with an
   annotation.)

"Drain" here is precise: OMENative flips a controller-owned readiness gate to
take the pod out of the Service's EndpointSlice, waits a grace period for
in-flight requests to finish, *then* deletes. That gate is what makes
`SurgeThenDrain` gap-free.

## Lifecycle: how one component rolls

`spec.<engine|decoder|router>.lifecycle` controls how *that one component's*
Instances move to a new version. The main knob is the update strategy:

| Strategy | What it does | Cost |
|----------|--------------|------|
| `SurgeThenDrain` (default) | start a new pod, wait until it serves, drain the old one | one extra pod, no traffic gap |
| `RecreatePod` | delete the old pod, then create the new one | a brief capacity dip |
| `InPlaceIfPossible` | change the pod in place; recreate only if the change can't be done in place | none |
| `InPlaceOnly` | change in place, or reject the change | none |

The budget is `rollingUpdate.maxSurge` and `rollingUpdate.maxUnavailable`
(a plain integer, or a percentage). `maxSurge` is how many extra pods you'll
allow; `maxUnavailable` is how many you'll let drop below ready at once.

Rule of thumb: *use `SurgeThenDrain` when you have a spare accelerator slot and
want zero gap; use `RecreatePod` when you're capacity-constrained and a short
dip is acceptable; use `InPlace*` for changes that don't need a reschedule
(an env var, a flag).*

## Rollout coordination: how components roll together

A prefill/decode model is two pools that talk to each other. Rolling them
independently can leave all-new prefill talking to all-old decode — a version
skew across the pair that must interoperate. `spec.rollout.coordination` exists
to prevent that.

Architecturally, coordination is a **thin layer on top** of the per-Component
reconcile loop above. It owns no pods. It only decides *when* each Component may
surge, wait, shift traffic, and scale down — by running a small state machine
per group:

```
Idle → Surging → Waiting → Shifting → Draining → ScalingDown → (loop, or back to Idle)
```

(plus `Failed`, `RollingBack`, and `Paused` for the unhappy paths). To shift
traffic it creates a Service per revision and writes the desired weights to
`status.components.<c>.traffic[]`; the routing layer turns those into weighted
routes. The policy you pick just changes how that machine is driven across the
Components.

You put the components in a group and pick a policy:

| Policy | What happens | Use it when |
|--------|--------------|-------------|
| `RollingUpdate` | components roll in parallel, in small paced steps; mixed revisions are tolerated mid-roll | steady upgrades where capacity is tight |
| `Sequential` | one component fully rolls before the next starts; optional `soak` to validate in between | you want to check one before risking the next |
| `BlueGreen` | bring up a full new copy of everything, then flip traffic at once | atomic cutover and instant rollback — needs about double the hardware |
| `Independent` | each component rolls on its own | the components don't need to move together |

The group also has a `pacing` block:

- `type: PerComponent` — no cross-component ratio coupling; each component rolls
  under the `min(group, per-component)` surge/unavailable caps.
- `type: RatioBalanced` — surge the components *in lockstep*, keeping their
  ratio (e.g. prefill:decode = 1:1) within `ratioTolerance` percent of where it
  started. This is the one to use for PD: the two pools come up in proportion,
  so you never roll into a lopsided skew.

One rule worth remembering: the group's `pacing.maxSurge` and a component's own
`lifecycle…maxSurge` are **independent caps** — the effective surge is
`min(group, per-component)`, and an unset per-component value defers to the group
(same for `maxUnavailable`).

The example in this folder uses `RollingUpdate` + `RatioBalanced`, which is the
recommended shape for prefill/decode.

## Two rules that are easy to miss

These are not opinions; the controller behaves this way, and missing either one
fails quietly.

1. **Set `spec.deploymentMode: OMENative` once** — it propagates to every
   component (engine/decoder/router) and the ingress. The per-component
   `ome.io/deploymentMode` annotation is an optional override for mixed-mode
   (e.g. one component on RawDeployment); a component annotation wins over the
   field for that component. Miss the field entirely and components run as plain
   Deployments — you won't get an error, just the wrong behavior.

2. **Name the engine/decoder container `ome-container`** (and the router
   container `router`). OME finds the main container by that exact name for
   status and for injecting sidecars. Rename it and those break.

## Watching a rollout

- `status.rolloutCoordination.groups[].phase` walks through
  `Idle → Surging → Waiting → Shifting → Draining → ScalingDown → Idle`
  (or `Failed` / `RollingBack` / `Paused` if something goes wrong).
- `status.rolloutCoordination.groups[].observedRatio` shows the live
  prefill:decode counts when you're using `RatioBalanced`.
- `status.components.<c>.lifecycle` shows `replicas`, `readyReplicas`,
  `servingReplicas`, and `updatedReadyReplicas` — the last is how far the roll
  has progressed.

## Further reading

The full designs — with the rationale, the corner cases, and the reasons for
each choice — are the OME enhancement proposals:

- **OEP-0007, OMENative Workload Strategy**
  (`oeps/0007-omenative-workload-strategy/`) — the object model, the two-loop
  controller, the five operations, drain semantics, and why OMENative replaces
  LeaderWorkerSet. Its `controller-mechanics.md` has the loop-level detail.
- **OEP-0007.1, OMENative Coordinated Rollout**
  (`oeps/0007.1-omenative-coordinated-rollout/`) — the coordination layer, the
  per-group state machine, per-revision Services, and each policy's behavior.

## FAQ

**Do I have to use coordination and lifecycle?**
No. Both are optional. With neither, each component rolls on its own with safe
defaults (`SurgeThenDrain`). Add them when you want more control.

**What's the difference between `readyReplicas` and `servingReplicas`?**
`ready` means the pod's containers pass their probes. `serving` means the pod is
*also* in the load balancer's rotation. They differ during an in-place update or
a drain, when OME holds a pod out of traffic while it's technically still ready.
`servingReplicas` is the number that reflects real traffic capacity.

**Can a component be OMENative while another is a plain Deployment?**
Yes — mode is per component. That's exactly why rule #1 above matters.

**Is `PDDisaggregated` the same thing?**
No. `PDDisaggregated` is the older path that wires up separate Deployments for
prefill/decode and has no lifecycle or coordination. OMENative is the newer
path that does PD *and* gives you the rollout controls on this page.
