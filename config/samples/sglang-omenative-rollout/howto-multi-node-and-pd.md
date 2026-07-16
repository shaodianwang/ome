# How-to: multi-node & prefill-decode (PD)

*Task recipe — page 4 of 4. Prev: [Update & roll](howto-update-and-roll.md) ·
[Launch a model](howto-launch-a-model.md) ·
[Write a runtime](howto-write-a-runtime.md)*

Two different "more than one pod" topologies. They're independent — you can use
either, or both together:

- **Prefill-decode (PD)** — split the model into a **prefill** pool (`engine`) and
  a **decode** pool (`decoder`) that talk to each other. Each is its own set of
  pods, scaled and rolled separately.
- **Multi-node** — one model *instance* is too big for one node, so it spans a
  **leader** pod + N **worker** pods that are scheduled and restarted *as a gang*.

## Prefill-decode (PD)

You express PD by filling in **both** `engine` (prefill) and `decoder` (decode),
plus a `router` as the single entrypoint. The runtime must define
`engineConfig` (prefill flags) and `decoderConfig` (decode flags).

### Simplest PD (no managed rollouts)

If you just want PD serving and don't need coordinated upgrades, this is enough —
the three components are all you set (this mirrors `avo-tier5-pd/`):

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: my-pd-model
  namespace: <ns>
spec:
  runtime:
    name: <pd-capable-runtime>     # engineConfig + decoderConfig + routerConfig; model via cluster cache
  engine:   { minReplicas: 1, maxReplicas: 1 }
  decoder:  { minReplicas: 1, maxReplicas: 1 }
  router:   { minReplicas: 1, maxReplicas: 1 }
```

### PD with safe, coordinated rollouts (recommended for prod)

To upgrade prefill and decode safely you want OMENative + `spec.rollout.coordination`.
This is the canonical shape (the full version is `inferenceservice.yaml` in this
folder):

```yaml
spec:
  deploymentMode: OMENative   # set once; propagates to engine / decoder / router
  engine:
    minReplicas: 2
    maxReplicas: 2
    lifecycle:
      updateStrategy:
        type: SurgeThenDrain
        rollingUpdate: { maxSurge: 1, maxUnavailable: 0 }
  decoder:
    minReplicas: 2
    maxReplicas: 2
    lifecycle:
      updateStrategy:
        type: SurgeThenDrain
        rollingUpdate: { maxSurge: 1, maxUnavailable: 0 }
  router:
    minReplicas: 1
    maxReplicas: 1
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

**Why `RatioBalanced`:** prefill and decode serve one model at a fixed ratio and
talk to each other. RatioBalanced rolls them in proportion so a new-revision
prefill never ends up talking to an old-revision decode. (`Sequential` would
maximize that skew window; `BlueGreen` would need ~2× the GPUs.) See the README
in this folder for the full rationale.

**Keep prefill:decode in proportion as they scale** (not just during rollout)
with a `scalingPolicy`:

```yaml
spec:
  scalingPolicy:
    mode: Proportional
    proportional:
      anchor: engine
      ratios:
        decoder: "1"      # 1 decoder per engine (dimensionless ratio)
```

## Multi-node (leader + worker gang)

When one instance spans several nodes, set `leader` and `worker` on the
component. `worker.size` is the number of worker pods; one *instance* =
1 leader + `size` workers, gang-scheduled.

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: my-big-model
  namespace: <ns>
spec:
  deploymentMode: OMENative   # set once; propagates to all components + ingress
  runtime:
    name: <multinode-capable-runtime>   # model via cluster cache
  engine:
    minReplicas: 1            # 1 instance = 1 leader + worker.size workers
    maxReplicas: 1
    leader:
      runner:
        name: ome-container   # leader-specific image/command/resources go here
    worker:
      size: 3                 # 3 worker pods in the gang
      runner:
        name: ome-container
```

Pods come up as `<isvc>-engine-0-leader-0` and `<isvc>-engine-0-worker-0..2`.

> **This combination has no copy-paste sample in-tree yet.** The shape above is
> built from the API types; validate it against your runtime with
> `kubectl apply --dry-run=server -f` before relying on it.

### PD + multi-node together

Set `leader`/`worker` under **both** `engine` and `decoder` — each component
becomes its own multi-node gang. (Verify with `--dry-run=server`.)

## Footguns specific to these topologies

1. **`spec.deploymentMode: OMENative` set once**, same as everywhere else — it
   propagates to all components (the per-component `ome.io/deploymentMode`
   annotation is an optional override for mixed-mode). Without it the
   gang/coordination machinery is inert. (Bare `engine + decoder` with *no*
   deployment mode still works for *simple* PD via auto-defaulting, but you get
   none of the lifecycle/rollout features.)
2. **`worker.size` must be ≥ 1.** There's no schema minimum on it today, so a
   `0` or negative slips past the apiserver and fails later — set it explicitly.
3. **Gang scheduling needs the scheduler-plugins `PodGroup` CRD and a gang-aware
   scheduler selected with `schedulerName`.** The CRD alone does not change the
   default scheduler. Without both, the workload degrades to ordinary
   scheduling, so a partially scheduled gang can sit half-up. Confirm both in
   clusters where you rely on all-or-nothing placement.
4. **The router becomes the entrypoint.** Once you add a `router`, external
   traffic goes to it, not the engine. That's what you want for PD; just know
   your URL now points at the router.

## Next

- Roll a new image across a PD/multi-node service safely →
  [Update & roll](howto-update-and-roll.md)
- The architecture behind all this (Instances, Runners, gang lifecycle) →
  `guide-omenative.md`
