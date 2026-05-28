# PodGroup Operator Design

## Goal

`podgroup-operator` implements a `PodGroupDeployment` workload API for grouped
GPU workflows. A group is treated as the operational unit: it schedules
together, rolls out together, restarts together, and exposes one target Pod
through a stable group Service.

The implementation intentionally relies on the Kubernetes scheduler path. The
operator does not implement a custom scheduler and does not set `spec.nodeName`.
It creates upstream scheduler-plugins `PodGroup` objects and lets the
`default-scheduler` profile, with Coscheduling enabled, make scheduling
decisions.

The operator directly manages Pods. It does not create StatefulSets,
Deployments, or ReplicaSets for workload Pods.

OME `InferenceService` or GitOps is expected to sit above this operator. That
layer owns app-level composition: router Deployments, model PVCs, runtime
selection, and user-facing service status. `podgroup-operator` owns only the
prefill/decode or unified PodGroup lifecycle.

## Current API

The operator-owned CRD is:

```yaml
apiVersion: podgroup.meta.com/v1alpha1
kind: PodGroupDeployment
metadata:
  name: inference
spec:
  groups: 3
  replicasPerGroup: 4
  topologyKey: kubernetes.io/hostname
  serviceTargetOrdinal: 0
  scheduleTimeoutSeconds: 60
  schedulerName: default-scheduler
  updateStrategy:
    rollingUpdate:
      maxSurge: 0
      maxUnavailablePercent: 20
  template:
    metadata:
      labels:
        app: inference
    spec:
      containers:
      - name: worker
        image: example/inference:latest
        ports:
        - name: http
          containerPort: 8080
        resources:
          requests:
            nvidia.com/gpu: "1"
            cpu: "500m"
            memory: "1Gi"
          limits:
            nvidia.com/gpu: "1"
```

Supported fields:

- `groups`: number of logical groups.
- `replicasPerGroup`: number of Pods in each group.
- `updateStrategy.type`: rollout mechanism. Supported values are
  `InPlaceIfPossible`, `InPlaceOnly`, and `RecreatePod`.
- `topologyKey`: topology domain used for required group Pod affinity.
- `serviceTargetOrdinal`: ordinal selected by group Services and the top-level
  Service.
- `scheduleTimeoutSeconds`: copied to the scheduler-plugins `PodGroup`.
- `schedulerName`: scheduler used by managed Pods.
- `updateStrategy.rollingUpdate.maxSurge`: extra replacement groups allowed
  during rollout.
- `updateStrategy.rollingUpdate.maxUnavailable`: old groups allowed to be taken
  down during rollout before replacements are Ready. This integer field is
  kept for compatibility.
- `updateStrategy.rollingUpdate.maxUnavailablePercent`: percentage of groups
  allowed to be unavailable during terminate-first rollout.
- `updateStrategy.rollingUpdate.progressDeadlineSeconds`: time an unready
  replacement Pod may remain unready before the controller deletes it so the
  same group can be recreated and scheduled again. Defaults to `3600`.
- `multiNode.enabled`: enables multi-node argument injection for
  groups whose tensor parallel size is greater than 4.
- `multiNode.tensorParallelSize`: optional TP size override. When unset,
  PGD reads `--tp-size` from the selected container command or args.
- `multiNode.containerName`: optional target container. When unset, PGD
  uses the container with `--tp-size`, or the only container.
- `multiNode.distInitPort`: port used in `--dist-init-addr`. Defaults to
  `30000`.
- `template`: Pod template used for every managed Pod.

Defaults applied by the controller:

- `topologyKey`: `kubernetes.io/hostname`
- `serviceTargetOrdinal`: `0`
- `schedulerName`: `default-scheduler`
- `updateStrategy.type`: `InPlaceIfPossible`
- `template.spec.restartPolicy`: `Never`
- `template.spec.terminationGracePeriodSeconds`: `1`
- `maxUnavailablePercent`: `20` when rollout limits are unset

## Why A Separate API

The upstream `scheduling.x-k8s.io/v1alpha1 PodGroup` is a scheduler
coordination object. It is not a workload controller API and does not describe
Pods, Services, revisions, rollouts, or group-level restart behavior.

`PodGroupDeployment` is the workload-level API. It owns the Pod template,
desired group count, group size, topology policy, service behavior, rollout
policy, and status. The operator translates that into scheduler `PodGroup`
objects plus directly managed Pods and Services.

## Object Model

For:

```yaml
metadata:
  name: gpu-one
spec:
  groups: 3
  replicasPerGroup: 4
```

The operator creates one active group instance per logical group:

```text
scheduling.x-k8s.io/PodGroup gpu-one-0
scheduling.x-k8s.io/PodGroup gpu-one-1
scheduling.x-k8s.io/PodGroup gpu-one-2

Pod gpu-one-0-0
Pod gpu-one-0-1
Pod gpu-one-0-2
Pod gpu-one-0-3

Pod gpu-one-1-0 ... gpu-one-1-3
Pod gpu-one-2-0 ... gpu-one-2-3

Service gpu-one
Service gpu-one-0
Service gpu-one-1
Service gpu-one-2
```

During a rollout, replacement group instance names include the template revision
hash:

```text
PodGroup gpu-one-8a80854d-0
Pod gpu-one-8a80854d-0-0
Pod gpu-one-8a80854d-0-1
Pod gpu-one-8a80854d-0-2
Pod gpu-one-8a80854d-0-3
```

The stable group Service name remains `gpu-one-0`; the Service selector moves
from the old group instance to the new group instance only after the replacement
group is Ready.

The top-level Service does not forward to the per-group Services. Kubernetes
Services select Pods directly, so `gpu-one` selects service-target Pods across
all active groups and `gpu-one-0` selects the service-target Pod for group 0.

For multi-node groups, the existing per-group Service is also the rank-0
distributed init address. PGD injects:

```text
--nnodes <replicasPerGroup>
--node-rank <ordinal>
--dist-init-addr <pgd-name>-<group>.<namespace>.svc.cluster.local:<distInitPort>
```

The per-group Service continues to select `serviceTargetOrdinal`, normally
ordinal `0`. PGD adds the `dist-init` port when the port is not already exposed and enables
`publishNotReadyAddresses` on that per-group Service so non-zero ranks can
reach rank 0 during startup. The top-level Service keeps normal readiness
filtering for serving traffic.

## Dependency Flow

The dependency graph is intentionally narrow:

```text
OME InferenceService / GitOps
  -> PodGroupDeploymentRollout, for PD or coordinated unified lifecycle
    -> generated PodGroupDeployment children
      -> scheduler-plugins PodGroup objects
      -> directly managed Pods
      -> per-group and top-level Services

Router Deployment / Service
  -> discovers PGD service-target Pods or PGD Services by label
```

PGDR does not create Pods or Services directly. PGD does not manage routers.
The scheduler-plugins Coscheduling plugin is the scheduling dependency that
enforces gang semantics for each PGD group.

## Labels

Managed objects use stable labels:

```yaml
app.kubernetes.io/managed-by: podgroup-operator
app.kubernetes.io/name: pod-group-deployment
app.kubernetes.io/instance: gpu-one
podgroup.meta.com/group-index: "0"
podgroup.meta.com/group-instance: gpu-one-0
podgroup.meta.com/revision: 8a80854d
```

Pods also get:

```yaml
podgroup.meta.com/ordinal: "0"
podgroup.meta.com/service-target: "true"
scheduling.x-k8s.io/pod-group: gpu-one-0
```

Only the Pod whose ordinal equals `serviceTargetOrdinal` has
`podgroup.meta.com/service-target: "true"`.

## Scheduling Model

Scheduling is split between Coscheduling and normal Kubernetes constraints:

- Coscheduling provides all-or-nothing quorum behavior.
- Resource fit handles CPU, memory, `nvidia.com/gpu`, and other resources.
- Node selectors, taints, tolerations, and template affinity still apply.
- Operator-injected required Pod affinity keeps a group in one topology domain.

Each scheduler `PodGroup` gets:

```yaml
spec:
  minMember: <spec.replicasPerGroup>
  scheduleTimeoutSeconds: <spec.scheduleTimeoutSeconds, when non-zero>
```

Each managed Pod is labeled with:

```yaml
scheduling.x-k8s.io/pod-group: <group-instance-name>
```

Each managed Pod also receives required Pod affinity:

```yaml
affinity:
  podAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
    - labelSelector:
        matchLabels:
          app.kubernetes.io/instance: gpu-one
          podgroup.meta.com/group-instance: gpu-one-0
      topologyKey: <spec.topologyKey>
```

Kubernetes self-affinity permits the first Pod in the group to schedule, then
the remaining Pods must schedule into the same topology domain.

## Resource Patterns

Single-node TP1-style group:

- Worker node advertises `nvidia.com/gpu=4`.
- `replicasPerGroup: 4`.
- Each Pod requests `nvidia.com/gpu: "1"`.
- `topologyKey: kubernetes.io/hostname`.
- Result: the group consumes one 4-GPU node.

Single-node 2-GPU-per-Pod group:

- Worker node advertises `nvidia.com/gpu=4`.
- `replicasPerGroup: 2`.
- Each Pod requests `nvidia.com/gpu: "2"`.
- `topologyKey: kubernetes.io/hostname`.
- Result: the group consumes one 4-GPU node.

Multi-node TP8/NVLink-domain group:

- Two worker nodes each advertise `nvidia.com/gpu=4`.
- Both nodes share a label such as `ds.coreweave.com/nvlink.domain=1`.
- `replicasPerGroup: 2`.
- Each Pod requests `nvidia.com/gpu: "4"`.
- `topologyKey: ds.coreweave.com/nvlink.domain`.
- Result: the group consumes two 4-GPU nodes in the same NVLink domain.

The operator does not derive `replicasPerGroup` from resource requests. The user
sets the group size explicitly.

## Services

The operator creates one stable Service for each logical group and one top-level
Service for the whole `PodGroupDeployment`.

For `gpu-one`:

```text
gpu-one     -> all active service-target Pods across groups
gpu-one-0   -> active service-target Pod for group 0
gpu-one-1   -> active service-target Pod for group 1
gpu-one-2   -> active service-target Pod for group 2
```

Group Service selector:

```yaml
selector:
  app.kubernetes.io/instance: gpu-one
  podgroup.meta.com/group-index: "0"
  podgroup.meta.com/group-instance: <active-group-instance>
  podgroup.meta.com/service-target: "true"
```

Top-level Service selector:

```yaml
selector:
  app.kubernetes.io/instance: gpu-one
  podgroup.meta.com/service-target: "true"
```

For multi-node groups, `serviceTargetOrdinal` is the serving rank. For example,
a TP8-style group on 4-GPU nodes can use `replicasPerGroup: 2`, each Pod
requesting `nvidia.com/gpu: "4"`, and `serviceTargetOrdinal: 0`. The group
Service then points only at rank 0 for that group, while the top-level Service
points at rank 0 for every active group.

During a `maxSurge: 0` rollout, a group may have no active Pods between
delete-old and create-new. The affected per-group Service can temporarily have
zero endpoints, and the top-level Service has fewer endpoints until the
replacement group is Ready. Existing top-level endpoints still point at Ready
service-target Pods.

Routers should either target the top-level PGD Service or discover
service-target Pods by label. For PGDR-managed disaggregated deployments, the
rank-0 Pod selectors look like:

```text
prefill: app.kubernetes.io/component=prefill,app.kubernetes.io/instance=<name>-prefill,podgroup.meta.com/service-target=true
decode:  app.kubernetes.io/component=decode,app.kubernetes.io/instance=<name>-decode,podgroup.meta.com/service-target=true
```

Service ports are generated from unique container ports in the Pod template. If
the template has no container ports, a default `http` port on `80` is used.

## Rollout Semantics

The controller computes a revision hash from `spec.template`. Template changes
roll by group, not by Pod.

With `maxSurge: 1` and `maxUnavailable: 0`:

1. Create one replacement scheduler `PodGroup`.
2. Create all Pods for that replacement group.
3. Wait until every Pod in the replacement group is Ready.
4. Update the stable group Service selector to point at the replacement group.
5. Delete the old group instance.
6. Continue with the next group.

With `maxSurge: 0` and `maxUnavailable: N`, the controller may delete up to `N`
old groups first, then recreate those groups at the new revision.

With `maxSurge: 0` and `maxUnavailablePercent: P`, the controller computes the
unavailable window as `max(1, floor(groups * P / 100))`. `maxUnavailable`
takes precedence when both fields are set. All limits are group-based and are
not counted per Pod.

### In-Place Updates

The in-place path follows the RBG/InstanceSet pattern: classify the template
diff first, then update an entire group in place only when the diff is safe.
The controller stores a `podgroup.meta.com/recreate-revision`
annotation on each Pod. This hash ignores template metadata and container
images, but includes fields that require Pod recreation, such as env, args,
resources, volumes, affinity, tolerations, ports, init containers, and
scheduler-related fields.

When the full template revision changes but the recreate revision stays the
same, PGD patches the existing group Pods instead of creating a replacement
group instance:

1. Patch managed Pod labels and annotations, including the new full revision.
2. Patch `spec.containers[*].image` on each Pod in the group.
3. Keep the same scheduler `PodGroup`, Pod names, group instance, and Service
   selectors.
4. Let kubelet restart containers in place and let normal Pod readiness decide
   when endpoints are ready.

`updateStrategy.type` controls fallback behavior:

- `InPlaceIfPossible`: use in-place for eligible changes and fall back to
  recreate for non-eligible changes.
- `InPlaceOnly`: use in-place for eligible changes and reject non-eligible
  changes.
- `RecreatePod`: always use the replacement group path.

For PGDR-managed disaggregated rollout, in-place updates use a different phase
order from recreate. Decode groups update first, then prefill groups update.
This preserves the important dependency direction for image-only updates:
new prefill should not run against old decode. Recreate rollouts keep the
existing terminate-prefill, terminate-decode, create-decode, create-prefill
ordering.

## Coordinated Rollouts

`PodGroupDeploymentRollout` is the top-level API for the prefill/decode
lifecycle of one disaggregated inference engine. It owns generated
`PodGroupDeployment` children for each role and coordinates their rollout waves
as one operation. PGD remains the workload primitive: it owns Pods, Services,
scheduler `PodGroup` objects, topology placement, group-level restart, and
group-level replacement. PGDR coordinates when member PGDs are allowed to
advance to the next rollout wave.

The coordinator CRD is:

```yaml
apiVersion: podgroup.meta.com/v1alpha1
kind: PodGroupDeploymentRollout
metadata:
  name: qwen-engine
spec:
  mode: disagg
  strategy:
    maxUnavailablePercent: 20
    maxSkew: 0
  base:
    template:
      spec:
        containers:
        - name: engine
          image: example/engine:v1
  prefill:
    groups: 40
    replicasPerGroup: 1
    template:
      spec:
        containers:
        - name: engine
          args: ["--role", "prefill"]
  decode:
    groups: 20
    replicasPerGroup: 1
    template:
      spec:
        containers:
        - name: engine
          args: ["--role", "decode"]
```

Supported modes:

- `disagg`: owns two generated member PGDs named `<rollout-name>-prefill`
  and `<rollout-name>-decode`.
- `unified`: owns one generated member PGD named `<rollout-name>-unified`.

`base` contains shared PGD and pod template fields. `groups` and
`replicasPerGroup` are role-specific and must be set under `prefill`, `decode`,
or `unified`; the controller ignores those two fields in `base`.

There is intentionally no free-form `members` list in the first version. The
mode declares the engine shape and the member names are derived by convention.
Direct edits to PGDR-managed child PGDs are overwritten by the PGDR spec. PGDs
also remain independently useful when no PGDR owns them.

PGDR is implemented in the same `podgroup-operator` binary because the APIs
evolve together and PGDR needs PGD rollout status. It is still reconciled
separately so PGDs remain independently useful when no PGDR exists.

### PGDR Wave Planning

PGDR keeps the prefill/decode ratio aligned by computing a shared cohort wave
count and deriving each member's window from that count. For example, with 9
prefill groups, 3 decode groups, and `maxUnavailablePercent: 20`, the rollout
has three waves:

```text
wave 0: prefill groups 0-2, decode group 0
wave 1: prefill groups 3-5, decode group 1
wave 2: prefill groups 6-8, decode group 2
```

This avoids rolling one prefill group per one decode group when the deployment
ratio is 3:1.

PGDR uses the same wave planning for initial bringup and for template rollout.
For a cluster that cannot fit the full requested shape, this prevents first
create from submitting every group at once and letting the scheduler admit a
skewed subset such as mostly prefill. Instead, PGDR creates prefill and decode
groups in the configured ratio and stops naturally at the first wave that is
not Ready.

Within each disaggregated wave, rollout is ordered:

```text
TerminatePrefill -> TerminateDecode -> CreateDecode -> CreatePrefill
```

This preserves the runtime dependency direction:

- v1 prefill terminates before v1 decode, so old prefill does not remain after
  the old decode it depends on is gone.
- v2 decode starts before v2 prefill, so new prefill does not serve before the
  new decode is available.

`maxSkew` is reserved for future use and currently must be `0`, so members
advance in lockstep by wave number.

A wave is considered complete for advancement when every desired group in that
wave is Ready. Ready means the group has exactly the expected ordinals and each
Pod has `PodReady=True`; duplicate, missing, terminating, or merely Running
`0/1` Pods do not advance the wave, and services are not switched to that
group. PGDR counts completed waves from the beginning of the rollout, so a
later Ready group cannot skip an earlier unready wave. If replacement Pods stay
unready longer than
`progressDeadlineSeconds`, PGD deletes those unready replacement Pods so the
same group can be recreated and scheduled again instead of permanently blocking
on a bad node.

If the desired template revision changes while a previous rollout is stuck,
PGDR computes progress only against the new desired revision. The new rollout
therefore starts from wave 0 instead of inheriting the old rollout's completed
wave count.

### PGDR Control Flow

For each reconcile, PGDR:

1. Reconciles the generated child PGDs from `spec.prefill`, `spec.decode`, or
   `spec.unified`.
2. Reads each member's desired group count, desired revision, Pods, and rollout
   status.
3. Plans member windows for the current cohort wave.
4. Computes the slowest completed wave across members.
5. Publishes a rollout gate to each member PGD as controller-owned annotations:
   - allowed group index start
   - allowed group index end
   - rollout wave
   - rollout phase
   - rollout action: `hold`, `delete-old`, or `create-new`
   - paused reason, when held at a phase barrier
6. Updates PGDR status with member progress and cohort phase.

The PGD controller treats PGDR as the orchestrator and only executes the
published action. `delete-old` removes active old groups in the allowed window.
`create-new` creates replacements only after old groups are gone. `hold`
preserves the current state and deletes parked replacements that are not
selected by the stable group Service. This avoids hidden surge capacity while a
PGDR-coordinated rollout is paused at a barrier.

For PGDR-managed children, the same gate applies to first create. A child PGD
does not create groups outside the current PGDR window, even when it has no
active groups yet.

If a required role spec is missing or invalid, PGDR is `Blocked`. Stale rollout
gates are removed from PGDs when no current PGDR references them.

## Group Restart Semantics

A group is meant to load and serve model weights as a unit. If any active Pod in
the group is deleted, terminating, failed, or completed, the controller deletes
the remaining Pods in that same group instance. The group is then recreated as a
fresh group instance using the current revision.

This covers manual Pod deletion and non-Running terminal states. The behavior is
intentionally group-wide so partial reloads do not leave one member serving
weights loaded at a different time than the rest of its group.

## Status

The CRD exposes printer columns:

```text
GROUPS       .spec.groups
GROUP-SIZE   .spec.replicasPerGroup
RUNNING      .status.total.runningPods
REVISION     .status.currentRevision
```

Status shape:

```yaml
status:
  observedGeneration: 3
  currentRevision: 8a80854d
  groups:
  - index: 0
    schedulerPodGroup: gpu-one-0
    service: gpu-one-0
    groupInstance: gpu-one-0
    revision: 8a80854d
    topologyValue: podgroup-operator-m02
    phase: Ready
    running: 4
    ready: 4
    pending: 0
    failed: 0
    targetPod: gpu-one-0-0
  total:
    desiredGroups: 3
    desiredPods: 12
    runningPods: 12
    pendingPods: 0
    failedPods: 0
```

`topologyValue` is reported from the configured `topologyKey`. For hostname
groups it is usually the node name. For NVLink-domain groups it is the domain
label value, such as `1` or `2`.

PGD status also exposes the active PGDR rollout gate, when present:

```yaml
status:
  rolloutGate:
    rollout: qwen-engine
    wave: 0
    phase: TerminatePrefill
    action: delete-old
    allowedStart: 0
    allowedEnd: 3
    pausedReason: waiting for rollout phase TerminatePrefill
```

PGDR exposes printer columns for operational debugging:

```text
MODE      .spec.mode
WAVE      .status.currentWave
PHASE     .status.phase
ACTIONS   .status.actions
STEP      .status.currentPhase, in kubectl get -o wide
```

`kubectl get podgroupdeploymentrollout -owide -w` shows the current step
and member actions:

```text
NAME          MODE     WAVE   PHASE         STEP               ACTIONS                          AGE
qwen-engine   disagg   2      Progressing   CreatePrefill      prefill=create-new,decode=hold   6m1s
qwen-engine   disagg   3      Progressing   TerminatePrefill   prefill=delete-old,decode=hold   6m15s
qwen-engine   disagg   3      Progressing   TerminateDecode    prefill=hold,decode=delete-old   6m33s
qwen-engine   disagg   3      Progressing   CreateDecode       prefill=hold,decode=create-new   6m49s
```

PGDR member status reports `updatedReadyGroups`. Ready means desired-revision
groups with the exact expected ordinals and `PodReady=True` on every Pod.
Ratioed advancement uses contiguous Ready wave windows, not just the total
Ready count.

## Local Validation

The supported local environment is a Minikube profile that simulates a GB300
cluster:

- One control-plane node, tainted with `control-plane=true:NoSchedule`.
- Twenty-four worker nodes.
- Each worker is labeled and tainted with `nvidia.com/gpu=true`.
- Each worker is patched with mock `nvidia.com/gpu=4` capacity and allocatable.
- Workers are labeled into NVLink domains of eight nodes each:
  - GPU workers 1-8: `ds.coreweave.com/nvlink.domain=1`
  - GPU workers 9-16: `ds.coreweave.com/nvlink.domain=2`
  - GPU workers 17-24: `ds.coreweave.com/nvlink.domain=3`

The repo includes `scheduler-plugin/install-minikube.sh`, which installs the
scheduler-plugins Coscheduling CRDs and configures Coscheduling on the
`default-scheduler` profile.

Useful validation scenarios:

- `examples/gpu-1-podgroupdeployment.yaml`: 3 hostname groups, 4 Pods per
  group, 1 GPU per Pod.
- `examples/gpu-2-podgroupdeployment.yaml`: hostname groups, 2 Pods per group,
  2 GPUs per Pod.
- `examples/gpu-4-nvlink-podgroupdeployment.yaml`: NVLink-domain groups, 2 Pods
  per group, 4 GPUs per Pod.
- `examples/gpu-tp8-nvlink-podgroupdeployment.yaml` plus
  `examples/gpu-tp1-hostname-podgroupdeployment.yaml`: fills four worker nodes
  with one TP8 group in `ds.coreweave.com/nvlink.domain=1` and two TP1 hostname groups in
  `ds.coreweave.com/nvlink.domain=2`.

## RBG Prefill/Decode Lifecycle Parity

The comparison with RBG is intentionally scoped to prefill/decode PodGroup
lifecycle. App-level composition, router roles, monitoring roles, runtime
profiles, and user-facing `InferenceService` status aggregation belong in the
OME `InferenceService` layer, not in `podgroup-operator`. Volcano support is
also out of scope for this operator.

Supported parity items:

1. Prefill/decode role lifecycle. `disagg` PGDR owns and coordinates two member
   PGDs named `<engine>-prefill` and `<engine>-decode`.
2. Group or role-instance lifecycle. PGD treats each group as the operational
   unit for create, restart, replacement, status, and Service selection.
3. Gang scheduling. PGD creates scheduler-plugins `PodGroup` objects and
   labels every managed Pod with the corresponding scheduler PodGroup.
4. Same-role topology placement. PGD injects required Pod affinity using
   `spec.topologyKey`, so Pods within one group stay in the same hostname or
   NVLink-domain topology.
5. Coordinated prefill/decode recreate rollout. PGDR rolls disaggregated
   updates in dependency-safe order:
   `TerminatePrefill -> TerminateDecode -> CreateDecode -> CreatePrefill`.
6. Coordinated prefill/decode in-place rollout. For image or metadata-only
   template changes, PGDR updates decode groups first, then prefill groups,
   while preserving existing Pod names, UIDs, group instances, and Services.
7. Recreate fallback and safety policy. `InPlaceIfPossible` falls back to
   recreate when the template diff requires Pod replacement. `InPlaceOnly`
   rejects recreate-required changes, and `RecreatePod` disables in-place
   updates.
8. Ratio-aware rollout waves. Disaggregated PGDR computes shared cohort waves
   so prefill and decode advance in their configured group ratio instead of
   one group per role at a time.
9. Ratio-aware initial bringup. PGDR applies the same gated wave plan to first
   create, advancing only after the previous wave is Ready.
10. New-revision reset while blocked. If a new template revision is applied
   while an older rollout is blocked on unready pods, PGDR recomputes progress
   against the new desired revision and starts from wave 0.
11. Group-wide restart semantics. If a managed Pod is deleted, terminating, or
   reaches a terminal phase, PGD recreates the whole group rather than leaving
   a partial group in service.
12. Basic lifecycle status. PGD reports per-group phase, revision, target Pod,
   running/pending/failed counts, and topology value. PGDR reports current
   wave, phase, member actions, and blocked reasons.

Remaining good-to-have gaps:

1. Prefill/decode topology pairing. PGD can keep Pods within one group in the
   same topology domain through `spec.topologyKey`, such as
   `ds.coreweave.com/nvlink.domain`. PGDR does not currently pair related
   prefill and decode groups into the same topology domain. If runtime
   communication benefits from P/D locality, PGDR needs an explicit pairing
   policy instead of relying on gang scheduling alone.
2. Richer group lifecycle status. PGD and PGDR expose core rollout state, but
   operators may need more detailed per-group reasons for scheduling blocks,
   topology constraints, restart cascades, rollout phase barriers, and
   partially completed lifecycle operations.
3. Escape hatches for controller-owned mutations. The operator injects
   controller-owned labels, affinity, scheduler `PodGroup` references, rollout
   gates, and Service selectors. Future versions may need per-mutation opt-outs
   for advanced workloads that need to disable one injected behavior without
   disabling the rest of the controller.

## Non-Goals For This Operator

- Single user-facing app object. OME `InferenceService` should compose and own
  PGD/PGDR resources.
- Router, monitoring, and auxiliary roles.
- Runtime profile, sidecar, init container, and shared volume injection.
- Autoscaling policy and coordinated scaling decisions. OME `InferenceService`
  or external autoscalers should decide desired prefill/decode group counts.
- Request routing and app-level service discovery.
- User-facing `InferenceService` condition aggregation.
- Volcano scheduler integration.

## Current Limits

- `maxSurge` and integer `maxUnavailable` are group counts; percentage
  unavailable rollout is available through `maxUnavailablePercent`.
- The operator does not derive group size from GPU requests or domain capacity.
- The operator relies on scheduler-plugins Coscheduling being installed and
  active for the configured scheduler name.
- Service routing is label-selector based; the operator does not manage
  EndpointSlices directly.
- Pod template changes roll groups. Image and metadata-only changes can use the
  in-place path; changes to already-scheduled immutable Pod fields require
  group replacement.

## Sources

- Scheduler-plugins PodGroup Coscheduling KEP: https://github.com/kubernetes-sigs/scheduler-plugins/blob/master/kep/42-podgroup-coscheduling/README.md
- Scheduler-plugins Coscheduling docs: https://scheduler-plugins.sigs.k8s.io/docs/plugins/coscheduling/
- Kubernetes scheduling affinity docs: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/
