# PodGroup Operator

`podgroup-operator` provides a `PodGroupDeployment` workload API for grouped GPU
workloads:

```yaml
apiVersion: podgroup.meta.com/v1alpha1
kind: PodGroupDeployment
```

The operator directly manages Pods, Services, and upstream scheduler-plugins
`scheduling.x-k8s.io/v1alpha1` `PodGroup` objects. It does not create
Deployments, ReplicaSets, or StatefulSets, and it does not bind Pods to nodes.
Scheduling still goes through the Kubernetes `default-scheduler` path with the
scheduler-plugins Coscheduling plugin enabled.

The same operator also provides a `PodGroupDeploymentRollout` API that can own
and generate disaggregated prefill/decode or unified `PodGroupDeployment`
resources, then coordinate their rollout waves as one deployment operation.

The binary runs as a `controller-runtime` manager with separate event-driven
reconcilers for `PodGroupDeployment` and `PodGroupDeploymentRollout`. PGD
reconciles are triggered by PGD changes plus owned Pod, Service, and scheduler
PodGroup events. PGDR reconciles are triggered by PGDR changes plus generated
child PGD, Pod, Service, and scheduler PodGroup events. A small background
cleaner only handles stale rollout-gate annotations left behind by deleted or
changed PGDRs.

OME `InferenceService`, Helm, or GitOps should compose this with the rest of
the serving stack. This operator owns the GPU engine PodGroup lifecycle. Router
Deployments, model PVCs, runtime selection, and user-facing service status stay
at the inference-service layer.

## What It Supports

- Gang scheduling per group through scheduler-plugins `PodGroup` objects.
- Direct Pod ownership with stable group and ordinal labels.
- Single-node groups by using `spec.topologyKey: kubernetes.io/hostname`.
- Multi-node GPU-domain groups by using a shared node label such as
  `spec.topologyKey: ds.coreweave.com/nvlink.domain`.
- One stable Service per group, selecting only the configured target ordinal
  Pod from the active group instance.
- One top-level Service named after the `PodGroupDeployment`, selecting the
  target ordinal Pod from every active group.
- Group-level rolling updates with `maxSurge`, integer `maxUnavailable`, or
  percentage `maxUnavailablePercent` counted in whole groups, not Pods.
- Group-level restart behavior: if one active Pod in a group is deleted,
  terminating, failed, or completed, the operator terminates the rest of that
  group instance so the full group restarts together.
- Cross-PGD rollout coordination through `PodGroupDeploymentRollout` for
  disaggregated prefill/decode engines. PGDR can generate the role PGDs from
  embedded `prefill` and `decode` specs.
- Coordinated unified rollouts through `PodGroupDeploymentRollout` with
  `mode: unified`.
- Ratioed initial bringup for PGDR-managed roles, so oversized PD deployments
  stop at the first Pending wave instead of submitting every prefill and decode
  group at once.
- New-revision reset for blocked PGDR rollouts. A template change is evaluated
  against the new desired revision and starts from wave 0.
- Status conditions for PGD and PGDR readiness, progress, rollout gates, and
  blocked/degraded rollout states.
- Kubernetes Events for rollout gate changes, child PGD reconciliation, stale
  gate cleanup, and finalizer completion.
- Finalizer-backed cleanup for managed Pods, Services, scheduler PodGroups, and
  generated child PGDs.
- Status columns for `GROUPS`, `GROUP-SIZE`, `RUNNING`, and `REVISION`.

## Dependency Flow

```text
InferenceService / GitOps
  -> PodGroupDeploymentRollout
    -> PodGroupDeployment children
      -> scheduler-plugins PodGroups
      -> Pods
      -> Services

Router Deployment
  -> PGD top-level Services or service-target Pods
```

PGD creates scheduler `PodGroup` objects for gang scheduling, directly manages
Pods, and creates one top-level Service plus one Service per logical group.
PGDR owns generated child PGDs and publishes rollout gates. It does not create
Pods or Services directly. The router is expected to discover PGD services or
rank/service-target Pods by label.

## API

Common fields:

```yaml
spec:
  groups: 3
  replicasPerGroup: 4
  multiNode:
    enabled: true
    tensorParallelSize: 8
    distInitPort: 20000
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
          limits:
            nvidia.com/gpu: "1"
```

Defaults:

- `topologyKey`: `kubernetes.io/hostname`
- `serviceTargetOrdinal`: `0`
- `schedulerName`: `default-scheduler`
- `template.spec.restartPolicy`: `Never`
- `template.spec.terminationGracePeriodSeconds`: `1`
- `updateStrategy.rollingUpdate.maxUnavailablePercent`: `20` when rollout
  limits are unset
- `updateStrategy.rollingUpdate.progressDeadlineSeconds`: `3600`
- `multiNode.distInitPort`: `20000`

## Scheduling Model

Each logical group gets one scheduler-plugins `PodGroup` with
`spec.minMember = spec.replicasPerGroup`. Every Pod in that group is labeled:

```yaml
scheduling.x-k8s.io/pod-group: <group-instance-name>
```

The scheduler-plugins Coscheduling plugin provides the gang/quorum behavior.
Normal Kubernetes scheduling handles resource fit, node selectors, taints,
tolerations, and affinity.

The operator also injects required Pod affinity for each group instance:

```yaml
topologyKey: <spec.topologyKey>
labelSelector:
  matchLabels:
    app.kubernetes.io/instance: <podgroupdeployment-name>
    podgroup.meta.com/group-instance: <group-instance-name>
```

For `topologyKey: kubernetes.io/hostname`, all Pods in a group land on the same
node if resources fit. For `topologyKey: ds.coreweave.com/nvlink.domain`, Pods in a group land in
the same labeled GPU domain and may spread across nodes inside that domain.

GPU runtime settings stay in the Pod template: resource requests such as
`nvidia.com/gpu` and `rdma/ib`, node selectors, tolerations, priority class,
Multus annotations, volumes, probes, and runtime command arguments are all
ordinary Pod template fields.

## Services

For `metadata.name: gpu-one` and `groups: 3`, the operator creates:

```text
Service gpu-one     -> target ordinal Pod from all active groups
Service gpu-one-0   -> target ordinal Pod from group 0
Service gpu-one-1   -> target ordinal Pod from group 1
Service gpu-one-2   -> target ordinal Pod from group 2
```

By default, the target ordinal is `0`, so each group Service routes only to the
group's `-0` Pod. The top-level Service routes to the active `-0` Pod from every
group. Service ports are derived from container ports in the Pod template; if no
container ports are present, the operator creates a default `http:80` port.

Services select Pods directly. The top-level Service does not forward to the
per-group Services. For a TP8-style multi-node group with `replicasPerGroup: 2`
and `serviceTargetOrdinal: 0`, the per-group Service points at rank 0 for that
group, while the top-level Service points at rank 0 for every currently active
group.

## Multi-Node Groups

PGD can inject distributed startup arguments for multi-node groups:

```yaml
spec:
  replicasPerGroup: 2
  multiNode:
    enabled: true
    tensorParallelSize: 8 # optional when --tp-size is already in the command
    distInitPort: 20000
```

When enabled and TP size is greater than 4, PGD sets these arguments on the
target container for each generated Pod:

```text
--nnodes <replicasPerGroup>
--node-rank <pod ordinal>
--dist-init-addr <pgd-name>-<group>.<namespace>.svc.cluster.local:<distInitPort>
```

Ordinal `0` is rank 0 because `serviceTargetOrdinal` defaults to `0`. The
existing per-group Service, for example `sglang-pd-prefill-1`, selects rank 0
for that group and is reused as the distributed init address. PGD also adds a
`dist-init` port to that per-group Service when the port is not already exposed and sets
`publishNotReadyAddresses` on the per-group Service so non-zero ranks can reach
rank 0 before the group is Ready. The top-level PGD Service keeps normal
readiness behavior for serving traffic.

For TP16 on GB300-style nodes with 4 GPUs per node, set `replicasPerGroup: 4`
and `tensorParallelSize: 16` or include `--tp-size 16` in the target container command.

During a `maxSurge: 0` rollout, a group can be intentionally unavailable
between deleting the old group and creating the replacement. In that window,
the top-level Service has fewer endpoints and the affected per-group Service
can have zero endpoints. Existing top-level endpoints still refer to Ready
target ordinal Pods.

For PGDR-created prefill/decode pods, routers should include
`podgroup.meta.com/service-target=true` so they discover only the configured
rank Pod in each group:

```text
prefill: app.kubernetes.io/component=prefill,app.kubernetes.io/instance=<name>-prefill,podgroup.meta.com/service-target=true
decode:  app.kubernetes.io/component=decode,app.kubernetes.io/instance=<name>-decode,podgroup.meta.com/service-target=true
```

## Rollouts And Restarts

Template changes produce a new revision hash. Replacement group instance names
include that hash:

```text
gpu-one-8a80854d-0-0
gpu-one-8a80854d-0-1
gpu-one-8a80854d-0-2
gpu-one-8a80854d-0-3
```

Rollout limits are counted by group:

- `maxSurge: 1` creates one extra replacement group, waits for all Pods in that
  group to become Ready, points the stable group Service at the replacement,
  and deletes the old group.
- `maxSurge: 0` with `maxUnavailablePercent: 20` rolls by deleting up to 20%
  of groups, rounded down with a minimum of one group, then creating their
  replacements.
- `maxUnavailable: 2` remains supported as an integer compatibility override.
- `progressDeadlineSeconds` deletes unready replacement Pods after the deadline
  so they can be recreated and scheduled again instead of blocking forever on a
  bad node.

If an active group loses one Pod, the operator deletes the remaining Pods in
that same group instance. This keeps multi-Pod model weight loading aligned.

## Coordinated Rollouts

`PodGroupDeploymentRollout` is the top-level API for disaggregated
prefill/decode lifecycle. It owns the generated role PGDs, while each PGD still
owns Pods, Services, scheduler `PodGroup` objects, topology affinity, and group
replacement.

The disaggregated shape is declared in one object:

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

- `mode: disagg` generates and coordinates `<name>-prefill` and
  `<name>-decode`.
- `base` carries shared PGD configuration and pod template fields. Role
  cardinality stays in `prefill`, `decode`, or `unified`; `base.groups` and
  `base.replicasPerGroup` are ignored.
- `mode: unified` generates and rolls a single `<name>-unified` PGD. A
  standalone PGD is still enough when no top-level rollout object is needed.

For 40 prefill groups and 20 decode groups with
`maxUnavailablePercent: 20`, the rollout waves are 8 prefill groups and 4
decode groups. PGDR uses the same ratioed wave plan for first bringup and for
template rollouts, so an oversized deployment does not create every prefill and
decode group at once and let the scheduler admit a skewed subset.

In `disagg` mode, each wave is ordered as:

```text
TerminatePrefill -> TerminateDecode -> CreateDecode -> CreatePrefill
```

PGDR publishes the current phase and action to each PGD. PGDs then execute only
`hold`, `delete-old`, or `create-new` for the allowed window. This prevents old
prefill from outliving old decode, prevents new prefill from starting before new
decode, and avoids hidden surge capacity while waiting at a barrier.

PGDR applies child changes in observable steps. If PGDR creates, updates, or
deletes child PGDs, it reports `CurrentPhase=ReconcilingChildren` and waits for
the next reconcile before making rollout decisions. If a child PGD has
`metadata.generation` greater than `status.observedGeneration`, PGDR holds that
member and records the stale generation in `status.members`.

For initial bringup and updates, a wave advances only after the desired groups
in the prior wave are Ready. Ready means the group has exactly the expected
ordinals and every Pod has `PodReady=True`; duplicate, missing, terminating, or
Running `0/1` Pods do not count. PGDR advances through contiguous Ready wave
windows, so a later Ready group cannot skip an earlier unready wave. If
replacement Pods remain unready past `progressDeadlineSeconds`, PGD deletes
them so the same group can be recreated and scheduled again.

If a new template revision is applied while an older rollout is blocked on
unready pods, PGDR recomputes progress against the new desired revision. The
new rollout starts from wave 0 and rolls both prefill and decode from that new
revision.

Deletion is explicit. PGD keeps a finalizer until its managed Pods, Services,
and scheduler `PodGroup` objects are gone. PGDR keeps a finalizer until its
owned child PGDs are gone. This prevents a parent resource from disappearing
while controller-owned children are still observable.
Finalizers are inserted before child objects are created or updated, so cleanup
protection is persisted before the controller starts managing workload
resources.

The operator also emits Kubernetes Events on PGD and PGDR resources for gate
changes, child reconciliation pauses, stale gate cleanup, and completed
finalization. These events are intended for quick debugging alongside status
conditions:

```bash
kubectl -n podgroup-demo describe podgroupdeploymentrollout qwen-engine
kubectl -n podgroup-demo describe podgroupdeployment qwen-engine-prefill
```

Watch the coordinated rollout with:

```bash
kubectl -n podgroup-demo get podgroupdeploymentrollout -owide -w
```

Example output during a disaggregated rollout:

```text
NAME          MODE     WAVE   PHASE         STEP               ACTIONS                          AGE
qwen-engine   disagg   2      Progressing   CreatePrefill      prefill=create-new,decode=hold   6m1s
qwen-engine   disagg   3      Progressing   TerminatePrefill   prefill=delete-old,decode=hold   6m15s
qwen-engine   disagg   3      Progressing   TerminateDecode    prefill=hold,decode=delete-old   6m33s
qwen-engine   disagg   3      Progressing   CreateDecode       prefill=hold,decode=create-new   6m49s
```

## Build And Test

Source layout follows the surrounding OME convention:

- `cmd/podgroup-operator`: binary entrypoint and in-cluster client bootstrap.
- `pkg/controller/v1alpha1`: PGD/PGDR controller logic, API structs, rollout
  planning, resource construction, and tests.
- `manifests`: CRDs, RBAC, and operator Deployment.
- `examples`: sample PGD/PGDR workloads.

```bash
go test ./...
go vet ./...
docker build -t podgroup-operator:<unique-tag> .
```

## Local Minikube

The local setup uses a GB300-shaped Minikube profile: one tainted control-plane
node and 24 worker nodes. The workers are patched with mock
`nvidia.com/gpu=4` capacity and labeled into NVLink domains of eight nodes
each:

```text
podgroup-operator-m02  ds.coreweave.com/nvlink.domain=1
podgroup-operator-m03  ds.coreweave.com/nvlink.domain=1
...
podgroup-operator-m09  ds.coreweave.com/nvlink.domain=1
podgroup-operator-m10  ds.coreweave.com/nvlink.domain=2
```

Run the local setup:

```bash
./scripts/local-minikube.sh
```

The script builds the image, starts Minikube, patches worker GPU resources,
installs the CRD/RBAC/operator, installs scheduler-plugins Coscheduling for the
`default-scheduler` profile, and applies the basic GPU example.

Manual install flow:

```bash
./scheduler-plugin/install-minikube.sh
kubectl apply -f manifests/crd.yaml
kubectl apply -f manifests/pgdr-crd.yaml
kubectl apply -f manifests/rbac.yaml
kubectl apply -f manifests/deployment.yaml
kubectl apply -f examples/namespace.yaml
```

Inspect state:

```bash
kubectl -n podgroup-demo get podgroupdeployments,podgroups,pods,svc -o wide
kubectl -n podgroup-demo get podgroupdeployment
```

Useful status fields:

```bash
kubectl -n podgroup-demo get pgd <name> -o jsonpath='{.status.conditions}'
kubectl -n podgroup-demo get pgdr <name> -o jsonpath='{.status.conditions}'
kubectl -n podgroup-demo get pgdr <name> -o jsonpath='{.status.members[*].generation}'
kubectl -n podgroup-demo get pgdr <name> -o jsonpath='{.status.members[*].observedGeneration}'
```

## Examples

Single-node TP1-style groups:

```bash
kubectl apply -f examples/gpu-1-podgroupdeployment.yaml
```

This creates 3 groups of 4 Pods. Each Pod requests `nvidia.com/gpu: 1`, so each
group consumes one 4-GPU node when `topologyKey` is `kubernetes.io/hostname`.

Single-node groups with larger per-Pod GPU requests:

```bash
kubectl apply -f examples/gpu-2-podgroupdeployment.yaml
```

This creates groups of 2 Pods where each Pod requests `nvidia.com/gpu: 2`,
again consuming one 4-GPU node per group.

Multi-node NVLink-domain groups:

```bash
kubectl apply -f examples/gpu-4-nvlink-podgroupdeployment.yaml
```

This creates 2 groups of 2 Pods. Each Pod requests `nvidia.com/gpu: 4`; with
`topologyKey: ds.coreweave.com/nvlink.domain`, each group consumes two 4-GPU nodes in the same
NVLink domain.

Mixed local fill test:

```bash
kubectl apply -f examples/gpu-tp8-nvlink-podgroupdeployment.yaml
kubectl apply -f examples/gpu-tp1-hostname-podgroupdeployment.yaml
```

The TP8 example consumes two 4-GPU nodes in `ds.coreweave.com/nvlink.domain=1`. The TP1 example
consumes two 4-GPU nodes in `ds.coreweave.com/nvlink.domain=2`, with each 4-Pod group pinned to a
single node.
