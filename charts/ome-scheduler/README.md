# ome-scheduler Helm chart

Installs [ome-scheduler](../../scheduler) — the upstream kube-scheduler built as a
library with the OME placement plugin (`OMEGangPack`, OEP-0022) — as a **second
scheduler**. It does not replace the cluster's default scheduler; workloads opt
in per-pod via `spec.schedulerName`.

## Why a separate chart

ome-scheduler is optional, opt-in, and pinned to the fleet's Kubernetes version
on its own cadence (it's a separate Go module). Keeping it out of the operator
chart (`ome-resources`) means its broad, sensitive scheduler RBAC isn't forced
onto every OME install, and it can be upgraded/rolled back independently.

## Prerequisite: the PodGroup CRD

The plugin reads the scheduler-plugins `PodGroup` CR
(`scheduling.x-k8s.io/v1alpha1`) for gang size and the topology annotation. That
CRD must be installed in the cluster first (it is **not** bundled here — it's an
external, shared dependency):

```sh
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/scheduler-plugins/v0.33.5/config/crd/bases/scheduling.x-k8s.io_podgroups.yaml
```

## Install

```sh
helm install ome-scheduler charts/ome-scheduler -n ome --create-namespace
```

## Using it

A workload opts in by setting the scheduler name and declaring its gang via a
PodGroup:

```yaml
# pod template
spec:
  schedulerName: ome-scheduler
  # + the standard gang label:
  #   metadata.labels["scheduling.x-k8s.io/pod-group"]: my-gang
```

```yaml
apiVersion: scheduling.x-k8s.io/v1alpha1
kind: PodGroup
metadata:
  name: my-gang
  annotations:
    ome.io/topology-key: <node-label-that-defines-a-domain>   # e.g. an NVLink clique / TPU slice / rack label
spec:
  minMember: <gang size>
  scheduleTimeoutSeconds: 600
```

The scheduler bakes in no workload conventions: gang size comes from
`minMember`, the domain label from the `ome.io/topology-key` annotation, and node
free-ness from the pod's own resource requests.

## Key values

| Value | Default | Meaning |
|---|---|---|
| `scheduler.name` | `ome-scheduler` | Profile / opt-in name (also the leader-election lock). |
| `scheduler.image.repository` / `.tag` | `ome-scheduler` / `latest` | Image (prefixed by `global.hub` if set). |
| `scheduler.replicaCount` | `1` | Set >1 with `leaderElect: true` for HA. |
| `scheduler.leaderElect` | `true` | Only the leader schedules. |
| `scheduler.verbosity` | `2` | klog `-v` level. |

See [`values.yaml`](values.yaml) for the full set.
