# Inspecting a TPU InferenceService: the read-order

Read top-down and stop at the first layer that disagrees with the one above
it; that is where the fault lives. `scripts/isvc-status.sh "$KC" "$CTX" "$NS"
"$NAME"` prints all of these in one go. `k` below is
`kubectl --kubeconfig "$KC" --context "$CTX"`.

## 1. The ISVC itself

```sh
k -n "$NS" get isvc "$NAME" -o wide
k -n "$NS" get isvc "$NAME" -o jsonpath='{.spec.deploymentMode}{"\n"}'
k -n "$NS" get isvc "$NAME" -o json | jq -r '.status.conditions[] | "\(.type)=\(.status) \(.reason // "") \(.message // "")"'
```

Columns: `URL` (routed hostname when Gateway API is on), `READY`, `MODEL`,
`RUNTIME`, `PLACEMENT`, `SOURCE`, `DRIFT`, `AGE`. `Ready=False` with a
reason naming the runtime means the runtime object is missing, disabled, or
in another namespace; a webhook rejection never gets this far - it fails the
`create`.

## 2. InferenceReplicas

OMENative dispatches through `InferenceReplica`s, one per component. If none
exist, the ISVC did not resolve to OMENative (check the field, not the
annotation) or the controller is not reconciling the namespace.

```sh
k -n "$NS" get inferencereplicas -o wide          # COMPONENT, DESIRED, CURRENT, READY, AVAILABLE
```

## 3. Pods and where they landed

```sh
k -n "$NS" get pods -l "ome.io/inferenceservice=$NAME" -o wide
k -n "$NS" get pods -l "ome.io/inferenceservice=$NAME" \
  -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,SCHED:.spec.schedulerName,GATES:.spec.schedulingGates[*].name,NODE:.spec.nodeName
k get nodes <node-a> <node-b> -L cloud.google.com/gke-tpu-topology,cloud.google.com/gke-tpu-partition-2x2x2-id
```

Expect two pods, `SCHED=ome-scheduler`, no gates, two different nodes with
the **same** partition id. A gate present means Kueue is holding the pod
(quota); different partition ids mean the gang split across slices.

## 4. Events

```sh
k -n "$NS" get events --sort-by=.lastTimestamp | grep "$NAME" | tail -25
k -n "$NS" describe pod <leader-pod> | sed -n '/^Events:/,$p'
```

`FailedScheduling` with `Insufficient google.com/tpu` or an untolerated taint
is capacity or selector; `Failed to pull image` is the mirror; a PodGroup
message names the gang scheduler.

## 5. Container state and the leader log

```sh
k -n "$NS" get pods -l "ome.io/inferenceservice=$NAME" -o json \
  | jq -r '.items[] | "\(.metadata.name): " + ([.status.containerStatuses[]? | "\(.name) ready=\(.ready) restarts=\(.restartCount) \(.state|keys[0])"] | join(" "))'
LEADER=$(k -n "$NS" get pods -l "ome.io/inferenceservice=$NAME,ome.io/runner=leader" -o jsonpath='{.items[0].metadata.name}')
k -n "$NS" logs "$LEADER" -c ome-container --tail=100
k -n "$NS" logs "$LEADER" -c ome-container --previous --tail=50     # after a restart
```

The first useful line is `JAX <version> global=<n> local=<m> devices=...`.
For a 2x2x2 gang the healthy reading is `global=16 local=8` (cores: two per
chip). `global=8` means one host never joined; no line at all for many minutes
means JAX is waiting for a peer that is not in the same slice.

## 6. Service, endpoints, route

```sh
k -n "$NS" get svc "$NAME" -o wide
k -n "$NS" get endpointslice -l "kubernetes.io/service-name=$NAME"
k -n "$NS" get isvc "$NAME" -o jsonpath='{.status.url}{"\n"}'
k -n "$NS" get httproute 2>/dev/null | grep -E "NAME|$NAME"
```

The Service selects the leader; an empty endpoint slice with a Ready leader
means the selector and the pod labels disagree (a hand-edited runtime), not a
network fault. Routes are OME-owned; never create or patch them.

## 7. Cluster-side state (read only)

```sh
k get nodes -l cloud.google.com/gke-tpu-topology=2x2x2 -o json \
  | jq '[.items[] | select((.spec.unschedulable // false) == false)] | length'
k get deploy -A | grep -iE 'ome-controller-manager|ome-scheduler'
k -n ome logs deploy/ome-controller-manager --tail=200 | grep -i "$NAME"
```

Controller and scheduler logs are for reading, not for restarting from a
devserver. If they show a bug, keep the evidence in `$RUN_DIR` and open a
PR or issue.
