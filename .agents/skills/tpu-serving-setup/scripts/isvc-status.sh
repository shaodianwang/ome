#!/bin/sh
# One-screen read of an OME InferenceService on a TPU cluster, in the order
# that localises a failure: ISVC conditions, InferenceReplicas, pods and the
# slice they landed on, scheduling gates, events, the leader's JAX line.
#
#   isvc-status.sh <kubeconfig> <context> <namespace> <name>
#
# Read-only: every call is a `get`/`logs`. POSIX sh; needs kubectl and jq.
set -u

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <kubeconfig> <context> <namespace> <name>" >&2
  exit 2
fi
KC=$1; CTX=$2; NS=$3; NAME=$4
command -v jq >/dev/null 2>&1 || { echo "jq is required" >&2; exit 2; }

k() { kubectl --kubeconfig "$KC" --context "$CTX" "$@"; }
h() { printf '\n== %s ==\n' "$1"; }
# kubectl prints "No resources found" on stderr and exits 0, so buffer and say so.
none() {
  out=$(cat)
  if [ -n "$out" ]; then printf '%s\n' "$out"; else echo "  (none)"; fi
}

h "inferenceservice $NS/$NAME"
k -n "$NS" get isvc "$NAME" -o wide 2>/dev/null | none
k -n "$NS" get isvc "$NAME" -o json 2>/dev/null | jq -r '
  "deploymentMode: \(.spec.deploymentMode // "-")   runtime: \(.spec.runtime.name // "-")   model: \(.spec.model.name // "-")",
  "url: \(.status.url // "-")",
  (.status.conditions // [])[] | "  \(.type)=\(.status) \(.reason // "") \(.message // "")"'

h "inferencereplicas"
k -n "$NS" get inferencereplicas -o wide 2>/dev/null | awk -v n="$NAME" 'NR==1 || index($1, n)==1' | none

h "pods (label ome.io/inferenceservice=$NAME)"
k -n "$NS" get pods -l "ome.io/inferenceservice=$NAME" -o wide 2>/dev/null | none
k -n "$NS" get pods -l "ome.io/inferenceservice=$NAME" -o json 2>/dev/null | jq -r '
  .items[] | "  \(.metadata.name): phase=\(.status.phase) scheduler=\(.spec.schedulerName // "default")"
    + " gates=\([.spec.schedulingGates[]?.name] | join(","))"
    + " containers=\([.status.containerStatuses[]? | "\(.name):ready=\(.ready),restarts=\(.restartCount),\(.state | keys[0])"] | join(" "))"'

h "nodes and slices under those pods"
nodes=$(k -n "$NS" get pods -l "ome.io/inferenceservice=$NAME" -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null | sort -u | grep -v '^$')
if [ -n "$nodes" ]; then
  # shellcheck disable=SC2086
  k get nodes $nodes -L cloud.google.com/gke-tpu-accelerator,cloud.google.com/gke-tpu-topology,cloud.google.com/gke-tpu-partition-2x2x2-id 2>/dev/null | none
else
  echo "  (no pod is scheduled yet)"
fi

h "service and endpoints"
k -n "$NS" get svc "$NAME" -o wide 2>/dev/null | none
k -n "$NS" get endpointslice -l "kubernetes.io/service-name=$NAME" 2>/dev/null | none

if k api-resources --no-headers 2>/dev/null | awk '{print $1}' | grep -qx podgroups; then
  h "podgroups"
  k -n "$NS" get podgroups 2>/dev/null | awk -v n="$NAME" 'NR==1 || index($1, n)>0' | none
fi

h "events mentioning $NAME (oldest first)"
k -n "$NS" get events --sort-by=.lastTimestamp 2>/dev/null | awk -v n="$NAME" 'NR==1 || index($0, n)>0' | tail -25 | none

h "leader log: JAX device line"
leader=$(k -n "$NS" get pods -l "ome.io/inferenceservice=$NAME" -o json 2>/dev/null \
  | jq -r '[.items[] | select(.metadata.labels["ome.io/runner"] == "leader")][0].metadata.name // empty')
if [ -n "$leader" ]; then
  k -n "$NS" logs "$leader" -c ome-container --tail=200 2>/dev/null | grep -E 'JAX .*global=|TPU init|Traceback|Error' | tail -5 | none
else
  echo "  (no leader pod yet)"
fi
