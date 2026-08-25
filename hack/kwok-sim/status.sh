#!/usr/bin/env bash
# Where did the gangs land, and how much of the fleet is committed?
set -euo pipefail

CTX="${KWOK_SIM_CONTEXT:-kind-ome-kwok}"
NS="${OME_SIM_NS:-kwok-sim}"
k() { kubectl --context "$CTX" "$@"; }

echo "== fleet =="
k get nodes -l kwok-sim/cube -o json | jq -r '
  (.items | group_by(.metadata.labels["cloud.google.com/gce-topology-subblock"])[] |
    "cube \(.[0].metadata.labels["kwok-sim/cube"])  \(.[0].metadata.labels["cloud.google.com/gke-tpu-topology"])  nodes=\(length)  chips=\([.[].status.capacity["google.com/tpu"]|tonumber]|add)")'

echo
echo "== inference services =="
k -n "$NS" get isvc 2>/dev/null || true

echo
echo "== gang placement (pods per node pool) =="
k -n "$NS" get pods -o json | jq -r '.items[] | select(.spec.nodeName) | "\(.metadata.labels["ome.io/inferenceservice"] // "-")\t\(.spec.nodeName)"' \
  | while IFS=$'\t' read -r isvc node; do
      printf '%s\t%s\n' "$isvc" "$(k get node "$node" -o jsonpath='{.metadata.labels.cloud\.google\.com/gke-nodepool}')"
    done | sort | uniq -c | awk '{printf "  %-3s %-14s %s\n", $1"x", $2, $3}'

echo
echo "== chip utilisation =="
total=$(k get nodes -l kwok-sim/cube -o json | jq '[.items[].status.capacity["google.com/tpu"]|tonumber]|add')
used=$(k -n "$NS" get pods -o json | jq '[.items[]|select(.spec.nodeName)|.spec.containers[].resources.limits["google.com/tpu"]//"0"|tonumber]|add // 0')
echo "  ${used:-0} / ${total:-0} chips"

echo
echo "== pending pods (if any) =="
k -n "$NS" get pods --field-selector=status.phase=Pending --no-headers 2>/dev/null | head || true

# Keyed on the tree, not on the CRD: ome-crd installs AcceleratorQuota whichever
# components up.sh selected, so the CRD's presence says nothing about whether
# this cluster runs a quota plane. The reserved root does — a workload-mode
# manager creates it for itself, so no nodes at all means no manager.
if [ -n "$(k get acceleratorquota -o name 2>/dev/null || true)" ]; then
  echo
  echo "== accelerator quota =="
  k get acceleratorquota 2>/dev/null || true
  # Capacity is reported on the root alone: it is the node that means "this whole
  # cluster". Budgets read against the high-water mark, not against allocatable.
  cap="$(k get acceleratorquota root -o json 2>/dev/null \
        | jq -r '.status.capacity // [] | .[] |
                 "  \(.resourceName) on \(.resourceFlavor): allocatable=\(.allocatable // 0) highWater=\(.highWaterMark // 0)"' \
        2>/dev/null || true)"
  echo
  echo "== derived capacity (root) =="
  echo "${cap:-  none — no ResourceFlavor matches the fleet, or the root does not exist}"
fi
