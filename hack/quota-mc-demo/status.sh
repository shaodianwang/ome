#!/usr/bin/env bash
# What landed where, across the hub and both members, in one screen.
#
#   hack/quota-mc-demo/status.sh
#
# Point the env vars below at the hub and member kubeconfigs (the defaults are
# placeholders): MC_DEMO_HUB, MC_DEMO_MEMBERS (space-separated), MC_DEMO_NS.
set -uo pipefail

HUB="${MC_DEMO_HUB:-$HOME/.kube/<hub>}"
MEMBERS="${MC_DEMO_MEMBERS:-$HOME/.kube/<member-a> $HOME/.kube/<member-b>}"
NS="${MC_DEMO_NS:-ome-mc-test}"

h() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }
sub() { printf '\n-- %s\n' "$1"; }

# kubectl writes "No resources found" to stderr and still exits 0, so a `||`
# fallback never fires and an empty section reads as a missing one. Buffer the
# output and say so instead.
none() {
  local out
  out="$(cat)"
  if [ -n "$out" ]; then printf '%s\n' "$out"; else echo "  (none)"; fi
}

hub() { kubectl --kubeconfig "$HUB" "$@" 2>/dev/null; }

h "hub  $(basename "$HUB")"

sub "workload clusters (labels are the placer's entire candidate input)"
hub get workloadclusters \
  -o custom-columns='NAME:.metadata.name,READY:.status.conditions[?(@.type=="Ready")].status,LABELS:.metadata.labels' | none

sub "quota tree, as authored"
hub get acceleratorquota -o wide | none

sub "resolved per-cluster shares"
hub get acceleratorquota -o json | jq -r '
  .items[]
  | select(.status.budgets != null)
  | .metadata.name as $n
  | .status.budgets[]
  | select(.perCluster != null)
  | "  \($n)  \(.resourceName) on \(.resourceFlavor):  " +
    ([.perCluster[] | "\(.cluster)=\(.nominal // 0) (admitted \(.admitted // 0))"] | join("  "))
' 2>/dev/null | none

# status.clusters is where a stalled projection explains itself, and no
# kubectl column surfaces it. A node whose shares cannot be resolved still
# reports Ready=True and Degraded=False, so the flat listing above looks
# healthy while nothing has reached a member.
sub "projection state per cluster (a message here means it did NOT land)"
hub get acceleratorquota -o json | jq -r '
  .items[]
  | .metadata.name as $n
  | (.status.clusters // [])[]
  | "  \($n) -> \(.cluster): " +
    (if .message then "STALLED: \(.message)"
     else "applied gen \(.appliedGeneration // 0), materialized gen \(.materializedGeneration // 0)" end)
' 2>/dev/null | none

sub "fleet capacity the hub read back from the members"
hub get acceleratorquota root -o json | jq -r '
  .status.capacity[]?
  | "  \(.resourceName) on \(.resourceFlavor):  allocatable=\(.allocatable // "-") highWater=\(.highWaterMark // "-")",
    ([.perCluster[]? | "    \(.cluster): allocatable=\(.allocatable // "-") highWater=\(.highWaterMark // "-")"] | join("\n"))
' 2>/dev/null | none

sub "source InferenceServices"
hub -n "$NS" get isvc \
  -o custom-columns='NAME:.metadata.name,MODE:.spec.placement.mode,PHASE:.status.placement.phase,WINNER:.status.placement.cluster' | none
hub -n "$NS" get isvc -o json | jq -r '
  .items[]
  | .metadata.name as $n
  | (.status.placement.candidates // [])[]
  | "  \($n) -> \(.cluster): phase=\(.phase // "-") ready=\(.readyReplicas // 0)"
' 2>/dev/null

for kc in $MEMBERS; do
  m() { kubectl --kubeconfig "$kc" "$@" 2>/dev/null; }
  h "member  $(basename "$kc")"

  sub "derived capacity on the root"
  m get acceleratorquota root -o json | jq -r '
    .status.capacity[]?
    | "  \(.resourceName) on \(.resourceFlavor):  allocatable=\(.allocatable // "-") highWater=\(.highWaterMark // "-")"
  ' 2>/dev/null | none

  sub "projected tree (nominal here is THIS cluster's share)"
  m get acceleratorquota -o wide | none

  sub "ClusterQueues the tree materialized"
  m get clusterqueue \
    -o custom-columns='NAME:.metadata.name,COHORT:.spec.cohort,PENDING:.status.pendingWorkloads,ADMITTED:.status.admittedWorkloads' | none

  sub "LocalQueues in $NS"
  m -n "$NS" get localqueue | none

  sub "derived InferenceServices"
  m -n "$NS" get isvc | none

  sub "pods — a name in GATES is a workload quota has not funded"
  m -n "$NS" get pods \
    -o custom-columns='NAME:.metadata.name,PHASE:.status.phase,NODE:.spec.nodeName,GATES:.spec.schedulingGates[*].name' | none

  sub "Kueue workloads"
  m -n "$NS" get workloads \
    -o custom-columns='NAME:.metadata.name,QUEUE:.spec.queueName,ADMITTED:.status.conditions[?(@.type=="Admitted")].status,REASON:.status.conditions[?(@.type=="QuotaReserved")].reason' | none
done

echo
