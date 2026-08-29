#!/usr/bin/env bash
# What the fleet looks like: the tree the hub holds, the copy each member
# received, and what each member's Kueue made of it. Read-only.
set -uo pipefail

HUB="${OME_FLEET_HUB:-ome-quota-hub}"
MEMBER_A="${OME_FLEET_MEMBER_A:-ome-quota-a}"
MEMBER_B="${OME_FLEET_MEMBER_B:-ome-quota-b}"

show() { # $1 = context, $2 = label
  echo "── ${2} (${1})"
  kubectl --context "$1" get acceleratorquota -o custom-columns='NAME:.metadata.name,ROLE:.spec.role,NOMINAL:.spec.budgets[0].nominal,ORIGIN:.metadata.labels.ome\.io/quota-origin,READY:.status.conditions[?(@.type=="Ready")].status' 2>/dev/null ||
    echo "  (unreachable)"
  echo
}

show "kind-${HUB}" "management plane — authored here"
show "kind-${MEMBER_A}" "member A — projected"
show "kind-${MEMBER_B}" "member B — projected"

# An ORIGIN of <none> on a member would mean a node an admin authored locally
# rather than a copy, which is what the hub's sweep must never touch.
echo "── registry"
kubectl --context "kind-${HUB}" get workloadclusters -o custom-columns='NAME:.metadata.name,READY:.status.conditions[?(@.type=="Ready")].status,REASON:.status.conditions[?(@.type=="Ready")].reason' 2>/dev/null
echo

echo "── what each member's Kueue made of its copy"
for c in "$MEMBER_A" "$MEMBER_B"; do
  echo "  ${c}:"
  kubectl --context "kind-${c}" get clusterqueues -o custom-columns='NAME:.metadata.name,ACTIVE:.status.conditions[?(@.type=="Active")].status' 2>/dev/null | sed 's/^/    /'
done
