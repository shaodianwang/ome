#!/usr/bin/env bash
# Create the kwok TPU nodes emitted by gen-tpu-nodes.sh.
#
# `kubectl create`, not `apply`, and that is the whole trick: the apiserver
# honours .status on POST but drops it on apply, and the status carries both the
# node's capacity and its unique InternalIP. Creating the node whole leaves no
# window in which kwok's node-initialize stage can fill in the defaults —
# 1k CPU / 1Ti / no accelerators, and the real kind node's IP on all of them.
# Re-runs fall back to apply + a status patch, which is racy for a node that
# does not exist yet but correct for one that does.
#
# Usage:
#   apply-tpu-nodes.sh [--context CTX] [-- <gen-tpu-nodes.sh args>]
#   apply-tpu-nodes.sh --context kind-ome-kwok -- --cubes 4x4x4,2x2x2 --model demo-noop
#
#   --delete   remove every node this tool created (labelled kwok-sim/cube)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CTX="${KWOK_SIM_CONTEXT:-kind-ome-kwok}"
DELETE=0
GEN_ARGS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --context) CTX="$2"; shift 2 ;;
    --delete) DELETE=1; shift ;;
    --) shift; GEN_ARGS=("$@"); break ;;
    -h|--help) sed -n '2,17p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

k() { kubectl --context "$CTX" "$@"; }

if [ "$DELETE" -eq 1 ]; then
  k delete nodes -l kwok-sim/cube --wait=false
  exit 0
fi

# Fake addresses have to land in the kind bridge subnet, so take it from the
# real node rather than assuming one.
PREFIX="$(k get node -l node-role.kubernetes.io/control-plane \
  -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' | cut -d. -f1,2)"
[ -n "$PREFIX" ] || { echo "FATAL: could not read the control-plane InternalIP" >&2; exit 1; }

MANIFEST="$(mktemp -t kwok-tpu-nodes.XXXXXX)"
trap 'rm -f "$MANIFEST"' EXIT
"$HERE/gen-tpu-nodes.sh" --ip-prefix "$PREFIX" ${GEN_ARGS+"${GEN_ARGS[@]}"} > "$MANIFEST"

echo "--- create $(grep -c '^kind: Node' "$MANIFEST") kwok nodes (status included) ---"
if ! k create -f "$MANIFEST" 2>/dev/null; then
  echo "    some nodes already exist; converging spec, then re-patching status"
  # Server-side: these nodes were POSTed, not applied, so a client-side apply
  # would warn about the missing last-applied-configuration on every one.
  k apply --server-side --force-conflicts -f "$MANIFEST" >/dev/null
  # Node name and address are emitted as an adjacent pair per document, so one
  # pass over the manifest recovers the mapping without a YAML parser.
  awk '/^  name: /{n=$2} /type: InternalIP/{for(i=1;i<=NF;i++) if($i=="address:"){ip=$(i+1); gsub(/[,}]/,"",ip); print n"\t"ip; n=""}}' "$MANIFEST" \
  | while IFS=$'\t' read -r host ip; do
      k patch "node/${host}" --subresource=status --type=merge -p \
        "{\"status\":{\"addresses\":[{\"type\":\"InternalIP\",\"address\":\"${ip}\"},{\"type\":\"Hostname\",\"address\":\"${host}\"}]}}" >/dev/null
    done
fi

echo "--- wait for Ready ---"
k wait --for=condition=Ready nodes -l kwok-sim/cube --timeout=180s >/dev/null
echo "nodes:  $(k get nodes -l kwok-sim/cube --no-headers | wc -l | tr -d ' ')"
echo "chips:  $(k get nodes -l kwok-sim/cube -o json | jq '[.items[].status.capacity["google.com/tpu"]|tonumber]|add')"
echo "unique IPs: $(k get nodes -l kwok-sim/cube -o json | jq '[.items[].status.addresses[]|select(.type=="InternalIP").address]|unique|length')"
k get nodes -l kwok-sim/cube --no-headers \
  -L cloud.google.com/gke-tpu-topology,cloud.google.com/gke-nodepool \
  -o custom-columns=NAME:.metadata.name,TOPO:'.metadata.labels.cloud\.google\.com/gke-tpu-topology',POOL:'.metadata.labels.cloud\.google\.com/gke-nodepool' \
  | awk 'NR<=3; END{if (NR>3) print "  ... and " NR-3 " more"}'
