#!/usr/bin/env bash
# Assert an ISVC's gang instances are spread across fault domains.
#
# For every engine instance of the ISVC:
#   * all its pods share one co-location domain (any gke-tpu-partition-*-id
#     label) — the gang invariant;
#   * placed instances occupy --distinct different fault domains
#     (cloud.google.com/gce-topology-subblock by default);
#   * exactly --pending instances are fully unplaced (Required
#     over-subscription holds them Pending by design; default 0).
#
# Usage:
#   verify-spread.sh <isvc> [--ns kwok-sim] [--distinct N] [--pending N]
#                    [--spread-key LABEL] [--context CTX]
set -euo pipefail

ISVC="${1:?usage: verify-spread.sh <isvc> [--ns NS] [--distinct N] [--pending N]}"
shift
NS="kwok-sim"
DISTINCT=2
PENDING=0
SPREAD_KEY="cloud.google.com/gce-topology-subblock"
CTX_ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --ns) NS="$2"; shift 2 ;;
    --distinct) DISTINCT="$2"; shift 2 ;;
    --pending) PENDING="$2"; shift 2 ;;
    --spread-key) SPREAD_KEY="$2"; shift 2 ;;
    --context) CTX_ARGS=(--context "$2"); shift 2 ;;
    *) echo "unknown flag $1" >&2; exit 2 ;;
  esac
done

kc() { kubectl ${CTX_ARGS+"${CTX_ARGS[@]}"} "$@"; }

PODS_JSON="$(kc -n "$NS" get pods \
  -l "ome.io/inferenceservice=${ISVC},component=engine" -o json)"
if [ "$(jq '.items | length' <<<"$PODS_JSON")" -eq 0 ]; then
  echo "verify-spread: no engine pods for ${ISVC} in ${NS}" >&2
  exit 1
fi
NODES_JSON="$(kc get nodes -o json)"

node_partition() {
  jq -r --arg n "$1" \
    '.items[] | select(.metadata.name==$n) | .metadata.labels
     | to_entries[] | select(.key | test("gke-tpu-partition-.*-id$")) | .value' \
    <<<"$NODES_JSON" | head -1
}
node_spread_domain() {
  jq -r --arg n "$1" --arg k "$SPREAD_KEY" \
    '.items[] | select(.metadata.name==$n) | .metadata.labels[$k] // ""' \
    <<<"$NODES_JSON"
}

fail=0
pending=0
domains_file="$(mktemp)"
trap 'rm -f "$domains_file"' EXIT

for idx in $(jq -r '[.items[].metadata.labels["ome.io/instance-index"]] | unique[]' <<<"$PODS_JSON"); do
  placed=0; unplaced=0; partition=""; sdom=""
  for n in $(jq -r --arg i "$idx" \
      '.items[] | select(.metadata.labels["ome.io/instance-index"]==$i) | .spec.nodeName // "<none>"' \
      <<<"$PODS_JSON"); do
    if [ "$n" = "<none>" ]; then unplaced=$((unplaced+1)); continue; fi
    placed=$((placed+1))
    p="$(node_partition "$n")"; d="$(node_spread_domain "$n")"
    if [ -z "$partition" ]; then partition="$p"; sdom="$d"; fi
    if [ "$p" != "$partition" ]; then
      echo "✗ instance $idx SPLIT across partitions ($partition vs $p)"; fail=1
    fi
  done
  if [ "$placed" -eq 0 ]; then
    pending=$((pending+1))
    echo "  instance $idx: Pending (no pod placed)"
    continue
  fi
  if [ "$unplaced" -gt 0 ]; then
    echo "✗ instance $idx partially placed ($placed placed, $unplaced pending)"; fail=1
  fi
  echo "  instance $idx: partition=$partition ${SPREAD_KEY##*/}=$sdom"
  echo "$sdom" >> "$domains_file"
done

distinct="$(sort -u "$domains_file" | grep -c . || true)"
if [ "$distinct" -ne "$DISTINCT" ]; then
  echo "✗ placed instances span $distinct fault domain(s), want $DISTINCT"; fail=1
else
  echo "✓ placed instances span $DISTINCT distinct fault domain(s)"
fi
if [ "$pending" -ne "$PENDING" ]; then
  echo "✗ $pending instance(s) Pending, want $PENDING"; fail=1
else
  echo "✓ $pending instance(s) Pending (expected)"
fi
exit "$fail"
