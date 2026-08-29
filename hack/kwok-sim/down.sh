#!/usr/bin/env bash
# Delete the simulator cluster. kind owns it entirely, so this is the whole
# teardown — there is no state anywhere else to unwind.
#
# A script rather than a documented one-liner because the cluster is named by
# --cluster or OME_KWOK_CLUSTER, so the correct delete command is not a fixed
# string: anyone who brought up a second simulator under another name would
# otherwise be told to delete the first one.
#
# Reads the same default as up.sh, so `up.sh` then `down.sh` is always a pair.
set -uo pipefail

. "$(dirname "${BASH_SOURCE[0]}")/lib/kind.sh"
resolve_kind

CLUSTER="${OME_KWOK_CLUSTER:-ome-kwok}"

while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)
      [ $# -ge 2 ] || { echo "--cluster needs a value" >&2; exit 2; }
      CLUSTER="$2"; shift 2 ;;
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 1 ;;
  esac
done

"$KIND" delete cluster --name "$CLUSTER"
