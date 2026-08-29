#!/usr/bin/env bash
# Delete the three fleet clusters. kind owns them entirely, so this is the whole
# teardown — there is no state anywhere else to unwind.
set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib/kind.sh"
resolve_kind

"$KIND" delete clusters \
  "${OME_FLEET_HUB:-ome-quota-hub}" \
  "${OME_FLEET_MEMBER_A:-ome-quota-a}" \
  "${OME_FLEET_MEMBER_B:-ome-quota-b}"
