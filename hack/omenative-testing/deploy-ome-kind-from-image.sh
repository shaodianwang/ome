#!/usr/bin/env bash
# Install OME into a KIND cluster from a PREVIOUSLY PUBLISHED manager+agent
# image pair, instead of building from the working tree.
#
# This is the "before" half of the operator-upgrade identity gate
# (tests/integration/singlecluster/e2e/omenative_upgrade_kind): the gate
# installs OME at an old commit's image via this script, creates fixtures,
# then re-runs deploy-ome-kind.sh in its normal (build-from-worktree) mode to
# land the CURRENT commit's image as the "after" — a second `helm upgrade
# --install` against the same release, so that second pass is the actual
# upgrade under test.
#
# Every install step past image resolution (cert-manager, CRDs, chart
# install, fixture seeding) is deploy-ome-kind.sh's own --skip-build path;
# this script's only job is turning a registry ref into the local tag that
# path expects.
#
# Usage:
#   hack/omenative-testing/deploy-ome-kind-from-image.sh [--cluster NAME]
#
# Environment:
#   OME_UPGRADE_FROM_IMAGE        full pullable ome-manager image ref (required)
#   OME_UPGRADE_FROM_AGENT_IMAGE  full pullable ome-agent image ref (default:
#                                 OME_UPGRADE_FROM_IMAGE with its "ome-manager"
#                                 path segment replaced by "ome-agent" — this
#                                 repo publishes both images from the same
#                                 commit under the same registry and tag; see
#                                 the IMAGES list in .buildkite/generate_pipeline.py)
#   every other deploy-ome-kind.sh variable except OME_IMAGE_TAG, which this
#   script sets itself, and SKIP_BUILD, which this script forces on.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

: "${OME_UPGRADE_FROM_IMAGE:?OME_UPGRADE_FROM_IMAGE is required}"
OME_UPGRADE_FROM_AGENT_IMAGE="${OME_UPGRADE_FROM_AGENT_IMAGE:-${OME_UPGRADE_FROM_IMAGE/ome-manager/ome-agent}}"
if [ "$OME_UPGRADE_FROM_AGENT_IMAGE" = "$OME_UPGRADE_FROM_IMAGE" ]; then
  echo "FATAL: could not derive an ome-agent ref from OME_UPGRADE_FROM_IMAGE=${OME_UPGRADE_FROM_IMAGE}" \
    "(no 'ome-manager' path segment to replace); set OME_UPGRADE_FROM_AGENT_IMAGE explicitly" >&2
  exit 1
fi

command -v docker >/dev/null 2>&1 || { echo "FATAL: docker not on PATH" >&2; exit 1; }

# A fixed local tag: deploy-ome-kind.sh --skip-build only cares that
# ome-manager:$OME_IMAGE_TAG / ome-agent:$OME_IMAGE_TAG already exist.
#
# Pull only when the ref is not already loaded: a restricted-egress runner
# (the GKE KIND lane) has already `docker load`ed these from a baked
# tarball under this exact ref, and cannot reach the registry itself — an
# unconditional pull would hang or fail there. A developer running this
# locally simply has not loaded it yet, so the inspect fails and the pull
# proceeds normally.
LOCAL_TAG="upgrade-from"
echo "--- resolve previous-version images ---"
echo "    manager: ${OME_UPGRADE_FROM_IMAGE}"
echo "    agent:   ${OME_UPGRADE_FROM_AGENT_IMAGE}"
docker image inspect "$OME_UPGRADE_FROM_IMAGE" >/dev/null 2>&1 || docker pull "$OME_UPGRADE_FROM_IMAGE"
docker image inspect "$OME_UPGRADE_FROM_AGENT_IMAGE" >/dev/null 2>&1 || docker pull "$OME_UPGRADE_FROM_AGENT_IMAGE"
docker tag "$OME_UPGRADE_FROM_IMAGE" "ome-manager:${LOCAL_TAG}"
docker tag "$OME_UPGRADE_FROM_AGENT_IMAGE" "ome-agent:${LOCAL_TAG}"

echo "--- install the previous version (deploy-ome-kind.sh --skip-build) ---"
exec env OME_IMAGE_TAG="$LOCAL_TAG" \
  "$(dirname "${BASH_SOURCE[0]}")/deploy-ome-kind.sh" --skip-build "$@"
