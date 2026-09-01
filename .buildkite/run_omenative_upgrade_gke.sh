#!/bin/sh
# Run the operator-upgrade identity gate (omenative_upgrade_kind) on a
# GKE-backed KIND runner.
#
# Mirrors run_kind_tests_gke.sh's shape (exact-commit test bundle, dind
# preflight, baked-image loads, no live pulls) with one addition: the
# previous-main manager+agent image pair package_kind_test_image.sh resolved
# and baked at package time (see the comment there). Their presence under
# OME_KIND_IMAGES_DIR is the run/skip signal — no image pair baked there
# means either this is not a main build or the parent commit's images were
# never published, and the gate skips loudly without failing the step.
set -eu

IMAGES_DIR="${OME_KIND_IMAGES_DIR:-/images}"
ARTIFACT_BUNDLE="${OME_KIND_TEST_BUNDLE:-/kind-test/omenative-upgrade-kind-test.tgz}"

manager_marker="${IMAGES_DIR}/upgrade-from-image"
agent_marker="${IMAGES_DIR}/upgrade-from-agent-image"
if [ ! -s "$manager_marker" ] || [ ! -s "$agent_marker" ]; then
  echo "--- operator-upgrade identity gate: skipping ---"
  echo "no previous-main image pair was baked for this build (see package_kind_test_image.sh);" \
    "this is expected off main or when the parent commit's images were never published"
  exit 0
fi
upgrade_from_manager="$(cat "$manager_marker")"
upgrade_from_agent="$(cat "$agent_marker")"

cleanup_ome_clusters() {
  command -v kind >/dev/null 2>&1 || return 0
  kind get clusters 2>/dev/null | while read -r c; do
    case "$c" in
      ome-*) kind delete cluster --name "$c" >/dev/null 2>&1 || true ;;
    esac
  done || true
}

artifact_stage="$(mktemp -d)"
cleanup() {
  cleanup_ome_clusters
  rm -rf -- "$artifact_stage"
}
trap cleanup EXIT

echo "--- load exact-commit KIND test bundle ---"
[ -s "$ARTIFACT_BUNDLE" ] || {
  echo "FATAL: runner image has no test bundle at $ARTIFACT_BUNDLE" >&2
  exit 1
}
tar -xzf "$ARTIFACT_BUNDLE" -C "$artifact_stage"

manifest="$artifact_stage/manifest"
[ -s "$manifest" ] || { echo "FATAL: test bundle has no manifest" >&2; exit 1; }
manifest_value() {
  sed -n "s/^$1=//p" "$manifest"
}
artifact_state="$(manifest_value state)"
artifact_commit="$(manifest_value commit)"
artifact_goos="$(manifest_value goos)"
artifact_goarch="$(manifest_value goarch)"
artifact_sha256="$(manifest_value sha256)"
artifact_binary="$(manifest_value binary)"
[ -n "$artifact_binary" ] || artifact_binary="omenative_upgrade_kind.test"
[ "$artifact_state" = ready ] || { echo "FATAL: test bundle is not ready" >&2; exit 1; }
[ "$artifact_commit" = "${BUILDKITE_COMMIT:?BUILDKITE_COMMIT is required}" ] || {
  echo "FATAL: test bundle commit $artifact_commit does not match $BUILDKITE_COMMIT" >&2
  exit 1
}
[ "$artifact_goos/$artifact_goarch" = linux/amd64 ] || {
  echo "FATAL: test bundle platform is $artifact_goos/$artifact_goarch, expected linux/amd64" >&2
  exit 1
}
kind_test_binary="$artifact_stage/$artifact_binary"
[ -x "$kind_test_binary" ] || chmod +x "$kind_test_binary"
printf '%s  %s\n' "$artifact_sha256" "$kind_test_binary" | sha256sum -c -
for dep in "$artifact_stage"/dep-crds/*/; do
  [ -d "$dep" ] || continue
  dest="tests/integration/dep-crds/$(basename "$dep")"
  mkdir -p "$dest"
  cp -R "$dep." "$dest/"
done

echo "--- docker preflight: wait for the dind sidecar ---"
command -v docker >/dev/null 2>&1 || { echo "FATAL: docker CLI missing from kind-ci image." >&2; exit 1; }
i=0
until docker info >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -gt 60 ]; then
    echo "FATAL: no reachable Docker daemon after ~120s. Expect a privileged" >&2
    echo "  docker:dind sidecar at DOCKER_HOST=${DOCKER_HOST:-<unset>}." >&2
    exit 1
  fi
  sleep 2
done
docker version --format 'client {{.Client.Version}} / server {{.Server.Version}}'

# kindest-node backs `kind create cluster`; busybox backs the inotify step
# below. Every other image deploy-ome-kind.sh's offline path needs (the
# golang:1.25 compile base, cert-manager, eval-nginx) is loaded by that
# script itself from the same OME_KIND_IMAGES_DIR — it already knows how to
# fall back to the baked tarball, so duplicating that here would only be
# two places to keep in sync.
echo "--- load baked images into dind (no registry pulls) ---"
for tar in "$IMAGES_DIR"/kindest-node.tar "$IMAGES_DIR"/busybox.tar; do
  [ -f "$tar" ] || { echo "FATAL: baked image tarball missing: $tar" >&2; exit 1; }
  docker load -i "$tar"
done

echo "--- load previous-version images for the upgrade-identity gate ---"
echo "    manager: ${upgrade_from_manager}"
echo "    agent:   ${upgrade_from_agent}"
docker load -i "${IMAGES_DIR}/upgrade-from-manager.tar"
docker load -i "${IMAGES_DIR}/upgrade-from-agent.tar"
docker image inspect "$upgrade_from_manager" >/dev/null
docker image inspect "$upgrade_from_agent" >/dev/null

echo "--- raise inotify limits on the docker daemon ---"
docker run --rm --privileged busybox sh -c '
  sysctl -w fs.inotify.max_user_watches=1048576
  sysctl -w fs.inotify.max_user_instances=8192
' || echo "WARN: could not raise inotify limits; continuing at daemon defaults" >&2

KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-$(tr -d '[:space:]' <.kind-node-image | cut -d@ -f1)}"
export KIND_NODE_IMAGE
export DOCKER_BUILDKIT=0
export OME_UPGRADE_FROM_IMAGE="$upgrade_from_manager"
export OME_UPGRADE_FROM_AGENT_IMAGE="$upgrade_from_agent"
export OME_KIND_OFFLINE=true

echo "--- kind version ---"
kind --version
cleanup_ome_clusters

echo "--- run precompiled suite $artifact_binary ---"
kind_test_timeout="${OME_KIND_TIMEOUT:-60m}"
"$kind_test_binary" -test.v "-test.timeout=$kind_test_timeout" -ginkgo.v
