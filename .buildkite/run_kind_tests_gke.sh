#!/bin/sh
# Named OFFLINE KIND integration test step for the BuildKite pipeline.
#
# Runs the omenative_coordination_kind suite on a GKE-backed agent. The GKE RC
# cluster has NO public egress from pods, so this script installs NOTHING at run
# time — KIND, kubectl, the Docker CLI, and every base image are baked into the
# workload image dockerfiles/kind-ci.Dockerfile. This script only:
#   1. confirms the privileged dind sidecar is reachable,
#   2. `docker load`s the baked image tarballs into dind (kind node + the
#      golang/distroless bases the fake-serving build needs),
#   3. loads the exact-commit test binary packaged by the unit-test job,
#   4. runs the semantic file group selected by OME_KIND_FOCUS.
#
# Requires the kind-ci workload image + a privileged docker:dind sidecar (see
# .buildkite/README-gke-kind.md). /bin/sh, no bashisms.
set -eu

IMAGES_DIR="${OME_KIND_IMAGES_DIR:-/images}"   # baked tarball dir in kind-ci
ARTIFACT_BUNDLE="${OME_KIND_TEST_BUNDLE:-/kind-test/omenative-kind-test.tgz}"

echo "--- named KIND group: ${OME_KIND_FOCUS:-all files} ---"

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
[ "$artifact_state" = ready ] || { echo "FATAL: test bundle is not ready" >&2; exit 1; }
[ "$artifact_commit" = "${BUILDKITE_COMMIT:?BUILDKITE_COMMIT is required}" ] || {
  echo "FATAL: test bundle commit $artifact_commit does not match $BUILDKITE_COMMIT" >&2
  exit 1
}
[ "$artifact_goos/$artifact_goarch" = linux/amd64 ] || {
  echo "FATAL: test bundle platform is $artifact_goos/$artifact_goarch, expected linux/amd64" >&2
  exit 1
}
kind_test_binary="$artifact_stage/omenative_coordination_kind.test"
[ -x "$kind_test_binary" ] || chmod +x "$kind_test_binary"
printf '%s  %s\n' "$artifact_sha256" "$kind_test_binary" | sha256sum -c -
mkdir -p tests/integration/dep-crds/keda tests/integration/dep-crds/scheduler-plugins
cp -R "$artifact_stage/dep-crds/keda/." tests/integration/dep-crds/keda/
cp -R "$artifact_stage/dep-crds/scheduler-plugins/." \
  tests/integration/dep-crds/scheduler-plugins/

echo "--- docker preflight: wait for the dind sidecar ---"
command -v docker >/dev/null 2>&1 || { echo "FATAL: docker CLI missing from kind-ci image." >&2; exit 1; }
# The dind sidecar starts in parallel with this container and takes a few
# seconds to accept connections, so poll rather than checking once.
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

echo "--- load baked images into dind (no registry pulls) ---"
for tar in "$IMAGES_DIR"/kindest-node.tar "$IMAGES_DIR"/fake-serving-build.tar \
           "$IMAGES_DIR"/fake-serving-base.tar "$IMAGES_DIR"/busybox.tar; do
  [ -f "$tar" ] || { echo "FATAL: baked image tarball missing: $tar" >&2; exit 1; }
  docker load -i "$tar"
done

# Raise the inotify ceilings on the Docker daemon's kernel before any cluster
# exists. Each shard runs a 3-node cluster and every nested kubelet/containerd
# holds watches and instances; at the distro defaults they exhaust silently —
# kubelet stops seeing filesystem events, pods wedge short of Ready, and specs
# fail as Eventually timeouts rather than a named error. Applied from a
# privileged throwaway container because this container is unprivileged; busybox
# is already loaded, so it costs no pull. Advisory: a daemon that refuses the
# write still runs the suite, just closer to the ceiling.
echo "--- raise inotify limits on the docker daemon ---"
docker run --rm --privileged busybox sh -c '
  sysctl -w fs.inotify.max_user_watches=1048576
  sysctl -w fs.inotify.max_user_instances=8192
' || echo "WARN: could not raise inotify limits; continuing at daemon defaults" >&2

# kind uses the loaded node image (no pull) because KIND_NODE_IMAGE names it and
# it is already present on the dind daemon.
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.32.2}"
export KIND_NODE_IMAGE
# Legacy builder (BUILDKIT=0) so the suite's `docker build` of fake-serving uses
# the loaded golang/distroless bases without attempting a registry pull.
export DOCKER_BUILDKIT=0

echo "--- kind version ---"
kind --version

# Delete leftover ome-* clusters from a prior job on this daemon.
kind get clusters 2>/dev/null | while read -r c; do
  case "$c" in
    ome-*) echo "--- deleting leftover kind cluster: $c ---"; kind delete cluster --name "$c" ;;
  esac
done

echo "--- run precompiled suite (${OME_KIND_FOCUS:-all files}) ---"
if [ -n "${OME_KIND_FOCUS:-}" ]; then
  "$kind_test_binary" -test.v -test.timeout=90m -ginkgo.v \
    "-ginkgo.focus-file=${OME_KIND_FOCUS}"
else
  "$kind_test_binary" -test.v -test.timeout=90m -ginkgo.v
fi

# Cluster teardown happens in the EXIT trap (cleanup_ome_clusters).
