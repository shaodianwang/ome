#!/bin/sh
# Named OFFLINE KIND integration test step for the BuildKite pipeline.
#
# Runs one precompiled KIND suite on a GKE-backed agent. Which suite is decided
# by OME_KIND_TEST_BUNDLE (default: the omenative_coordination_kind bundle); the
# bundle's manifest names the binary inside it. The GKE RC
# cluster has NO public egress from pods, so this script installs NOTHING at run
# time — KIND, kubectl, the Docker CLI, and every base image are baked into the
# workload image dockerfiles/kind-ci.Dockerfile. This script only:
#   1. confirms the privileged dind sidecar is reachable,
#   2. `docker load`s the image tarballs under /images into dind (kind node, the
#      golang/distroless bases the fake-serving build needs, Kueue, which the
#      multi-cluster suite installs into its members, and the alfred image the
#      alfred lane deploys, packaged only when that lane is on),
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
# Bundles built before the manifest carried `binary=` shipped the coordination
# suite, so fall back to that name rather than failing on an older artifact.
artifact_binary="$(manifest_value binary)"
[ -n "$artifact_binary" ] || artifact_binary="omenative_coordination_kind.test"
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
# Restore whatever vendored dep-crds the bundle carries. Enumerated from the
# bundle rather than named here so a suite that needs a new one is additive.
for dep in "$artifact_stage"/dep-crds/*/; do
  [ -d "$dep" ] || continue
  dest="tests/integration/dep-crds/$(basename "$dep")"
  mkdir -p "$dest"
  cp -R "$dep." "$dest/"
done

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
    # A sidecar that never came up is agent trouble, not a result: leave with
    # the exit code the group's infra-retry preset reruns.
    exit 125
  fi
  sleep 2
done
docker version --format 'client {{.Client.Version}} / server {{.Server.Version}}'

echo "--- load baked images into dind (no registry pulls) ---"
for tar in "$IMAGES_DIR"/kindest-node.tar "$IMAGES_DIR"/fake-serving-build.tar \
           "$IMAGES_DIR"/fake-serving-base.tar "$IMAGES_DIR"/busybox.tar \
           "$IMAGES_DIR"/kueue.tar; do
  [ -f "$tar" ] || { echo "FATAL: baked image tarball missing: $tar" >&2; exit 1; }
  docker load -i "$tar"
done

# The alfred lane's image and its reference. Other lanes never look for it.
if [ -f "$IMAGES_DIR/alfred.tar" ]; then
  docker load -i "$IMAGES_DIR/alfred.tar"
  OME_ALFRED_IMAGE="$(tr -d '[:space:]' <"$IMAGES_DIR/alfred-image")"
  export OME_ALFRED_IMAGE
  echo "--- alfred image: $OME_ALFRED_IMAGE"
fi
# The alfred bundle cannot build its image here (no egress); refuse to start
# without the preloaded one rather than fail minutes later inside the suite.
case "$ARTIFACT_BUNDLE" in
  */alfred-kind-test.tgz)
    [ -f "$IMAGES_DIR/alfred.tar" ] || { echo "alfred bundle selected but $IMAGES_DIR/alfred.tar is missing" >&2; exit 1; }
    ;;
esac

# Raise the inotify ceilings on the Docker daemon's kernel before any cluster
# exists. Each shard runs a 3-node cluster and every nested kubelet/containerd
# holds watches and instances; at the distro defaults they exhaust silently —
# kubelet stops seeing filesystem events, pods wedge short of Ready, and specs
# fail as Eventually timeouts rather than a named error. Applied from a
# privileged throwaway container because this container is unprivileged; busybox
# is already loaded, so it costs no pull. Advisory: a daemon that refuses the
# write still runs the suite, just closer to the ceiling.
# The scheduler dimension: package_kind_test_image.sh bakes this commit's
# ome-scheduler image on main; the suite loads it into its cluster under the
# tag OME_TEST_SCHEDULER_IMAGE names and installs the chart when
# OME_TEST_SCHEDULER is set. Without the archive a scheduler run cannot start.
if [ -f "$IMAGES_DIR/ome-scheduler.tar" ]; then
  docker load -i "$IMAGES_DIR/ome-scheduler.tar"
  scheduler_ref="${PROD_ECR_REGISTRY:-168653207203.dkr.ecr.us-east-2.amazonaws.com}/msl_infra/ome-scheduler:$(printf '%.8s' "$BUILDKITE_COMMIT")"
  docker image inspect "$scheduler_ref" >/dev/null
  docker tag "$scheduler_ref" ome-scheduler:kind-local
  export OME_TEST_SCHEDULER_IMAGE=ome-scheduler:kind-local
elif [ -n "${OME_TEST_SCHEDULER:-}" ]; then
  echo "FATAL: OME_TEST_SCHEDULER=${OME_TEST_SCHEDULER} but ${IMAGES_DIR}/ome-scheduler.tar is not baked (main-only artifact)" >&2
  exit 1
fi
echo "--- raise inotify limits on the docker daemon ---"
docker run --rm --privileged busybox sh -c '
  sysctl -w fs.inotify.max_user_watches=1048576
  sysctl -w fs.inotify.max_user_instances=8192
' || echo "WARN: could not raise inotify limits; continuing at daemon defaults" >&2

# kind uses the loaded node image (no pull) because KIND_NODE_IMAGE names it and
# it is already present on the dind daemon. Tag-only: the tarball loads under
# its RepoTag, and a digest ref would send kind to a registry it cannot reach.
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-$(tr -d '[:space:]' <.kind-node-image | cut -d@ -f1)}"
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

echo "--- run precompiled suite $artifact_binary (${OME_KIND_FOCUS:-all files}) ---"
kind_test_timeout="${OME_KIND_TIMEOUT:-90m}"
suite_log="$artifact_stage/suite.log"
suite_status_file="$artifact_stage/suite.status"
# tee so the run stays live in the job log while also being greppable below;
# the status goes through a file because /bin/sh has no PIPESTATUS.
set +e
{
  if [ -n "${OME_KIND_FOCUS:-}" ]; then
    "$kind_test_binary" -test.v "-test.timeout=$kind_test_timeout" -ginkgo.v \
      "-ginkgo.focus-file=${OME_KIND_FOCUS}"
  else
    "$kind_test_binary" -test.v "-test.timeout=$kind_test_timeout" -ginkgo.v
  fi
  echo "$?" >"$suite_status_file"
} 2>&1 | tee "$suite_log"
set -e
suite_status="$(cat "$suite_status_file" 2>/dev/null || echo 1)"

# A `kind create cluster` that dies of agent starvation is not a test result,
# and neither is the scheduler install when the just-created apiserver times
# out its first calls. Both fail in BeforeSuite, before a single It runs, and
# the binary exits 1 — indistinguishable from a real spec failure to Buildkite,
# so the KIND group's infra-retry preset (exit 125 / agent_stop, deliberately
# never exit 1) cannot see them. Translate exactly those shapes to 125 and
# leave every other exit code alone: a suite that reached even one spec keeps
# its own status, so "never retry a red spec" still holds.
if [ "$suite_status" -ne 0 ] \
   && ! grep -qE '^Ran [1-9][0-9]* of ' "$suite_log" \
   && { { grep -q 'EnsureCluster(' "$suite_log" && grep -q 'createCluster:' "$suite_log"; } \
        || grep -q 'RunKindSuite: installScheduler:' "$suite_log"; }; then
  echo "--- cluster bring-up failed in BeforeSuite with no spec run; exiting 125 for infra retry ---" >&2
  exit 125
fi

# Cluster teardown happens in the EXIT trap (cleanup_ome_clusters).
exit "$suite_status"
