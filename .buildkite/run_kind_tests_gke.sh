#!/bin/sh
# GKE-sharded, OFFLINE KIND integration test step for the BuildKite pipeline.
#
# Runs the omenative_coordination_kind suite on a GKE-backed agent. The GKE RC
# cluster has NO public egress from pods, so this script installs NOTHING at run
# time — the whole toolchain (Go + module cache, Rust + cargo cache, kind,
# kubectl, docker CLI, crane) and every base image are baked into the workload
# image dockerfiles/kind-ci.Dockerfile. This script only:
#   1. confirms the privileged dind sidecar is reachable,
#   2. `docker load`s the baked image tarballs into dind (kind node + the
#      golang/distroless bases the fake-serving build needs),
#   3. builds the xet staticlib offline (cargo cache),
#   4. shards the spec files and runs `go test` offline (baked module cache).
#
# Buildkite injects, per parallel job:
#   BUILDKITE_PARALLEL_JOB        0-based index of this job (0..N-1)
#   BUILDKITE_PARALLEL_JOB_COUNT  total number of parallel jobs (N)
# Both default to a single-shard run when unset.
#
# Requires the kind-ci workload image + a privileged docker:dind sidecar (see
# .buildkite/README-gke-kind.md). /bin/sh, no bashisms.
set -eu

SUITE_DIR="tests/integration/singlecluster/controller/omenative_coordination_kind"
IMAGES_DIR="${OME_KIND_IMAGES_DIR:-/images}"   # baked tarball dir in kind-ci

N="${BUILDKITE_PARALLEL_JOB_COUNT:-1}"
J="${BUILDKITE_PARALLEL_JOB:-0}"
echo "--- shard ${J} of ${N} (offline) ---"

# Baked cache + offline knobs (belt-and-suspenders; the kind-ci image also sets
# GOMODCACHE/CARGO_HOME). GOPROXY=off + CARGO_NET_OFFLINE fail FAST if something
# wasn't baked, instead of hanging on an unreachable registry.
export GOMODCACHE="${GOMODCACHE:-/go/pkg/mod}"
export GOFLAGS="${GOFLAGS:--mod=mod}"
export GOPROXY=off
export CARGO_NET_OFFLINE=true

# Module-cache preflight. The kind-ci image bakes its module cache from the
# go.mod/go.sum present when the IMAGE was built, so a commit that changes
# dependencies needs a rebuilt image. With GOPROXY=off the mismatch otherwise
# surfaces as "module lookup disabled by GOPROXY=off" from deep inside a package
# build, minutes in and after a kind cluster has been booted. Check it here, in
# seconds, and say what is actually wrong.
#
# Publishing kind-ci is main-only, so a PR that changes go.mod/go.sum cannot
# have a matching image until it merges; that case is reported as a skip rather
# than a failure, because no change to the branch can satisfy it.
echo "--- preflight: baked module cache covers this commit's go.sum ---"
if ! missing="$(go mod download all 2>&1)"; then
  echo "$missing" >&2
  echo "" >&2
  echo "The kind-ci image's baked module cache does not cover this commit's" >&2
  echo "dependencies. Rebuild + mirror kind-ci from a commit whose go.mod/go.sum" >&2
  echo "match this one, then re-run." >&2
  case "${BUILDKITE_BRANCH:-}" in
    main) exit 1 ;;
    *)
      echo "" >&2
      echo "SKIP: kind-ci publishes on main only, so this branch cannot have a" >&2
      echo "  matching image yet. Coverage returns once the dependency change" >&2
      echo "  merges and kind-ci is rebuilt." >&2
      exit 0
      ;;
  esac
fi

cleanup_ome_clusters() {
  command -v kind >/dev/null 2>&1 || return 0
  kind get clusters 2>/dev/null | while read -r c; do
    case "$c" in
      ome-*) kind delete cluster --name "$c" >/dev/null 2>&1 || true ;;
    esac
  done || true
}
trap cleanup_ome_clusters EXIT

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

echo "--- kind / go / cargo versions ---"
kind --version; go version; cargo --version

echo "--- build xet staticlib (offline, cargo cache) ---"
make xet-build
export LD_LIBRARY_PATH="$(pwd)/pkg/xet/target/release"

# Delete leftover ome-* clusters from a prior job on this daemon.
kind get clusters 2>/dev/null | while read -r c; do
  case "$c" in
    ome-*) echo "--- deleting leftover kind cluster: $c ---"; kind delete cluster --name "$c" ;;
  esac
done

# Vendor the optional CRD YAMLs the suite installs (KEDA / Prometheus). Runs off
# the baked module cache (GOPROXY=off).
make dep-crds

# ---------------------------------------------------------------------
# Compute this shard's spec-file subset. Partition the spec files (those with
# ginkgo.Describe / DescribeTable) round-robin by sorted index: file k -> shard
# (k mod N). focus-file is a REGEX; anchor with a leading `/` and trailing `$`
# (dot escaped) so each pattern matches exactly one file — otherwise
# `inplace_kind_test.go` would also match `multipod_inplace_kind_test.go`.
# ---------------------------------------------------------------------
# OME_KIND_FOCUS: run an explicit -ginkgo.focus-file value instead of the shard
# partition. For by-hand debugging of a single spec (e.g. the first
# offline-shard proof); unset in normal sharded CI runs.
if [ -n "${OME_KIND_FOCUS:-}" ]; then
  FOCUS_ARGS="-ginkgo.focus-file=${OME_KIND_FOCUS}"
  SHARD_FILES=" (explicit focus: ${OME_KIND_FOCUS})"
  echo "--- OME_KIND_FOCUS set; skipping shard partition ---"
else
  i=0
  FOCUS_ARGS=""
  SHARD_FILES=""
  for f in $(grep -lE 'ginkgo\.(Describe|DescribeTable)\(' "$SUITE_DIR"/*_kind_test.go | sort); do
    if [ "$((i % N))" -eq "$J" ]; then
      base=$(basename "$f")
      esc=$(printf '%s' "$base" | sed 's/\./\\./g')
      FOCUS_ARGS="$FOCUS_ARGS -ginkgo.focus-file=/${esc}\$"
      SHARD_FILES="$SHARD_FILES $base"
    fi
    i=$((i + 1))
  done

  if [ -z "$FOCUS_ARGS" ]; then
    echo "--- shard ${J}/${N} has no spec files assigned; nothing to run ---"
    exit 0
  fi
fi

echo "--- shard ${J}/${N} spec files:${SHARD_FILES} ---"
echo "--- go test (shard ${J}/${N}, offline) ---"
# shellcheck disable=SC2086  # FOCUS_ARGS is a deliberate list of flags.
go test "./${SUITE_DIR}/..." \
  -v -timeout 90m -ginkgo.v $FOCUS_ARGS

# Cluster teardown happens in the EXIT trap (cleanup_ome_clusters).
