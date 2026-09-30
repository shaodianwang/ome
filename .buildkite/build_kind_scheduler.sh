#!/bin/sh
# Package the bundle's verified scheduler without a toolchain or registry pull.
set -eu

stage="${1:?extracted test bundle directory is required}"
manifest="$stage/manifest"
manifest_value() {
  sed -n "s/^$1=//p" "$manifest"
}
[ "$(manifest_value state)" = ready ] || { echo "scheduler bundle is not ready" >&2; exit 1; }
[ "$(manifest_value commit)" = "${BUILDKITE_COMMIT:?BUILDKITE_COMMIT is required}" ] || {
  echo "scheduler bundle commit mismatch" >&2; exit 1
}
[ "$(manifest_value goos)/$(manifest_value goarch)" = linux/amd64 ] || {
  echo "scheduler bundle platform mismatch" >&2; exit 1
}
digest="$(manifest_value scheduler_sha256)"
[ -n "$digest" ] || { echo "scheduler bundle has no checksum" >&2; exit 1; }
printf '%s  %s\n' "$digest" "$stage/scheduler/ome-scheduler" | sha256sum -c -
chmod 755 "$stage/scheduler/ome-scheduler"
ln -sf ome-scheduler "$stage/scheduler/entrypoint"
DOCKER_BUILDKIT=0 docker build --pull=false \
  -f dockerfiles/kind-prebuilt.Dockerfile \
  --build-arg BASE_IMAGE=scratch --build-arg RUN_USER=65532:65532 \
  -t ome-scheduler:kind-local "$stage/scheduler"
