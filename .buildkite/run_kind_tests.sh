#!/bin/sh
# KIND-backed integration test step for the BuildKite pipeline.
#
# Runs the KIND-backed suites under tests/integration/singlecluster/
# controller/omenative_coordination_kind/. Each suite spins up a TWO-node KIND
# cluster (1 control-plane + 1 worker — migration/interleave specs need
# a second node to schedule the off-source surge pod), builds the
# fake-serving image, kind-loads it, starts an in-process
# controller-runtime manager pointed at the kind apiserver, and runs
# Ginkgo specs against real kubelet + scheduler + containerd.
#
# Coverage: ~40 spec files / 80+ active specs covering the OEP-0007.1 +
# OEP-0007.2 + OEP-0011 contracts the envtest sibling cannot exercise
# under its instant-Ready PodKubeletSimulator (pod-lifecycle timing,
# drain races, per-revision Service convergence, in-place image
# patches, RatioBalanced gate at scale, mid-rollout pause, Sequential
# active-Component handoff, partial-failure recovery, bad-image
# escalation). A handful of BlueGreen surge / HTTPRoute-cutover specs
# are still parked (ginkgo.PIt) — see the suite README spec map.
#
# Infrastructure requirements (Docker on the agent host):
#   - kind (installed by this script via `go install`)
#   - docker: the pipeline mounts the TW agent dind daemon's socket
#     into this step's container (see the kind_plugin block in
#     generate_pipeline.py) and this script installs the docker CLI.
#     kind nodes run as sibling containers on that daemon. The TW
#     docker wrapper injects --network host into workload containers,
#     so the apiserver port kind publishes on the daemon's loopback is
#     reachable from the test process.
#   - ~6 GB memory for the kind node container
#   - ~2 GB disk for the kind node image + fake-serving build
#
# Same env-setup pattern as run_tests.sh + run_integration_tests.sh
# (libxet link deps, IPv6 advertise-address shim NOT needed because
# kind owns its own apiserver). Mirrors the existing scripts'
# /bin/sh-with-no-bashisms convention because the docker plugin runs
# the command block under /bin/sh.
set -eu

# Delete this repo's kind clusters on ANY exit — success, test failure,
# or an early FATAL — so the agent's dind isn't left holding ~6 GB of
# cluster state. A plain tail-of-script sweep never runs when set -e
# aborts on a failing `go test`. Scoped to ome-* (every kindutil suite
# name is ome-prefixed) so a daemon shared with other jobs never loses
# clusters we don't own. No-op before the kind binary is installed.
cleanup_ome_clusters() {
  command -v kind >/dev/null 2>&1 || return 0
  kind get clusters 2>/dev/null | while read -r c; do
    case "$c" in
      ome-*) kind delete cluster --name "$c" >/dev/null 2>&1 || true ;;
    esac
  done || true
}
trap cleanup_ome_clusters EXIT

apt-get update
# docker.io provides the docker CLI that kind, `docker build`, and
# `kind load` shell out to; it talks to the TW agent's dind daemon
# through a socket the pipeline mounts into this container. The
# package's daemon half is inert here (no systemd in the container).
# Installed on its own, before the heavy toolchain packages, so the
# docker preflight below can fail the step in the first minute.
apt-get install -y --no-install-recommends docker.io

echo "--- docker preflight ---"
command -v docker >/dev/null 2>&1 || {
  echo "FATAL: docker CLI missing after docker.io install." >&2
  exit 1
}
if ! docker info >/dev/null 2>&1; then
  echo "FATAL: no reachable Docker daemon from this Buildkite job." >&2
  echo "  DOCKER_HOST=${DOCKER_HOST:-<unset>}" >&2
  echo "  Check the /var/run/docker.sock mount the kind_plugin block in" >&2
  echo "  generate_pipeline.py adds to this step." >&2
  exit 1
fi
echo "--- docker info (truncated) ---"
docker info 2>&1 | head -30

# Pre-create kind's docker network WITHOUT IPv6. kind's own ensureNetwork
# creates it dual-stack (--ipv6 --subnet fc00:...), which dies on this
# dind: the daemon cannot write /proc/sys/net/ipv6/conf/default/forwarding
# ("operation not permitted" — /proc/sys is read-only in the dind task).
# kind short-circuits and uses a pre-existing network with its fixed name
# as-is, and the suite's clusters are ipFamily=ipv4, which doesn't need a
# v6-enabled docker network. Mirror kind's own create flags (masquerade,
# default-network MTU) minus the v6 half. This doubles as the bridge
# capability probe: if even an IPv4 bridge can't be created or attached
# to, kind cannot run on this queue — fail in the first minute, before
# the toolchain bootstrap.
if ! docker network inspect kind >/dev/null 2>&1; then
  mtu=$(docker network inspect bridge --format '{{ index .Options "com.docker.network.driver.mtu" }}' 2>/dev/null || echo "")
  if [ -n "$mtu" ] && [ "$mtu" != "0" ]; then
    docker network create -d bridge \
      -o com.docker.network.bridge.enable_ip_masquerade=true \
      -o "com.docker.network.driver.mtu=$mtu" kind
  else
    docker network create -d bridge \
      -o com.docker.network.bridge.enable_ip_masquerade=true kind
  fi || {
    echo "FATAL: cannot create an IPv4 bridge network on this agent's dind" >&2
    echo "  daemon. kind requires bridge networking for its node containers;" >&2
    echo "  this queue cannot run the KIND suite until the dind allows it." >&2
    exit 1
  }
fi
# Attach probe on the kind network, using an image already present on
# the daemon (the image running this very container) so the probe does
# not depend on a registry pull. Bridge attach (veth + iptables) is
# where a NAT-incapable daemon actually falls over, so test it, not
# just network create.
probe_img=$(docker image ls --format '{{.Repository}}:{{.Tag}}' 2>/dev/null | grep -v '<none>' | grep golang | head -1)
[ -n "$probe_img" ] || probe_img=$(docker image ls --format '{{.Repository}}:{{.Tag}}' 2>/dev/null | grep -v '<none>' | head -1)
if [ -n "$probe_img" ]; then
  docker run --rm --network kind --entrypoint /bin/sh "$probe_img" -c 'echo bridge-attach-ok' || {
    echo "FATAL: attaching a container to the kind bridge network failed on" >&2
    echo "  this agent's dind daemon. kind node containers cannot run here." >&2
    exit 1
  }
else
  echo "WARN: no local image available for the bridge-attach probe; skipping." >&2
fi
echo "--- docker preflight OK ---"

# The TW dind runs the containerd image store (nydus snapshotter), which
# the legacy docker builder can't use; build fake-serving with BuildKit.
# kindutil's dockerBuild defaults to the legacy builder only when
# DOCKER_BUILDKIT is unset (for docker installs without buildx).
DOCKER_BUILDKIT=1
export DOCKER_BUILDKIT

apt-get install -y --no-install-recommends jq libssl-dev pkg-config build-essential curl ca-certificates

# Rust toolchain. The Makefile's `make test-no-xet` path doesn't
# require it, but the kind suites import the same packages cmd/manager
# does, and those transitively link the xet Rust staticlib.
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
sh /tmp/rustup.sh -y --default-toolchain stable --profile minimal

export PATH="$HOME/.cargo/bin:/usr/local/go/bin:/go/bin:$PATH"
make xet-build
export LD_LIBRARY_PATH="$(pwd)/pkg/xet/target/release"

# kind binary, installed from the Go module proxy. Pin to a known-good
# release and bump deliberately — each bump can shift the kubelet /
# containerd versions inside the node container, and the default in
# KIND_NODE_IMAGE below must move in lockstep with the pinned kind
# release's bundled default.
KIND_VERSION="${KIND_VERSION:-v0.27.0}"
go install sigs.k8s.io/kind@${KIND_VERSION}
echo "--- kind --version ---"
kind --version

# Pre-pull the kind node image so `kind create cluster --wait` doesn't
# burn its timeout on a ~900MB pull on a cold host. The digest-pinned
# default MUST equal the pinned kind release's bundled default (see
# pkg/apis/config/defaults/image.go in sigs.k8s.io/kind@$KIND_VERSION)
# because that's what an unset KIND_NODE_IMAGE boots. When the env var
# IS set (e.g. to point at a registry mirror), kindutil passes it to
# `kind create cluster --image`, so the pre-pull and the cluster agree
# either way — export it so the go-test process sees the same value.
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.32.2@sha256:f226345927d7e348497136874b6d207e0b32cc52154ad8323129352923a3142f}"
export KIND_NODE_IMAGE
docker pull -q "$KIND_NODE_IMAGE" || {
  echo "FATAL: failed to pull kind node image $KIND_NODE_IMAGE." >&2
  echo "  Failing here rather than inside EnsureCluster's 3-minute create" >&2
  echo "  timeout. If the registry is unreachable from this queue, point" >&2
  echo "  KIND_NODE_IMAGE at a mirror." >&2
  exit 1
}

# Delete leftover kind clusters from a prior job on this agent's dind
# (KIND_KEEP_CLUSTER + agent reuse can leave state behind that fails
# the next CreateNamespace). Same ome-* scoping rationale as the EXIT
# trap above.
kind get clusters 2>/dev/null | while read -r c; do
  case "$c" in
    ome-*)
      echo "--- deleting leftover kind cluster: $c ---"
      kind delete cluster --name "$c"
      ;;
  esac
done

# Vendor the optional CRD YAMLs the suite installs. OMECRDPaths points at
# gitignored tests/integration/dep-crds/{keda,prometheus-operator}, which are
# regenerated from go.mod by `make dep-crds` and absent on a clean checkout.
# The `go test` below bypasses the `make integration-test` prerequisite that
# normally runs this, so vendor them explicitly first.
make dep-crds

# Run the KIND-backed suites. Set KIND_KEEP_CLUSTER=true so a
# multi-package test invocation (if added later) reuses the same
# cluster across packages.
echo "--- go test ./tests/integration/singlecluster/controller/omenative_coordination_kind/... ---"
KIND_KEEP_CLUSTER=true \
  go test ./tests/integration/singlecluster/controller/omenative_coordination_kind/... \
  -v -timeout 90m -ginkgo.v

# Cluster teardown happens in the EXIT trap (cleanup_ome_clusters), so
# it also runs when the go test above fails.
