#!/bin/sh
# KIND-backed integration test step for the BuildKite pipeline.
#
# Runs the KIND-backed suites under tests/integration/singlecluster/
# controller/coordination_kind/. Each suite spins up a single-node KIND
# cluster, builds the fake-serving image, kind-loads it, starts an
# in-process controller-runtime manager pointed at the kind apiserver,
# and runs Ginkgo specs against real kubelet + scheduler + containerd.
#
# Coverage: 24 specs (~4-5 min wall-clock) covering the OEP-0007.1 +
# OEP-0007.2 + OEP-0011 contracts the envtest sibling cannot exercise
# under its instant-Ready PodKubeletSimulator (pod-lifecycle timing,
# drain races, per-revision Service convergence, in-place image
# patches, RatioBalanced gate at scale, mid-rollout pause, Sequential
# active-Component handoff, partial-failure recovery, bad-image
# escalation).
#
# Infrastructure requirements (Docker on the agent host):
#   - kind (downloaded by this script on first run)
#   - docker (must be available; the k8s-plugin pod inherits it from
#     the runner — if not, the kind cluster bring-up fails fast with
#     "Could not connect to Docker"; surface as a clear infra ask)
#   - ~6 GB memory for the kind node container
#   - ~2 GB disk for the kind node image + fake-serving build
#
# Same env-setup pattern as run_tests.sh + run_integration_tests.sh
# (libxet link deps, IPv6 advertise-address shim NOT needed because
# kind owns its own apiserver). Mirrors the existing scripts'
# /bin/sh-with-no-bashisms convention because the k8s plugin runs
# /bin/sh.
set -eu

apt-get update
apt-get install -y --no-install-recommends jq libssl-dev pkg-config build-essential curl ca-certificates

# Rust toolchain. The Makefile's `make test-no-xet` path doesn't
# require it, but the kind suites import the same packages cmd/manager
# does, and those transitively link the xet Rust staticlib.
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
sh /tmp/rustup.sh -y --default-toolchain stable --profile minimal

export PATH="$HOME/.cargo/bin:/usr/local/go/bin:/go/bin:$PATH"
make xet-build
export LD_LIBRARY_PATH="$(pwd)/pkg/xet/target/release"

# kind binary. Install via `go install` rather than curl from
# kind.sigs.k8s.io — the mks-cpu agents can't reach external HTTPS
# endpoints, but they DO have the Go module proxy wired up
# (xet's transitive deps fetch from huggingface/xet-core successfully
# in the steps above). Pin to a known-good release; bump when
# KIND_VERSION changes are intentional (each bump can shift kubelet
# / containerd versions inside the node container).
KIND_VERSION="${KIND_VERSION:-v0.27.0}"
go install sigs.k8s.io/kind@${KIND_VERSION}
echo "--- kind --version ---"
kind --version

# Verify docker is reachable. Bail out early with an unambiguous error
# if not — kind cluster bring-up's failure mode is less helpful.
if ! docker info >/dev/null 2>&1; then
  echo "FATAL: docker is not reachable from this BuildKite agent." >&2
  echo "  The KIND tests need Docker (or a Docker-compatible runtime) on the host." >&2
  echo "  If you're seeing this on mks-cpu, the queue may not support DinD / sysbox." >&2
  exit 1
fi
echo "--- docker info (truncated) ---"
docker info 2>&1 | head -20

# Pre-pull the kind node image. The default node image is bundled
# with the kind binary, so this is a noop on a cold runner but avoids
# a ~600MB pull stall at suite-startup time.
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.30.0}"
docker pull "$KIND_NODE_IMAGE" || true

# Ensure no leftover kind clusters from a prior run on the same agent
# (KIND_KEEP_CLUSTER + agent reuse can leave state behind that fails
# the next CreateNamespace).
kind get clusters 2>/dev/null | while read -r c; do
  echo "--- deleting leftover kind cluster: $c ---"
  kind delete cluster --name "$c"
done

# Run the KIND-backed suites. Set KIND_KEEP_CLUSTER=true so a
# multi-package test invocation (if added later) reuses the same
# cluster across packages.
echo "--- go test ./tests/integration/singlecluster/controller/coordination_kind/... ---"
KIND_KEEP_CLUSTER=true \
  go test ./tests/integration/singlecluster/controller/coordination_kind/... \
  -v -timeout 30m -ginkgo.v

# Best-effort cleanup so the agent isn't left holding ~6 GB of kind
# state for the next job. Failures are non-fatal — the next run's
# leftover-cluster check above will pick up the slack.
kind get clusters 2>/dev/null | while read -r c; do
  kind delete cluster --name "$c" || true
done
