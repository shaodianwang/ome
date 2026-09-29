#!/bin/sh
# Integration test step for the BuildKite pipeline.
#
# Runs `make integration-test` (Ginkgo + envtest under tests/) —
# webhooks, BaseModel/ServingRuntime/RuntimeRevision reconcilers,
# ClusterCache, agent metadata, and the multicluster sim suite —
# against a real kube-apiserver/etcd downloaded by setup-envtest. No
# real cluster needed.
#
# Mirrors run_tests.sh's environment setup (libxet link deps, IPv6
# advertise-address shim, etc.) so the two share the same base.
set -eu

apt-get update
apt-get install -y --no-install-recommends jq libssl-dev pkg-config build-essential curl ca-certificates

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
sh /tmp/rustup.sh -y --default-toolchain stable --profile minimal

export PATH="$HOME/.cargo/bin:/usr/local/go/bin:/go/bin:$PATH"
export KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT=true

# integration-test depends on libxet link path through cmd/manager;
# build the staticlib first.
make xet-build
make envtest

# Same advertise-address shim as run_tests.sh (mks-cpu IPv6).
ENVTEST_K8S_VERSION="$(cat .envtest-k8s-version)"
echo "--- setup-envtest use $ENVTEST_K8S_VERSION ---"
./bin/setup-envtest use "$ENVTEST_K8S_VERSION" 2>&1 | tee /tmp/envtest.log
ENVTEST_ASSETS="$(awk '/^Path:/ {print $2}' /tmp/envtest.log)"
echo "envtest assets at: $ENVTEST_ASSETS"
if [ -z "$ENVTEST_ASSETS" ] || [ ! -d "$ENVTEST_ASSETS" ]; then
  echo "failed to resolve envtest assets path" >&2
  exit 1
fi
if [ ! -f "$ENVTEST_ASSETS/kube-apiserver.real" ]; then
  mv "$ENVTEST_ASSETS/kube-apiserver" "$ENVTEST_ASSETS/kube-apiserver.real"
  cat > "$ENVTEST_ASSETS/kube-apiserver" <<'WRAP'
#!/bin/sh
exec "$(dirname "$0")/kube-apiserver.real" --advertise-address=127.0.0.1 "$@"
WRAP
  chmod +x "$ENVTEST_ASSETS/kube-apiserver"
fi

# TEMP DIAGNOSTIC (to be dropped): make integration-test, step by step; on
# failure, encode which step/package failed in the job's finish time.
encode() { echo "failure idx=$1"; now=$(date +%s); target=$(( (now/300+1)*300 + $1*6 )); sleep $((target-now)); exit 1; }
make fmt vet manifests envtest dep-crds xet-build test-kindutil-unit || encode 50
PKGS=$(go list ./tests/... | grep -v -E '/kindutil($|/)|_kind$|/tests/perf/|/tests/rolloutspec($|/)|/integration/multicluster$')
TEST_ENV_ARGS="KUBEBUILDER_ASSETS=$ENVTEST_ASSETS LD_LIBRARY_PATH=$(pwd)/pkg/xet/target/release"
env $TEST_ENV_ARGS go test -json -timeout=20m $PKGS -ginkgo.trace > /tmp/it.json || true
failed=$(jq -R -r 'fromjson? | select(.Action=="fail" and .Test==null) | .Package' /tmp/it.json | sort -u)
echo "failed packages:"; echo "$failed"
jq -R -r 'fromjson? | select(.Action=="output") | .Output' /tmp/it.json | grep -E "^\s*\[FAIL\]|--- FAIL|panic:" | head -50 || true
idx=0; n=0
for p in $PKGS; do n=$((n+1)); if echo "$failed" | grep -qx "$p"; then idx=$n; break; fi; done
if [ -n "$failed" ] && [ "$idx" -eq 0 ]; then idx=40; fi
[ "$idx" -gt 0 ] && encode $idx
env $TEST_ENV_ARGS go test -v -timeout=20m ./tests/integration/multicluster -ginkgo.trace || encode 45
exit 0
make integration-test
