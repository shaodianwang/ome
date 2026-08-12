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

make integration-test
