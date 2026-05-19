#!/bin/sh
# Test step for the BuildKite pipeline.
#
# Lives in its own file so shell variables like $ENVTEST_ASSETS aren't
# eaten by BuildKite's command-field interpolation (which expands $VAR
# at job-submit time and replaces unset vars with the empty string).
set -eu

# The Buildkite k8s plugin runs commands under /bin/sh (dash on
# bookworm), so avoid bashisms like `pipefail`.
#
# System deps:
#   - jq: required by `make manifests` CRD post-processing.
#   - libssl-dev / pkg-config / build-essential: needed to link libxet.
#   - curl + ca-certificates: bootstraps rustup.
apt-get update
apt-get install -y --no-install-recommends jq libssl-dev pkg-config build-essential curl ca-certificates

# Rust toolchain for `pkg/xet` (CGo wrapper around a Rust staticlib).
# cmd/manager transitively links libxet, so the "no-xet" target still
# requires it at link time today. Tracked for follow-up: extract the
# model controller out of cmd/manager so the manager binary no longer
# depends on libxet.
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
sh /tmp/rustup.sh -y --default-toolchain stable --profile minimal

# Buildkite's k8s plugin injects its own PATH that omits the golang
# image's /usr/local/go/bin and /go/bin, so re-add them explicitly
# alongside cargo.
export PATH="$HOME/.cargo/bin:/usr/local/go/bin:/go/bin:$PATH"

# Forward envtest's kube-apiserver/etcd stdout+stderr into the test
# log so we can see why the control plane fails to start.
export KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT=true

make xet-build
make envtest

# The mks-cpu pod's external interface is IPv6. kube-apiserver
# auto-detects it as the external host, then refuses to start because
# the IPv6 host doesn't match the IPv4 service-cluster IP range envtest
# passes (10.0.0.0/24). Wrap the binary with a shim that pins
# --advertise-address to 127.0.0.1 so the auto-detection is skipped.
echo "--- setup-envtest use 1.30 ---"
./bin/setup-envtest use 1.30 2>&1 | tee /tmp/envtest.log
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

make test-no-xet
