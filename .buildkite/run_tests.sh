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

# `make test` (rather than the cheaper `test-no-xet`) is required
# because the coverage step that follows reads coverage-{cmd,pkg,
# internal}.out, which only `make test` produces — `make test-no-xet`
# writes the *-no-xet.out variants that the threshold check ignores.
# We already built libxet above so the marginal cost is the
# cmd/ome-agent test packages (~tens of seconds), not a Rust recompile.
make test

# Enforce the 48% average coverage threshold defined in `make
# coverage` (Makefile §coverage). Operates on the coverage-{cmd,
# pkg,internal}.out files emitted by the preceding `make test`; if
# the average drops below 48% the target exits non-zero and fails
# this step.
make coverage

# Reuse the already-warm Go and Rust build caches to produce the exact-commit
# KIND suite binary once. Consumer jobs use this bundle instead of compiling.
echo "--- package OMENative KIND test binary ---"
make dep-crds
kind_artifact_stage="$(mktemp -d)"
kind_artifact_dir=".buildkite/artifacts"
kind_test_binary="omenative_coordination_kind.test"
mkdir -p "$kind_artifact_stage/dep-crds" "$kind_artifact_dir"
go test -c \
  -o "$kind_artifact_stage/$kind_test_binary" \
  ./tests/integration/singlecluster/controller/omenative_coordination_kind
cp -R tests/integration/dep-crds/keda "$kind_artifact_stage/dep-crds/"
cp -R tests/integration/dep-crds/scheduler-plugins "$kind_artifact_stage/dep-crds/"
kind_test_sha256="$(sha256sum "$kind_artifact_stage/$kind_test_binary" | awk '{print $1}')"
{
  printf 'state=ready\n'
  printf 'commit=%s\n' "${BUILDKITE_COMMIT:?BUILDKITE_COMMIT is required}"
  printf 'goos=%s\n' "$(go env GOOS)"
  printf 'goarch=%s\n' "$(go env GOARCH)"
  printf 'sha256=%s\n' "$kind_test_sha256"
} >"$kind_artifact_stage/manifest"
tar -C "$kind_artifact_stage" -czf \
  "$PWD/$kind_artifact_dir/omenative-kind-test.tgz" .
rm -rf -- "$kind_artifact_stage"
