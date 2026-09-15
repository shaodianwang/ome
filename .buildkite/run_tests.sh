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
#   - python3: runs the Buildkite generator and deployed OMENative harness tests.
apt-get update
apt-get install -y --no-install-recommends jq libssl-dev pkg-config build-essential curl ca-certificates python3

# Pipeline-generator unit tests (KIND spec classification enforcement
# and shard emission, including the focus regexes that decide which
# spec files each KIND shard runs). Cheap, so they run first.
echo "--- .buildkite generator unit tests ---"
python3 -m unittest discover -s .buildkite -p 'test_*.py' -v

echo "--- OMENative YAML/Python harness unit tests ---"
(
  cd hack/omenative-testing/perm
  python3 -m unittest -v run_test.py
)

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

# Perm-corpus contract: every committed case YAML (hand-written and
# generated) must parse with the Go loader — header fields, strict
# InferenceService decode, gang worker sizes for pod-weight derivation.
# Pure file parsing: no cluster, no suite bootstrap.
echo "--- perm corpus parse (Go) ---"
go test -count=1 -run 'TestGeneratedCorpusParses' \
  ./tests/integration/singlecluster/controller/omenative_perm_kind/

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
make test-meta-internal

# Enforce the 48% average coverage threshold defined in `make
# coverage` (Makefile §coverage). Operates on the coverage-{cmd,
# pkg,internal}.out files emitted by the preceding `make test`; if
# the average drops below 48% the target exits non-zero and fails
# this step.
make coverage

# Reuse the already-warm Go and Rust build caches to produce the exact-commit
# KIND suite binaries once. Consumer jobs use these bundles instead of
# compiling: the GKE KIND pods have no public egress, so a `go build` there
# would reach for the module proxy and fail.
#
# One bundle per suite rather than one shared bundle with several binaries. Each
# carries its own manifest, so a new suite is additive — it cannot change what
# an existing lane downloads or verifies.
make dep-crds
kind_artifact_dir=".buildkite/artifacts"
mkdir -p "$kind_artifact_dir"

# package_kind_bundle <bundle-name> <binary-name> <package>
#
# The manifest names the binary it ships, so the runner does not have to know
# which suite it was handed — it reads `binary=` and verifies that file's digest.
package_kind_bundle() {
  bundle_name="$1"
  binary_name="$2"
  package_path="$3"

  echo "--- package KIND test bundle: $bundle_name ($binary_name) ---"
  stage="$(mktemp -d)"
  mkdir -p "$stage/dep-crds"
  go test -c -o "$stage/$binary_name" "$package_path"
  # Only the gitignored, vendored dep-crds travel with the bundle; the committed
  # ones (prometheus-operator, envoy-gateway) arrive with the agent's checkout.
  # kueue-install is a rendered install manifest rather than a CRD set, and it
  # is derived from the Kueue version in .kueue-crd-version, so it has to be the
  # one built from this commit.
  cp -R tests/integration/dep-crds/keda "$stage/dep-crds/"
  cp -R tests/integration/dep-crds/scheduler-plugins "$stage/dep-crds/"
  cp -R tests/integration/dep-crds/kueue-install "$stage/dep-crds/"
  {
    printf 'state=ready\n'
    printf 'commit=%s\n' "${BUILDKITE_COMMIT:?BUILDKITE_COMMIT is required}"
    printf 'goos=%s\n' "$(go env GOOS)"
    printf 'goarch=%s\n' "$(go env GOARCH)"
    printf 'binary=%s\n' "$binary_name"
    printf 'sha256=%s\n' "$(sha256sum "$stage/$binary_name" | awk '{print $1}')"
  } >"$stage/manifest"
  tar -C "$stage" -czf "$PWD/$kind_artifact_dir/$bundle_name" .
  rm -rf -- "$stage"
}

package_kind_bundle omenative-kind-test.tgz omenative_coordination_kind.test \
  ./tests/integration/singlecluster/controller/omenative_coordination_kind
package_kind_bundle multicluster-kind-test.tgz scaleout_kind.test \
  ./tests/integration/multicluster/scaleout_kind
package_kind_bundle omenative-upgrade-kind-test.tgz omenative_upgrade_kind.test \
  ./tests/integration/singlecluster/e2e/omenative_upgrade_kind

# The kind binary rides along as an artifact for the same reason the suite
# binaries do: it is built here, where the Go toolchain and the module proxy
# are available, and consumed where neither is. The packaging step that puts
# it in the KIND runner image can reach registries and the Buildkite artifact
# store but not the kind release CDN, and the copy baked into kind-ci is a
# main-only artifact that a branch raising the pin cannot refresh.
#
# The version is the sigs.k8s.io/kind require in hack/internal/tools/go.mod —
# the same pin `make kind` installs — so CI and a developer's laptop run one
# kind. Built as its own module so nothing here perturbs the root go.mod.
kind_version=$(awk '$1 == "sigs.k8s.io/kind" { print $2; exit }' \
  hack/internal/tools/go.mod)
case "$kind_version" in
  v*) ;;
  *) echo "no sigs.k8s.io/kind version in hack/internal/tools/go.mod" >&2; exit 1 ;;
esac
echo "--- build kind $kind_version ---"
GOBIN="$PWD/$kind_artifact_dir" GOFLAGS= go install "sigs.k8s.io/kind@$kind_version"
test -x "$kind_artifact_dir/kind"
"$kind_artifact_dir/kind" --version
