#!/bin/sh
# Scheduler-module test step for the BuildKite pipeline.
#
# The scheduler (scheduler/go.mod) is a SEPARATE Go module — it pins
# k8s.io/kubernetes via a staging `replace` block on its own cadence, so
# it can't live in the root module. Consequently the root pipeline's
# `go test ./...` never descends into it: this step is the only thing
# that builds and tests ome-scheduler (the OMEGangPack plugin + its pure
# topology/placement packages + the envtest integration harness).
#
# No xet/Rust here — the scheduler links no CGo staticlib. It does need
# envtest binaries for the integration suite, so it reuses run_tests.sh's
# setup-envtest + IPv6 advertise-address shim (pinned to 1.33 to match the
# module's k8s version, not the root's 1.30).
set -eu

apt-get update
apt-get install -y --no-install-recommends ca-certificates

# Buildkite's k8s plugin injects a PATH that omits the golang image's
# /usr/local/go/bin and /go/bin; re-add them.
export PATH="/usr/local/go/bin:/go/bin:$PATH"
export KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT=true

# `make envtest` installs bin/setup-envtest from the root module.
make envtest

# Same advertise-address shim as run_tests.sh (mks-cpu IPv6), but the
# scheduler module targets k8s 1.33, so fetch matching envtest assets.
echo "--- setup-envtest use 1.33 ---"
./bin/setup-envtest use 1.33 2>&1 | tee /tmp/envtest.log
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
export KUBEBUILDER_ASSETS="$ENVTEST_ASSETS"

# Build + test the scheduler module. `go build ./...` compile-checks every
# package; `go test -race ./...` runs unit (topology/placement/gangpack) + the
# envtest integration harness (test/integration).
#
# -race is a deliberate merge-gate: the plugin runs a background pin-GC goroutine
# that touches the (mutex-guarded) pin store concurrently with the scheduling
# loop, so every PR must clear the race detector. It roughly doubles runtime
# (~40s total here), which is well within budget for a per-PR gate. The race
# detector needs cgo + a C toolchain, both present in the CI image.
cd scheduler
go build ./...
CGO_ENABLED=1 go test -race ./... -count=1
