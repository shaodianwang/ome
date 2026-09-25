# Reproducing a CI lane locally

Prefer the `make` targets: they pin tools and export `KUBEBUILDER_ASSETS` and
`LD_LIBRARY_PATH` for you. For a direct `go test`, export once per shell:

```sh
export KUBEBUILDER_ASSETS="$(bin/setup-envtest use "$(cat .envtest-k8s-version)" -p path)"   # after `make envtest`
export LD_LIBRARY_PATH="$(pwd)/pkg/xet/target/release"                                        # xet-linked packages, after `make xet-build`
export KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT=true                                            # envtest apiserver/etcd output in the test log
```

CI's `run_tests.sh`, `run_integration_tests.sh`, `run_perf_tests.sh` and
`run_scheduler_tests.sh` wrap envtest's `kube-apiserver` with an
`--advertise-address=127.0.0.1` shim because the CI pod's external interface is
IPv6-only. A test that fails only in CI with an apiserver that refuses to start
is that shim's territory, not the test's.

## Per step

| Step | Local command |
|---|---|
| `lint` | `make ci-lint`; `make lint-fix` for auto-fixable findings |
| `vet` | `make vet` |
| `test`: generator stage | `python3 -m unittest discover -s .buildkite -p 'test_*.py' -v` |
| `test`: harness unit tests | `(cd hack/omenative-testing/perm && python3 -m unittest -v run_test.py)` |
| `test`: perm corpus parse | `go test -count=1 -run TestGeneratedCorpusParses ./tests/integration/singlecluster/controller/omenative_perm_kind/` |
| `test`: ColumnarV2 golden | `go test -count=1 ./hack/omenative-testing/perm/goldengen/` |
| `test`: `make test` | `make test-no-xet` (no Rust needed) or `go test ./pkg/<pkg>/... -run <Test> -v`; xet-linked packages need `make test` |
| `test`: `make test-meta-internal` | `make test-meta-internal` (the `meta_internal` build tag) |
| `test`: coverage | `make test && make coverage` (the `-no-xet` coverage files do not feed the threshold) |
| `test`: bundle packaging | `go test -c -o /dev/null ./tests/integration/singlecluster/controller/omenative_coordination_kind` (and `multicluster/scaleout_kind`, `singlecluster/e2e/omenative_upgrade_kind`): a compile error in a KIND suite fails `test`, not the KIND lane |
| `rollout-behavior-lock` | `make rollout-behavior-lock MACHINE=instance` (needs `python3` + PyYAML). Tables only: `make rolloutspec-check MACHINE=instance`. Replay goldens that legitimately changed: `go test ./tests/rolloutspec/replay/... -update`, then re-run the lock and review the golden diff |
| `integration-test` | `make integration-test`; focused: `go test ./tests/integration/singlecluster/<area>/<suite>/... -v` |
| `perf-test` | `make test-perf` (soft-fail in CI; the CI-sized generators live under `tests/perf/*/configs/ci/`) |
| `scheduler-test` | `cd scheduler && go build ./... && CGO_ENABLED=1 go test -race ./... -count=1` (separate module; envtest assets exported) |
| `muse-rust-checks` | `make -C muse ci-rust` |
| `helm-lint` | `make helm-lint` |
| `fmt-drift` | `make fmt` |
| `tidy-drift` | `make tidy` |
| `manifests-drift` | `make manifests` (needs `jq`) |
| `generate-drift` | `make generate` |
| `dep-crds-drift` | `make dep-crds` |
| `oep-status-drift` | `make oep-status` |
| `internal-tokens` | `python3 -m unittest hack/check_internal_tokens_test.py && python3 hack/check_internal_tokens.py --tree` |
| `git-author-email` | `bash hack/check-git-author-email.sh --current` (your config) or `--revision origin/main..HEAD` (the commits) |
| pipeline generation | `python3 .buildkite/generate_pipeline.py >/dev/null`; `BUILDKITE_BRANCH=main python3 .buildkite/generate_pipeline.py` for the main-only shape |

For a drift step, commit whatever the target changed. If the target changes
nothing locally, compare toolchain versions with the CI image (`go version`,
the `bin/` tool pins) before assuming CI is wrong.

## KIND lanes (`kind-gke-<slug>`, `kind-gke-mc-*`)

The GKE jobs run the precompiled suite with `-ginkgo.focus-file` over the
group's spec files. Locally, `GINKGO_FOCUS` matches `It` text instead:

```sh
export KUBECONFIG="$(mktemp -t ome-kind-XXXXXX.kubeconfig)"    # hard rule: never the ambient kubeconfig
GINKGO_FOCUS='<fragment of the failing It>' make test-kind     # coordination suite; ~5 minutes per spec file
KIND_RECREATE=true GINKGO_FOCUS='...' make test-kind            # after suspected dirty cluster state
make test-kind-clean                                            # delete leftover clusters
rm -f "$KUBECONFIG"
```

Requirements: a running Docker daemon and a `kind` binary on `PATH` or at
`bin/kind` (`make kind` installs the pinned version). `KIND_KEEP_CLUSTER=true`
is the default, so the second run is fast. Other KIND targets exist for other
suites (`make test-kind-irmanaged`, `make test-kind-raw`, `make test-kind-eval`,
`make test-kind-upgrade`, `make test-kind-perm-up` + `make test-kind-perm`);
see the Makefile help for each one's inputs. Never run `kubectl config
use-context`; select a context per command with `kubectl --kubeconfig <tmp>
--context kind-<name>`.

A new `*_kind_test.go` must be added to `KIND_SPEC_GROUPS` in
`.buildkite/generate_pipeline.py` and the generator unit tests re-run, or every
build fails at pipeline upload.

## Eval lane (`omenative-e2e-*`, `main` only)

Case YAMLs live in `hack/omenative-testing/perm/isvcs/`; `run.py
--validate-only` checks them, `batch.py` runs them and writes a junit file per
job. `make test-kind-eval` runs the migrated cases against a chart-installed
OME in a local KIND cluster (same kubeconfig rule as above).
