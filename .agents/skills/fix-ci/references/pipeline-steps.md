# Pipeline steps, step by step

Everything here is read from `.buildkite/generate_pipeline.py` and the
`.buildkite/run_*.sh` scripts next to it. When a detail below disagrees with
those files, the files win; re-read them. The generator runs at pipeline
upload with `BUILDKITE_BRANCH`, `BUILDKITE_COMMIT` and the kill-switch
variables from the build environment; render it locally with
`python3 .buildkite/generate_pipeline.py` (add `BUILDKITE_BRANCH=main` for the
main-only shape).

The build is uploaded in several documents because Buildkite caps the jobs of
one upload (`UPLOAD_JOB_LIMIT` in the generator, below the organization's
limit). The entrypoint uploads what the generator prints: every step that is
not an eval lane, ending on `main` with `upload-eval-lanes`, which runs
`.buildkite/upload_pipeline_batches.sh`: it writes every document
(`--batch-dir`, attached as artifacts) and uploads the rest in order, one eval
group per document and the release marker last. Both logs print the job count
per upload and the total.

## GitHub commit statuses

`_gh_status(key)` attaches `notify: github_commit_status: context: <key>` to a
step, so the GitHub context equals the step key. Groups attach one notify for
the group key (`integration-test-kind-gke`, `integration-test-kind-mc-gke`,
`integration-test-kind-upgrade`, `omenative-e2e-kind-gke-<i>-of-<n>`); their
children post nothing of their own, and the quarantine group posts nothing at
all.
`buildkite/ome` is Buildkite's pipeline rollup; its `target_url` carries the
build number.

## Retry presets

| Preset | Conditions | Used by |
|---|---|---|
| infra retry | `exit_status: 125` or `signal_reason: agent_stop`, limit 2; never matches exit 1 | `git-author-email`, `internal-tokens`, `lint`, `vet`, `test`, `rollout-behavior-lock`, `integration-test`, `scheduler-test`, `muse-rust-checks`, `helm-lint`, `upload-eval-lanes`, every `kind-gke-<slug>` child |
| auto retry | any non-zero exit, limit 2 | every `*-drift` step, `package-kind-test-image`, `package-kind-eval-image`, `build-*`, `mirror-*`, `chart-ome-release` |
| none | | `perf-test`, `kind-gke-mc-*`, `kind-gke-quarantine`, `kind-gke-upgrade-identity`, `omenative-e2e-*`, `chart-<chart>` |

Exit 125 is what the container-based agents return when the Docker daemon is
unreachable (`failed to connect to the docker API`). `run_kind_tests_gke.sh`
also translates one shape to 125 on purpose: the suite failed in `BeforeSuite`
while creating the KIND cluster (`EnsureCluster(` and `createCluster:` in the
log, no `Ran N of M` line). Any run that reached a spec keeps its own exit code.

## Every-branch steps (`ci_steps`)

| Key | Label | Command | Notes |
|---|---|---|---|
| `git-author-email` | Git Author Email | `bash .buildkite/check_git_author_email.sh` | on PRs checks every commit since the merge base; requires `@users.noreply.github.com` |
| `internal-tokens` | Internal Identifiers | `bash .buildkite/check_internal_tokens.sh` | runs `hack/check_internal_tokens_test.py` then `hack/check_internal_tokens.py --tree` |
| `lint` | Lint | `make ci-lint` | golangci-lint, 15 minute budget |
| `vet` | Vet | `make vet` | |
| `test` | Unit Tests + Coverage | `sh .buildkite/run_tests.sh` | stages below; uploads the KIND bundles and the `kind` binary as artifacts |
| `rollout-behavior-lock` | Rollout Behavior Lock | `make rollout-behavior-lock MACHINE=instance` | table checkers, replay goldens, decided-cell tests |
| `integration-test` | Integration Tests | `sh .buildkite/run_integration_tests.sh` | `make integration-test` under envtest |
| `perf-test` | Perf Tests | `sh .buildkite/run_perf_tests.sh` | `make test-perf`; **soft-fail** |
| `scheduler-test` | Scheduler Module | `sh .buildkite/run_scheduler_tests.sh` | separate module under `scheduler/`; `go build ./...` then `CGO_ENABLED=1 go test -race ./... -count=1` |
| `muse-rust-checks` | MUSE Rust Checks | `make -C muse ci-rust` | rustfmt, clippy, tests for the console |
| `helm-lint` | Helm Lint | `make helm-lint` | |
| `fmt-drift` | Fmt Drift | `make fmt && git diff --exit-code` | |
| `tidy-drift` | Tidy Drift | `make tidy && git diff --exit-code` | |
| `manifests-drift` | Manifests Drift | `make manifests && git diff --exit-code` | installs `jq` first |
| `generate-drift` | Generate Drift | `make generate && git diff --exit-code` | |
| `dep-crds-drift` | Dep CRDs Drift | `make dep-crds && git diff --exit-code` | |
| `oep-status-drift` | OEP Status Drift | `make oep-status && git diff --exit-code` | `oeps/STATUS.md` is rendered from every `oeps/*/oep.yaml` |

Stages inside `test` (`run_tests.sh`), each under its own `--- name ---` header:
generator unit tests (`python3 -m unittest discover -s .buildkite -p 'test_*.py'`);
OMENative harness unit tests (`hack/omenative-testing/perm/run_test.py`); Rust
toolchain and `make xet-build`; `make envtest`; perm corpus parse
(`TestGeneratedCorpusParses`); perm ColumnarV2 golden (`hack/omenative-testing/perm/goldengen/`);
the envtest `--advertise-address=127.0.0.1` shim; `make test`;
`make test-meta-internal`; `make coverage` (48% average threshold); `make dep-crds`
and packaging of the three KIND test bundles plus the pinned `kind` binary.

## KIND on GKE (emitted when `GKE_KIND_ENABLED`, default on)

| Key | Depends on | Notes |
|---|---|---|
| `package-kind-test-image` | `test` (on main also `mirror-kind-ci`, `mirror-canary-metrics`) | bakes the exact-commit bundles into a runner image; auto retry |
| group `integration-test-kind-gke` → `kind-gke-<slug>` | `package-kind-test-image` | one three-node KIND cluster per child; `OME_KIND_FOCUS` selects spec files (`-ginkgo.focus-file`); **hard gate**; infra retry |
| group `integration-test-kind-quarantine` → `kind-gke-quarantine` | `package-kind-test-image` | only when `KIND_QUARANTINE` is non-empty; **soft-fail** |
| group `integration-test-kind-mc-gke` → `kind-gke-mc-placement`, `kind-gke-mc-quota` | `package-kind-test-image` | multi-cluster suite, three KIND clusters per job, `OME_KIND_TIMEOUT=60m`; **soft-fail** |

Child slugs come from `KIND_SPEC_GROUPS` (coordination suite under
`tests/integration/singlecluster/controller/omenative_coordination_kind/`):
`foundation-lifecycle`, `status-events-services`, `runtime-retry-recovery`,
`pd-topology-isolation`, `bluegreen-core`, `canary-rollouts`, `canary-lifecycle`,
`ratio-core`, `ratio-edges`, `ratio-load`, `sequential-core`,
`sequential-variants`, `sequential-interleavings`, `sequential-holds`,
`update-core`, `percomponent-maxsurge`, `rolling-updates`, `gang-recovery`,
`gang-surge-multipod`, `migration-relocation`, `migration-lifecycle`,
`pause-cancel-interleavings`, `scale-transitions`, `scale-transition-boundaries`,
`scale-transition-races`, `deletion-disruption`. The job label is the group's
human name plus its file count, for example `Sequential coordination (2 files)`.
Multi-cluster slugs come from `MC_KIND_SPEC_GROUPS` (`placement`, `quota`) over
`tests/integration/multicluster/scaleout_kind/`.

Ownership is exact: a `*_kind_test.go` that is in no group, in two groups, or
listed but missing fails pipeline generation for every build (`unclassified
KIND specs: ...`, `belongs to both ...`, `mapped KIND specs do not exist ...`).
A quarantine entry needs `spec`, `owner`, `reason`, `expires`; the day after
`expires` generation fails until the spec is fixed or the entry extended.

Queue: `GKE_KIND_QUEUE` (default `gke-tpu-v7x`). A step targeting a queue with
no agents waits forever; jobs long in `pending`/`scheduled` mean queue capacity.

## Images and charts (`cd_steps`, every branch)

| Key | Behaviour on PR branches | On `main` |
|---|---|---|
| `build-<image>` for `ome-manager`, `ome-agent`, `canary-metrics`, `muse-server`, `ome-scheduler`, `kind-ci`, `ome-quota-manager`, `alfred` | builds linux/amd64 and linux/arm64, validates the Dockerfile, no push | pushes tags `latest` and the 8-char SHA |
| `mirror-<image>` | `--only-main`: exits without work | registry-level copy to the production registry |
| `chart-<chart>` for `ome-crd`, `ome-resources`, `ome-isvc`, `muse`, `ome-scheduler`, `ome-quota-manager` | packages (image-stamping charts wait for the image build and mirror) | pushes |

All of these depend on `test`; a red `test` leaves them `waiting_failed`, which
is a dependency state, not a failure of their own.

## `main`-only steps

| Key | Gate | Notes |
|---|---|---|
| `package-kind-eval-image` | `GKE_EVAL_ENABLED` | after the manager, agent, canary-metrics and kind-ci mirrors |
| `upload-eval-lanes` | `GKE_EVAL_ENABLED` | last step of the entrypoint's upload; uploads the eval groups and then the release marker as further documents |
| groups `omenative-e2e-kind-gke-<i>-of-<n>` → `omenative-e2e-<feature>-<topology>[-i-of-n]` | `GKE_EVAL_ENABLED` | deployed OMENative eval over the case YAMLs in `hack/omenative-testing/perm/isvcs/`, one group per upload (per scheduler, cut between families when a scheduler's jobs exceed the limit); 180 minute timeout; results archive uploaded as an artifact |
| group `integration-test-kind-upgrade` → `kind-gke-upgrade-identity` | `GKE_UPGRADE_ENABLED` and `GKE_KIND_ENABLED` | operator-upgrade identity gate; skips loudly when the previous images were not baked |
| `chart-ome-release` | `build.branch == "main"` | uploaded last; depends on every other top-level step and group by key (a soft-failing step completes before it without blocking it): the release marker for the control-plane charts and images |
