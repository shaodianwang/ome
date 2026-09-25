# Failure signatures and dispositions

Classify before acting. A retry is a tool for infrastructure failures; it never
fixes a code failure, a drift, or a generator error. `scripts/ci_build.py
classify <build>` applies the table below to every failed job's cleaned log
and prints the first matching line as evidence; read the log around it before
deciding.

## Job fields first

| Field | Meaning |
|---|---|
| `exit_status: 125` | the container agent could not reach its Docker daemon (`failed to connect to the docker API`), or `run_kind_tests_gke.sh` translated a `BeforeSuite` cluster-create failure |
| `signal_reason: agent_stop`, `exit_status: -1`, log ends abruptly | agent lost or job killed from outside; not a test result |
| `state: timed_out` | Buildkite step timeout; `omenative-e2e-*` carry 180 minutes, the KIND runner passes `-test.timeout` (`OME_KIND_TIMEOUT`, 90m default, 60m multi-cluster) |
| `soft_failed: true` | advisory lane (`perf-test`, `kind-gke-mc-*`, `kind-gke-quarantine`); the build passes regardless |
| `state: waiting_failed` / `blocked_failed` | a dependency failed; nothing to fix in this job |
| `retried: true`, `retried_in_job_id` | an older attempt; look at the newest one |

## Log signatures

| Signature | Class | Action |
|---|---|---|
| `failed to connect to the docker API`, `Cannot connect to the Docker daemon` | infra/docker-daemon | one retry; most steps already retry exit 125 twice |
| `FATAL: no reachable Docker daemon after ~120s` | infra/dind-sidecar | retry once; if it repeats the GKE queue's pod spec lost its privileged `dind` container (operator runbook `.buildkite/README-gke-kind.md`) |
| `cluster create failed in BeforeSuite with no spec run; exiting 125` | infra/kind-node-boot | auto-retried; one manual retry if the limit was spent |
| `no ECR credential source produced a token`, `authorization token has expired` | infra/registry-auth | retry once; twice means the cluster secret is broken: escalate CI infrastructure |
| module proxy timeouts, `TLS handshake timeout`, `i/o timeout` before any test ran | infra/network | retry once (drift, build, mirror steps auto-retry on any exit) |
| jobs sit in `scheduled`/`pending` for a long time on the GKE queue | not a failure | queue capacity; wait or ask for the queue to be enlarged |
| `unclassified KIND specs: <file>` | generator | add the stem to `KIND_SPEC_GROUPS`; verify with `python3 -m unittest discover -s .buildkite -p 'test_*.py'` |
| `KIND quarantine for <spec> expired on <date>` | generator | fix the spec and return it to its group, or extend `expires` with a reason |
| `belongs to both`, `mapped KIND specs do not exist`, `invalid or duplicate KIND spec group` | generator | fix the group tables |
| `Git author email '<x>' is not a GitHub noreply address` | hygiene | see `fix-hygiene.md`; rewriting the rejected commits is required |
| `<path>:<line>: [<group>] ...` then `N finding(s) in M scanned file(s)` | hygiene/internal-token | replace the identifier with a neutral fixture; `hack/internal-tokens.txt` names the group; only deployment-only files may be exempted in `hack/internal-tokens.allow`, with a reason |
| `[unclassified] new top-level entry` | hygiene/internal-token | classify the new directory in `hack/check_internal_tokens.py` and the boundary section of `AGENTS.md` |
| `diff --git ...` after `make <target>` on a `*-drift` step | drift | run the same target locally and commit the output |
| `--- FAIL: TestX` | code | reproduce with `go test ./<pkg>/... -run TestX -v` (envtest assets exported) |
| Ginkgo `[FAIL]` / `[FAILED]`, `Summarizing N Failures` | code | reproduce the named `It`; envtest suites under `tests/integration/singlecluster/...`, KIND under `make test-kind` with `GINKGO_FOCUS` |
| `WARNING: DATA RACE` in `scheduler-test` | code | fix the race; the `-race` gate is deliberate |
| `panic: test timed out after`, goroutine dump | code/timeout | a blocked `Eventually` or channel; the dump names it |
| coverage average below 48% (`make coverage` output) | code | add tests; the threshold is enforced in the Makefile |
| golangci-lint findings `file.go:L:C: message (linter)` | code | `make ci-lint`; `make lint-fix` |
| `NON-PASS:` list and `FAILED CASE LOGS` in an `omenative-e2e-*` job | code/eval-case | the results archive artifact holds per-case logs and the junit file; `INFRA-FAIL` is the fault apparatus, not the product |
| `failed to solve:` / `ERROR: failed to build` in `build-*` | code/dockerfile | the Dockerfile under `dockerfiles/` failed; PR branches build but never push |

## Known flakes (state them, do not chase them)

| Where | Signature | Disposition |
|---|---|---|
| `perf-test` | `TestScalability/FunnelBeatsPoll` (`tests/perf/multicluster/checker`) | soft-fail lane; does not block merge; mention it, move on |
| `kind-gke-mc-quota` (`Multi-cluster Kueue quota`) | suite or spec timeout | soft-fail lane; note it |
| `omenative-e2e-kind-gke` (`main` only) | case `p317` non-pass | known; a `main` follow-up, not a PR blocker |

A "flake" that reproduces on a second agent, or on `main` without an engine
change, is a defect with an owner. The repository's answer is quarantine
(`KIND_QUARANTINE` in the generator, with owner, reason and expiry), not retry;
see `fix-hygiene.md`.

## Reading a log

- Each lane prints `--- stage ---` headers; the failing stage is the last
  header before the first error. In `test`, that tells you whether the
  generator tests, the harness tests, `make test`, coverage, or bundle packaging
  failed.
- KIND jobs: `--- named KIND group: <focus regex> ---` names the spec files;
  `--- run precompiled suite ... ---` starts the Ginkgo output; the `Ran N of M
  Specs` line tells you whether any spec ran.
- Go test output interleaves; search for `--- FAIL` first, then for `panic:`,
  then read the 40 lines above the first hit.
- The first marker is rarely the cause when the log ends with exit -1 or a
  timeout: read how the log ends before reading how it starts.
