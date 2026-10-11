# Classification, known flakes, and the flaky-or-real decision

Classification describes where execution failed, not who caused it. An exit
code or a missing artifact alone never establishes infra; a quoted log line
does. Reasons are short fixed phrases; never paste secrets, tokens, or
registry credentials that a log may echo.

## Reading a log

- Fetch the raw text (`log.txt`, `Accept: text/plain`); the JSON `/log`
  endpoint mangles output. Strip `_bk;t=<ms>` timestamp escapes, OSC
  hyperlinks and ANSI colour before grepping (`ci_build.py log` does this).
- Lanes print `--- <stage> ---` headers; the failing stage is the last header
  before the first error. `test` runs the pipeline generator's unit tests
  first, then the Go packages, then coverage; a generator failure aborts the
  lane before any Go test runs.
- Ginkgo lanes end with `Summarizing N Failures` and one `[FAIL]` line per
  spec naming the file and line. `omenative-e2e-*` ends with a `NON-PASS:`
  list; `ci_build.py verdicts <build>` tallies every lane from the verdict
  file artifacts, and per-case logs are in the job's results archive artifact.
- The first real error is rarely the last line: Buildkite appends the
  command's exit status and, for the docker plugin, container teardown.

## Signature table

First match wins; step-key filters are in brackets. The classes are the ones
`ci_build.py classify` prints, so the two tools agree.

| Class | Signature | Meaning | Action |
|---|---|---|---|
| infra/docker-daemon | exit 125; `failed to connect to the docker API`, `Cannot connect to the Docker daemon` | the container queue's dind died before or during the step | one retry if the automatic `_infra_retry` (limit 2) is spent |
| infra/dind-sidecar | `FATAL: no reachable Docker daemon` | GKE agent pod lost its privileged dind sidecar | one retry; a repeat means the queue's pod spec is broken: escalate |
| infra/kind-node-boot | `cluster create failed in BeforeSuite with no spec run` (exit 125) | KIND node containers starved on the agent pod | auto-retried; at most one manual retry |
| infra/agent-lost | `signal_reason: agent_stop`, exit -1, log stops mid-stage | agent evicted or pod killed | one retry; check the queue's pending count if it repeats |
| infra/registry-auth | `no ECR credential source produced a token`, `authorization token has expired`, `no basic auth credentials` | registry credentials on the agent | one retry; a second hit means a broken secret: escalate |
| infra/secrets | `failed to fetch secrets`, `Error setting up job executor` | agent could not fetch job secrets | one retry, then escalate |
| infra/network | `proxy.golang.org ... timeout`, `TLS handshake timeout`, `dial tcp ... i/o timeout`, `EOF` on a download | transient egress | build, mirror and drift steps already `_auto_retry` any exit; otherwise one retry |
| infra/queue-starved | job sat in `scheduled` for most of the build and timed out, or the build `timed_out` with jobs never started | no agent picked the job up | not retryable by itself; check the queue with `buildkite-capacity-report` |
| generator/unclassified-spec | `unclassified KIND specs: <file>` | a new `*_kind_test.go` is not in `KIND_SPEC_GROUPS` | mechanical: add the stem, run the generator unit tests |
| generator/quarantine-expired | `KIND quarantine for <spec> expired` | an entry in `KIND_QUARANTINE` passed its `expires` date | fix the spec or extend the entry with a reason; ask first |
| generator/spec-groups | `mapped KIND specs do not exist`, `belongs to both`, `invalid or duplicate KIND spec group` | group table inconsistent | mechanical fix in `.buildkite/generate_pipeline.py` |
| hygiene/author-email | `Git author email '...' is not a GitHub noreply address` | commit author not a noreply address | the author rewrites the commit; on `main` it means the merge bypassed the check: report it |
| hygiene/internal-token | `N finding(s) in M scanned file(s)` | an identifier matched `hack/internal-tokens.txt` | replace with a neutral identifier; the finding names the pattern group |
| drift [`*-drift`] | `diff --git` after the make target | generated output not committed | mechanical: run the named target (`fmt`, `tidy`, `manifests`, `generate`, `dep-crds`, `oep-status`), commit |
| code/go-test | `--- FAIL: <Test>` | deterministic or nondeterministic Go test failure | reproduce with `go test ./<pkg>/... -run <Test> -v` (envtest lanes need `KUBEBUILDER_ASSETS`) |
| code/ginkgo-spec | `[FAIL]`, `Summarizing N Failures` | envtest or KIND spec failure | reproduce; KIND under a throwaway kubeconfig; never retry a red spec |
| code/data-race | `WARNING: DATA RACE` | `scheduler-test` runs under `-race` | fix the race |
| code/coverage [`test`] | `average coverage`, `below the ... threshold` | merged coverage under 48 % | add tests; `make coverage` prints the number |
| code/lint [`lint`] | `<file>.go:<line>:<col>: ... (<linter>)` | golangci-lint finding | `make ci-lint`, `make lint-fix` |
| code/helm [`helm-lint`] | `[ERROR]`, `N chart(s) failed` | chart template or values error | `make helm-lint` |
| code/rust [`muse-rust-checks`] | `error[E...]`, `clippy::` | console build or lint | `make -C muse ci-rust` |
| code/dockerfile [`build-*`] | `failed to solve:`, `ERROR: failed to build` | Dockerfile or its inputs | read the failing `RUN` |
| code/eval-case [`omenative-e2e-*`] | `NON-PASS:` | an eval case's expectation failed | `verdicts <build> --job <key>` names the cases and reasons; the case log is in the results archive |
| timeout/go-test | `panic: test timed out after`, `Test killed with quit` | hung test past `-test.timeout` | read the goroutine dump for the blocked wait |
| timeout/spec | a spec's `Eventually` past `kindLongTimeout`, job `timed_out` | slow convergence or starvation | code unless the whole lane was starved (see queue-starved) |
| advisory | `soft_failed: true` | `perf-test`, `kind-gke-mc-*`, `kind-gke-quarantine` | note it; never blocks merge |
| needs-evidence | none of the above | | read from the last stage header; say what is missing |

Retry presets, for reading `attempts` in `main_builds.py diff`: `_infra_retry`
(exit 125 or `agent_stop`, limit 2) on the container-queue lanes and the KIND
children; `_auto_retry` (any exit, limit 2) on `build-*`, `mirror-*`, and the
drift steps; none on the eval, upgrade, and quarantine jobs. A step whose
newest attempt passed after an exit-125 attempt is the preset working; a
step whose newest attempt passed after a `--- FAIL` or `[FAIL]` attempt is a
nondeterministic defect and gets written up.

## Known flakes

Confirm the signature before calling any failure a known flake, and always
link the evidence. Three identical hits in a row on `main` stop being a
flake: file the issue and propose hardening.

| Lane and key | Signature | Note |
|---|---|---|
| `perf-test` | `--- FAIL: TestScalability/FunnelBeatsPoll` | soft-fail lane; timing assertion sensitive to a noisy agent |
| `kind-gke-mc-quota` (`Multi-cluster Kueue quota`) | job `timed_out` or the Kueue admission `Eventually` past its timeout | soft-fail group; three KIND clusters in one pod |
| `omenative-e2e-kind-gke`, case `p317` | `NON-PASS:` list containing `p317` | main-only eval; the case's log in the results archive shows the same convergence wait each time |

## Flaky or real

| Situation | Verdict | What to do |
|---|---|---|
| matches a known flake, single hit | flaky-known | note in the write-up; no issue |
| same key red in the previous standard main build, same first error | real, standing | find the first red build with `main_builds.py list`; commit range from the last green |
| key green in the previous build; commit range touches the failing area | real, likely code | rank suspects mechanistically; write up; draft PR only if mechanical |
| key green in the previous build; commit range unrelated; deterministic error | real, needs reproduction | reproduce locally before naming anything |
| infra signature, first hit | transient until proven | one retry; recovered → `infra-recovered`; failed identically → escalate |
| infra signature on two different agents in one build | not transient | escalate with both job links |
| newest attempt passed, an earlier attempt failed a spec or Go test | nondeterministic defect | write up and file; never call it a flake |
| soft-failed only | advisory | note; dispatch only after three builds in a row |
| build still `running` or `failing` | no verdict yet | say when to look again |

## Ranking suspects

1. `gh api repos/mslsrc/ome/compare/<prev>...<sha> --jq '.commits[] | .sha[0:8] + " " + (.commit.message | split("\n")[0])'`
   lists the merges between the previous standard main build and this one
   (one build per merge on `main`, so the range is usually one PR).
2. Map touched paths to lanes: `pkg/`, `internal/`, `cmd/` → `test`,
   `integration-test`, KIND children; `scheduler/` → `scheduler-test`;
   `charts/` → `helm-lint`, `chart-*`; `dockerfiles/` → `build-*`;
   `.buildkite/` → the generator stage of `test` and the lane it edits;
   `pkg/apis/` → `manifests-drift`, `generate-drift`; `go.mod` → `tidy-drift`,
   `dep-crds-drift`; `oeps/` → `oep-status-drift`; `muse/` → `muse-rust-checks`;
   `tests/rolloutspec/` → `rollout-behavior-lock`.
3. Connect the failing symbol or behaviour to a diff hunk before naming a
   culprit; otherwise present ranked suspects and say what would confirm.
4. Name suspects by PR number and title. Never by a person.

## Agent-pod evidence

Only for a GKE job that died before its log said anything (image pull,
secret mount, eviction). It needs operator credentials for that cluster, and
every command names the kubeconfig and context explicitly; never
`kubectl config use-context` on the ambient file:

```sh
KCFG=<operator kubeconfig>; CTX=<context>; NS=<agent namespace>
kubectl --kubeconfig "$KCFG" --context "$CTX" -n "$NS" get pods --show-labels | grep <job-id-prefix>
kubectl --kubeconfig "$KCFG" --context "$CTX" -n "$NS" describe pod <pod>
kubectl --kubeconfig "$KCFG" --context "$CTX" -n "$NS" logs <pod> -c container-0
kubectl --kubeconfig "$KCFG" --context "$CTX" -n "$NS" logs <pod> -c dind
```
