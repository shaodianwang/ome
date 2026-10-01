# User guide — the GKE KIND suite (`integration-test-kind-gke`)

For **OME developers**: how to read the GKE-backed OMENative KIND CI signal and
trigger ad-hoc runs. This is the *consumer* side. To stand up or change the agent
queue itself, see the operator runbook `README-gke-kind.md`.

## What it is (30-second version)

The OMENative deployment mode is validated by the KIND coordination suite
(`tests/integration/singlecluster/controller/omenative_coordination_kind/`, with
dozens of spec files needing a real kubelet: rollout coordination, drain races,
Sequential handoff, per-revision Service convergence, in-place patches). It
can't run on the TW dind (read-only `/proc/sys/net` → kind can't network), so it
runs on a **GKE agent queue** as parallel, named semantic jobs.

- Buildkite step key: **`integration-test-kind-gke`**.
- Commit-status context (what shows on the PR): **`integration-test-kind-gke`**.
- Runner: `.buildkite/run_kind_tests_gke.sh` (offline; loads baked images into a
  privileged dind sidecar and runs the exact-commit precompiled test binary).
- Each named job owns a stable set of related spec files and creates one
  three-node KIND cluster. Groups are balanced by observed runtime, not file
  count; `KIND_SPEC_GROUPS` in `generate_pipeline.py` is the source of truth.

## When it runs

`generate_pipeline.py` emits the step by default. Set `GKE_KIND_ENABLED=0` only
as an emergency kill switch if the queue is unavailable. The package step and
every named test job are hard gates: a failure fails the build and blocks the
merge.

## Access you need

- Buildkite access to the **`msl/ome`** pipeline (`buildkite.com/msl/ome`) — to
  view builds and click **New Build**.
- Nothing else for CI runs: the test is fully offline and the agent queue handles
  its own kube/docker access. (You do **not** need cluster kubeconfig to trigger a
  CI run — that's only for the by-hand path in the operator runbook.)

## Trigger an ad-hoc run

Buildkite → `msl/ome` → **New Build**. Set **Branch** (and Commit/Message) to
what you want to test. The full set of named jobs runs by default. To retarget
the run, add an environment variable:

```
GKE_KIND_QUEUE=gke-tpu-v7x
```

### Re-run just a failed job

You don't need a new build: on the finished build, open the red named job and
click **Retry**. It re-runs that job's file group only.

## The knobs

| Env var | Read at | Default | Meaning |
|---|---|---|---|
| `GKE_KIND_ENABLED` | pipeline upload | `1` | Emit the group. Set to `0` only as a kill switch. |
| `GKE_KIND_QUEUE` | pipeline upload | `gke-tpu-v7x` | Agent queue for the named jobs. |
| `OME_IMAGE_REBUILD` | pipeline upload | `0` | `1` makes every image step rebuild instead of reusing the image published for the same inputs (see "Image reuse" in README-gke-kind.md). |

## Reading results

- **PR check**: the `integration-test-kind-gke` commit status is the rollup.
  Buildkite reports it green only if **all** named jobs pass.
- **Which job ran my spec?** The job label names the behavior family and its log
  starts with the exact `-ginkgo.focus-file` selection.
- **A green `lifecycle_basic` looks like**:
  `PASS: TestOMENativeCoordinationKindController` /
  `ok  …/omenative_coordination_kind`.

## Local alternative (no CI)

You don't have to use CI to run these specs. On a machine with Docker + `kind`:

```sh
make test-kind                               # full suite (~30-60 min)
GINKGO_FOCUS='RecreatePod' make test-kind    # narrow by It-string fragment
make test-kind-clean                         # delete leftover KIND clusters
```

Note the difference: local `GINKGO_FOCUS` matches **It-text** (`-ginkgo.focus`),
while CI's `OME_KIND_FOCUS` matches **file paths** (`-ginkgo.focus-file`). See the
"Integration Tests" section of `CLAUDE.md` for the local requirements.

## Troubleshooting

| Symptom in the job log | Likely cause / fix |
|---|---|
| `FATAL: no reachable Docker daemon after ~120s` | The dind sidecar didn't come up. Retry the job; if it persists, the queue's pod-spec-patch lost the privileged `dind` container (operator runbook). |
| Jobs stuck in `pending` | Not enough capacity on the queue. Wait for earlier named jobs to release their clusters, or retarget the build to a larger queue. |
| `Index … ome.io/omenative-pod does not exist` (or similar harness gap) | KIND harness drift — the test setup is missing wiring `cmd/manager` has. File a small fix PR (precedent: the pod-index fix). |

## See also

- `README-gke-kind.md` — operator runbook: standing up / changing the queue.
- `run_kind_tests_gke.sh` — the offline named-group runner.
- `dockerfiles/kind-ci.Dockerfile` — what's baked into the offline image.
- `CLAUDE.md` → "Integration Tests" — local `make test-kind` details.
