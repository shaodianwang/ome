# User guide — the GKE KIND suite (`integration-test-kind-gke`)

For **OME developers**: how to read the GKE-backed OMENative KIND CI signal and
trigger ad-hoc runs. This is the *consumer* side. To stand up or change the agent
queue itself, see the operator runbook `README-gke-kind.md`.

## What it is (30-second version)

The OMENative deployment mode is validated by the KIND coordination suite
(`tests/integration/singlecluster/controller/omenative_coordination_kind/`, ~72
spec files needing a real kubelet: rollout coordination, drain races, Sequential
handoff, per-revision Service convergence, in-place patches). It can't run on the
TW dind (read-only `/proc/sys/net` → kind can't network), so it runs on a
**GKE agent queue** where kind boots, **sharded across parallel pods**.

- Buildkite step key: **`integration-test-kind-gke`**.
- Commit-status context (what shows on the PR): **`integration-test-kind-gke`**.
- Runner: `.buildkite/run_kind_tests_gke.sh` (offline; loads baked images into a
  privileged dind sidecar, builds xet, shards the specs, runs `go test`).
- Each build fans into **N pods** (`GKE_KIND_PARALLELISM`, default 8). Pod *i*
  runs a disjoint ~9-file subset (spec files sorted, file *k* → shard *k mod N*).

## When it runs

The step is **gated** — `generate_pipeline.py` emits it only when
`GKE_KIND_ENABLED` is truthy (a step targeting a queue with no agents would hang
the whole build). While it's being rolled out it also ships **`soft_fail: true`**,
so a red shard does **not** block merges yet — treat it as advisory until the gate
is flipped to hard-fail (tracked in the operator runbook).

So on a normal PR you'll see `integration-test-kind-gke` only once the pipeline
env has `GKE_KIND_ENABLED=1` set (Pipeline Settings → Environment). Until then,
run it ad-hoc (below).

## Access you need

- Buildkite access to the **`msl/ome`** pipeline (`buildkite.com/msl/ome`) — to
  view builds and click **New Build**.
- Nothing else for CI runs: the test is fully offline and the agent queue handles
  its own kube/docker access. (You do **not** need cluster kubeconfig to trigger a
  CI run — that's only for the by-hand path in the operator runbook.)

## Trigger an ad-hoc run

Buildkite → `msl/ome` → **New Build**. Set **Branch** (and Commit/Message) to
what you want to test, then add **Environment Variables** (one `KEY=VALUE` per
line). The env is read both at pipeline-upload time (`GKE_KIND_ENABLED`,
`GKE_KIND_PARALLELISM`, `GKE_KIND_QUEUE`) and inside the job pod (`OME_KIND_FOCUS`).

### Run the whole suite (8-way sharded)

```
GKE_KIND_ENABLED=1
GKE_KIND_QUEUE=gke-tpu-v7x
GKE_KIND_PARALLELISM=8
```

(no `OME_KIND_FOCUS`) → 8 jobs, each a disjoint 9-file shard.

### Run a single spec / a focused subset

```
GKE_KIND_ENABLED=1
GKE_KIND_QUEUE=gke-tpu-v7x
GKE_KIND_PARALLELISM=1
OME_KIND_FOCUS=lifecycle_basic
```

`OME_KIND_FOCUS` is passed straight to `-ginkgo.focus-file` and is a **regex over
the spec file path**, so it skips the shard partition and runs only matching
files:

| `OME_KIND_FOCUS` value | Runs |
|---|---|
| `lifecycle_basic` | `lifecycle_basic_kind_test.go` |
| `sequential_` | every `sequential_*_kind_test.go` |
| `pd_` | the PD-disaggregated specs (`pd_*`) |
| `lifecycle` | `lifecycle_basic`, `lifecycle`, **and** `cleanup_lifecycle` (substring!) |

Keep `GKE_KIND_PARALLELISM=1` with a focus — otherwise every one of the N pods
runs the same focused set.

### Re-run just a failed shard

You don't need a new build: on the finished build, open the red parallel job
(e.g. `integration-test-kind-gke 4/8`) and click **Retry**. It re-runs that one
shard's file subset only.

## The knobs

| Env var | Read at | Default | Meaning |
|---|---|---|---|
| `GKE_KIND_ENABLED` | pipeline upload | *(unset)* | Emit the step at all. Must be truthy (`1`/`true`/`yes`/`on`). |
| `GKE_KIND_QUEUE` | pipeline upload | `gke-kind-x86` | Which agent queue to run on. Stopgap: `gke-tpu-v7x`. |
| `GKE_KIND_PARALLELISM` | pipeline upload | `8` | Number of shard pods. |
| `OME_KIND_FOCUS` | job runtime | *(unset)* | `-ginkgo.focus-file` regex; skips sharding, runs only matching files. |

## Reading results

- **PR check**: the `integration-test-kind-gke` commit status is the rollup. With
  `parallelism`, Buildkite reports the step green only if **all** shards pass.
- **Which shard ran my spec?** Each job logs `--- shard J/N spec files: … ---`
  near the top listing its files; the failing spec's file tells you the shard.
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

| Symptom in the shard log | Likely cause / fix |
|---|---|
| `FATAL: no reachable Docker daemon after ~120s` | The dind sidecar didn't come up. Retry the job; if it persists, the queue's pod-spec-patch lost the privileged `dind` container (operator runbook). |
| `go: … : GOPROXY=off` / cargo offline error | A dependency wasn't baked into `kind-ci`. The image bakes caches at build time from `go.mod`/`Cargo.lock`; a dep change needs a fresh `kind-ci` (rebuilt on the next `main` build). Rebase onto a `main` that has the rebuilt image. |
| Jobs stuck in `pending` | Not enough capacity on the queue (the `gke-tpu-v7x` stopgap is CPU-limited). Lower `GKE_KIND_PARALLELISM` (e.g. `4`) or wait. |
| `Index … ome.io/omenative-pod does not exist` (or similar harness gap) | KIND harness drift — the test setup is missing wiring `cmd/manager` has. File a small fix PR (precedent: the pod-index fix). |

## See also

- `README-gke-kind.md` — operator runbook: standing up / changing the queue.
- `run_kind_tests_gke.sh` — the offline sharded runner (source of truth for knobs).
- `dockerfiles/kind-ci.Dockerfile` — what's baked into the offline image.
- `CLAUDE.md` → "Integration Tests" — local `make test-kind` details.
