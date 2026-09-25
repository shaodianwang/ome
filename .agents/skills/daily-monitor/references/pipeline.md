# The daily RC pipeline, stage by stage

Everything here is read from `.buildkite/generate_pipeline.py`,
`.buildkite/generate_daily_pipeline.py` and the `run_daily_*.sh` /
`daily_*.py` scripts next to them. When a detail below disagrees with those
files, the files win — re-read them.

## Selection

`generate_pipeline.py` reads `OME_PIPELINE_MODE` from the build environment.
`daily` loads `generate_daily_pipeline.py`; `standard` or unset renders the
normal CI/CD pipeline. Any other value exits 2 (`invalid OME_PIPELINE_MODE`).
There is no schedule definition in the repository; the generator's own
comment states the pipeline has no nightly schedule and that `daily` mode is
the RC promotion pipeline. The trigger (schedule or API) lives in Buildkite's
pipeline settings.

Two shapes exist:

| Mode | Trigger env | Steps |
|---|---|---|
| full | `OME_PIPELINE_MODE=daily` | discovery -> three smokes -> GitOps PR |
| Kargo (smoke-only) | `OME_PIPELINE_MODE=daily` + `KARGO_BUILD=true` at an exact commit | three smokes only; the pipeline YAML carries `env: KARGO_BUILD: "true"` and `KARGO_BUILD_COMMIT: <sha8>` (first 8 chars of `BUILDKITE_COMMIT`, which must be 8-64 lowercase hex) |

## Recognising a daily build through the REST API

`GET .../builds/<n>` (and the list endpoint) return `env`, `meta_data`,
`source` and `jobs`. In order of reliability:

1. `env.OME_PIPELINE_MODE == "daily"` — set by whatever created the build.
2. Any job whose `step_key` is one of `daily-discovery`, `daily-rc-smoke`,
   `daily-rc-tpu-smoke`, `daily-rc-tpu-ome-scheduler-smoke`,
   `daily-gitops-pr`. Only the daily generator emits these keys.
3. Kargo mode: `env.KARGO_BUILD` truthy, or a daily build that has no
   `daily-discovery` and no `daily-gitops-pr` job.

`daily_build.py` applies exactly these rules (`daily_mode()`). Message text,
branch and `source` of the scheduled build are not defined anywhere in the
repository, so do not filter on them.

## Stages

| Stage | Step key | Script | Runs on | Timeout | Depends on |
|---|---|---|---|---|---|
| Discover candidate | `daily-discovery` | `run_daily_discover_candidate.sh` -> `daily_candidate.py` | CPU queue (`tw-cpu-x86`), docker plugin, secret `PROD_ECR_AUTH_TOKEN` | 15 min | - |
| RC CPU smoke | `daily-rc-smoke` | `run_daily_rc_smoke.sh` | GKE queue (`GKE_KIND_QUEUE`, default `gke-tpu-v7x`), kubernetes plugin, pod SA `DAILY_RC_SERVICE_ACCOUNT` (default `ome-rc-smoke`), image `DAILY_RC_IMAGE` (`kind-ci:latest`) | 45 min | discovery (full mode only) |
| RC TPU smoke, default scheduler | `daily-rc-tpu-smoke` | `run_daily_rc_tpu_smoke.sh` with fixture `.buildkite/fixtures/rc-tpu-smoke-isvc.yaml` | same as CPU smoke | 270 min | discovery (full mode only) |
| RC TPU smoke, ome-scheduler | `daily-rc-tpu-ome-scheduler-smoke` | same script; env `DAILY_RC_TPU_SMOKE_NAME=ome-rc-tpu-smoke-ome-scheduler`, `DAILY_RC_TPU_FIXTURE=.buildkite/fixtures/rc-tpu-smoke-ome-scheduler-isvc.yaml`, `DAILY_RC_TPU_EXPECTED_ENGINE_SCHEDULER=ome-scheduler`, `DAILY_RC_TPU_METADATA_KEY=daily-rc-tpu-ome-scheduler-smoke-b64` | same | 270 min | discovery (full mode only) |
| Open GitOps promotion PR | `daily-gitops-pr` | `run_daily_gitops_pr.sh` -> `daily_gitops.py`, `daily_changeset.py` | CPU queue, docker plugin, secret `GH_TOKEN` | 15 min | all three smokes; absent in Kargo mode |

None of the daily steps declares automatic retries. An agent or Docker blip
fails the step outright; the manual retry is the only recovery.

Each smoke has `concurrency: 1` in its own group (`ome/rc-daily-smoke`,
`ome/rc-daily-tpu-smoke`, `ome/rc-daily-tpu-ome-scheduler-smoke`) because
every run uses the same fixed fixture names in the RC namespace. A job in
state `limited` is waiting for another build's smoke, not failing.

## What each stage does and leaves behind

**Discovery.** Logs in to the production registry, lists the four release
charts (`ome-crd`, `ome-resources`, `ome-scheduler`, `ome-quota-manager`)
under the chart prefix, parses versions `1.0.0-<12-digit UTC ts>.<rev>` (rev
with or without a `g` prefix), takes the newest SHA present in all four charts
whose four runtime images (`ome-manager`, `ome-agent`, `ome-scheduler`,
`ome-quota-manager` tagged `<sha8>`) all resolve, and writes `candidate.json`
(`short_sha`, `charts`, `images`, `registry`, `discovered_at`). It also probes
read access to the GitOps repository (`git ls-remote`), recording the result
without failing. Outputs: build meta-data `daily-candidate-b64`; artifacts
`artifacts/daily-connectivity/{candidate.json,gitops-access.json,gitops-ls-remote.txt,gitops-ls-remote.err}`.
Discovery qualifies on charts and images only; it does not read the
`ome-release` marker.

**Every smoke** first builds a kubeconfig from the pod's projected
ServiceAccount token (`daily_rc_setup_kubeconfig`: temp dir, `chmod 600`,
context `daily-rc`, namespace preset), checks `kubectl auth can-i` for the
verbs it needs, then waits up to 900 s for the RC control plane to run the
candidate: `ome/ome-controller-manager` and `ome/ome-scheduler` images equal
the candidate's, `inferenceservice-config`'s `omeAgent.image` equals the
candidate's agent image, the `helm.sh/chart` labels match the candidate chart
versions (Kargo mode: end in `.g<sha8>` or `.<sha8>`), and both rollouts are
complete. Every smoke re-checks that identity before recording PASS and fails
immediately if RC changed release mid-run.

**CPU smoke** (namespace `DAILY_RC_SMOKE_NAMESPACE`, default `omenative-eval`;
fixture name `ome-rc-daily-smoke`): 4 Gi PVC (`dynamic-rwo`) + seed Job writing
a one-line `config.json`; BaseModel on `pvc://`; waits for the metadata Job
(label `models.ome/model-name=<name>`), asserts its `model-metadata` container
image equals the candidate agent image, waits for BaseModel `Ready`; then a
ServingRuntime (nginx runner, `schedulerName: ome-scheduler`) and a one-replica
OMENative InferenceService; asserts every pod's `schedulerName` is
`ome-scheduler`; waits pod Ready (10 min) and InferenceService Ready (5 min).
Cleans its fixtures in `finish()`. Output: meta-data `daily-rc-smoke-b64` (the
`record` lines: `candidate-chart`, `release-images`, `seed-job`, `ome-agent`,
`inferenceservice`, `release-images-final`, `result: PASS`). No artifacts.

**TPU smokes** (namespace `omenative-eval`, fixed): require the RC-managed
ServingRuntime `ome-rc-tpu-smoke` and configmap `gcs-rapid-reader`; apply the
fixture (one engine leader + one worker on a `2x2x2` partition, one router);
wait up to `DAILY_RC_TPU_READY_TIMEOUT_SECONDS` (14400) for two Ready engine
pods, all routers Ready and `readyReplicas=1` on the engine InferenceReplica;
then InferenceService Ready (2 min); then assert shape (2 pods, 1 leader,
1 worker, 4 TPU chips per `ome-container`), engine `schedulerName` equal to the
expected scheduler and router `schedulerName == default-scheduler`. Outputs:
meta-data `<METADATA_KEY>` (`prerequisites`, `release-images`, `started-at`,
`gang`, `time-to-ready-seconds`, `result: PASS`); artifacts under
`artifacts/<smoke-name>/`: `report.txt`, `monitor.log` (per-minute pod/replica
table), `final-state.log`, `events.log`, plus on failure `isvc-describe.log`,
`irep-describe.log` and one `<pod>.log` per pod.

**GitOps PR.** Clones the GitOps repository's base branch, pins the candidate
in three production cluster overlays with `daily_gitops.py`, exits 0 with
`argo-gitops main already pins OME release <sha>; no PR needed` when nothing
changes, otherwise pushes branch `automation/ome-rc-<sha8>` and opens or
refreshes a PR titled `chore(ome): promote RC-tested release <sha8>` whose body
lists chart versions, the OME commits since the currently pinned release, and
the validation performed. Outputs: meta-data `daily-gitops-pr-url`, annotation
context `daily-gitops-pr`. The pipeline never merges the PR.

## The feeder: what makes a commit discoverable

Only `main` builds of the standard pipeline publish. Per commit:

- `build-<image>` (dev registry, tags `latest` and `<sha8>`) and
  `mirror-<image>` (copy to the production registry) for every image in
  `IMAGES`; the release set is `ome-manager`, `ome-agent`, `ome-scheduler`,
  `ome-quota-manager`.
- `chart-<chart>` runs `push_chart.sh`: version `1.0.0-<UTC ts at package
  time>.g<sha8>`; image-stamping charts wait for their image build and mirror.
- `chart-ome-release` runs `push_release_marker.sh` after a `wait` barrier, so
  it exists only when every hard step before it passed. It re-verifies each
  release image in the production registry and pushes `ome-release`
  `1.0.0-<commit UTC ts>.g<sha8>` (chart annotation `ome.io/source-revision`).

`daily_build.py release <sha|build>` shows these stages for a commit. A commit
can be discoverable (charts + images green) while its test lanes or marker are
red; check the main build before trusting a candidate.

## Identity formats

| Artifact | Format |
|---|---|
| image tag | `<sha8>` (first 8 of the full commit) |
| component chart | `1.0.0-<YYYYMMDDHHMM>.g<sha8>` |
| release marker | `1.0.0-<commit YYYYMMDDHHMM>.g<sha8>` |
| promotion branch | `automation/ome-rc-<sha8>` |

## RC cluster facts used by the smokes

- Agent pods run inside the RC cluster (the GKE queue's agent stack); the
  kubeconfig is the pod's own ServiceAccount, never a developer credential.
- Pods there have no public egress; runtime images come from the production
  registry.
- Control plane namespace `ome` (`ome-controller-manager`, `ome-scheduler`,
  `inferenceservice-config`); fixtures live in `omenative-eval`.
- The mechanism that rolls a new release onto RC is outside this repository;
  the smokes only observe it (the 900 s convergence wait).
