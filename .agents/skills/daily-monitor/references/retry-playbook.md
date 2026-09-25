# Retry playbook and stop conditions

A retry is a tool for infrastructure failures. It never fixes a product
regression, a missing RC prerequisite, or a candidate the RC is not running.
Classify first (`daily_build.py classify <build>`), then act.

## Mechanics

```sh
S=.agents/skills/daily-monitor/scripts/daily_build.py
python3 $S retry <build> <job-id|step-key>      # PUT .../jobs/<id>/retry; prints the new job id
python3 $S watch <build>                         # wakes when the new attempt finishes
```

- The REST call is `PUT https://api.buildkite.com/v2/organizations/msl/pipelines/ome/builds/<n>/jobs/<job-id>/retry`;
  it returns the new job object. The original keeps `retried: true` and is
  hidden unless `?include_retried_jobs=true` (the script always asks for it).
- Only the newest attempt of a step can be retried; a retried job cannot be
  retried again.
- Passed jobs cannot be retried (no `permit_on_passed` on daily steps).
- Build meta-data survives retries: a retried smoke re-reads
  `daily-candidate-b64` and tests the same candidate.
- Jobs in `waiting_failed` become runnable again when the dependency they
  waited on is retried and passes.

## Never retry `daily-discovery` after a smoke ran

Discovery re-runs pick the newest complete release at that moment. If a smoke
already tested candidate A and discovery is retried into candidate B, the
GitOps PR step would promote B on evidence gathered for A. The script refuses
this unless `--force`. Retry discovery only while all three smokes are still
`waiting_failed`; otherwise start a new daily build.

## Signature table

Classes are `infra/*` (retry once), `env/*` (RC or CI environment; usually
wait or escalate), `release/*` and `trigger/*` (fix upstream), `product/*`
(open an issue, never retry), `needs-evidence/*` (read artifacts before
deciding).

| Signature (log or job field) | Class | Action |
|---|---|---|
| exit status 125, `failed to connect to the docker API` | infra/docker-daemon | retry once |
| exit status -1, abrupt log end, not canceled by a person | infra/agent-lost-or-canceled | retry once |
| `ecr_login_for: no ECR credential source produced a token` | infra/registry-auth | retry once; a second hit means the cluster secret is broken — escalate CI infra |
| `GH_TOKEN is required` | infra/secret-missing | escalate CI infra; no retry |
| `GitHub pull lookup failed with HTTP 5xx` / timeout | infra/github-api | retry once |
| `... failed with HTTP 401/403` | infra/github-api | token scope; escalate, no retry |
| job state `limited` for a long time | not a failure | another build holds the concurrency group; wait |
| `RC did not converge to the requested release within 900s` | env/rc-not-on-candidate | read the `expected ... / live ...` lines. Live newer than candidate: the RC moved on, a fresh discovery will follow it. Live older: RC has not synced; wait for RC, then retry the smoke |
| `new RC version detected while the smoke test was running` | env/rc-release-changed-midrun | wait until RC deployments are stable, retry once |
| `missing permission: <verb> <resource> in <ns>` | env/rbac | the smoke ServiceAccount lost a grant; escalate the RC owners, no retry |
| `configmap "gcs-rapid-reader" not found`, `servingruntime "ome-rc-tpu-smoke" not found` | env/rc-prerequisites | RC-managed prerequisites missing; escalate, no retry |
| `Insufficient google.com/tpu`, pods Pending with `FailedScheduling` | env/tpu-capacity | no free slice; wait, do not retry into an empty pool |
| `argo-gitops read access: unavailable` (discovery) | env/gitops-read-access | discovery still passes; expect the PR step to fail; escalate access |
| `no complete OME release has all required charts and images` | release/incomplete-publish | run `daily_build.py release <newest main commit>`; fix or retry the red `mirror-*`/`chart-*` step in the standard main build, then a new daily |
| `KARGO_BUILD requires BUILDKITE_COMMIT ...`, `KARGO_BUILD_COMMIT must ...` | trigger/misconfigured | fix the trigger; no retry |
| `RC candidate rollout is no longer healthy` | needs-evidence/rc-rollout-unhealthy | inspect `ome-controller-manager` / `ome-scheduler` pods in RC; crash loop on the candidate image is product |
| `timed out waiting for production readiness: engine=x/2 router=y/z ...` | needs-evidence/tpu-not-ready | download the smoke's artifacts; Pending + capacity = env, CrashLoopBackOff/Error = product, long PodInitializing = model fetch (env) |
| `BaseModel did not become Ready within 3 minutes` | needs-evidence/basemodel-not-ready | the metadata Job pod log is in the same output; failed download or parse = product unless storage failed |
| `timed out waiting for the condition` (kubectl wait) | needs-evidence/kubectl-wait-timeout | the describe output after it names the resource; classify from that |
| `ImagePullBackOff` / `ErrImagePull` on a smoke pod | needs-evidence/image-pull | confirm the candidate tags exist in the production registry; absent = release gap, present = env |
| `unexpected TPU gang shape`, `unexpected engine scheduler`, `unexpected router scheduler` | product/gang-shape | open an issue |
| `OME did not create the BaseModel metadata Job within 3 minutes` | product/basemodel-controller | open an issue |
| `metadata Job image mismatch` | product/agent-image-mismatch | open an issue |
| `OME did not create an InferenceService pod within 5 minutes` | product/isvc-controller | open an issue |
| `<pod> uses scheduler X, expected ome-scheduler` | product/scheduler-name | open an issue |
| `TPU smoke fixture does not exist` | product/fixture-missing | open an issue (repository change) |
| `unexpected GitOps paths changed` | product/gitops-layout-drift | open an issue; the pin script and the GitOps layout disagree |
| `argo-gitops main already pins OME release ...; no PR needed` | pass/already-pinned | nothing to do |

## Root-cause discipline

The first marker in a log is rarely the cause. For every failure:

1. Read how the log ends: abrupt end with exit -1 is an external kill (step
   timeout, agent loss); a clean error line is the script's own assertion.
2. Note runtime against the stage timeout (45 / 270 / 15 min) — a
   `timed_out` state means the wait inside the script never returned.
3. For smokes, the `finish()` trap prints the fixture state, describes and pod
   logs after the failure; the TPU smokes also upload them as artifacts.
4. Compare with the previous daily build: the same signature two days in a
   row on different agents is not a flake.

## Stop conditions

Stop watching and hand off when any of these holds:

- Build state is `passed`, `failed`, `canceled` or `blocked`.
- Meta-data has `daily-gitops-pr-url`: the run succeeded; the PR is for a
  person to review and merge.
- Kargo mode and all three smokes are terminal.
- A second retry of the same failure would be needed. One manual retry per
  infra failure; if it fails again, classify it as env or product.
- Any `product/*` class: no retry, open or update an issue and stop.
- `env/*` that needs an RC or CI change: escalate with the evidence and stop.
- The watch window (`--max-hours`, default 9) elapsed: relaunch deliberately
  or hand off.

## Starting a fresh daily build

Prefer the Buildkite UI "New Build" on `main` with environment
`OME_PIPELINE_MODE=daily` (add `KARGO_BUILD=true` and an exact commit for a
smoke-only run). The API equivalent is
`POST .../builds` with `{"commit":"HEAD","branch":"main","message":"daily RC (manual)","env":{"OME_PIPELINE_MODE":"daily"}}`.
Both rely on the pipeline's upload step passing build env to the generator,
which the repository documents for `KARGO_BUILD` but which is configured in
Buildkite, not in the tree. Never fire a second full build while another
full build is mid-smoke unless you accept that the smokes serialize on the
concurrency groups.
