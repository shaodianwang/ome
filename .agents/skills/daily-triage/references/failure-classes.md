# Infrastructure failure or product regression?

The daily pipeline exercises three things at once: Buildkite agents and
secrets (CI infrastructure), the RC cluster and the release it runs (the
environment), and OME itself (the product). A verdict names which one broke
and shows the line that proves it.

## Decision rules

1. **Did the stage's own assertion fire, or did something around it die?**
   A script assertion (`echo ... >&2; exit 1` in the `run_daily_*.sh`
   scripts) is evidence about the environment or the product. An exit status
   of 125 or -1, a `timed_out` state with no assertion line, or a log that
   ends mid-command is infrastructure.
2. **Was the RC running the candidate at the time?** Every smoke prints
   `release-images:` after the convergence wait and re-checks identity before
   PASS. If the RC was not on the candidate (`RC did not converge`, `new RC
   version detected`), nothing after that line says anything about OME.
3. **Did OME do what the script asked?** The CPU smoke asserts the controller
   created the metadata Job, used the configured agent image, marked the
   BaseModel Ready, created the InferenceService pod and honoured
   `schedulerName`. The TPU smokes assert gang shape and scheduler names. A
   failed assertion there, on a converged RC, with no capacity or storage
   symptom in the describe output, is a product regression.
4. **Would the same failure happen on yesterday's candidate?** Compare with
   the previous daily build. Same signature across two candidates with
   different agents is either environment or a regression that landed
   earlier; a signature unique to this candidate points at its commits
   (`daily_build.py release <sha>` links the main build; the GitOps PR body
   lists the commits since the pinned release).
5. **Unknown is a verdict.** When the evidence does not settle it, write
   `needs-evidence` with what is missing; do not guess "flake".

## Class reference

Classes are the ones `daily_build.py classify` emits. The stage column says
where the string originates.

### Infrastructure (Buildkite agents, secrets, GitHub API)

| Signal | Stage | Meaning | Evidence to attach |
|---|---|---|---|
| exit 125, `failed to connect to the docker API` | discovery, gitops-pr (docker plugin steps) | agent-side Docker daemon failure | job link, exit status |
| exit -1 / abrupt end | any | agent lost or step killed | last lines, `finished_at`, timeout |
| `ecr_login_for: no ECR credential source produced a token` | discovery | registry credential missing on the agent | the `ecr_login_for` lines |
| `GH_TOKEN is required`, `... failed with HTTP 401/403` | gitops-pr | GitHub token missing or under-scoped | the HTTP code line |
| `... failed with HTTP 5xx` | gitops-pr | transient GitHub API failure | the HTTP code line |

### Environment (RC cluster, its release, capacity, prerequisites)

| Signal | Stage | Meaning | Evidence to attach |
|---|---|---|---|
| `RC did not converge to the requested release within 900s` + `expected/live` lines | any smoke | RC not on the candidate | both expected and live lines |
| `new RC version detected while the smoke test was running` | any smoke | RC changed release mid-run | expected/live lines, timestamp |
| `RC candidate rollout is no longer healthy` | any smoke | manager or scheduler rollout regressed mid-run | `rollouts complete:` line; control-plane pod state if inspected |
| `missing permission: <verb> <resource> in <ns>` | any smoke | ServiceAccount RBAC changed | the line |
| `configmap "gcs-rapid-reader" not found`, `servingruntime "ome-rc-tpu-smoke" not found` | tpu smokes | RC-managed prerequisites absent | the NotFound line |
| `Insufficient google.com/tpu`, `FailedScheduling`, pods Pending in `final-state.log` | tpu smokes | no free 2x2x2 slice | `events.log` excerpt, `monitor.log` last table |
| PVC Pending, seed Job not complete | cpu smoke | storage class `dynamic-rwo` did not provision | describe output |
| `argo-gitops read access: unavailable` | discovery | GitOps repo unreachable from the agent | `gitops-access.json` artifact |
| `ImagePullBackOff` with the candidate tag present in the registry | any smoke | node cannot pull from the registry | describe output, `crane manifest` result |

### Release and trigger (upstream of the daily)

| Signal | Stage | Meaning | Evidence to attach |
|---|---|---|---|
| `no complete OME release has all required charts and images` | discovery | newest main commits have a red `mirror-*` or `chart-*` step, or none published yet | `daily_build.py release` output for the newest main commit |
| `ImagePullBackOff` with the candidate tag absent from the registry | any smoke | a chart published ahead of its image | `crane manifest` result, main build links |
| `KARGO_BUILD requires BUILDKITE_COMMIT ...` | pipeline upload | Kargo trigger passed a bad commit | build env |

### Product (OME behaviour on a converged RC)

| Signal | Stage | Component under suspicion |
|---|---|---|
| `OME did not create the BaseModel metadata Job within 3 minutes` | cpu smoke | BaseModel controller |
| `metadata Job image mismatch: expected <agent> got <other>` | cpu smoke | BaseModel controller image rendering |
| `BaseModel did not become Ready within 3 minutes` with a completed Job | cpu smoke | BaseModel status handling |
| `OME did not create an InferenceService pod within 5 minutes` | cpu smoke | InferenceService / OMENative controller |
| `<pod> uses scheduler <x>, expected ome-scheduler` | cpu smoke | runtime `schedulerName` propagation |
| InferenceService never Ready while pods are Ready | cpu or tpu smoke | status projection |
| `unexpected TPU gang shape: pods=.. leaders=.. workers=.. tpu-containers=..` | tpu smokes | gang rendering (leader/worker, TPU requests) |
| `unexpected engine scheduler` / `unexpected router scheduler` | tpu smokes | scheduler selection per component |
| Engine pods CrashLoopBackOff on the candidate image with a clean environment | tpu smokes | the serving runtime or the controller's pod spec; read the pod log first |
| `unexpected GitOps paths changed` | gitops-pr | `daily_gitops.py` versus the GitOps layout |
| `TPU smoke fixture does not exist` | tpu smokes | repository fixture path |

A control-plane pod (`ome-controller-manager`, `ome-scheduler`) crash looping
on the candidate image is the most serious product class: it means the
release itself is not deployable. Say so in the first line of the summary.

## Evidence standard

Every disposition in the summary carries, in this order: the build and job
link, the exact log line (quoted, with its line number from `daily_build.py
log --grep`), the artifact or describe excerpt that supports the class, and
the comparison with the previous daily build. A retry outcome is evidence
too: link the new attempt and state its result.
