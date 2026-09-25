---
name: daily-monitor
description: "Watch one OME daily release-candidate (RC) Buildkite build end to end — candidate discovery, the three RC smokes, the GitOps promotion PR — plus the main-build chart push and release marker that feed it; per-stage verdicts, cleaned logs and artifacts, a retry playbook and hard stop conditions. Use on \"monitor the daily\", \"watch daily build N\", \"daily build failed\", \"check the RC promotion\", \"did the release marker publish\". Needs a Buildkite token in $BUILDKITE_API_TOKEN or ~/.buildkite/token."
---

# daily-monitor — watch an OME daily RC build

The daily pipeline (Buildkite org `msl`, pipeline `ome`, build env
`OME_PIPELINE_MODE=daily`) finds the newest complete OME release in the
production registry, proves the RC cluster is running it, smokes it three
ways, and opens a GitOps PR that a person reviews and merges. This skill
watches one such build, separates infrastructure noise from real failures,
retries only what a retry can fix, and stops at the PR.

Details live next to this file: `references/pipeline.md` (every stage, key,
script, output), `references/retry-playbook.md` (signatures, actions, stop
conditions), `references/rc-cluster.md` (read-only cluster checks under
kubeconfig hygiene).

## Quick start

```sh
S=.agents/skills/daily-monitor/scripts/daily_build.py     # stdlib Python, no installs
python3 $S find --days 2                 # daily builds (full and Kargo), newest first
python3 $S status <build>                # per-stage verdicts + decoded reports + PR link
python3 $S classify <build>              # failed jobs matched against known signatures
python3 $S log <build> <step-key|job-id> --tail 120       # cleaned log (saved to scratch)
python3 $S artifacts <build> daily-rc-tpu-smoke --download ~/.cache/ome-agent.noindex/scratch/daily-monitor
python3 $S release <sha-or-main-build>   # did main publish charts, images and the marker?
python3 $S watch <build>                 # run with Bash run_in_background; exits on "WAKE: <reason>"
python3 $S retry <build> <step-key|job-id>                 # explicit, one job, prints the new job id
python3 $S summary <build> --out ~/.cache/ome-agent.noindex/scratch/daily-monitor/b<build>.md
```

Token: `$BUILDKITE_API_TOKEN` or `~/.buildkite/token` (mode 600); `read_builds`
for everything, `write_builds` for `retry`. The script fails clearly without
one. Scratch, state and downloads go to
`~/.cache/ome-agent.noindex/scratch/daily-monitor/`, never under the repo.
Kill a watcher with `pkill -f "daily_build[.]py watch"` (bracket the dot, or
the pattern matches your own shell).

## What a daily build is

| Stage | Step key | Runs | Timeout |
|---|---|---|---|
| Discover candidate | `daily-discovery` | `run_daily_discover_candidate.sh` -> `daily_candidate.py`: newest SHA with all four release charts and four images in the production registry | 15 min |
| RC CPU smoke | `daily-rc-smoke` | `run_daily_rc_smoke.sh`: PVC-seeded BaseModel -> metadata Job (candidate agent image) -> one OMENative replica through `ome-scheduler` | 45 min |
| RC TPU smoke, default scheduler | `daily-rc-tpu-smoke` | `run_daily_rc_tpu_smoke.sh`: one two-node TPU gang + router, fixture `rc-tpu-smoke-isvc.yaml` | 270 min |
| RC TPU smoke, ome-scheduler | `daily-rc-tpu-ome-scheduler-smoke` | same script, fixture `rc-tpu-smoke-ome-scheduler-isvc.yaml`, engine scheduler must be `ome-scheduler` | 270 min |
| Open GitOps promotion PR | `daily-gitops-pr` | `run_daily_gitops_pr.sh`: pins the candidate in the production overlays, opens/refreshes PR `chore(ome): promote RC-tested release <sha8>` | 15 min |

The smokes run as agent pods inside the RC cluster on the GKE queue
(`GKE_KIND_QUEUE`, default `gke-tpu-v7x`) with ServiceAccount
`DAILY_RC_SERVICE_ACCOUNT` (`ome-rc-smoke`) and image `DAILY_RC_IMAGE`; each
first waits up to 900 s for RC to run the candidate (manager, scheduler and
agent images plus `ome-resources`/`ome-scheduler` chart labels), and each
fails at once if RC changes release mid-run. Discovery and the PR step run on
the CPU queue under the docker plugin. No daily step has automatic retries.
Each smoke holds a `concurrency: 1` group, so two daily builds serialize; a
job in state `limited` is queued, not broken.

**Kargo mode** (`KARGO_BUILD=true` at an exact commit): discovery and the PR
step are not emitted; the smokes take the candidate from `KARGO_BUILD_COMMIT`
(first 8 hex of `BUILDKITE_COMMIT`) and end the build. Kargo owns promotion.

### Recognising a daily build in the API

1. Build `env.OME_PIPELINE_MODE == "daily"`.
2. Otherwise any job with a `daily-*` step key above; only the daily generator
   emits them.
3. Kargo mode: `env.KARGO_BUILD` truthy, or no `daily-discovery` and no
   `daily-gitops-pr` job.

The schedule (time, message, branch, `source`) is configured in Buildkite,
not in the repository, so never filter on message text. `find` applies the
three rules; `--pipeline` overrides the slug if daily builds ever move.

## The loop

1. `find`, pick the build (or take the number you were given); `status`.
2. If anything is already red, triage it before starting the watcher: the
   watcher snapshots existing failures as known and wakes only on new ones.
3. `watch` in the background. It exits 0 printing one of:
   `WAKE: new failed job(s)`, `WAKE: retry fired on an untriaged job`,
   `WAKE: watched retry attempt(s) finished`, `WAKE: promotion PR opened`,
   `WAKE: build settled: <state>`, or the window elapsed (`--max-hours`, 9).
4. On a wake: `classify`, then `log --grep` / `artifacts` for the evidence,
   decide with the playbook, act, relaunch the watcher.
5. On settle or PR: final `status`, write the summary, hand off.

Read a stage from what it publishes before opening the log:

- `status` decodes build meta-data: `daily-candidate-b64` (candidate JSON),
  the smoke reports (`daily-rc-smoke-b64`, `daily-rc-tpu-smoke-b64`,
  `daily-rc-tpu-ome-scheduler-smoke-b64`: `release-images`, `gang`,
  `time-to-ready-seconds`, `result: PASS`) and `daily-gitops-pr-url`.
- TPU smokes upload `artifacts/<smoke-name>/` (`report.txt`, `monitor.log`,
  `final-state.log`, `events.log`, on failure describes and pod logs);
  discovery uploads `artifacts/daily-connectivity/`. The CPU smoke has no
  artifacts; its `finish()` trap prints state, describes and pod logs into
  the job log.
- Logs: `GET .../jobs/<id>/log.txt` with `Accept: text/plain`; the script
  strips ANSI and `_bk;t=` timestamp escapes and parses job JSON with
  `strict=False` (payloads carry raw control characters).

## Before the daily: the feeder

A commit is discoverable when its `main` build published all four charts
(`chart-ome-crd`, `chart-ome-resources`, `chart-ome-scheduler`,
`chart-ome-quota-manager`, version `1.0.0-<UTC ts>.g<sha8>`) and mirrored all
four images (`mirror-ome-manager`, `mirror-ome-agent`, `mirror-ome-scheduler`,
`mirror-ome-quota-manager`, tag `<sha8>`). `chart-ome-release` runs after a
`wait` barrier and pushes the `ome-release` marker
(`1.0.0-<commit ts>.g<sha8>`) only when every hard step passed. Discovery does
not consult the marker: a commit with a red test lane can still be the
candidate. `release <sha>` shows all of this for one commit; `docs/RELEASE.md`
explains the registries and version formats.

## Retry playbook (short form)

| Signal | Class | Do |
|---|---|---|
| exit 125 / `failed to connect to the docker API` | infra/docker-daemon | retry once |
| exit -1, abrupt log end | infra/agent-lost | retry once unless a person canceled |
| `ecr_login_for: no ECR credential source produced a token` | infra/registry-auth | retry once; twice = escalate CI infra |
| `GH_TOKEN is required`, HTTP 401/403 from GitHub | infra/secret | escalate; no retry |
| `RC did not converge to the requested release` | env/rc-not-on-candidate | read expected vs live; wait for RC, then retry the smoke |
| `new RC version detected while the smoke test was running` | env/rc-release-changed | retry when RC is stable |
| `missing permission`, prerequisites `not found` | env/rbac, env/rc-prerequisites | escalate RC owners; no retry |
| `Insufficient google.com/tpu`, Pending gang | env/tpu-capacity | wait; do not retry into an empty pool |
| `no complete OME release has all required charts and images` | release/incomplete-publish | fix the main build's red publish step; new daily |
| any script assertion on a converged RC (`unexpected TPU gang shape`, `did not create`, `uses scheduler X, expected ome-scheduler`, `image mismatch`) | product/* | open an issue (see the daily-triage skill); never retry |
| `timed out waiting for production readiness`, `timed out waiting for the condition` | needs-evidence | artifacts and describes decide: capacity is env, crash loops are product |

Never retry `daily-discovery` once a smoke has run: a re-discovered newer
candidate would be promoted on smokes that tested the old one (the script
refuses without `--force`). Start a fresh daily build instead: Buildkite "New
Build" on `main` with env `OME_PIPELINE_MODE=daily`.

## Stop conditions

Hand off when: the build is `passed`/`failed`/`canceled`/`blocked`; meta-data
has `daily-gitops-pr-url` (the PR is for a person to merge); a Kargo build's
three smokes are terminal; a second retry of the same failure would be
needed; any `product/*` class is established; an `env/*` fix needs someone
else; or the watch window elapsed and nobody asked for another.

## RC cluster checks

Only when logs, meta-data and artifacts do not answer the question, and only
read-only. The smoke scripts build a dedicated temp kubeconfig (`chmod 600`,
exported for the step, deleted at exit); do the same with your credentials:
`RC_KUBECONFIG="$(mktemp -t ome-rc-XXXXXX.kubeconfig)"`, obtain credentials
into that file, pass `--kubeconfig "$RC_KUBECONFIG"` on every command, delete
it afterwards. Never `kubectl config use-context`, never merge into
`~/.kube/config`, never run a local-cluster tool with it exported. Commands
and the do-not list are in `references/rc-cluster.md`.

## Never

- Merge, approve or edit the GitOps promotion PR, or bump the RC release.
- Apply or delete anything in the RC namespaces `omenative-eval` or `ome`.
- Run `push_chart.sh`, `push_release_marker.sh` or `mirror_image.sh` outside
  CI; they publish to the registries.
- Retry a failure you have not classified, or the same failure twice.
- Put a token, kubeconfig or cluster credential in a summary, issue or file.

## Open questions

- The daily schedule's cron time, message and branch are Buildkite settings
  outside the repository; identification here relies on `env` and step keys.
- Manual daily triggers assume the pipeline's upload step passes build env to
  the generator (the repository documents this for `KARGO_BUILD`, not for a
  scheduled run).
- What rolls a new release onto RC (and how long it lags the registry) is
  outside the repository; it sets how long `RC did not converge` persists.
- Whether scheduled builds expose `env` in the REST build object is expected
  but unconfirmed here; the step-key rule is the fallback.
- The RC cluster's GCP project and region for `gcloud container clusters
  get-credentials` are not recorded in the repository.
