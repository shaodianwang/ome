# Buildkite API, logs, artifacts, agent-pod evidence

`scripts/ci_build.py` wraps all of this; use the raw calls when you need a
field the helper does not print.

## Token

- Source, in order: `$BUILDKITE_API_TOKEN`, then `~/.buildkite/token` (mode
  600). Nothing else; never a literal in a file or a command line that ends up
  in a transcript.
- Scopes: `read_builds`, `read_build_logs`, `read_artifacts`; `write_builds`
  for `retry`. A `401` means the token is stale or lacks the scope: create a
  new user API token for the `msl` organization in Buildkite and store it:
  ```sh
  umask 077 && mkdir -p ~/.buildkite && printf '%s\n' "$NEW_TOKEN" > ~/.buildkite/token
  ```

## Endpoints

Base: `https://api.buildkite.com/v2/organizations/msl/pipelines/ome`. Header:
`Authorization: Bearer <token>`.

| Purpose | Call |
|---|---|
| builds for a commit | `GET /builds?commit=<sha>&per_page=20&include_retried_jobs=true` |
| builds for a branch | `GET /builds?branch=<name>&per_page=5` |
| one build with jobs | `GET /builds/<n>?include_retried_jobs=true` |
| job log | `GET /builds/<n>/jobs/<job-id>/log.txt` with `Accept: text/plain` |
| job artifacts | `GET /builds/<n>/jobs/<job-id>/artifacts`; each has a `download_url` that redirects to storage (fetch the redirect target without the bearer header) |
| build artifacts | `GET /builds/<n>/artifacts` |
| retry a job | `PUT /builds/<n>/jobs/<job-id>/retry` (returns the new job; the old one gets `retried: true`, `retried_in_job_id`) |
| annotations | `GET /builds/<n>/annotations` |

Useful job fields: `type` (`script` for real steps; `waiter`, `manual`,
`trigger` otherwise), `step_key`, `name`, `state`, `exit_status`,
`signal_reason`, `soft_failed`, `retried`, `retried_in_job_id`,
`retries_count`, `web_url`, `raw_log_url`, `agent.name`, `agent_query_rules`
(the queue).

Build states: `running`, `failing` (still running, something red), then the
terminal `passed`, `failed`, `canceled`, `blocked`, `skipped`, `not_run`.

## GitHub side

```sh
gh pr checks <n> --repo mslsrc/ome
gh api repos/mslsrc/ome/commits/<sha>/status --jq '.statuses[] | "\(.context)\t\(.state)\t\(.target_url)"'
```

Each context is a step key (or a KIND group key); `buildkite/ome` is the
rollup whose `target_url` is `https://buildkite.com/msl/ome/builds/<n>`.

## Parsing and cleaning

- Job JSON can contain raw control characters. Parse with Python
  `json.loads(text, strict=False)`; `jq` and strict parsers fail on it.
- Logs carry `_bk;t=<ms>` timestamp escapes (`ESC _bk;t=... BEL`), ANSI colour
  sequences and OSC hyperlinks. Strip before grepping:
  ```sh
  perl -pe 's/\x1b_bk;t=\d+\x07//g; s/_bk;t=\d+//g; s/\x1b\][^\x07]*\x07//g; s/\x1b\[[0-9;?]*[ -\/]*[@-~]//g' raw.log > clean.log
  ```
- Save logs under `~/.cache/ome-agent.noindex/scratch/fix-ci/`, never in the
  repository; `/tmp` is fine for a single command but is reaped.

## Artifacts worth knowing

| Step | Artifact |
|---|---|
| `test` | `.buildkite/artifacts/omenative-kind-test.tgz`, `multicluster-kind-test.tgz`, `omenative-upgrade-kind-test.tgz`, `kind` (the exact-commit bundles the GKE lanes run) |
| `omenative-e2e-*` | the results archive (`<results-root>.tgz`) with per-case logs, summary and junit |
| `chart-<chart>` | `.buildkite/artifacts/chart-versions/<chart>` |

## Agent-pod evidence (operators only)

The GKE KIND and eval jobs run in pods created by an agent-stack-k8s
controller; the every-branch lanes run on the container queue, which is not
reachable with `kubectl`. Pod evidence is only for a job that died before its
log said anything useful (image pull, secret mount, eviction, OOM). It needs
operator credentials for that cluster; regular CI triage never does. Always
name the kubeconfig and context explicitly, never switch the ambient one:

```sh
KCFG=<path to the operator kubeconfig>; CTX=<context name>; NS=<agent namespace>
kubectl --kubeconfig "$KCFG" --context "$CTX" -n "$NS" get pods --show-labels | grep <job-id-prefix>
kubectl --kubeconfig "$KCFG" --context "$CTX" -n "$NS" describe pod <pod>
kubectl --kubeconfig "$KCFG" --context "$CTX" -n "$NS" logs <pod> -c container-0    # the step command
kubectl --kubeconfig "$KCFG" --context "$CTX" -n "$NS" logs <pod> -c dind           # the Docker sidecar
```

The queue, images, resource requests and sidecar layout are documented in
`.buildkite/README-gke-kind.md`; the developer-facing view is
`.buildkite/README-gke-kind-userguide.md`.
