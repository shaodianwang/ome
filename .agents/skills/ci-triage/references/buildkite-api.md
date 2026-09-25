# Buildkite API — the calls this skill makes

`ci_build.py` and `main_builds.py` wrap these; use the raw calls when a helper
is missing or you need a field it does not print. The complete endpoint,
artifact and log-cleaning reference is
`.agents/skills/fix-ci/references/buildkite-api.md`.

Base: `https://api.buildkite.com/v2/organizations/msl/pipelines/ome`.
Token: `$BUILDKITE_API_TOKEN` or `~/.buildkite/token` (mode 600). Pass it to
curl through a config on stdin so it never appears in a process list:

```sh
bk() {  # bk <url> [curl args...]
  printf 'header = "Authorization: Bearer %s"\n' "${BUILDKITE_API_TOKEN:-$(cat ~/.buildkite/token)}" \
    | curl -sSg -K - "$@"
}
B=https://api.buildkite.com/v2/organizations/msl/pipelines/ome
```

| Purpose | Call |
|---|---|
| recent main builds with jobs | `bk "$B/builds?branch=main&per_page=20&include_retried_jobs=true"` |
| builds for a commit | `bk "$B/builds?commit=<sha>&per_page=20&include_retried_jobs=true"` |
| one build | `bk "$B/builds/<n>?include_retried_jobs=true"` |
| job log, raw text | `bk "$B/builds/<n>/jobs/<job-id>/log.txt" -H 'Accept: text/plain'` |
| retry a job (needs `write_builds`) | `bk "$B/builds/<n>/jobs/<job-id>/retry" -X PUT` |
| commit statuses (GitHub side) | `gh api repos/mslsrc/ome/commits/<sha>/status --jq '.statuses[] | "\(.context)\t\(.state)\t\(.target_url)"'` |

Parsing:

- Job JSON may contain raw control characters: `python3 -c 'import json,sys; d=json.loads(sys.stdin.read(), strict=False)'`,
  never `jq`.
- Logs carry `_bk;t=<ms>` timestamp escapes, OSC hyperlinks and ANSI colour:
  `perl -pe 's/\x1b_bk;t=\d+\x07//g; s/_bk;t=\d+//g; s/\x1b\][^\x07]*\x07//g; s/\x1b\[[0-9;?]*[ -\/]*[@-~]//g'`.
- Job fields that matter: `step_key`, `state`, `exit_status`, `signal_reason`,
  `soft_failed`, `retried`, `retried_in_job_id`, `retries_count`,
  `parallel_group_index`, `agent.name`, `agent_query_rules` (`queue=...`),
  `web_url`, `raw_log_url`. A job with `retried: true` is a superseded
  attempt; the newest attempt per `(step_key, parallel_group_index)` is the
  one that counts.
- Build states: `running`, `failing` (still running with a red job), then the
  terminal `passed`, `failed`, `canceled`, `blocked`, `skipped`, `not_run`.
- Build `env.OME_PIPELINE_MODE == "daily"` (or any `daily-*` step key) marks a
  release-candidate build, which is out of scope here.

Save anything you download under `~/.cache/ome-agent.noindex/scratch/ci-triage/`.
