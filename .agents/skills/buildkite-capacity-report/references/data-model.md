# Data model, families, classes, formulas

## Sweep files (`collect_sweeps.sh`)

| Path | Endpoint | Used for |
|---|---|---|
| `agents/p*.json` | `GET /v2/organizations/msl/agents?page=N&per_page=100` | supply: agents per queue (`meta_data` contains `queue=<name>`), busy = `job` present, `job.build_url` and `job.started_at` |
| `live/p*.json` | `GET /v2/organizations/msl/builds?state[]=running&state[]=failing&state[]=scheduled` | demand at the instant: pending, running and held jobs per queue across every pipeline in the org |
| `hist/ome_p*.json` | `GET /v2/organizations/msl/pipelines/ome/builds?created_from=<now-HOURS>` | 24h concurrency curve, ceiling, durations, agent-hours for the `ome` pipeline |
| `hist/org_p*.json` (`--org-history`) | `GET /v2/organizations/msl/builds?created_from=<now-HOURS>` | the same, org-wide, for the shared container queue; capped at `ORG_HISTORY_MAX_PAGES` (30) |
| `meta.json` | — | `collected_at`, `hours`, `org_history`, `org_history_capped` |

Fan-out is six requests at a time; a sweep stops at the first empty page.
Only `type: script` jobs occupy agents; `waiter`, `manual` and `trigger` jobs
are ignored everywhere.

## Job states

| Bucket | States | Meaning |
|---|---|---|
| pending | `scheduled`, `assigned`, `accepted` | eligible and waiting for an agent; wait = now − `runnable_at` (fallback `scheduled_at`, `created_at`) |
| held | `limited`, `limiting` | blocked by a concurrency group (the daily smokes); not queue demand |
| running | `running` | occupies one agent |
| not demand | `waiting`, `blocked`, `broken`, `skipped`, `canceled`, `passed`, `failed`, `timed_out` | dependencies unmet, or finished |

## Pipeline class of a build

| Class | Rule |
|---|---|
| `ome main` | pipeline `ome`, branch `main`, not daily |
| `ome PR` | pipeline `ome`, any other branch, not daily |
| `ome daily` | pipeline `ome` with `env.OME_PIPELINE_MODE == "daily"` or any `daily-*` step key |
| `other pipelines` | any other pipeline in the org (the top five are listed by slug) |

## Job family (from `step_key`)

| Family | Step keys |
|---|---|
| PR checks | `git-author-email`, `internal-tokens`, `lint`, `vet`, `test`, `scheduler-test`, `integration-test`, `perf-test`, `rollout-behavior-lock`, `helm-lint`, `muse-rust-checks` |
| drift checks | `*-drift` |
| images | `build-*`, `mirror-*` |
| charts | `chart-*` |
| KIND packaging | `package-kind-*` |
| KIND coordination | `kind-gke-<slug>` (the `KIND_SPEC_GROUPS` children) |
| KIND multi-cluster | `kind-gke-mc-*` |
| KIND quarantine | `kind-gke-quarantine` |
| KIND upgrade | `kind-gke-upgrade-identity` |
| OMENative eval | `omenative-e2e-*` |
| daily RC | `daily-*` |
| other ome step | anything else in the `ome` pipeline (for example the pipeline upload job) |
| other pipelines | every job of another pipeline |

Verify against `.buildkite/generate_pipeline.py` when a key looks new; the
report puts unknown keys under "other ome step" rather than guessing.

## Formulas

- **Concurrency curve**: for each job with `started_at`, +1 at its start
  minute and −1 at its end minute (or the window end if still running);
  prefix-sum per family; total = sum of families. Window = 24 h ending at the
  latest `finished_at` in the history (or the snapshot time if later).
- **Ceiling**: the p99.5 value of the per-minute total. An estimate of the
  pool's practical maximum, not a configured limit; the live busy count can
  exceed it briefly.
- **Utilization**: mean of the per-minute total divided by the ceiling.
- **Saturation windows**: 10-minute buckets sampled at their midpoint whose
  total is at least 95 % of the ceiling; consecutive hot buckets are merged
  into one window. Midpoint sampling keeps the per-family stack additive.
- **Wait percentiles**: nearest-rank over the pending jobs' waits.
- **Duration percentiles**: nearest-rank over finished jobs (`passed`,
  `failed`, `timed_out`, `canceled`) longer than 12 seconds, per family.
- **Agent-hours by outcome**: sum of durations in hours, bucketed into
  `passed`, `failed / timed out`, `canceled after start`.
- **Orphan holds**: busy agents whose `job.build_url` does not resolve to a
  build in the live sweep (finished or cancelled builds whose agents have not
  released yet). Judge from two sweeps ten minutes apart.
- **Long running**: running jobs older than the family's p90 duration times
  `--long-factor` (1.5), or 90 minutes when the family has no history.
- **Verdict hint**: `stuck` when agents = 0 and pending > 0; `saturated` when
  pending > 0 and busy ≥ 95 % of the ceiling; `backlog without saturation`
  when pending > 0 with p50 wait ≥ 10 minutes and idle agents exist (agent
  health, tags, or a queue-name mismatch); otherwise `clear`.

## summary.json shape

```
{ "collected_at": "...",
  "queues": { "<queue>": {
      "snapshot": { agents, busy, idle, orphan_holds, pending, pending_by_class, pending_by_family,
                    pending_other_pipelines, held_by_concurrency, running, running_by_class,
                    wait_p50, wait_p90, wait_max, demand_owners[] },
      "long_running": [ {key, family, pipeline, build, minutes, family_p90, limit, web_url} ],
      "history": { scope, capped, window, jobs_started, builds, ceiling, mean_busy, utilization,
                   saturated_buckets, buckets_total, saturation_windows, dur_p50, dur_p90,
                   dur_by_family, agent_hours_by_outcome, timeline[] },
      "verdict": "..." } } }
```
