---
name: buildkite-capacity-report
description: Explain why OME CI jobs are pending on a Buildkite agent queue — sweep the msl org's agents, live builds and the ome pipeline's 24h history through the REST API, then report per queue (tw-cpu-x86 container queue, the GKE KIND queue from GKE_KIND_QUEUE) the pending backlog by owner and job family, wait percentiles, the busy-agent ceiling, saturation windows, and long-held agents, as text plus summary.json. Use on "why are CI jobs pending", "is the KIND queue saturated", "queue capacity report", "who is eating the CI queue", or "/buildkite-capacity-report".
---

# buildkite-capacity-report — pending-queue analysis for OME's agent queues

Answers, from one live sweep plus 24 hours of history: is the queue stuck or
saturated, who owns the demand, when the pool pins at its ceiling, where the
agent-hours go, and which jobs hold agents unusually long. Output is a text
report and a `summary.json`; everything is derived from Buildkite REST
sweeps, so it needs no cluster access and no GraphQL scope.

## The queues

`.buildkite/generate_pipeline.py` is the source of truth for which step runs
where; verify against it before quoting a queue name.

| Queue | Constant | Runs | Supply |
|---|---|---|---|
| `tw-cpu-x86` | `DEFAULT_QUEUE`, `BUILD_QUEUE` | every-branch lanes (`lint`, `vet`, `test`, `integration-test`, `perf-test`, `scheduler-test`, `rollout-behavior-lock`, `helm-lint`, `muse-rust-checks`, hygiene, `*-drift`), image `build-*` / `mirror-*`, `chart-*`, `package-kind-*` | container agents shared org-wide with other pipelines; one job per agent |
| GKE KIND queue | `GKE_QUEUE = GKE_KIND_QUEUE` (default `gke-tpu-v7x`; `.buildkite/README-gke-kind.md` documents the dedicated `gke-kind-x86` design) | `kind-gke-<slug>`, `kind-gke-mc-*`, `kind-gke-quarantine`, `kind-gke-upgrade-identity`, `omenative-e2e-*` | agent pods created per job by an agent-stack-k8s controller; each KIND job requests about 5 vCPU, 14 GiB RAM, 24 GiB ephemeral; zero idle agents is by design |

There is no separate docker or dind queue: the container queue's agents run
the docker plugin against a local daemon, and each GKE job pod carries its own
privileged dind sidecar. A step that targets a queue with no registered
agents waits indefinitely:
zero agents plus pending jobs is a dead queue, not a capacity problem
(`.buildkite/README-gke-kind.md`). The pipeline's kill switches
(`GKE_KIND_ENABLED`, `GKE_EVAL_ENABLED`, `GKE_UPGRADE_ENABLED`) remove whole
groups from future builds; they never drain jobs already scheduled.

## Prerequisites

- Buildkite user API token in `$BUILDKITE_API_TOKEN` or `~/.buildkite/token`
  (mode 600) with `read_agents` and `read_builds`. The collector fails clearly
  without one; the report builder only reads files. Never copy the token
  anywhere else.
- `curl` and `python3` (standard library only). Set `HTTPS_PROXY` if your host
  reaches `api.buildkite.com` through a proxy; curl honours it.
- Scratch: `~/.cache/ome-agent.noindex/scratch/buildkite-capacity-report/`
  (`OME_CAPACITY_SCRATCH`), never under the repository, never `/tmp` for
  anything you want to compare an hour later.

## Steps

### 1. Collect the sweeps (about 1–3 minutes)

```sh
S=.agents/skills/buildkite-capacity-report/scripts
DIR="${OME_CAPACITY_SCRATCH:-$HOME/.cache/ome-agent.noindex/scratch/buildkite-capacity-report}/data"
sh $S/collect_sweeps.sh "$DIR"                       # agents, live builds, 25h of ome history
sh $S/collect_sweeps.sh "$DIR" 25 --org-history      # add org-wide 24h history (capped) for the shared queue
```

Fetches every org agent page, the org's live builds
(`state[]=running&state[]=failing&state[]=scheduled`), and the `ome`
pipeline's build history for the last 25 hours, then stamps `meta.json` with
the snapshot time. Bounded fan-out (six requests at a time), stops at the
first empty page. Re-run for a fresh snapshot; the pending numbers describe
the instant of the sweep.

### 2. Build the report

```sh
python3 $S/queue_report.py --data-dir "$DIR"                    # both queues, text to stdout
python3 $S/queue_report.py --data-dir "$DIR" --queue gke-tpu-v7x --json "$DIR/summary.json"
```

Per queue it prints: agents (total, busy, idle), pending jobs by pipeline
class (`ome main`, `ome PR`, other pipelines) and by job family,
wait percentiles (p50, p90, max, from `runnable_at`), the top demand owners
(builds with the most pending and running jobs, with links), running jobs
older than the family's p90 duration, and from history: the 24h concurrency
curve (busy agents per 10-minute bucket), the ceiling estimate (p99.5 of that
curve), the saturation windows (buckets at or above 95 % of the ceiling),
job duration percentiles per family, and agent-hours by outcome. Read the
JSON before drawing conclusions; it is the evidence.

### 3. Analyst pass (what the script cannot do)

- **Name the burst owners.** `demand_owners` lists builds; say which merges
  or PRs caused the wave, by PR number and title.
- **Judge held agents from two sweeps, not one.** `orphan_holds` counts busy
  agents whose build is no longer live (cancel grace); re-sweep ten minutes
  later before calling it a leak.
- **Long runners.** For each entry in `long_running`, open the job: a KIND
  child past its group's p90 is usually a stuck spec (see `kind-gke-*`
  timeouts in the pipeline), an eval shard has a 180-minute budget by
  design.
- **Saturated or undersized.** Pinned plateaus with a low 24h mean is a
  burst problem (serialise or shard less); pinned around the clock is an
  undersized pool. For the GKE queue the ceiling is the node pool's room for
  the per-job request, so state it as "N concurrent KIND jobs".
- **Dead queue.** Agents = 0 with pending jobs: the controller or its proxy
  egress is down; escalate CI infrastructure with the sweep, do not wait it
  out.
- **Shared queue.** On `tw-cpu-x86` the other pipelines' share is the first
  thing to report; an OME backlog with little OME demand is someone else's
  wave.

### 4. Share

Paste the verdict line and the per-queue block into your reply, attach or
link `summary.json` from scratch, and keep the data directory until the
follow-up sweep. The verdict is one sentence: "saturated: demand N vs
ceiling M, p50 wait X min" or "stuck: 0 agents on <queue>, K jobs pending
since <time>" or "clear: backlog N, p50 wait under a minute".

If pod-level evidence is needed for the GKE queue (an agent pod that never
registered), use operator credentials with an explicit
`kubectl --kubeconfig <path> --context <ctx> -n <agent-ns> get pods`; never
switch the ambient kubeconfig (`.agents/skills/fix-ci/references/buildkite-api.md`).

## Traps

- Buildkite build state `failing` is still running (at least one red job);
  only `passed`, `failed`, `canceled` are terminal. The live sweep must
  request `failing` too or every failing-but-live build's agents count as
  orphan holds.
- Agent-view busy and builds-view running never match exactly; the gap is
  cancel grace plus sweep timing. Report it, do not "fix" it.
- The queue of a job is `agent_query_rules` (`queue=...`) on the job, and
  `meta_data` (`queue=...`) on the agent. Jobs of type other than `script`
  never occupy an agent.
- `runnable_at` is when a job became eligible; wait = now − `runnable_at`.
  Jobs in `limited`/`limiting` are held by a concurrency group, not by the
  queue.
- The 10-minute buckets sample concurrency at the bucket midpoint so stacked
  families stay additive; do not replace it with per-family maxima.
- The ceiling is an estimate from history; the live busy count can wobble
  above it. Quote it as "about".
- Org-wide 24h history is capped (`--org-history` pages); when the cap is
  hit the report says so, and the ceiling for the shared queue is a lower
  bound.
- Job JSON can carry raw control characters; the scripts parse with
  `json.loads(text, strict=False)`. Do not pipe the sweeps through `jq`.

Details: `references/data-model.md` (fields, families, classes, formulas).
