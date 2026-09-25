---
name: oncall-loop
description: Recurring monitor for red main-branch Buildkite builds of mslsrc/ome — each iteration lists new standard main builds, dedupes by build number against a local state file, runs the ci-triage skill once per newly failed build (grouping identical consecutive breakages), follows up on retries, issues and draft PRs from earlier iterations, and reports in a few lines. Starts only when the user explicitly asks to "start the oncall loop", "monitor main CI", "watch main builds", or "run one oncall-loop iteration"; stops on request.
---

# oncall-loop — watch main, hand each new breakage to ci-triage

The loop never triages. It qualifies, dedupes, dispatches, follows through,
and reports. All judgement about a failure lives in
`.agents/skills/ci-triage/SKILL.md`; this skill only decides *which* builds
get that treatment and remembers what happened to them.

## State

Everything lives under `~/.cache/ome-agent.noindex/scratch/oncall-loop/`
(`OME_ONCALL_STATE_DIR`), never under the repository:

```
state.json        watermark build number, handled builds and their status
iterations.log    one line per iteration: time, latest build, new, followed up
```

`handled` maps a build number to `{status, keys, issue, pr, note, updated}`.
Statuses: `triage-dispatched`, `flaky-known`, `infra-recovered`,
`awaiting-retry`, `issue-open`, `pr-open`, `escalated`, `grouped-with:<n>`,
`user-handled`, `closed`. Only the loop writes the file; a triage worker
returns its result in text and the loop records it.

Helpers (standard-library Python, no network of their own):

```sh
T=.agents/skills/ci-triage/scripts/main_builds.py
L=.agents/skills/oncall-loop/scripts/loop_state.py
python3 $T list --json --limit 30 | python3 $L init        # watermark = newest main build
python3 $T list --json --limit 30 | python3 $L new         # unhandled red builds above the watermark
python3 $T list --json --limit 30 | python3 $L greens --since <build>   # consecutive green builds after <build>
python3 $L mark <build> <status> [--keys k1,k2] [--issue URL] [--pr URL] [--note '...']
python3 $L show                                           # state summary
python3 $L prune --keep 40                                # drop old terminal entries
```

## Start (only on an explicit request)

1. Confirm the token and `gh` work: `python3 $T list --limit 5` prints builds.
2. `python3 $T list --json --limit 30 | python3 $L init`. The watermark is the
   newest standard main build; every build at or below it is recorded as
   `user-handled`. The loop never touches pre-existing failures unless the
   user says "include the current red build", in which case mark that build
   `triage-dispatched` and triage it in the first iteration.
3. Run one iteration immediately (below) and report.
4. Tell the user how the loop recurs; do not schedule anything yourself:
   - Claude Code: the user runs `/loop 15m run one oncall-loop iteration`
     (10–20 minutes matches the cadence of main builds; a KIND lane takes
     30–60 minutes, so a tighter interval only reports "still running").
   - Any agent without a scheduler: the user re-issues "run one oncall-loop
     iteration" whenever they want a check; the state file makes every
     iteration idempotent.

## One iteration

Protocol in full, with the dispatch prompt: `references/iteration.md`.

1. `python3 $T list --json --limit 30 | python3 $L new` — terminal standard
   main builds above the watermark that are red and not yet handled. Builds
   still running are listed separately and left alone; a build whose only
   red jobs are soft-failed is recorded `flaky-known` with the keys and not
   dispatched, unless the same advisory key has been red for three builds
   in a row (then dispatch once).
2. **Group before dispatching.** Consecutive new builds whose red key set and
   first error line are identical are one breakage: dispatch for the lowest
   build number, mark the rest `grouped-with:<n>`.
3. Mark each build `triage-dispatched`, then run ci-triage for it: inline
   when there is one build, otherwise one background subagent per build or
   group with the prompt template in the reference (it must read
   `.agents/skills/ci-triage/SKILL.md` first and return the result block).
4. Record the outcome: `python3 $L mark <build> <status> --keys ... --issue ... --pr ... --note '...'`.
5. Follow through on earlier entries (next section), even when nothing new
   qualified.
6. Append the iteration line and reply briefly: newest build and its state;
   each new red build as one line (`b<n> <keys>: <class> — <status>, <link>`);
   follow-through changes; or "no new red main builds since b<n>".

## Follow-through

- `awaiting-retry`: `python3 $F status <build>` (`F=.agents/skills/fix-ci/scripts/ci_build.py`).
  Retry passed: mark `infra-recovered`. Retry failed the same way: the worker
  did not escalate, so escalate now with the write-up and mark `escalated`.
- `issue-open`: `python3 $T list --json | python3 $L greens --since <build>`.
  Three consecutive green standard main builds: comment on the issue with the
  three build links and tell the user it can be closed; the loop never closes.
  A later red build with the same key and error: comment on the same issue
  instead of dispatching a new triage, mark the new build `grouped-with:<n>`.
- `pr-open`: `gh pr view <url> --json state,isDraft,statusCheckRollup`. Merged:
  mark `closed`. Red checks: report it; the loop does not push fixes.
- `escalated`: report it once per iteration until the user acknowledges.
- Prune entries in terminal states once they are older than the kept window.

## Rules the loop enforces on its workers

- Dedupe by build number, never by issue; one write-up per breakage.
- Every GitHub comment or issue body a worker posts ends with
  `<!-- ci-triage step=<key> -->`; the loop checks for it before dispatching
  when the state file was lost (`gh issue list --label ci-triaged`).
- Workers never close issues, never merge, never bisect, never quarantine,
  never retry a red spec or Go test, never edit tests to pass.

## Stop

On "stop the oncall loop": stop the recurrence the user set up (their `/loop`
or their habit), append a final iteration line, and leave the state file in
place — a restarted loop resumes from it. Move the watermark only when the
user asks (`python3 $T list --json | python3 $L init --reset`).
