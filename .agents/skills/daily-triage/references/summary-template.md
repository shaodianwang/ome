# Triage summary template

`daily_build.py summary <build> [<build>...] --out <path>` writes the builds,
stages and decoded reports; you fill in the lead sentence, the failures table,
actions, waiting-on and open questions. Keep the file under
`~/.cache/ome-agent.noindex/scratch/daily-triage/<YYYY-MM-DD>.md`; never under
the repository.

```markdown
# OME daily RC triage — YYYY-MM-DD

**<One sentence: what was promoted (PR link) or why nothing was, what is broken, what needs a person.>**

## Builds

| Build | Mode | Commit | Candidate | Verdict | Stages | Promotion PR |
|---|---|---|---|---|---|---|
| [bN](buildkite url) | full | [sha8](commit url) | `sha8` | PASS / FAIL / IN-PROGRESS | disc:PASS cpu:PASS tpu-def:PASS tpu-ome:PASS pr:PASS | [PR](url) or - |

## Stages

| Build | Stage | Verdict | Exit | Duration | Attempts | Job |
|---|---|---|---|---|---|---|

## Failures and dispositions

| Build / stage | Class | Evidence | Disposition |
|---|---|---|---|
| bN / tpu-default-scheduler | env/tpu-capacity | `events.log`: `0/12 nodes are available: Insufficient google.com/tpu` ([artifact](url)); same on bM yesterday | parked: capacity; no retry; RC owners informed |
| bN / cpu-smoke | infra/docker-daemon | exit 125, log line 14 `failed to connect to the docker API` ([job](url)) | retried -> [attempt 2](url) PASS |
| bN / cpu-smoke | product/scheduler-name | line 233 `pod/x uses scheduler default-scheduler, expected ome-scheduler` ([job](url)); first seen on candidate sha8 | issue [mslsrc/ome#N](url); no retry |

## Reports (from build meta-data)

<pasted by the script: candidate, smoke report lines, time-to-ready-seconds>

## Actions

- <at most 5 one-liners: retries fired (job links), issues opened, escalations sent>

## Waiting on

- <owner or team role>: <item> (at most 3)

## Open questions

- <what the evidence could not settle, and what would settle it>
```

Rules for the text:

- Verdicts are the script's words (`PASS`, `FAIL`, `TIMEOUT`, `SKIPPED`,
  `QUEUED(concurrency)`, `RUNNING`, `WAITING`); classes are the ones in
  `failure-classes.md`.
- Every claim links to a Buildkite job, an artifact, a commit or an issue.
  A sentence without a link is an opinion.
- One line per failure, grouped by class when several stages share a cause.
- Name the candidate SHA and the pinned production SHA when they differ; the
  GitOps PR body lists the commits between them.
- No people's names, chat links or ticket numbers from other systems; owners
  are roles (RC owners, CI infra, OME maintainers).
- The summary describes this shift's state, not history: retire lines that
  were resolved in an earlier shift unless they recurred.
