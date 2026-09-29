---
name: ome-oncall
description: Answer CI and release oncall questions about the OME repository from the OME bot - "why is main red", "is this failure flaky", "what broke build N", "did my commit pass", "what is promoted where", "what runs on cluster X" - through the ome-bridge tools builds, ci and docs and the release endpoint, and reply with a verdict, quoted evidence and the next action. Read-only; never retries, promotes or files anything. Load after ome-bridge.
---

# ome-oncall — CI and release questions, answered from the bridge

Read `ome-bridge` first. Every call below is `$B ...` with
`B="python3 <ome-bridge skill dir>/scripts/ome_bridge.py"`. The
bridge runs the repository's own CI scripts (`ci-triage/scripts/main_builds.py`
as the `builds` tool, `fix-ci/scripts/ci_build.py` as the `ci` tool) with a
read-only CI token, so the bot sees what an oncall engineer sees and can
change nothing. Retries, issues, fixes and promotions belong to the oncall;
name them as the next action and stop.

## Load the classification table

Before classifying anything, read the table the humans use, so the bot and
the oncall agree on classes and known flakes:

```sh
$B tool docs read skills/ci-triage/references/classification.md
```

Use its signature table, its known-flake list and its flaky-or-real
decision; do not classify from memory. `skills/fix-ci/references/failure-signatures.md`
and `skills/fix-ci/references/pipeline-steps.md` (step keys, lanes, what is
soft-fail) are there too when a step key is unfamiliar.

## CI: the calls

```sh
$B tool builds list --limit 10          # recent standard main builds, red and soft-failed step keys
$B tool builds diff <build>             # failed jobs vs the previous standard build: new, persists, not-run
$B tool ci find --commit <sha>          # builds for a commit (newest first)
$B tool ci find --branch <branch>       # builds for a branch
$B tool ci status <build>               # every job: step key, state, exit, attempts
$B tool ci classify <build>             # failed jobs' logs against the known signatures
$B tool ci log <build> <step-key> --grep 'FAIL|Error|FATAL|panic' --context 20
$B tool ci log <build> <step-key> --tail 200
$B tool ci artifacts <build> <step-key> # list only
```

`ci find --pr` needs a GitHub client the bridge lacks and fails with "pass
--commit". For a pull request, ask for its head commit or branch, or use
`ci find --branch <head-branch>` when the asker named the branch.

Logs are long: always `--grep` or `--tail`. A `timeout` refusal means narrow
the query, not retry it.

## CI: reply shapes

### "Why is main red?"

1. `builds list`: the newest red standard build and how long main has been
   red (the first red build in the run).
2. `builds diff <build>`: which step keys are new and which persist.
3. `ci classify <build>`, then `ci log` on each red key for the first real
   error line (not the last line; the table explains how to find it).
4. Reply:

```
*Verdict* — main is red since build <N> (<age>) on <step keys>; <class>.
*Evidence* —
> build <N> <step-key>: <first error line>
> build <N> diff: <step-key> new since build <N-1> / persists since <first red>
*Next action* — <what the oncall should do: fix owner area, retry once for an infra signature with its retries spent, or nothing for advisory steps>
*Scope* — builds <range> read; compare <URL from builds diff>
```

Name suspects only by the compare range the diff prints; the bot cannot list
commits, so do not guess authors or PRs.

### "Is it flaky?"

Apply the table's decision in order: known flake, persists, new with a
related change, new with an infra signature, passed on automatic retry.
Answer with one of those names and its basis:

```
*Verdict* — not flaky: persists since build <N> with the same first error.
*Evidence* — two quoted first-error lines from different builds, or the known-flake entry that matches.
*Next action* — retry once (oncall) / fix / ignore advisory.
```

A red Go test or Ginkgo spec that passed on retry is a nondeterministic
defect, not noise; say so.

### "Did my commit (or branch) pass?"

`ci find --commit <sha>` or `--branch <name>`, then `ci status <build>` for
the newest build: state, red keys, and for each red key the class from
`ci classify`.

## Release: the calls

```sh
$B release                              # Kargo stages, current freight, chart versions, last promotion, health; newest freight; running manager version per cluster
$B clusters                             # fleet table with the OME version each cluster reports
$B command "version --cluster <name>"   # one cluster's client and operator versions
```

`release` answering `not_configured` means the bridge has no release source;
say release data is unavailable from the bot and point at the cluster
versions from `clusters` instead.

Chart versions are `1.0.0-<UTC timestamp>.<short SHA>` of a `main` commit;
the running manager image tag is the short SHA.

### "What is promoted where?"

Reply as a table built only from what `release` and `clusters` returned:

```
*Release* — <newest freight> (<age>) is on <stages>; <stages> still run <older freight>.
STAGE        FREIGHT (ome-resources)      PROMOTED   HEALTH   RUNNING
<stage>      1.0.0-<ts>.<sha>             <age>      Healthy  <sha>
*Waiting* — <freight> since <age>, not yet on <stage> (<phase or health reason from the stage>)
*Mismatch* — <cluster> runs <sha> while its stage holds <sha>: the sync has not landed
```

A stage whose freight is older than the newest freight is a pending
promotion: say which and how old. A cluster running a different SHA than
its stage's freight is a sync that has not landed, or a cluster the bridge
could not read; say which.

## Rules

- Read-only. Never ask the bridge to retry, download artifacts, promote or
  touch Stage or Freight objects; those subcommands and verbs do not exist
  in its grammar.
- Every build number, step key, version and SHA in the reply comes from a
  tool output you read. Never infer a version from a commit message.
- Quote at most ten log lines in total; never paste anything that looks like
  a token or credential even if a log echoes it.
- An unreachable cluster or a missing release source is reported as such,
  not as a stale release.
- The not-yet-promoted commit list needs GitHub, which the bridge does not
  have; say "not available from the bot" if asked.
