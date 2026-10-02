---
name: ci-triage
description: Triage red Buildkite CI on the mslsrc/ome main branch with no filer in front of you — list recent main builds through the Buildkite API, find the failed jobs, pull and clean their logs, classify infra vs code vs drift vs advisory, decide flaky vs real against the previous main build and the known-flake list, and write an evidence-linked conclusion; may open or update a `ci-triaged` GitHub issue and, for mechanical fixes only, a draft PR. Use on "triage main", "why is main red", "what broke the last main build", "is this failure flaky", "check main CI", or as the per-build worker of the oncall-loop skill.
---

# ci-triage — triage one red main build of mslsrc/ome

Nothing files main-branch breakages for this repository: there are no GitHub
Actions, and Buildkite only posts commit statuses. This skill discovers the
breakage itself, then works like an oncall engineer: evidence first, a written
conclusion second, action last. Deliverables, in order:

1. an evidence-linked write-up (in your reply, and as a `ci-triaged` issue or
   issue comment when the failure is real or unresolved);
2. only when justified, one narrow action: a single evidence-backed retry, a
   draft PR for a mechanical fix, or an explicit escalation.

Scope: builds on `main` (Buildkite org `msl`, pipeline `ome`). Pull-request
builds are the `fix-ci` skill's job.

## Ground rules

- **Investigate before acting.** No retry, issue, or PR before you can quote
  the first real error line and name the step key and the build.
- **Never retry blindly.** One retry, one job, only on an infrastructure
  signature from the table below. A red Go test or Ginkgo spec is never
  retried to make it green; passing on retry with no code change is a defect
  with an owner, not noise.
- **Never edit a test to make it pass**, never quarantine without asking,
  never merge, never close an issue on your own.
- **Evidence, not verdicts.** Link the build, the job, the commit range; quote
  at most ten log lines; label speculation as speculation. Name suspects by
  PR number and title, never by person.
- Nothing from a triage is written under the repository. Scratch is
  `~/.cache/ome-agent.noindex/scratch/ci-triage/` (`OME_CITRIAGE_SCRATCH`).

## Prerequisites

- Buildkite user API token in `$BUILDKITE_API_TOKEN` or `~/.buildkite/token`
  (mode 600) with `read_builds`, `read_build_logs`, `read_artifacts`, and
  `write_builds` if you will retry. Both scripts refuse to run without one.
  Never paste a token into a file, a command line, or a reply.
- `gh auth status` succeeds for `mslsrc/ome`; issues and PRs are created under
  your own login.
- The `fix-ci` helper for logs, artifacts, and retries:
  `F=.agents/skills/fix-ci/scripts/ci_build.py`. Raw endpoints for when it is
  absent: `references/buildkite-api.md`.

## Tools

```sh
T=.agents/skills/ci-triage/scripts/main_builds.py     # stdlib Python, no state
python3 $T list                     # recent standard main builds, red step keys per build
python3 $T list --json              # same, machine-readable (oncall-loop consumes this)
python3 $T diff <build>             # failed jobs with attempts, agent, queue; new vs persisting
                                    # against the previous standard main build; compare URL
python3 $F status <build>           # every job: key, state, exit, attempts, state-only guess
python3 $F classify <build>         # failed jobs' cleaned logs matched against known signatures
python3 $F log <build> <key> --grep 'FAIL|Error|FATAL|panic' --context 20
python3 $F artifacts <build> <key> [--download DIR]
python3 $F retry <build> <key>      # PUT .../jobs/<id>/retry, newest attempt only
```

## The pipeline you are reading

Every step posts a GitHub commit status whose context is its step key, so
`gh api repos/mslsrc/ome/commits/<sha>/status` and the Buildkite job list name
the same things; `buildkite/ome` is the rollup whose `target_url` ends in the
build number. Full step table: `.agents/skills/fix-ci/references/pipeline-steps.md`.

| Lane | Step keys | Queue | Gating |
|---|---|---|---|
| Hygiene | `git-author-email`, `internal-tokens` | `tw-cpu-x86` | hard |
| Go | `lint`, `vet`, `test`, `scheduler-test` | `tw-cpu-x86` | hard |
| Suites | `integration-test`, `rollout-behavior-lock` | `tw-cpu-x86` | hard |
| Perf | `perf-test` | `tw-cpu-x86` | soft-fail |
| Charts, console | `helm-lint`, `muse-rust-checks` | `tw-cpu-x86` | hard |
| Drift | `fmt-drift`, `tidy-drift`, `manifests-drift`, `generate-drift`, `dep-crds-drift`, `oep-status-drift` | `tw-cpu-x86` | hard |
| KIND on GKE | group `integration-test-kind-gke`, children `kind-gke-<slug>`, plus `package-kind-test-image` | GKE queue | hard |
| KIND advisory | group `integration-test-kind-mc-gke` (`kind-gke-mc-placement`, `kind-gke-mc-quota`), `kind-gke-quarantine` | GKE queue | soft-fail |
| Images, charts | `build-<image>`, `mirror-<image>`, `chart-<chart>`, `chart-ome-release` | `tw-cpu-x86` | hard; push only on `main` |
| main only | groups `omenative-e2e-kind-gke-<i>-of-<n>` (`omenative-e2e-<feature>-<topology>[-i-of-n]`, uploaded by `upload-eval-lanes`), `package-kind-eval-image`, group `integration-test-kind-upgrade` (`kind-gke-upgrade-identity`) | GKE queue | hard |

The GKE queue is whatever `GKE_KIND_QUEUE` names, default `gke-tpu-v7x`;
`.buildkite/README-gke-kind.md` documents the dedicated `gke-kind-x86` design.
There is no separate docker or dind queue: the container queue's agents run
the docker plugin against a local daemon (exit 125 when it dies), and each GKE
job pod carries its own privileged dind sidecar.
Kill switches read at pipeline upload: `GKE_KIND_ENABLED`, `GKE_EVAL_ENABLED`,
`GKE_UPGRADE_ENABLED` (default on). Retry presets: `_infra_retry` (exit 125 or
`agent_stop`, limit 2) on the container-queue lanes and the KIND children;
`_auto_retry` (any exit, limit 2) on image build, mirror, and drift steps. A
KIND group posts one GitHub context; the failing child is visible only as the
Buildkite job whose `step_key` is `kind-gke-<slug>`.

## Step 1: discover the breakage

```sh
python3 $T list                       # newest first; RED keys, SOFT keys, RETRIED keys per build
python3 $T diff <build>               # the build you are triaging
```

Pick the newest red standard build unless the user named one. `diff` prints,
per failed job: exit status, signal reason, attempts and retried-into ids,
agent name and queue, and whether the same key was red in the previous
standard main build (`new`, `persists`, `not-run`). It also prints the
GitHub compare URL between the previous build's commit and this one; open it
with `gh api repos/mslsrc/ome/compare/<prev>...<sha> --jq '.commits[] | .sha[0:8] + " " + (.commit.message | split("\n")[0])'`
to list the candidate commits. Confirm the build's commit is what you think it
is; a build for a superseded commit is not evidence about the current head.

## Step 2: collect evidence per failed job

```sh
python3 $F classify <build>                      # signature per red job
python3 $F log <build> <key> --grep 'FAIL|Error|FATAL|panic|exit status' --context 20
python3 $F log <build> <key> --tail 200          # how the log ends
```

Find the FIRST real error, not the last line. Buildkite groups a lane's stages
under `--- name ---` headers; the failing stage is the last header before the
error. For `test`, the generator stage runs first and its failure aborts the
lane. For KIND children read the Ginkgo summary (`Summarizing N Failures`) and
the `[FAIL]` line naming the spec file. For `omenative-e2e-*` read the
`NON-PASS:` list and download the results archive with `artifacts --download`.

## Step 3: classify

Full rules and signature table: `references/classification.md`. The short form:

| Class | Evidence | Default action |
|---|---|---|
| infra: docker daemon | `exit_status` 125, log says `failed to connect to the docker API` or `Cannot connect to the Docker daemon` | one retry if the automatic retry limit was spent |
| infra: agent lost | `signal_reason` `agent_stop`, exit -1, log stops mid-stage | one retry |
| infra: KIND node boot | `cluster create failed in BeforeSuite` (exit 125) | auto-retried; one manual retry at most |
| infra: registry, network, secrets | ECR auth, `proxy.golang.org` timeouts, `failed to fetch secrets` | one retry; repeat means escalate |
| drift | `git diff --exit-code` fails on a `*-drift` step | mechanical fix: run the make target, commit |
| generator | `unclassified KIND specs`, `quarantine ... expired`, spec-group errors | mechanical fix in `.buildkite/generate_pipeline.py` |
| code | `--- FAIL:`, Ginkgo `[FAIL]`, `WARNING: DATA RACE`, lint findings, compile errors, coverage below 48%, `NON-PASS:` | reproduce, then fix or escalate |
| timeout | `panic: test timed out`, Buildkite `timed_out`, a spec exceeding its Eventually | code unless the whole lane was starved |
| advisory | `soft_failed: true` (`perf-test`, `kind-gke-mc-*`, `kind-gke-quarantine`) | note; never blocks merge |

Classification says where execution failed, not who caused it. An exit code
alone never establishes infra; the log line does.

## Step 4: flaky or real

Decide with the previous standard main build (`diff`), the known-flake list,
and the commit range, in that order:

1. **Known flake** — the failure matches an entry in
   `references/classification.md#known-flakes`: perf
   `TestScalability/FunnelBeatsPoll`; `kind-gke-mc-quota` (`Multi-cluster Kueue
   quota`) timing out; eval case `p317` under `omenative-e2e-kind-gke`. Say so,
   link the evidence, do not chase it. A second identical failure on a
   different agent in the same build is not a flake.
2. **Persists** — the same key was red in the previous main build with the
   same first error: a standing breakage, not a flake. Find the first red
   build (`list` shows the run) and the commit range that introduced it.
3. **New, with a related commit** — the key was green in the previous build
   and a commit in the range touches the failing package, spec, chart,
   Dockerfile, or `.buildkite/`: rank suspects mechanistically (failing symbol
   or behaviour connected to a diff hunk) before naming any of them.
4. **New, infra signature** — retry once; recovered means transient (record
   it, no issue unless it repeats across builds); failed the same way means
   an outage or a repo-side bug that only looks like infra.
5. **Passed on automatic retry** — the step is green but an attempt failed.
   For exit-125 attempts this is the preset doing its job. For a red spec or
   Go test it is a nondeterministic defect: write it up and file it.

Reproduce locally only through `make` targets (map in
`.agents/skills/fix-ci/references/local-reproduction.md`). Any KIND
reproduction runs under a throwaway kubeconfig:
`KUBECONFIG="$(mktemp -t ome-kind-XXXXXX.kubeconfig)" GINKGO_FOCUS='<It text>' make test-kind`,
then `rm -f "$KUBECONFIG"`. Never `kubectl config use-context`.

## Step 5: write the conclusion

Use `references/writeup-template.md`. It has: build and commit, failed step
keys with job links, class per key, the quoted first error, the
flaky-or-real verdict with its basis, ranked suspects (PR numbers and
titles), the action taken or recommended, and the dedupe marker
`<!-- ci-triage step=<key> -->` on anything posted to GitHub. Post it in your
reply first; then to GitHub as below; then, when `OME_SLACK_BOT` is set,
to the working-group channel with
`python3 .agents/skills/slack-post/scripts/slack_post.py --markdown-file <write-up>`
(the `slack-post` skill), threading later updates for the same breakage
under that post with `--thread-ts`.

## Step 6: act, once

- **Retry** (`python3 $F retry <build> <key>`) only for an infra class whose
  automatic retries are spent; then `python3 $F wait <build>` and report.
- **Issue** — for a real breakage, a persisting failure, an unrecovered infra
  signature, or a nondeterministic spec. Search first, then update or open
  (commands and body shape in `references/github-issue.md`):
  ```sh
  gh issue list --repo mslsrc/ome --label ci-triaged --state open --search "<step key>"
  ```
  Same step key and same first error already open: add a comment with the
  new build; otherwise `gh issue create --label ci-triaged` (create the label
  if it is missing; skip non-fatally if you cannot). Never close; suggest
  closing after three consecutive green main builds for that key.
- **Draft PR** — only for a mechanical fix: a drift regeneration, a generator
  classification, a lint auto-fix, an obviously broken import or fixture,
  a flake hardening that raises a timeout or adds a bounded retry with a
  stated reason. Branch from a fresh `origin/main`, title `[CI/Tests] ...` or
  `[Bugfix] ...` under 52 characters, body Why / What / Verification linking
  the issue, `gh pr create --draft`. Before pushing run the gates for what
  changed: `make fmt vet ci-lint test-no-xet`; `.buildkite/` changes add
  `python3 -m unittest discover -s .buildkite -p 'test_*.py'`; rollout tables
  add `make rolloutspec-check`; any text adds
  `python3 hack/check_internal_tokens.py --tree`. Repository rules in
  `.agents/skills/fix-ci/references/fix-hygiene.md` apply unchanged.
- **Escalate** with the write-up when an infra signature repeats after one
  retry, the GKE queue has no agents, a registry or secret is broken, or the
  fix needs a product decision. Name the lane and the evidence, not a team.

## Stop conditions

Stop and report instead of continuing when: the token lacks a scope; the
build is still running (say when to look again); the failure is outside
`mslsrc/ome` (an upstream outage, a dependency registry); or the only next
step would be a bisect, a quarantine, or a merge — those need a human's
explicit go-ahead.
