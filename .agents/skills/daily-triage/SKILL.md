---
name: daily-triage
description: "Run the OME daily release-candidate (RC) triage shift: find the latest daily Buildkite builds, decide pass/fail per stage (discovery, CPU smoke, two TPU smokes, GitOps promotion PR), separate infrastructure failures from product regressions with log and artifact evidence, and write an evidence-linked summary; may open a GitHub issue with gh under the operator's own identity, never merges or promotes anything. Use on \"triage the daily\", \"daily build failed\", \"check the RC promotion\", \"what broke the RC smoke\", \"was today's release promoted\"."
---

# daily-triage — the RC triage shift

One shift covers every daily build since the last shift (normally one full
build, sometimes a Kargo smoke-only build beside it). The output is a short,
linked summary that says what was promoted, what failed, which failures are
infrastructure and which are OME regressions, and what was done about each.
The tooling is the daily-monitor skill's script; this skill adds the
decision rules, the evidence standard and the issue workflow.

References: `references/failure-classes.md` (infra vs product rules and the
class table), `references/summary-template.md` (the summary shape),
`references/github-issue.md` (filing under your own identity). Pipeline
anatomy and retry mechanics: `../daily-monitor/references/`.

## Setup

```sh
S=.agents/skills/daily-monitor/scripts/daily_build.py      # stdlib Python
OUT=~/.cache/ome-agent.noindex/scratch/daily-triage; mkdir -p "$OUT"
```

Buildkite token in `$BUILDKITE_API_TOKEN` or `~/.buildkite/token` (mode 600).
`gh auth status` must show your own login before any issue is filed. Nothing
from this shift is written under the repository.

## The shift

1. **Find the builds.**
   `python3 $S find --days 2`. Daily builds are the ones with build env
   `OME_PIPELINE_MODE=daily` or `daily-*` step keys; mode `full` has
   discovery and the PR step, mode `kargo` has only the three smokes. Include
   builds still running; note the ones queued in a concurrency group
   (`QUEUED(concurrency)`) as waiting, not failed.
2. **Verdict per stage.** `python3 $S status <build>` for each. Verdicts are
   `PASS`, `FAIL`, `TIMEOUT`, `ERRORED`, `CANCELED`, `SKIPPED` (dependency
   failed), `QUEUED(concurrency)`, `RUNNING`, `WAITING`. A build is PASS when
   every present stage passed; in full mode that includes the PR step, whose
   PASS without a `daily-gitops-pr-url` means production already pinned the
   candidate. Record the candidate SHA (`meta-data candidate short_sha`), the
   RC identity lines (`release-images:`) and `time-to-ready-seconds`.
3. **Classify every non-PASS stage.** `python3 $S classify <build>` gives
   the signature hits and the log tail; then dig:
   - `python3 $S log <build> <step-key> --grep '<signature>'` for the exact
     line and number; `--tail 200` for how the log ends.
   - `python3 $S artifacts <build> <tpu step-key> --download "$OUT/b<build>"`
     for `events.log`, `final-state.log`, `monitor.log`, describes and pod
     logs.
   - Compare with the previous full build (`find --days 4`): same signature
     on a different candidate and agent is environment or an older
     regression; unique to this candidate points at its commits.
   Apply the decision rules in `references/failure-classes.md`: script
   assertion versus external kill; was RC on the candidate; did OME do what
   the script asked; unknown is `needs-evidence`, never "flake".
4. **Act, once per failure.**
   - `infra/*`: one retry (`python3 $S retry <build> <step-key>`), then
     `python3 $S watch <build>` in the background until
     `WAKE: watched retry attempt(s) finished`. A second failure of the same
     kind is not retried; it is escalated with both attempts linked.
   - `env/*`: no retry that cannot succeed (RC not on the candidate,
     missing RBAC or prerequisites, no TPU capacity); write the evidence and
     the owner role (RC owners, CI infra) into the summary.
   - `release/*`: `python3 $S release <newest main commit>` and name the red
     `mirror-*`/`chart-*` step; the fix is in the standard main build.
   - `product/*`: open or update a GitHub issue (below), never retry.
   - `needs-evidence/*`: say what is missing and what would settle it.
   Never retry `daily-discovery` after a smoke ran (the script refuses); a
   new candidate needs a new build.
5. **Write the summary.**
   `python3 $S summary <build> [<build>...] --out "$OUT/$(date -u +%F).md"`
   pre-fills builds, stages and decoded reports; complete the lead sentence,
   the failures-and-dispositions table, actions, waiting-on and open
   questions per `references/summary-template.md`. Deliver the file path and
   the lead sentence to the operator.
6. **Reconcile before closing.** Every retry has its outcome recorded, every
   product finding has an issue link, every running build has its next
   check time named. Nothing is left as "under investigation" without the
   missing evidence spelled out.

## Infrastructure or product: the short rules

- **Exit 125 / `failed to connect to the docker API`, exit -1 with an abrupt
  log end, `no ECR credential source`, `GH_TOKEN is required`, GitHub HTTP
  errors** are CI infrastructure. The RC and OME were never exercised.
- **`RC did not converge to the requested release`, `new RC version detected
  while the smoke test was running`, `missing permission`, prerequisites
  `not found`, `Insufficient google.com/tpu`, PVC Pending** are environment:
  the candidate was not observed under the conditions the smoke needs.
- **A script assertion on a converged RC** (`OME did not create the BaseModel
  metadata Job`, `metadata Job image mismatch`, `OME did not create an
  InferenceService pod`, `uses scheduler X, expected ome-scheduler`,
  `unexpected TPU gang shape`, `unexpected engine/router scheduler`,
  `unexpected GitOps paths changed`) is product. So is a control-plane pod
  crash looping on the candidate image; that one leads the summary.
- **Timeouts** (`timed out waiting for production readiness`,
  `timed out waiting for the condition`, a `timed_out` job) are undecided
  until the describe output and artifacts say whether pods were Pending
  (environment) or crashing (product).

## Evidence standard

Each disposition line carries: build and job links, the quoted log line
with its number, the artifact or describe excerpt that supports the class,
and the comparison with the previous build. Retry outcomes are linked as
attempts. Owners are roles, not names. No chat links, no ticket numbers from
other systems, no secrets, no kubeconfigs.

## GitHub issue (product findings only)

Under your own login, in `mslsrc/ome`, after a dedupe search; labels only from
`gh label list`. Title `[CI/Tests] daily RC: <stage> — <symptom>` (under 70
chars); body: what failed (build, stage, candidate), evidence (quoted lines,
links, RC identity), why it is product (the rule that applied; previous build
that passed), the suspect commit window (the GitOps PR body lists the commits
since the pinned release), and the script assertion that reproduces it. Full
recipe in `references/github-issue.md`. Existing match: comment, do not
duplicate.

## Boundaries

- Never merge, approve or edit the GitOps promotion PR or any PR; never bump
  or roll back the RC release; never run the publish scripts outside CI.
- Never `kubectl apply`/`delete`/`patch` in the RC cluster. If a read-only
  look is needed, use a throwaway kubeconfig file passed with `--kubeconfig`
  on every command, exactly as `../daily-monitor/references/rc-cluster.md`
  shows; never `kubectl config use-context`, never touch the ambient
  `KUBECONFIG`, never let a local-cluster tool see it.
- Never close, relabel or reassign issues you did not open this shift.
- Never file an issue for infrastructure or environment failures in the
  OME tracker; those go to their owners through the summary.
- One manual retry per infrastructure failure, none for anything else.

## Open questions

- The daily schedule (time, message, branch) is a Buildkite setting outside
  the repository; "since the last shift" is therefore measured by
  `created_at`, not by a message pattern.
- Which labels exist on `mslsrc/ome` for CI regressions is not recorded in
  the repository; check `gh label list` each time.
- The RC rollout mechanism and its lag behind the registry are outside the
  repository; they decide whether `RC did not converge` is transient.
- Whether the REST build object exposes `env` for scheduled builds is
  expected but unconfirmed here; the step-key rule is the fallback.
