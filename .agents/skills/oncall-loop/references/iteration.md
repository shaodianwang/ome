# One iteration, in full

```sh
T=.agents/skills/ci-triage/scripts/main_builds.py
L=.agents/skills/oncall-loop/scripts/loop_state.py
F=.agents/skills/fix-ci/scripts/ci_build.py
```

## 1. Qualify

```sh
python3 $T list --json --limit 30 > "$OME_ONCALL_STATE_DIR/last-list.json"   # one fetch per iteration
python3 $L new < "$OME_ONCALL_STATE_DIR/last-list.json"
```

`new` prints `NEW` (terminal, red, above the watermark, unhandled), `ADV`
(soft-failed only, with the per-key streak), `RUN` (not terminal) and the
latest build.

- `NEW` → candidate for dispatch.
- `ADV` with a streak under 3 → `mark <n> flaky-known --keys <keys> --note 'soft-fail only'`.
  Streak of 3 or more → dispatch once for the lowest build of the streak.
- `RUN` → nothing; it shows up again next iteration.

## 2. Group

Two or more `NEW` builds with the same `red_keys` set, in consecutive
standard builds, are one breakage unless the first error differs. Check the
first error cheaply before deciding:

```sh
python3 $F classify <n>          # per red job: class, matched line
```

Same keys and same matched line → dispatch the lowest build number, and for
each sibling `mark <sibling> grouped-with:<lowest> --keys <keys>`.

## 3. Dispatch

Mark first, then run:

```sh
python3 $L mark <n> triage-dispatched --keys <k1,k2>
```

One new build: run ci-triage inline (read `.agents/skills/ci-triage/SKILL.md`
and follow it). Several: one background subagent per build or group with the
prompt below, and continue with follow-through while they run. Workers
return text; the loop writes state.

### Dispatch prompt (send verbatim, fill the brackets)

```
Read and follow .agents/skills/ci-triage/SKILL.md in this repository, then
triage Buildkite build <n> of mslsrc/ome (org msl, pipeline ome) on main.
Red step keys: <k1,k2>. Sibling builds with the same breakage: <list or none>.
Constraints: evidence first; one retry at most and only on an infra
signature; never retry a red spec or Go test; never edit tests; never
quarantine, merge, bisect, or close an issue. GitHub actions allowed: search
for an open `ci-triaged` issue for the same step key and first error and
comment on it, else open one with the label; a DRAFT PR only for a
mechanical fix gated by the local make targets named in the skill. Every
GitHub body you post ends with `<!-- ci-triage step=<key> -->`. Do not run
kubectl against any cluster unless you name --kubeconfig and --context
explicitly. Write nothing under the repository; scratch is
~/.cache/ome-agent.noindex/scratch/ci-triage/.
Return: the write-up block from references/writeup-template.md and, as the
last line, the RESULT line it defines.
```

## 4. Record

Map the worker's `RESULT` line to a status:

| RESULT verdict / action | mark |
|---|---|
| `flaky-known` | `flaky-known --keys ... --note '<matched flake>'` |
| `infra-recovered` | `infra-recovered --keys ...` |
| action `retry` and the build is still running | `awaiting-retry --keys ...` |
| action `issue` | `issue-open --issue <url> --keys ...` |
| action `pr` | `pr-open --pr <url> --issue <url> --keys ...` |
| action `escalate` or verdict `infra-unrecovered` | `escalated --note '<what and to which lane>'` |
| verdict `needs-evidence` | `escalated --note 'needs-evidence: <what is missing>'` and tell the user |

## 5. Follow through

```sh
python3 $L show
```

For every non-terminal entry:

| status | check | then |
|---|---|---|
| `awaiting-retry` | `python3 $F status <n>` — the retried job's newest attempt | passed → `infra-recovered`; failed identically → `escalated`, report |
| `issue-open` | `python3 $L greens --since <n> --key <key> < last-list.json` | 3 greens → comment the three links, tell the user it can close; key red again → comment on the issue, mark the new build `grouped-with:<n>` |
| `pr-open` | `gh pr view <url> --repo mslsrc/ome --json state,isDraft,mergedAt,statusCheckRollup` | merged → `closed`; red checks → report; nothing else |
| `escalated` | none | repeat the one-line reminder in the iteration reply until the user acknowledges, then `closed` |
| `triage-dispatched` older than one iteration | the subagent's result | apply step 4; a worker that returned nothing → re-dispatch once, else `escalated` |

## 6. Report and log

```sh
python3 $L log "latest b<n> <state>; new: <n1 keys...>|none; followed: <n2 status>..."
```

Reply shape, five lines or fewer when nothing happened:

```
main: latest b<n> <passed|failed|running> (<sha8>)
new: b<n> kind-gke-ratio-core — code/ginkgo-spec, real (new) — issue <url>
new: none since b<n>
follow-up: b<m> issue-open → 3 greens, can close; b<k> awaiting-retry → recovered
escalated: b<j> kind-gke-* infra/dind-sidecar repeats on two agents — needs the CI-infrastructure owner
```
