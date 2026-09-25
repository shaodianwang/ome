# Write-up template

One write-up per breakage, posted in the reply first and then, when the
failure is real or unresolved, as a `ci-triaged` issue or an issue comment.
Every line that is a claim carries a link or a quoted log line. Names of
people never appear; suspects are PR numbers and titles.

```markdown
## main build <n> — <step key>[, <step key>] failed

**Build:** <buildkite build url> · commit `<sha8>` (<first line of the merge message>, PR #<n>)
**Previous standard main build:** b<n-1> <passed|failed> `<sha8>` · compare: <github compare url>
**Failed jobs (newest attempts):**
| step key | class | exit | attempts | agent / queue | job |
|---|---|---|---|---|---|
| `kind-gke-<slug>` | code/ginkgo-spec | 1 | 1 | <agent> / gke-tpu-v7x | <job url> |

**First error** (`<step key>`, stage `--- <stage> ---`):
```
<at most ten cleaned log lines, the first real error first>
```

**Verdict:** <flaky-known | real, standing since b<n> | real, new | infra, recovered on retry | infra, unrecovered | needs-evidence>
**Basis:** <previous build state for this key; known-flake match or not; whether the range touches the failing area; retry outcome>

**Suspects (ranked):**
1. PR #<n> "<title>" — <mechanistic link: file/symbol touched → failing assertion>
2. <or: "none; the range does not touch <area>; reproduction needed">

**Action taken:** <retry of job <id> → <result> | issue <url> opened/updated | draft PR <url> | none>
**Recommended next step:** <one sentence: who does what, gated by which make target>

<!-- ci-triage step=<step key> -->
```

Rules for the block:

- Quote the FIRST real error; the trailing exit-status line is not it.
- `class` uses the names in `classification.md`; `verdict` uses exactly one of
  the listed phrases so the loop can record it.
- For a KIND child, name the spec file and the `It` text; for an eval shard,
  the case ids from the `NON-PASS:` list.
- Speculation is labelled "speculation"; a suspect without a mechanistic
  link stays a suspect.
- The marker line is the last line of anything posted to GitHub; it is how a
  later triage (or the oncall loop after a lost state file) recognises a
  handled step key.

## Result block for the oncall loop

When run as a worker, end the reply with one machine-readable line so the
loop can `mark` the build without re-reading the prose:

```
RESULT build=<n> keys=<k1,k2> class=<class> verdict=<flaky-known|real|infra-recovered|infra-unrecovered|needs-evidence> action=<none|retry|issue|pr|escalate> issue=<url|-> pr=<url|-> note="<one short sentence>"
```
