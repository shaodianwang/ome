# Opening a GitHub issue for a product regression

Only `product/*` findings, and `needs-evidence/*` findings whose evidence
points at OME after the dig, become issues. Infrastructure and environment
findings go to their owners in the summary, not into the OME tracker.

## Identity

The issue is filed by you, under your own GitHub login, never with a CI or
bot token. Before filing:

```sh
env | grep -E '^(GH_TOKEN|GITHUB_TOKEN)=' && echo "unset these first: a token in the environment overrides your login"
gh auth status --hostname github.com
```

`gh auth status` must show your account as the active login for
`github.com`. If it shows anything else, stop.

## Deduplicate first

```sh
gh issue list --repo mslsrc/ome --state open --search '"<distinctive log fragment>"' --limit 20
gh issue list --repo mslsrc/ome --state open --search 'daily RC <stage>' --limit 20
```

If an open issue matches the signature, add a comment with today's build
link, candidate SHA and the quoted line instead of opening another.

## Labels

Use only labels that exist; check first and pick the closest:

```sh
gh label list --repo mslsrc/ome --limit 100
```

If none fits, file without labels rather than inventing one.

## Title and body

Title under 70 characters, prefixed like a PR: `[CI/Tests] daily RC: <stage> — <one-phrase symptom>`
(for example `[CI/Tests] daily RC: cpu-smoke pods ignore runtime schedulerName`).

```sh
BODY="$(mktemp -t ome-issue-XXXXXX.md)"
cat >"$BODY" <<'EOF'
## What failed

Daily RC build <bN> (<buildkite url>), stage `<step key>`, candidate `<sha8>` (<commit url>).

## Evidence

- Log line <n>: `<exact quoted line>` (<job url>)
- <artifact or describe excerpt> (<artifact url or "attached below">)
- RC was running the candidate: `release-images: manager=... scheduler=... agent=...`

## Why this is a product regression

<the decision rule that applied: converged RC, script assertion fired, no capacity or storage symptom; previous daily build <bM> on candidate <sha8> passed the same stage>

## Suspect change window

Commits between the pinned production release and this candidate: <link to the GitOps PR body or `git log <old>..<new>` output>

## Reproduction

`.buildkite/run_daily_rc_smoke.sh` (or the TPU script) against a cluster running `<sha8>`; the assertion is `<script line>`.
EOF
gh issue create --repo mslsrc/ome --title "<title>" --body-file "$BODY" [--label "<existing label>"]
rm -f "$BODY"
```

Paste the issue URL into the summary's dispositions table.

## Boundaries

- Never merge anything: not the GitOps promotion PR, not an OME PR, not
  a fix you wrote yourself.
- Never close, reassign or relabel issues you did not open in this shift.
- Never attach secrets, kubeconfigs or full agent environment dumps; the
  cleaned log excerpt is enough.
- No names of people, chat threads or tracker numbers from other systems.
