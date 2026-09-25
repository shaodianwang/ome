# GitHub issue and draft PR workflow

Everything is done with `gh` under your own login; `gh auth status` must
succeed for `mslsrc/ome` first. Write bodies to a file under scratch and pass
`--body-file`; never inline a multi-line body on the command line.

## Label

```sh
gh label list --repo mslsrc/ome --search ci-triaged
gh label create ci-triaged --repo mslsrc/ome --color D93F0B \
  --description "main-branch CI failure with an evidence-linked triage"     # once; skip non-fatally on 403
```

## Dedupe before opening

Same step key and same first error already open → comment, do not open:

```sh
gh issue list --repo mslsrc/ome --label ci-triaged --state open \
  --json number,title,url,body --jq '.[] | select(.body | contains("<!-- ci-triage step=<key> -->")) | "\(.number) \(.title) \(.url)"'
gh issue list --repo mslsrc/ome --state open --search "<step key> in:title"   # issues filed by people
gh pr list --repo mslsrc/ome --state open --search "<step key or error keyword>"   # a fix may already be up
```

## Open

```sh
OUT=~/.cache/ome-agent.noindex/scratch/ci-triage; mkdir -p "$OUT"
# write the write-up block (writeup-template.md) to "$OUT/b<n>-issue.md"
gh issue create --repo mslsrc/ome --label ci-triaged \
  --title "[CI] main b<n>: <step key> <class in two words>" \
  --body-file "$OUT/b<n>-issue.md"
```

Title shape: `[CI] main b<n>: <step key> <short class>` — for example
`[CI] main b2710: kind-gke-ratio-core spec failure` or
`[CI] main b2712: test coverage below threshold`. Under 70 characters.

## Update

```sh
gh issue comment <number> --repo mslsrc/ome --body-file "$OUT/b<n>-comment.md"
```

A comment for a repeat carries: the new build link, whether the first error
is identical (quote it if it differs), the new attempts count, and the
marker line. After three consecutive green standard main builds for the key,
comment with the three build links and say the issue can be closed; closing
is the operator's call.

## What the issue never contains

- People, usernames, team or oncall names; the PR number and title identify
  the change.
- Tokens, credentials, registry or cloud account identifiers echoed by a log.
- Internal cluster, namespace or hostnames from agent-pod evidence; describe
  the layer ("the agent pod's dind sidecar was missing") instead.
- Guesses presented as facts.

## Draft PR (mechanical fixes only)

A fix is mechanical when its correctness is decided by a make target, not by
judgement: a drift regeneration, a `KIND_SPEC_GROUPS` classification, a lint
auto-fix, a broken import or fixture path, a bounded timeout or retry
hardening with a one-line stated reason. Anything touching rollout
behaviour, API types, chart values, or test expectations is not mechanical:
write it up and stop.

```sh
git fetch origin && git checkout -b ci/<short-desc> origin/main       # or <username>/<desc>
# make the change; then the gates for what changed
make fmt && make vet && make ci-lint && make test-no-xet
python3 -m unittest discover -s .buildkite -p 'test_*.py'          # .buildkite/ touched
make rolloutspec-check                                            # rollout tables touched
python3 hack/check_internal_tokens.py --tree                      # any text touched
git push -u origin HEAD
gh pr create --draft --repo mslsrc/ome --title "[CI/Tests] <what>" --body-file "$OUT/b<n>-pr.md"
```

Title prefix `[CI/Tests]` for pipeline, test or tooling fixes, `[Bugfix]` for
a product-code fix; under 52 characters. Body: Why / What / Verification,
linking the issue; nothing else. Code comments state the invariant, never
the build number or the incident. Leave the PR a draft; a person reviews
and lands it. Never merge.
