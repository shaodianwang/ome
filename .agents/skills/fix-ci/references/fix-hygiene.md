# Fix hygiene

Read before branching, committing, pushing, retrying, or quarantining.

## Do not duplicate an open fix

```sh
gh pr list --repo mslsrc/ome --state open --search "<error keyword or step key>"
gh pr list --repo mslsrc/ome --state open --search "[CI/Tests]"
```

If the user is fixing an existing PR's CI, commit to that PR's branch. A fix
for red `main` gets its own branch from a fresh `origin/main`:

```sh
git fetch origin && git checkout -b <type>/<desc> origin/main     # or <username>/<desc>
```

Branch names are enforced by the pre-commit hook (`<type>/<desc>` or
`<username>/<desc>`).

## Commits

- New commit per fix round; do not amend a pushed commit. The one exception is
  the `git-author-email` step: it rejects commits whose author is not a GitHub
  noreply address, and only rewriting those commits fixes it:
  ```sh
  git config --global user.email "$(gh api user --jq '"\(.id)+\(.login)@users.noreply.github.com"')"
  git commit --amend --reset-author --no-edit          # single offending commit; otherwise rebase with --reset-author
  ```
- PR title prefix: `[Bugfix]`, `[Core]`, `[API]`, `[Helm]`, `[Docs]`,
  `[CI/Tests]`, `[Misc]`, `[OEP]`; under 52 characters.
- Open new PRs as drafts until the local gates and the Buildkite checks are
  green: `gh pr create --draft --repo mslsrc/ome ...`.

## What the code must respect

- No new hardcoded behavioural value (hostnames, ports, timeouts, replica
  defaults, image refs, feature toggles) in Go: it goes in config (the
  `inferenceservice-config` ConfigMap, Helm values, flags, CRD fields). A fix
  that needs one must say so explicitly and justify it.
- Leak-free by default: neutral hostnames (`example.com`), no internal cluster,
  environment, people, team, tracker, codename or registry names in code,
  comments, fixtures or test names. `internal-tokens` enforces the mechanical
  part; passing it is necessary, not sufficient.
- Comments state the invariant, never the change, the incident, the build or
  the PR number. Test names likewise.
- API type changes (`pkg/apis/ome/v1beta1/`) require `make manifests && make
  generate`; `go.mod` changes require `make tidy`; chart template changes
  require `make helm-lint`; a bump of the vendored optional CRD modules
  requires `make dep-crds`.

## Local gates before every push

```sh
make fmt && make vet && make ci-lint && make test-no-xet
```

Add the targeted gate for what changed: `python3 -m unittest discover -s
.buildkite -p 'test_*.py'` (`.buildkite/`), `python3 hack/check_internal_tokens.py
--tree` (any text), `make rolloutspec-check MACHINE=instance` (rollout tables),
`make helm-lint` (charts). `pre-commit run --all-files` runs the hook set
(gofmt, vet, helm lint and template, codespell, tidy, author email, internal
tokens, branch name).

## Retrying

- Retry the narrowest thing: one job, newest attempt, via
  `scripts/ci_build.py retry <build> <key>` (or the Buildkite UI). A retried
  job cannot be retried again; retry its successor.
- One manual retry per infrastructure signature. If it fails the same way,
  stop retrying and escalate CI infrastructure with the evidence.
- Never retry a red spec or a red Go test to make it green. If it is green on
  retry with no code change, it is a defect with an owner.
- A push supersedes retries: the new head SHA gets its own build.

## Quarantining a KIND spec

When a coordination spec fails on `main` without an engine change, or passes on
retry without one, the repository's mechanism is `KIND_QUARANTINE` in
`.buildkite/generate_pipeline.py`: remove the stem from its `KIND_SPEC_GROUPS`
entry and add `{"spec": "<stem>_kind_test", "owner": "<github login>",
"expires": "<YYYY-MM-DD, two weeks>", "reason": "<one line>"}`. The spec then
runs in the soft-fail quarantine job on every build, and generation fails the
day after `expires` until the spec is fixed and moved back. Ask before
quarantining; it changes what gates every PR.

## Escalation

Hand off, with the build link, the job, the decisive log line and what was
tried, when: an infrastructure signature repeats after one retry; a GKE queue
has no capacity for a long time; a registry or secret is broken; a product
defect is outside the PR's scope (open an issue with the evidence instead of
widening the PR).
