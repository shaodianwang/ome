---
name: oss-sync
description: >-
  Sync internal mslsrc/ome changes to the public ome-projects/ome repo
  (sigs.k8s.io/ome). Use when the user asks to "upstream", "sync to OSS",
  "port to public", or to check internal↔OSS drift. Covers the writable
  clone, the per-batch workflow, the hard blocklist of internal-only
  code, CI failure playbooks, and this Mac's sandbox workarounds.
---

# OSS Sync (mslsrc/ome → ome-projects/ome)

## Weekly run (default when invoked with no arguments)

1. Read `STATE.md` (next to this file) for the internal watermark and
   the pending list. Fetch both mains. The triage list is
   `git log <watermark>..origin/main --oneline --stat` on the internal
   repo — commit-driven, not a full tree re-scan. Classify each commit:
   sync / blocked (see blocklist) / already-in-flight on an open OSS PR.
2. Spot-check with the tree drift check (below) only for dirs the
   triage touched — it catches copy mistakes the commit log can't.
3. Report to the user first: the triage table, proposed batch
   boundaries, and blocked items. Do not push or open PRs before the
   report unless the user pre-approved in the invocation.
4. On approval, run the per-batch workflow below.
5. **At the end, update STATE.md**: new watermark (the internal SHA the
   sync processed through), fresh OSS main SHA, refreshed pending list,
   one history line.

Internal is the source of truth. OSS tests get fixed to match internal
behavior — never bend internal code to satisfy a stale OSS test. The one
exception: when OSS has something genuinely missing internally (a test, a
bug fix like the LWSName truncation), reverse-port it INTO internal first,
then copy outward. Internal stays a strict superset.

## Setup (already in place on this Mac)

- **Agent workspace root: `~/.cache/ome-agent.noindex/`** (sandbox-writable,
  Spotlight-excluded by the suffix). Clones, worktrees, scratch files and
  build caches ALL go there — NEVER under the repo's `.claude/` (a dozen
  worktrees plus 6 GB of module cache there froze the owner's IDE on
  2026-09-19). Worktrees: `git worktree add ~/.cache/ome-agent.noindex/wt/<name> ...`;
  scratch: `~/.cache/ome-agent.noindex/scratch/<run>/`. Remove worktrees with
  `git worktree remove` when a PR is merged.
- **Writable OSS clone**: `~/.cache/ome-agent.noindex/oss-ome`
  - `origin` = https://github.com/ome-projects/ome.git — push works via
    HTTPS + gh keychain directly.
  - `local` = the user's read-only checkout at
    `~/go/src/github.com/ome-projects/ome` (agent sandbox cannot write there).
- **Internal pushes**: `GIT_SSH_COMMAND="ssh -F $HOME/.fb-3pai/ssh/github.config -o UserKnownHostsFile=/tmp/gh_known_hosts" git push ...`
  Rebuild the pinned known_hosts whenever /tmp was cleaned:
  `gh api meta -q '.ssh_keys[]' | sed 's/^/github.com /; p; s/^github.com /ssh.github.com /' | sort -u > /tmp/gh_known_hosts`

## Identity and commit rules (OSS side)

- Author = `Simo Lin <25425177+slin1237@users.noreply.github.com>`
  (clone's git config is already set). Never a corp email.
- **DCO**: every OSS commit uses `git commit -s`.
- **Co-authors: EVERY OSS commit carries these two trailers** (owner rule,
  2026-09-03), after the Signed-off-by line:
  `Co-authored-by: Fan Yang <250624800+fanyang-real@users.noreply.github.com>`
  `Co-authored-by: Wei Huang <1425903+Huang-Wei@users.noreply.github.com>`
  Add them with `git commit -s --trailer "Co-authored-by: ..."` (twice) or
  `--amend --no-edit --trailer ...` when rewriting a tip; single-commit PRs
  keep them through GitHub's squash merge. Additional co-authors only when
  the user asks, always in GitHub noreply form
  (`<id>+<login>@users.noreply.github.com`). Resolve identities from a
  commit: `gh api repos/mslsrc/ome/commits/<sha> -q '.author.login + " " + (.author.id|tostring)'`.
  Known: Xudong Ma `1962333+madongfly`, Yunfan Wang `7553253+yun-fan-wang`,
  Nishant Gupta `261326649+nishantgpt-lab`, Alexander Matyushentsev
  `426437+alexmt`, Shaodian Wang `255224151+shaodianwang`,
  Sladyn = `sladyn98` (NOT the unrelated GitHub user "slydan").
- Never any AI attribution.
- **PR titles/bodies describe the change functionally** ("[Core] validation:
  add rollout budget validators") — never "sync from primary tree", never
  any wording implying an out-of-band source repo, never Meta mentions.
- **PR descriptions are SHORT plain summaries (owner rule, 2026-09-14):**
  a few sentences or a short bullet list of what changes and why. Never
  paste the commit body, never include the Signed-off-by / Co-authored-by
  block — the trailers live only in the commit, and a single-commit squash
  carries them (verified on the #851/#852 squashes; both repos squash
  with PR_TITLE + blank body). Nothing in the description may read like
  an import from another tree.
- **Internal ports of upstream fixes carry NO external co-authors** (owner
  rule, 2026-09-14): author = the clone config, provenance = one sentence
  "Same fix as ome-projects/ome#N" in the body. Internal commits do not
  use Signed-off-by.
- Titles ≤52 chars with the repo prefixes ([Core], [Bugfix], [Helm], ...).

## HARD BLOCKLIST — never upstream

- **cluster_cache / sharded model cache** (user rule, absolute):
  `pkg/controller/v1beta1/basemodel/backends/sharded/` and every wire-up —
  manager `BuildProvider`/`NewModelCacheConfig`, controller `ModelCache`
  field, `base.go` clusterCacheEnvVars / modelCacheEnvVarsForOverlays /
  modelCacheEndpointHostPort, chart `modelCache` values + configmap block,
  `ModelCacheConfigName`. On OSS, `distribution: Sharded` models take the
  per-node path. Sweep EVERY batch for `sharded|cluster_cache|ClusterCache`.
- **`meta-internal/`** (owner rule, 2026-09-23, absolute): the
  deployment-specific extensions compiled in only under the `meta_internal`
  build tag (`capacityformat`, `xdspublisher`, and anything added there
  later) and EVERY wire-up — `cmd/manager/register_meta_internal.go` (the
  blank-import registration file, the one thing under `cmd/` that never
  flows upstream), the Makefile `GO_BUILD_TAGS ?= meta_internal` block and
  `test-meta-internal` target, `.buildkite/run_tests.sh`'s call to it, any
  Dockerfile `COPY meta-internal/` line, and any chart value, configmap key
  or config field that selects those extensions (the xDS publisher, the
  load-array capacity format). Upstream builds are untagged and must keep
  working without the directory. Sweep EVERY batch and every patch for
  `meta-internal|meta_internal|register_meta_internal|xdspublisher|capacityformat|loadarray`.
- **charts/muse**, **charts/ome-isvc** (built around internal vendor
  bundles: cks-gb300, internal ECR images), **values-usc1-*.yaml** and any
  per-cluster values overlays.
- **Internal-only pieces of the shipped charts** (owner rules, 2026-09-27):
  `charts/ome-resources/templates/alfred/` with the `alfred:` values block
  and README section (upstream carries its own `charts/ome-alfred`), and
  the `ome-viewer` ClusterRole (`templates/ome-controller/rbac/viewer_role.yaml`
  plus the `viewer:` values block), which grants internal bot access. When
  resolving README/values conflicts, keep the public copy of those blocks;
  sweep added chart lines for `ome-viewer|alfred:|alfred.`.
- **pkg/kubectl + cmd/kubectl-qt** — upstream drives its own CLI
  (kubectl-ome OEP + pkg/cli); porting ours would collide.
- `.buildkite/`, `hack/` internal tooling, `oeps/`, `grafana/`, `muse/`,
  internal dockerfiles (a Dockerfile may be ported only after de-forking
  its header/comments, like ome-scheduler.Dockerfile was).
- **tests/integration + KIND harness** — deferred wave. Needs the
  KIND_SPEC_GROUPS map extracted to a CI-neutral file + a tiered GHA
  matrix (PR smoke tier / main full tier / nightly perm harness) before
  it can go.
- Internal wire values that must NOT overwrite OSS: OSS keeps
  `DragonFly = "model_cache"` (internal uses `cluster_cache`) — swap
  `v1beta1.ModelCacheProviderClusterCache` → `v1beta1.DragonFly` and any
  `"cluster_cache"` fixture strings in copied tests.

- **Rollout behavior-lock coverage trailers** (`// Cells: instance/...` and
  `// Arrows: T-...` comment lines above Go tests, and any prose naming
  `tests/rolloutspec/...` files or the tests that live there): they index the
  machine tables under `tests/rolloutspec/`, which the public tree does not
  carry (tests wave). Strip them when copying — `perl -ni -e 'print unless
  /^\s*\/\/ (Cells|Arrows): /'` then collapse the dangling `//` separator
  left before the `func` — until the tables ship upstream (2026-09-24
  decision; 294 lines in 43 files that run). The 3-way merge keeps them
  stripped on later runs; only new trailers need the pass again.

## OSS-only surfaces — never wipe when copying

- pkg dirs internal consumes VIA the module pin (`require sigs.k8s.io/ome`
  + replace in internal go.mod): afero, alfred, cli, configutils, hfutil,
  imds, logging, migration, modelagent, modelconfig, modelver,
  ociobjectstore, principals, testing, vault, zipper. These are the
  canonical copies — internal has no in-tree versions.
- Benchmark feature: controller, webhook config, `BenchmarkJobConfig`/
  `PodConfig` in controllerconfig, benchmarkjob-config ConfigMap + values.
- Pod webhook `model-init` injector (+ its slot at the front of the
  mutator chain and the init-container ordering sort).
- enigma agent (internal/ome-agent/enigma) incl. its private
  getNodeInstanceType.
- Chart plumbing: instanceTypeMap helper/values/configmap key +
  INSTANCE_TYPE_MAP env, inferencereplica validator webhook file,
  `disableIstioVirtualHost` (until OSS drops it itself), servingSidecar
  configmap block, model-agent daemonset flags, chart version anchor
  (OSS `v1.2.2` scheme, not internal `v0.1.x`), `modelAgent.enabled: false`.
- `pkg/testing` (byte-identical ancestor of internal `pkg/utils/testing`
  base files; its importers are OSS-canonical dirs).

## Per-batch workflow

Two methods, pick by range shape. **Per-commit patch stack** (2026-09-16
run) when commits are small and separable: `git format-patch -1 <sha> --
<allowed paths> | sed module-rewrite | git apply --3way`, one squashed
commit per PR. **Tree-state sync** (2026-09-24 run, 150 commits with a
300-file restructure): detached worktrees for internal@watermark
(`int-base`) and internal@tip (`int-tip`) under `~/.cache/ome-agent.noindex/wt/`,
a per-file plan (COPY / MERGE3 = `git merge-file -p ours base theirs` on
normalized files / DELETE-MOVED when the OSS file existed at the watermark
internally / KEEP-OSS-ONLY otherwise / GENERATED / CARVE), builder agents
per batch with the plan inline, then parity check (`norm(int-tip/f)` vs
OSS file: residuals must be carves, OSS-only surfaces, leak rewrites, or
import order only), the deleted-content audit, carve grep, codespell with
the repo `-L` list, and a fresh-eyes read of the added hunks (`git diff
-M50%` to skip pure moves) before pushing. Compile coupling usually forces
ONE code PR + ONE charts PR stacked on it; do not promise finer slices
before checking that each slice compiles alone.

1. Fetch both mains. Branch in the clone off `origin/main`.
2. **Drift check** (module-path-normalized). Per directory:
   ```sh
   INT=$HOME/go/src/github.com/mslsrc/ome
   while read f; do rel=${f#$INT/$d/};
     [ ! -f "$d/$rel" ] && echo "ONLY-INT $rel" && continue
     cmp -s <(sed 's|github.com/ome-projects/ome|sigs.k8s.io/ome|g' "$f") "$d/$rel" || echo "DIFF $rel"
   done < <(find $INT/$d -name '*.go' -not -name '*.orig')
   ```
   Known permanent diffs (skip): servingruntime_types.go (DragonFly),
   zz_generated (follows it), pod mutator + injector_error_test
   (model-init), validation_test (DragonFly fixtures), cmd/manager
   (sharded carve), import-order-only files.
3. **Reverse-port scan**: OSS-only files/symbols/test funcs in the target
   dirs. Port what targets shared behavior into internal FIRST (own
   internal PR); drop tests that pin behavior internal deliberately
   changed (document which, and why, in the internal PR body).
4. **Copy**: `sed 's|github.com/ome-projects/ome|sigs.k8s.io/ome|g'` per
   .go file; copy non-Go files too (READMEs, testdata binaries). Then
   ALWAYS `gofmt -w` (the rewrite breaks import sort order).
5. **Carve** blocklist items; weave preserved OSS-only surfaces back in.
6. **Deleted-content audit** (mandatory since the charts wipe):
   `git diff origin/main -- <paths> | grep '^-'` — triage every deleted
   line as OSS feature (restore), internal improvement (keep), or noise.
7. **Gates in the clone** (env below): `go build ./...`,
   `go vet ./pkg/... ./cmd/... ./internal/...` (test compilation!), the
   affected test packages plus consumers, codespell with the repo config:
   `PYTHONPATH=~/.cache/ome-agent.noindex/codespell-pkg python3 -m codespell_lib -L 'AfterAll,NotIn,UpToDate,WithT,allReady,rouge,trough' <files>`
   (install once: `pip3 install --target ~/.cache/ome-agent.noindex/codespell-pkg codespell`).
   Spelling fixes go INTERNAL-FIRST, then re-copy.
8. After API/webhook-marker/dep changes: OSS `make manifests` +
   `make generate` (k8s dep bumps change embedded PodSpec schemas in
   CRDs — always regen). Regenerate chart role.yaml via make manifests
   instead of hand-copying (restores OSS-only RBAC like benchmark).
9. Commit `-s` + the two co-author trailers, push, PR, watch CI ~9 min:
   `gh pr checks <n> --repo ome-projects/ome | awk -F'\t' '$2!="pass" && $2!="skipping"'`

## Confidentiality review — mandatory before every push (2026-09-25)

A push, a draft PR, a PR comment or a CI log on the public repo is a
publication; removing a disclosure later does not undo it. Before the FIRST
push of a branch and again before EVERY later push, PR edit or comment:

1. **Run the checker over the outgoing files** from the internal repo's
   pattern list (the public tree has no copy of it — never commit it there):
   ```sh
   git -C $OSS diff --name-only --diff-filter=d origin/main...HEAD \
     | xargs python3 $INT/hack/check_internal_tokens.py --root $OSS \
         --patterns $INT/hack/internal-tokens.txt --allow /dev/null --files
   ```
   It must print `clean (N files, 0 exemptions)` with N equal to the number
   of outgoing files. A path that does not resolve is a `[missing]` or
   `[unclassified]` finding (fail closed, #1705) — usually a wrong `--root`
   or cwd; before #1705 such paths were skipped silently, so treat a
   smaller N as a failure. Pipe through xargs (zsh does not word-split a
   `$var` holding the list). Only `--files` on the public
   tree: `--tree` is for the internal repo (the public tree has OSS-only
   top-level dirs such as `site/`, and OSS-only fixtures — the canonical
   example cloud keys and placeholder tokens in the CLI tests, public
   model-hub organisation names in modelconfig testdata — that the
   patterns flag; those files never come from the sync). Never create an
   allowlist on the public side: change the fixture or narrow the pattern
   internal-first. The checker covers hosts, clusters, people, trackers,
   codenames, storage/accounts, GitOps repos, team names, fleet phrases and
   design-document numbers; it does not cover meaning.
2. **Read the added hunks** (`git diff -M50% origin/main...HEAD`, skipping
   pure moves and generated files) for what regexes miss: internal
   techniques or architecture described without their name, model and
   checkpoint identities implied by shapes or numbers, capacity and topology
   of real clusters, internal design vocabulary used as fixtures, history
   and incident narrative, and references to artifacts the public tree does
   not carry. For a batch above ~2k added lines use a fresh-eyes reader
   agent with the class list and act on every ≥medium finding.
3. **Read everything else that will be published**: branch name, every
   commit message and its metadata, the PR title and body, comments,
   reproduction commands and test output. Public or synthetic examples only;
   never paste internal logs, review notes, or the source PR link. Author
   and co-author identities must be the verified public noreply forms in
   this file; never a corp alias.
4. **Fix internal-first, then mirror**, so the internal tree stays the
   superset; if a hunk cannot be separated from internal information, leave
   it out and keep the reason private. Never push first and plan to
   sanitize later.

The internal repo enforces the mechanical part of this on every commit
(pre-commit hook + CI step `internal-tokens`, merged 2026-09-25 as #1698 =
330d35fb; patterns in `hack/internal-tokens.txt`, nine deployment-only
exemptions in `hack/internal-tokens.allow`, deny by default everywhere
else including hack/ and the Makefile — the harness image refs became
`${OME_EVAL_*_IMAGE}` tokens with the mirror only in
`.buildkite/eval_image_mirror.sh`). Every internal commit at or after
330d35fb is mechanically clean under `--tree`, so the sync's own run
guards the copy step (files the sync creates or edits on the public side);
the review above is what the sync adds on top.

## Leak sweep — what the 2026-09-14 careful review taught

The regex classes live in `hack/internal-tokens.txt` (13 groups) and run
through the checker above — do not rebuild ad-hoc grep sweeps; extend the
pattern file internal-first when a sweep finds a new token class.
Name/host/ticket regexes are not enough. Read every added line for:
- **internal design vocabulary** used as fixtures or in comments: quota
  paths like `dedicated.<tenant>`, private nicknames for the control
  plane, private capacity-format names, option keys of private plugins;
- **fleet shape and numbers**: node counts, per-node specs, "4-chip TPU
  node", sentences about how many tenants share one namespace, hardware SKUs
  as cluster names (gb300/tpu → fast/slow), realistic-looking UIDs;
- **history and motivation**: "the case that motivated this", "the X
  incident", "before the flag existed", "the optional tree", first-person
  "we control";
- paths that look like real endpoints (`/health_deep` → `/healthz/deep`).
Fix internal-first, mirror, then re-grep the pushed tips. Also: codespell
reads identifiers (a camelCase name can split into dictionary words), so run it on new Go files.

- **Cascade guard (2026-09-17 near-miss):** before ANY `git rebase --onto`
  in a stack, resolve the parent PR's merge commit with `gh pr view <n>
  --json state,mergeCommit` and make `git merge-base --is-ancestor <squash>
  origin/main` FATAL (`|| exit 1`). "I merged one more" may mean an internal
  PR; rebasing a child onto main while its parent is still open replays it
  without the parent's commits (patch-id changes) and the grandchild conflicts.
  Always compare `git patch-id --stable` before and after a rebase.

## CI failure playbook

- **Lint**: OSS `.golangci.yml` was ported from internal (SA1019 Requeue
  + test-errcheck exclusions). New `unused` hits usually = helpers
  orphaned by test deletions. `result.Requeue` reads → `!result.IsZero()`.
- **Pre-commit go-mod-tidy**: trust CI's diff verbatim (it may promote
  test-only imports to direct requires). Re-check the
  **controller-runtime pin (v0.22.5)** after every tidy — a silent bump
  past 0.22 breaks keda (`webhook.Validator` removal was why keda went
  to v2.18.3).
- **Generated Code Drift**: rerun make manifests+generate; dep bumps are
  the usual cause.
- **CodeQL**: runs only on OSS; expect new alert classes on big copies
  (e.g. unchecked int→int32: fix internal-first with ParseInt(...,10,32)).
- **codespell**: iterates in waves as file sets change; add genuinely
  legitimate vocabulary (like `trough`) to BOTH repos' -L lists.

## Sandbox/env recipes (this Mac)

- Module caches and tool bins live under `~/.cache/ome-agent.noindex/` —
  NOT under /tmp (macOS reaps /tmp files not accessed for 3 days; module
  files vanish one by one → "no such file or directory" build failures),
  NOT under the repo (IDE indexing), and always inside a `.noindex` parent
  (Spotlight indexes caches under /Users otherwise; 22 mdworkers made
  `git status` hang for minutes). Per repo, so internal gates and OSS builds
  never race on one cache:
  `C=$HOME/.cache/ome-agent.noindex`
  OSS clone: `GOMODCACHE=$C/gomodcache-oss GOBIN=$C/gobin-oss`
  internal:  `GOMODCACHE=$C/gomodcache GOBIN=$C/gobin`
  plus, for every go command in either tree:
  `GOPROXY="file://$HOME/go/pkg/mod/cache/download,https://proxy.golang.org" GOSUMDB=off GOTOOLCHAIN=local PATH=$GOBIN:$PATH`
- envtest: `KUBEBUILDER_ASSETS="$($INT/bin/setup-envtest use "$(cat $INT/.envtest-k8s-version)" -p path)"`
- xet-linked tests: build once (`make -C pkg/xet build`), then copy
  `~/.cargo/target/release/libxet.*` into `pkg/xet/target/release/`
  (user's global CARGO_TARGET_DIR redirects cargo output) and export
  `LD_LIBRARY_PATH=$PWD/pkg/xet/target/release`.
- Internal `make generate` with the overlay cache: first
  `rm -rf .cache/go-mod` and
  `chmod -R u+w /tmp/gomodcache/github.com/ome-projects/ome@<pin>`
  (kube_codegen writes into the module-cache copy of the self-pin).
- Two known env-only OSS test failures (not regressions):
  `TestManagerSetup` (local port 18081 collision) and xet
  `TestConfig_Validate` (cache-dir env assumption).

## State (as of 2026-08-31)

Everything through the core is merged: apis+consumers, constants,
validation, selectors/parser/utils, webhooks+deps (k8s 0.35 / CR 0.22.5 /
lws 0.8.0 / keda 2.18.3), base controllers, ISVC core+workload+manager,
quota+scheduler (#807), charts (#809, #810). Remaining planned: the
agents batch (internal/ome-agent 26 files + cmd/ome-agent 6 +
pkg/xet download.go), then the tests/KIND wave. New internal merges since
those batches create fresh drift — run the drift check first, always.
