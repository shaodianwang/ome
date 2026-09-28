# OSS sync state — updated at the end of every completed sync

Read this FIRST on every run. The internal watermark is the contract:
every internal main commit up to and including it has been triaged
(synced, in-flight on an open OSS PR, or deliberately blocked). The
weekly triage list is exactly:

    git log <internal-watermark>..origin/main --oneline --stat

## Watermark

- internal (mslsrc/ome) main: cf6dc468  # [Misc] tests: neutral fixture name, spelling fix (#1799)
- oss (ome-projects/ome) main at last sync: 653ee9cc  # #1135 squash — #1134 (code) + #1135 (charts) merged 2026-09-27
- state written: 2026-09-27

## Last sync (2026-09-27) — COMPLETE

Range 624955e7..cf6dc468 (52 internal commits, 37 of them touching shipped
paths: the placement-plan and fleet-capacity wave #1718-#1744, RDMA fixes
#1686/#1798, cluster affinity API #1723, lifecycle generation #1719,
scheduler metrics #1730, publication fallback #1697, hygiene #1690-#1694).
Built from a dry run of the candidate tool on the automation branch
(`hack/oss-sync/candidate.py`, branch ci/oss-sync-automation-v2; the
tooling is NOT on main because its PR #1726 was closed by the owner) plus
hand work: its 10 set-aside conflicts (public copy kept for the Alfred
blocks of the ome-resources README/values, the model-agent daemonset
flags, the status_writer approvals for absent tests/ files, one
`// Cells:` trailer and the rdma_injector comment wording; internal copy
for workload/revision/revision_test.go, placement/controller.go and
controller_test.go), removal of the two public-only characterization
tests superseded by #1691's ports (workload/ops and inferencereplica
`*_source_identity_characterization_test.go`), the go.mod bumps that
matched the public lines (x/sys, x/term, k8s.io/component-helpers added
by tidy; the dependabot bumps of oci-go-sdk, ginkgo, istio client-go and
validator did not apply), OSS `make generate` + `make manifests`.
Internal-first before the push: #1799 (fixture `example-router-standard`,
`unparsable`), so shipped paths of internal cf6dc468 and the candidate
are identical. Two OSS PRs, drafts under the owner's identity with the two
trailers, opened after the go and MERGED by the owner the same evening:

| OSS PR | branch | content | squash |
|---|---|---|---|
| #1134 | core/placement-plans-capacity | code + regenerated CRDs/openapi (109 files, 2 deletions) | 4fab7585 |
| #1135 | helm/quota-manager-report-interval | quota-manager `capacity.reportInterval` + render tests, placement timeout comments (5 files) | 653ee9cc |

Owner decision 2026-09-27: the `ome-viewer` ClusterRole from #1793
(`viewer_role.yaml` + the `viewer:` values block) is internal bot access and
is NEVER shipped — dropped from #1135, recorded in the SKILL.md blocklist
(with the Alfred templates and per-cluster overlays) and in the automation
branch's `config.NEVER_SHIPPED` / `CARVE_SWEEP`.

Gates before the push: build, vet, unit tests of the 22 touched packages
plus the scheduler module on its own, identifier gate clean (109 and 114
outgoing files), codespell, carve sweep, chart render tests + helm lint;
fresh-eyes read of all 13.4k added lines → one medium (the fixture name,
fixed via #1799) and four lows (pending below).

Blocked this range (never shipped): OEPs #1689 #1696 #1699 #1717, docs
#1746, CI/harness #1705 #1711 #1712 #1714 #1715 #1735 and the hack/.buildkite
parts of #1698, agent skills #1707 #1741 #1781, tests/ #1695 and the Alfred
KIND suite #1688, the meta-internal parts of #1700/#1701, the viewer role
of #1793.

## Last sync (2026-09-24/25) — COMPLETE

Range bbacdfa0..624955e7 (150 internal commits; the workload restructure
#1641/#1646/#1648/#1649 makes per-commit patching impossible, so this run is
a TREE-STATE sync: per-file 3-way merge, base = internal@bbacdfa0
(`~/.cache/ome-agent.noindex/wt/int-base`), theirs = internal@624955e7
(`wt/int-tip`), ours = OSS main b8e12913; new files copied, 106 files the
restructure renamed deleted upstream, 23 genuine OSS-only files kept,
generated files regenerated upstream). Owner decisions 2026-09-24: port
upstream #989 (RDMA profiles no longer privileged) internally → PR #1686
(MERGED 2026-09-24); port upstream #987 (preset webhook scope) + #776
(model-agent verification flag) → PR #1687 (MERGED 2026-09-24) — both landed
AFTER the tip this run syncs from (624955e7), so the next drift check will
show three tiny residuals to reconcile internal-wins: the RDMA chart comment
wording (internal dropped the "(#833)" issue ref), and the placement of the
model-agent flag / preset render-test block in the internal chart files; internal/ome-agent + cmd/ome-agent are OUT of this run
(bidirectional divergence; own reconciliation run later); upstream #925's
two characterization tests are adapted on the OSS side and ported internal
afterwards if they still apply. OSS PRs planned: A = one code PR
"[Core] workload engine, routing and API updates" (branch
core/engine-routing-refresh, worktree wt/oss-A, plan $S/A.plan.txt where
S=~/.cache/ome-agent.noindex/scratch/sync-2026-09-24), C = charts +
config/configmap "[Helm] routing, rollout tunables and RBAC updates"
(branch helm/routing-tunables-rbac, wt/oss-C), stacked C on A. Status
2026-09-24 evening: A built = 8873487e (510 files; builder journal
$S/journal.A.md; parity residuals = known carves + import order only;
codespell residuals fixed both sides → internal PR #1690), C built =
rebased onto A cleanly (12 files, render tests green); upstream #925's two
characterization tests adapted on A and ported internally as PR #1691;
fresh-eyes leak audit of A found ~60 narrative comments (fixed internal-first as PR #1692 and mirrored) and 294 `// Cells:`/`// Arrows:` rollout-table coverage trailers (stripped upstream; rule in SKILL.md). OSS #996 = A MERGED 2026-09-24 (squash 4dfb96b1; after lint/CodeQL fix-ups the branch tip was dcfb4135 and the squash tree matches it), OSS #997 = C MERGED 2026-09-25 (squash f3c6693b, trailers intact). Internal side PRs: #1690 spelling, #1691 upstream #925 characterization tests, #1692 comments. Watermark → 624955e7. Internal side still open at close-out: #1690 spelling, #1691 upstream #925 tests, #1692 comments (rebased once after main moved), #1693 bounded parse, #1698 internal-identifier check + CLAUDE.md boundaries rewrite (adds the pre-commit hook and the `internal-tokens` CI step). Blocked
this range: 30 OEP commits, hack, .buildkite, tests/ (replay arrows,
rolloutspec, KIND), all meta-internal xDS commits and their parts of
#1510/#1590/#1592/#1595/#1667/#1669, grafana, muse, ome-client-rs,
dockerfiles, CLAUDE.md, dependabot otel bumps, pkg/kubectl,
pkg/utils/testing, sharded backend, the Alfred templates internal added to
ome-resources (#1676; upstream keeps charts/ome-alfred), values-usc1.

## Last sync (2026-09-16/17) — COMPLETE

Range 654cde4f..bbacdfa0 (31 internal commits). Upstream bc0cf3ef..bc518a14
was 24 pkg/cli + pkg/alfred commits (OSS-only, nothing to port back). Six
OSS PRs, ALL MERGED by the owner 2026-09-17 in this order (squash, one
commit each, DCO + the two co-author trailers, short plain descriptions,
full CI green before each merge):

| OSS PR | branch | content | squash |
|---|---|---|---|
| #950 H | helm/prometheus-ipv6-port-relabel | #1517 | b25f6434 |
| #951 R | core/routing-publisher-selectable | #1485 #1487 | b17885b0 |
| #952 A | core/workload-terminal-recovery-and-rejections | #1494 #1489 #1491 #1490 #1377 #1378 #1501 | d45a913a |
| #953 B | core/inferencereplica-columnar-status-codec | #1496 #1497 #1500 #1502 #1505 (pkg part) | 751f6171 |
| #954 D | core/canary-unit-shape-and-strategy-defaults | #1480 #1486 #1513 #1516 | 75b33629 |
| #955 E | core/columnar-status-default | #1504 (merged as-is; kubectl-ome/Alfred decoder follow-up in the PR body) | f6e1f7f7 |

Method: the range was chronologically interleaved (workload fixes, the
ColumnarV2 codec and the canary changes share files), so A→B→D→E were
built as ONE chronological stack by per-commit patch application
(`git format-patch -1 <sha> -- <allowed paths> | sed module-rewrite |
git apply --3way`), one squashed commit per PR cut at the boundaries
above, generated files regenerated per PR; R and H were independent.
Recipe `.claude/oss-sync-scratch/RECIPE-STACK.md`, ledger LEDGER.md
("RUN 2026-09-16"), journals stack.journal.md / r1_routing.journal.md /
h1_prom.journal.md. Stacked PRs get only 2 CI checks until GitHub
retargets them to main after the parent squash; cascade rebases after
each squash kept patch-ids equal.

Internal side: #1521 (fde9bede) = ONE generically worded hygiene commit
(the re-filed third commit of last run's #1481 plus this run's audit
fixes: fleet table → generic example, "dozens" wording, dedicated./hub
fixtures, incident-narrative fixture prose, OEP identifiers in test
names, stale helper comments, /health_deep → /healthz/deep). Its content
was mirrored into #951-#955 — the next drift check should show it
identical; do not re-port.

OSS-only adaptations carried in the stack: B status_inventory_test.go
classification `rawReaderOutsideManager` for pkg/cli + pkg/alfred
readers, preflight docs sentence dropped, Makefile ome-status-preflight
target without the docs echo; D four pkg/cli files EffectiveRollout →
rollout.Effective, canaryevidence fixture engine+decoder; E inventory
approvals for internal-only tests/ files removed.

Blocked this range: OEPs #1495 #1472 #1498 (+hack statemachine +
tests/rolloutspec), #1488 meta-internal xds publisher, #1515, #1518,
grafana d4e9b5f9, docs/omenative-status-representation.md + README
Operations hunk (no docs tree upstream; docs wave).

Lessons (new this run): the cascade guard MUST be fatal — "merged one
more" meant internal #1521, my script's ancestor check did not exit, D
was rebased without B (patch-id changed) and E conflicted; aborted,
branches restored from the recorded tips, nothing pushed (rule now in
SKILL.md). Verify every owner squash's content afterwards (#1481 had
dropped a commit). Audit findings were mostly incident narrative in
fixtures, OEP identifiers in test names and stale comments — all fixed
internal-first and mirrored. The prometheus audit surfaced a real bug
(bare-IPv6 relabel, pending below). OSS-only consumers (pkg/cli,
pkg/alfred) read IR status rows raw — any wire change needs a consumer
sweep. Before fast-forwarding the internal checkout, check that HEAD is
main: the owner works on feature branches in that checkout.

## Last sync (2026-09-14/15) — COMPLETE

Range ddc08ead..654cde4f (75 internal commits; OSS #853 had closed the
2026-09-05 item). Nine OSS PRs, ALL MERGED 2026-09-15 (squash, --admin on
the owner's instruction, in this order; each one commit, DCO + the two
co-author trailers; PR descriptions are short plain summaries and carry
NO trailers — a single-commit squash takes them from the commit):

| OSS PR | branch | content | squash |
|---|---|---|---|
| #912 | helm/prometheus-revision-label-scrape-configs | #1441 | bba38395 |
| #913 | core/leader-election-timing | #1422 | c5cd179a |
| #914 | bugfix/runtime-event-fanout-inheritance | #1421 | c6ec681d |
| #916 | bugfix/placement-preserve-routes-on-reconnect | #1479 (landed mid-run) | 7fd99184 |
| #919 | core/rollout-progress-surge-canary-fixes | 2356a934 #1423 #1448 #1438 + mid-run #1471 #1453 #1439 | deab4908 |
| #915 | core/revision-hash-annotation-exclusions | #1442 #1449 #1447 | 075c6395 |
| #917 | core/kueue-gate-accelerator-pods | #1464 | fcc19c3a |
| #920 | core/multicluster-routing-trafficmap | #1425-#1478 series (meta-internal carved) | 81623e1d |
| #918 | core/quota-tenancy-by-name | #1434 #1429 #1462 (breaking AQ API) | bc0cf3ef |

Internal side, all merged by the owner: #1482 (port of OSS #873, webhook
admits terminating ISVC), #1483 (port of OSS #868, destinationrules RBAC),
#1484 (port of OSS #866, basemodel orphan ConfigMap GC), #1481 hygiene
— CAUTION: its squash (54f4dd7c) carries only the first two commits
(MKS/OEP comment neutralization, unparsable spelling, msl-mks-ome-lco
fixtures, usc1-1 incident comment, homeState → homeProbeState). The
third commit (fleet-sizing table → generic example, "dozens routinely
share one namespace" wording, dedicated.team-a / control-plane-nickname fixtures,
routing test wording, synthetic UID) was mirrored into OSS #915/#918/#920
but did NOT land internally; re-filed 2026-09-16 as branch
misc/neutral-wording-quota-routing-tests (cherry-pick of 950d0993). NO external co-authors on internal commits
(owner rule); OSS commits credit only Fan Yang + Wei Huang.

Blocked this range: 24 OEP commits, #1351 + kwok/demo hack fixtures,
#1417 (.buildkite), #1424 (CLAUDE.md), #1477 (ome-client-rs, absent
upstream), #1460 grafana, meta-internal/ + register_meta_internal.go +
Makefile GO_BUILD_TAGS from #1473, pkg/kubectl + docs parts of #1434,
every tests/integration spec, 10 dependabot bumps (OSS dependabot owns
its go.mod; istio 1.19.4 vs 1.31.0 gap → upstream PR #764).

Lessons (new this run): per-commit patching for files another batch also
touches (a whole-file copy smuggles the other batch's hunks); the owner
merges without waiting for the coordinator, so never assume order; the
owner-requested careful leak review after the batch audits still found a
borderline-major comment ("the optional tree / deletion contract") plus
fleet-shape wording and internal design vocabulary in fixtures
(dedicated.<tenant>, hub-*) — add "internal design vocabulary" and
"fleet numbers" to every future leak sweep; codespell reads identifiers
(homeState → "home state"); builders that background heavy jobs stall
until nudged; zsh does not word-split unquoted variables (multi-package
`go test $pkgs` silently ran nothing).

## Known pending above/around the watermark

- Landed after cf6dc468: nothing at close-out (2026-09-27 evening); the
  next triage is `git log cf6dc468..origin/main`.
- Follow-ups from the 2026-09-27 review (internal-first, then mirror; none
  blocking): rolloutrun.go / rolloutrun_test.go comments telling the "zero
  Progression fails every status write" story → state the invariant;
  placement/controller.go "(graceful degradation, no magic literal in the
  hot path)" → "zero falls back to DefaultPlaceTimeout"; the ome-resources
  render-test comment retelling the preset-webhook rollout ordering → state
  the failurePolicy invariant. The dependabot bumps in the range
  (oci-go-sdk 65.126.0, ginkgo 2.33.0, istio client-go 1.31.1, validator
  10.30.5) did not match the public go.mod lines — check next run whether
  upstream is ahead or behind. `pkg/xet` and `scheduler/go.mod` had no
  diff in range. The sync tooling (`hack/oss-sync/`: report, candidate,
  App token helper, carve config) lives only on the closed #1726 branch —
  decide whether to revive the automation.
- Internal follow-ups found by the 2026-09-16 audits (internal-first, then
  mirror): prometheus relabel regex mangles a bare IPv6 pod IP for
  port-less containers (`fd00::1` + 9090 → `fd00::9090`; rewrite from
  `__meta_kubernetes_pod_ip` or host group `(\[[^\]]+\]|[^:]+)`, add a
  bare-IPv6 test); chart `routing.probe: []` / `routing.publisher: []`
  placeholders should be `{}` (helm coalesce warnings); "the existing
  endpoint reconciler/publisher" wording in the routing publisher code;
  `config/configmap/inferenceservice.yaml` has no `deploy.updateStrategy`
  block while the chart configmap/values carry the strategy defaults
  (kustomize installs get none); the Makefile ome-status-preflight target
  comment says the command "only reads the API" while the target's own
  help line advertises a repair subcommand that writes when not dry-run —
  reword to the real invariant ("no xet dependency, pure Go"); the OSS
  Makefile carries the same comment via #953.
- Hygiene round 3 (2026-09-19, owner request "another round of fixes across
  all OEPs, code, tests and hack"): ALL MERGED by the owner 2026-09-19 —
  internal #1560 hack (0bbd43ce), #1571 tests (686f5a54), #1574 code +
  charts (6e1fa74b), OEP PRs #1561-#1569; OSS mirror #982 (e18d8fa5, 76 of
  the 84 files exist upstream; the OSS-only alfred/cli/quota-manager OEP
  references were fixed directly there). Two comment commits pushed after
  #1574 had merged were re-filed as #1579 (quota-manager package comment,
  Makefile help texts). The next drift check should show all of it
  identical, do not re-port. Deliberately left, decide
  later: `pkg/webhook/admission/pod/rdma_injector.go` profile names
  `cks-gb-*` + namespace `cw-multus` + `ibs%dp%d-macvlan` (behavioral,
  public since #804; proper fix = config-driven RDMA profiles via the
  inferenceservice-config ConfigMap with the internal table carried in the
  internal chart values); `pkg/validation/isvc.go` admission error strings
  containing "no longer" (user-facing text, behavioral); the
  `irstatustest` mass-failure fixture counts (2,812/814/374 — changing them
  moves the etcd size gate); `charts/ome-resources/tests/render_test.sh`
  block that renders `values-usc1-1-tpu.yaml` (internal overlay, carved
  upstream); `charts/muse`, `charts/ome-isvc` (internal-only deployment
  config); (ECR image refs in hack fixtures: RESOLVED by #1698 — `${OME_EVAL_*_IMAGE}` tokens with public defaults, mirror paths only in `.buildkite/eval_image_mirror.sh`); the statemachine `prs:` ledger
  and perm MANIFEST `issue` column (tracker ids by design). Sweep lessons:
  this Mac's `git grep -E` silently ignores `\b` (no PCRE) — use BSD `grep -E`
  or `[[:<:]]`; zsh does not word-split an unquoted path list (a multi-dir
  `grep -r $area` silently searches nothing). The scratch dir
  `.claude/oss-sync-scratch/` (ledger, recipes, journals) was deleted on
  2026-09-19 at the owner's request — this file and the memory notes are
  the record; agent workspaces now live under `~/.cache/ome-agent.noindex/`.
- Upstream follow-up (OSS-only code, author it directly upstream):
  kubectl-ome (pkg/cli) and Alfred (pkg/alfred) read the dense
  InferenceReplica `instanceStatuses` list directly and need
  `irstatus.GetDecoded` now that ColumnarV2 is the OSS default (#955
  body); the inventory test's `rawReaderOutsideManager` class lists the
  sites.
- Internal hygiene follow-ups (not blocking): ArgoCD strip-prefix list is
  a literal in placement/derive.go — make it config-driven per the
  CLAUDE.md rule; TrafficMap CRD missing from config/crd/{full,minimal}
  kustomization lists (pattern: only 9 of 15 CRDs listed); no positive
  RollbackOnStall test in the three adapted OSS pkg/cli sites; stale
  "bound namespace" wording in acceleratorquota_types.go + quota-manager
  rbac.yaml; kueue/render_test.go "now ... rather than" narration.
- Upstream contributor PRs touching shared code (all still OPEN on
  2026-09-17; reverse-port once merged): #788 stage:// storage, #838
  download priority (API), #797 oci:// ModelPack (breaking), draft #867
  scheduler gang pin; #773 is superseded by OSS #804 and can be closed;
  #764 istio bump.
- Still queued waves: agents batch (internal/ome-agent + cmd/ome-agent +
  pkg/xet download.go) and tests/KIND (needs KIND_SPEC_GROUPS extraction
  + tiered GHA matrix upstream first). Makefile test-no-xet hardening
  still not ported.

## Last sync (2026-09-04) — COMPLETE

Range fe78040d..5ae88509 (16 internal commits) + #1386 (34423161, landed mid-run, folded in as batch 5; 4 grafana commits blocked;
#1371's KIND spec deferred with the tests wave; its OEP text blocked).
Upstream merged its own #847 (recreate waits for PodReady) first — it is
NOT in internal yet: batch 0 ports it internal-first.

| batch | OSS PR | branch (worktree .claude/oss-wt/…) | content | state |
|---|---|---|---|---|
| 0 | internal #1387 | fix/recreate-wait-pod-ready | port of OSS #847 + test, OEP-0007 row, 3 codespell spellings | MERGED f8daf1c0 |
| — | internal #1385 | misc/leakfree-fixture-comment-hygiene | placement fixture region=lco→us-east; revision.go comment | MERGED 5911c7a4 |
| 1 | #848 | core/revision-hash-queue-labels (b1) | #1367 | MERGED a4c4f0d7 |
| 2 | #849 | core/quota-accounting-fleet-metrics (b2) | #1370 #1376 #1379 #1382 | MERGED e5ef9cf2 |
| 4 | #850 | helm/quota-manager-metrics-collector (b4) | #1369 #1374 (fb_metric render case + internal plumbing paragraph carved) | MERGED 1160af19 |
| 3 | #851 | api/lifecycle-minreadyseconds (b3) | #1371 minus KIND spec/OEP + #1372 PDB default + #1387 interplay; OSS-side regen (zero drift) | MERGED 26b21f53 |
| 5 | #852 | bugfix/isvc-missing-external-service (b5) | #1386 (landed after triage; 2 files) | MERGED 65d13221 |

All four OSS PRs are independent (disjoint files, all based on main) — no
cascade. Audits: b1/b4 clean; b2 had one confirmed leak (lco fixture) fixed
on both sides. Watermark = f8daf1c0: every internal commit through it is synced, mirrored (#1385/#1387 internal-first ports), or blocked by policy. Worktrees .claude/oss-wt/b1..b5 removed after merge.

## Last sync (2026-09-03) — COMPLETE (all six PRs merged)

Range a3e62ae0..fe78040d (84 internal commits; 28 OEP-scrub commits
blocked). Method: per-file THREE-WAY MERGE with normalized bases
(`git merge-file -p ours base theirs`; base = internal@previous batch
tip, theirs = internal@batch tip, both module-path-rewritten + gofmt).
Permanent drifts fall out automatically; only genuine conflicts need
hands (usually the sharded/ModelCache carve). Every batch got an
adversarial audit workflow (auditors + 3 refuters per finding) before
commit — they caught every real mistake this run; keep them mandatory.

OSS stack, squash-merged IN ORDER by the owner (each PR = one commit):

| OSS PR | branch | content | state |
|---|---|---|---|
| #840 | api/remove-columnarv2-stub | stub types + pkg/cli/instancestatus removal | MERGED 2b155429 |
| #841 | core/omenative-quota-drift | batches 1+2: core + quota drift (T1 bb6d667d) | MERGED f97f9cfa |
| #842 | core/autoscaler-policies | batch 3: #1330 autoscaler policies (+prometheus v0.304.2) | MERGED 2a476580 |
| #843 | deps/kueue-0.19-cr-0.23 | batch 4: #1221 dep wave (effective CR v0.23.3 / k8s v0.35.8; keda 2.20.2, kueue 0.19.2, lws 0.9.0, gw-api 1.6.0; typed webhooks) | MERGED b03384f7 |
| #844 | core/rollout-policies | batch 5: #1345 rollout policies (cmd/ome-policy never ported) + #1366 comment hygiene | MERGED d7ad4b90 |
| #845 | helm/policy-webhooks-and-config | batch 6: charts (policy webhooks, config parity, CRD gating, helm-lint runs render tests) | MERGED 9834ef6d |

CASCADE after every squash merge (GitHub auto-retargets the direct child
to main; its branch still carries the pre-squash parent commit, so it
shows CONFLICTING until rebased):

    git fetch origin            # CHECK THE EXIT CODE; `| tail` hides 503s
    git merge-base --is-ancestor <expected squash sha> origin/main
    git rebase --onto origin/main <old parent tip> <child>
    # then every grandchild, in order:
    git rebase --onto <new parent tip> <old parent tip> <grandchild>
    git diff <old tip> <new tip>    # MUST be empty per branch
    git push --force-with-lease=<branch>:<old tip> origin <branch>

Cascade trail for this run lived in /tmp/oss-cascade-oldtips.txt; the
/tmp/oss-cascade worktree was removed after #845 merged. NEVER chain rebases with `;`/`set -e` in one script: a
stopped rebase leaves HEAD at the onto commit, so the next
`git rev-parse HEAD` silently feeds the wrong base to the next step —
check every rebase's exit code and stop.

Internal side of this run: reverse-ports #1364 (prometheus image
qualification from OSS #832, absent-root guard from OSS #813, 5 comment
fixes) and #1365 (serving_agent Errorf directive) MERGED; batch-5 audit
comment follow-up on branch misc/rollout-policy-comment-hygiene
(faf1b66e) → internal PR #1366. #1364/#1365/#1366 content is already mirrored on
the OSS branches (#1366 = the six comment rewrites, applied as a patch onto
#844 core/rollout-policies) — next run: confirm with the drift check, do
not re-port.

Leak audit (2026-09-03, whole sync-owned range 0fcf9b42..224561e1 = 241
files / 26.4k added lines): pattern sweep + 9 area auditors reading every
added line + commit/PR-text auditor + 3 refuters per finding → 0 confirmed
leaks; only actions were the dangling OEP-0017 comment (now rewritten via
#1366) and two PR-body sentences reworded (#840 "being finalized… return",
#841 "a ceiling again"). Repeat this audit at the end of every sync.

Lessons (new this run): run `go mod tidy` (overlay env) in the OSS clone
before every commit — `go list -m`/download can add go.sum lines that
the Generated Code Drift gate's tidy then prunes (#842 first run);
never copy internal go.mod/Makefile comments verbatim (leaked .buildkite paths); a 3-way merge around a carved block
can strand a bare context line (duplicate `controller:` key in
values.yaml — helm is last-wins, so every gate passed silently) → run a
strict duplicate-key YAML load on chart values after chart merges; OSS
`make helm-lint` needs the overlay env or it dies installing helm;
durable Go caches live in `.claude/gomodcache*.noindex`.

## Known pending as of 2026-09-04 (superseded)

- Nothing unsynced below f8daf1c0. First items for the next run: whatever
  lands after it, plus the in-place / restart PodReady-parity follow-up
  if it is filed (update_inplace.go and restart.go still promote on
  ContainersReady + serving).
- Policy-excluded from the 2026-09-04 range (do not re-triage): grafana
  commits #1368/#1375/#1381/#1383; #1371's KIND spec (tests wave); its
  OEP text; the perm-harness README note.
- Makefile test-no-xet hardening (filter-out package lists + guard)
  still not ported; port with `sigs.k8s.io/ome/` in the patterns.
- ome-crd values.yaml comment says the gate covers "manager RBAC"; RBAC is
  unconditional in both trees. Fix INTERNAL first.
- OSS render tests pin literal chart versions; fine for PR CI, would
  break after the release yq rewrite.
- Two long-queued waves: the agents batch (internal/ome-agent +
  cmd/ome-agent, 54 Go files, + pkg/xet download.go; de-fork headers,
  needs the xet build for tests) and the tests/KIND wave (needs the
  KIND_SPEC_GROUPS extraction + tiered GHA matrix upstream first).

## Deliberately never synced

See the HARD BLOCKLIST in SKILL.md — do not re-triage those every week.

## Update procedure (end of every sync)

1. Set the internal watermark to the internal main SHA the sync
   processed through (NOT the tip if newer commits landed mid-sync).
2. Refresh the OSS main SHA and the pending list.
3. Keep one line of history below so state moves are auditable.

## History

- 2026-08-31: initial state. Synced through a3e62ae0; batches 1-9
  (#798-#804, #807, #809, #810) — see memory
  project_upstream_batch_scrub.md for the full ledger.
- 2026-09-03: synced a3e62ae0..fe78040d as OSS #840-#845 (3-way-merge
  method), ALL MERGED (OSS main 9834ef6d). Internal reverse-ports
  #1364/#1365 merged, #1366 open. Range-wide leak audit: 0 leaks.
  Watermark → fe78040d.
- 2026-09-04: synced fe78040d..f8daf1c0 as OSS #848-#852 (+ upstream's own #847
  ported internal-first as #1387; #1385 fixture/comment hygiene), ALL MERGED
  (OSS main 26b21f53). Parallel worktree builders + per-batch audits; one
  leak (lco fixture) caught and fixed both sides. Watermark → f8daf1c0.
- 2026-09-14/15: synced ddc08ead..654cde4f as OSS #912-#920 (9 PRs, all merged
  2026-09-15, OSS main bc0cf3ef); internal ports #1482/#1483/#1484 + hygiene
  #1481 merged. Owner-requested careful leak review → 0 blockers, wording
  and fixture cleanups mirrored before merge. Watermark → 654cde4f.
- 2026-09-16/17: synced 654cde4f..bbacdfa0 as OSS #950-#955 (chronological
  per-commit patch stack A→B→D→E plus independent R/H), ALL MERGED
  2026-09-17 (OSS main f6e1f7f7); internal hygiene #1521 merged. Cascade
  near-miss (guard must be fatal) recorded in SKILL.md. Watermark → bbacdfa0.
- 2026-09-24/25: synced bbacdfa0..624955e7 (150 commits, workload restructure) as OSS #996 code + #997 charts via the tree-state 3-way method, ALL MERGED (OSS main f3c6693b). Internal-first ports #1686 (#989 RDMA), #1687 (#987/#776 charts). Fresh-eyes audit → 294 rollout-table trailers stripped upstream, ~60 comments rewritten internal-first (#1692). New standing tooling: hack/check_internal_tokens.py (#1698). Watermark → 624955e7.
- 2026-09-27: synced 624955e7..cf6dc468 (52 commits, placement plans and
  fleet capacity wave) as OSS #1134 code + #1135 charts (candidate tool dry
  run + hand-resolved conflicts), ALL MERGED 2026-09-27 (OSS main 653ee9cc).
  Internal-first #1799. Owner rule: the ome-viewer ClusterRole is never
  shipped. Watermark → cf6dc468.
