# AGENTS.md

Guidance for coding agents (Claude Code, Codex) and contributors working in this repository.

## Project Overview

Repo-level agent skills live in `.agents/skills/` (available to Codex and Claude Code; `.claude` is a tracked symlink to `.agents`, and `CLAUDE.md` is a symlink to this file). Local agent state (`.agents/specs/`, `tmp/`, `worktrees/`, `settings.local.json`) is gitignored.

OME (Open Model Engine) is a Kubernetes operator for managing and serving Large Language Models. It automates model management, runtime selection, accelerator-aware scheduling, and deployment orchestration. The API group is `ome.io`, version `v1beta1`. Built with Go 1.26 and controller-runtime v0.23.3.

## Design Constraints (HARD BLOCKERS)

**No hardcoded magic values baked into the binary.** Behavioral or user-facing values — hostnames, host/subdomain prefixes, domains, ports, paths, URLs, timeouts, replica/limit defaults, image refs, feature toggles, etc. — MUST be config-driven (the `inferenceservice-config` ConfigMap / `IngressConfig`-style structs, Helm values, flags, or CRD fields), NOT literals in Go. The default belongs in config (Helm `values.yaml` / GitOps), not the code; absent config should degrade gracefully, not silently inject a magic string.

This is a **blocker, not a preference**: any change that introduces (or preserves) a hardcoded value like this must be called out explicitly and justified before it is accepted — do not add it silently. When you encounter an existing hardcode while working nearby, flag it. The fix pattern: add a config field (no in-code default) + supply the value via the chart/GitOps (e.g. the `sharedHostPrefix` field replacing the old hardcoded `"llm."` host prefix). This applies everywhere in the repo.

## Vendor Neutrality (HARD BLOCKERS)

OME is a vendor-neutral engine. Its core, the Go packages under `pkg/`, `cmd/`, `internal/` and `scheduler/`, the charts `ome-resources`, `ome-quota-manager`, `ome-scheduler`, `ome-serving` and `ome-crd`, `config/configmap`, and the OEPs, tests and hack tooling beside them, must read the same for any operator: no logic that only one organization needs, and no information that names one. Three rules keep it that way:

**Write neutral by default, everywhere.** Code, comments, log/error/event strings, test names, fixtures, testdata, chart values and templates, OEPs, runbooks and docs must not carry operator-specific information. The classes, with the neutral replacement in parentheses:
- hostnames, domains and URL schemes of specific systems (`example.com`, `oci://`/`s3://bucket`);
- cluster, environment, namespace and GitOps names (`cluster-a`/`member-a`, `team-b`, "GitOps");
- people, usernames, oncall and team names (`example-user`, `team-a`);
- ticket, incident and diff numbers, PR numbers, and design-document numbers in code or test names (state the invariant instead);
- model, checkpoint and product codenames (`llama-70b`, `example-model`);
- storage buckets, registries and cloud account or project ids;
- fleet shape and capacity of real clusters, and "we run/operate" narrative about them;
- private design vocabulary used as fixtures (real tenant paths, hub/app names);
- incident and change narrative ("the outage", "used to", "pre-fix") — see Code Comments;
- references from core code or docs to review tooling that is not part of the engine (the rollout state table under `tests/rolloutspec` and its coverage trailers are review artifacts, not something the core refers to).

**Operator-specific logic lives behind designated seams, never in the core.** Those seams are `meta-internal/` and its build-tag wire-up (`cmd/manager/register_meta_internal.go`), the sharded model-cache backend and its wire-ups, `pkg/kubectl` with `cmd/kubectl-qt`, the charts `charts/muse/`, `charts/ome-bot/` and `charts/ome-isvc/`, per-cluster `values-*.yaml` overlays and the Alfred templates in `ome-resources`, and the deployment tooling under `.buildkite/`, `dockerfiles/`, `grafana/`, `muse/`, `.github/` and `.agents/` (with its `.claude` symlink). A core Go file imports a seam only from the listed wire-up files; everything else reaches operator behaviour through configuration (the `inferenceservice-config` ConfigMap, Helm values, flags, CRD fields), exactly as the Design Constraints above require for values.

`hack/check_internal_tokens.py` enforces the mechanical part of these rules as a pre-commit hook and the `internal-tokens` CI step. It scans every tracked text file and every path by default (`hack/internal-tokens.txt` holds the patterns); the only blanket exemptions are `muse/`, `charts/muse/` and `meta-internal/`; everything else, including `.buildkite`, `dockerfiles`, `config/` and the operator charts, is scanned and may only be exempted per pattern group with a reason in `hack/internal-tokens.allow` (an exemption without a reason, or one that matches nothing, fails the check). It also fails on a new top-level directory until it is classified as core, internal-only or clean, and on any Go file under the core roots that imports `meta-internal`, the sharded model-cache backend or `pkg/kubectl` from outside the listed wire-up files. A clean run is necessary, not sufficient: read what you wrote for meaning.

**Treat a violation like a hardcoded magic value:** a review blocker to call out explicitly, not a nit. Fix it where it was written; a neutral core is the property, not a cleanup pass.

## Writing for people (HARD BLOCKER)

Every PR description, issue, commit body, event string and review comment is written for an operator who has never read this controller's source. The test: a teammate from another team can tell from the first paragraph alone what breaks, when, and what changes. If it needs a glossary, rewrite it.

- Lead with the story: what the user did, what they saw go wrong, with one concrete example that has numbers ("20 instances, each a gang of 1 leader and 7 workers; the worker count is changed to 8"). Mechanism comes after the story; files come last.
- Everyday words. Say "version 1 / version 2", "the new gang / the old gang", "retries three times, then stops", "the operator rolls back", "OME created two more version 2 pods". The engine's private vocabulary (pin, hand-over, ladder, dark row, floor, marker, supersede, withdrawn, re-arm, re-drive, disposition, home, surge ordinal) may appear only after it is defined in the same sentence.
- Sentences of about twenty words, one idea each, active voice, a verb in every sentence. No semicolon chains. No nominalizations: not "the disposition records a relocation directive" but "OME marks the node as bad and rebuilds the pod elsewhere".
- PR body sections, in this order: Problem (the story), Reproduce (steps a person can run), What changes (behaviour, not code), Detailed changes (files), Testing (what ran, with results; what did not run and why).
- Code comments follow the same rule: state the invariant in plain words; a comment that only parses with the private vocabulary is not a comment.
- Agents briefing other agents pass this section verbatim and reject a report that breaks it.

## Code Comments

Comments describe what the code **is** and **why** — never what *changed* or what's *coming*.

- **No work-progress / diff-narrative comments.** Banned in source: "…no longer / not X anymore", "moved from/to", "previously", "used to", "…in a later step/PR", and roadmap or task tags (e.g. `(single-transition-owner)`). These belong in the commit message or PR body; in code they go stale the instant the next change lands and read as a changelog, not documentation. If a comment only makes sense to someone reading the diff, delete it or rewrite it as the current invariant.
- **No incident narrative.** Don't retell a debugging story or name narrow internals inside general-purpose code — state the general reason instead.
- **No references in code comments.** Issue/PR numbers, incident dates, image/build hashes, cluster names, and investigation shorthand do not belong in source comments or test names — they are meaningless to a reader without tracker access and rot as context moves. References live in commit messages and PR bodies. A comment must carry its full meaning in-place: state the invariant, not a pointer to where the invariant was once discussed.
- Keep comments short (1–3 lines), match the surrounding density, and explain the "why" over the "what".

## Kubeconfig Hygiene (HARD BLOCKER)

**Never let an ephemeral local cluster touch the ambient `$KUBECONFIG`.** That file points at the developer's real clusters. `kind create cluster` — and `kwokctl`, `k3d`, `minikube` — merges a `kind-<name>` context into whatever `$KUBECONFIG` names *and flips `current-context`* to it. A single verification or bugfix run therefore leaves dead contexts behind and silently re-aims the next bare `kubectl` at a cluster that no longer exists.

This binds every Claude session, subagent, and workflow — and any script or `make` target one of them invokes — as much as it binds a human at a terminal. Point the throwaway cluster at a throwaway file: a temp file first, `~/.kube/config` only if a tool insists on the default path.

```sh
# Own one file for the whole session, then clean up.
export KUBECONFIG="$(mktemp -t ome-kind-XXXXXX.kubeconfig)"
make test-kind
rm -f "$KUBECONFIG"
```

```sh
# Or scope it to a single command.
KUBECONFIG=/tmp/ome-kind.kubeconfig make test-kind
KUBECONFIG=/tmp/ome-kind.kubeconfig hack/kwok-sim/fleet-up.sh
kind create cluster --name scratch --kubeconfig /tmp/ome-kind.kubeconfig
```

| Never | Instead |
|---|---|
| `kind create` / `delete` / `export kubeconfig` with neither `--kubeconfig` nor an overridden `KUBECONFIG` | pass `--kubeconfig <tmp>`, or run it under `KUBECONFIG=<tmp>` |
| `kubectl config use-context` / `set-context` / `set-cluster` / `delete-context` against the ambient file | select per command: `kubectl --kubeconfig <tmp> --context kind-<name> …` |
| exporting `KUBECONFIG` to a real-cluster path and then running a cluster-creating target | export the temp path *before* the target runs |

`kind get kubeconfig --name <c> > "$tmp"` is read-only and safe — it is how `tests/integration/kindutil` and `make test-kind-perm-up` obtain credentials.

No in-tree creator passes `--kubeconfig`, so every one of them inherits the caller's: `createCluster` in `tests/integration/kindutil/cluster.go` (behind the `make test-kind*` targets), `hack/kwok-sim/fleet-up.sh`, and the `.buildkite/run_*_gke.sh` scripts. The temp-file wrapper above is what contains them. Treat a new caller that skips it like a hardcoded magic value: call it out, don't add it silently.

## Commands

Prefer `make` targets over raw `go`/`kubectl` commands: they pin the toolchain and set the environment.

### Build
```bash
make ome-manager          # Build controller manager binary
make ome-agent            # Build ome-agent binary (requires xet-build)
make xet-build            # Build the Rust xet native library (required before ome-agent)
```

### Test
```bash
make test                 # Run all unit tests (builds xet first)
make test-no-xet          # Run tests excluding xet-linked ome-agent packages (no Rust dependency needed)
make integration-test     # Run Ginkgo integration tests from tests/ directory (envtest)
make test-kind            # Run KIND-backed integration tests (real kubelet — needs docker + kind locally)
make test-kind-clean      # Delete leftover KIND clusters from prior test-kind runs
make coverage             # Show coverage summary (48% threshold enforced)
```

**Required env for direct `go test` calls** (save these as shell aliases — you will retype them constantly):

```sh
# Controller tests + anything under tests/integration/ — needs envtest assets.
# .envtest-k8s-version at the repo root is the single source of truth for the
# k8s version; every Makefile and CI step reads it.
export KUBEBUILDER_ASSETS="$(bin/setup-envtest use "$(cat .envtest-k8s-version)" -p path)"

# Anything that links the xet Rust staticlib (ome-agent, hf-download, etc.):
export LD_LIBRARY_PATH=$(pwd)/pkg/xet/target/release
```

With those exported, you can drop the inline prefix:

```sh
# Run a single test package:
go test ./pkg/runtimeselector/... -v
# Run a specific test by name:
go test ./pkg/controller/v1beta1/inferenceservice/... -run TestReconcile -v
```

Without the exports, prefix each invocation manually:
`KUBEBUILDER_ASSETS="$(bin/setup-envtest use "$(cat .envtest-k8s-version)" -p path)" go test ./pkg/runtimeselector/... -v`

`make test-kind` needs Docker and a `kind` binary (`make kind` installs the pinned one), and a throwaway `KUBECONFIG` (see Kubeconfig Hygiene). Every new `*_kind_test.go` must be classified in `KIND_SPEC_GROUPS` in `.buildkite/generate_pipeline.py` (alfred specs in `ALFRED_KIND_SPEC_GROUPS`); pipeline generation rejects unowned specs. Validate with `python3 -m unittest discover -s .buildkite -p 'test_*.py'`. The suites, their scopes, timeouts and the KIND and multi-cluster details are in `tests/integration/README.md`.

### Lint & Format
```bash
make fmt                  # gofmt + goimports
make vet                  # go vet (with -structtag=false -unsafeptr=false)
make ci-lint              # golangci-lint (15m timeout)
make lint-fix             # golangci-lint with auto-fix
make helm-lint            # Lint Helm charts
make tidy                 # go mod tidy
```

### Code Generation
```bash
make manifests            # Generate CRDs, RBAC, webhooks from markers, copy to Helm charts
make generate             # Generate DeepCopy methods, client-go code, and OpenAPI specs
```
Run `make manifests` after changing API types in `pkg/apis/ome/v1beta1/` or kustomize patches in `config/`. Run `make generate` after changing API type definitions.

### Pre-commit Hooks
```bash
pip install pre-commit && pre-commit install
pre-commit run --all-files
```
Hooks run: go-fmt, go-vet, helm-lint, helm-template, codespell, go-mod-tidy, branch naming check (`<type>/<desc>` or `<username>/<desc>`).

CI runs the codespell hook's configuration over every tracked text file as the `spelling` step (`.buildkite/check_spelling.sh`, which reads the version, word list and exclusions from `.pre-commit-config.yaml` through `hack/check_spelling.py`). A new legitimate word goes into the hook's `-L` list; the CI step follows it.

Deployment targets (`make install`, `make deploy-helm`, `make run-ome-manager`, `make delete-webhooks`) are listed in `charts/README.md`.

## Make Command Discipline

Always prefer `make` targets over raw `go`/`kubectl`/etc. commands — they wire in toolchain pinning, generation, and the right env (KUBEBUILDER_ASSETS, LD_LIBRARY_PATH).

**Required after specific changes:**

| Triggered by | Run |
|---|---|
| Touching `pkg/apis/ome/v1beta1/` | `make manifests && make generate` |
| Touching kustomize patches in `config/` | `make manifests` |
| Touching `charts/*/templates/` directly (rare; usually generated) | `make helm-lint` |
| Touching `go.mod` / `go.sum` | `make tidy` |
| Bumping `sigs.k8s.io/lws`, `github.com/kedacore/keda/v2`, `sigs.k8s.io/gateway-api`, or `sigs.k8s.io/scheduler-plugins` in `go.mod` | `make dep-crds` (re-vendors CRD YAMLs into `tests/integration/dep-crds/`, which are gitignored — not committed; CI's `dep-crds-drift` step catches a forgotten regen) |

**Required before every commit / hand-off:**

```sh
make fmt
make vet
make ci-lint
make test-no-xet   # full suite minus ome-agent (no Rust toolchain needed)
```

For iterative work, focused `go test ./pkg/<package>/... -v` is fine, but the four commands above must pass before pushing.

## Architecture

API group `ome.io`, version `v1beta1`; seven CRDs in `pkg/apis/ome/v1beta1/` (BaseModel/ClusterBaseModel, ServingRuntime/ClusterServingRuntime, InferenceService, AcceleratorClass, FineTunedWeight). Controllers live in `pkg/controller/v1beta1/` (the InferenceService reconciler is the largest; OMENative workload engine under `workload/`, multi-cluster placement under `placement/`), webhooks in `pkg/webhook/admission`, runtime and accelerator selection in `pkg/runtimeselector` and `pkg/acceleratorclassselector`. Binaries are under `cmd/` (`manager`, `ome-agent`, `crd-gen`, `spec-gen`), charts under `charts/`. The full map, CRD relationships and package table are in `docs/architecture.md`.

## Code agents and sandboxes

Stripped-down agent containers need extra packages, a Rust toolchain for `pkg/xet`, and an IPv6 shim for envtest; the recipes are in `hack/README.md`.

## OEP Convention

Substantial architectural changes require an OEP (OME Enhancement Proposal) before implementation. See `oeps/README.md` for the process, and follow the **required** writing style in `oeps/WRITING-STYLE.md` — for an OEP, clarity is a correctness property, not a nicety. OEPs live under `oeps/<number>-<slug>/`:

```
oeps/<number>-<slug>/
├── oep.yaml      # metadata: oep-number, title, authors, status, dates, reviewers, approvers
└── README.md     # full proposal text
```

`oep-number` is a quoted string: `"NNNN"` for new proposals (`"0010"`, `"0011"`) and `"NNNN.M"` for follow-ups to an existing OEP (e.g., `"0007.1"` is a follow-up to OEP 0007). Branch the work off the slug, e.g., `feat/oep-0014-phase3-revision-gc`.

Use `git fetch origin && git checkout -b <branch> origin/main` to ensure you branch off the latest `main`, never a stale local copy. Branch names follow `<type>/<desc>` or `<username>/<desc>` (enforced by the pre-commit hook).

## PR Conventions

Prefix PR titles with: `[Bugfix]`, `[Core]`, `[API]`, `[Helm]`, `[Docs]`, `[CI/Tests]`, `[Misc]`, or `[OEP]`. Keep title under 52 characters. Substantial architectural changes require an OEP (OME Enhancement Proposal) in `oeps/` — see the OEP Convention section above.

PR descriptions follow the five sections in Writing for people. Keep the body free of tracker numbers other than the issue it fixes.

## Git author email

Commits must carry a GitHub `noreply` author address (`@users.noreply.github.com`); the pre-commit hook and CI reject others. Set it from the GitHub CLI:

```sh
github_email="$(gh api user --jq '"\(.id)+\(.login)@users.noreply.github.com"')"
git config --global user.email "$github_email"
```
