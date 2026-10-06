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

## Common Commands

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

`make test-kind` runs only the coordination suite under
`tests/integration/singlecluster/controller/omenative_coordination_kind/`. It
needs Docker on the host and a `kind` binary on PATH or at `bin/kind` (install
the pinned version with `make kind`). `KIND_KEEP_CLUSTER=true` is
on by default so re-runs are fast; set `KIND_RECREATE=true` to force
a fresh cluster. Narrow with `GINKGO_FOCUS='regex'`.

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

#### Integration Tests (detailed)

Integration suites live under `tests/integration/` and split into two halves:

```
tests/integration/
├── dep-crds/                              # vendored optional-CRD YAMLs (LWS, KEDA, Gateway API, scheduler-plugins, Envoy Gateway, Prometheus)
├── framework/                             # shared test scaffolding (envtest)
├── kindutil/                              # shared test scaffolding (KIND) + fake-serving image source
├── components/, util/                     # helpers
└── singlecluster/
    ├── webhook/{inferenceservice,basemodel,servingruntime,pod,runtimepreset,runtimerevision,acceleratorquota,acceleratorquota_cert}
    ├── controller/{inferenceservice,basemodel,servingruntime,runtimerevision,acceleratorquota,acceleratorquota_capacity,omenative_coordination_kind,alfred_kind}
    ├── clustercache/
    └── agents/metadata/
```

Each leaf directory is its own Ginkgo suite with a single `TestXxx(t)` entry that calls `util.RunSuite` (envtest) or `kindutil.RunKindSuite` (KIND).

**envtest suites** — fast, hermetic, run by `make integration-test`. Use them for: webhooks, CRD/RBAC defaulters, BaseModel/ServingRuntime/RuntimeRevision reconcilers, ClusterCache, agent-side metadata. NOT used for OMENative reconciler behavior — that's KIND territory now (the envtest `coordination/` + `omenative/` suites were deleted because PodKubeletSimulator races made them chronically flaky).

```sh
make integration-test                               # full envtest suite
KUBEBUILDER_ASSETS="$(bin/setup-envtest use "$(cat .envtest-k8s-version)" -p path)" \
  go test ./tests/integration/singlecluster/webhook/inferenceservice/... -v   # focused
```

`KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT=true` forwards the envtest kube-apiserver/etcd stdout+stderr into test logs when debugging control-plane startup failures.

**KIND suite** (`omenative_coordination_kind/`) — real kubelet + scheduler + containerd via Docker. Authoritative for: cross-Component rollout coordination, pod-lifecycle timing (drain races, recreate ordering, in-place patch convergence), per-revision Service convergence, Sequential handoff timing, RatioBalanced gate at scale, ImagePullBackOff failure escalation, pod-UID rotation, migration, PD-disaggregated topologies. ~80 spec files, ~5 min/file wall-clock.

```sh
make test-kind                                       # full suite (~30-60 min)
GINKGO_FOCUS='PD per-Component' make test-kind       # narrow to specific specs (~5-10 min)
GINKGO_FOCUS='RecreatePod' make test-kind            # narrow by It-string fragment
make test-kind-clean                                 # delete leftover KIND clusters
```

Requirements:
- Docker daemon running on the host (KIND brings up the cluster as a Docker container).
- `kind` binary on `PATH` or at `bin/kind`; run `make kind` to install the pinned version.
- A throwaway `KUBECONFIG` — these targets create the cluster in whatever the caller exports. See **Kubeconfig Hygiene** above.

`KIND_KEEP_CLUSTER=true` is on by default in `make test-kind` — the cluster persists between runs for fast iteration. Set `KIND_RECREATE=true` to force a fresh cluster (use after suspected dirty state).

Direct `go test` (when you need extra flags):

```sh
KIND_KEEP_CLUSTER=true \
  PATH="$(pwd)/bin:$PATH" \
  go test ./tests/integration/singlecluster/controller/omenative_coordination_kind/... \
  -v -timeout 30m -ginkgo.v -ginkgo.focus 'rollout converges'
```

KIND specs use generous timeouts (`kindShortTimeout=30s`, `kindMediumTimeout=2m`, `kindLongTimeout=5m`) and a 100ms poll interval — tuned for real kubelet latency, not envtest microseconds.

CI runs the suite through the hard-gating `integration-test-kind-gke` group. The unit-test lane compiles one exact-commit test binary, and named semantic jobs run disjoint file groups in separate three-node KIND clusters on GKE agents. The TW dind cannot provide the bridge networking KIND requires.

**Every new KIND spec file must be classified** into a group in `KIND_SPEC_GROUPS` in `.buildkite/generate_pipeline.py` — the generator enforces exact ownership and rejects unowned (or stale) specs, so an unclassified `*_kind_test.go` fails pipeline generation for every build. Validate locally with `python3 -m unittest discover -s .buildkite -p 'test_*.py'` before pushing.

**Alfred suite** (`alfred_kind/`) — deploys the alfred image built from `.alfred-oss-ref` through the ome-resources alfred templates into a three-worker cluster with fake GPU nodes, and drives it black-box against the in-process controller: node-health and maintenance signals are patched onto Nodes, and the outcome is read back through alfred's metrics, events, recommendations and journal ConfigMaps, the ISVC migration annotations and InferenceReplica status. `make test-kind-alfred` (set `OME_ALFRED_IMAGE` to skip the local image build). New spec files must be classified in `ALFRED_KIND_SPEC_GROUPS` in `.buildkite/generate_pipeline.py`.

**Multi-cluster real-Kueue suite** (`tests/integration/multicluster/`, gated by `OME_MC_REALKUEUE=1`) — validates OEP-0018 single-active placement against a *real* Kueue admission stack. The default `make integration-test` runs this suite's **sim** path (`admitDerivedOn`); with the env gate it stands up 1 control-plane + 2 member envtest apiservers in one process, each member running a full OME manager + Kueue (core controllers + scheduler + pod webhook), and drives fan-out → real quota-gated admission → race → loser-sweep for real (no kubelet — admission = Kueue scheduling-gate removal). Run it via:

```sh
make mc-realkueue                          # full suite, opt-in
GINKGO_FOCUS='S1|S3' make mc-realkueue     # narrow to specific scenarios
```

Opt-in / soft-fail (it's heavy: 3 apiservers + 3 OME managers + 2 Kueue managers in-process). Loser-sweep assertions check the placer's contract — derived **deleted or Terminating** — not full reaping, because the member's finalizer teardown (IR + Kueue-gated pods) is eventually-consistent and lags under load.

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

### Deployment
```bash
make install              # Deploy to cluster via kustomize (interactive confirmation)
make deploy-helm          # Deploy via Helm charts
make run-ome-manager      # Run controller locally against configured cluster
make delete-webhooks      # Remove webhooks (needed for local controller debugging)
```

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

### CRDs (7 types in `pkg/apis/ome/v1beta1/`)
- **BaseModel / ClusterBaseModel** — Model source definitions (HuggingFace, S3, OCI, Azure, GCS, GitHub). Auto-parsed for architecture, parameters, capabilities.
- **ServingRuntime / ClusterServingRuntime** — How models are served. Defines container images, supported model formats/architectures, accelerator requirements, multi-node worker configs.
- **InferenceService** (short name: `isvc`) — The central deployment resource. Links a model to a runtime. Supports Engine/Decoder components for prefill-decode disaggregation.
- **AcceleratorClass** — Cluster-scoped GPU hardware definitions with discovery patterns, capabilities, cost info. Policies: BestFit, Cheapest, MostCapable.
- **FineTunedWeight** — LoRA adapters and fine-tuned weight deltas referencing a base model.

### Key relationships
- `InferenceService.spec.model` → `BaseModel` or `ClusterBaseModel`
- `InferenceService.spec.runtime` → `ServingRuntime` or `ClusterServingRuntime` (optional; auto-selected if omitted)
- `InferenceService.spec.acceleratorSelector` → `AcceleratorClass` by name or policy
- `FineTunedWeight.spec.baseModelRef` → `BaseModel` or `ClusterBaseModel`

### Binaries (`cmd/`)
| Binary | Role |
|---|---|
| `cmd/manager` | Main Kubernetes operator. Runs all controllers and webhooks. Dynamically registers schemes for LWS, Volcano, KEDA based on cluster CRD availability. |
| `cmd/ome-agent` | Multi-command CLI (Cobra + uber/fx): hf-download, replica (cross-region replication), serving-agent (sidecar), fine-tuned-adapter, model-metadata. |
| `cmd/crd-gen` | Post-processes generated CRD YAML (strips unwanted validation fields). Invoked by `make manifests`. |
| `cmd/spec-gen` | Emits the OpenAPI JSON for the CRDs. Invoked by `make generate`; consumed by the Python SDK generator and the API reference site. |

The `model-agent` DaemonSet deployed by `charts/ome-resources/` runs an image built outside this repo.

### Controllers (`pkg/controller/v1beta1/`)
- **InferenceServiceReconciler** — The largest controller. Auto-selects runtimes via `runtimeselector`, selects accelerators via `acceleratorclassselector`, generates Deployments/LeaderWorkerSets/HPAs/KEDA ScaledObjects. Supports RawDeployment, MultiNode, PDDisaggregated, OMENative, and VirtualDeployment modes.
- **BaseModelReconciler / ClusterBaseModelReconciler** — Manages model lifecycle. Monitors download status via ConfigMaps. Requeues every 5s during importing/transit states.
- **AcceleratorClassReconciler** — Discovers matching GPU nodes, maintains `status.nodes` and `status.availableNodes`.

### Key Packages
| Package | Purpose |
|---|---|
| `pkg/runtimeselector` | Fetch-match-score pipeline for auto-selecting the best ServingRuntime for a model based on format, architecture, framework, quantization, size. |
| `pkg/acceleratorclassselector` | Selects the best AcceleratorClass for an InferenceService component using BestFit/Cheapest/MostCapable policies. |
| `pkg/modelconfig` / `pkg/modelparser` | Parse and normalize model metadata (architecture, params, capabilities) from HuggingFace config files. |
| `pkg/validation` | Pure validation helpers (no K8s deps) shared between CRD validators and webhooks. |
| `pkg/xet` | CGo bridge to Rust `libxet` library for high-performance HuggingFace downloads. Build with `cd pkg/xet && make build`. |
| `pkg/webhook/admission` | Pod mutator (injects config into ISVC pods), ServingRuntime/InferenceService validators and defaulters. |
| `pkg/constants`, `pkg/version`, `pkg/utils` | Shared label/annotation strings, build-time version stamps, and tiny cross-cutting helpers. |
| `internal/ome-agent` | Agent implementations dispatched as `ome-agent` subcommands: `replica` (cross-region replication), `serving-agent` (sidecar), `fine-tuned-adapter`, `model-metadata`. |

### Helm Charts (`charts/`)
- **ome-crd** — CRD-only chart. `make manifests` generates its templates from `config/crd/full/` through `crd-gen helmchart`, which factors repeated schema subtrees into shared partials (`_schemas.tpl`) so the Helm release record stays well under its 1 MiB limit. The rendered CRDs equal `config/crd/full/`; the chart render test enforces it. Never edit the templates by hand.
- **ome-resources** — Controller deployment, model-agent DaemonSet, RBAC, webhooks, cluster resources.

### Sample Manifests (`config/samples/`)
- `autoscalerpolicy/` — AutoscalerPolicy examples.

Runtime/model catalog YAMLs (vendor-specific ClusterServingRuntime / ClusterBaseModel) live in operators' own GitOps repos, not in-tree.

## Code Agent / Sandbox Notes

OME builds work out-of-the-box on a fully-provisioned developer Mac, but several steps fail subtly in stripped-down code-agent containers. Workarounds:

- **System packages** (Debian/Ubuntu base images): `apt-get install -y jq libssl-dev pkg-config build-essential curl ca-certificates`.
  - `jq` is consumed by `make manifests` (CRD post-processing in `hack/` scripts).
  - `libssl-dev pkg-config build-essential` are required to build `pkg/xet` (Rust → static C lib).
- **Rust toolchain** for `pkg/xet`:
  ```sh
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable --profile minimal
  ```
- **`make generate` writes to the Go env file** (`~/.config/go/env`) if you run it on a Makefile from before commit `[Misc] Makefile: avoid go env -w in make generate`. Update or rebase before reporting failures.
- **IPv6-only CI sandboxes**: envtest's `kube-apiserver` auto-detects the pod's IPv6 external address, then refuses to start because it doesn't match the IPv4 service-cluster range. Wrap the binary with a shim pinning `--advertise-address=127.0.0.1`; see `.buildkite/run_tests.sh` for the canonical pattern.
- **envtest debugging**: `export KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT=true` to forward kube-apiserver/etcd output into test logs.
- **xet-linked binaries / tests**: `export LD_LIBRARY_PATH=$(pwd)/pkg/xet/target/release` so the dynamic linker finds `libxet.{so,dylib}` (needed for `ome-agent` tests and any binary that links the Rust staticlib path).
- **KUBEBUILDER_ASSETS one-liner**: rather than retyping the full `bin/setup-envtest` invocation for every `go test` call, save this as a shell alias / persistent env:
  ```sh
  export KUBEBUILDER_ASSETS="$(bin/setup-envtest use "$(cat .envtest-k8s-version)" -p path)"
  ```

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

## Git author email

GitHub recommends using a GitHub-provided `noreply` email address as your Git
author identity instead of exposing your actual email address. Configure Git
with the address shown under your GitHub email settings; supported addresses
end in `@users.noreply.github.com`.

The repository's pre-commit hook and CI validate the author email on new
commits. For example, derive the current account's address with GitHub CLI and
set it globally:

```sh
github_email="$(gh api user --jq '"\(.id)+\(.login)@users.noreply.github.com"')"
git config --global user.email "$github_email"
```
