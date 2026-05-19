# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OME (Open Model Engine) is a Kubernetes operator for managing and serving Large Language Models. It automates model management, runtime selection, accelerator-aware scheduling, and deployment orchestration. The API group is `ome.io`, version `v1beta1`. Built with Go 1.25 and controller-runtime v0.19.7.

## Common Commands

### Build
```bash
make ome-manager          # Build controller manager binary
make model-agent          # Build model-agent binary (requires xet-build)
make ome-agent            # Build ome-agent binary (requires xet-build)
make xet-build            # Build the Rust xet native library (required before model-agent/ome-agent)
```

### Test
```bash
make test                 # Run all unit tests (builds xet first)
make test-no-xet          # Run tests excluding ome-agent (no Rust dependency needed)
make integration-test     # Run Ginkgo integration tests from tests/ directory
make coverage             # Show coverage summary (48% threshold enforced)

# Run a single test package:
KUBEBUILDER_ASSETS="$(bin/setup-envtest use 1.30 -p path)" go test ./pkg/runtimeselector/... -v
# Run a specific test by name:
KUBEBUILDER_ASSETS="$(bin/setup-envtest use 1.30 -p path)" go test ./pkg/controller/v1beta1/inferenceservice/... -run TestReconcile -v
```

Controller tests require envtest (KUBEBUILDER_ASSETS). Tests involving HuggingFace downloads need `LD_LIBRARY_PATH` pointing to `pkg/xet/target/release`.

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

**Required before every commit / hand-off:**

```sh
make fmt
make vet
make ci-lint
make test-no-xet   # full suite minus ome-agent (no Rust toolchain needed)
```

For iterative work, focused `go test ./pkg/<package>/... -v` is fine, but the four commands above must pass before pushing.

## Architecture

### CRDs (8 types in `pkg/apis/ome/v1beta1/`)
- **BaseModel / ClusterBaseModel** — Model source definitions (HuggingFace, S3, OCI, Azure, GCS, GitHub). Auto-parsed for architecture, parameters, capabilities.
- **ServingRuntime / ClusterServingRuntime** — How models are served. Defines container images, supported model formats/architectures, accelerator requirements, multi-node worker configs.
- **InferenceService** (short name: `isvc`) — The central deployment resource. Links a model to a runtime. Supports Engine/Decoder components for prefill-decode disaggregation.
- **AcceleratorClass** — Cluster-scoped GPU hardware definitions with discovery patterns, capabilities, cost info. Policies: BestFit, Cheapest, MostCapable.
- **FineTunedWeight** — LoRA adapters and fine-tuned weight deltas referencing a base model.
- **BenchmarkJob** — Performance benchmarks against inference endpoints.

### Key relationships
- `InferenceService.spec.model` → `BaseModel` or `ClusterBaseModel`
- `InferenceService.spec.runtime` → `ServingRuntime` or `ClusterServingRuntime` (optional; auto-selected if omitted)
- `InferenceService.spec.acceleratorSelector` → `AcceleratorClass` by name or policy
- `FineTunedWeight.spec.baseModelRef` → `BaseModel` or `ClusterBaseModel`

### Binaries (`cmd/`)
| Binary | Role |
|---|---|
| `cmd/manager` | Main Kubernetes operator. Runs all controllers and webhooks. Dynamically registers schemes for Knative, LWS, Volcano, KEDA, Istio based on cluster CRD availability. |
| `cmd/model-agent` | DaemonSet on GPU nodes. Scout watches models, Gopher downloads from storage backends. Labels nodes with model readiness. Uses xet (CGo/Rust) for HuggingFace downloads. |
| `cmd/ome-agent` | Multi-command CLI (Cobra + uber/fx): hf-download, replica (cross-region replication), serving-agent (sidecar), fine-tuned-adapter, model-metadata. |

### Controllers (`pkg/controller/v1beta1/`)
- **InferenceServiceReconciler** — The largest controller. Auto-selects runtimes via `runtimeselector`, selects accelerators via `acceleratorclassselector`, generates Deployments/Knative Services/LeaderWorkerSets/HPAs/KEDA ScaledObjects. Supports RawDeployment, Serverless, MultiNode, PDDisaggregated, and VirtualDeployment modes.
- **BaseModelReconciler / ClusterBaseModelReconciler** — Manages model lifecycle. Monitors download status via ConfigMaps. Requeues every 5s during importing/transit states.
- **AcceleratorClassReconciler** — Discovers matching GPU nodes, maintains `status.nodes` and `status.availableNodes`.
- **BenchmarkJobReconciler** — Creates Kubernetes batch Jobs for benchmarking.

### Key Packages
| Package | Purpose |
|---|---|
| `pkg/runtimeselector` | Fetch-match-score pipeline for auto-selecting the best ServingRuntime for a model based on format, architecture, framework, quantization, size. |
| `pkg/acceleratorclassselector` | Selects the best AcceleratorClass for an InferenceService component using BestFit/Cheapest/MostCapable policies. |
| `pkg/modelagent` | Scout (model watcher) + Gopher (concurrent downloader) used by model-agent DaemonSet. Manages per-node-per-model ConfigMaps for download state. |
| `pkg/hfutil/hub` | HuggingFace Hub client: file download with resume, progress tracking, LFS support. |
| `pkg/xet` | CGo bridge to Rust `libxet` library for high-performance HuggingFace downloads. Build with `cd pkg/xet && make build`. |
| `pkg/webhook/admission` | Pod mutator (injects config into ISVC pods), ServingRuntime/InferenceService/BenchmarkJob validators and defaulters. |
| `pkg/configutils` | Viper-based config with `imports` key resolution, used by ome-agent sub-commands. |
| `internal/ome-agent` | Agent implementations: replica (cross-region replication), serving-agent (sidecar). |

### Helm Charts (`charts/`)
- **ome-crd** — CRD-only chart. CRDs are copied from `config/crd/full/` during `make manifests`.
- **ome-resources** — Controller deployment, model-agent DaemonSet, RBAC, webhooks, cluster resources.

### Sample Manifests (`config/samples/`)
- `avo-tier5-pd/` — end-to-end PD-disaggregated walkthrough (CBM + CSR + ISVC).
- `basemodel/` — PVC-backed BaseModel and ClusterBaseModel fixtures.
- `modelcache/` — Sharded BaseModel + `cluster_cache` values.

Runtime/model catalog YAMLs (vendor-specific ClusterServingRuntime / ClusterBaseModel) live in operators' own GitOps repos, not in-tree.

## PR Conventions

Prefix PR titles with: `[Bugfix]`, `[Core]`, `[API]`, `[Helm]`, `[Docs]`, `[CI/Tests]`, `[Misc]`, or `[OEP]`. Keep title under 52 characters. Substantial architectural changes require an OEP (OME Enhancement Proposal) in `oeps/`.
