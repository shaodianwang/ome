# Architecture

## CRDs (7 types in `pkg/apis/ome/v1beta1/`)
- **BaseModel / ClusterBaseModel** — Model source definitions (HuggingFace, S3, OCI, Azure, GCS, GitHub). Auto-parsed for architecture, parameters, capabilities.
- **ServingRuntime / ClusterServingRuntime** — How models are served. Defines container images, supported model formats/architectures, accelerator requirements, multi-node worker configs.
- **InferenceService** (short name: `isvc`) — The central deployment resource. Links a model to a runtime. Supports Engine/Decoder components for prefill-decode disaggregation.
- **AcceleratorClass** — Cluster-scoped GPU hardware definitions with discovery patterns, capabilities, cost info. Policies: BestFit, Cheapest, MostCapable.
- **FineTunedWeight** — LoRA adapters and fine-tuned weight deltas referencing a base model.

## Key relationships
- `InferenceService.spec.model` → `BaseModel` or `ClusterBaseModel`
- `InferenceService.spec.runtime` → `ServingRuntime` or `ClusterServingRuntime` (optional; auto-selected if omitted)
- `InferenceService.spec.acceleratorSelector` → `AcceleratorClass` by name or policy
- `FineTunedWeight.spec.baseModelRef` → `BaseModel` or `ClusterBaseModel`

## Binaries (`cmd/`)
| Binary | Role |
|---|---|
| `cmd/manager` | Main Kubernetes operator. Runs all controllers and webhooks. Dynamically registers schemes for LWS, Volcano, KEDA based on cluster CRD availability. |
| `cmd/ome-agent` | Multi-command CLI (Cobra + uber/fx): hf-download, replica (cross-region replication), serving-agent (sidecar), fine-tuned-adapter, model-metadata. |
| `cmd/crd-gen` | Post-processes generated CRD YAML (strips unwanted validation fields). Invoked by `make manifests`. |
| `cmd/spec-gen` | Emits the OpenAPI JSON for the CRDs. Invoked by `make generate`; consumed by the Python SDK generator and the API reference site. |

The `model-agent` DaemonSet deployed by `charts/ome-resources/` runs an image built outside this repo.

## Controllers (`pkg/controller/v1beta1/`)
- **InferenceServiceReconciler** — The largest controller. Auto-selects runtimes via `runtimeselector`, selects accelerators via `acceleratorclassselector`, generates Deployments/LeaderWorkerSets/HPAs/KEDA ScaledObjects. Supports RawDeployment, MultiNode, PDDisaggregated, OMENative, and VirtualDeployment modes.
- **BaseModelReconciler / ClusterBaseModelReconciler** — Manages model lifecycle. Monitors download status via ConfigMaps. Requeues every 5s during importing/transit states.
- **AcceleratorClassReconciler** — Discovers matching GPU nodes, maintains `status.nodes` and `status.availableNodes`.

## Key Packages
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

## Helm Charts (`charts/`)
- **ome-crd** — CRD-only chart. `make manifests` generates its templates from `config/crd/full/` through `crd-gen helmchart`, which factors repeated schema subtrees into shared partials (`_schemas.tpl`) so the Helm release record stays well under its 1 MiB limit. The rendered CRDs equal `config/crd/full/`; the chart render test enforces it. Never edit the templates by hand.
- **ome-resources** — Controller deployment, model-agent DaemonSet, RBAC, webhooks, cluster resources.

## Sample Manifests (`config/samples/`)
- `autoscalerpolicy/` — AutoscalerPolicy examples.

Runtime/model catalog YAMLs (vendor-specific ClusterServingRuntime / ClusterBaseModel) live in operators' own GitOps repos, not in-tree.
