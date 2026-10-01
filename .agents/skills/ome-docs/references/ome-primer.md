# OME in one page

Read this once per session before the first bridge call. It is the mental
model; the docs tool (`$B tool docs ...`) and the OEP index
(`oeps/STATUS.md`) hold the detail, and `kubectl-ome/README.md` holds every
verb. Status in brackets is the OEP status at the time of writing; check
`oeps/STATUS.md` before calling anything "available".

## What OME is

A Kubernetes operator (API group `ome.io/v1beta1`, control plane
`ome-controller-manager` in namespace `ome`) that runs LLM inference on
accelerator fleets: it turns a model plus a runtime into serving pods, rolls
them out safely, places them across clusters, routes traffic to them and
accounts for accelerator quota. The CLI `kubectl-ome` reads all of it; the
bot reaches that CLI only through the bridge.

## The objects, in the order a request meets them

| Object | What it is | Read with |
|---|---|---|
| `BaseModel` / `ClusterBaseModel` | a model artifact onboarded to a cluster: format, architecture, size, how it is distributed (model cache, OEP-0009) and overlays (OEP-0010) | `get models -A -o wide` |
| `ServingRuntime` / `ClusterServingRuntime` | how to run a class of models: engine image and args, `supportedModelFormats`, accelerator shapes. Runtimes inherit from parents (OEP-0012, "runtime tree"); a service may pin a runtime revision so a runtime change does not reach it until re-synced (OEP-0014) | `get runtimes -A`, `runtime tree`, `runtime effective`, `runtime explain --model` |
| `InferenceService` (ISVC) | the deployable unit: `spec.model`, `spec.runtime` (or the selector's pick), the components `engine` (serving pods), `router` (the front door) and `decoder` (prefill/decode disaggregation when set), plus `acceleratorSelector`, `rollout`, `traffic`, `scalingPolicy`, `placement`, `routing`, `replicaRefs` | `get isvc -A -o wide`, `status <name>`, `instance list` |
| `InferenceReplica` (IR) | OMENative's replica set for one component: one status row per Instance (OEP-0007); instances are Pending, Running or Failed, and a revision can be Held by a rollout | `instance list`, `instance status <name> <idx> --component engine` |
| `RolloutPolicy`, rollout runs | `spec.rollout` groups a change into canary, blueGreen or rollingUpdate steps (OEP-0017); policies are reusable (OEP-0017.1); canary can be per component (OEP-0017.2) and gated on Prometheus analysis (OEP-0015.2) | `rollout status`, `rollout explain`, `rollout history` |
| `AutoscalerPolicy` | component-scoped autoscaling (OEP-0013) with reusable policies (OEP-0013.1) | `autoscale status`, `autoscale explain` |
| `AcceleratorQuota` | fleet accelerator budgets per tenant, enforced at admission by `ome-quota-manager` (OEP-0024); a declared budget is not measured capacity | `quota status`, `quota tree`, `quota validate` |
| `WorkloadCluster`, placement | the hub registers member clusters (OEP-0018) and places a service with policy `ClusterAffinity` in mode `Single` (race, one winner), `All`, `Split` (fixed shares) or `SplitByCapacity` (OEP-0018.3, 0018.4); every placed component must be OMENative | `placement status`, `placement explain`, `placement endpoint` (hub) |
| `TrafficMap`, traffic policy | capacity-aware weights across the clusters that serve one service (OEP-0025, fallback 0025.1) and the per-service backend traffic policy (OEP-0011) | `traffic status`, `traffic explain` (hub) |
| scheduler, slices | `ome-scheduler` packs gangs by topology (OEP-0022); TPU slice provisioning is in progress (OEP-0030, nothing provisions a slice yet) | `accelerator explain`, `instance status` |
| Alfred | the GPU caretaker: defragmentation and node-health evacuation, recommend-only by default (OEP-0008); not a TPU component | `admin recommendations` |
| MUSE | the read-only console over all of the above (OEP-0021); its write plane was withdrawn | docs only |

## Status, in plain words

- A service is healthy when `Ready` is true, which needs `EngineReady`,
  `RouterReady` (and `DecoderReady` when there is one), `RuntimeReady`,
  `IngressReady`, `OverlaysReady`. `status -o wide` names the one that is
  false and why.
- CLI reports label every line Declared (what the spec asks for), Reported
  (what the controller wrote), Observed (what the API shows right now) or
  Computed (the CLI's conclusion). Prefer Reported and Observed as evidence.
- A quota line marked `Reported/Unverifiable` is a declared budget, not a
  measurement; say "budgeted", not "available".
- `cluster status` reads the hub's registry view; on a standalone cluster it
  is empty, which is not a finding.

## Failure signatures and what they usually mean

| What the CLI shows | Plain meaning | Usually because |
|---|---|---|
| engine instances Pending, event `0/N nodes are available ... untolerated taint` or `Insufficient google.com/tpu` | no node matches the accelerator shape, or the pool is full | wrong accelerator selector or topology, exhausted node pool, quota admission waiting |
| instance phase Failed, `InstanceFailed` events repeating | the engine container keeps crashing | runtime arguments, model load, probe timing; read `logs <name> --tail 200` |
| `RouterReady` false, router readiness probe failing | the front door is up but nothing healthy behind it | engines not ready, or the router points at the wrong port |
| a Held revision, rollout step waiting | the rollout is paused on purpose | a manual gate, failed analysis, or capacity pacing; moving it is the oncall's `rollout promote|resume` |
| `RolloutPlanDrift` | the spec changed while a rollout was running | needs a `rollout repin` by the oncall |
| `PlacementBackendReady` false or unknown | a member cannot take the placement | a non-OMENative backend, or runtime inputs the member cannot read |
| `Unavailable` / `Unreadable` / `forbidden` in a report | the CLI could not read that source | access or transport, never the workload |

## Release and fleet mechanics

CI builds images and charts from green `main` commits (chart
`1.0.0-<UTC timestamp>.<short SHA>`, image tag = short SHA). Kargo promotes
one freight through the stages in order (release-candidate first, then the
production planes and clusters); Argo CD applies each cluster from the GitOps
repository. `$B release` shows stage, freight, promotion time and health;
`$B clusters` shows the version each cluster actually runs. A cluster behind
its stage is an unfinished sync, not a stale release.

## From a question to the first verb

| The asker says | Start with |
|---|---|
| "down", "not ready", "stuck", "failing" | `status <name> -n <ns> -o wide`, then the branch table in `ome-answer` |
| "rollout", "canary", "promote", "held" | `rollout status`, `rollout explain` |
| "traffic", "weights", "drain", "which cluster serves" | hub: `placement status`, `traffic status` |
| "quota", "capacity", "chips", "admission" | `quota status`, `quota tree` on the cluster; node health is `ome-fleet` |
| "which runtime", "recipe", "how do I serve model X" | `ome-recipe` |
| "what is promoted", "is main red", "did my commit pass" | `ome-oncall` |
| "what does X mean", "is X implemented" | `ome-docs`, `oeps/STATUS.md` |
| "all clusters", "incident", "broken nodes" | `ome-fleet` |
