# MUSE

Read-plane API server for OME (OEP-0021): read-only, **multi-cluster** OME +
Node/GPU **capacity** views, served entirely from informer caches — never a
per-request apiserver call. Built on [`ome-client-rs`](../ome-client-rs).

## Run

```sh
# single cluster (ambient kube config):
cargo run -p muse-server

# multiple clusters + the cluster's real topology labels:
cargo run -p muse-server -- \
  --cluster cw-use2=~/.kube/msl-cw-use2-dev-2.conf \
  --cluster cw-use3=~/.kube/msl-cw-use3-dev.conf \
  --rdma-island-label   network.meta.com/ai-zone,backend.coreweave.cloud/fabric \
  --nvlink-domain-label network.meta.com/domain,nvidia.com/gpu.clique
```

API on `:8080`, Prometheus `/metrics` on `:9090` (both configurable). Each
topology flag accepts a comma-separated candidate list (first present wins), so
one config covers clusters that label topology differently. An unreachable
cluster is skipped, not fatal.

The topology flags have **no default** — the correct label keys are
provider/cluster-specific, so they must be supplied for your environment (flag,
`MUSE_RDMA_ISLAND_LABEL` / `MUSE_NVLINK_DOMAIN_LABEL` env, or GitOps). Nodes
carrying none of the configured labels group as `unassigned` and are excluded
from the placement planner rather than collapsed into a fabricated domain.

## Endpoints

| Method/Path | Description |
|---|---|
| `GET /healthz` `/livez` `/readyz` | health |
| `GET /v1/clusters` | registered clusters + per-cluster summary |
| `GET /v1/capacity[?cluster=]` | fleet GPU allocatable/allocated/free, grouped by cluster / RDMA island / NVLink domain / instance type |
| `GET /v1/{kind}[?cluster=]` | list a kind across clusters; each row tagged with `cluster` |
| `GET /v1/{kind}/{name}[?raw=true][?cluster=]` | get one (projected view, or full object with `raw=true`) |

`{kind}` ∈ `inferenceservices`, `basemodels`, `servingruntimes`,
`inferencereplicas`, `clusterbasemodels`, `clusterservingruntimes`,
`finetunedweights`, `workloadclusters`, `nodes`.

## Why aggregate server-side?

Per-resource reads are thin projections (the frontend renders them; `?raw=true`
escapes to the full object). But cross-resource analytics — GPU
allocated-vs-allocatable (a Node × Pod join) and fleet roll-ups across clusters —
live here because the frontend can't cheaply join thousands of pods or fan out to
N clusters.
