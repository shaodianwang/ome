# MUSE

Read-plane API server for OME (OEP-0021): read-only, **multi-cluster** OME +
Node/GPU **capacity** views, served entirely from informer caches — never a
per-request apiserver call. Built on [`ome-client-rs`](../ome-client-rs).

## Run

```sh
# single cluster (ambient kube config):
make -C muse run

# multiple clusters + the cluster's real topology labels:
set -a; . muse/config/dev.env; set +a
cd muse && cargo run -p muse-server -- \
  --cluster cw-use2=~/.kube/msl-cw-use2-dev-2.conf \
  --cluster cw-use3=~/.kube/msl-cw-use3-dev.conf \
  --rdma-island-label   network.meta.com/ai-zone,backend.coreweave.cloud/fabric \
  --nvlink-domain-label network.meta.com/domain,nvidia.com/gpu.clique
```

The local values above come from [`config/dev.env`](config/dev.env); production
values come from `charts/muse/values.yaml`. The binary has no behavioral
fallbacks. With the checked-in development config, the API listens on `:8080`
and Prometheus `/metrics` on `:9090`. Each
topology flag accepts a comma-separated candidate list (first present wins), so
one config covers clusters that label topology differently. An unreachable
cluster is skipped, not fatal.

## Verify

```sh
make ci
```

This checks Rust formatting and Clippy, runs the Rust tests, installs the web
dependencies without writing a lockfile, and tests and builds the console.
Buildkite runs the equivalent Rust group on a native Rust image. The existing
`muse-server` image build runs the web checks in its cached Node stage before
the production web build.

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
| `GET /v1/metrics?cluster=&namespace=&inferenceservice=[&range=]` | cached Query Gateway metrics for one exact deployment; fleet scope is available only when the configured catalog declares safe fleet queries |
| `GET /v1/{kind}[?cluster=]` | list a kind across clusters; each row tagged with `cluster` |
| `GET /v1/{kind}/{name}[?raw=true][?cluster=][?namespace=]` | get one by full identity (projected view, or full object with `raw=true`); `namespace` disambiguates namespaced kinds |

`{kind}` ∈ `inferenceservices`, `basemodels`, `servingruntimes`,
`inferencereplicas`, `clusterbasemodels`, `clusterservingruntimes`,
`finetunedweights`, `workloadclusters`, `nodes`.

## Why aggregate server-side?

Per-resource reads are thin projections (the frontend renders them; `?raw=true`
escapes to the full object). But cross-resource analytics — GPU
allocated-vs-allocatable (a Node × Pod join) and fleet roll-ups across clusters —
live here because the frontend can't cheaply join thousands of pods or fan out to
N clusters.

Monitoring is the exception to the informer-only rule: it calls the configured
PromQL Query Gateway on demand. The server owns the query catalog, safely expands
only configured cluster identity and exact cached pod names, coalesces identical requests, and
bounds upstream concurrency, timeout, response bytes, result series, and cache
size. Missing metrics configuration never affects Kubernetes inventory.
