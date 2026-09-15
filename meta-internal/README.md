# meta-internal

Deployment-specific extensions that are **not** part of the OME contract.

Everything here is optional and additive. The repository is designed so this
directory can be deleted outright: what you lose is the features listed below,
not a working build.

## The deletion contract

A default build never compiles this tree. Every file here carries the
`meta_internal` build tag, and the only reference from outside is one
build-tagged file in the manager's composition root:

```
cmd/manager/register_meta_internal.go   //go:build meta_internal
```

So:

| You delete | Untagged build (`go build ./...`) | Tagged build (`-tags meta_internal`) |
|---|---|---|
| `meta-internal/` only | works, features gone | **breaks** — the import above dangles |
| `meta-internal/` + the file above | works, features gone | works, features gone |

Deleting the directory alone is therefore safe for anyone not opting in, which
is the case that matters for a consumer of this repository. A deployment that
*does* opt in removes both, and the tag then matches no files and turns into a
no-op rather than an error.

Verify either way with:

```sh
go build ./...            # must pass with the directory absent
go vet ./pkg/... ./cmd/...
go test ./pkg/controller/v1beta1/placement/routing/ ./cmd/manager/
go test -tags meta_internal ./meta-internal/...
```

Why one file outside the directory: Go links statically and has no plugin
loading here, so *something* compiled has to reference the package for its
`init()` to run. A build tag is the smallest such surface, and it keeps the
reference out of every build that has not asked for it.

## Extending, not forking

Extensions register against an interface the OME packages own, rather than
editing them. Adding one should not require a diff outside this directory
beyond the single registration file.

Current extension points:

| Registry | Owner |
|---|---|
| `routing.RegisterCapacityFormat` | `pkg/controller/v1beta1/placement/routing` |
| `routing.RegisterTrafficMapPublisher` | `pkg/controller/v1beta1/placement/routing` |

Registration collides loudly: registering a name twice is an error rather than
an overwrite, so the active implementation can never depend on import order.

## What lives here

### `capacityformat/` — the `LoadArray` capacity response format

The routing controller polls each home for what it can serve and applies the
answer as a ceiling on that home's planned allocation. It ships one built-in
response shape, `Report`: an absolute servable count plus an `observedAt` stamp
the producer sets.

`LoadArray` reads a two-element array of floats — a damped reading followed by
an undamped one — from a router that predates that contract and cannot be
changed to emit it.

**Prefer `Report` for any producer you control.** `LoadArray` is a
compatibility shim and is weaker in two ways that decide real incidents:

- **No timestamp.** The reading is stamped at receipt, so `maxAge` can never
  fire. A home wedged while still answering keeps its ceiling indefinitely.
  `Report` does not have this gap.
- **It does not cover prefill/decode skew.** Where it counts only prefill-side
  accelerators it never observes decode, so it reports full capacity during
  exactly the rollout window the capacity input was justified by. It covers a
  different failure — accelerators that are unhealthy or ejected.

Configuration (all under the routing block of the operator ConfigMap):

```yaml
routing:
  capacity:
    path: /some/capacity/route
    method: GET
    format: LoadArray
    options:
      unitsPerReplica: "4"   # weighted accelerators per replica
    period: 30s
    timeout: 5s
    maxAge: 5m               # accepted, but inert for this format
```

`unitsPerReplica` is required: the reading is in weighted accelerators while the
plan counts replicas, and comparing them directly would produce a meaningless
ceiling. Because the reading already folds in hardware weighting, do not also
set a per-home `capacityFactor` for the same home — that applies the weighting
twice.

### `xdspublisher/` — TrafficMap to the internal xDS publisher

This extension implements the existing `placement/endpoint.EndpointPublisher`
contract. The shared endpoint reconciler continues to own the
InferenceService/TrafficMap watches and finalizer; this package only translates
TrafficMap homes into calls to the native cluster-weight API:

```go
p, err := publisher.NewClusterWeightPublisher(oncall, acl)
err = p.PublishWeight(cluster, weight)
err = p.ClearWeight(cluster)
```

The API stores `xds_auto.cluster_weight`; it does not mutate EDS membership.
The target must already be a statically authored route arm, and its SMC tier
must already contain usable service endpoints.

Selecting `routing.publisher.name: xds` replaces the default Gateway API
publisher; the two publishers do not run side by side.

OME journals targets on the source InferenceService before each external call.
Targets removed from an active map are held at weight zero; deletion or an
explicit disable clears the overrides before the shared endpoint finalizer is
released. Journals also prevent two InferenceServices in one control plane from
claiming the same target.

Configuration is deliberately explicit and has no defaults:

```yaml
routing:
  enabled: true
  publisher:
    name: xds
    options:
      oncall: ome
      acl: ome-xds-publisher       # optional
      targetTemplate: '{{.EndpointHostname}}'
      resyncInterval: 1m
```

The target template receives `Namespace`, `Service`, `Cluster`, `Endpoint`,
`EndpointHost`, `EndpointHostname`, and `EndpointPort`. It must render a bare
cluster name. A `;fallback=` suffix is canonicalized to its primary tier.

The Ligen-generated package is not a Go module. A checked-in `.go` import would
make `go mod tidy` attempt to resolve it publicly, so the binding lives in
`native_meta_xds.go.in`. A deployment-owned Buck composition must materialize
it as `native_meta_xds_generated.go`, provide the Ligen dependency, and supply
both `meta_internal` and `meta_xds`. Only that composition registers
`publisher.name: xds`; the normal OME image and a plain `-tags meta_internal`
build do not advertise an unusable publisher.

The composing Go target must use `cgo = True` and depend on
`//mrpc/xds/automation:cluster_weight_publisher-go_serialized` when OME is
mirrored into the fbcode cell (or the cell-qualified `fbcode//mrpc/...` label
from another registered cell), matching the D119218279 smoke test. No xDS-owned
source change is required. OME is not currently a Buck project, so adding a
BUCK file here alone is insufficient: the internal build must first place OME
and its transitive Go dependencies in the shared Buck graph.

The native API is synchronous and exposes no context or deadline. Periodic
reconciliation repairs drift, but callers should not enable the adapter until
the native implementation's timeout behavior is acceptable. Disable it in two
steps: first set `routing.enabled: false` while keeping `publisher.name: xds`,
then remove the publisher configuration after the endpoint finalizers drain.
Likewise, drain the currently selected backend before switching between xDS and
Gateway API; the shared finalizer invokes only the publisher selected at startup.
