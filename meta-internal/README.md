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

The repository's `ome-manager` Make target and internal manager Dockerfile opt
in explicitly. A deployment removing this tree must also remove that tagged
registration file and the Dockerfile's `COPY meta-internal/` instruction.

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
TrafficMap homes into calls to the xDS automation HTTP API:

```http
PUT /api/v1/clusters/{cluster}/weight
{"weight": 10, "createIfMissing": {"oncall": "ome"}}

DELETE /api/v1/clusters/{cluster}/weight
```

The service stores `xds_auto.cluster_weight` on its namespaced configuration
tier (`cluster` becomes `xds_auto:cluster`), creating that tier when necessary.
It does not mutate EDS membership or create a route arm, so the target must
already exist in the xDS route graph.

TrafficMap weights are applied verbatim. With the routing probe's explicit
all-failed `Drain` policy, this includes publishing zero for every target; probe
recovery republishes the newly computed capacity weights.

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
      urlTemplate: 'https://xds-automation.mrpc.fbinfra.net/api/v1/clusters/{cluster}/weight'
      requestTimeout: 30s
      tlsCertFile: /var/facebook/x509_identities/server.pem
      tlsKeyFile: /var/facebook/x509_identities/server.pem
      tlsCAFile: /var/facebook/rootcanal/ca.pem
```

The target template receives `Namespace`, `Service`, `Cluster`, `Endpoint`,
`EndpointHost`, `EndpointHostname`, and `EndpointPort`. It must render a bare
cluster name. A `;fallback=` suffix is canonicalized to its primary tier.
`{cluster}` in `urlTemplate` is replaced with the path-escaped result.

The client presents the configured workload certificate directly to the remote
proxy and trusts only the configured CA bundle. It deliberately ignores ambient
HTTP proxy settings because a proxy would terminate mutual TLS under another
identity. The client certificate is reloaded for each new TLS connection so
workload certificate rotation does not require a manager restart. Every call is
bounded by both the reconcile context and `requestTimeout`; periodic
reconciliation retries transient failures. The configured certificate, key,
and CA files must be mounted in the controller container.

The internal manager image compiles this package with `meta_internal`; no
fbsource mirror, Ligen binding, or alternate OME build graph is required.
Disable it in two steps: first set `routing.enabled: false` while keeping
`publisher.name: xds`, then remove the publisher configuration after the
endpoint finalizers drain. Likewise, drain the currently selected backend
before switching between xDS and Gateway API; the shared finalizer invokes only
the publisher selected at startup.
