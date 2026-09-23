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

This extension implements the TrafficMap publisher contract. The shared
TrafficMap publisher reconciler owns watches, target claims, status, and its
TrafficMap finalizer; this package translates immutable publication plans into
calls to the xDS automation HTTP API:

```http
PUT /api/v1/clusters/{cluster}/weight
{"weight": 10, "createIfMissing": {"oncall": "ome"}}

DELETE /api/v1/clusters/{cluster}/weight
```

The service stores `xds_auto.cluster_weight` on its namespaced configuration
tier (`cluster` becomes `xds_auto:cluster`), creating that tier when necessary.
It does not mutate EDS membership or create a route arm, so the target must
already exist in the xDS route graph.

The required `weightMode` option selects the xDS wire representation.
`normalized` uses largest-remainder apportionment to publish nonnegative integer
weights that sum exactly to the required positive integer `normalizedTotal`; an
all-zero map remains all-zero. Equal remainders are resolved by canonical target
name, so input order does not affect rounding. A small positive share can round
to zero when the configured total is too coarse to represent it. The total must
be from 1 through 4,294,967,295. `relative` publishes the TrafficMap's integer
ratios directly and rejects `normalizedTotal`. With the routing probe's explicit
all-failed `Drain` policy, either mode publishes zero for every target. Changing
either option changes the publisher options digest and republishes each selected
TrafficMap.

Existing xDS deployments must add `weightMode` when adopting a binary with this
option, and normalized deployments must also set `normalizedTotal`. Use
`relative` without `normalizedTotal` for the first rollout when the wire
representation must remain unchanged, then switch every leader candidate to
`normalized` with the intended total; binaries that predate these options reject
them during configuration validation.

Selecting `routing.publisher.name: xds` replaces the default Gateway API
publisher; the two publishers do not run side by side.

OME journals canonical targets in `TrafficMap.status.publisher.claimedTargets`
before each external call. Targets removed from an active map are held at
weight zero; TrafficMap deletion clears exactly the targets in that journal
before its publisher finalizer is released. The shared reconciler also prevents
two TrafficMaps in one control plane from claiming the same target. Because
these external mutations are stateful, manager leader election must be enabled
when this publisher is selected.

Configuration is deliberately explicit and has no defaults:

```yaml
routing:
  enabled: true
  publisher:
    name: xds
    resyncInterval: 1m
    options:
      oncall: ome
      acl: ome-xds-publisher       # optional
      sourceSelector: 'routing.example.com/publish=true'  # optional
      weightMode: normalized
      normalizedTotal: "100"
      targetTemplate: 'gke_llm_tpu_{{replaceAll "-" "_" .Service}}_{{replaceAll "-" "_" (mustTrimSuffix "-tpu" (mustTrimPrefix "msl-gcp-" .Cluster))}}'
      urlTemplate: 'https://xds-automation.mrpc.fbinfra.net/api/v1/clusters/{cluster}/weight'
      requestTimeout: 30s
      tlsCertFile: /var/facebook/x509_identities/server.pem
      tlsKeyFile: /var/facebook/x509_identities/server.pem
      tlsCAFile: /var/facebook/rootcanal/ca.pem
```

`publisher.resyncInterval` is the common TrafficMap publisher reconciliation
backstop. It is not an xDS option and is required whenever routing is enabled.
The transport-specific settings remain under `publisher.options`.

The target template receives `Namespace`, `Service`, `Cluster`, `Endpoint`,
`EndpointHost`, `EndpointHostname`, and `EndpointPort`. It must render a bare
cluster name. A `;fallback=` suffix is canonicalized to its primary tier.
`{cluster}` in `urlTemplate` is replaced with the path-escaped result.

Templates also provide `replaceAll old new value`,
`mustTrimPrefix prefix value`, and `mustTrimSuffix suffix value`. The `must`
helpers stop publication when an input does not have the configured affix or
has no value left after trimming. The example above implements the conventional
logical-name mapping for the selected GKE-backed services while keeping its
deployment-specific prefixes in configuration. The xDS configuration stores a
logical cluster name and gateway hostname independently, so use an explicit
template for any target that does not follow that convention.

`sourceSelector` is an optional standard Kubernetes label selector evaluated
against each source InferenceService. Omitted or empty matches every source for
compatibility. A fresh nonmatching source converges to `Published=False` with
reason `Withdrawn` without claiming or calling any xDS target. If the source
previously matched, the shared publisher lifecycle first drains its existing
claims to zero and retains those claims until the whole TrafficMap is deleted.
Source label changes enqueue publication immediately; changing the global
selector requires the same manager restart as other ConfigMap settings. Legacy
endpoint finalizers and target annotations still block planning even for a
nonmatch, so selection cannot bypass the required old-lifecycle cleanup.

Publisher options on individual InferenceServices are rejected. In particular,
`targetTemplate`, `sourceSelector`, `weightMode`, and `normalizedTotal` are
installation-wide: allowing an object to replace them could redirect writes or
make shared target weights inconsistent. These global publication settings
participate in the per-TrafficMap options digest; transport paths, credentials,
and service metadata remain manager-local.

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

Upgrading from the InferenceService-owned publisher lifecycle requires a drain
with the old binary:

1. Keep `publisher.name: xds`, deploy the old binary with
   `routing.enabled: false`, and leave its publisher configuration in place.
   The old binary still expects `resyncInterval` under `publisher.options`;
   move it to `publisher.resyncInterval` only with the new binary.
2. Wait until every affected InferenceService has neither the
   `ome.io/placement-endpoint` finalizer nor the
   `routing.ome.io/xds-published-targets` annotation.
3. Deploy the TrafficMap-owned publisher and re-enable routing.

Before adopting an unjournaled TrafficMap, the shared reconciler waits until no
InferenceService in the cluster retains the old endpoint finalizer. This
prevents a partially drained rollout from publishing a target that an old
lifecycle still owns. Once a TrafficMap has its new publisher journal, it
continues to refresh while other metadata changes so a user-editable finalizer
cannot freeze established traffic updates.

The xDS adapter deliberately does not interpret, migrate, or delete the old
annotation. It rejects a TrafficMap while that map's owner has either legacy
marker, including an empty or malformed annotation, because only the old
lifecycle can prove that its external overrides were cleaned. An annotation on
another owner is not treated as a target claim. The same drain rule applies
before switching between xDS and Gateway API: the selected publisher can
finalize only journals it owns.
