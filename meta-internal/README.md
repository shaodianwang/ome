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

The manager logs the formats a binary carries at startup
(`capacityFormats=[...]`), so a build missing an optional one is visible
immediately rather than at the first poll. Naming a format the build does not
carry is a startup error, never a silent fallback to the plan.
