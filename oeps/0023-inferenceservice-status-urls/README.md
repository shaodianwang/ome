# OEP-0023: InferenceService status URLs — correct, gateway-aware endpoints

## Status and decision

**Provisional.** This OEP diagnoses a shipped correctness bug (`status.url` is
wrong or incomplete on real shared-host and multi-gateway clusters) and proposes
a fix. The diagnosis and evidence are settled; the status *schema* shape (one
canonical URL vs. a list) and which endpoint is "canonical" are the open design
decisions — see [Open questions](#open-questions).

## Summary

`InferenceService.status.url` is supposed to be the URL a caller uses to reach
the service. Today it is derived from a code path (`createRawURL` →
`DomainService.GenerateDomainName`, rendering the `domainTemplate`) that is
**completely separate** from the code path that builds the actual Gateway API
routes (`httproute_builder.gatewaysForRoute` + `gatewayHostnameForDomain` +
`pathPrefix`). Two independent renderers of "the URL" guarantee drift, and they
have drifted: on real clusters, `status.url` is either **non-routable** or
**missing the endpoint the caller actually wants**.

This OEP proposes (1) deriving status URLs from the *same* builder that produces
the routes — a single source of truth so they cannot drift — and (2) extending
status to carry **all** externally-reachable endpoints (internal + external +
per-namespace gateways), because OME now attaches one route to multiple gateways
and a single `url` field structurally cannot represent that.

## Motivation

### The failure mode (what breaks today)

Two distinct, live-verified failures, captured on three real clusters — two
shared-host clusters (A and B) and one multi-gateway cluster (C) — by comparing
`status.url` against the actual `HTTPRoute` (hostnames + path matches) and the
`Gateway` listeners on each cluster.

**Failure 1 — shared-host/path clusters: `status.url` is non-routable.**
On the shared-host clusters (A and B), the ingress config is
`perISVCSubdomain: false` with `sharedHostPrefix: "llm"`. Routing is
shared-host + path-prefix. For isvc `team-b/my-model`:

| | value |
|---|---|
| `status.url` | `https://my-model.team-b.int-gw-https.example.com` |
| actual `HTTPRoute` host | `llm.int-gw-https.example.com` |
| actual `HTTPRoute` path | `PathPrefix: /team-b/my-model/` |
| real reachable URL | `https://llm.int-gw-https.example.com/team-b/my-model/` |
| gateway listeners that exist | `int-gw-https`, `ext-gw-https`, `int-gw-tls`, `ext-gw-tls` |

The advertised host `my-model.team-b.int-gw-https…` is a
per-ISVC subdomain that **has no gateway listener** and the URL **omits the
`/team-b/<isvc>/` path prefix the route requires**. A caller who copies
`status.url` gets a URL that does not resolve and would not route even if it did.
`status.url` is simply wrong on these clusters — for every isvc.

**Failure 2 — multi-gateway clusters: `status.url` hides the endpoint callers
need.** On cluster C, the config is `perISVCSubdomain: true` with a
cluster-default primary (internal) gateway, a cluster-default **additional
(external)** gateway, and per-namespace int/ext gateway pairs
(`namespaceIngressGateways` for several namespaces). For isvc
`team-a/gpt-oss-20b`:

| | value |
|---|---|
| `status.url` | `https://gpt-oss-20b.team-a.int-gw-https.cluster-c.example.com` |
| actual `HTTPRoute` hostnames | `…int-gw-https…` **and** `gpt-oss-20b.team-a.ext-gw-https.cluster-c.example.com` |
| actual `HTTPRoute` parentRefs | `team-a-int-gw-https` **and** `team-a-ext-gw-https` |

Here `status.url` is *reachable* (the internal host is real) but exposes only the
**internal** endpoint. The **external** endpoint — the one an off-cluster
consumer must use — is on the same route, but `status` never surfaces it. A
consumer cannot discover the external URL from the API object at all.

### Root cause

`status.url` and the routes are rendered by two different functions from two
different inputs:

- **Routes** (`pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/httproute_builder.go`):
  `gatewaysForRoute` resolves the per-namespace / additional gateways and, for
  each, `gatewayHostnameForDomain` produces the host (honoring
  `perISVCSubdomain`, `sharedHostPrefix`, and each gateway's own
  `ingressDomain`); `pathPrefix` produces `/<ns>/<svc>/` in shared-host mode.
- **Status** (`.../ingress/strategies/gateway_api_strategy.go` `createRawURL`
  → `.../ingress/services/domain_service.go` `GenerateDomainName`): renders the
  single cluster-default `ingressDomain` through `domainTemplate`
  (`{{.Name}}.{{.Namespace}}.{{.IngressDomain}}`), with no path, no
  `sharedHostPrefix`, and no awareness of `additionalIngressGateways` or
  `namespaceIngressGateways`.

A comment in `httproute_builder.go` asserts the route host and the status host
"can't drift" — but that invariant only holds in the `perISVCSubdomain: true`
branch. In the `perISVCSubdomain: false` branch (clusters A and B) the two diverge
completely (Failure 1), and in the multi-gateway case (cluster C) status is a strict
subset of the routes (Failure 2). **Two renderers of the same fact is the root
cause; one renderer is the fix.**

Secondary defects the same investigation surfaced (in scope because they live in
the same URL-construction code; see [Design Details](#design-details)):

- **Hardcoded `http` scheme** in three status writers — the per-component URL
  (`raw_kube_reconciler.go createRawURL`), `VirtualDeployment`
  (`controller.go handleVirtualDeployment`), and the disabled-ingress external
  URL (`controller.go setExternalServiceURL`) — all emit `http://` regardless of
  `urlScheme: https`. On every cluster above `urlScheme` is `https`, so these
  fields are wrong whenever they are set. This also violates the repo's
  "no hardcoded magic values" design constraint.
- **Raw vs Gateway strategy inconsistencies:** the Kubernetes-Ingress strategy
  appends `:<port>` to `status.url` and uses the bare `<isvc>` service host for
  `status.address`, while the Gateway-API strategy appends no port and uses
  `<isvc>-engine`. Consumers see different shapes depending on which strategy a
  cluster runs.

### Goals

1. **`status.url` is always a real, reachable URL** — correct in *both* routing
   modes (`perISVCSubdomain` true and false) and for *any* gateway config we
   ship. *Rationale:* the field's entire purpose is "paste this and reach the
   service"; a non-routable value (Failure 1) is worse than an empty one.
2. **All externally-reachable endpoints are discoverable from status** — when a
   route is attached to N gateways (internal + external + per-namespace), status
   exposes all N, tagged so a consumer can pick the external one.
   *Rationale:* Failure 2 — the external endpoint exists on the route but is
   invisible in the API; multi-gateway routes are the default on cluster C.
3. **Single source of truth.** Status URLs are derived from the *same* builder
   logic that produces the `HTTPRoute` hosts + paths, so they cannot drift.
   *Rationale:* the root cause is two renderers; anything less than merging them
   re-opens the same bug the next time the route builder changes.
4. **Honor `urlScheme` everywhere.** No hardcoded `http`.
5. **Backward compatible.** Existing consumers of `status.url` /
   `status.address` keep working; new information is additive.

### Non-Goals

- **Changing routing behavior.** The `HTTPRoute` objects are correct (they carry
  both hosts and both parentRefs — verified on cluster C). This OEP changes only what
  `status` *reports*, plus the shared derivation. No change to `gatewaysForRoute`
  routing semantics.
- **Adding gateway configuration.** Per-namespace / additional gateways already
  exist (`namespaceIngressGateways`, `additionalIngressGateways`).
- **DNS provisioning / TLS issuance.** Whether a hostname resolves or has a cert
  is the platform's job; this OEP reports the URL OME's routes define.
- **Multi-cluster global endpoint.** OEP-0018's `PlacementStatus.Endpoint`
  mirrors `status.url`; it is a *consumer* of this fix (it currently mirrors a
  wrong value) but its own design is out of scope. See-also in `oep.yaml`.
- **Splitting REST vs gRPC URLs / protocol multiplexing.** `ComponentStatusSpec.RestURL`
  exists but is never written. Protocol/port addressing is orthogonal to URL
  correctness and is deferred to a later OEP; this OEP leaves `RestURL` untouched
  (a candidate for separate cleanup).

## Terminology

Defined before use, with the consequence baked in:

- **Subdomain routing** (`perISVCSubdomain: true`): each isvc gets its own host,
  `<isvc>.<ns>.<domain>`, path `/`. The host identifies the service. Used on
  cluster C.
- **Shared-host / path routing** (`perISVCSubdomain: false`): all isvcs share one
  host, `<sharedHostPrefix>.<domain>`, and are disambiguated by a path prefix
  `/<ns>/<isvc>/` (an Envoy `urlRewrite` filter strips it before forwarding).
  Used on clusters A and B. **The path is
  load-bearing: drop it and the request does not route.**
- **Primary (internal) gateway** (`omeIngressGateway` + `ingressDomain`): the
  default gateway every route attaches to; by convention the in-datacenter
  ("int") gateway, e.g. `…int-gw-https…`.
- **Additional / external gateway** (`additionalIngressGateways[]`, each with its
  own `omeIngressGateway` + `ingressDomain`): extra gateways every route *also*
  attaches to; by convention the externally-exposed ("ext") gateway, e.g.
  `…ext-gw-https…` (typically an internet-facing load balancer).
- **Per-namespace gateway** (`namespaceIngressGateways[<ns>]`): overrides the
  primary and additional gateways for isvcs in `<ns>` (e.g. `team-a` routes via
  `team-a-int-gw-https` / `team-a-ext-gw-https` instead of the cluster
  default). The *host* is unchanged (the namespace is already in the host); the
  *parentRef gateway* differs.
- **Endpoint** (proposed): one reachable URL for the isvc, tagged with a
  free-form **class** (`internal` / `external` / `cluster-local` / any
  config-defined value) that the operator declares on the gateway.

## Evidence: what each cluster actually reports

Cross-cluster snapshot, showing `status.url` against the real route:

| Cluster | mode | gateways (OME config) | real reachable URL | `status.url` today | correct? |
|---|---|---|---|---|---|
| C | subdomain | int + ext (cluster-default **and** per-ns) | `…int…` **and** `…ext…` host | `…int…` only | reachable, **external missing** |
| A | shared-host/path | int only | `llm.int-gw-https…/<ns>/<isvc>/` | `<isvc>.<ns>.int-gw-https…` (subdomain, no path) | **non-routable** |
| B | shared-host/path | int only | `llm.int-gw-https…/<ns>/<isvc>/` | `<isvc>.<ns>.int-gw-https…` | **non-routable** |

Consider a cluster that exposes an internet-facing gateway alongside the shared
internal one (e.g. `ext-gateway-https`) at the infrastructure layer, while its
OME config leaves `additionalIngressGateways` unset. Its isvcs are not wired to
that gateway *and* status has no external URL. That is a config gap (out of
scope) but it underscores why status must faithfully report whatever gateways a
route is attached to, not a fixed assumption.

## Proposal

### Part 1 — one renderer for hosts + paths (kill the drift)

Extract the host/path derivation that `httproute_builder.go` already performs
into a function the status writer calls, so status and routes are produced from
the same code. Concretely, a function that, given `(isvc, ingressConfig)`,
returns the same set of `(gateway, host, pathPrefix, scheme)` tuples that
`gatewaysForRoute` + `gatewayHostnameForDomain` + `pathPrefix` produce for the
route. The status URL for a tuple is then exactly
`<scheme>://<host><pathPrefix>` — including the `/<ns>/<isvc>/` prefix in
shared-host mode (fixing Failure 1) and one tuple per gateway (fixing Failure 2).

*Rationale:* any fix that re-derives the URL a second way will drift again the
next time the route builder changes. The only durable fix is a shared function;
this is a required property, not a preference.

### Part 2 — status carries all endpoints

A single `status.url` cannot represent a route attached to N gateways. Add a
structured list to status (`status.addresses`, exact shape in Design Details),
one entry per gateway the route attaches to plus the cluster-local address, each
tagged with a free-form `class` (see Part 5). This list is added **both** at the
top level and per-component (`ComponentStatusSpec`), since OME already emits a
per-component `HTTPRoute` for `<isvc>-engine` / `<isvc>-router` /
`<isvc>-decoder` and PD/disaggregated topologies may address a component
directly. Keep `status.url` and `status.address` as backward-compatible
projections of the list (Part 3).

### Part 3 — `status.url` = the primary gateway's URL (decided)

`status.url` is the URL of the **config-designated primary gateway**
(`omeIngressGateway`, or the per-namespace `primary`), whatever `class` the
operator labeled it. Today that is the int-gw, so the field's *meaning* is
unchanged — it just becomes correct.

*Rationale:* the alternative (promote the external endpoint to `status.url` when
one exists) makes the field's meaning **cluster-dependent** — clusters A and B have no
external gateway, so `url` would mean different things per cluster — and breaks
the two properties worth keeping: `status.url` is always populated and stable. A
consumer that specifically wants the external endpoint selects it explicitly via
`addresses[class=="external"]`; explicit beats a magic promotion. Note the
canonical is defined by the **primary gateway**, not by the string `"internal"`,
so OME never treats any class name as special.

### Part 4 — scheme correctness

Route the three hardcoded-`http` writers (per-component, VirtualDeployment,
external-service) through `urlScheme`, so `https` clusters report `https`. Small,
mechanical, and removes a hardcoded-magic-value design-constraint violation.
`status.url` carries **no explicit port** for gateway URLs (the port is implied
by scheme —
`:443` for `https`); this standardizes the two ingress strategies, which
currently disagree (the Kubernetes-Ingress strategy appends `:<port>`, Gateway
API does not).

### Part 5 — endpoint `class` is declared in config, free-form

Each gateway config entry carries an optional `class` string
(`IngressGatewaySpec.Class`) that the controller copies verbatim onto the
endpoint. The value space is **open** — adding a class (`exp`, `partner`, …) is a
config change, never an API change. `internal` / `external` / `cluster-local`
are *documented conventional values* that OME tooling recognizes, but any value
is accepted. A light *format* constraint applies (RFC-1123 label:
`[a-z0-9]([-a-z0-9]*[a-z0-9])?`) so the class is safe as a selector key / metrics
dimension / path segment. This retires the "additional gateway ⇒ external" guess:
the controller infers nothing — it reports the declared class.

## Design Details

### Proposed status shape (additive)

Reuse `knative.dev/pkg/apis/duck/v1.Addressable` — the duck type OME already uses
for `status.address` — as the endpoint element, with `Name` carrying the
operator-declared class. Add the list at both the top level and per-component:

```go
// InferenceServiceStatus (additions only; existing url/address unchanged)
type InferenceServiceStatus struct {
    // ... existing: Address *duckv1.Addressable, URL *apis.URL, Components ...

    // Addresses lists every reachable endpoint for this service — one per
    // gateway its route attaches to, plus the cluster-local address. Each
    // entry's Name carries the operator-declared class (see
    // IngressGatewaySpec.Class): "internal", "external", "cluster-local", or
    // any config-defined value. Derived from the same builder that produces
    // the HTTPRoutes, so it never drifts from the actual routing.
    // +optional
    Addresses []duckv1.Addressable `json:"addresses,omitempty"`
}

// ComponentStatusSpec (additions only; existing url/restURL/address unchanged)
type ComponentStatusSpec struct {
    // ... existing: URL *apis.URL, RestURL *apis.URL, Address *duckv1.Addressable ...

    // Addresses is the per-gateway endpoint list for THIS component's route
    // (<isvc>-engine / <isvc>-router / <isvc>-decoder). Same shape/semantics
    // as InferenceServiceStatus.Addresses. Lets a PD/disaggregated consumer
    // address a component directly.
    // +optional
    Addresses []duckv1.Addressable `json:"addresses,omitempty"`
}
```

`duckv1.Addressable` is `{ URL *apis.URL; Name *string; ... }` — `Name` = the
class (exactly how Knative `Route.status.addresses` uses it), `URL` = the full
reachable URL including the path prefix in shared-host mode
(`https://llm.int-gw-https.<cluster>/<ns>/<isvc>/`). Reusing it keeps OME
consistent with the duck-typing it already relies on and brings the
`CACerts`/`Audience` fields for free if per-endpoint TLS/auth is surfaced later.
We deliberately do **not** carry the source gateway's `namespace/name` in status:
within one isvc a class is effectively unique, so consumers select by class, and
gateway provenance is derivable from the `HTTPRoute` if ever needed.

Config side — the class is **declared, not guessed**. Add an optional `Class` to
the gateway spec so the operator labels each gateway; the controller copies it
into the endpoint's `Name`:

```go
type IngressGatewaySpec struct {
    OmeIngressGateway string `json:"omeIngressGateway"`
    IngressDomain     string `json:"ingressDomain"`
    // Class labels this gateway's endpoints in status ("internal", "external",
    // "exp", ...). Free-form; NO in-code default (config-driven per the repo's
    // no-hardcoded-magic-values constraint). Must match RFC-1123 label form
    // [a-z0-9]([-a-z0-9]*[a-z0-9])? so it is safe as a selector/metrics/path
    // token. Empty ⇒ endpoint carries no class Name.
    // +optional
    Class string `json:"class,omitempty"`
}
```

- **Why a free-form string, not a closed enum.** *Rationale:* the endpoint
  taxonomy must be exactly as open as the gateway config that produces it. OME
  already lets an operator add arbitrary gateways in config (cluster C has `int`/`ext`
  today; an `exp` gateway is a pure config change). A closed
  `Internal|External|ClusterLocal` enum would turn "add a gateway class" into a
  CRD-schema + client change, and would force the controller to *guess* scope
  from gateway provenance ("additional ⇒ external") — a guess that is wrong the
  moment an operator adds a third class. A config-sourced string classifies by
  declared intent and never needs an API bump. Prior art: Knative
  `Route.status.addresses []Addressable` keys endpoints by a free-form `Name`.
- `status.url` = the **primary gateway** endpoint's URL (Part 3), kept for
  compatibility.
- `status.address` = the cluster-local endpoint (unchanged semantics), corrected
  for scheme; it also appears in `Addresses` with `Name: "cluster-local"`.
- **(historical note, now decided)** An earlier draft proposed a custom
  `ServiceEndpoint{url, class, gateway}` type; we chose to reuse
  `duckv1.Addressable` instead — idiomatic, already imported, and the dropped
  `gateway` provenance field is not load-bearing (consumers select by class;
  provenance is derivable from the `HTTPRoute`).

### Relationship to `status.url` / `status.address` (no deprecation)

`status.addresses` is the **source of truth**; `status.url` and `status.address`
become **projections** of it, kept for backward compatibility and never removed.
*Rationale:* `status.url` is read by kubectl, dashboards, users, and OEP-0018's
`PlacementStatus.Endpoint`; deprecating a universally-read field is churn with no
benefit. Prior art agrees — Knative added `Route.status.addresses` (plural)
*alongside* `status.address` (singular) without deprecating the singular; KServe
keeps `status.url`.

- `status.url` = the URL of the **primary gateway** endpoint (Part 3). A consumer
  that just wants "the URL" keeps reading it unchanged; one that wants a specific
  class reads the `addresses` entry whose `Name` matches (e.g. `"external"`).
- `status.address` = the `Name: "cluster-local"` entry (unchanged semantics).
- Because all three come from the same builder, they cannot disagree.

This is a **value correction, not a schema break**: on shared-host clusters
`status.url`'s value changes from the non-routable subdomain to the correct
`<sharedHostPrefix>.<domain>/<ns>/<isvc>/`. Nothing that works today breaks,
because the old value never routed. Announce it anyway (see Drawbacks).

### Shared derivation

The status writer calls the extracted endpoint builder (Part 1) instead of
`createRawURL`/`GenerateDomainName`. For each `(gateway, host, pathPrefix)` the
route builder computes, it emits one `Addressable` (`Name` = the gateway's
class, `URL` = `<scheme>://<host><pathPrefix>`). Because both paths call the same
function, adding a gateway to a route automatically adds it to status; changing
`perISVCSubdomain` or `sharedHostPrefix` automatically changes both.

### Correctness matrix the fix must satisfy

The derivation must produce these (verified against live routes):

| config | endpoint URL(s) |
|---|---|
| `perISVCSubdomain:true`, primary only | `https://<isvc>.<ns>.<domain>/` |
| `perISVCSubdomain:true`, primary+additional | `…int…/` and `…ext…/` |
| `perISVCSubdomain:false`, `sharedHostPrefix:llm` | `https://llm.<domain>/<ns>/<isvc>/` |
| per-namespace override | primary/additional resolved from `namespaceIngressGateways[<ns>]` |

### Conversion / migration

All new fields are `+optional` and additive; no stored-object migration. The
*value* of `status.url` changes on shared-host clusters (from a wrong subdomain
to the correct shared-host+path URL) — that is the bug fix, and is called out as
a behavior change for any consumer that (incorrectly) parsed the old value.

## Test Plan

- **Unit** — table test the endpoint builder across the full config matrix above
  (subdomain / shared-host, single / int+ext, per-namespace override,
  `http`/`https`), using the three real cluster configs as fixtures. Assert the
  derived URL equals `<scheme>://<host><path>` for each gateway.
- **Drift guard** — a unit test that asserts, for a set of isvcs, the status
  endpoints' `(host, path)` equal the `HTTPRoute` the builder emits for the same
  input. This is the regression test that keeps the two paths merged.
- **Integration (envtest)** — the ingress controller suite
  (`tests/integration/singlecluster/controller/inferenceservice`) asserting
  `status.url` and `status.addresses` (top-level and per-component) match the
  created `HTTPRoute` hostnames + path matches, for both `perISVCSubdomain`
  values and a per-namespace override.
- **Scheme** — assert per-component / virtual / external URLs honor
  `urlScheme: https`.

## Graduation Criteria

- **Alpha:** endpoint builder + `status.addresses` (top-level and per-component)
  added; `IngressGatewaySpec.Class` added; `status.url` corrected in both routing
  modes; scheme hardcodes removed; unit + drift-guard tests green. Correctness
  fix ships on by default (it replaces wrong values).
- **Beta:** integration coverage across the config matrix; clusters A, B and C
  spot-checked live (`status.url` reachable; external endpoint present in
  `status.addresses` on C).
- **Stable:** OEP-0018 `PlacementStatus.Endpoint` consumes the corrected
  `status.url`; operators have set `class` on their gateways; docs updated.

## Drawbacks

- **Status schema growth.** A new list field on an already-large status. Mitigated
  by keeping it `+optional` and small (one entry per gateway; typically 1–2).
- **Behavior change on shared-host clusters.** Any consumer that parsed the
  (wrong) subdomain `status.url` on shared-host clusters will see it change. This is
  unavoidable — the old value never worked — but must be announced.
- **Class is declared, not verified.** The `class` on an endpoint is whatever the
  operator wrote in gateway config; OME cannot verify an `"external"`-labeled
  gateway is actually internet-exposed. The tag reports declared intent, not a
  reachability guarantee.
- **Consumers must migrate to read `status.addresses`** to get non-primary
  (e.g. external) endpoints; `status.url` alone still surfaces only the primary.

## Alternatives

- **Fix `status.url` in place, no endpoints list.** Solves Failure 1
  (non-routable) but not Failure 2 (external endpoint undiscoverable) — a single
  field structurally can't hold a multi-gateway route's URLs. Rejected: leaves
  half the bug.
- **Expose extra URLs via annotations.** Untyped, undiscoverable, and not part of
  the status contract; consumers would parse strings. Rejected.
- **Compute URLs client-side from the ingress ConfigMap.** Leaks controller
  config to every consumer and re-implements the builder (the exact drift we are
  removing) outside the controller. Rejected.

## Resolved decisions

These were open in the first draft and are now settled (kept here for the record;
rationale in [Proposal](#proposal) / [Design Details](#design-details)):

1. **Canonical `status.url`** → the **primary gateway** endpoint (not a class
   name; not conditional external promotion). Meaning unchanged, value corrected.
2. **Field name + type** → `status.addresses []duckv1.Addressable` (mirrors
   Knative `Route.status.addresses`; `Name` = class). Reuse over a custom type;
   gateway provenance intentionally dropped.
3. **Component-level** → **in scope**: `ComponentStatusSpec.addresses` added with
   the same shape, for PD/disaggregated direct-to-component addressing.
4. **Protocol split (`RestURL`, gRPC vs REST)** → **out of scope**, deferred to a
   later OEP; `RestURL` left untouched.
5. **Class taxonomy** → free-form values (open), with a documented conventional
   set (`internal`/`external`/`cluster-local`) tooling recognizes, plus a light
   RFC-1123-label *format* constraint. No value enum; no scope inference.
6. **Port** → omit `:port` from gateway URLs (implied by scheme); standardize
   both ingress strategies to match.

## Open Questions

- **Class format enforcement point:** validate the RFC-1123-label form on
  `IngressGatewaySpec.Class` at config-load time (reject bad config) vs. sanitize
  when copying into status. Leaning config-load rejection so bad config fails
  loudly.
- **Multiple gateways of the same class on one isvc:** not a real case today
  (would make `addresses` `Name` non-unique within an isvc). Do we forbid it in
  config validation, or just document that `Name` is not guaranteed unique?
