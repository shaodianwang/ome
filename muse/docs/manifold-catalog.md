# Manifold model catalog for MUSE

## 1. Summary

Add a **Manifold model catalog** to MUSE: a read-only browse of Meta's Manifold
blob store (the source of truth for model weights), surfaced in the Models tab
alongside the OME `BaseModel`/`ClusterBaseModel` registry. MUSE lists servable
checkpoints under configured Manifold subtrees, parses their metadata, and
cross-references each against the OME registry so operators can see what is
*available* to serve — not just what has already been onboarded. This is the
on-ramp to a future deploy flow. Everything is config-gated; with no Manifold
config, MUSE runs exactly as it does today.

## 2. Motivation

The Models tab today lists only OME `BaseModel`/`ClusterBaseModel` CRDs — models
someone already **onboarded** into OME. That is a small, curated subset. At Meta
the source of truth for model weights is **Manifold**: OME already consumes
`manifold://fair_llms_prod/tree/vllm_test/<run>/checkpoint_NNNNNN/<variant>` URIs
as `ClusterBaseModel.spec.storageUri` and as cluster_cache `CC_MODEL_PATH`.

Operators asking "what can I serve?" get the wrong answer from the registry: it
shows the tip of the iceberg. The interesting question is which **checkpoints
exist in Manifold**, which are servable, and which are *not yet onboarded*. MUSE
already fans out across clusters and materializes cross-resource views the
frontend can't compute itself (capacity). The Manifold catalog is the same
shape of work: aggregate an external listing, project a lean view, cross-
reference the informer caches, serve from cache.

The flow we are enabling: **browse Manifold → (future) onboard as a
BaseModel → (future) deploy an InferenceService.** This doc covers the browse
step only.

## 3. Goals / Non-goals

### Goals

- Read-only **browse** of one or more configured Manifold subtrees (prefixes)
  under a configured bucket, level at a time.
- **Servable-checkpoint detection**: distinguish HF / Meta-consolidated /
  diffusion leaves from training artifacts, from path hints plus (optionally) a
  peek at the leaf's `config.json`.
- **Metadata parsing**: decode path tokens (tier, size, context, quant, KV
  cache, target HW) and format, reusing existing heuristics.
- **Cross-reference** each entry against the `BaseModel`/`ClusterBaseModel`
  informer caches (match by manifold path in `storageUri`) and tag it
  `onboardedAs` when a registry object already points at it.
- Surface the catalog in the **Models tab** (source toggle or merged view).
- **Config-gated and fail-closed**: absent config ⇒ the Manifold source is OFF;
  the OME registry still works.

### Non-goals (this doc)

- The **write / deploy path** (onboard-as-BaseModel, create InferenceService).
  That is a separate design; it will link to the GitOps-PR write plane, not
  mutate the cluster directly from MUSE. This doc is the read half only.
- **Mutation of Manifold** — no writes, no deletes, no weight transfer.
- **Arbitrary bucket browsing** beyond the configured prefixes. The catalog is
  scoped to what config (and the ACL) permits; MUSE is not a general Manifold
  file browser.
- Transiting **weight bytes** through MUSE. Only listing + small metadata files
  (e.g. `config.json`) are read. Weights never pass through the read plane.

## 4. Architecture

### 4.1 Where it lives

A new module in `muse-server`: `crates/muse-server/src/manifold.rs`. This is
**another read source for the existing read-plane role**, not a separate binary.
It sits beside `capacity.rs` and `feed.rs` and is wired into `AppState` and the
router in `server.rs`, exactly as capacity is.

Rationale (see MEMORY: "Separate binaries over mode flags"): a separate binary
is warranted when you are splitting a *process topology* (distinct
deploy/scale/RBAC lifecycles), as OEP-0019 does for `cmd/isvc` / `cmd/models` /
`cmd/webhooks`. The Manifold catalog is not a process split — it is one more
data source feeding the same axum server, the same DTO contract, the same Models
tab. It belongs inside muse-server.

### 4.2 Access path: S3-compatible proxy via `aws-sdk-s3`

muse-server is a **standalone Cargo workspace**, not fbcode (precedent:
`pkg/xet`). That rules out the native Manifold Rust client
(`//manifold/clients/rust:manifold_client`, Buck-only: cxx/folly) and Thrift
`BlobStore` codegen (Buck-only). It also rules out anything that needs Buck at
build time.

**Chosen access path: Manifold's S3-compatible proxy, spoken with
`aws-sdk-s3`.** Plain S3 over HTTP, zero fbcode deps, a first-class crate. This
is the same S3 gateway the OME model-download Job already uses
(`s3://fair_llms_prod/...` via `cloud_oss` creds — see
`hack/model-download/download-job.yaml`).

Listing is `ListObjectsV2` with **delimiter `/`**, one level at a time:

- `CommonPrefixes` in the response = "subdirectories" at this level.
- `Contents` = objects (files) at this level.

We descend deliberately (prefix by prefix), never a recursive full-bucket
listing. This maps cleanly onto the layout in §7 and bounds the blast radius of
a single request.

Alternatives considered, not chosen (kept here so the decision is legible):

- **Manifold HTTP API** `https://manifold.facebook.net/v0/` via `reqwest` —
  works only on the Meta network, and reimplements auth we already have via the
  S3 proxy. Fallback, not primary.
- **Thrift `BlobStore.enumerate` / `continueEnumeration`** — single-level with a
  continuation token, `tree` namespace only, but the Thrift codegen is Buck-
  only. Ruled out for a Cargo service.

### 4.3 Cached catalog (mirror the capacity-snapshot pattern)

Manifold list calls are not free: ~1K QPS per directory is the documented soft
ceiling, and pagination is **not atomic** (a directory can change between
pages). We must not hit Manifold in the request path, and we must not storm it.

Reuse the pattern already proven in `capacity.rs` / `server.rs`:

- A materialized snapshot behind `Arc<RwLock<Arc<CatalogSnapshot>>>` in
  `AppState`, serialized once to JSON, served O(1) per request (flat latency as
  the catalog grows — same property capacity has).
- A background refresh task, coalesced, on a **TTL** (`cache_ttl_secs`) rather
  than the event-driven signal capacity uses. Capacity is driven by informer
  events (Node/Pod changes); Manifold has no watch, so the catalog refreshes on
  a timer, with a single in-flight refresh at a time (coalesce concurrent
  triggers so a slow Manifold never stacks refreshes).
- First refresh runs at startup so the first request is served, not computed —
  as `AppState::new` warms capacity today.

Unlike capacity (pure CPU over in-memory caches), a catalog refresh does network
I/O to Manifold. It runs on a Tokio task, never blocking the request path, and
never inside the `RwLock`. On refresh failure we keep serving the last good
snapshot and log the error (fail-static, not fail-empty), so a transient
Manifold blip doesn't blank the Models tab.

### 4.4 Cross-reference with the OME registry

The catalog is joined against the existing `BaseModel` / `ClusterBaseModel`
informer caches (already in `ClusterState` — no new informer needed). For each
catalog entry we build its canonical `manifold://<bucket>/<key>` URI and look
for a registry object whose `spec.storageUri` resolves to the same Manifold
path. On a match, the entry is tagged `onboardedAs: { kind, name }`.

Matching notes (call out in review — these are the fiddly bits):

- Normalize `s3://` vs `manifold://` scheme and any trailing `/checkpoint`
  segment (`download.py` appends `/checkpoint`; a registry URI may or may not
  carry it — see the model-download README).
- Match should be **path-prefix aware**: a `storageUri` pointing at
  `.../checkpoint_0000200/checkpoint` should match the `checkpoint` variant leaf
  we cataloged under `.../checkpoint_0000200/`.
- The join is done at snapshot-build time against the current caches, and again
  cheaply at request time is *not* needed — but the registry changes more often
  than the catalog TTL, so we re-run the (cheap, in-memory) join on registry
  informer changes and patch the `onboardedAs` field without re-listing
  Manifold. (Open question §12: is per-request join over the cached catalog +
  live caches simpler? It's O(catalog × registry); likely fine at expected
  sizes.)

### 4.5 Frontend

The Models tab (`web/src/panels/Models.tsx`) gains a **source toggle**:
`Registry | Manifold` (or a single merged catalog with a "source" column —
open question §12). Manifold entries render as cards like registry entries, with:

- the parsed spec tags (tier / size / context / quant / KV / HW), reusing the
  `toModelCards` tag-rendering path in `web/src/map.ts`;
- a **servable** badge and a **format** tag (hf / consolidated / diffusion);
- an **onboarded** pill when `onboardedAs` is set (links to the registry entry),
  vs. an "available — not onboarded" state otherwise;
- (phase 2) an **Onboard / Deploy** action that hands off to the write plane.

When Manifold is disabled server-side, the toggle is hidden (or shows a clear
"Manifold source disabled" note) and the Registry view is unchanged. The
frontend learns this from the API (§8): a disabled source returns an explicit
`enabled: false` rather than an error.

### 4.6 Data flow (end to end)

```
Manifold S3 proxy ──ListObjectsV2(delimiter=/)──► manifold.rs client
        │                                              │
   (cloud_oss CAT token, mTLS via proxy)               ▼
                                        parse layout + (opt) peek config.json
                                                       │
                                                       ▼
                              CatalogEntry[]  ──join──  BaseModel/ClusterBaseModel
                                                       │        (informer caches)
                                                       ▼
                               CatalogSnapshot (Arc<RwLock<Arc<…>>>, JSON)
                                                       │
                         GET /v1/catalog  ────────────►  Models tab (Registry|Manifold)
```

## 5. Auth & deployment

Manifold auth is the real prerequisite, and it has two shapes. The design
supports both, config-selected; the **cloud path is primary** because the MUSE
target is MSL = GCP/CoreWeave.

### 5.1 Cloud path (GCP / CoreWeave — primary)

This is the exact pattern OME's model-download Job already uses
(`hack/model-download/download-job.yaml`). Three pieces:

1. **Floodgate x509 identity.** A pod annotation
   (`identity.cf.metafb.cloud/default: "true"`) makes Floodgate inject a service
   x509 cert at `/var/facebook/x509_identities/{tls.crt,tls.key}` (the
   download-job mounts an Aqueduct cert secret at `/var/facebook/x509_svc/`
   instead; either source works, config-selected).
2. **`cloud_oss` credential_process.** The AWS SDK is pointed at a
   `credential_process` that mints a short-lived CAT token:
   ```
   /opt/cf/cloud_oss/cloud_oss cat generate cat \
     --verifier-identity SERVICE_IDENTITY:manifold.blobstore \
     -o cli --timeout-secs 3600
   ```
   `cloud_oss` is present on the node at hostPath `/opt/cf/cloud_oss` (true on
   e.g. `metafb.cloud/node-type=gb300` nodes). This is the **same snippet**
   already in the OME download YAMLs.
3. **mTLS proxy DaemonSet.** The S3 request goes to a local proxy that re-adds
   mTLS to `manifold-s3.x2p.facebook.net:443`:
   - GKE / MSL: `http://mtls-proxy.mtls-proxy.svc.cluster.local:8081`
   - CKS / EKS: `s32m-proxy:10080`
   - devserver: `s3-manifold-proxy.fbinfra.net`

`aws-sdk-s3` is configured with a custom endpoint URL (the proxy), path-style
addressing, the configured region, and credentials from the `cloud_oss`
credential_process. muse-server does **not** implement token minting itself — it
shells out to `cloud_oss` exactly as the AWS CLI's `credential_process` does, or
uses the AWS config file mechanism (write `~/.aws/config` with the
`credential_process` line at startup, then let `aws-config` pick it up).

### 5.2 Native path (Twine / Meta-infra clusters)

On native Meta-infra clusters, ServiceRouter provides auto mTLS: no `cloud_oss`,
no proxy DaemonSet. The client talks to the Manifold endpoint directly with the
ambient service identity. Selected by `credential_mode: native`.

We recommend the cloud path as primary and treat native as a config option,
because the actual MUSE target cluster is GCP/CoreWeave. **Which path the target
cluster actually uses is an open question (§12) to confirm before enabling.**

### 5.3 Helm wiring

All of this is expressed in the `ome-resources` chart for the muse-server
Deployment, gated by `.Values.muse.manifold.enabled`:

- **Pod identity annotation** `identity.cf.metafb.cloud/default: "true"` (cloud
  path).
- **cloud_oss hostPath mount**: `volume { hostPath: /opt/cf/cloud_oss }` →
  `volumeMount /opt/cf/cloud_oss` (readOnly).
- **Cert volume**: the Aqueduct/service cert (secret or Floodgate path) mounted
  at the configured cert dir.
- **Env**: `THRIFT_TLS_SRV_CA_PATH`, `THRIFT_TLS_CL_CERT_PATH`,
  `THRIFT_TLS_CL_KEY_PATH` (mirrors the download-job).
- **Node placement**: schedule where `cloud_oss` + the mTLS proxy exist (the
  same node constraints the download-job uses).

None of these values are baked into the binary; they are Helm values that render
into the Deployment, and the binary reads only the resulting config block (§6).
When `enabled: false`, none of the above is rendered and MUSE has no Manifold
surface at all.

### 5.4 ACL ask (explicit precondition)

A **dedicated `SERVICE_IDENTITY` for MUSE** must be granted on the
`fair_llms_prod` Manifold ACL with **read + enumerate + `cloud`** actions.
Missing `cloud` ⇒ 403 on cloud (GCP/CoreWeave) clusters. Do **not** reuse
blocklisted shared identities (`faas`, `nest_*`). ACL admin:
`internalfb.com/amp/ACL/MANIFOLD%3Amanifold%2Ffair_llms_prod`.

This is a hard precondition for enabling the feature. MUSE ships
disabled-by-default and is flipped on once the ACL lands (§11).

## 6. Configuration

A `manifold` config block, following the existing clap-flags-plus-env pattern in
`config.rs`. **Hard constraint (OME repo blocker): no behavioral/user-facing
value is a code literal.** Bucket, endpoints, prefixes, credential mode, region,
TTL, pagination caps — all come from flags / env / Helm values. Absent config ⇒
the source is OFF (fail closed), and the OME registry path is untouched.

Note the deviation from the existing `config.rs` style: today MUSE's *topology
labels* carry in-code `default_value`s (e.g. the island/NVLink label lists).
Those are label-key *candidates* (discovery hints), not behavioral endpoints;
per the repo blocker, the Manifold **behavioral** values (bucket, endpoint,
credential mode, prefixes) get **no in-code defaults** — they must be supplied,
or the feature stays off. `enabled` defaults to `false`.

| Key | Type | Meaning | In-code default |
|---|---|---|---|
| `enabled` | bool | Master switch. False ⇒ no Manifold surface. | `false` |
| `endpoint` / `proxy_url` | string (URL) | S3 proxy endpoint (e.g. the mtls-proxy svc URL). | none |
| `bucket` | string | Manifold bucket, e.g. `fair_llms_prod`. | none |
| `prefixes` | list\<string\> | Subtrees to expose, e.g. `["tree/vllm_test"]`. | none |
| `credential_mode` | enum | `cloud_oss` \| `native` \| `static`. | none |
| `region` | string | AWS region for the SDK (e.g. `us-east-2`). | none |
| `cache_ttl_secs` | u64 | Catalog refresh interval. | none (must set) |
| `max_depth` | u32 | Max descent levels below a prefix. | none |
| `max_entries` | u32 | Cap on cataloged entries (guardrail vs. runaway listing). | none |
| `peek_config` | bool | Read each leaf's `config.json` for ground-truth (extra list/get calls) vs. path-only parsing. | `false` |

- `enabled: false` (default) — Manifold code paths are inert; `/v1/catalog`
  returns `{ enabled: false, items: [] }`; the Models tab hides the toggle.
- `credential_mode: static` exists only for local dev / testing against a
  mock S3 (e.g. MinIO); it reads static creds from env. Never used in prod.
- Every behavioral value is supplied via `charts/ome-resources` `values.yaml`
  (GitOps), not the binary. This is the same discipline as the `sharedHostPrefix`
  fix that replaced the old hardcoded `"llm."` host prefix.

Example Helm values (illustrative, GCP/CoreWeave):

```yaml
muse:
  manifold:
    enabled: true
    proxyUrl: http://mtls-proxy.mtls-proxy.svc.cluster.local:8081
    bucket: fair_llms_prod
    prefixes: ["tree/vllm_test"]
    credentialMode: cloud_oss
    region: us-east-2
    cacheTtlSecs: 900
    maxDepth: 4
    maxEntries: 5000
    peekConfig: false
```

## 7. Data model

### 7.1 `CatalogEntry`

The lean view DTO the API serves (shape mirrors the `views.rs` DTOs — clean,
stable, `serde` with `skip_serializing_if` for optionals):

```
CatalogEntry {
  bucket: string,             // e.g. "fair_llms_prod"
  prefix: string,             // configured subtree, e.g. "tree/vllm_test"
  run: string,                // "<run-name>-<8charMASThash>"
  checkpointStep: u64?,       // NNNNNN from checkpoint_NNNNNN
  variant: string,            // "checkpoint" | "nvfp4-moe-fp8kv-gb300" | ...
  uri: string,                // canonical "manifold://<bucket>/<key>"
  parsed: {                   // decoded from path tokens (best-effort)
    tier: string?,            // "tier5" (Avocado capability class, NOT param count)
    size: string?,            // "42b" | "17Bx16E"
    context: string?,         // "128k"
    quant: string?,           // "nvfp4" | "mxfp8" | "fp8" | "bf16"
    kvcache: string?,         // "fp8kv"
    hw: string?,              // "gb300" | "gb200" | "h100"
  },
  servable: bool,             // passed the servable-leaf test
  format: string?,            // "hf" | "consolidated" | "diffusion"
  onboardedAs: { kind, name }?  // set iff a BaseModel/ClusterBaseModel points here
}
```

Every value in `parsed` is a **hint** decoded from the path. Path is not ground
truth: `tier5` is an Avocado capability class, not a raw parameter count. When
`peek_config: true`, `config.json` overrides the path-derived fields (§12).

### 7.2 Layout & path-parsing conventions (`fair_llms_prod`)

Canonical servable layout:

```
tree/vllm_test/<run-name>-<8charMASThash>/checkpoint_NNNNNN/<variant>/
```

`<variant>` is either `checkpoint/` (bf16 servable) or a quantized sibling like
`nvfp4-moe-fp8kv-gb300/`, `nvfp4-mm-8k/`.

**Servable-leaf detection** (a leaf dir is servable if it matches one of):

- **HF format**: has `config.json` + `*.safetensors` + a tokenizer file.
- **Meta consolidated**: has `consolidated.*.pth` + `params.json`.
- Quantized: additionally has `hf_quant_config.json`.

**Not servable / skip:**

- Presence of `optimizer.*` / `dataloader.*` shards ⇒ **training artifact**.
- `model_index.json` ⇒ **diffusion pipeline** (`format: diffusion`; servable is
  a separate question — flag, don't silently include).

**Path-token decode** (port from Model Inspector, §10):

| Token pattern | Field | Notes |
|---|---|---|
| `tier5`, `tier4`, … | `tier` | Avocado capability class. **Not** param count. |
| `42b`, `17Bx16E` | `size` | Param size / MoE shape. |
| `128k`, `8k` | `context` | Context length. |
| `nvfp4`, `mxfp8`, `fp8`, `bf16` | `quant` | Weight quantization. |
| `fp8kv` | `kvcache` | FP8 KV cache. |
| `gb300`, `gb200`, `h100` | `hw` | Target hardware. |

**Other subtrees in the bucket** (context; not all servable, and only
configured prefixes are ever listed):

- `tree/<model-id>/<snapshot>` — MetaGen snapshots.
- `tree/hf/...` — HF-format trees.
- `tree/hyperloop_copy/...` — raw pre-conversion, **not servable**.

The catalog only ever lists the **configured** `prefixes`; the above is why the
prefix allow-list matters (don't expose `hyperloop_copy` as servable).

## 8. API

New read-plane endpoints on the existing axum router (`server.rs`), served from
the `CatalogSnapshot`:

### `GET /v1/catalog`

Query params:

- `prefix` — restrict to one configured prefix (must be in `prefixes`;
  otherwise 400/empty — never an arbitrary path).
- `depth` — cap the descent depth for this response (≤ `max_depth`).
- `onboarded` — optional filter (`true` / `false`) to show only
  onboarded / not-yet-onboarded entries.

Response:

```json
{
  "enabled": true,
  "count": 128,
  "items": [ CatalogEntry, ... ]
}
```

**Disabled behavior** (config `enabled: false`): return `200` with
`{ "enabled": false, "count": 0, "items": [] }` — an explicit, non-error signal
the frontend uses to hide the toggle. Never a 5xx, never a silent empty that
looks like "no models found".

### `GET /v1/catalog/entry?uri=<manifold://…>` (optional, phase 1.5)

Detail for one entry, including (when `peek_config`) the parsed `config.json`
summary. Same disabled behavior. Useful for the card-expand / pre-onboard view.

Notes:

- Endpoints are **aggregation-free** w.r.t. clusters — Manifold is global, not
  per-cluster — so unlike `/v1/{kind}` there is no `?cluster=` fan-out. Entries
  carry `onboardedAs` (which may reference a cluster-scoped or namespaced
  registry object), not a `cluster` tag.
- Served O(1) from the snapshot; no Manifold call in the request path.
- Client (`web/src/api.ts`) gains `api.catalog()` returning
  `{ enabled, count, items }`, and `CatalogEntry` mirrors §7.1.

## 9. Security & privacy

- **Read-only.** MUSE only ever calls `ListObjectsV2` (and, with `peek_config`,
  `GetObject` on small metadata files). No `PutObject`/`DeleteObject` code path
  exists. No weight bytes transit MUSE.
- **ACL-scoped.** Access is bounded by the dedicated `SERVICE_IDENTITY`'s grant
  on `fair_llms_prod` (read + enumerate + cloud), and further by the configured
  `prefixes` allow-list. `prefix` query params are validated against that list —
  no arbitrary-path traversal.
- **CAT-token TTL.** The `cloud_oss` credential_process mints short-lived tokens
  (`--timeout-secs`, e.g. 3600); the AWS SDK re-invokes it on expiry. No
  long-lived Manifold credential is stored.
- **No secrets in logs.** Cert paths, tokens, and credential_process output are
  never logged. Errors log the operation + bucket/prefix, not credentials.
- **Fail closed.** No config ⇒ no Manifold surface. Missing ACL ⇒ 403 handled as
  "source unavailable" (log + last-good snapshot), never a crash and never a
  fallback to some hardcoded bucket.
- **Least privilege identity.** A MUSE-specific identity (not a shared/
  blocklisted one) means the grant is auditable and revocable without affecting
  other services.

## 10. Reuse — MSL Model Inspector

Do **not** reinvent the servable-detection / avocado-tier / arch heuristics. The
internal Nest app **MSL Model Inspector** (`/nest/apps/msl_model_inspector/`,
author `tianchen`) already implements a Manifold viewer with exactly this
parsing:

- `lib/manifold.ts` — Manifold listing + traversal.
- `lib/avocado-tiers.ts` — the `tierN` → capability-class mapping.
- `lib/manifold-model-config.ts` — `config.json` parsing / model-config decode.
- `lib/model-families.ts` — architecture / family detection.

**Plan: port/adapt this logic into `manifold.rs`** (Rust) rather than invent new
heuristics. The path-token table in §7.2 and the servable-leaf rules are the
same rules Model Inspector encodes; porting keeps MUSE consistent with the tool
operators already trust, and avoids drift in the tier taxonomy. Where the logic
is display-only (tag formatting), it can stay in the frontend (`map.ts`); the
*classification* (servable, format, tier) belongs server-side so the API is the
source of truth.

## 11. Rollout / sequencing

**Phase 0 — infra precondition (blocking).**

- ACL grant: dedicated MUSE `SERVICE_IDENTITY` gets read + enumerate + cloud on
  `fair_llms_prod`.
- `cloud_oss` + mTLS proxy available on the MUSE node(s); cert wiring confirmed.
- Confirm cloud vs native auth on the **actual** target cluster (§12).

Until phase 0 lands, the feature ships **disabled-by-default** and is inert.

**Phase 1 — read-only browse (this doc).**

- `manifold.rs`: `aws-sdk-s3` client → proxy, level-at-a-time listing, cached
  snapshot (TTL + coalesced refresh), servable detection + path parsing, join
  with registry caches.
- `GET /v1/catalog` (+ optional entry detail); disabled ⇒ explicit empty.
- Models tab: `Registry | Manifold` toggle (or merged view), servable/format/
  onboarded badges.
- Enable in one cluster's Helm values once the ACL lands; verify against real
  `fair_llms_prod`.

**Phase 2 — deploy on-ramp (separate design).**

- From a not-yet-onboarded catalog entry: **Onboard** (create a
  `BaseModel`/`ClusterBaseModel`) and **Deploy** (create an `InferenceService`),
  handed off to the **GitOps-PR write plane** — MUSE opens a PR, it does not
  mutate clusters directly. Links to that design when it exists.

Phase 1 is safe to ship dark: with `enabled: false` (the default) nothing about
today's MUSE changes, and flipping it on is a Helm-values change once the ACL is
in place.

## 12. Open questions

1. **Auth path on the real target cluster.** We recommend and default to the
   cloud path (Floodgate + cloud_oss + mtls-proxy), because MSL is
   GCP/CoreWeave. But whether the *specific* MUSE deployment cluster uses cloud
   or native (ServiceRouter) mTLS is **unverified** — confirm before enabling,
   and confirm the mtls-proxy endpoint variant (mtls-proxy vs s32m-proxy).
2. **Which subtrees to expose.** `tree/vllm_test` is the obvious first prefix.
   Do we also want MetaGen snapshots (`tree/<model-id>/<snapshot>`) or HF trees
   (`tree/hf/...`)? Each adds listing cost and needs its own servable rules.
   `hyperloop_copy` is explicitly out (raw, not servable).
3. **Path-only vs peek-inside.** `peek_config: false` (path-only) is cheap but
   trusts path tokens; `peek_config: true` reads each leaf's `config.json` for
   ground truth at the cost of extra `GetObject` calls (× number of leaves).
   Which is the default, and do we peek lazily (only on entry-detail) vs. during
   the bulk refresh?
4. **Caching / rate-limit tuning.** What `cache_ttl_secs` balances freshness vs.
   the ~1K QPS/dir ceiling and non-atomic pagination? Do we need per-prefix
   staggering so a refresh doesn't list every prefix at once?
5. **Merged view vs. toggle.** One Models list with a "source: Registry |
   Manifold" column and a shared filter, or a hard toggle between two lists?
   Merged is nicer UX but complicates sorting/paging across two very different
   population sizes (registry: tens; Manifold: potentially thousands).
6. **Multi-bucket support.** Config allows one `bucket` today. Do we need
   multiple buckets (each with its own prefixes/ACL) in one MUSE, or is a single
   bucket sufficient for the foreseeable rollout?
7. **Cross-reference cost.** Is the on-registry-change re-join (§4.4) worth the
   complexity, or is a per-request O(catalog × registry) join over the cached
   catalog + live caches simpler and fast enough at expected sizes?

## Status

Draft — 2026-07-01, author MUSE (design), pending review
