# muse — MUSE console

MUSE is the OME management console. This chart deploys it as **one image**
(`muse-server`) that serves both the read-plane API (`/v1`) and the web UI (`/`),
and exposes it through an existing Envoy Gateway — no nginx, no separate frontend.

## What it deploys

- **Deployment** `muse-server` (API + UI) + **Service** (ClusterIP `:8080`, metrics `:9090`).
- **Headless Service** `muse-api` for direct, server-to-server API access through
  the MKS HTTP frontend.
- **ClusterRole/Binding**: read-only `get/list/watch` on the OME CRDs + `nodes`/`pods`.
- **HTTPRoute** on the shared Envoy Gateway, serving the console under `/muse` — the
  general route strips the prefix (`URLRewrite`), so muse-server serves at root.
  The exact activity stream rule disables the Gateway request timeout without
  removing that protection from REST, metrics, or chat; set
  `route.streams.requestTimeout: null` to omit those exact rules.
- Optional **ReferenceGrant** and **CiliumNetworkPolicy**.

## Deploy (GitOps / ArgoCD)

ArgoCD already runs on the management cluster, so point an Application at this chart:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: muse
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/mslsrc/ome
    path: charts/muse
    targetRevision: main
    helm:
      values: |
        image:
          repository: <account>.dkr.ecr.us-east-2.amazonaws.com/msl_infra/muse-server
          tag: latest
  destination:
    server: https://kubernetes.default.svc
    namespace: muse
  syncPolicy:
    automated: {}
    syncOptions: ["CreateNamespace=true"]
```

Or directly: `helm install muse charts/muse -n muse --create-namespace`.

## Reaching it

The gateway is reached via silfra (corp → the gateway IPv6 + port). Use the same
port as ArgoCD's HTTPS listener (8443 on `msl-mks-cp-lco-0`):

```
https://sv6-<gateway-ipv6-dashed>---8443.silfra.facebook.net/muse
```

Find the gateway IPv6:

```
kubectl get gateway ingress-eg-https -n envoy-gateway-system \
  -o jsonpath='{.status.addresses[0].value}'
```

e.g. `2401:db00:72ff:c8b6::` →
`https://sv6-2401-db00-72ff-c8b6-0000-0000-0000-0000---8443.silfra.facebook.net/muse`

### Server-to-server API access

The chart also creates a single-port headless Service annotated for MKS SMC
registration. Internal clients can reach the API without traversing the
interactive console gateway:

```bash
curl \
  -H "service_endpoint: muse-api.muse.msl-mks-cp-lco-0.mks" \
  https://mks-http-fe.fbinfra.net/v1/clusters
```

Set `service.api.smcTierName` to the target cluster's tier name when deploying in
another cluster, or set `service.api.enabled=false` when direct API access is not
needed.

## Watching workload clusters

By default muse-server watches the cluster it runs in (via its ServiceAccount). To
aggregate remote clusters, create a Secret per cluster holding its kubeconfig
(key `kubeconfig`) and list them:

```yaml
clusters:
  - name: cw-use2-1
    kubeconfigSecret: muse-kubeconfig-cw-use2-1
```

Monitoring uses the configured PromQL Query Gateway with Grafana Cloud routing,
independently of the workload-cluster kubeconfigs. Map each MUSE cluster name to
its metrics identity:

```yaml
config:
  metrics:
    gatewayUrl: https://observability-query-gateway.fbinfra.net/api/v1/
    queryRouting: 2
    gatewayCaCert: /var/facebook/x509_identities/combined.pem
    clusterIds:
      msl-cw-use2-1: 652086758864/us-east-2/msl-cw-use2-1
      msl-cw-use2-2: 652086758864/us-east-2/msl-cw-use2-2
      msl-cw-use2-dev-2: 652086758864/us-east-2/msl-cw-use2-dev-2
      msl-gcp-usc1-1-tpu: msl-gcp-1-uwob1/us-central1/msl-gcp-usc1-1-tpu
      msl-gcp-usc1-2-tpu: msl-gcp-1-uwob1/us-central1/msl-gcp-usc1-2-tpu
```

A missing mapping marks only that cluster's metrics source unconfigured; its
inventory, capacity, runtime, and activity views remain available. Workload
kubeconfigs do not need `services` or `services/proxy` access for metrics.

The bundled catalog queries Grafana Cloud native Prometheus metrics only for the
active exact deployment scope. A future catalog can enable fleet scope only when
its measurements expose safe deployment attribution. Identical requests are
coalesced by the per-replica cache. `queryConcurrency`, `requestTimeoutSecs`,
`cacheTtlSecs`, `cacheMaxEntries`, `maxResponseBytes`, and `maxSeries` bound load
and memory; `maxScopePods` bounds one deployment's selector.
Tune them with both the MUSE replica count and the shared Query Gateway budget in
mind; 429 and 503 responses remain visible as query errors and are not retried
immediately.

Deployment Monitoring range choices come from `config.metrics.timeRanges`. The
selected option's `seconds` and `stepSeconds` are sent to MUSE. The window can
shorten but never expand a query's catalog `window`; the configured step must be
no smaller than any catalog query's minimum `step` and no larger than its window.
The bundled choices use `60s` for 15m and 1h, and `300s` for 6h.

## Inference test (chat)

Off by default (`config.chat.enabled: false`) — it proxies one request to a
model's own endpoint. Enable per environment; when off, the console shows a
copyable `curl` instead.

The exact `InferenceService.status.url` origin is permitted automatically. If the
console must target a different external gateway, configure it explicitly; MUSE
does not infer an allowed parent domain from the status hostname:

```yaml
config:
  chat:
    enabled: true
    timeoutSecs: 60
    maxResponseBytes: 1048576
    connectTimeoutSecs: 5
    clientGraceSecs: 5
    allowedHosts:
      - gateway.example.com:8443
    allowedHostSuffixes:
      - serving.example.com
```

Enabling chat requires both `timeoutSecs` and `maxResponseBytes`. If either is
absent, muse-server leaves the proxy disabled instead of inventing a runtime
default. The response cap is enforced on the streamed body, including responses
without a trustworthy `Content-Length` header.

Chat targets must use HTTPS unless `allowHttp: true` is explicitly set. Redirects
are refused rather than followed. The packaged console and API are same-origin;
the server does not emit permissive CORS headers. For mTLS, client certificate,
client key, and CA certificate must all be configured; any partial tuple leaves
the proxy disabled.

## Dashboard health policy

Health colors are deployment policy, not binary heuristics. Configure warning and
critical pairs under `config.dashboardPolicy`; set the whole policy or an
individual category to `null` to render that metric neutrally. Percentage metrics
use 0..100 and cache metrics use 0..1 fractions. The chart's values file carries
the default policy and the server exposes the effective typed policy via
`/v1/config`.

## Serving metric identity

MUSE resolves the exact deployment's pods from its informer cache, rejects
ambiguous pod names, and inserts an anchored pod allowlist into every Query
Gateway selector. Returned pod series are checked against that allowlist and
enriched with namespace, InferenceService, and component labels from the same
cache.

Every catalog selector carries the configured Grafana Cloud `cluster` identity
and receives the exact pod matcher. Replica diagnostics group only by `pod`; the
bundled catalog is
deployment-only, so `/v1/config` reports `fleetMetrics: false` and the console
hides unsupported fleet performance panels instead of issuing broad queries.
