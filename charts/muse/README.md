# muse — MUSE console

MUSE is the OME management console. This chart deploys it as **one image**
(`muse-server`) that serves both the read-plane API (`/v1`) and the web UI (`/`),
and exposes it through an existing Envoy Gateway — no nginx, no separate frontend.

## What it deploys

- **Deployment** `muse-server` (API + UI) + **Service** (ClusterIP `:8080`, metrics `:9090`).
- **Headless Service** `muse-api` for direct, server-to-server API access through
  the MKS HTTP frontend.
- **ClusterRole/Binding**: read-only `get/list/watch` on the OME CRDs + `nodes`/`pods`;
  metrics add `get` on `services` and `services/proxy` for discovery and queries.
- **HTTPRoute** on the shared Envoy Gateway, serving the console under `/muse` — the
  general route strips the prefix (`URLRewrite`), so muse-server serves at root.
  Exact activity and metrics stream rules disable the Gateway request timeout
  without removing that protection from REST or chat; set
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

When fleet metrics are enabled, each remote kubeconfig also needs `get` permission
for core `services` and `services/proxy`. MUSE validates that the configured
`config.metrics.promService` and port exist independently in every workload
cluster before running the query catalog. A missing Service or port marks that
cluster's metrics source unconfigured; a failed discovery read marks it unavailable.
Both include a machine-readable code and target details, and skip the query catalog.
Other resource and capacity views for that cluster remain available.

Metrics requests are bounded per Muse replica by
`config.metrics.queryConcurrency`, and every Service discovery or PromQL call
uses `config.metrics.requestTimeoutSecs` as its deadline. Tune the concurrency
with the replica count and Prometheus capacity in mind; timed-out PromQL calls
remain visible as individual query errors while the rest of the snapshot publishes.
Both settings are optional outside the chart: without them, requests run serially
and wait without a Muse-imposed deadline.

Deployment Monitoring range choices come from `config.metrics.timeRanges`. Muse
still fetches each query's configured maximum `window` once per refresh and sends
that shared cached snapshot to every console; changing the selector only filters
the returned timestamps in the browser. Each option must be at least the largest
configured query `step` and at most the smallest configured query `window`.
Consequently, the bundled 5-minute step yields at most four samples in a 15-minute
view. Lower the query-set `step` when an environment needs denser charts, accounting
for the corresponding Prometheus evaluation and snapshot-size increase.

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

The bundled query set aggregates historical dashboard signals by
`namespace,inferenceservice`, so deployments with the same name stay separate.
Replica diagnostics are engine-only instant queries grouped by
`namespace,inferenceservice,pod,component`; router signals remain deployment-level
because a router request cannot be attributed to one engine pod. Keeping replica
queries instant avoids returning a per-pod time series for the entire fleet on
every refresh.

The bundled OME Prometheus scrape config supplies all four labels. A custom
Prometheus or PodMonitor pipeline must preserve them for exact deployment and
replica selection.
