# Per-hop sources

Scuba queries use `meta scuba.dataset query --dataset=<d> --sql='...' -o json`,
which returns string values. Filter a window with `time >= <epoch>`.

## API proxy — `llm_service_responses_api`

- Filter with `model = '<isvc>'`. Some tiers log the tier name in `model`
  instead of the ISVC name, so check `SELECT model, COUNT(*) ... GROUP BY model`.
- A failed row has a non-null, non-empty `exception_type`, and its
  `response_status` is empty.
- Group by `exception_type`, `model_manifold_path` (checkpoint),
  `decode_host` (`http://<engine pod IP>:<port>`), `client_id`,
  `client_tw_job`, `router_policy` and `reasoning_effort`. Useful size
  columns are `num_output_tokens` and `num_reasoning_tokens`.
- Map a pod IP to its pod with
  `kubectl --kubeconfig ~/.kube/<c>.conf get pods -A --field-selector status.podIP=<ip>`.
  A full `get pods -A` takes minutes on large clusters.

## Envoy sidecar — `envoy_access_logs`

- Filter with `source_tw_job LIKE '%<proxy tier>%'`.
- Useful columns: `response_code`, `response_flags`, `response_flags_long`,
  `request_authority` (names the member cluster's gateway), `dst_ip` and
  `node_name` (the proxy task).
- The log is sampled, so use it to see whether something happens and on
  which cluster, never for rates.

## ODS (the alert's own series)

- `meta ods.metric load-url --url '<chart URL from the alert>'` replays the
  alert query exactly.
- To find keys, run
  `meta ods.metric suggest-keys --query <word> --entity smc.<proxy tier>`.
  Try `fatal`, `parse`, `upstream`, `ttfc`. `ods.metric keys --regex`
  returns nothing on tier entities.
- The SR fatal alert is `(SR.num_fatals + SR.stream.fatal) / SR.num_requests`.
  Proxy parse rejects are under `llm_service.llm.msl_chat.*`, and upstream
  latency under `llm_service.llm.proxy_upstream_ttfc_ms.*`.
- The sidecar does not export ODS keys on the tier entity. Use Scuba.

## Multi-cluster traffic map (hub)

- Run `kubectl --kubeconfig ~/.kube/<hub>.conf get trafficmap <isvc> -n <ns> -o yaml`.
- Per member cluster, `spec.entries[]` holds `weight`, `healthy` and
  `capacity.{allocated,ready}`. `status.published` and the publisher's
  claimed targets show what the gateways actually received.
- A missing object means the service is not hub-placed.

## Gateway and router (`ome-prometheus`, 1h retention)

Query through the API server, with no port-forward:

```sh
kubectl --kubeconfig ~/.kube/<c>.conf get --raw \
  '/api/v1/namespaces/ome/services/ome-prometheus:9090/proxy/api/v1/query?query=<urlencoded>'
```

- Gateway: `envoy_cluster_upstream_rq_xx{envoy_cluster_name="httproute/<ns>/<isvc>/rule/0"}`,
  split by `envoy_response_code_class`.
- Router: `router_requests_total{namespace,inferenceservice}`, split by
  `status`, `path`, `stream` and `revision_hash`. Also check
  `router_routing_errors_total` and `router_pd_pair_trips_total`.
- For more than 1h of history, use the serving dashboard, whose router and
  engine panels come from long-retention storage. Gateway pod logs reach back
  only minutes.

## Engine

- Run `kubectl get pods -n <ns> -l ome.io/inferenceservice=<isvc> -o wide`
  and check readiness, restarts and `lastState.terminated`.
- For PD, check prefill and decode separately; the component label is on
  the pod.
- Read the engine logs of the hot pod: `kubectl logs <pod> -c ome-container --since=30m`.
- `sglang:*` metrics are on the pod's metrics port. Watch queue depth,
  running requests and aborts.

## What changed near onset

- Run `kubectl ome rollout history <isvc> -n <ns>` on each member.
- Check merged GitOps changes that touch the ISVC, its runtime or its image
  tag within ±30 min of onset.
- A new checkpoint or `model_manifold_path` appearing in the proxy log at
  onset also counts as a change.
