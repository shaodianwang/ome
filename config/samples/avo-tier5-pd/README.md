# avo-tier5-pd deployment bundle

End-to-end test bundle for the AVO tier5 42B-128k checkpoint, served in
prefill/decode disaggregated mode (1 router + 1 prefill + 1 decode pod,
4 GPUs each for engine/decoder).

Bundle contents:

| File | Purpose |
|---|---|
| `cluster-base-model.yaml` | Sharded CBM (manifold:// URI, fetched via cluster_cache) |
| `cluster-base-model-pvc.yaml` | PVC-backed CBM (mounts a pre-populated PVC) — alternate storage path for the same checkpoint |
| `cluster-serving-runtime.yaml` | Runtime supporting both distributions (two entries in `supportedModelFormats`) |
| `inference-service.yaml` | ISVC referencing the Sharded CBM by default |

Apply in order:

```bash
export KUBECONFIG=~/.kube/msl-cw-use2-2.conf
kubectl apply -f cluster-base-model.yaml
# wait for CBM to reach state=Ready (~minutes)
kubectl wait --for=jsonpath='{.status.state}'=Ready clusterbasemodel/avo-tier5-pd-test --timeout=10m

kubectl apply -f cluster-serving-runtime.yaml
kubectl apply -f inference-service.yaml
```

Watch the rollout:

```bash
kubectl get pods -n playground -l ome.io/inferenceservice=avo-tier5-pd-test -w
kubectl get isvc -n playground avo-tier5-pd-test -o yaml
```

## Switching the ISVC to the PVC base model (manual swap)

When cluster_cache is misbehaving (stale-mtime 412s, daemon fleet
outage, …) it's useful to fail the ISVC over to the PVC-backed copy of
the same checkpoint. Until model overlays (OEP-0010) ship, this is a
spec edit:

```bash
# 1. Apply the PVC CBM (one-time; the PVC itself must already exist in
#    the ISVC's namespace).
kubectl apply -f cluster-base-model-pvc.yaml
kubectl wait --for=jsonpath='{.status.state}'=Ready \
  clusterbasemodel/avo-tier5-pd-test-pvc --timeout=10m

# 2. Point the ISVC at the PVC CBM. Triggers a normal Deployment
#    rollout (~minute per pod) — in-flight requests on the old pods
#    are dropped.
kubectl patch isvc -n playground avo-tier5-pd-test --type=merge \
  -p '{"spec":{"model":{"name":"avo-tier5-pd-test-pvc"}}}'

# 3. Watch the new pods come up.
kubectl get pods -n playground -l ome.io/inferenceservice=avo-tier5-pd-test -w
```

To switch back to the Sharded copy, patch the field again with
`avo-tier5-pd-test`.

Two things to know about this manual swap:

- **It IS a spec mutation.** `kubectl edit isvc` and `argocd diff` will
  show the new value as the desired state until you change it back.
  For GitOps-managed ISVCs this means committing the swap (or using a
  hand-managed override file).
- **The runtime must support both distributions.** `cluster-serving-runtime.yaml`
  in this bundle declares two entries in `supportedModelFormats` — one
  with `modelCacheProviders: [cluster_cache]` for Sharded, one without
  for PVC. A runtime that only supports Sharded will reject the patch
  at the validating webhook with `runtime does not support model
  cache provider 'cluster_cache'` (or the inverse).

Once OEP-0010 model overlays land, the same swap will be expressible
as `spec.model.overlays: [name: avo-tier5-pd-test-pvc]` with no spec
mutation required at failover time — the controller swaps which model
drives `MODEL_PATH` based on an annotation, and the PVC is pre-mounted
so the new pods boot fast (no model re-download).

## What's parameterized vs hardcoded

The live runtime this is derived from baked in cluster-specific values
in container env. Here we drop everything the OME ISVC controller
already injects automatically for Sharded models
(`pkg/controller/v1beta1/inferenceservice/components/base.go:340`):

| Env var | Source |
|---|---|
| `MODEL_PATH` | `BaseModel.spec.storage.storageUri` verbatim |
| `MODEL_CACHE_PROVIDER` | `inferenceservice` ConfigMap → `modelCache.provider` |
| `MODEL_CACHE_ENDPOINT` | `modelCache.endpoint` |
| `CLUSTER_CACHE_HEADLESS_SERVICE` | host:port of `endpoint` (cluster_cache only) |
| `MODEL_CACHE_OPTIONS` | `modelCache.options` as JSON |

The runner's `--model-path $(MODEL_PATH)` (and `--speculative-draft-model
$(MODEL_PATH)/draft_hf`) expand against the injected env var.
`NAMESPACE` / `INFERENCESERVICE_NAME` on the router come from the pod's
own metadata via `fieldRef`.

What remains cluster-specific and would need editing for other clusters:

- Container images (private ECR account `168653207203`).
- Multus `k8s.v1.cni.cncf.io/networks` annotation (CoreWeave `cw-multus`).
- `kubernetes.io/arch: arm64` node selector (gb300 nodes).
- `rdma/ib: 4` extended resource (CoreWeave SR-IOV plugin).

## Teardown

```bash
kubectl delete -f inference-service.yaml
kubectl delete -f cluster-serving-runtime.yaml
kubectl delete -f cluster-base-model.yaml
kubectl delete -f cluster-base-model-pvc.yaml --ignore-not-found
```
