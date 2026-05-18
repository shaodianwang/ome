# avo-tier5-pd deployment bundle

End-to-end test bundle: ClusterBaseModel + ClusterServingRuntime + InferenceService
for the AVO tier5 42B-128k checkpoint, served in prefill/decode disaggregated
mode (1 router + 1 prefill + 1 decode pod, 4 GPUs each for engine/decoder).

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
```
