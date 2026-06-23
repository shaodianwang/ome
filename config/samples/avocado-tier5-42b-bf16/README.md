# Avocado Tier5 42B 128k — lean multi-node OMENative (BF16)

A self-contained, **model-less (lean)** InferenceService for the Avocado Tier5
42B 128k **BF16** sglang server on **gb300** (16 GPU / 4 nodes, tp16/ep16).
Weights load from cluster cache; there is **no BaseModel CRD**.

| File | What it is |
|---|---|
| `servingruntime.yaml` | Namespaced `ServingRuntime` (self-contained: image, BF16 sglang flags, IB/NCCL env, probes). Engine = leader+worker gang; plus a CPU router. |
| `inferenceservice.yaml` | Lean `InferenceService` (OMENative, no `spec.model`) naming the runtime + shaping the gang (1 leader + 3 workers) + router. |

Checkpoint (cluster cache `CC_MODEL_PATH`):
`manifold://fair_llms_prod/tree/vllm_test/consolidation_sv4_1_tier5_42b_128k-gxrj1qwt/checkpoint_0000200/checkpoint/`
— full-precision BF16, 35 safetensors, **~1.65 TB** (config.json has no `quantization_config`).

## ⚠️ Known failure: CC cold-hydrate of ~1.65 TB

The blocker is **not** this config — it's that cluster cache cold-hydrating ~1.65 TB
flaps owner nodes under load, producing `hydrate exhausted owner candidates after 2
attempts` for a varying subset of shards (the gang then crashloops, which piles on
*more* load). Owners themselves are healthy (verified on the ring). Mitigations:

1. **Stop the retry storm** before debugging: `kubectl -n prod scale isvc avocado-tier5-42b-bf16 --replicas=0`.
2. **Pre-warm** the checkpoint into CC once (controlled `POST /hydrate`), then deploy → gang load is a cache hit.
3. **Raise `CACHE_HYDRATE_OWNER_CANDIDATE_ATTEMPTS`** on the CC daemonset (default 2) so chunks route past a flapping owner.
4. **PVC fallback** (bypass CC): add `spec.model` (a `ClusterBaseModel`) + `--fallback-model-path $(MODEL_PATH)` — see `config/samples/sglang-omenative-multinode/`.

## Prerequisites
- ≥4 arm64 gb300 GPU nodes (4×`nvidia.com/gpu` + 4×`rdma/ib` each) free in one `network.meta.com/domain` (MNNVL).
- cluster-cache reachable + holding (or able to hydrate) the checkpoint; IB macvlan networks (`cw-multus/ibs*`) present.
- OME control plane with the gang rendezvous env (`OME_INSTANCE_POD_RANK` / `_COUNT` / `OME_LEADER_ADDRESS`).
- *(optional)* a prod `priorityClassName` under `engine.leader`/`engine.worker`.

## Deploy
```sh
export KUBECONFIG=~/.kube/msl-cw-use2-2.conf
CTX="--context cw/msl-cw-use2-2"
kubectl $CTX -n prod apply -f servingruntime.yaml      # runtime first
kubectl $CTX -n prod apply -f inferenceservice.yaml
```

## Verify
```sh
kubectl $CTX -n prod get isvc avocado-tier5-42b-bf16 -o wide
kubectl $CTX -n prod get pods -l ome.io/inferenceservice=avocado-tier5-42b-bf16 -o wide
kubectl $CTX -n prod logs avocado-tier5-42b-bf16-engine-0-leader-0 -c ome-container -f
```

## Test
```sh
kubectl $CTX -n prod port-forward svc/avocado-tier5-42b-bf16-router 30080:30080 &
curl -s localhost:30080/v1/chat/completions -H 'content-type: application/json' \
  -d '{"model":"external","messages":[{"role":"user","content":"hello"}],"max_tokens":32}' | jq .
```
(Engine `--served-model-name external`; router advertises `avocado-tier5-42b-bf16`.)

## Teardown
```sh
kubectl $CTX -n prod delete -f inferenceservice.yaml -f servingruntime.yaml
```
