# sglang OMENative — multi-node, engine-only (no PD)

A worked example of a **multi-node** sglang deployment on OME: one model *instance*
spans a gang of **1 leader + 3 worker** pods (4 nodes), gang-scheduled together, fronted
by a **router**. There is **no decoder** (this is not prefill-decode). The ~2.18 TiB bf16
checkpoint loads from **cluster cache** at runtime, with a PVC-backed **ClusterBaseModel**
as the fallback.

This is the first in-tree multi-node sglang sample. Bringing it up end-to-end on a real
cluster surfaced several OME multi-node gaps — all now fixed or in review (see **OME gaps
found** below). The sample already bakes in the workarounds, so it deploys as-is; the
table tells you which PR removes each workaround. Validate with `--dry-run=server` first.

## Files

| File | What it is |
|---|---|
| `base-clusterservingruntime.yaml` | Platform layer — GPU/IB/RDMA, dshm/tmp volumes, probes, NCCL/CUDA infra env. No image, no flags. Owned by the platform team. |
| `tenant-servingruntime.yaml` | Workload layer — inherits the base, adds the sglang image, the multi-node launch command, and the model path. **This is the file you edit.** |
| `clusterbasemodel-pvc.yaml` | PVC-backed `ClusterBaseModel` — the cluster-cache fallback. |
| `inferenceservice.yaml` | The deployment — OMENative, engine gang (`leader` + `worker.size: 3`) + router, no decoder. |

## Before you deploy — verify these

1. **Images** — `tenant-servingruntime.yaml` uses placeholder gb300 engine + router images.
   Pin your own sglang / sgl-router builds.
2. **IB macvlan list** — confirm `k8s.v1.cni.cncf.io/networks` matches your fabric.
3. **`--dist-init-addr` port** — `:5000` is a chosen rendezvous port (also the `dist`
   containerPort). Confirm sglang's expected form for your build.
4. **CC tensor-hash verify** — `CLUSTER_CACHE_VERIFY_TENSOR_HASH_AGAINST` is commented out;
   enable only with a precomputed `@tp16ep16` artifact for this checkpoint.
5. **Capacity is the real blocker.** The gang needs **4 schedulable nodes with 4 free GPU
   each, in one `network.meta.com/domain`**. On a busy cluster this — not config — is what
   keeps it `Pending`. Free/uncordon nodes, or raise `priorityClassName` to preempt.
6. **Startup-probe budget vs load time.** A cold ~2.18 TiB CC load is slow; the engine
   startup-probe budget (`failureThreshold × periodSeconds`, 30 min here) must exceed the
   load time, or the container restarts mid-load. Raise it if your load is slower.

## Deploy

```bash
# Validate first (server-side schema + OME webhooks; nothing created):
kubectl apply --dry-run=server -f base-clusterservingruntime.yaml
kubectl apply --dry-run=server -f tenant-servingruntime.yaml
kubectl apply --dry-run=server -f clusterbasemodel-pvc.yaml
kubectl apply --dry-run=server -f inferenceservice.yaml

# Apply (base CSR + CBM are cluster-scoped; tenant SR + ISVC are namespaced):
kubectl apply -f base-clusterservingruntime.yaml
kubectl apply -f tenant-servingruntime.yaml
kubectl apply -f clusterbasemodel-pvc.yaml
kubectl apply -f inferenceservice.yaml

# Watch the gang come up (1 leader + 3 workers + router):
kubectl -n sandbox get pods -l ome.io/inferenceservice=sglang-tier5-42b-bf16 -w

# Confirm all 4 engine pods landed in ONE MNNVL domain:
for p in leader-0 worker-0 worker-1 worker-2; do
  n=$(kubectl -n sandbox get pod sglang-tier5-42b-bf16-engine-0-$p -o jsonpath='{.spec.nodeName}')
  d=$(kubectl get node "$n" -o jsonpath='{.metadata.labels.network\.meta\.com/domain}')
  echo "  $p -> $n [$d]"
done
```

> **Gang scheduling** needs both the scheduler-plugins `PodGroup` CRD and a
> gang-aware scheduler selected with `schedulerName`; the CRD alone does not
> change default-scheduler behavior. Without both, the workload degrades to
> ordinary scheduling and a partially scheduled gang can sit half-up.
>
> **The router** depends on the engine leader: it discovers backends via `--label-selector
> component=engine,ome.io/runner=leader,…` and stays `0/1` (restart-looping) until the
> engine leader is `Ready`. That's expected during the long weight load; it stabilizes once
> the engine serves `/health`.

## Test the model (tokens in → tokens out)

Once the engine is `Ready` (the cold ~2.18 TiB weight load takes a while — watch
`kubectl -n sandbox get pods -l ome.io/inferenceservice=sglang-tier5-42b-bf16`), hit the
router. Its API is sglang's, proxied on `:30080`:

```bash
# Port-forward the router — this BLOCKS the terminal, so background it with `&`
# (or run the curl from a second shell). Stop it later with `kill %1`:
kubectl -n sandbox port-forward svc/sglang-tier5-42b-bf16-router 30080:30080 &
sleep 2

# sglang NATIVE /generate — raw text in, generated text + token counts out:
curl http://localhost:30080/generate -H 'Content-Type: application/json' -d '{
  "text": "The capital of France is",
  "sampling_params": {"max_new_tokens": 32, "temperature": 0}
}'
# → {"text": " Paris. ...", "output_ids": [...],
#    "meta_info": {"prompt_tokens": 5, "completion_tokens": 32, "finish_reason": {"type": "length"}, ...}}

# Or the OpenAI chat-completions API (model = the router's --model-name):
curl http://localhost:30080/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "tier5-42b-bf16",
  "messages": [{"role": "user", "content": "Say hello in one sentence."}],
  "max_tokens": 32
}'
```
