# How-to: launch a model

*Task recipe — page 1 of 4. Next: [Write a runtime](howto-write-a-runtime.md) ·
[Update & roll](howto-update-and-roll.md) · [Multi-node & PD](howto-multi-node-and-pd.md)*

Goal: get one model serving and call it. ~10 minutes. No prior OME knowledge
assumed. For *why* OME (vs a plain Deployment) and the object model, read the
`guide-*.md` reference pages — this page is just the steps.

## What you need

Two objects:

1. **A runtime** — a `ClusterServingRuntime` / `ServingRuntime`. The container
   image + launch command that serves the model, *and where the weights come
   from* — the runtime fetches them at runtime (cluster cache). **Your platform
   team usually already published one** — list them with
   `kubectl get clusterservingruntime`. If you need to write your own, see
   [Write a runtime](howto-write-a-runtime.md).
2. **An InferenceService** (`isvc`) — points at the runtime and says how many
   replicas. This is the thing you deploy.

> **No model resource needed.** The runtime pulls the weights at runtime via
> cluster cache, so you do **not** create a `BaseModel` and you omit `spec.model`
> entirely. (`spec.model` *is* a real, optional field — for serving weights from
> a PVC / object store that OME should manage and auto-select against; see
> [Models & InferenceService](guide-models-and-inferenceservice.md). The internal
> cluster-cache path below doesn't use it.)

## The smallest working example

One model, one pod, served as a plain Kubernetes Deployment. Copy, edit the two
`<...>` placeholders, `kubectl apply -f`.

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: my-model
  namespace: <your-namespace>
spec:
  runtime:
    name: <a-runtime-name>    # from `kubectl get clusterservingruntime`;
                              # the runtime carries the model path (cluster cache)
  engine:                     # the serving pool
    minReplicas: 1
    maxReplicas: 1
```

That's it — no `spec.model`, no deployment-mode annotation, no rollout config.
You get a plain Deployment + Service, which is exactly right for "serve one
model." The runtime's launch command (`--model-path $(CC_MODEL_PATH)` + the
`CLUSTER_CACHE_*` env) pulls the weights when the pod starts — see
[Write a runtime](howto-write-a-runtime.md).

## Check it came up

```sh
kubectl get isvc my-model -n <your-namespace>
# NAME       URL                          READY   ...
# my-model   http://my-model.<ns>...      True

kubectl get pods -n <your-namespace> -l ome.io/inferenceservice=my-model
```

`READY=True` and the `URL` column is your endpoint. If it's stuck, jump to
[Update & roll → "When a rollout is stuck"](howto-update-and-roll.md#when-a-rollout-is-stuck)
— the same diagnosis steps apply to a first launch.

## Call it

```sh
# From inside the cluster (the URL is a cluster-internal Service by default):
curl http://my-model.<your-namespace>.svc.cluster.local/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"<served-model-name>","messages":[{"role":"user","content":"hi"}]}'
```

(Path/port and the `model` name depend on the runtime's server — sglang/vLLM
speak the OpenAI API; `<served-model-name>` is whatever name the runtime serves
under.)

## Two things that will bite you on day one

1. **A bare launch is "RawDeployment" mode.** That's fine for one model. You only
   need OME's coordinated rollouts / blue-green / prefill-decode / multi-node when
   you opt into **OMENative** mode — set deliberately, *per component*. See
   [Update & roll](howto-update-and-roll.md) and
   [Multi-node & PD](howto-multi-node-and-pd.md). Until then, don't set it.

2. **Name the runtime — don't rely on auto-selection here.** OME's
   auto-selection (omit `runtime` and let OME pick) matches a runtime's
   `supportedModelFormats` against a **model's** format / architecture — so it
   needs a `spec.model`. With the cluster-cache path there's no `spec.model`, so
   there's nothing to match against: **always name `runtime` explicitly.** A
   `runtime.kind` you set must also be right — it defaults to
   `ClusterServingRuntime`; for a namespaced `ServingRuntime` add
   `kind: ServingRuntime`.

## Next

- Need to author the runtime yourself? → [Write a runtime](howto-write-a-runtime.md)
- Shipping a new image or checkpoint to a running service? → [Update & roll](howto-update-and-roll.md)
- Model too big for one GPU / one node, or want prefill-decode split? →
  [Multi-node & PD](howto-multi-node-and-pd.md)
