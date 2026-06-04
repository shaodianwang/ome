# Models & the InferenceService

*OME user guide — page 4 of 4. Prev: [Runtimes](guide-runtimes.md) ·
[OMENative](guide-omenative.md) · [To OME or not](guide-when-to-use-ome.md)*

This page covers the last two pieces: the model (where the weights come from)
and the InferenceService (the thing you actually deploy, which ties a model and
a runtime together).

On this page:
- How the weights reach the pod: cluster cache vs PVC
- The model is optional
- What's inside an InferenceService
- Engine, decoder, router
- Putting it together
- Rules of thumb
- FAQ

## How the weights reach the pod: cluster cache vs PVC

There are two ways the model weights get onto a serving pod, and it's worth
being clear about which one is doing the work.

**Cluster cache (the real path).** The runtime is launched with
`--model-path $(CC_MODEL_PATH)`, where `CC_MODEL_PATH` is a `manifold://`
location, and a set of `CLUSTER_CACHE_*` env vars. At startup the pod fetches
the weights through cluster cache. Nothing about this needs a model *resource* —
the path and the cache env are enough.

**PVC (today's fallback).** The runtime also passes
`--fallback-model-path $(MODEL_PATH)`. `$(MODEL_PATH)` points at weights mounted
from a PersistentVolumeClaim, which is what a `ClusterBaseModel` with `pvc://`
storage provides. It's there so the pod has something to fall back to while
cluster cache is still being hardened.

> The PVC fallback is temporary. Once cluster cache is fully stable, remove it:
> delete the `ClusterBaseModel`, drop `spec.model` from the InferenceService,
> and drop the two `--fallback-*` flags from the runtime. Until then, keep it.

## The model is optional

Because cluster cache supplies the weights, **`spec.model` is optional**. An
InferenceService is valid with just a runtime; the model resource exists only
for the PVC fallback above. This is deliberate — it means researchers don't have
to create and manage a model object to ship a service. (You must set at least
one of `spec.model` or `spec.runtime`; in practice you always have a runtime.)

When you do use it, a `ClusterBaseModel` is small: a name, the format and
architecture, and where the weights live (`storageUri`). OME reads the rest
(parameter count, capabilities) from the model files.

## What's inside an InferenceService

The InferenceService is the resource you deploy. A minimal OMENative one has:

- `spec.deploymentMode: OMENative` — set once; it propagates to every component
  and the ingress (see [OMENative](guide-omenative.md), rule #1).
- `spec.runtime.name` — the runtime to use (usually your tenant runtime).
- `spec.model` — optional; only for the PVC fallback.
- one or more components — `engine`, `decoder`, `router` — each with
  `minReplicas`/`maxReplicas`, an optional `priorityClassName`, and an optional
  `lifecycle` block.
- optional `spec.rollout.coordination` — how the components roll together.

That's the whole surface a scientist touches: which runtime, how many of each
component, and how upgrades should behave.

## Engine, decoder, router

The component names map to roles:

- **engine** — prefill (processing the prompt).
- **decoder** — decode (generating tokens). Present only in PD setups.
- **router** — fronts engine and decoder and load-balances across them. When a
  router is present, it's the entry point; otherwise the engine is.

A non-PD model is just an engine (and maybe a router). A PD model is engine +
decoder + router, which is the example in this folder.

## Putting it together

The files in this folder, applied in this order, are a complete OMENative PD
deployment:

1. **base runtime** (`base-clusterservingruntime.yaml`) — the hardware profile,
   owned by the platform team.
2. **tenant runtime** (`tenant-servingruntime.yaml`) — your image and flags;
   inherits the base.
3. **model** (`clusterbasemodel-pvc.yaml`) — the temporary PVC fallback.
4. **InferenceService** (`inferenceservice.yaml`) — ties model + runtime
   together, sets replicas and the rollout.

Read the folder's `README.md` for the operational walk-through; read pages 1–3
of this guide for the concepts behind each piece.

## Rules of thumb

- *Don't create a model resource you don't need.* If cluster cache serves your
  weights, `spec.model` is optional — leave it out once the fallback is gone.
- *Treat the PVC model as scaffolding.* It's there to be removed, not to grow
  features.
- *Keep the InferenceService small.* Runtime + replicas + rollout. If it's
  filling up with image and flag details, those belong in the tenant runtime.

## FAQ

**If `spec.model` is optional, why is it in the example?**
For the PVC fallback. Cluster cache is the primary path; the fallback needs a
`ClusterBaseModel` to mount, and `spec.model` is how the service references it.
When the fallback goes away, so does `spec.model`.

**Can two services share one model?**
Yes. A `ClusterBaseModel` is cluster-scoped; many InferenceServices can
reference the same one.

**Do I need a router?**
Only for PD (and any case where you want a single load-balanced entry point in
front of multiple component replicas). A single-engine service doesn't require
one.

**What's the smallest valid InferenceService?**
A runtime reference plus one engine component, with `OMENative` on the engine.
Everything else — model, decoder, router, coordination — is optional.
