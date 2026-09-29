---
name: ome-recipe
description: Answer "which runtime should I use for model X" or "what is the recipe for use case Y" on the OME fleet from the OME bot - from the installed ServingRuntime catalog read through the ome-bridge helper, how OME's selector matches a model to a runtime, and precedents already serving similar models - and hand back an InferenceService skeleton. Read-only; load after ome-bridge.
---

# ome-recipe — from a model or use case to a runtime and an ISVC skeleton

OME picks a runtime for a model by matching format, architecture, framework,
quantization and size against each ServingRuntime's `supportedModelFormats`,
then scoring the candidates; an explicit `spec.runtime` on the ISVC overrides
the pick. The selector is described in the upstream docs
(`$B tool docs search "supportedModelFormats" --root upstream-docs`) and
implemented in `pkg/runtimeselector`.

Every live read is `$B command "<verb and args>"` with
`B="python3 <ome-bridge skill dir>/scripts/ome_bridge.py"`; read `ome-bridge`
first for the verbs, the fan-out rule and how to pick clusters.

## 1. Pin down the model

```sh
$B command "get models -A -o wide"        # BaseModel / ClusterBaseModel already onboarded, every cluster
```

From the model object or the user: architecture, format (safetensors, GGUF,
...), parameter count, quantization, context needs, and the accelerator the
team has (TPU slice shape or GPU class). If the model is not onboarded, say
so and answer for the closest onboarded one.

## 2. Read the catalog

```sh
$B command "get runtimes -A -o wide"                                   # ClusterServingRuntime + ServingRuntime, every cluster
$B command "runtime explain --model <model> -n <ns> --cluster <name>"  # which runtimes the selector accepts, with rejection reasons
$B command "get isvc -A -o wide"                                       # candidate precedents
$B command "runtime explain --isvc <existing-isvc> -n <ns> --cluster <member>"   # why a precedent got its runtime
$B command "runtime effective <existing-isvc> -n <ns> --cluster <member>"        # the merged runtime it runs, pins, drift
$B command "runtime tree <runtime> --cluster <name>"                   # parents, descendants and services using it
$B command "accelerator explain <existing-isvc> -n <ns> --cluster <member>"
```

`runtime explain --model` is the selector's own verdict for that model on
that cluster; run it on each cluster the team could deploy to, since
catalogs differ between clusters. It has human output only (no `-o`).

Precedent beats theory: an ISVC already serving the same architecture on the
same accelerator is the recipe. Name it, with its cluster.

## 3. Compose the answer

- The runtime name and cluster, with the `supportedModelFormats` line that
  matched.
- The accelerator selection: class or explicit selector, chips or GPUs per
  pod, leader and worker shape for multi-host.
- Replica and rollout defaults the team should expect, and what they must set
  themselves.
- An ISVC skeleton, generic names, no cluster specifics baked in:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: <service>
  namespace: <team-namespace>
spec:
  model:
    name: <base-model-name>
  runtime:
    name: <runtime-name>      # omit to let OME select
  engine:
    minReplicas: 1
    maxReplicas: 1
```

- Caveats: quantization mismatches, context length versus memory, a
  runtime that exists only on some clusters.

## Rules

- Read-only. Do not create or edit runtimes, models or services.
- If two runtimes match, show both with the scoring differences instead of
  choosing silently.
- Runtime and model catalogs are authored in the operators' GitOps
  repositories, not in the OME repository; point there for changes.
- Reply with `*Verdict*` (the runtime), `*Evidence*` (the matching catalog
  line and the precedent, each prefixed by cluster), `*Next action*` (the
  skeleton), `*Scope*`, closing `read-only, nothing changed`.
