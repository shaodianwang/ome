# OME (Open Model Engine) — Kubernetes Operator for LLM Serving

Forked from [ome-projects/ome](https://github.com/ome-projects/ome).

## What is OME?

OME is a Kubernetes operator for managing and serving LLMs in production. It handles model lifecycle, runtime selection, and PD-disaggregated / multi-node deployment patterns.

## Features Overview

- **Model Management:** Models are first-class custom resources. Sophisticated parsing extracts architecture, parameter count, and capabilities from SafeTensors model files.

- **Intelligent Runtime Selection:** Automatic matching of models to optimal runtime configurations through weighted scoring based on architecture, format, quantization, parameter size, and framework compatibility.

- **Optimized Deployments:** Supports multiple deployment patterns including prefill-decode disaggregation, multi-node inference, and traditional Kubernetes deployments with advanced scaling controls.

- **Resource Optimization:** Specialized GPU bin-packing scheduling with dynamic re-optimization to maximize cluster efficiency while ensuring high availability.

- **Kubernetes Ecosystem Integration:** Deep integration with Kueue, LeaderWorkerSet, KEDA, Gateway API, and Gateway API Inference Extension.

## Architecture

OME uses a component-based architecture built on Kubernetes custom resources:

- **BaseModel/ClusterBaseModel:** Define model sources and metadata with automatic parsing of architecture, parameters, and capabilities
- **ServingRuntime/ClusterServingRuntime:** Define how models are served with runtime-specific configurations
- **InferenceService:** Connects models to runtimes for deployment with support for prefill-decode disaggregation and multi-node inference

## PodGroupDeploymentRollout Mode

OME can delegate prefill/decode podgroup lifecycle to the
`podgroup.meta.com/v1alpha1` `PodGroupDeploymentRollout` controller. This is an
opt-in deployment mode on `engine` and `decoder`; the top-level
`InferenceService` shape remains `engine`, `decoder`, and optional `router`.

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: sglang-pd
spec:
  engine:
    minReplicas: 8
    annotations:
      ome.io/deploymentMode: PodGroupDeploymentRollout
    podGroupDeployment:
      replicasPerGroup: 2
      topologyKey: ds.coreweave.com/nvlink.domain
      scheduleTimeoutSeconds: 60
      schedulerName: default-scheduler
      rollout:
        maxUnavailablePercent: 25
        maxSkew: 0
      multiNode:
        enabled: true
        tensorParallelSize: 8
        distInitPort: 30000
  decoder:
    minReplicas: 4
    annotations:
      ome.io/deploymentMode: PodGroupDeploymentRollout
    podGroupDeployment:
      replicasPerGroup: 2
      topologyKey: ds.coreweave.com/nvlink.domain
      scheduleTimeoutSeconds: 60
      schedulerName: default-scheduler
      multiNode:
        enabled: true
        tensorParallelSize: 8
        distInitPort: 30000
  router:
    annotations:
      ome.io/deploymentMode: RawDeployment
```

OME renders one `PodGroupDeploymentRollout` named after the
`InferenceService`. `engine` becomes the PGDR `prefill` member and `decoder`
becomes the `decode` member. `engine.minReplicas` and `decoder.minReplicas`
default the member group counts unless `podGroupDeployment.groups` is set
explicitly. Router remains a normal OME component and should discover PGDR
rank-0 pods through the podgroup labels, for example
`app.kubernetes.io/component=prefill,podgroup.meta.com/service-target=true`.

## Development

See [CLAUDE.md](CLAUDE.md) for build commands, test instructions, and detailed architecture reference.
