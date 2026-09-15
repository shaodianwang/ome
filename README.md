# OME (Open Model Engine) — Kubernetes Operator for LLM Serving

Forked from [ome-projects/ome](https://github.com/ome-projects/ome).

## What is OME?

OME is a Kubernetes operator for managing and serving LLMs in production. It handles model lifecycle, runtime selection, and PD-disaggregated / multi-node deployment patterns.

## Features Overview

- **Model Management:** Models are first-class custom resources. Sophisticated parsing extracts architecture, parameter count, and capabilities from SafeTensors model files.

- **Intelligent Runtime Selection:** Automatic matching of models to optimal runtime configurations through weighted scoring based on architecture, format, quantization, parameter size, and framework compatibility.

- **Optimized Deployments:** Supports multiple deployment patterns including prefill-decode disaggregation, multi-node inference, and traditional Kubernetes deployments with advanced scaling controls.

- **Resource Optimization:** Specialized GPU bin-packing scheduling with dynamic re-optimization to maximize cluster efficiency while ensuring high availability.

- **Accelerator Management:** Hardware-aware scheduling through AcceleratorClass resources that define GPU capabilities, discovery patterns, and cost information.

- **Kubernetes Ecosystem Integration:** Deep integration with Kueue, LeaderWorkerSet, KEDA, Gateway API, and Gateway API Inference Extension.

## Architecture

OME uses a component-based architecture built on Kubernetes custom resources:

- **BaseModel/ClusterBaseModel:** Define model sources and metadata with automatic parsing of architecture, parameters, and capabilities
- **ServingRuntime/ClusterServingRuntime:** Define how models are served with runtime-specific configurations
- **InferenceService:** Connects models to runtimes for deployment with support for prefill-decode disaggregation and multi-node inference
- **AcceleratorClass:** Define GPU hardware classes with capabilities, discovery patterns, and cost information for intelligent scheduling

## Development

See [CLAUDE.md](CLAUDE.md) for build commands, test instructions, and detailed architecture reference.

## Operations

- [OMENative InferenceReplica status representation](docs/omenative-status-representation.md): the `omenativeStatus` configuration block, the schema-first rollout order, the downgrade rule, the status write-boundary metrics and events, and the read-only `ome-status-preflight` command.
- [kubectl-qt](docs/kubectl-qt/README.md): render the AcceleratorQuota hierarchy as a tree.
