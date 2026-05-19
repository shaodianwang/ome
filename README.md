# OME (Open Model Engine) — Kubernetes Operator for LLM Serving

Forked from [ome-projects/ome](https://github.com/ome-projects/ome).

## What is OME?

OME (Open Model Engine) is a Kubernetes operator for enterprise-grade management and serving of Large Language Models (LLMs). It optimizes the deployment and operation of LLMs by automating model management, intelligent runtime selection, efficient resource utilization, and sophisticated deployment patterns.

## Features Overview

- **Model Management:** Models are first-class citizen custom resources. Sophisticated model parsing extracts architecture, parameter count, and capabilities directly from model files. Supports distributed storage with automated repair, double encryption, namespace scoping, and multiple formats (SafeTensors, PyTorch, TensorRT, ONNX).

- **Intelligent Runtime Selection:** Automatic matching of models to optimal runtime configurations through weighted scoring based on architecture, format, quantization, parameter size, and framework compatibility.

- **Optimized Deployments:** Supports multiple deployment patterns including prefill-decode disaggregation, multi-node inference, and traditional Kubernetes deployments with advanced scaling controls.

- **Resource Optimization:** Specialized GPU bin-packing scheduling with dynamic re-optimization to maximize cluster efficiency while ensuring high availability.

- **Runtime Integrations:** First-class support for SGLang and vLLM for high-throughput inference, and Triton for general model inference.

- **Kubernetes Ecosystem Integration:** Deep integration with Kueue, LeaderWorkerSet, KEDA, Gateway API, and Gateway API Inference Extension.

## Architecture

OME uses a component-based architecture built on Kubernetes custom resources:

- **BaseModel/ClusterBaseModel:** Define model sources and metadata with automatic parsing of architecture, parameters, and capabilities
- **FineTunedWeight:** Define LoRA adapters and fine-tuned weights that extend base models
- **ServingRuntime/ClusterServingRuntime:** Define how models are served with runtime-specific configurations
- **InferenceService:** Connects models to runtimes for deployment with support for prefill-decode disaggregation and multi-node inference
- **BenchmarkJob:** Measures model performance under different workloads with configurable traffic patterns

OME's controller automatically:
1. Downloads and parses models to understand their characteristics
2. Selects the optimal runtime configuration for each model
3. Generates Kubernetes resources for efficient deployment
4. Continuously optimizes resource utilization across the cluster

## Development

See [CLAUDE.md](CLAUDE.md) for build commands, test instructions, and detailed architecture reference.
