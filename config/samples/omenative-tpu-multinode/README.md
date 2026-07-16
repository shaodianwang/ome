# OMENative multi-host TPU serving (example)

Runs a multi-host (multi-node) model server on a GKE TPU slice with OME's
**OMENative** deployment mode: a leader + worker gang placed on one multi-host
TPU slice (e.g. `tpu7x` `2x2x2` = 2 hosts × 4 chips).

## What OME injects

OME owns the gang topology, so it injects the coordination env every pod needs
— into **every** container of the leader and worker pods:

| Variable | Purpose |
|---|---|
| `OME_LEADER_ADDRESS` | leader pod DNS (`<pod>.<headless-svc>`) |
| `OME_INSTANCE_POD_RANK` | flat gang rank, `0..N-1` (leader = 0) |
| `OME_INSTANCE_POD_COUNT` | total pods in the gang |
| `OME_INSTANCE_SUBDOMAIN` | per-Instance DNS subdomain |
| `TPU_WORKER_HOSTNAMES` | all peer host DNS names, in rank order |
| `TPU_WORKER_ID` | this pod's gang rank |
| `TPU_PROCESS_ADDRESSES` | `host:port` for each peer |
| `TPU_PROCESS_PORT` / `TPU_NAME` | mesh handshake port / slice name |

The `TPU_*` vars are what JAX/libtpu reads to build the cross-host device mesh.
OME injects them automatically for any **multi-pod** Instance whose runner
**requests `google.com/tpu`** — no LeaderWorkerSet, and no GKE TPU
slice-selector label. Single-pod (single-host) TPU pods keep GKE's default
single-host env, which is correct for one host.

The InferenceService also declares the GKE partition label key explicitly as
`engine.topologyKey`. OME uses that config value for worker-to-leader affinity
and for the PodGroup topology contract; it does not infer provider-specific
label names from the pod's node selector. `topologyKey` does not select or
install a scheduler. To use ome-scheduler, install its chart and then set
`spec.schedulerName: ome-scheduler` on the ClusterServingRuntime; without that
opt-in, the default scheduler ignores the PodGroup contract.

## Apply

```sh
kubectl apply -f clusterservingruntime.yaml
kubectl apply -f inferenceservice.yaml
```

The leader/worker runners in `clusterservingruntime.yaml` carry a real
vLLM-on-TPU launch **in the runtime itself** (no external launch ConfigMap):
each forms a Ray multi-host cluster — head sized to `OME_INSTANCE_POD_COUNT`,
workers joining `OME_LEADER_ADDRESS` — then the leader runs `vllm serve`. To
adapt it, set `<tpu-serving-image>` to your vLLM-on-TPU image and the runner
`env` (`CKPT`, `TENSOR_PARALLEL_SIZE` = chips per gang, the `VLLM_*` knobs).
Enable **eagle3 speculative decoding** or **DP-attention sharding** by
uncommenting the `SPECULATIVE_CONFIG` / `ADDITIONAL_CONFIG` env entries. For a
different stack (MaxText, …) swap the runner `args`; the only contract is to
form a Ray cluster sized to `OME_INSTANCE_POD_COUNT` with workers joining
`OME_LEADER_ADDRESS`.

## Scaling

- **Bigger slice / more hosts:** raise `engine.worker.size` and pick a TPU
  topology with the matching host count (e.g. `2x2x4` = 4 hosts). OME extends
  the peer list and ranks automatically.
- **More replicas:** raise `engine.maxReplicas` — each replica is its own gang
  (its own TPU slice). Add a KEDA/HPA trigger to autoscale.
