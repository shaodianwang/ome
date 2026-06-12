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

## Apply

```sh
kubectl apply -f clusterservingruntime.yaml
kubectl apply -f inferenceservice.yaml
```

Replace `<tpu-serving-image>` and the leader/worker launch commands in
`clusterservingruntime.yaml` with your serving stack (vLLM-on-TPU, MaxText,
…), launched as a Ray multi-host cluster sized to `OME_INSTANCE_POD_COUNT`,
with workers joining `OME_LEADER_ADDRESS`. The placeholder command just prints
the injected `TPU_*` / `OME_*` env so you can confirm the wiring.

## Scaling

- **Bigger slice / more hosts:** raise `engine.worker.size` and pick a TPU
  topology with the matching host count (e.g. `2x2x4` = 4 hosts). OME extends
  the peer list and ranks automatically.
- **More replicas:** raise `engine.maxReplicas` — each replica is its own gang
  (its own TPU slice). Add a KEDA/HPA trigger to autoscale.
