# fake-serving

Tiny test-pod image used by OME's KIND-backed integration tests
(`tests/integration/kindutil/`). Built locally and copied into the
KIND cluster's containerd via `kind load docker-image`. No registry,
no auth.

## Behavior contract

| Env var | Effect |
|---|---|
| (unset) | bind `:8080`, return `200 ok` to `/healthz` |
| `FAIL_AT=startup` | exit 1 immediately (CrashLoopBackOff) |
| `FAIL_AT=ready` | bind `:8080` but return `503` to `/healthz` |
| `FAIL_AT=after-Nm` | run normally for N minutes, then exit 1 |
| `FAIL_ON_NODE=<node>` | return `503` to `/healthz` only while running on `<node>` (node-scoped fault; pod spec supplies `NODE_NAME` via fieldRef) |
| `FAIL_ON_POD=<pod>` | narrow `FAIL_ON_NODE` to one pod name (pod spec supplies `POD_NAME` via fieldRef) |
| `STARTUP_DELAY_SEC=N` | sleep N seconds before binding |

OMENative's serving-gate (`ome.io/serving=True`) is set by the
controller, separate from kubelet's `ContainersReady`. fake-serving
only controls the latter. Pod is "Serving" only when both are true.

## Build manually

The `kindutil.BuildImages` helper handles this in test setup, but you
can also build by hand:

```bash
cd tests/integration/kindutil/fake-serving
docker build -t fake-serving:v1 .
docker tag fake-serving:v1 fake-serving:v2   # same binary, second tag
kind load docker-image fake-serving:v1 fake-serving:v2 --name <cluster>
```

Use `:v1` and `:v2` as distinct tags so an image-bump test triggers a
ControllerRevision change. The binary is identical between tags; only
the image reference differs.
