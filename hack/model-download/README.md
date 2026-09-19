# Model-weight download → shared-vast PVC

Reproducible recipe for staging a model/checkpoint onto a `shared-vast` PVC in a
CoreWeave cluster, pulling from the blob store (`manifold://` URIs) authenticated by a
**service identity** (identity cert + `cloud_oss`). The sample manifests use
placeholder names (`example-model`, `example-user`, `<namespace>`); substitute
your own, or render real ones with `gen-download.sh`.

> This is NOT the same as the older `hack/model-download.yaml` (a HuggingFace →
> hostPath `/raid` DaemonSet). This one pulls from the blob store into a **PVC**.

## What it does
1. **PVC** (`pvc.yaml`) — `ReadWriteMany`, `shared-vast` storageclass. The PV is
   **dynamically provisioned** (Retain), so you only create the PVC.
2. **Job** (`download-job.yaml`) — runs `model-downloader` which writes an
   `~/.aws/config` whose `credential_process` is `cloud_oss` (so the AWS SDK gets
   short-lived creds for the blob store's S3 gateway), then runs
   `/download.py checkpoint --provider=manifold --local_dir=/mnt/models --s3_download_base_uri=<URI>`.
   The PVC is mounted at `/mnt/models`.

## Prerequisites (must ALREADY exist — these files do not create them)
- Secret **`inference-aqueduct-cert`** in the target namespace (the
  service-identity cert; mounted at `/var/facebook/x509_svc/`).
- **`cloud_oss`** present on the scheduled node at hostPath `/opt/cf/cloud_oss`
  (true for `metafb.cloud/node-type=gb300` nodes).
- **`shared-vast`** StorageClass in the cluster.
- Your service identity authorized to read the blob-store path (the `<bucket>`
  in the URI).

## Usage
Input is a `manifold://` URI. The job's `--s3_download_base_uri` uses the **same
path with the scheme swapped to `s3://`** (download.py is invoked with
`--provider=manifold`). Inputs usually look like
`manifold://<bucket>/tree/<prefix>/<model-id>/checkpoint_0000150/checkpoint`.

> **`download.py` appends `/checkpoint` to the base URI itself.** So the script
> strips a trailing literal `/checkpoint` from the base URI — otherwise it would
> fetch `.../checkpoint/checkpoint` (empty) and the job "succeeds" in seconds having
> downloaded nothing. A genuine download takes many minutes; a 3-second "success"
> means the path was wrong.

The PVC name is derived from the `checkpoint_NNNN` segment and the model-id before it.

```sh
# Generate the manifests for a checkpoint (auto PVC name, 3Ti default). PRINTS ONLY.
./gen-download.sh manifold://<bucket>/tree/<prefix>/<model-id>/checkpoint_0000150/checkpoint

# Override anything:
./gen-download.sh <uri> --name ckpt-my-model --size 4Ti --namespace <namespace> \
  --node-type gb300 --user "$USER"

# Review, then apply YOURSELF against the right cluster (intentionally manual):
./gen-download.sh <uri> > /tmp/dl.yaml
less /tmp/dl.yaml
kubectl --context <cluster-context> -n <namespace> apply -f /tmp/dl.yaml
```
The script **never applies** — it writes YAML to stdout so you review before touching
a production cluster. Auto-derived names are `ckpt-<model-id>-ckpt<N>` (PVC) and
`dl-<model-id>-ckpt<N>-<user>` (Job), truncated to a valid k8s name; pass `--name`
for a clean fixed name.

## Verify
```sh
kubectl -n <namespace> get pvc <PVC_NAME>
kubectl -n <namespace> get job <JOB_NAME> -w
kubectl -n <namespace> logs job/<JOB_NAME> -f
```
The Job is `restartPolicy: Never`, `backoffLimit: 3`. On `Complete`, the weights are
in the PVC at the checkpoint's directory layout under `/mnt/models`.

## Files
- `pvc.yaml` — sample PVC (`ckpt-example-model`, 3Ti).
- `download-job.yaml` — sample Job (`dl-example-model-example-user`), mounting that PVC.
- `example-checkpoint-a_dl.yaml`, `example-checkpoint-b_dl.yaml` — PVC + Job pairs
  as `gen-download.sh` renders them, with placeholder names.
- `gen-download.sh` — generator: `<manifold-uri> [flags]` → renders both manifests.
