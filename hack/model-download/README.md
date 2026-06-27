# Model-weight download → shared-vast PVC (Manifold)

Reproducible recipe for staging a model/checkpoint onto a `shared-vast` PVC in a
CW cluster, using Manifold (Meta blob store) authenticated by a Meta **service
identity** (Aqueduct cert + `cloud_oss`). This is the pattern behind the prod job
`dl-tier5-42b-128k-bf16-simolin` (PVC `ckpt-tier5-42b-128k-bf16`, ns `prod`,
cluster `cw/msl-cw-use2-2`).

> This is NOT the same as the older `hack/model-download.yaml` (a HuggingFace →
> hostPath `/raid` DaemonSet). This one pulls from Manifold into a **PVC**.

## What it does
1. **PVC** (`pvc.yaml`) — `ReadWriteMany`, `shared-vast` storageclass. The PV is
   **dynamically provisioned** (Retain), so you only create the PVC.
2. **Job** (`download-job.yaml`) — runs `model-downloader` which writes an
   `~/.aws/config` whose `credential_process` is `cloud_oss` (so the AWS SDK gets
   short-lived creds for Manifold's S3 gateway), then runs
   `/download.py checkpoint --provider=manifold --local_dir=/mnt/models --s3_download_base_uri=<URI>`.
   The PVC is mounted at `/mnt/models`.

## Prerequisites (must ALREADY exist — these files do not create them)
- Secret **`inference-aqueduct-cert`** in the target namespace (the Aqueduct
  service-identity cert; mounted at `/var/facebook/x509_svc/`).
- **`cloud_oss`** present on the scheduled node at hostPath `/opt/cf/cloud_oss`
  (true for `metafb.cloud/node-type=gb300` nodes).
- **`shared-vast`** StorageClass in the cluster.
- Your service identity authorized to read the Manifold path (e.g. `fair_llms_prod`).

## Usage
Input is a `manifold://` URI. The job's `--s3_download_base_uri` uses the **same
path with the scheme swapped to `s3://`** (download.py is invoked with
`--provider=manifold`). Inputs usually look like
`manifold://fair_llms_prod/tree/vllm_test/<model-id>/checkpoint_0000150/checkpoint`.

> **`download.py` appends `/checkpoint` to the base URI itself.** So the script
> strips a trailing literal `/checkpoint` from the base URI — otherwise it would
> fetch `.../checkpoint/checkpoint` (empty) and the job "succeeds" in seconds having
> downloaded nothing. A genuine download takes many minutes; a 3-second "success"
> means the path was wrong.

The PVC name is derived from the `checkpoint_NNNN` segment and the model-id before it.

```sh
# Generate the manifests for a checkpoint (auto PVC name, 3Ti default). PRINTS ONLY.
./gen-download.sh manifold://fair_llms_prod/tree/vllm_test/<model-id>/checkpoint_0000150/checkpoint

# Override anything:
./gen-download.sh <uri> --name ckpt-my-model --size 4Ti --namespace prod \
  --node-type gb300 --user "$USER"

# Review, then apply YOURSELF against the right cluster (intentionally manual):
./gen-download.sh <uri> > /tmp/dl.yaml
less /tmp/dl.yaml
kubectl --context cw/msl-cw-use2-2 -n prod apply -f /tmp/dl.yaml
```
The script **never applies** — it writes YAML to stdout so you review before touching
a prod cluster. Auto-derived names are `ckpt-<model-id>-ckpt<N>` (PVC) and
`dl-<model-id>-ckpt<N>-<user>` (Job), truncated to a valid k8s name; pass `--name`
for a clean fixed name.

## Verify
```sh
kubectl -n prod get pvc <PVC_NAME>
kubectl -n prod get job <JOB_NAME> -w
kubectl -n prod logs job/<JOB_NAME> -f
```
The Job is `restartPolicy: Never`, `backoffLimit: 3`. On `Complete`, the weights are
in the PVC at the checkpoint's directory layout under `/mnt/models`.

## Files
- `pvc.yaml` — sample PVC (the real `ckpt-tier5-42b-128k-bf16`, 3Ti).
- `download-job.yaml` — sample Job (the real `dl-tier5-42b-128k-bf16-simolin`).
- `gen-download.sh` — generator: `<manifold-uri> [flags]` → renders both manifests.
