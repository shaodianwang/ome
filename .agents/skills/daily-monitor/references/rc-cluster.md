# Looking at the RC cluster without breaking it

The smokes run inside the RC cluster as agent pods with their own
ServiceAccount; you have no such token. Look only when the build's logs,
meta-data and artifacts are not enough, and look read-only.

## Kubeconfig hygiene (hard blocker)

`daily_rc_common.sh` obtains its kubeconfig by writing a dedicated file in a
fresh temp dir, `chmod 600`, exporting `KUBECONFIG` to it for the step, and
deleting the dir at exit. Do the same with your own RC credentials: a
throwaway file, never the ambient `$KUBECONFIG`, never
`kubectl config use-context`.

```sh
RC_KUBECONFIG="$(mktemp -t ome-rc-XXXXXX.kubeconfig)"
chmod 600 "$RC_KUBECONFIG"
# Obtain credentials INTO that file. For a GKE cluster:
KUBECONFIG="$RC_KUBECONFIG" gcloud container clusters get-credentials msl-gcp-usc1-rc-1 \
  --region <region> --project <project>
# Every command names the file explicitly.
kubectl --kubeconfig "$RC_KUBECONFIG" -n ome get deploy ome-controller-manager ome-scheduler -o wide
# Done:
rm -f "$RC_KUBECONFIG"
```

Region and project are not recorded in this repository; take them from your
own GCP access. `.buildkite/README-gke-kind.md` names `msl-gcp-usc1-rc-1` as
the RC cluster; the smokes build their kubeconfig from the agent pod's own
ServiceAccount, so they run inside whichever cluster hosts the GKE queue's
agent stack. Confirm the cluster name against the queue's agent-stack
configuration before relying on it.

## Read-only checks that answer the usual questions

```sh
K="kubectl --kubeconfig $RC_KUBECONFIG"

# Is RC running the candidate? (the same three identities the smoke compares)
$K -n ome get deploy ome-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
$K -n ome get deploy ome-scheduler          -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
$K -n ome get configmap inferenceservice-config -o jsonpath='{.data.omeAgent}{"\n"}'
$K -n ome get deploy ome-controller-manager ome-scheduler \
  -o custom-columns='NAME:.metadata.name,CHART:.metadata.labels.helm\.sh/chart,GEN:.metadata.generation,OBS:.status.observedGeneration,UPD:.status.updatedReplicas,AVAIL:.status.availableReplicas'

# Is the control plane healthy on it?
$K -n ome get pods -o wide
$K -n ome logs deploy/ome-controller-manager --tail=200

# What did a smoke leave behind? (normally nothing; the next run cleans stale fixtures itself)
$K -n omenative-eval get basemodel,servingruntime,inferenceservice,pvc,job,pod -l ome.io/rc-smoke=ome-rc-daily-smoke
$K -n omenative-eval get inferenceservice,inferencereplicas,pods -l ome.io/inferenceservice=ome-rc-tpu-smoke
$K -n omenative-eval get inferenceservice,inferencereplicas,pods -l ome.io/inferenceservice=ome-rc-tpu-smoke-ome-scheduler

# TPU capacity when a gang sits Pending
$K -n omenative-eval get events --sort-by=.lastTimestamp | tail -50
$K get nodes -l cloud.google.com/gke-tpu-topology=2x2x2 -o wide

# Prerequisites the TPU smoke asserts
$K -n omenative-eval get servingruntime ome-rc-tpu-smoke configmap gcs-rapid-reader
```

## Do not

- Apply, delete or patch anything in `omenative-eval` or `ome`. The smoke
  owns its fixtures and cleans them; a stale fixture is removed by the next
  run's "remove any stale fixed-name fixture" step.
- Bump, sync or roll back the RC release. The candidate must be observed, not
  arranged.
- Copy the RC kubeconfig into `~/.kube/config` or merge contexts.
- Run anything that creates a local cluster (`kind`, `k3d`, `minikube`) with
  the RC kubeconfig exported.
