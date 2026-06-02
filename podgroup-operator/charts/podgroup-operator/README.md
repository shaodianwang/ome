# podgroup-operator Helm chart

This chart installs the `podgroup-operator` CRDs, RBAC, and controller
Deployment. It assumes scheduler-plugins `scheduling.x-k8s.io/v1alpha1`
`PodGroup` and Coscheduling are already installed in the target cluster.
By default it also grants scheduler PodGroup RBAC to `system:kube-scheduler`
and `system:kube-scheduler:coscheduler`.

The default image repository points at staging ECR:

```text
588845226011.dkr.ecr.us-east-2.amazonaws.com/msl_infra/podgroup-operator
```

Build and push an image manually:

```bash
cd podgroup-operator
TAG=$(git rev-parse --short HEAD)
IMAGE=588845226011.dkr.ecr.us-east-2.amazonaws.com/msl_infra/podgroup-operator:${TAG}
docker buildx build --platform linux/amd64,linux/arm64 -t "${IMAGE}" --push .
```

Render for Argo or local inspection:

```bash
helm template podgroup-operator ./charts/podgroup-operator \
  --namespace podgroup-system \
  --set image.tag="${TAG}"
```

Useful production settings:

```yaml
leaderElection:
  enabled: true
metrics:
  bindAddress: ":8080"
healthProbe:
  bindAddress: ":8081"
```
