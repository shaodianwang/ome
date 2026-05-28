# Scheduler Plugin Setup

This directory contains the local Minikube scheduler-plugins setup used to run
`PodGroupDeployment` workloads through `default-scheduler` with Coscheduling
enabled.

It installs:

- scheduler-plugins PodGroup and ElasticQuota CRDs from upstream
- scheduler-plugins controller RBAC and Deployment
- a scheduler config enabling the `Coscheduling` plugin on `default-scheduler`
- a replacement static `kube-scheduler` Pod manifest for the Minikube control plane

Run from the repository root:

```bash
./scheduler-plugin/install-minikube.sh
```

The script backs up the existing Minikube scheduler manifest to:

```text
/etc/kubernetes/kube-scheduler.yaml.backup-podgroup-operator
```

Verify:

```bash
kubectl -n kube-system get pod kube-scheduler-podgroup-operator \
  -o jsonpath='{.spec.containers[0].image}{"\n"}'
kubectl -n scheduler-plugins get pods
```

