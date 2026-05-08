# Model Cache Samples

These samples show the operator-facing pieces for using a cluster-wide model
cache provider with OME.

Provider selection is controller configuration. Models opt in independently by
setting `spec.distribution: Sharded` on a `BaseModel` or `ClusterBaseModel`.
Engines that serve sharded models must load weights through a compatible
provider data-plane integration.

Install OME with the sample values:

```sh
helm upgrade --install ome charts/ome-resources \
  --namespace ome \
  --create-namespace \
  -f config/samples/modelcache/cluster-cache-values.yaml
```

Then apply a sharded model:

```sh
kubectl apply -f config/samples/modelcache/sharded-clusterbasemodel.yaml
```

The controller will check the configured provider for source reachability,
submit a prewarm operation, and update `status.cache` plus the
`SourceReachable`, `Cached`, and `Ready` conditions.
