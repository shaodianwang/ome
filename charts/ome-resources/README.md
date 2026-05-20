# ome-resources

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 1.16.0](https://img.shields.io/badge/AppVersion-1.16.0-informational?style=flat-square)

OME Resources and Controller

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| global.imagePullSecrets | list | `[]` |  |
| global.hub | string | `"ghcr.io/moirai-internal"` |  |
| modelAgent.health.port | int | `8080` |  |
| modelAgent.hostPath | string | `"/mnt/data/models"` |  |
| modelAgent.image.pullPolicy | string | `"Always"` |  |
| modelAgent.image.repository | string | `"model-agent"` |  |
| modelAgent.image.tag | string | `"v0.1.2"` |  |
| modelAgent.nodeSelector | object | `{}` |  |
| modelAgent.extraVolumeMounts | list | `[]` |  |
| modelAgent.extraVolumes | list | `[]` |  |
| modelAgent.priorityClassName | string | `"system-node-critical"` |  |
| modelAgent.resources.limits.cpu | string | `"10"` |  |
| modelAgent.resources.limits.memory | string | `"100Gi"` |  |
| modelAgent.resources.requests.cpu | string | `"10"` |  |
| modelAgent.resources.requests.memory | string | `"100Gi"` |  |
| modelAgent.serviceAccountName | string | `"ome-model-agent"` |  |
| modelAgent.tolerations | list | `[{"key":"nvidia.com/gpu","operator":"Exists","effect":"NoSchedule"}]` |  |
| ome.controller.affinity | object | `{}` |  |
| ome.controller.deploymentMode | string | `"RawDeployment"` |  |
| ome.controller.image | string | `"ome-manager"` |  |
| ome.controller.ingressGateway.additionalIngressDomains | string | `nil` |  |
| ome.controller.ingressGateway.disableIngressCreation | bool | `true` |  |
| ome.controller.ingressGateway.domain | string | `"svc.cluster.local"` |  |
| ome.controller.ingressGateway.domainTemplate | string | `"{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}"` |  |
| ome.controller.ingressGateway.enableGatewayAPI | bool | `false` |  |
| ome.controller.ingressGateway.ingressGateway.className | string | `"istio"` |  |
| ome.controller.ingressGateway.ingressGateway.gateway | string | `"istio-system/ingress-gateway"` |  |
| ome.controller.ingressGateway.ingressGateway.gatewayService | string | `"istio-ingressgateway.istio-system.svc.cluster.local"` |  |
| ome.controller.ingressGateway.omeIngressGateway | string | `""` |  |
| ome.controller.ingressGateway.pathTemplate | string | `""` |  |
| ome.controller.ingressGateway.urlScheme | string | `"http"` |  |
| ome.controller.nodeSelector | object | `{}` |  |
| ome.controller.replicaCount | int | `3` |  |
| ome.controller.resources.limits.cpu | int | `2` |  |
| ome.controller.resources.limits.memory | string | `"4Gi"` |  |
| ome.controller.resources.requests.cpu | int | `2` |  |
| ome.controller.resources.requests.memory | string | `"4Gi"` |  |
| ome.controller.tag | string | `"v0.1.2"` |  |
| ome.controller.tolerations | list | `[]` |  |
| ome.controller.topologySpreadConstraints | list | `[]` |  |
| ome.kedaConfig.customPromQuery | string | `""` |  |
| ome.kedaConfig.enableKeda | bool | `true` |  |
| ome.kedaConfig.promServerAddress | string | `"http://prometheus-operated.monitoring.svc.cluster.local:9090"` |  |
| ome.kedaConfig.scalingOperator | string | `"GreaterThanOrEqual"` |  |
| ome.kedaConfig.scalingThreshold | string | `"10"` |  |
| ome.metricsaggregator.enableMetricAggregation | string | `"false"` |  |
| ome.metricsaggregator.enablePrometheusScraping | string | `"false"` |  |
| ome.modelCache.endpoint | string | `""` | HTTP endpoint for the optional model cache provider control-plane API. |
| ome.modelCache.options | object | `{}` | Provider-specific options. For `cluster_cache`, supported keys include `timeout` and `operation_retention`. |
| ome.modelCache.provider | string | `""` | Optional model cache provider name. Use `cluster_cache` for sharded model distribution through cluster_cache. |
| ome.omeAgent.authType | string | `"InstancePrincipal"` |  |
| ome.omeAgent.compartmentId | string | `"ocid1.compartment.oc1..dummy-compartment"` |  |
| ome.omeAgent.fineTunedAdapter.cpuLimit | int | `15` |  |
| ome.omeAgent.fineTunedAdapter.cpuRequest | int | `15` |  |
| ome.omeAgent.fineTunedAdapter.memoryLimit | string | `"320Gi"` |  |
| ome.omeAgent.fineTunedAdapter.memoryRequest | string | `"300Gi"` |  |
| ome.omeAgent.image | string | `"ome-agent"` |  |
| ome.omeAgent.modelInit.cpuLimit | int | `15` |  |
| ome.omeAgent.modelInit.cpuRequest | int | `15` |  |
| ome.omeAgent.modelInit.memoryLimit | string | `"180Gi"` |  |
| ome.omeAgent.modelInit.memoryRequest | string | `"150Gi"` |  |
| ome.omeAgent.region | string | `"ap-osaka-1"` |  |
| ome.omeAgent.tag | string | `"v0.1.2"` |  |
| ome.omeAgent.vaultId | string | `"ocid1.vault.oc1.ap-osaka-1.dummy.dummy-vault"` |  |
| ome.version | string | `"v0.1.2"` |  |
