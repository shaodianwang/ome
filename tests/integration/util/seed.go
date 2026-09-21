package util

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus"
)

// The seeded omenativeStatus block: the per-Instance representation the
// seeded manager targets and the ColumnarV2 row bound it decodes under.
// Both match the chart default.
const (
	SeededInstanceStatusEncoding        = irstatus.EncodingColumnarV2
	SeededMaxDecodedInstances    uint64 = 20000
)

// OMENativeStatusConfigJSON returns the omenativeStatus block that every
// seeded inferenceservice-config carries. Suites that build the ConfigMap
// themselves include it under controllerconfig.OMENativeStatusConfigName.
func OMENativeStatusConfigJSON() string {
	return fmt.Sprintf(`{"instanceStatusEncoding":%q,"maxDecodedInstances":%d}`,
		SeededInstanceStatusEncoding, SeededMaxDecodedInstances)
}

// SeedDefaultConfigMaps creates the ConfigMap the controllers read at
// reconcile time:
//
//   - inferenceservice-config — keys: ingress, deploy, lifecycle, omeAgent,
//     rollout, omenativeStatus. The ISVC reconciler reads ingress + deploy +
//     rollout; the BaseModel PVC path reads omeAgent; the InferenceReplica
//     reconciler reads lifecycle and omenativeStatus; the manager binary
//     requires omenativeStatus at startup (ColumnarV2 with the chart's
//     bound here, matching the chart default).
//
// Values are minimal-viable defaults that satisfy each helper's
// validation:
//   - ingress carries non-empty IngressGateway + IngressServiceName
//     (NewIngressConfig errors otherwise) and DisableIngressCreation=true
//     to keep the controller from materializing routes
//   - deploy.defaultDeploymentMode = RawDeployment (the safe default;
//     NewDeployConfig errors on empty), plus the deploy.replicas and
//     deploy.updateStrategy admission defaults (the values the chart ships)
//     so defaulter-dependent specs see chart-equivalent behavior. Without
//     the updateStrategy block an OMENative component is stamped with no
//     rollout budget at all, since the binary carries none
//   - lifecycle scaleUpPodBatchSize and scaleDownPodBatchSize bound OMENative
//     create and delete work in Pod-equivalent units while keeping each
//     Instance or gang atomic
//   - omeAgent gets a minimal image so the helpers don't panic on nil
//     dereferences when a test doesn't override
//   - rollout mirrors the chart defaults (pinned-plan size cap and the
//     canary capacity-gate defaultReadyTimeout). The escalation timeout is
//     config-driven with no in-code fallback, so without this key a canary
//     step waiting on capacity would never escalate to Failed; supplying
//     the chart's value keeps suite behavior chart-equivalent
//
// Idempotent: subsequent calls with the same desired state are no-ops.
// If a ConfigMap already exists with different data, this function
// does NOT overwrite — the assumption is that a per-test override was
// applied deliberately.
func SeedDefaultConfigMaps(ctx context.Context, c client.Client) error {
	if err := ensureNamespace(ctx, c, constants.OMENamespace); err != nil {
		return fmt.Errorf("ensure %s namespace: %w", constants.OMENamespace, err)
	}

	if err := createConfigMapIfAbsent(ctx, c, constants.InferenceServiceConfigMapName, map[string]string{
		"ingress": `{
  "ingressGateway": "istio-system/ingress-gateway",
  "ingressService": "istio-ingressgateway.istio-system.svc.cluster.local",
  "ingressDomain": "svc.cluster.local",
  "domainTemplate": "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}",
  "urlScheme": "http",
  "disableIstioVirtualHost": false,
  "disableIngressCreation": true
}`,
		"deploy": `{
  "defaultDeploymentMode": "RawDeployment",
  "replicas": {
    "defaultMinReplicas": 1,
    "defaultMaxReplicas": {
      "engine": 3,
      "decoder": 3,
      "router": 2
    }
  },
  "updateStrategy": {
    "router":  {"type": "SurgeThenDrain", "maxSurge": "25%", "maxUnavailable": "25%"},
    "engine":  {"type": "SurgeThenDrain", "maxSurge": "25%", "maxUnavailable": "25%"},
    "decoder": {"type": "SurgeThenDrain", "maxSurge": "25%", "maxUnavailable": "25%"}
  }
}`,
		"lifecycle": `{
  "scaleUpPodBatchSize": 100,
  "scaleDownPodBatchSize": 100,
  "revisionHistoryLimit": 10,
  "retryBlockHistoryLimit": 3,
  "instanceReadyTimeout": "30m",
  "audit": {"maxInFlightMigrations": 3, "maxMigrationsPerWindow": 10, "window": "1h"},
  "requeue": {"operation": "5s", "gate": "3s"},
  "gangScheduleTimeout": {"min": "60s", "max": "600s"}
}`,
		"omeAgent": `{
  "image": "ghcr.io/test/ome-agent:test",
  "serviceAccount": "ome-model-metadata",
  "memoryRequest": "256Mi",
  "memoryLimit": "512Mi",
  "cpuRequest": "100m",
  "cpuLimit": "500m",
  "backoffLimit": 2,
  "ttlSecondsAfterFinished": 3600
}`,
		"rollout": `{
  "maxPinnedPlanBytes": 16384,
  "defaultReadyTimeout": "15m"
}`,
		controllerconfig.OMENativeStatusConfigName: OMENativeStatusConfigJSON(),
	}); err != nil {
		return fmt.Errorf("seed inferenceservice-config: %w", err)
	}

	return nil
}

func ensureNamespace(ctx context.Context, c client.Client, name string) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	err := c.Create(ctx, ns)
	if err == nil || apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func createConfigMapIfAbsent(ctx context.Context, c client.Client, name string, data map[string]string) error {
	existing := &corev1.ConfigMap{}
	err := c.Get(ctx, types.NamespacedName{Namespace: constants.OMENamespace, Name: name}, existing)
	if err == nil {
		// Already there — assume a deliberate per-test override; do
		// not clobber. Tests that need a different shape should
		// Delete + Create themselves before SeedDefaultConfigMaps.
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: constants.OMENamespace},
		Data:       data,
	}
	if err := c.Create(ctx, cm); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}
