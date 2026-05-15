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
)

// SeedDefaultConfigMaps creates the two ConfigMaps the controllers
// read at reconcile time:
//
//   - inferenceservice-config — keys: ingress, deploy, omeAgent. The
//     ISVC reconciler reads ingress + deploy; the BaseModel PVC path
//     reads omeAgent.
//   - benchmarkjob-config — key: benchmarkjob. Read by the BenchmarkJob
//     reconciler.
//
// Values are minimal-viable defaults that satisfy each helper's
// validation:
//   - ingress carries non-empty IngressGateway + IngressServiceName
//     (NewIngressConfig errors otherwise) and DisableIngressCreation=true
//     to keep the controller from materializing routes
//   - deploy.defaultDeploymentMode = RawDeployment (the safe default;
//     NewDeployConfig errors on empty)
//   - omeAgent gets a minimal image so the helpers don't panic on nil
//     dereferences when a test doesn't override
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
  "ingressGateway": "knative-serving/knative-ingress-gateway",
  "ingressService": "istio-ingressgateway.istio-system.svc.cluster.local",
  "ingressDomain": "svc.cluster.local",
  "domainTemplate": "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}",
  "urlScheme": "http",
  "disableIstioVirtualHost": false,
  "disableIngressCreation": true
}`,
		"deploy": `{
  "defaultDeploymentMode": "RawDeployment"
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
	}); err != nil {
		return fmt.Errorf("seed inferenceservice-config: %w", err)
	}

	if err := createConfigMapIfAbsent(ctx, c, constants.BenchmarkJobConfigMapName, map[string]string{
		"benchmarkjob": `{
  "podConfig": {
    "image": "ghcr.io/test/genai-bench:test",
    "cpuRequest": "1",
    "memoryRequest": "1Gi",
    "cpuLimit": "1",
    "memoryLimit": "1Gi"
  }
}`,
	}); err != nil {
		return fmt.Errorf("seed benchmarkjob-config: %w", err)
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
