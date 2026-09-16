// Package autoscalerpolicy hosts integration tests for the AutoscalerPolicy
// status controller: Ready/InUse conditions, the portable spec digest, and
// the attached-component count, driven end-to-end through a real apiserver
// with the consumer-side watch fan-out doing the re-enqueueing.
//
// Only the status controller runs here. The render/resolve path a consuming
// InferenceService takes lives in the InferenceService reconciler and is
// covered by that package's unit tests; booting the full reconciler in this
// leaf would drag in its entire dependency surface to test none of it.
package autoscalerpolicy

import (
	"context"
	"fmt"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	policycontroller "github.com/ome-projects/ome/pkg/controller/v1beta1/autoscalerpolicy"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

func TestAutoscalerPolicyStatusController(t *testing.T) {
	util.RunSuite(t, "AutoscalerPolicy Status Controller Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

// boundProvider is the logical metric-provider name the seeded operator
// config binds; policies referencing anything else go ProviderUnknown.
const boundProvider = "llm-metrics"

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	// The operator config must exist before the manager starts: a policy
	// referencing a provider reconciles against it, and the controller
	// treats a missing ConfigMap as transient (error-requeue) rather than
	// a condition.
	seedOperatorConfig()

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		// The consumers-by-policy field index is owned by the
		// InferenceService controller in production; with only the status
		// controller booted, the suite registers the same extractor.
		if err := mgr.GetFieldIndexer().IndexField(ctx, &v1beta1.InferenceService{},
			"spec.autoscalerPolicyRefs", policyRefIndexer); err != nil {
			return fmt.Errorf("register autoscalerPolicyRefs index: %w", err)
		}
		clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
		if err != nil {
			return fmt.Errorf("build clientset: %w", err)
		}
		// TTL 0 disables config caching so a spec's ConfigMap edit is
		// visible to the very next reconcile.
		return policycontroller.SetupWithManager(mgr, clientset, controllerconfig.NewConfigCache(0))
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())
})

var _ = ginkgo.AfterSuite(func() {
	if suiteCancel != nil {
		suiteCancel()
	}
	if suiteFwk != nil {
		_ = suiteFwk.StopManager(context.Background())
		_ = suiteFwk.Teardown()
	}
})

// policyRefIndexer mirrors the InferenceService controller's extractor:
// distinct referenced policy names across engine, decoder, and router.
func policyRefIndexer(obj client.Object) []string {
	isvc, ok := obj.(*v1beta1.InferenceService)
	if !ok {
		return nil
	}
	seen := map[string]bool{}
	var names []string
	for _, componentType := range []v1beta1.ComponentType{
		v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent,
	} {
		ref := autoscaler.ComponentPolicyRef(isvc, componentType)
		if ref == nil || ref.Name == "" || seen[ref.Name] {
			continue
		}
		seen[ref.Name] = true
		names = append(names, ref.Name)
	}
	return names
}

// seedOperatorConfig creates the OME namespace and the operator ConfigMap
// with one bound metric provider. Specs that exercise the binding flip
// update this ConfigMap in place.
func seedOperatorConfig() {
	ginkgo.GinkgoHelper()
	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: constants.OMENamespace},
	})).To(gomega.Succeed())
	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.InferenceServiceConfigMapName,
			Namespace: constants.OMENamespace,
		},
		Data: map[string]string{
			controllerconfig.AutoscalerPolicyConfigName: fmt.Sprintf(
				`{"metricProviders":{%q:{"serverAddress":"http://prometheus.example.com:9090"}}}`,
				boundProvider),
			controllerconfig.OMENativeStatusConfigName: util.OMENativeStatusConfigJSON(),
		},
	})).To(gomega.Succeed())
}

// newTestNamespace gives each spec its own namespace so consumer counts
// never leak across specs (attachment is namespace-scoped by design).
func newTestNamespace() string {
	ginkgo.GinkgoHelper()
	ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "aspolicy-")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return ns.Name
}

func kedaSpec(provider string) v1beta1.AutoscalerPolicySpec {
	return v1beta1.AutoscalerPolicySpec{
		Class: v1beta1.AutoscalerKEDA,
		Keda: &v1beta1.KedaPolicyTemplate{
			Triggers: []v1beta1.KedaTriggerTemplate{{
				Type:        "prometheus",
				ProviderRef: &v1beta1.MetricProviderRef{Name: provider},
				Metadata: map[string]string{
					"query":            `sum(rate(http_requests_total{namespace="{{ .Namespace }}"}[1m]))`,
					"threshold":        "10",
					"ignoreNullValues": "false",
				},
			}},
		},
	}
}

func mkPolicy(namespace, name string, spec v1beta1.AutoscalerPolicySpec) *v1beta1.AutoscalerPolicy {
	return &v1beta1.AutoscalerPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       spec,
	}
}

// mkConsumer builds an InferenceService whose named components carry a
// policy ref. No webhook runs in this suite, so the spec needs nothing
// beyond the component blocks the ref lives on.
func mkConsumer(namespace, name string, engineRef, decoderRef string) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
	}
	if engineRef != "" {
		isvc.Spec.Engine = &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				AutoscalerPolicyRef: &v1beta1.AutoscalerPolicyRef{Name: engineRef},
			},
		}
	}
	if decoderRef != "" {
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				AutoscalerPolicyRef: &v1beta1.AutoscalerPolicyRef{Name: decoderRef},
			},
		}
	}
	return isvc
}
