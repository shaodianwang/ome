// Package inferenceservicenokeda boots the InferenceService controller with
// the KEDA CRD absent, guarding the conditional ScaledObject watch in
// SetupWithManager. The old unconditional watch made StartManager hang on the
// ScaledObject cache sync (crash-looping the controller for every ISVC) when
// keda.sh/v1alpha1 is absent; the fix lets it start clean. All other optional
// bundles are installed so KEDA is the only variable.
package inferenceservicenokeda

import (
	"context"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	mcfake "github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded/fake"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// TestInferenceServiceControllerNoKEDA is the Go entrypoint Ginkgo hooks into.
func TestInferenceServiceControllerNoKEDA(t *testing.T) {
	util.RunSuite(t, "InferenceService Controller No-KEDA Regression Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	// framework.KEDA is deliberately omitted; the other watched CRDs stay so
	// KEDA absence is the only thing that could fail cache sync.
	s, err := framework.NewSuite(&framework.Framework{
		OptionalCRDs: []framework.OptionalCRDBundle{
			framework.EnvoyGateway,
			framework.GatewayAPI,
			framework.PrometheusOperator,
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	gomega.Expect(util.SeedDefaultConfigMaps(suiteCtx, k8sClient)).To(gomega.Succeed())

	// Benign provider to satisfy reconciler wiring; no ISVC is reconciled here.
	provider := sharded.NewProvider(
		sharded.ProviderClusterCache,
		&mcfake.Client{},
		sharded.ProviderConfig{
			Provider: sharded.ProviderClusterCache,
			Endpoint: "http://cluster-cache.clustercache.svc.cluster.local:10090",
		},
	)

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		// Scheme registered but CRD not installed, mirroring production
		// (scheme always registered, CRD may be absent) — the exact condition
		// that used to hang cache sync. A regression re-fails at StartManager.
		if err := kedav1.AddToScheme(mgr.GetScheme()); err != nil {
			return err
		}
		return components.SetupInferenceService(ctx, mgr,
			components.WithISVCModelCacheProvider(provider))
	}

	// Regression assertion: manager starts clean with KEDA absent.
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

var _ = ginkgo.Describe("InferenceService controller with the KEDA CRD absent", func() {
	ginkgo.It("has a live, cache-synced manager (proves startup did not hang on ScaledObject)", func() {
		// Reachable only if BeforeSuite's StartManager succeeded; the read
		// confirms the ISVC cache synced and the manager is serving.
		list := &v1beta1.InferenceServiceList{}
		gomega.Expect(k8sClient.List(suiteCtx, list)).To(gomega.Succeed())
	})
})
