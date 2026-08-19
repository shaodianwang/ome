// Package inferenceservice_defaulted hosts integration tests that run
// the InferenceService MUTATING webhook (the defaulter) and the
// InferenceService reconciler together in one manager — the full
// create path a live cluster takes. The controller/inferenceservice
// suite deliberately runs without admission so it can pin reconciler
// behavior for stored (never-defaulted) objects; this suite pins the
// opposite contract: what the reconciler does with the DEFAULTED spec
// every admitted InferenceService actually carries.
package inferenceservice_defaulted

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// TestInferenceServiceDefaulted is the Go entrypoint Ginkgo hooks into.
func TestInferenceServiceDefaulted(t *testing.T) {
	util.RunSuite(t, "InferenceService Defaulted Reconcile Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{
		// Only the mutating webhook configuration is installed: the
		// validator handler is registered on the server (the builder
		// wires both) but never dialed, so specs exercise defaulting
		// without also re-testing validation.
		WebhookPath: "tests/integration/singlecluster/controller/inferenceservice_defaulted/testdata",
		OptionalCRDs: []framework.OptionalCRDBundle{
			// The ISVC RawDeployment path unconditionally reconciles a
			// PodMonitor — without this the SetControllerReference
			// call fails with "no kind is registered for the type v1.PodMonitor".
			framework.PrometheusOperator,
			// KEDA keeps the autoscaler dispatch's scheme complete so
			// per-class cleanup lookups resolve.
			framework.KEDA,
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	// Both ends of the chain read inferenceservice-config from the OME
	// namespace: the defaulter via NewDeployConfig (failurePolicy=Fail,
	// so a missing CM 500s every ISVC create) and the reconciler at
	// reconcile time.
	ginkgo.By("seeding inferenceservice-config for the defaulter and the reconciler")
	gomega.Expect(util.SeedDefaultConfigMaps(suiteCtx, k8sClient)).To(gomega.Succeed())

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		if err := components.SetupInferenceServiceWebhook(mgr); err != nil {
			return err
		}
		// BaseModel reconciler drives the CBM fixtures to Ready so the
		// ISVC reconciler's model resolution succeeds.
		if err := components.SetupBaseModel(ctx, mgr); err != nil {
			return err
		}
		return components.SetupInferenceService(ctx, mgr)
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
