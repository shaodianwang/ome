// Package inferenceservice hosts integration tests for the
// InferenceService reconciler driven against envtest via the shared
// framework. Phase 2 covers RawDeployment + VirtualDeployment paths
// with no optional CRDs installed (Knative / LWS / KEDA / Ray / Istio
// arrive in Phase 3 alongside their respective mode tests).
package inferenceservice

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/tests/integration/components"
	"github.com/sgl-project/ome/tests/integration/framework"
	"github.com/sgl-project/ome/tests/integration/util"
)

// TestInferenceServiceController is the Go entrypoint Ginkgo hooks
// into. Other files in this package contribute Describes registered
// at package init via `var _ = ginkgo.Describe(...)`.
func TestInferenceServiceController(t *testing.T) {
	util.RunSuite(t, "InferenceService Controller Integration Suite")
}

// Suite-wide state populated by BeforeSuite, used by every Describe.
var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	// Phase 3 lights up optional bundles so each deployment mode's
	// spec runs in the same suite. Bundles are mode-orthogonal:
	// installing Knative + LWS does not change RawDeployment behavior
	// (annotation + MinReplicas + Leader/Worker presence are the
	// controller-side mode triggers).
	s, err := framework.NewSuite(&framework.Framework{
		OptionalCRDs: []framework.OptionalCRDBundle{
			framework.Knative,
			framework.LWS,
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	ginkgo.By("seeding inferenceservice-config + benchmarkjob-config so the reconciler can read them at reconcile time")
	gomega.Expect(util.SeedDefaultConfigMaps(suiteCtx, k8sClient)).To(gomega.Succeed())

	ginkgo.By("registering BaseModel + InferenceService reconcilers (no optional CRDs in P2)")
	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		// SetupBaseModel without WithOmeAgentConfig is fine here —
		// the ISVC tests don't use PVC-backed models.
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
