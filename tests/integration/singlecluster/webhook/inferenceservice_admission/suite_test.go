// Package inferenceservice_admission hosts integration tests for the
// full InferenceService admission chain: the MUTATING webhook (the
// defaulter) AND the VALIDATING webhook installed together, exactly as
// the canonical manifests deploy them. The sibling suites
// webhook/inferenceservice (validator only) and
// webhook/inferenceservice_defaulter (defaulter only) each isolate a
// single handler; this suite pins the interaction matrix — what the
// validator sees is the DEFAULTED object, so create-vs-update ×
// defaulted-vs-explicit combinations behave differently than either
// handler does alone.
package inferenceservice_admission

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

func TestInferenceServiceAdmissionMatrix(t *testing.T) {
	util.RunSuite(t, "InferenceService Admission Matrix Integration Suite")
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
		WebhookPath: "tests/integration/singlecluster/webhook/inferenceservice_admission/testdata",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	// The defaulter reads inferenceservice-config from the OME
	// namespace via NewDeployConfig — without this seed, every ISVC
	// create returns a 500 from the webhook (failurePolicy=Fail).
	ginkgo.By("seeding inferenceservice-config so the defaulter can read DeployConfig")
	gomega.Expect(util.SeedDefaultConfigMaps(suiteCtx, k8sClient)).To(gomega.Succeed())

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		return components.SetupInferenceServiceWebhook(mgr)
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
