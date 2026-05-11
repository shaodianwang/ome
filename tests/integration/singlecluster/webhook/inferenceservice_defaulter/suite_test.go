// Package inferenceservice_defaulter hosts integration tests for
// the InferenceService MUTATING webhook (the defaulter). Sibling
// to webhook/inferenceservice/ which covers the validator. Split
// because the canonical manifests install them as separate
// webhook configurations and we want one suite per configuration —
// otherwise a stray Update through the validator-only suite would
// also trigger defaulter logic and confuse the assertion.
package inferenceservice_defaulter

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

func TestInferenceServiceDefaulterWebhook(t *testing.T) {
	util.RunSuite(t, "InferenceService Defaulter Webhook Integration Suite")
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
		WebhookPath: "tests/integration/singlecluster/webhook/inferenceservice_defaulter/testdata",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	// Defaulter reads inferenceservice-config from the OME
	// namespace via NewDeployConfig — without this seed, every
	// ISVC create returns a 500 from the webhook
	// (failurePolicy=Fail).
	ginkgo.By("seeding inferenceservice-config so the defaulter can read DeployConfig")
	gomega.Expect(util.SeedDefaultConfigMaps(suiteCtx, k8sClient)).To(gomega.Succeed())

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		// SetupInferenceServiceWebhook wires BOTH validator AND
		// defaulter on the same builder call (controller-runtime
		// requires it). The validator's effect is gated by the
		// installed webhook configuration — our testdata only
		// installs the mutating webhook, so validation errors from
		// the validator path are off-table for this suite.
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
