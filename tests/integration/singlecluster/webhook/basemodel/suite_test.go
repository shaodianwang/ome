// Package basemodel hosts integration tests for the BaseModel +
// ClusterBaseModel ValidatingWebhook handlers. Boots envtest with
// the testdata/webhooks.yaml fixture installed (NOT the full
// config/webhook/manifests.yaml) so we don't have to wire every OME
// webhook into this manager — just BaseModel + ClusterBaseModel.
package basemodel

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

func TestBaseModelWebhook(t *testing.T) {
	util.RunSuite(t, "BaseModel Webhook Integration Suite")
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
		// Suite-local fixture: only BaseModel + ClusterBaseModel
		// validators. Full manifests.yaml would require every other
		// OME webhook handler to be registered too, which this suite
		// doesn't need.
		WebhookPath: "tests/integration/singlecluster/webhook/basemodel/testdata",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		return components.SetupBaseModelWebhooks(mgr)
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
