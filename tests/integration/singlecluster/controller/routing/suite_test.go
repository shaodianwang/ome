// Package routing hosts integration tests for the TrafficMap routing
// controller driven against envtest via the shared framework. The controller
// projects a Placed InferenceService into a capacity-aware, health-gated
// TrafficMap; these specs exercise that projection end-to-end through a real
// apiserver and a real manager watch, whereas the package unit tests use a fake
// client. Only the routing controller is wired, so the specs own the ISVC
// status the controller reads and nothing else writes it.
package routing

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

func TestRoutingController(t *testing.T) {
	util.RunSuite(t, "TrafficMap Routing Controller Integration Suite")
}

// Suite-wide state. Populated by BeforeSuite, used by each Describe.
var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	suiteCfg    *rest.Config
	k8sClient   client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	suiteCfg = s.Cfg
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	mgrSetup := func(_ context.Context, mgr ctrl.Manager) error {
		// Generation on: the controller must produce TrafficMaps for Placed
		// ISVCs so the specs can assert the projection. Reap-on-unplace is the
		// same controller's disabled/not-routable path, exercised by unplacing an
		// ISVC rather than by toggling the feature.
		return (&routing.Reconciler{
			Client: mgr.GetClient(),
			Log:    ctrl.Log.WithName("PlacementRouting"),
			Config: routing.Config{Enabled: true},
		}).SetupWithManager(mgr)
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
