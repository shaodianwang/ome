// Package routing hosts integration tests for the TrafficMap routing
// controller driven against envtest via the shared framework. The controller
// projects a Placed InferenceService into a capacity-aware, health-gated
// TrafficMap; these specs exercise that projection end-to-end through a real
// apiserver and real manager watches, whereas the package unit tests use a fake
// client. The routing controller and the disabled legacy endpoint cleanup path
// are wired, so the specs own the ISVC status the routing controller reads.
package routing

import (
	"context"
	"sync"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

func TestRoutingController(t *testing.T) {
	util.RunSuite(t, "TrafficMap Routing Controller Integration Suite")
}

// Suite-wide state. Populated by BeforeSuite, used by each Describe.
var (
	suiteCtx     context.Context
	suiteCancel  context.CancelFunc
	suiteFwk     *framework.Framework
	suiteCfg     *rest.Config
	k8sClient    client.Client
	handoffReads *trafficMapReadObserver
)

type trafficMapReadObserver struct {
	client.Reader
	mu       sync.Mutex
	key      types.NamespacedName
	observed chan struct{}
}

func (r *trafficMapReadObserver) arm(key types.NamespacedName) <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.key = key
	r.observed = make(chan struct{}, 1)
	return r.observed
}

func (r *trafficMapReadObserver) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	opts ...client.GetOption,
) error {
	err := r.Reader.Get(ctx, key, object, opts...)
	if err != nil {
		return err
	}
	if _, ok := object.(*v1beta1.TrafficMap); !ok {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if key == r.key && r.observed != nil {
		select {
		case r.observed <- struct{}{}:
		default:
		}
	}
	return nil
}

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{
		OptionalCRDs: []framework.OptionalCRDBundle{framework.GatewayAPI},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	suiteCfg = s.Cfg
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	mgrSetup := func(_ context.Context, mgr ctrl.Manager) error {
		handoffReads = &trafficMapReadObserver{Reader: mgr.GetAPIReader()}
		legacyPublisher := placementendpoint.NewGatewayAPIPublisher(
			mgr.GetClient(),
			placementendpoint.Config{},
			placementendpoint.WithGatewayAPIReader(handoffReads),
		)
		if err := (&placementendpoint.Reconciler{
			Client:    mgr.GetClient(),
			APIReader: handoffReads,
			Log:       ctrl.Log.WithName("PlacementEndpoint"),
			Publisher: legacyPublisher,
			Config:    placementendpoint.Config{},
		}).SetupWithManager(mgr); err != nil {
			return err
		}
		// Generation on: the controller must produce TrafficMaps for Placed
		// ISVCs so the specs can assert the projection. A placement-status
		// transition out of Placed empties the map and records why on Routable.
		return (&routing.Reconciler{
			Client:    mgr.GetClient(),
			APIReader: mgr.GetAPIReader(),
			Log:       ctrl.Log.WithName("PlacementRouting"),
			Config:    routing.Config{Enabled: true},
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
