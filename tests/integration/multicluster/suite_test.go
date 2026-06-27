// Package multicluster is the multi-cluster integration suite. It
// stands up one control-plane envtest instance and two member envtest instances
// IN A SINGLE PROCESS by instantiating the shared tests/integration/framework
// three times, then exercises the placement (fan-out) and WorkloadCluster
// (registry/transport) controllers end-to-end across the real apiservers.
//
// Modeled on Kueue's test/integration/multikueue suite_test.go: a cluster
// struct per envtest instance, a createCluster helper that mutex-guards the
// (scheme-mutating, therefore non-thread-safe) framework Init/SetupClient, and a
// BeforeSuite that brings the clusters up.
package multicluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Poll cadences for this suite. envtest apiservers answer in ms, but the
// placement race is poll-driven (a status refresh every requeue interval) and
// fans out across three apiservers, so the assertion timeout is generous while
// the requeue/health intervals are tight enough that a happy-path race resolves
// within a couple of poll cycles.
const (
	// placementRequeue / clusterHealthInterval drive the control-plane
	// reconcilers fast so a fan-out, a winner flip, or a reachability change is
	// observed within a few poll cycles rather than the production cadence.
	placementRequeue      = 500 * time.Millisecond
	clusterHealthInterval = 500 * time.Millisecond

	// statusBatchPeriod debounces the cross-cluster status funnel's events. Small
	// so a derived-status change on a member enqueues a source reconcile promptly;
	// status then converges in well under a second via the funnel (the happy path
	// these specs primarily exercise).
	statusBatchPeriod = 100 * time.Millisecond

	// statusSafetyRequeue is the backstop re-read cadence behind the funnel. The
	// funnel drives fast event-based convergence; this requeue only matters when an
	// event is missed. It is set short here (production sets it to minutes) because
	// envtest runs three apiservers in one process, where a single watch event —
	// notably a derived's DELETE on a winner — can be delayed enough under CPU
	// contention that an events-only assertion is non-deterministic. With the
	// backstop the re-place spec resolves within a few seconds whether the delete
	// arrives as an event or is caught by the next backstop poll, while the status
	// specs still converge sub-second on the event.
	statusSafetyRequeue = 2 * time.Second

	// winnerLostGrace is the test grace before the placer re-places after the
	// sticky winner's derived is lost. Far below the production default (1m) so the
	// re-place spec resolves inside the Eventually window; the grace self-requeues
	// at placementRequeue once the loss is observed (by a funnel delete event or
	// the safety-requeue backstop).
	winnerLostGrace = 2 * time.Second

	// gcInterval is the placement orphan-sweep cadence. Short so the GC spec
	// observes a reap within the Eventually window (production default is minutes).
	// Harmless to the other specs: their deriveds have live sources, so the sweep
	// never classifies them as orphans.
	gcInterval = 1 * time.Second

	// eventuallyTimeout / eventuallyPoll bound the Gomega Eventually assertions.
	// Sized for a cross-apiserver poll loop, not envtest microseconds.
	eventuallyTimeout = 30 * time.Second
	eventuallyPoll    = 200 * time.Millisecond

	// controlPlaneID is this suite's control-plane identity, stamped onto every
	// derived ISVC. A fixed test value (config-driven in production via
	// --placement-control-plane-id); the suite asserts the stamp lands on the
	// derived copies the members receive.
	controlPlaneID = "mc-envtest-cp"
)

// cluster bundles one envtest instance: its framework, rest.Config, a
// non-cached client, and the context its manager (if any) runs under.
type cluster struct {
	fwk      *framework.Framework
	cfg      *rest.Config
	client   client.Client
	ctx      context.Context
	clusters *components.ClusterManager // set on the control plane only
}

func (c *cluster) teardown() {
	if c.fwk == nil {
		return
	}
	_ = c.fwk.StopManager(context.Background())
	_ = c.fwk.Teardown()
}

var (
	controlCluster cluster
	member1Cluster cluster
	member2Cluster cluster

	// schemeMu serializes framework Init + SetupClient across the concurrent
	// cluster bring-up. framework.Init mutates the process-global client-go
	// scheme (registering OME types on the apiserver scheme), which is not
	// thread-safe — the same race Kueue's createCluster guards with a mutex.
	// It guards Init-vs-Init; the control-plane MANAGER (whose cache informers
	// also read the scheme) is started only after every Init has completed (see
	// BeforeSuite), so a cache read never races a sibling cluster's Init write.
	schemeMu sync.Mutex
)

func TestMultiCluster(t *testing.T) {
	util.RunSuite(t, "Multi-Cluster Integration Suite")
}

// newFramework builds a framework for one envtest instance. Every instance
// installs the OME CRDs (the framework default) so a derived InferenceService
// and a WorkloadCluster can be created on it. No optional-dep CRDs and no
// webhooks are needed: the placer is the only writer, and the apiserver accepts
// the raw derived spec without the OME admission chain.
func newFramework() *framework.Framework {
	return &framework.Framework{}
}

// createCluster stands up one envtest instance: framework.Init (envtest apiserver
// + scheme registration) and SetupClient, mutex-guarded because Init mutates the
// process-global scheme. It does NOT start a manager — the control-plane manager
// is started separately by startControlPlaneManager once every cluster's Init has
// completed, so a manager cache informer reading the scheme never races a sibling
// cluster's Init writing it.
func createCluster() cluster {
	c := cluster{fwk: newFramework()}

	schemeMu.Lock()
	cfg, err := c.fwk.Init()
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "framework.Init")
	ctx, cl, err := c.fwk.SetupClient(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "framework.SetupClient")
	schemeMu.Unlock()

	c.cfg = cfg
	c.ctx = ctx
	c.client = cl
	return c
}

// startControlPlaneManager wires + starts the control-plane manager (the
// placement + WorkloadCluster controllers, the origin-scoped derived-ISVC cache,
// and the cross-cluster status funnel) and captures the shared cluster Manager so
// specs can inspect the live connections. It MUST be called only after every
// cluster's createCluster has returned: the scheme is then fully registered and
// stable, so the manager's cache informers read it without racing an Init.
func startControlPlaneManager(c *cluster) {
	var cm *components.ClusterManager
	err := c.fwk.StartManager(c.ctx, c.cfg, func(setupCtx context.Context, mgr ctrl.Manager) error {
		m, err := components.SetupWorkloadCluster(setupCtx, mgr,
			components.WithClusterHealthInterval(clusterHealthInterval),
			components.WithDerivedISVCCache(controlPlaneID))
		if err != nil {
			return err
		}
		cm = m
		if err := components.SetupPlacement(setupCtx, mgr, m,
			components.WithPlacementRequeue(placementRequeue),
			components.WithControlPlaneID(controlPlaneID),
			components.WithStatusFunnel(controlPlaneID),
			components.WithStatusBatchPeriod(statusBatchPeriod),
			components.WithStatusSafetyRequeue(statusSafetyRequeue),
			components.WithWinnerLostGracePeriod(winnerLostGrace)); err != nil {
			return err
		}
		return components.SetupPlacementGC(setupCtx, mgr, m, controlPlaneID, gcInterval)
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "control-plane StartManager")
	c.clusters = cm
}

var _ = ginkgo.BeforeSuite(func() {
	// Install the shared Eventually timeout/poll defaults so the bare-default
	// helpers (CreateAndWaitReady) inherit the suite's cross-apiserver window.
	ometesting.SetDefaultEventually()
	ginkgo.By("standing up the control-plane and member envtest clusters", func() {
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer ginkgo.GinkgoRecover()
			defer wg.Done()
			controlCluster = createCluster()
		}()
		go func() {
			defer ginkgo.GinkgoRecover()
			defer wg.Done()
			member1Cluster = createCluster()
		}()
		go func() {
			defer ginkgo.GinkgoRecover()
			defer wg.Done()
			member2Cluster = createCluster()
		}()
		wg.Wait()
	})
	// Every cluster's scheme registration (framework.Init) is now complete and the
	// scheme is stable. Only now start the control-plane manager, so its cache
	// informers read the scheme without racing a sibling cluster's Init write.
	ginkgo.By("starting the control-plane manager", func() {
		startControlPlaneManager(&controlCluster)
	})
})

var _ = ginkgo.AfterSuite(func() {
	controlCluster.teardown()
	member1Cluster.teardown()
	member2Cluster.teardown()
})
