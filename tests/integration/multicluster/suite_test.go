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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// realKueueEnvVar gates the full member-manager (ISVC + InferenceReplica +
// real Kueue) path. When set to "1" the members run real managers that drive
// derived-ISVC status via the real reconcile→Kueue gate chain, and the sim
// specs (which fake admission via admitDerivedOn) are skipped. When unset the
// suite behaves EXACTLY as before: bare members, sim path only.
const realKueueEnvVar = "OME_MC_REALKUEUE"

// realKueueEnabled reports whether the real-Kueue member-manager path is on.
func realKueueEnabled() bool {
	return os.Getenv(realKueueEnvVar) == "1"
}

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
	// quotaClusters is the management plane's own transport, wired beside
	// clusters so a spec can assert the two follow the registry independently.
	quotaClusters *components.ClusterManager // set on the control plane only
}

func (c *cluster) teardown() {
	if c.fwk == nil {
		return
	}
	_ = c.fwk.StopManager(context.Background())
	_ = c.fwk.Teardown()
}

const (
	// quotaOrigin marks every copy this hub writes, and is what its sweep
	// selects on. A distinct value from the placement control-plane ID: the two
	// planes mark their own work and must not reap each other's.
	quotaOrigin = "quota-hub"
	// quotaFieldManager owns the fields the projector applies on a member.
	quotaFieldManager = "ome-quota-projector"
)

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

// newFramework builds a framework for one envtest instance. By default every
// instance installs only the OME CRDs (the framework default) so a derived
// InferenceService and a WorkloadCluster can be created on it; no optional-dep
// CRDs and no webhooks are needed because in the sim path the placer is the only
// writer and the apiserver accepts the raw derived spec without admission.
//
// In the real-Kueue path the members ALSO need the Kueue CRD bundle (so the
// scheme + apiserver know the Kueue types) and the Kueue pod webhook installed
// (so StartKueue can gate queue-labeled pods). The control plane also installs
// Gateway API CRDs for routing publication specs.
func newFramework(withKueue, withGatewayAPI bool) *framework.Framework {
	f := &framework.Framework{}
	if withGatewayAPI {
		f.OptionalCRDs = append(f.OptionalCRDs, framework.GatewayAPI)
	}
	if withKueue {
		// The member runs the full ISVC + InferenceReplica controllers, which
		// unconditionally Owns(ScaledObject) (KEDA) and the ISVC path reconciles
		// a PodMonitor (PrometheusOperator). Without those schemes + CRDs the
		// controllers' caches fail to start the Owns informers ("no kind is
		// registered for v1alpha1.ScaledObject") and reconciles stall. Mirrors
		// the singlecluster ISVC suite + kindfix.OMEScheme/OMECRDPaths.
		f.OptionalCRDs = append(f.OptionalCRDs,
			framework.Kueue,
			framework.KEDA,
			framework.PrometheusOperator,
		)
		whDir, err := framework.KueueWebhookDir()
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "resolve Kueue webhook dir")
		f.ExtraWebhookPaths = []string{whDir}
		// The pod webhook gates Pending pods that never reach a kubelet, so a
		// stray gated pod can linger; do not treat that as a write conflict.
		f.SkipWriteConflictCheck = true
	}
	return f
}

// createCluster stands up one envtest instance: framework.Init (envtest apiserver
// + scheme registration) and SetupClient, mutex-guarded because Init mutates the
// process-global scheme. It does NOT start a manager — the control-plane manager
// is started separately by startControlPlaneManager once every cluster's Init has
// completed, so a manager cache informer reading the scheme never races a sibling
// cluster's Init writing it.
//
// withKueue installs the member-side Kueue CRDs + webhook (real-Kueue path only).
// withGatewayAPI installs the portable route types used by control-plane specs.
func createCluster(withKueue, withGatewayAPI bool) cluster {
	c := cluster{fwk: newFramework(withKueue, withGatewayAPI)}

	schemeMu.Lock()
	defer schemeMu.Unlock()
	cfg, err := c.fwk.Init()
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "framework.Init")
	ctx, cl, err := c.fwk.SetupClient(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "framework.SetupClient")

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
	// The control plane carries the same inferenceservice-config the manager
	// binary requires: placement decodes member InferenceReplica rows under
	// its omenativeStatus bound.
	gomega.Expect(util.SeedDefaultConfigMaps(c.ctx, c.client)).To(gomega.Succeed(), "seed control-plane inferenceservice-config")

	var cm, quotaCM *components.ClusterManager
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
		if err := components.SetupPlacementGC(setupCtx, mgr, m, controlPlaneID, gcInterval); err != nil {
			return err
		}
		// The quota plane's own transport, beside the placement one rather than
		// sharing it — the two run in separate binaries in production, and this
		// is what proves a second Manager can follow the same registry without
		// contending with the reconciler that owns it.
		qcm, err := components.SetupQuotaTransport(setupCtx, mgr)
		if err != nil {
			return err
		}
		quotaCM = qcm
		// The management half: split an authored fleet tree and write each
		// member's copy. Resync is long, so a spec that sees a projection
		// converge proves an event drove it rather than a tick.
		// Watching members is what keeps the fleet view current: consumption and
		// capacity both move without anything on the hub changing, and with a
		// ten-minute resync a spec that sees either land has been driven by an
		// event.
		memberEvents, err := components.SetupQuotaMemberFunnel(setupCtx, mgr, qcm)
		if err != nil {
			return err
		}
		return components.SetupQuotaProjection(setupCtx, mgr, qcm,
			quotaOrigin, quotaFieldManager, 10*time.Minute, memberEvents)
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "control-plane StartManager")
	c.clusters = cm
	c.quotaClusters = quotaCM
}

// startMemberManager wires + starts a member manager for the real-Kueue path:
// the InferenceService + InferenceReplica reconcilers (the OMENative path that
// turns a derived ISVC into queue-labeled pods) plus the full Kueue admission
// stack (StartKueue gates those pods and ungates them once quota admits). This
// is the real replacement for the sim-path admitDerivedOn fake: the member now
// flips InstanceStatus.Admitted itself, driven by real Kueue gate removal.
//
// Like startControlPlaneManager it must run only after every cluster's
// createCluster has returned (scheme stable). The reconcilers read
// inferenceservice-config during setup and reconciliation, so it is seeded first.
func startMemberManager(c *cluster) {
	gomega.Expect(util.SeedDefaultConfigMaps(c.ctx, c.client)).To(gomega.Succeed(), "seed member inferenceservice-config")
	err := c.fwk.StartManager(c.ctx, c.cfg, func(setupCtx context.Context, mgr ctrl.Manager) error {
		// The OMENative pod field index is a hard dependency of the IR
		// status-aggregation + ISVC coordination read paths (they List pods via
		// MatchingFields against the cached client). cmd/manager registers it
		// globally; the member manager must do the same or those reconciles fail
		// with "Index ... does not exist".
		if err := query.RegisterOMENativePodIndex(setupCtx, mgr.GetFieldIndexer()); err != nil {
			return err
		}
		if err := components.SetupInferenceService(setupCtx, mgr); err != nil {
			return err
		}
		if err := components.SetupInferenceReplica(setupCtx, mgr); err != nil {
			return err
		}
		return framework.StartKueue(setupCtx, mgr)
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "member StartManager")
}

var _ = ginkgo.BeforeSuite(func() {
	// Install the shared Eventually timeout/poll defaults so the bare-default
	// helpers (CreateAndWaitReady) inherit the suite's cross-apiserver window.
	ometesting.SetDefaultEventually()
	// In the real-Kueue path the members additionally install the Kueue CRDs +
	// pod webhook so their managers can gate queue-labeled pods. The control
	// plane never runs Kueue.
	memberWithKueue := realKueueEnabled()
	ginkgo.By("standing up the control-plane and member envtest clusters", func() {
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer ginkgo.GinkgoRecover()
			defer wg.Done()
			controlCluster = createCluster(false, true)
			if memberWithKueue {
				// The write-conflict log observer is process-global, so the
				// control-plane's Teardown gate also sees the members' Kueue
				// controllers' benign transient LocalQueue/Workload 409 retries.
				// Those are expected (Kueue retries on conflict), so disable the
				// gate suite-wide in the real-Kueue path.
				controlCluster.fwk.SkipWriteConflictCheck = true
			}
		}()
		go func() {
			defer ginkgo.GinkgoRecover()
			defer wg.Done()
			member1Cluster = createCluster(memberWithKueue, false)
		}()
		go func() {
			defer ginkgo.GinkgoRecover()
			defer wg.Done()
			member2Cluster = createCluster(memberWithKueue, false)
		}()
		wg.Wait()
	})
	if !memberWithKueue {
		for _, member := range []cluster{member1Cluster, member2Cluster} {
			runtime := ometesting.MakeClusterServingRuntime(placementRuntimeName).
				SupportsModelFormat("safetensors", true, 10).
				EngineRunner(corev1.Container{Name: "ome-container", Image: "example/model-server:test"}).Obj()
			mustCreate(member, runtime)
		}
	}
	// Every cluster's scheme registration (framework.Init) is now complete and the
	// scheme is stable. Only now start the managers, so their cache informers read
	// the scheme without racing a sibling cluster's Init write.
	ginkgo.By("starting the control-plane manager", func() {
		startControlPlaneManager(&controlCluster)
	})
	if memberWithKueue {
		// Real-Kueue path: bring up full member managers (ISVC + IR + Kueue) so
		// the members drive derived-ISVC admission for real. The sim specs are
		// skipped in this mode (see realKueueEnabled guards), so there is no
		// double-writer of derived status.
		ginkgo.By("starting the member managers (real-Kueue path)", func() {
			startMemberManager(&member1Cluster)
			startMemberManager(&member2Cluster)
		})
	}
})

var _ = ginkgo.AfterSuite(func() {
	controlCluster.teardown()
	member1Cluster.teardown()
	member2Cluster.teardown()
})
