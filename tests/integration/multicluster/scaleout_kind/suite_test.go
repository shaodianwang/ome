// Package scaleout_kind exercises multi-cluster placement against REAL member
// clusters: two KIND clusters with real kubelets, driven by a control plane
// whose placement + WorkloadCluster controllers run in-process.
//
// What this adds over tests/integration/multicluster (three envtest apiservers):
// envtest proves the placer WRITES the right derived objects and reads the right
// status back, but it has no kubelet, so nothing it places ever runs. Here the
// members schedule and run pods for real, so a fan-out is asserted by the
// workload actually coming up on each home.
//
// Kueue is deliberately absent. Gating requires Kueue's mutating pod webhook and
// the KIND harness runs no webhook server, so admission semantics stay on the
// envtest real-Kueue suite; this suite covers only the kubelet delta. With no
// gate on the pods every Instance reports Admitted as soon as it has pods, which
// is why the specs here assert on pods and derived objects rather than on the
// admission race.
//
// The control plane is an envtest apiserver rather than a third KIND cluster: it
// never schedules a pod, so a kubelet buys it nothing and costs a third Docker
// cluster per run.
package scaleout_kind

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// KIND cluster names for the two members. One cluster per member so the placer
// exercises genuinely separate apiservers, schedulers, and kubelets.
const (
	member1ClusterName = "ome-mc-m1-kind"
	member2ClusterName = "ome-mc-m2-kind"
)

// controlPlaneID identifies this control plane on the members, so the GC sweep
// and the origin guards can tell our deriveds from anyone else's.
const controlPlaneID = "cp-scaleout-kind"

// Per-spec timeouts — the shared KIND tier, tuned for real kubelet latency
// (image pull, schedule, container start) rather than envtest microseconds.
const (
	mediumTimeout = kindfix.MediumTimeout
	longTimeout   = kindfix.LongTimeout
	pollInterval  = kindfix.PollInterval
)

// Control-plane reconciler cadences. Faster than production so a fan-out or a
// reachability change is observed within a couple of poll cycles, but an order
// of magnitude slower than the envtest suite's sub-second tier: every poll here
// crosses to a real KIND apiserver.
const (
	placementRequeue      = 1 * time.Second
	clusterHealthInterval = 1 * time.Second
	statusBatchPeriod     = 250 * time.Millisecond
	statusSafetyRequeue   = 30 * time.Second
	gcInterval            = 15 * time.Second

	// winnerLostGrace must outlast a real pod recreate. An Instance reports
	// Admitted only while it HAS pods, so the gap between a pod being deleted
	// and its replacement appearing reads as lost admission; on a kubelet that
	// gap is seconds, not the sub-millisecond envtest equivalent. Too short a
	// grace and ordinary pod churn re-races a perfectly healthy placement.
	winnerLostGrace = 30 * time.Second

	// connectionGrace bounds how long a member that fails its reachability
	// probe keeps its connection. The pause-based specs freeze a node for well
	// under this, so the control plane treats it as a blip; stated explicitly
	// rather than inherited from the package default, because those specs are
	// only meaningful in relation to it.
	connectionGrace = 2 * time.Minute
)

// cluster is one apiserver the suite drives — the envtest control plane or a
// KIND member. The fields a given role does not use stay nil (fwk and clusters
// are control-plane only; stopManager is member only).
type cluster struct {
	name   string
	cfg    *rest.Config
	client client.Client
	ctx    context.Context

	fwk         *framework.Framework       // control plane only
	clusters    *components.ClusterManager // control plane only
	manager     ctrl.Manager
	stopManager func() // members only
}

var (
	controlCluster cluster
	member1        cluster
	member2        cluster

	// suiteCancel stops the member managers and the KIND-side context at
	// AfterSuite. The control plane owns its own context via framework.
	suiteCancel context.CancelFunc
)

func TestMultiClusterScaleOutKind(t *testing.T) {
	util.RunSuite(t, "Multi-Cluster Scale-Out KIND Suite")
}

// startControlPlane brings up the envtest apiserver that hosts the source
// InferenceServices and the WorkloadCluster registry. Only the OME CRDs are
// installed: the control plane runs the placement + WorkloadCluster controllers,
// neither of which touches KEDA, Prometheus, or Kueue types.
func startControlPlane() cluster {
	f := &framework.Framework{}
	cfg, err := f.Init()
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "control-plane framework.Init")
	ctx, cl, err := f.SetupClient(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "control-plane framework.SetupClient")

	return cluster{name: "control-plane", cfg: cfg, client: cl, ctx: ctx, fwk: f}
}

// startControlPlaneManager wires and starts the control-plane manager: the
// WorkloadCluster registry (with the origin-scoped derived-ISVC cache), the
// placement reconciler, and the orphan GC. It must run only after every member
// exists, because the WorkloadCluster reconciler connects to them on its first
// pass.
func startControlPlaneManager(c *cluster) {
	var cm *components.ClusterManager
	err := c.fwk.StartManager(c.ctx, c.cfg, func(setupCtx context.Context, mgr ctrl.Manager) error {
		m, err := components.SetupWorkloadCluster(setupCtx, mgr,
			components.WithClusterHealthInterval(clusterHealthInterval),
			components.WithClusterConnectionGracePeriod(connectionGrace),
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
	c.manager = c.fwk.Manager()
}

// startMember brings up one KIND member: the cluster itself, the fake-serving
// images, the OME + optional-dep CRDs, and an in-process manager running the
// same BaseModel / InferenceService / InferenceReplica reconcilers a real member
// would. This is what turns a placed derived ISVC into pods on a real kubelet.
func startMember(ctx context.Context, name string) cluster {
	// Single-node members. What this suite exercises is placement ACROSS
	// clusters, not scheduling spread within one, so extra worker nodes buy
	// nothing and cost a container each — with two members that is the
	// difference between four nodes and two. A single-node KIND cluster also
	// leaves its control plane schedulable, whereas a multi-node one keeps the
	// control-plane NoSchedule taint and so has nowhere to run pods if a worker
	// fails to join (`kind create --wait` only waits on the control plane, so a
	// half-built cluster otherwise surfaces much later as unschedulable pods).
	cfg, err := kindutil.EnsureCluster(ctx, name, 0)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "EnsureCluster %s", name)

	gomega.Expect(kindutil.BuildAndLoadImages(ctx, name, kindfix.FakeServingImages())).
		To(gomega.Succeed(), "BuildAndLoadImages %s", name)
	gomega.Expect(kindutil.ApplyCRDs(ctx, cfg, kindfix.OMECRDPaths())).
		To(gomega.Succeed(), "ApplyCRDs %s", name)

	scheme := kindfix.OMEScheme()
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "client.New %s", name)

	purgeLeftovers(ctx, cl, name)

	mgr, stop, err := kindutil.StartManager(ctx, cfg, scheme,
		kindfix.StandardOMESetup(kindfix.SetupOpts{WireInferenceReplica: true}))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "member StartManager %s", name)

	return cluster{
		name:        name,
		cfg:         cfg,
		client:      cl,
		ctx:         ctx,
		manager:     mgr,
		stopManager: stop,
	}
}

var _ = ginkgo.BeforeSuite(func() {
	kindutil.SetupLogger()

	ctx, cancel := context.WithCancel(context.Background())
	suiteCancel = cancel

	// Sequential bring-up. framework.Init mutates the process-global client-go
	// scheme, so it must not run concurrently with anything that reads it; doing
	// the control plane first and the members after removes the race entirely
	// (the members' managers are started last, once the scheme is stable).
	ginkgo.By("standing up the control-plane envtest apiserver", func() {
		controlCluster = startControlPlane()
	})
	ginkgo.By("standing up member KIND cluster "+member1ClusterName, func() {
		member1 = startMember(ctx, member1ClusterName)
	})
	ginkgo.By("standing up member KIND cluster "+member2ClusterName, func() {
		member2 = startMember(ctx, member2ClusterName)
	})
	ginkgo.By("starting the control-plane manager", func() {
		startControlPlaneManager(&controlCluster)
	})
})

var _ = ginkgo.AfterSuite(func() {
	if member1.stopManager != nil {
		member1.stopManager()
	}
	if member2.stopManager != nil {
		member2.stopManager()
	}
	if suiteCancel != nil {
		suiteCancel()
	}
	if controlCluster.fwk != nil {
		_ = controlCluster.fwk.StopManager(context.Background())
		gomega.Expect(controlCluster.fwk.Teardown()).To(gomega.Succeed(), "control-plane Teardown")
	}
	// KIND cluster teardown honors KIND_KEEP_CLUSTER, so a local iteration loop
	// reuses the clusters while CI gets a clean delete.
	for _, name := range []string{member1ClusterName, member2ClusterName} {
		if err := kindutil.DeleteCluster(context.Background(), name); err != nil {
			ginkgo.GinkgoWriter.Printf("DeleteCluster(%s): %v\n", name, err)
		}
	}
})

// specNamespacePrefixes are the namespace names this suite mints per spec. A
// purge keys on them so it only ever removes namespaces this suite created.
var specNamespacePrefixes = []string{"mc-workload-", "mc-secrets-"}

// purgeLeftovers removes state a PREVIOUS run left on a reused member. Per-spec
// cleanup cannot cover a run that was killed or died mid-teardown, and the
// members persist across runs by default (KIND_KEEP_CLUSTER), so without this
// the debris — namespaces, and the pods still running inside them — accumulates
// until the cluster is recreated by hand.
//
// It runs BEFORE the member's manager starts, so no reconciler races the
// deletes. Deletes are issued without waiting: a namespace whose contents are
// still finalizing drains on its own, and blocking here would add minutes to
// every start.
func purgeLeftovers(ctx context.Context, c client.Client, memberName string) {
	nsList := &corev1.NamespaceList{}
	gomega.Expect(c.List(ctx, nsList)).To(gomega.Succeed(), "list namespaces on %s", memberName)
	var purged int
	for i := range nsList.Items {
		ns := &nsList.Items[i]
		for _, prefix := range specNamespacePrefixes {
			if strings.HasPrefix(ns.Name, prefix) && ns.DeletionTimestamp == nil {
				gomega.Expect(client.IgnoreNotFound(c.Delete(ctx, ns))).To(gomega.Succeed(),
					"delete leftover namespace %s on %s", ns.Name, memberName)
				purged++
				break
			}
		}
	}

	// The cluster-scoped model and runtime a spec seeds survive a namespace
	// sweep, so they need removing explicitly. These members serve only this
	// suite, so everything of these kinds is ours by construction.
	cbms := &v1beta1.ClusterBaseModelList{}
	gomega.Expect(c.List(ctx, cbms)).To(gomega.Succeed(), "list ClusterBaseModels on %s", memberName)
	for i := range cbms.Items {
		gomega.Expect(client.IgnoreNotFound(c.Delete(ctx, &cbms.Items[i]))).To(gomega.Succeed())
		purged++
	}
	csrs := &v1beta1.ClusterServingRuntimeList{}
	gomega.Expect(c.List(ctx, csrs)).To(gomega.Succeed(), "list ClusterServingRuntimes on %s", memberName)
	for i := range csrs.Items {
		gomega.Expect(client.IgnoreNotFound(c.Delete(ctx, &csrs.Items[i]))).To(gomega.Succeed())
		purged++
	}

	if purged > 0 {
		ginkgo.GinkgoWriter.Printf("purged %d leftover object(s) from %s\n", purged, memberName)
	}
}
