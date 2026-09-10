// Package acceleratorquota_kueue drives a WORKLOAD-mode quota plane against a
// real Kueue: an AcceleratorQuota tree is materialized into Kueue objects,
// workloads are admitted against one of them, and the consumption Kueue reports
// finds its way back onto the tree's status.
//
// Workload mode is not a flag on the reconciler — it is which options are set.
// cmd/ome-quota-manager turns on capacity derivation and materialization only
// under --mode=workload, and management mode sets neither, so a suite that
// configures both is running the workload plane and one that configures neither
// is running the management plane. This suite sets both, which is why the
// reserved root is never authored here: a workload-mode manager creates it
// itself, and a fixture that hand-wrote one would be testing a shape no real
// cluster produces.
//
// Every layer below this is covered by unit tests, and each was green while the
// flow as a whole was not: the reader mapped a status nobody had produced, the
// rollup summed figures nobody had read, and the controller wrote them on a
// pass nothing triggered. What only a real Kueue can show is that the pieces
// meet — that the queue Kueue updates is the one the materializer wrote, that
// the labels the reader selects on are the labels the materializer applied, and
// that an admission wakes the controller at all.
//
// Separate from the other quota suites because this one needs Kueue's own
// admission stack, and with it a webhook server and Kueue's module-vendored pod
// webhook manifests. The tree-status and capacity suites want neither and
// should not pay for them.
//
// No kubelet. Kueue admission is scheduling-gate removal, and a ClusterQueue
// reports usage for admitted workloads whether or not a pod ever runs, so
// envtest is sufficient and a KIND cluster would only be slower.
package acceleratorquota_kueue

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

const (
	// tenantsName is a grouping tier between the self-created root and the
	// leaf. It exists because the root a workload-mode manager creates is bare
	// — no budgets, since a budget there is the admin's fleet total to write —
	// and a node with no budgets reports no budget status, so the root cannot
	// show a rollup. The rollup is asserted here instead.
	tenantsName = "e2e-tenants"
	leafName    = "e2e-tenant"
	// A sibling that funds a budget and never uses it. Its only job is to give
	// the cohort idle capacity: with one leaf there is nothing to borrow FROM,
	// so a queue that borrowed without limit would look identical to one that
	// could not borrow at all.
	idleName = "e2e-idle"
	tenantNS = "quota-e2e"
	// The idle sibling's namespace. Nothing is ever submitted here; a leaf has
	// to bind one to materialize at all, because Kueue rejects a ClusterQueue
	// whose namespaceSelector matches In against an empty value list.
	idleNS     = "quota-e2e-idle"
	flavorName = "e2e-flavor"
	// fieldManager is both the apply owner and the value of the managed-by
	// label, so it is also what the usage reader and the watch predicate
	// select on.
	fieldManager = "ome-quota-e2e"
	// acceleratorResource is deliberately not a real vendor's name: nothing
	// here schedules onto a node, and a neutral name keeps the fixture from
	// implying hardware the suite does not have.
	acceleratorResource = "example.com/accelerator"

	// nominal admits exactly two of the workloads below, so the second
	// admission moves a figure the first already set and a third has to wait.
	nominal = 4
	perPod  = 2
	// The tier funds both leaves, so containment holds: a parent below the sum
	// of its children freezes the node for a violation this suite is not about.
	tierNominal = 8
	// Above the 8 chips the fake node reports, so a budget raised to it is
	// affordable nowhere on this cluster.
	unaffordable = 16
	// installed is what the fake node reports. Comfortably above nominal:
	// a budget over measured capacity freezes the node, and this suite is not
	// about that check.
	installed = "8"
	// hysteresis is irrelevant here — capacity never dips — but derivation
	// needs a band, and this is the value the capacity suite uses.
	hysteresis = 20

	// Admission crosses two controllers and a webhook, so it is given more
	// room than a status write.
	timeout  = "90s"
	interval = "200ms"
)

func TestAcceleratorQuotaKueue(t *testing.T) {
	util.RunSuite(t, "AcceleratorQuota Kueue Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	// Kueue's pod webhook is what gates a queue-labeled pod on create. Without
	// the manifests installed, pods are simply never managed and every
	// admission assertion would time out with nothing to explain it.
	webhookDir, err := framework.KueueWebhookDir()
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "resolve Kueue's webhook manifests")

	s, err := framework.NewSuite(&framework.Framework{
		OptionalCRDs:      []framework.OptionalCRDBundle{framework.Kueue},
		ExtraWebhookPaths: []string{webhookDir},

		// The conflict this suite provokes is Kueue's own, and nothing the
		// materializer does can cause it. A server-side apply that changes
		// nothing does not bump resourceVersion, so the spec-only applies made
		// here cannot invalidate a lock held by a status writer — measured on
		// an apiserver, not assumed.
		//
		// Kueue's LocalQueue reconciler is the only writer of that status, and
		// it races itself: it read-modify-writes through a cached client while
		// being re-enqueued from sources unrelated to that informer's
		// freshness, so it can read a resourceVersion it has just superseded.
		// It fires once as the controller's workers start, before any workload
		// exists, then requeues and converges.
		//
		// The gate is for an OME reconciler missing conflict-aware retry, which
		// is a real bug. There is no way to scope it to one controller, so a
		// suite that runs a dependency's controllers has to turn it off.
		SkipWriteConflictCheck: true,
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		// Capacity derivation AND materialization: together these are what
		// --mode=workload configures. Capacity is what makes the reserved root
		// self-create, which is the shape a real workload cluster is in.
		//
		// No WithQuotaResync, so the reconciler is otherwise purely
		// event-driven. That is deliberate: with no tick, the only thing that
		// can refresh consumption after an admission is the watch on the
		// backend's own objects, so a spec that sees a figure move proves the
		// watch rather than the clock.
		if err := components.SetupAcceleratorQuota(ctx, mgr,
			components.WithQuotaCapacity([]string{acceleratorResource}, hysteresis),
			components.WithQuotaMaterialization(mgr, fieldManager,
				// Every rendered ClusterQueue must fund cpu and memory or Kueue
				// calls the pod's request uncovered and admits nothing, with no
				// object in an error state to say why.
				map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:    resource.MustParse("100"),
					corev1.ResourceMemory: resource.MustParse("100Gi"),
				}, tenantNS, idleNS),
		); err != nil {
			return err
		}
		return framework.StartKueue(ctx, mgr)
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())

	// A flavor with no node labels is a catch-all, so it claims the node below
	// without the fixture having to agree on a labelling scheme with it.
	gomega.Expect(k8sClient.Create(suiteCtx, &kueuev1beta2.ResourceFlavor{
		ObjectMeta: metav1.ObjectMeta{Name: flavorName},
	})).To(gomega.Succeed())

	// Capacity derivation is on, so a budget written against a pair no node
	// reports is a CapacityExceeded violation and freezes the node before it
	// ever materializes. One fake node carrying the accelerator is what keeps
	// this suite testing consumption rather than that check.
	createReadyNode("e2e-node", installed)

	for _, ns := range []string{tenantNS, idleNS} {
		gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		})).To(gomega.Succeed())
	}
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

// createReadyNode registers a node carrying the accelerator and marks it
// schedulable. envtest runs no kubelet, so nothing else ever reports either.
func createReadyNode(name, chips string) {
	ginkgo.GinkgoHelper()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	gomega.Expect(k8sClient.Create(suiteCtx, node)).To(gomega.Succeed())

	// Allocatable and conditions live in status, which Create does not persist.
	gomega.Eventually(func() error {
		var live corev1.Node
		if err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: name}, &live); err != nil {
			return err
		}
		live.Status.Allocatable = corev1.ResourceList{
			acceleratorResource: resource.MustParse(chips),
		}
		live.Status.Conditions = []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(), Reason: "KubeletReady",
		}}
		return k8sClient.Status().Update(suiteCtx, &live)
	}, timeout, interval).Should(gomega.Succeed())
}
