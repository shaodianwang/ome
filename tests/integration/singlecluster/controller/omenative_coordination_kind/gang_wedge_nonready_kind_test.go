package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// stalledGangLifecycleJSON keeps the escalation paths a gang repair can
// touch within the test budget.
const stalledGangLifecycleJSON = `{"updateRetry":{"maxAttempts":2,"initialDelay":"5s","maxDelay":"20s","multiplier":2.0},` +
	`"stuckPodGracePeriod":"10s","autoMigrate":{"maxAttempts":2}}`

// A partially materialized gang that loses a member must restart as a unit.
// Backfilling only the missing pod can leave a surviving placement anchor that
// prevents the gang from being scheduled together again.
var _ = ginkgo.Describe("gang wedge — member lost while the Instance is not Ready", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "gang-wedge-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// The Ready case establishes the expected whole-gang restart behavior.
	ginkgo.It("Ready Instance: losing a gang member restarts the whole gang", func() {
		const name = "gw-ready"
		configureFastDisposition()
		fix := newCoordFixture(ns.Name, WithISVCName(name),
			WithoutDecoder(),
			WithEngineWorkerSize(1),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var readyIncarnation int64
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			readyIncarnation = insts[0].Incarnation
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		leader, worker := gangPods(Default, ns.Name, name)
		deleteGangPod(ns.Name, leader)

		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Incarnation).To(BeNumerically(">", readyIncarnation),
				"a Ready Instance that loses a gang member must restart the whole gang")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The survivor going away is the operative half: it is what removes the
		// scheduler's anchor and lets the gang re-plan.
		Eventually(func(g Gomega) {
			g.Expect(gangPodGone(g, ns.Name, worker)).To(BeTrue(),
				"the surviving worker must be torn down, not left bound at the old incarnation")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// The forming case exercises the same member loss before readiness.
	//
	// The Instance must be NOT Ready when the member is lost: the startup delay
	// and its readiness probe hold the gang in Creating for the whole spec.
	// Member loss below Ready restarts the gang as a unit whether or not the
	// replacement could be placed; cordoning the workers only keeps the
	// recreated gang from re-forming, so the spec observes the survivor's
	// teardown rather than a fresh gang under the same names.
	ginkgo.It("Creating Instance: a stalled partial gang is recreated, not backfilled", func() {
		const name = "gw-creating"
		configureLifecycleConfig(stalledGangLifecycleJSON)
		// Never reaches Ready: the startup delay holds every pod NotReady, so the
		// Instance remains in Creating/CreatePods.
		fix := newCoordFixture(ns.Name, WithISVCName(name),
			WithoutDecoder(),
			WithEngineWorkerSize(1),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithEngineImage("fake-serving:v1"),
			WithStartupDelay(600))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for both members to be BOUND while the Instance is still not
		// Ready. With nothing bound there is no anchor and deleting a member
		// proves nothing.
		var formingIncarnation int64
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).NotTo(Equal(v1beta1.OMENativeInstanceReady),
				"the startup delay must hold the Instance out of Ready")
			g.Expect(insts[0].PodCount).To(Equal(int32(2)),
				"status must record the complete gang before testing member loss")
			formingIncarnation = insts[0].Incarnation
			g.Expect(boundGangPods(g, ns.Name, name)).To(Equal(2),
				"waiting for both gang members to be bound")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Deny the replacement anywhere to land. cordonNode registers its own
		// uncordon cleanup, so a failing spec cannot leave the shared kind
		// cluster unschedulable.
		for _, node := range schedulableWorkerNodes(Default) {
			cordonNode(node)
		}

		leader, worker := gangPods(Default, ns.Name, name)
		deleteGangPod(ns.Name, leader)

		// The assertion. Losing a gang member is not less serious because the
		// Instance happened to still be forming: a partially-formed gang cannot
		// make progress on its own, and the survivor is what pins the scheduler.
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Incarnation).To(BeNumerically(">", formingIncarnation),
				"a non-Ready Instance that loses a member must ALSO restart the whole "+
					"gang; an unchanged Incarnation means Create backfilled the missing pod alone, "+
					"leaving the survivor bound as a permanent scheduling anchor")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(gangPodGone(g, ns.Name, worker)).To(BeTrue(),
				"the surviving worker must be torn down so the gang can re-plan freely")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// The update state machine owns pod churn while Phase=Updating, so restart
	// detection deliberately suppresses this case.
	ginkgo.PIt("Updating Instance: a stalled partial gang mid-roll is recreated", func() {
		const name = "gw-updating"
		configureLifecycleConfig(stalledGangLifecycleJSON)
		fix := newCoordFixture(ns.Name, WithISVCName(name),
			WithoutDecoder(),
			WithEngineWorkerSize(1),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Roll to v2 AND make v2 slow to become Ready: one edit mints the new
		// revision and holds the Instance out of Ready long enough for the member
		// loss to land inside the window.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, name)
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = "fake-serving:v2"
			applyStartupDelay(cur, 600)
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		var rollIncarnation int64
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceUpdating))
			rollIncarnation = insts[0].Incarnation
			g.Expect(boundGangPods(g, ns.Name, name)).To(Equal(2))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		for _, node := range schedulableWorkerNodes(Default) {
			cordonNode(node)
		}
		leader, worker := gangPods(Default, ns.Name, name)
		deleteGangPod(ns.Name, leader)

		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Incarnation).To(BeNumerically(">", rollIncarnation))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(gangPodGone(g, ns.Name, worker)).To(BeTrue())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})

// gangPods returns the (leader, worker) pod names of the single engine
// Instance, ignoring pods already terminating. query.LabelRunner is the label
// the renderer stamps; pod names are only incidentally suffixed with the runner.
func gangPods(g Gomega, namespace, isvcName string) (string, string) {
	var leader, worker string
	for _, p := range listOMENativePods(g, namespace, isvcName) {
		if p.DeletionTimestamp != nil {
			continue
		}
		switch p.Labels[query.LabelRunner] {
		case "leader":
			leader = p.Name
		case "worker":
			worker = p.Name
		}
	}
	g.Expect(leader).NotTo(BeEmpty(), "no live leader pod")
	g.Expect(worker).NotTo(BeEmpty(), "no live worker pod")
	return leader, worker
}

// boundGangPods counts live engine pods that have landed on a node.
func boundGangPods(g Gomega, namespace, isvcName string) int {
	n := 0
	for _, p := range listOMENativePods(g, namespace, isvcName) {
		if p.DeletionTimestamp == nil && p.Spec.NodeName != "" {
			n++
		}
	}
	return n
}

// deleteGangPod removes a pod the way an eviction does, so the controller sees
// a member disappear rather than a graceful scale-down.
func deleteGangPod(namespace, name string) {
	pod := &corev1.Pod{}
	Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: namespace, Name: name}, pod)).To(Succeed())
	Expect(k8sClient.Delete(suiteCtx, pod)).To(Succeed())
}

// gangPodGone reports whether the named pod is absent or terminating.
func gangPodGone(g Gomega, namespace, name string) bool {
	pod := &corev1.Pod{}
	err := k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: namespace, Name: name}, pod)
	if apierrors.IsNotFound(err) {
		return true
	}
	g.Expect(err).NotTo(HaveOccurred())
	return pod.DeletionTimestamp != nil
}
