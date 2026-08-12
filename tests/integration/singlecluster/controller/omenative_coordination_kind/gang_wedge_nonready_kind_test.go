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

// stalledGangLifecycleJSON is fastDispositionLifecycleJSON plus a seconds-scale
// stalledGangGracePeriod, so the partial-gang escape is reachable inside a spec
// instead of at the 5m production default.
const stalledGangLifecycleJSON = `{"updateRetry":{"maxAttempts":2,"initialDelay":"5s","maxDelay":"20s","multiplier":2.0},` +
	`"stuckPodGracePeriod":"10s","stalledGangGracePeriod":"15s","autoMigrate":{"maxAttempts":2}}`

// Gang wedge: an Instance that loses a gang member while NOT Ready.
//
// Root cause: https://www.internalfb.com/intern/px/p/cg1pj/. Reproduced live on
// msl-gcp-usc1-2-tpu on 2026-08-12 with the recipe these specs encode: roll a
// multi-pod gang, make the new revision slow to become Ready, and drain a node
// under it while the roll is in flight.
//
// RecreateInstanceOnPodRestart promises "lose a pod, rebuild the whole gang".
// It kept that promise only from Phase=Ready: DetectRestartTrigger rejected
// every other phase, and Create's partial-Instance backfill defers to Restart
// only when the Instance is Ready. Below Ready the missing pod was recreated
// alone, at the SAME Incarnation, leaving the surviving sibling bound — which
// for a gang-scheduled workload pins the scheduler to a domain that can no
// longer hold the gang, permanently.
//
// A gang is most fragile exactly when it is not Ready: forming and rolling are
// long windows (tens of minutes of cold compile on the real workload), so a
// drain landing inside one is an ordinary coincidence rather than a rare one.
// Field measurement on four wedged production gangs: leader and worker carried
// identical Incarnation (74/74, 67/67, 164/164, 10/10) with the workers 4 days
// old and the leaders ~1h — Create backfill, not whole-Instance Restart.
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

	// The control. Same event, Instance Ready. It establishes that the
	// drain-shaped event recovers on its own, so a failure in the next spec is
	// attributable to the phase and to nothing else. Live control on gke-2:
	// Ready -> Restarting, incarnation 1 -> 2, survivor Terminating, gang back
	// in ~3.5 min.
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

	// The defect. Identical event, Instance still forming.
	//
	// Two conditions make this the production shape rather than a cosmetic
	// variation, and both are set up explicitly:
	//
	//   - the Instance must be NOT Ready when the member is lost. The startup
	//     delay holds it out of Ready; without it the window is milliseconds and
	//     the race is unwinnable by hand.
	//   - the replacement must be genuinely UNSCHEDULABLE. That is what turns the
	//     survivor into a permanent anchor rather than a transient one, and it is
	//     the discriminator the controller keys on: a gang whose replacement can
	//     still be placed is merely slow and must NOT be recreated. Cordoning
	//     every worker node reproduces "the domain cannot fit the remaining
	//     member" without needing real TPU topology, and cordoning does not
	//     evict, so the survivor stays bound exactly as it does in the field.
	ginkgo.It("Creating Instance: a stalled partial gang is recreated, not backfilled", func() {
		const name = "gw-creating"
		configureLifecycleConfig(stalledGangLifecycleJSON)
		// Never reaches Ready: the startup delay holds every pod NotReady, so the
		// Instance sits in Creating/CreatePods. This is the exact production
		// shape -- all four wedged gangs were Phase=Creating, Operation=Create.
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
				"a non-Ready Instance whose replacement cannot be placed must ALSO restart the whole "+
					"gang; an unchanged Incarnation means Create backfilled the missing pod alone, "+
					"leaving the survivor bound as a permanent scheduling anchor")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(gangPodGone(g, ns.Name, worker)).To(BeTrue(),
				"the surviving worker must be torn down so the gang can re-plan freely")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// PENDING — a known remaining gap, not a flake.
	//
	// The same wedge is reachable from Phase=Updating: roll a gang, make the new
	// revision slow to become Ready, and lose a member while the roll is in
	// flight. Reproduced live on msl-gcp-usc1-2-tpu on 2026-08-12: instance
	// Updating/Drain at incarnation 2, leader node drained, leader Pending and
	// worker Running at the SAME incarnation 9+ minutes later, with the
	// scheduler reporting the production error verbatim --
	//   pinned domain 0027bcdb... cannot fit all remaining gang members
	//
	// stalledGangPhase deliberately admits only Creating and Failed today.
	// Updating is owned by the update state machine, which is mid-way through
	// its own drain/recreate of the pod set, so routing it into a whole-Instance
	// Restart needs the interaction between the two worked out first rather than
	// a phase added to a list. Un-pend this spec with that change.
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
