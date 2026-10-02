package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A first Create that never becomes Ready on a node fault, with no sibling
// serving its revision, is not relocated — the deadline disposition's
// served-revision rule.
//
// A single-pod engine parks Running-but-unready on one node (fake-serving
// FAIL_ON_NODE: readiness fails only there — a node-scoped fault with NO
// workload-caused kubelet waiting reason). The revision has served
// nowhere, so nothing says the node is at fault rather than the push:
// when the Create attempt exceeds InstanceReadyTimeout the disposition
// charges the revision's retry ladder and never the node. No AutoRecover
// ledger entry is recorded, no rebuild carries a required hostname NotIn,
// and the ladder Holds the revision at its bound. A revision that serves
// on a sibling is the case the node is blamed in (the multi-Instance
// relocation spec). Once the operator pushes a corrected revision the
// Instance converges Ready.
//
// Initial placement is forced onto the failing node by cordoning the
// other worker. Migration mode Auto and a relocation budget are
// configured on purpose: the node stays unblamed because the rule
// declines, not because relocation is off.
//
// Cells: instance/Create/CreatePods/timer.operationDeadline/before
var _ = ginkgo.Describe("OMENative never-served revision on a node fault is not relocated (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-auto-mig-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("a never-Ready first Create blames its revision's ladder, not the node, and a corrected push lands", func() {
		const (
			isvcName = "auto-mig-am1"
			podName  = isvcName + "-engine-0-default-0"
		)

		workers := schedulableWorkerNodes(Default)
		Expect(len(workers)).To(BeNumerically(">=", 2),
			"the story needs a second worker to cordon; got %v", workers)
		failNode, spareNodes := workers[0], workers[1:]

		configureFastDisposition()

		// Cordon every other worker so the first pod deterministically
		// lands on the failing node whatever the worker count or the
		// scheduler. cordonNode's cleanup uncordons on any exit.
		for _, node := range spareNodes {
			cordonNode(node)
		}

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithEngineImage("fake-serving:v1"))
		withNodeScopedFailure(&fix.ISVC.Spec.Engine.PodSpec.Containers[0], failNode, "")
		fix.ISVC.Spec.Engine.Lifecycle.MigrationPolicy = &v1beta1.MigrationPolicy{
			Mode: v1beta1.MigrationPolicyModeAuto,
		}
		fix.ISVC.Spec.Engine.Lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 45 * time.Second}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the pod to park Running-but-unready on the failing node")
		var firstTarget string
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: podName}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).To(Equal(failNode),
				"initial placement must hit the failing node (every spare is cordoned)")
			g.Expect(pod.Status.Phase).To(Equal(corev1.PodRunning),
				"the node-scoped fault must leave the container Running (no kubelet waiting reason)")

			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceCreating))
			firstTarget = engineTargetRevision(g, ns.Name, isvcName)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The standing invariant, threaded through every poll below: the
		// node is never blamed. No relocation directive is recorded and no
		// pod of the service carries a hostname exclusion for it.
		observeNoNodeBlame := func(g Gomega) {
			g.Expect(autoRecoverEntries(loadLedger(g, ns.Name, isvcName), "engine")).To(BeEmpty(),
				"a never-Ready pod on a revision nobody serves must not record a relocation directive")
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				g.Expect(podExcludesHostname(&pod, failNode)).To(BeFalse(),
					"pod %s must not exclude the failing node; affinity=%+v", pod.Name, pod.Spec.Affinity)
			}
		}

		ginkgo.By("waiting for the ladder to hold the revision after the deadline, with the node unblamed")
		Eventually(func(g Gomega) {
			observeNoNodeBlame(g)
			block := findRetryBlock(retryBlocks(g, ns.Name, isvcName, v1beta1.EngineComponent), firstTarget)
			g.Expect(block).NotTo(BeNil(), "the expired attempt must count on the revision's ladder")
			g.Expect(block.State).To(Equal(v1beta1.RetryBlockHeld),
				"the ladder must Hold the revision at its bound; got %+v", *block)
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceFailed))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("pushing a corrected revision that is healthy on every node")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			engine := &cur.Spec.Engine.PodSpec.Containers[0]
			kept := engine.Env[:0]
			for _, env := range engine.Env {
				if env.Name != "FAIL_ON_NODE" {
					kept = append(kept, env)
				}
			}
			engine.Env = kept
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		awaitEngineTargetMoved(ns.Name, isvcName, firstTarget)

		ginkgo.By("waiting for the Instance to converge Ready on the corrected revision")
		Eventually(func(g Gomega) {
			observeNoNodeBlame(g)
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Index).To(Equal(int32(0)), "the same Instance index converges; no surge index appears")
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
