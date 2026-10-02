package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A bad push on the only node with room never excludes that node.
//
// The pool is packed: every worker but one is cordoned, so the engine's
// single Instance runs on the one feasible node and every rebuild must
// land there too. The operator pushes a revision whose container crashes
// at start (fake-serving FAIL_AT=startup) under RecreatePod, so the only
// free capacity is what the crashing pod just vacated. The crash loop is
// a failure of a revision nobody serves, so the disposition charges the
// revision's retry ladder and never the node: no AutoRecover directive
// is recorded, no rebuild carries a hostname NotIn, and the ladder Holds
// the revision. The operator then pushes the fix; it lands on the same
// node and the Instance ends Ready and serving on it.
//
// Migration mode Auto and a relocation budget are configured on purpose:
// the node stays unblamed because the rule declines, not because
// relocation is off.
//
// Cells: instance/Update/Drain/pod.waiting[CrashLoopBackOff]/before
var _ = ginkgo.Describe("behavior — a bad push on the only feasible node is never blamed on the node (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "packed-bad-push-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("the fixed revision lands on the node the bad revision crashed on and serves", func() {
		const (
			isvcName   = "packed-bad-push"
			index      = "0"
			fixedImage = "fake-serving:v2"
		)

		workers := schedulableWorkerNodes(Default)
		Expect(len(workers)).To(BeNumerically(">=", 2),
			"a packed pool needs a second worker to cordon; got %v", workers)
		onlyNode, spareNodes := workers[0], workers[1:]

		configureFastDisposition()

		// Every other worker is cordoned for the whole story, so the one
		// feasible node is where the bad revision crashes and where the
		// fix must land. cordonNode's cleanup uncordons on any exit.
		for _, node := range spareNodes {
			cordonNode(node)
		}

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithEngineImage("fake-serving:v1"))
		fix.ISVC.Spec.Engine.PodSpec.Containers[0].ReadinessProbe = fakeServingReadinessProbe()
		fix.ISVC.Spec.Engine.Lifecycle.MigrationPolicy = &v1beta1.MigrationPolicy{
			Mode: v1beta1.MigrationPolicyModeAuto,
		}
		fix.ISVC.Spec.Engine.Lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 45 * time.Second}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the Instance to serve the first revision on the only feasible node")
		var firstTarget string
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := singleEnginePodOfInstance(g, ns.Name, isvcName, index)
			g.Expect(pod.Spec.NodeName).To(Equal(onlyNode), "every spare is cordoned")
			g.Expect(podreadiness.IsServing(pod)).To(BeTrue())
			firstTarget = engineTargetRevision(g, ns.Name, isvcName)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The standing invariant, threaded through every poll below: the
		// node is never blamed. No relocation directive is recorded and no
		// pod of the service carries a hostname exclusion for it.
		observeNoNodeBlame := func(g Gomega) {
			g.Expect(autoRecoverEntries(loadLedger(g, ns.Name, isvcName), "engine")).To(BeEmpty(),
				"a crash loop on a revision nobody serves must not record a relocation directive")
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				g.Expect(podExcludesHostname(&pod, onlyNode)).To(BeFalse(),
					"pod %s must not exclude the only feasible node; affinity=%+v", pod.Name, pod.Spec.Affinity)
			}
		}

		ginkgo.By("pushing a revision that crashes at start")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			engine := &cur.Spec.Engine.PodSpec.Containers[0]
			engine.Env = append(engine.Env, corev1.EnvVar{Name: "FAIL_AT", Value: "startup"})
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		awaitEngineTargetMoved(ns.Name, isvcName, firstTarget)
		var badTarget string
		Eventually(func(g Gomega) {
			badTarget = engineTargetRevision(g, ns.Name, isvcName)
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the ladder to hold the bad revision while the node stays unblamed")
		Eventually(func(g Gomega) {
			observeNoNodeBlame(g)
			block := findRetryBlock(retryBlocks(g, ns.Name, isvcName, v1beta1.EngineComponent), badTarget)
			g.Expect(block).NotTo(BeNil(), "the crash loop must count on the bad revision's ladder")
			g.Expect(block.State).To(Equal(v1beta1.RetryBlockHeld),
				"the ladder must Hold the bad revision at its bound; got %+v", *block)
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceFailed))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("pushing the fix")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			engine := &cur.Spec.Engine.PodSpec.Containers[0]
			kept := engine.Env[:0]
			for _, env := range engine.Env {
				if env.Name != "FAIL_AT" {
					kept = append(kept, env)
				}
			}
			engine.Env = kept
			engine.Image = fixedImage
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		awaitEngineTargetMoved(ns.Name, isvcName, badTarget)
		var fixedTarget string
		Eventually(func(g Gomega) {
			fixedTarget = engineTargetRevision(g, ns.Name, isvcName)
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the fixed revision to serve on the same node")
		Eventually(func(g Gomega) {
			observeNoNodeBlame(g)
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := singleEnginePodOfInstance(g, ns.Name, isvcName, index)
			g.Expect(pod.Labels[query.LabelRevisionHash]).To(Equal(query.RevisionHashFromControllerRevisionName(fixedTarget)),
				"the serving pod must run the fixed revision")
			g.Expect(pod.Spec.NodeName).To(Equal(onlyNode),
				"the fix must land on the node the bad revision crashed on; it is the only one with room")
			g.Expect(podreadiness.IsServing(pod)).To(BeTrue())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
