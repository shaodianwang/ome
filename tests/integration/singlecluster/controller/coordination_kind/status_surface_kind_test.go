package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// OMENative status-surface specs. Pin the parts of
// AggregateAndWriteStatus downstream tooling depends on:
// ObservedGeneration tracking, per-Instance AllPods* condition
// triad, and per-Instance counters at MinReplicas>1.
var _ = ginkgo.Describe("OMENative status surface", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "stat-surf-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("Component.ObservedGeneration tracks ISVC.metadata.generation across edits", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("og-isvc"),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var initialGen int64
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "og-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.ObservedGeneration).To(Equal(c.Generation),
				"ObservedGeneration should catch up to .metadata.generation")
			initialGen = c.Generation
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Edit the spec to bump generation.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "og-isvc")
			two := 2
			cur.Spec.Engine.MinReplicas = &two
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// ObservedGeneration advances to the new generation.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "og-isvc")
			g.Expect(c.Generation).To(BeNumerically(">", initialGen),
				"ISVC.metadata.generation must have bumped on spec edit")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.ObservedGeneration).To(Equal(c.Generation),
				"ObservedGeneration must catch up; stuck at %d (current gen=%d)", cs.Lifecycle.ObservedGeneration, c.Generation)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("per-Instance Conditions populate the AllPods* triad at Ready", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("inst-cond-isvc"),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "inst-cond-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			s := cs.Lifecycle.InstanceStatuses[0]
			for _, t := range []string{"AllPodsScheduled", "AllPodsReady", "AllPodsAvailable"} {
				cond := meta.FindStatusCondition(s.Conditions, t)
				g.Expect(cond).NotTo(BeNil(), "InstanceStatus[0] missing Condition %q", t)
				g.Expect(cond.Status).To(Equal(metav1.ConditionTrue),
					"InstanceStatus[0].Conditions[%s] must be True at Ready; got %+v", t, cond)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("per-Instance counters converge with the live pod set (engine MinReplicas=2)", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("counter-isvc"),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "counter-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(2))
			for i, s := range cs.Lifecycle.InstanceStatuses {
				g.Expect(s.PodCount).To(Equal(int32(1)),
					"InstanceStatus[%d].PodCount must be 1 (single-pod runner)", i)
				g.Expect(s.ReadyPodCount).To(Equal(int32(1)),
					"InstanceStatus[%d].ReadyPodCount must be 1 at Ready", i)
				g.Expect(s.AvailablePodCount).To(Equal(int32(1)),
					"InstanceStatus[%d].AvailablePodCount must be 1 at Ready", i)
			}
			g.Expect(cs.Lifecycle.Replicas).To(Equal(int32(2)))
			g.Expect(cs.Lifecycle.ReadyReplicas).To(Equal(int32(2)))
			g.Expect(cs.Lifecycle.AvailableReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
