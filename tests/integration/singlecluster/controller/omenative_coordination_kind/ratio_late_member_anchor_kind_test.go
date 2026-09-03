package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// RatioBalanced anchor bookkeeping across a mid-life membership change.
//
// A Component added to a live group must gain its own ObservedRatio
// anchor entry from its real observed replicas, the member it replaces
// must have its stale entry pruned, and the entries that stay recorded
// must survive verbatim. MaintainRatio is inert on single-Component
// groups, so the group starts with two members and the change swaps
// one member for another. Real kubelet matters here: the join lands on
// a group whose members are genuinely serving, so the late member's
// anchor comes from live counts rather than a zero-clamped placeholder.
var _ = ginkgo.Describe("permutation — RatioBalanced anchor for a late-joining member", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "ratio-late-anchor-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("anchors a member added to a live group from real counts and gates the next roll on it", func() {
		tol := int32(25)
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("ratio-late-anchor"),
			WithEngineReplicas(4),
			WithDecoderReplicas(2),
			WithRouterReplicas(2),
			// In-place keeps each engine drain transient (the same pod
			// returns), so the engine-only roll below steps 4->3->4 and
			// never needs surge headroom.
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyInPlaceIfPossible),
			// The group starts engine+decoder; the router serves
			// ungrouped and joins mid-life below.
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
				Pacing: &v1beta1.CoordinationPacing{
					Type:                  v1beta1.CoordinationPacingRatioBalanced,
					RatioTolerancePercent: &tol,
					MaxUnavailable:        &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// v1 convergence: the anchor lands as the real 4:2 counts and
		// the ungrouped router has no anchor entry.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "ratio-late-anchor")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			rtrCS := c.Status.Components[v1beta1.RouterComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(rtrCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(4)))
			g.Expect(decCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(rtrCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			ratio := c.Status.RolloutCoordination.Groups[0].ObservedRatio
			g.Expect(ratio).NotTo(BeNil(), "RatioBalanced pacing must surface ObservedRatio")
			g.Expect(ratio.Original).To(HaveKeyWithValue(v1beta1.EngineComponent, int32(4)))
			g.Expect(ratio.Original).To(HaveKeyWithValue(v1beta1.DecoderComponent, int32(2)))
			g.Expect(ratio.Original).NotTo(HaveKey(v1beta1.RouterComponent),
				"a Component outside the group must not be anchored")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Swap the decoder for the router in the live group. Membership
		// alone is not a revision-affecting change, so no rollout starts.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "ratio-late-anchor")
			cur.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{
				v1beta1.EngineComponent, v1beta1.RouterComponent,
			}
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The late member gains an anchor from its real observed count
		// (2, not a zero-clamped 1), the recorded engine anchor stays
		// verbatim, and the departed decoder's entry is pruned. The
		// membership change itself leaves the group Idle.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "ratio-late-anchor")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			ratio := c.Status.RolloutCoordination.Groups[0].ObservedRatio
			g.Expect(ratio).NotTo(BeNil())
			g.Expect(ratio.Original).To(HaveKeyWithValue(v1beta1.RouterComponent, int32(2)),
				"late member must be anchored from its real observed replicas")
			g.Expect(ratio.Original).To(HaveKeyWithValue(v1beta1.EngineComponent, int32(4)),
				"recorded anchors must not be rewritten by the join")
			g.Expect(ratio.Original).NotTo(HaveKey(v1beta1.DecoderComponent),
				"a member that left the group must have its stale anchor pruned")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"a membership change is not a rollout")
			g.Expect(c.Status.Components[v1beta1.RouterComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.RouterComponent].Lifecycle.ServingReplicas).
				To(Equal(int32(2)), "the join must not disturb the late member's pods")
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.ServingReplicas).
				To(Equal(int32(2)), "leaving the group must not disturb the departed member's pods")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture pre-bump revisions; the wait above races revision
		// promotion, so poll rather than read once.
		var v1EngineRev string
		Eventually(func(g Gomega) {
			pre := freshISVC(g, ns.Name, "ratio-late-anchor")
			g.Expect(pre.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			v1EngineRev = pre.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
			g.Expect(v1EngineRev).NotTo(BeEmpty())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Gated correctly end-to-end: an engine-only bump paces engine
		// drains against the router's anchored serving. With the real
		// 4:2 anchor each drain projects 3/2 = 1.5, the inclusive band
		// edge at tol 25 — but against a zero-clamped router anchor of
		// 1 the band would sit at [3.0, 5.0] and every engine drain
		// would be refused, wedging this roll. (The 2-replica member is
		// deliberately not rolled: draining it to 1 doubles the live
		// ratio, which the band honestly rejects at this tolerance.)
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "ratio-late-anchor")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "ratio-late-anchor")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			rtrCS := c.Status.Components[v1beta1.RouterComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1 — a fictitious late-member anchor would wedge it")
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(4)))
			g.Expect(rtrCS.Lifecycle).NotTo(BeNil())
			g.Expect(rtrCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle), "rollout must return to Idle")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The completed roll changed no replica counts, so the anchor
		// still reads the recorded 4:2 — and the decoder stays out.
		c := freshISVC(Default, ns.Name, "ratio-late-anchor")
		ratio := c.Status.RolloutCoordination.Groups[0].ObservedRatio
		Expect(ratio).NotTo(BeNil())
		Expect(ratio.Original).To(HaveKeyWithValue(v1beta1.EngineComponent, int32(4)))
		Expect(ratio.Original).To(HaveKeyWithValue(v1beta1.RouterComponent, int32(2)))
		Expect(ratio.Original).NotTo(HaveKey(v1beta1.DecoderComponent))
	})
})
