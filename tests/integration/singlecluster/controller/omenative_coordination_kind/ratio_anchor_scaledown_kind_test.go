package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// RatioBalanced anchor bookkeeping under a deliberate scale-down.
//
// The ObservedRatio.Original anchor is snapshotted from real replica
// counts once every member's status is populated (snapshot deferral),
// and it is preserved verbatim when the operator later scales one
// member — the anchor is the rollout-ratio contract, not a mirror of
// the live fleet. Both halves need real kubelet: the deferral window
// only exists when status writes genuinely lag Create, and the
// scale-down path must drain a real pod without the coordination
// layer treating it as a rollout.
//
// The group uses RollingUpdate progression: translateCoordGroupToV2
// carries RatioBalanced pacing into the v2 MaintainRatio guard only
// on RollingUpdate groups (BlueGreen groups translate without pacing),
// and ObservedRatio is only surfaced when that guard is present.
var _ = ginkgo.Describe("permutation — RatioBalanced anchor across deliberate scale-down", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "ratio-co-anchor-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("anchors ObservedRatio to real counts and preserves it across a decoder scale-down", func() {
		tol := int32(25)
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("ratio-co-anchor"),
			WithEngineReplicas(4),
			WithDecoderReplicas(2),
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

		// Snapshot deferral end-to-end: the first reconciles observe
		// zero-replica component status (the OMENative status writer
		// lags Create), and the anchor must NOT be taken from that
		// window — it lands as the real 4:2 shape, never a zero-clamped
		// 1:1 that would wedge every later band decision.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "ratio-co-anchor")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(4)))
			g.Expect(decCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			ratio := c.Status.RolloutCoordination.Groups[0].ObservedRatio
			g.Expect(ratio).NotTo(BeNil(), "RatioBalanced pacing must surface ObservedRatio")
			g.Expect(ratio.Original).To(HaveKeyWithValue(v1beta1.EngineComponent, int32(4)),
				"anchor must record the real engine count, not a deferred-window clamp")
			g.Expect(ratio.Original).To(HaveKeyWithValue(v1beta1.DecoderComponent, int32(2)),
				"anchor must record the real decoder count, not a deferred-window clamp")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Deliberate scale-down of one member while the group is Idle.
		// No image change — this is a pure capacity decision, not a
		// rollout, so the RatioBalanced gate must not hold the drain.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "ratio-co-anchor")
			one := 1
			cur.Spec.Decoder.MinReplicas = &one
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "ratio-co-anchor")
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(decCS.Lifecycle.ServingReplicas).To(Equal(int32(1)),
				"decoder must scale down to 1 — a deliberate scale is not gated by the ratio band")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Anchor preservation: many reconciles have run since the scale
		// change landed, so a re-snapshot (which would record 4:1) would
		// be visible by now. The anchor must still read 4:2.
		c := freshISVC(Default, ns.Name, "ratio-co-anchor")
		ratio := c.Status.RolloutCoordination.Groups[0].ObservedRatio
		Expect(ratio).NotTo(BeNil())
		Expect(ratio.Original).To(HaveKeyWithValue(v1beta1.EngineComponent, int32(4)))
		Expect(ratio.Original).To(HaveKeyWithValue(v1beta1.DecoderComponent, int32(2)),
			"anchor must stay at the snapshot taken at first observation; a deliberate scale-down must not rewrite it")

		// Pod-level invariant: exactly one alive decoder pod remains.
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "ratio-co-anchor")
			g.Expect(err).NotTo(HaveOccurred())
			alive := 0
			for _, p := range snap.ByComponent[v1beta1.DecoderComponent] {
				if p.DeletionTimestamp != nil {
					continue
				}
				alive++
			}
			g.Expect(alive).To(Equal(1),
				"expected exactly 1 alive decoder pod post-scale-down, got %d", alive)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
