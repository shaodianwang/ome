package coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	corev1apis "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// KP-1 — RollingUpdate coordination + InPlaceIfPossible per-Component.
//
// Covers a permutation the existing matrix didn't: the RollingUpdate
// coordination policy paired with InPlaceIfPossible update strategy on
// every Component in the group. The in-place patch path keeps the same
// pod object (image-only patch via strategic-merge) and the Rolling
// policy lets multiple Components proceed in parallel — so we expect
// concurrent in-place patches across engine + decoder.
//
// Invariants:
//   - L-style: pod UIDs preserved on the in-place mutation (proves
//     in-place, not recreate).
//   - Convergence: both Components advance to a new CurrentRevision
//     while the group returns to Idle.
//   - Serving budget: MaxUnavailable=1 — engine and decoder each keep
//     at least N-1 pods serving across the rollout.
var _ = ginkgo.Describe("permutation — RollingUpdate + InPlaceIfPossible", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-ru-inplace-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("KP-1 RollingUpdate + InPlace per-Component patches both Components in place", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-ru-inplace"),
			WithEngineReplicas(2),
			WithDecoderReplicas(2),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyInPlaceIfPossible),
			WithDecoderUpdateStrategy(v1beta1.UpdateStrategyInPlaceIfPossible),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingPerComponent,
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for v1 convergence.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ru-inplace")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(decCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture v1 pod UIDs.
		engineV1UIDs := map[string]corev1apis.UID{}
		decoderV1UIDs := map[string]corev1apis.UID{}
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-ru-inplace")
			g.Expect(err).NotTo(HaveOccurred())
			engPods := snap.ByComponent[v1beta1.EngineComponent]
			decPods := snap.ByComponent[v1beta1.DecoderComponent]
			g.Expect(engPods).To(HaveLen(2))
			g.Expect(decPods).To(HaveLen(2))
			for _, p := range engPods {
				engineV1UIDs[p.Name] = p.UID
			}
			for _, p := range decPods {
				decoderV1UIDs[p.Name] = p.UID
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-ru-inplace")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
		v1DecoderRev := preBump.Status.Components[v1beta1.DecoderComponent].Lifecycle.CurrentRevision

		// Sampler: serving budget invariant across the rollout. Both
		// Components must keep >= 1 of 2 pods serving (MaxUnavailable=1).
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-ru-inplace")
			if err != nil {
				return
			}
			g.Expect(snap.Serving(v1beta1.EngineComponent)).To(BeNumerically(">=", 1),
				"KP-1: engine MaxUnavailable=1 — at least 1 of 2 pods must stay serving")
			g.Expect(snap.Serving(v1beta1.DecoderComponent)).To(BeNumerically(">=", 1),
				"KP-1: decoder MaxUnavailable=1 — at least 1 of 2 pods must stay serving")
		})
		defer stop()

		// Bump both engine + decoder image. RollingUpdate + InPlace allows
		// both Components to roll concurrently with no Sequential gate.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-ru-inplace")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ru-inplace")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"KP-1: engine CurrentRevision must advance off v1")
			g.Expect(decCS.Lifecycle.CurrentRevision).NotTo(Equal(v1DecoderRev),
				"KP-1: decoder CurrentRevision must advance off v1")
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(decCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"KP-1: rollout must return to Idle")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		// Pod UID preservation across the in-place rollout. In-place
		// patches the same pod object; UIDs must match the v1 capture.
		// A non-matching UID would indicate the path fell back to
		// recreate (e.g., if the diff exceeded image-only).
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-ru-inplace")
		Expect(err).NotTo(HaveOccurred())
		for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
			origUID, ok := engineV1UIDs[p.Name]
			Expect(ok).To(BeTrue(),
				"KP-1: post-bump engine pod %s should have been present at v1 (same name)", p.Name)
			Expect(p.UID).To(Equal(origUID),
				"KP-1: in-place must preserve engine pod UID for %s (got %s, want %s) — recreate would mint a new UID",
				p.Name, p.UID, origUID)
		}
		for _, p := range snap.ByComponent[v1beta1.DecoderComponent] {
			origUID, ok := decoderV1UIDs[p.Name]
			Expect(ok).To(BeTrue(),
				"KP-1: post-bump decoder pod %s should have been present at v1 (same name)", p.Name)
			Expect(p.UID).To(Equal(origUID),
				"KP-1: in-place must preserve decoder pod UID for %s (got %s, want %s)",
				p.Name, p.UID, origUID)
		}
	})
})
