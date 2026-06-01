package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	corev1apis "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// KP-3 — Sequential coord with InPlace on both Components.
//
// The Sequential coordination gate still serializes the rollout
// (decoder finishes before engine starts) even when individual
// Components use in-place patching. This combines:
//   - Sequential ordering — the cross-Component handoff invariant.
//   - InPlaceIfPossible — image-only diff stays in-place; pod UIDs
//     are preserved.
//
// Asserts:
//   - There's a window where decoder rolled off v1 while engine
//     still on v1 (Sequential gate enforced).
//   - All pod UIDs are preserved across the rollout (in-place).
//   - Both Components advance to v2 and group returns to Idle.
var _ = ginkgo.Describe("permutation — Sequential + InPlaceIfPossible", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-seq-inplace-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("KP-3 Sequential + InPlace serializes Components AND preserves pod UIDs", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-seq-inplace"),
			WithEngineReplicas(2),
			WithDecoderReplicas(2),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyInPlaceIfPossible),
			WithDecoderUpdateStrategy(v1beta1.UpdateStrategyInPlaceIfPossible),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
				Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-seq-inplace")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(2)))
			g.Expect(decCS.OMENative.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture v1 pod UIDs.
		v1UIDs := map[string]corev1apis.UID{}
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-seq-inplace")
			g.Expect(err).NotTo(HaveOccurred())
			eng := snap.ByComponent[v1beta1.EngineComponent]
			dec := snap.ByComponent[v1beta1.DecoderComponent]
			g.Expect(eng).To(HaveLen(2))
			g.Expect(dec).To(HaveLen(2))
			for _, p := range eng {
				v1UIDs[p.Name] = p.UID
			}
			for _, p := range dec {
				v1UIDs[p.Name] = p.UID
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-seq-inplace")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision
		v1DecoderRev := preBump.Status.Components[v1beta1.DecoderComponent].OMENative.CurrentRevision

		// Sample the Sequential handoff window: decoder rolled off v1
		// while engine still on v1. Same observation pattern as D-2.
		sawDecoderDoneWhileEngineV1 := false
		stop := util.StartSampler(suiteCtx, kindPollInterval, func(_ Gomega) {
			c := &v1beta1.InferenceService{}
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "perm-seq-inplace"}, c); err != nil {
				return
			}
			dec := c.Status.Components[v1beta1.DecoderComponent].OMENative
			eng := c.Status.Components[v1beta1.EngineComponent].OMENative
			if dec == nil || eng == nil {
				return
			}
			if dec.CurrentRevision != v1DecoderRev && eng.CurrentRevision == v1EngineRev {
				sawDecoderDoneWhileEngineV1 = true
			}
		})
		defer stop()

		// Bump BOTH images. Sequential gates the engine until decoder
		// completes.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-seq-inplace")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-seq-inplace")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative.CurrentRevision).NotTo(Equal(v1EngineRev))
			g.Expect(decCS.OMENative.CurrentRevision).NotTo(Equal(v1DecoderRev))
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		Expect(sawDecoderDoneWhileEngineV1).To(BeTrue(),
			"KP-3: never observed decoder rolled-off-v1 while engine still on v1 — Sequential ordering not enforced under in-place")

		// Pod UID preservation across the in-place rollout (all 4 pods).
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-seq-inplace")
		Expect(err).NotTo(HaveOccurred())
		for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
			for _, p := range snap.ByComponent[comp] {
				origUID, ok := v1UIDs[p.Name]
				Expect(ok).To(BeTrue(),
					"KP-3: post-bump %s pod %s should have been present at v1", comp, p.Name)
				Expect(p.UID).To(Equal(origUID),
					"KP-3: in-place must preserve %s pod UID for %s (got %s, want %s)",
					comp, p.Name, p.UID, origUID)
			}
		}
	})
})
