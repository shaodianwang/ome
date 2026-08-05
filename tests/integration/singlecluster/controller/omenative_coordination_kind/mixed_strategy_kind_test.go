package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// mixed update strategies in a single coordination group:
// engine InPlaceIfPossible + decoder SurgeThenDrain (default).
//
// Every existing spec sets a single update strategy per ISVC; the
// dispatcher's per-Component routing isn't otherwise pinned. Real
// PD topologies often want engine in-place (KV-cache costly to rebuild)
// AND decoder via surge (stateless, faster rollouts). The contract: an
// image bump on a Component with InPlaceIfPossible must preserve pod
// UID; the same bump on a Component with SurgeThenDrain (single
// replica) surges to the alternate ordinal and the old UID disappears.
//
// Both Components belong to the same coordination group (Independent
// policy — no cross-Component gating gets in the way of the per-strategy
// behavior we want to assert).
var _ = ginkgo.Describe("behavior — mixed update strategies in one group", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "mixed-strat-kcg3-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("engine InPlaceIfPossible preserves UID, decoder default SurgeThenDrain rotates UID", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("mixed-strat-kcg3"),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{
					v1beta1.EngineComponent, v1beta1.DecoderComponent,
				},
				// Independent: no cross-Component gating so each strategy
				// runs to completion on its own per-Component schedule.
				Policy: v1beta1.CoordinationPolicyIndependent,
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))

		// Engine opts into InPlaceIfPossible. An image-only diff IS
		// in-place-eligible per chooseUpdateMode, so this should preserve
		// the pod UID.
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			},
		}
		// Decoder gets the default SurgeThenDrain. A single-replica
		// SurgeThenDrain on an image bump surges to ordinal 1 and drains
		// ordinal 0 — the pod UID at ordinal 0 disappears, a new UID
		// appears at ordinal 1. (Pattern matches update_kind_test.go's
		// env-var SurgeThenDrain spec, which already pins this dispatch.)
		// No explicit Lifecycle override needed — the defaulter stamps
		// SurgeThenDrain.

		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for both Components Ready; capturing engine UID + decoder UID @ ordinal 0")
		var engineUIDv1, decoderUIDv1 string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "mixed-strat-kcg3")
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.EngineComponent, v1beta1.DecoderComponent,
			} {
				cs := c.Status.Components[comp]
				g.Expect(cs.Lifecycle).NotTo(BeNil(), "%s missing OMENative status", comp)
				insts := instanceStatuses(g, ns.Name, "mixed-strat-kcg3", comp)
				g.Expect(insts).To(HaveLen(1))
				g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(insts[0].ActiveOrdinal).To(Equal(int32(0)),
					"%s must start at ordinal 0", comp)
			}

			ePod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "mixed-strat-kcg3-engine-0-default-0",
			}, ePod)).To(Succeed())
			engineUIDv1 = string(ePod.UID)

			dPod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "mixed-strat-kcg3-decoder-0-default-0",
			}, dPod)).To(Succeed())
			decoderUIDv1 = string(dPod.UID)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("bumping BOTH images v1 → v2 in a single Update")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mixed-strat-kcg3")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting per-Component dispatch: engine UID preserved (in-place), decoder UID rotates (surge)")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "mixed-strat-kcg3")
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.EngineComponent, v1beta1.DecoderComponent,
			} {
				cs := c.Status.Components[comp]
				g.Expect(cs.Lifecycle).NotTo(BeNil())
				insts := instanceStatuses(g, ns.Name, "mixed-strat-kcg3", comp)
				g.Expect(insts).To(HaveLen(1))
				g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"%s must reconverge to Ready after the bump", comp)
			}

			// Engine: InPlaceIfPossible + image-only diff → in-place →
			// pod UID at ordinal 0 STAYS engineUIDv1. ActiveOrdinal
			// stays 0.
			ePod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "mixed-strat-kcg3-engine-0-default-0",
			}, ePod)).To(Succeed())
			g.Expect(string(ePod.UID)).To(Equal(engineUIDv1),
				"engine UID must be preserved (InPlaceIfPossible + image-only diff = in-place patch); "+
					"got %s expected %s", string(ePod.UID), engineUIDv1)
			engInsts := instanceStatuses(g, ns.Name, "mixed-strat-kcg3", v1beta1.EngineComponent)
			g.Expect(engInsts[0].ActiveOrdinal).To(Equal(int32(0)),
				"engine InPlaceIfPossible must NOT advance ActiveOrdinal")

			// Decoder: default SurgeThenDrain + image diff →
			// surge-to-ordinal-1 path → old pod at ordinal 0 gone,
			// new pod at ordinal 1 with a fresh UID. ActiveOrdinal
			// advances 0 → 1.
			decInsts := instanceStatuses(g, ns.Name, "mixed-strat-kcg3", v1beta1.DecoderComponent)
			g.Expect(decInsts[0].ActiveOrdinal).To(Equal(int32(1)),
				"decoder SurgeThenDrain must advance ActiveOrdinal 0 → 1")

			dPod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "mixed-strat-kcg3-decoder-0-default-1",
			}, dPod)).To(Succeed())
			g.Expect(string(dPod.UID)).NotTo(Equal(decoderUIDv1),
				"decoder UID at ordinal 1 must differ from the original (surge produces fresh pod)")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
