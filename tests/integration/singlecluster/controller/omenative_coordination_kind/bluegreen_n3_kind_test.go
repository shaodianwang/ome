package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// BlueGreen across 3 Components.
//
// n3_sequential_kind covers Sequential N=3; bluegreen_kind covers
// BlueGreen N=2. This spec pins BlueGreen at N=3, where the gang-flip
// semantic crosses all three Components (engine + decoder + router) in
// lockstep.
//
// Asserts:
//   - All three Components advance off v1 once the bump is applied.
//   - Group returns to Idle.
//   - Each Component returns to full ServingReplicas.
var _ = ginkgo.Describe("permutation — BlueGreen N=3 gang-flip", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-bg-n3-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("BlueGreen N=3 rolls all three Components and returns to Idle", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-bg-n3"),
			WithRouter(),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{
					v1beta1.EngineComponent,
					v1beta1.DecoderComponent,
					v1beta1.RouterComponent,
				},
				Policy: v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for all three Components to reach Idle on v1.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-bg-n3")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.EngineComponent,
				v1beta1.DecoderComponent,
				v1beta1.RouterComponent,
			} {
				cs := c.Status.Components[comp]
				g.Expect(cs.Lifecycle).NotTo(BeNil(), "Component %s missing OMENative status", comp)
				g.Expect(cs.Lifecycle.ServingReplicas).To(BeNumerically(">=", 1),
					"Component %s must have at least 1 serving replica before bump", comp)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Snapshot all three Component CurrentRevisions.
		preBump := freshISVC(Default, ns.Name, "perm-bg-n3")
		v1Revs := map[v1beta1.ComponentType]string{
			v1beta1.EngineComponent:  preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision,
			v1beta1.DecoderComponent: preBump.Status.Components[v1beta1.DecoderComponent].Lifecycle.CurrentRevision,
			v1beta1.RouterComponent:  preBump.Status.Components[v1beta1.RouterComponent].Lifecycle.CurrentRevision,
		}
		for comp, rev := range v1Revs {
			Expect(rev).NotTo(BeEmpty(), "v1 revision for %s should not be empty", comp)
		}

		// Bump all three images. BlueGreen coordinates the surge across
		// the group; per-Component rollouts proceed in parallel (no
		// Sequential gate).
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-bg-n3")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Router.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-bg-n3")
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.EngineComponent,
				v1beta1.DecoderComponent,
				v1beta1.RouterComponent,
			} {
				cs := c.Status.Components[comp]
				g.Expect(cs.Lifecycle).NotTo(BeNil())
				g.Expect(cs.Lifecycle.CurrentRevision).NotTo(Equal(v1Revs[comp]),
					"Component %s CurrentRevision must advance off v1", comp)
				g.Expect(cs.Lifecycle.ServingReplicas).To(BeNumerically(">=", 1),
					"Component %s must remain serving on v2", comp)
			}
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"BlueGreen N=3 rollout must return to Idle")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
