package coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// E-1 — PerComponent pacing, maxSurge>0 upper bound.
//
// Companion to B (which pins maxSurge=0). Asserts that during the
// rollout, the alive pod count for engine never exceeds
// minReplicas + maxSurge. With real SurgeThenDrain (PR #238) the
// gate's CheckSurgeGate IS exercised here — verified by injecting a
// deliberate gate bypass which makes both Instances surge in parallel
// and pushes alive=4 (the round-2 E-1 bug report's symptom). See
// percomponent_maxsurge_multiinstance_kind_test.go for the BlueGreen
// + 3-Instance variants that stress the cross-Instance enforcement
// path on the gate's deny side.
var _ = ginkgo.Describe("behavior — PerComponent maxSurge upper bound", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "surge-e1-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("E-1 alive engine pods never exceed minReplicas + maxSurge", func() {
		// Config: 2 engine + 1 decoder, RollingUpdate + PerComponent
		// pacing, maxSurge=1, maxUnavailable=1. Bound: alive(engine) <= 3.
		maxSurge := intstr.FromInt(1)
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("surge-e1"),
			WithEngineReplicas(2),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingPerComponent,
					MaxSurge:       &maxSurge,
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "surge-e1")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "surge-e1")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision

		// Sampler: alive(engine) must stay <= 3 throughout.
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "surge-e1")
			g.Expect(err).NotTo(HaveOccurred())
			alive := snap.Alive(v1beta1.EngineComponent)
			g.Expect(alive).To(BeNumerically("<=", 3),
				"E-1: engine alive count must stay <= minReplicas(2) + maxSurge(1); got alive=%d", alive)
		})
		defer stop()

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "surge-e1")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "surge-e1")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"E-1: engine CurrentRevision must advance off v1")
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()
	})
})
