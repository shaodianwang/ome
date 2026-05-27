package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior test I — single-replica safe default.
//
// Guards the E2E-1 deadlock: with MaxUnavailable=0 the rollout would
// deadlock at MinReplicas=1 because the gate refuses to take the only
// pod offline. With the 25% default (which ceils to 1 at single-replica),
// the default must work for 1-replica ISVCs without explicit pacing.
var _ = ginkgo.Describe("behavior — single-replica safe default", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "single-i-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.PIt("I-1 default config does not deadlock at 1 replica — PIt: real SurgeThenDrain (creates new pod at alternate ordinal) isn't yet modeled by the envtest PodKubeletSimulator, which was built around delete-then-create patterns. Real-cluster covers this in .claude/specs/2026-05-25-real-cluster-tests/. Re-enable after the simulator learns surge ordinals", func() {
		// Config: 1 prefill + 1 decoder, BlueGreen with DEFAULT pacing
		// (no explicit Pacing block — exercises the default
		// maxSurge=25%, maxUnavailable=25%). At single-replica both
		// budgets ceil-round to 1 → rollout proceeds without deadlock.
		fix := newCoordFixture(ns.Name, WithISVCName("single-i"),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				// Pacing omitted on purpose — exercise the defaults.
			}),
			WithEngineImage("ghcr.io/test/runtime:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for v1 convergence.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "single-i")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.CurrentRevision).NotTo(BeEmpty(),
				"engine must reach CurrentRevision before bump")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "single-i")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision

		// Bump engine image.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "single-i")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Rollout completes. THIS is the E2E-1 regression guard: with
		// the old MaxUnavailable=0 default, this would deadlock.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "single-i")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1 — if this hangs at 90s, the default-pacing single-replica deadlock is back")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("I-2 serving stays >= 1 throughout — requires full SurgeThenDrain (currently delegates to recreate which has a brief gap per pod)", func() {})
})
