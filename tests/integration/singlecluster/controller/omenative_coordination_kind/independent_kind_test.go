package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Independent coordination policy.
//
// Independent skips the gang gates entirely: each Component reconciles
// on its own. Phase only ever walks Idle / Shifting / Failed —
// never the BlueGreen Surging / Waiting / ScalingDown sequence. This
// test pins that contract end-to-end on real kubelet.
var _ = ginkgo.Describe("behavior — Independent coordination", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "indep-d4-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// OEP-0017: the Independent coordination policy has no v2 equivalent — a
	// Component in no rollout group rolls independently, and the controller no
	// longer populates status.rolloutCoordination for it. This spec is built
	// entirely around an Independent GROUP that surfaces a non-nil
	// status.rolloutCoordination with a Groups[0].Phase to inspect, which is
	// the exact contract OEP-0017 removed. Skipped pending a rewrite that
	// asserts independent-rollout behavior via per-Component status (Lifecycle
	// phases) instead of the removed coordination-group status surface.
	ginkgo.PIt("Independent steady-state Idle; bump only walks Idle/Shifting", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("indep-d4"),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyIndependent,
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "indep-d4")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "indep-d4")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "indep-d4")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Verify rollout converges + Phase never reports the BlueGreen
		// gang sequence (Surging/Waiting/ScalingDown). Collect every
		// observed phase during the rollout and assert containment.
		observed := map[v1beta1.CoordinationPhase]bool{}
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "indep-d4")
			if c.Status.RolloutCoordination != nil && len(c.Status.RolloutCoordination.Groups) > 0 {
				observed[c.Status.RolloutCoordination.Groups[0].Phase] = true
			}
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Expect(observed[v1beta1.CoordinationPhaseSurging]).To(BeFalse(),
			"Independent must NOT report Phase=Surging (that's a BlueGreen/RollingUpdate gang state)")
		Expect(observed[v1beta1.CoordinationPhaseWaiting]).To(BeFalse(),
			"Independent must NOT report Phase=Waiting")
		Expect(observed[v1beta1.CoordinationPhaseScalingDown]).To(BeFalse(),
			"Independent must NOT report Phase=ScalingDown")
	})
})
