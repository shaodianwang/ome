package coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// RollingUpdate + InPlaceOnly + env-only change.
//
// chooseUpdateMode rejects InPlaceOnly when the diff exceeds image-only
// (an env-var change requires recreate to take effect). The runtime
// path records a Warning event with reason InPlaceUpdateNotPossible
// and refuses to advance the Component's CurrentRevision.
//
// At the coordination layer the rollout SHOULD stay non-Failed at
// group level — the per-Instance reject is not the same kind of
// permanent failure as a pod that can't reach Ready (compare F-* /
// bad_image_kind which DO drive the group to Failed). This spec
// pins both halves of that contract:
//
//   - InPlaceUpdateNotPossible event is recorded.
//   - Engine CurrentRevision does NOT advance off v1.
//   - Group phase does NOT enter Failed (no permanent rollout error).
var _ = ginkgo.Describe("permutation — RollingUpdate + InPlaceOnly env-change rejects without Failed", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-ru-iponly-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("InPlaceOnly + env-change emits InPlaceUpdateNotPossible event and holds CurrentRevision", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("perm-ru-iponly"),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyInPlaceOnly),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for v1 convergence.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ru-iponly")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-ru-iponly")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
		Expect(v1EngineRev).NotTo(BeEmpty())

		// Inject an env-var change — this is exactly the diff
		// chooseUpdateMode rejects for InPlaceOnly (env requires
		// recreate to take effect).
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-ru-iponly")
			cur.Spec.Engine.PodSpec.Containers[0].Env = append(
				cur.Spec.Engine.PodSpec.Containers[0].Env,
				corev1.EnvVar{Name: "KP2_ENV_TRIGGER", Value: "1"},
			)
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// InPlaceUpdateNotPossible warning event is recorded on the ISVC.
		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "InPlaceUpdateNotPossible", "InferenceService", "perm-ru-iponly")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Engine CurrentRevision does NOT advance off v1 (recreate
		// blocked by InPlaceOnly + ineligible diff).
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ru-iponly")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).To(Equal(v1EngineRev),
				"InPlaceOnly + env-change must NOT advance CurrentRevision off v1")
		}, 5*time.Second, kindPollInterval).Should(Succeed())

		// Group phase MUST NOT be Failed — this is a per-Instance
		// update reject, not a permanent rollout failure.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ru-iponly")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				NotTo(Equal(v1beta1.CoordinationPhaseFailed),
					"per-Instance InPlaceOnly reject must NOT drive group to Failed")
		}, 5*time.Second, kindPollInterval).Should(Succeed())
	})
})
