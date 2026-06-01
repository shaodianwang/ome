package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	corev1apis "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// KP-5 — BlueGreen + InPlaceOnly happy path.
//
// The interesting case: BlueGreen normally surges a v2 set and flips
// traffic atomically. With InPlaceOnly + image-only diff the per-pod
// patch IS eligible, so the rollout runs as an in-place patch sequence
// — there's no BlueGreen surge to observe. The "happy path" pin is:
//
//   - The eligible image-only diff is accepted (no
//     InPlaceUpdateNotPossible event).
//   - Engine CurrentRevision advances off v1.
//   - All pod UIDs preserved across the bump.
//   - Group returns to Idle.
var _ = ginkgo.Describe("permutation — BlueGreen + InPlaceOnly happy path", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-bg-iponly-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("KP-5 BlueGreen + InPlaceOnly image-only diff stays in place and converges", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-bg-iponly"),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyInPlaceOnly),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-bg-iponly")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(3)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture v1 pod UIDs.
		v1UIDs := map[string]corev1apis.UID{}
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-bg-iponly")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(snap.ByComponent[v1beta1.EngineComponent]).To(HaveLen(3))
			for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
				v1UIDs[p.Name] = p.UID
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-bg-iponly")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision

		// Bump image-only — this IS eligible for in-place under
		// InPlaceOnly so chooseUpdateMode returns updateModeInPlace.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-bg-iponly")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-bg-iponly")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"KP-5: engine CurrentRevision must advance off v1 on image-only InPlaceOnly bump")
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(3)))
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Pod UID preservation across in-place. If chooseUpdateMode
		// had routed to recreate (which InPlaceOnly should never do
		// for an eligible diff), UIDs would differ.
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-bg-iponly")
		Expect(err).NotTo(HaveOccurred())
		Expect(snap.ByComponent[v1beta1.EngineComponent]).To(HaveLen(3))
		for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
			origUID, ok := v1UIDs[p.Name]
			Expect(ok).To(BeTrue(),
				"KP-5: post-bump engine pod %s should have been present at v1", p.Name)
			Expect(p.UID).To(Equal(origUID),
				"KP-5: InPlaceOnly + eligible diff must preserve pod UID for %s (got %s, want %s)",
				p.Name, p.UID, origUID)
		}
	})
})
