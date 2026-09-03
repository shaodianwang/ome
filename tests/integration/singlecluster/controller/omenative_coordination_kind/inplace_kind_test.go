package omenative_coordination_kind

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

// Behavior test L — in-place opt-in respects MaxUnavailable.
//
// The in-place patch path keeps the same Pod object (image-only patch
// via strategic-merge); the assertions here under real kubelet exercise:
//   - at-least-2-of-3 serving during the rollout (MaxUnavailable=1)
//   - no surge (alive count <= replicas)
//   - pod UID is preserved across the bump (proves in-place, not
//     recreate; recreate would mint a new UID)
var _ = ginkgo.Describe("behavior — in-place opt-in respects MaxUnavailable", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "inplace-l-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("in-place bump preserves serving budget, alive cap, and pod UIDs", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("inplace-l"),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "inplace-l")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(BeEmpty())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(3)),
				"all 3 engine pods should be serving on v1")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture engine pod UIDs at v1 for the pod-identity assertion.
		v1UIDs := map[string]corev1apis.UID{}
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "inplace-l")
			g.Expect(err).NotTo(HaveOccurred())
			pods := snap.ByComponent[v1beta1.EngineComponent]
			g.Expect(pods).To(HaveLen(3))
			for _, p := range pods {
				v1UIDs[p.Name] = p.UID
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// CurrentRevision is promoted on a later reconcile than the one the wait
		// above gates on, so poll for it: a bare read races the promotion and
		// captures the empty pre-promotion value.
		var v1EngineRev string
		Eventually(func(g Gomega) {
			preBump := freshISVC(g, ns.Name, "inplace-l")
			g.Expect(preBump.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			v1EngineRev = preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
			g.Expect(v1EngineRev).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the bump")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "inplace-l")
			g.Expect(err).NotTo(HaveOccurred())
			eng := snap.Serving(v1beta1.EngineComponent)
			alive := snap.Alive(v1beta1.EngineComponent)
			g.Expect(eng).To(BeNumerically(">=", 2),
				"in-place with MaxUnavailable=1 must keep >=2 of 3 engine pods serving (observed serving=%d alive=%d)", eng, alive)
			g.Expect(alive).To(BeNumerically("<=", 3),
				"in-place must not create extra pods (observed alive=%d)", alive)
		})
		defer stop()

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "inplace-l")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "inplace-l")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision should advance off v1")
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(3)),
				"all 3 engine pods should be back to serving on v2")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		stop()

		// pod UIDs preserved. In-place patches the same pod object;
		// the UID must match the v1 capture. Recreate would mint a new UID.
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "inplace-l")
		Expect(err).NotTo(HaveOccurred())
		Expect(snap.ByComponent[v1beta1.EngineComponent]).To(HaveLen(3))
		for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
			origUID, ok := v1UIDs[p.Name]
			Expect(ok).To(BeTrue(),
				"post-bump engine pod %s should have been present at v1 (same name)", p.Name)
			Expect(p.UID).To(Equal(origUID),
				"in-place must preserve pod UID for %s (got %s, want %s)", p.Name, p.UID, origUID)
		}
	})

	// PIt: covered by the convergence wait inside the in-place bump spec above.
	ginkgo.PIt("rollout converges", func() {})
})
