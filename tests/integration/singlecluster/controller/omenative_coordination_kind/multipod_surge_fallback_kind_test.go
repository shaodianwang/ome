package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Multi-pod (Worker.Size=2) + SurgeThenDrain falls back to
// RecreatePod (per docs).
//
// pkg/.../workload/ops/update.go around line 90 records:
//
//	"OMENative %s: multi-pod SurgeThenDrain falls back to recreate
//	 (per-gang-member surge deferred)"
//
// This spec pins the production behavior. The per-gang-member surge
// scheme isn't yet shipped (OEP-0007 deferred), so until then any
// multi-pod SurgeThenDrain rollout must be treated as a recreate
// internally. Operator-visible contract:
//   - Instance Incarnation bumps (proves recreate, not in-place).
//   - Pod count stays at 3 (no surge — recreate doesn't add a pod).
//   - Rollout converges and group returns to Idle.
//
// SurgeThenDrain is the default UpdateStrategy.Type when Lifecycle
// is unset — so this spec deliberately leaves the strategy unset
// (the defaulter writes SurgeThenDrain at admission).
var _ = ginkgo.Describe("permutation — multi-pod SurgeThenDrain falls back to recreate", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-mp-sfb-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("multi-pod default SurgeThenDrain bumps Incarnation (recreate fallback) without surging pods", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("perm-mp-sfb"),
			WithoutDecoder(),
			WithEngineWorkerSize(2),
			// Lifecycle deliberately left unset — defaulter writes
			// SurgeThenDrain. This is what we want: production-shape
			// inputs.
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-mp-sfb")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-mp-sfb")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
		v1Incarnation := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0].Incarnation

		// Multi-pod reads Leader.PodSpec + Worker.PodSpec
		// (engine.go:339-348). Bump BOTH so the rollout actually
		// triggers — bumping only Spec.Engine.PodSpec leaves the
		// rendered gang hash unchanged.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-mp-sfb")
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Convergence. Incarnation must bump (proves recreate dispatch).
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-mp-sfb")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).
				To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Incarnation).
				To(BeNumerically(">", v1Incarnation),
					"multi-pod SurgeThenDrain must fall back to recreate (Incarnation must bump)")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Pod count stays at 3 — no surge to 4. The fallback is recreate,
		// not surge.
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-mp-sfb")
		Expect(err).NotTo(HaveOccurred())
		alive := 0
		for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
			if p.DeletionTimestamp == nil {
				alive++
			}
		}
		Expect(alive).To(Equal(3),
			"multi-pod SurgeThenDrain fallback to recreate must NOT surge — alive engine pods %d should be 3", alive)
	})
})
