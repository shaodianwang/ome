package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Multi-pod (gang) SurgeThenDrain surges a whole replacement gang at a
// fresh instance index — it does NOT fall back to recreate.
//
// pkg/.../workload/ops/update_gang.go implements gangSurgeUpdate: a gang
// Instance surged under SurgeThenDrain allocates a fresh index, stands up
// a full replacement gang (leader + workers) there, waits for it to serve,
// then drains the source. Capacity moves N -> N+1 -> N; the source keeps
// serving until the replacement is PodReady.
//
// This spec pins the surge contract for a 2-gang engine (N=2) with the
// default SurgeThenDrain strategy and maxSurge=1:
//   - Alive gang instances peak at exactly 3 (surge added one gang, never
//     more — maxSurge=1 caps concurrent surge at a single extra gang).
//   - Serving gang instances never drop below 2 (source drains only after
//     the replacement serves — no capacity trough).
//   - A fresh instance index appears (the surged gang's index), proving a
//     surge rather than an in-place / recreate on the original indices.
//
// SurgeThenDrain is the default UpdateStrategy.Type, so this spec leaves the
// type unset for BuildPlan to fill while explicitly configuring MaxSurge=1.
var _ = ginkgo.Describe("permutation — multi-pod SurgeThenDrain surges a replacement gang", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-mp-surge-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("multi-pod default SurgeThenDrain surges a fresh gang without dropping capacity", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("perm-mp-surge"),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineWorkerSize(1),
			WithEngineImage("fake-serving:v1"))
		// Set the exact one-gang surge budget asserted below. A nil budget reaches
		// the workload layer as BudgetNoLimit and would not enforce this ceiling.
		maxSurge := intstr.FromInt(1)
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				// Leave Type unset so BuildPlan still exercises the default
				// SurgeThenDrain strategy, but make the surge ceiling explicit.
				RollingUpdate: &v1beta1.RollingUpdate{MaxSurge: &maxSurge},
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for both gang Instances to reach Ready.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-mp-surge")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "perm-mp-surge", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(2))
			for _, is := range insts {
				g.Expect(is.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// CurrentRevision is promoted on a later reconcile than the one the wait
		// above gates on, so poll for it: a bare read races the promotion and
		// captures the empty pre-promotion value.
		var v1EngineRev string
		Eventually(func(g Gomega) {
			preBump := freshISVC(g, ns.Name, "perm-mp-surge")
			g.Expect(preBump.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			v1EngineRev = preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
			g.Expect(v1EngineRev).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the bump")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Record the initial gang instance indices so we can prove a fresh
		// (surge) index appears during the rollout.
		initialIdx := aliveGangInstanceIndices(ns.Name, "perm-mp-surge")
		Expect(initialIdx).To(HaveLen(2), "expected 2 gang instances before the bump")

		// Sampler: alive gang instances must peak at exactly 3 (one surge
		// gang, never more) and serving gang instances must never dip below
		// 2 (source keeps serving until the replacement does).
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-mp-surge")
			g.Expect(err).NotTo(HaveOccurred())
			aliveGangs := len(aliveGangIndicesFromSnapshot(snap))
			g.Expect(aliveGangs).To(BeNumerically("<=", 3),
				"gang surge must stay within minReplicas(2) + maxSurge(1); got alive gang instances=%d", aliveGangs)
			servingGangs := len(servingGangIndicesFromSnapshot(snap))
			g.Expect(servingGangs).To(BeNumerically(">=", 2),
				"gang surge must keep serving capacity at >= 2; got serving gang instances=%d", servingGangs)
		})
		defer stop()

		// Bump BOTH leader and worker images — multi-pod reads Leader.PodSpec
		// + Worker.PodSpec, so bumping only one leaves the rendered gang hash
		// unchanged and the rollout never triggers.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-mp-surge")
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Convergence: revision advances, both gang Instances Ready again.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-mp-surge")
			cs := c.Status.Components[v1beta1.EngineComponent]
			insts := instanceStatuses(g, ns.Name, "perm-mp-surge", v1beta1.EngineComponent)
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(insts).To(HaveLen(2))
			for _, is := range insts {
				g.Expect(is.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		peakSamples := stop()

		// The sampler must have actually observed the rollout.
		Expect(peakSamples).To(BeNumerically(">", 0), "sampler observed no samples")

		// A fresh gang index (the surge index) must have replaced at least
		// one of the originals — proof the gang surged rather than recreating
		// in place on its original indices.
		finalIdx := aliveGangInstanceIndices(ns.Name, "perm-mp-surge")
		Expect(finalIdx).To(HaveLen(2), "expected 2 gang instances after convergence")
		fresh := 0
		for idx := range finalIdx {
			if _, ok := initialIdx[idx]; !ok {
				fresh++
			}
		}
		Expect(fresh).To(BeNumerically(">=", 1),
			"gang surge must introduce a fresh instance index; initial=%v final=%v", initialIdx, finalIdx)
	})
})

// aliveGangInstanceIndices returns the set of instance indices with at least
// one non-terminating engine pod for the named ISVC.
func aliveGangInstanceIndices(namespace, isvcName string) map[string]struct{} {
	snap, err := util.SnapshotPods(suiteCtx, k8sClient, namespace, isvcName)
	Expect(err).NotTo(HaveOccurred())
	return aliveGangIndicesFromSnapshot(snap)
}

// aliveGangIndicesFromSnapshot collects the distinct instance indices among
// engine pods that exist and are not terminating.
func aliveGangIndicesFromSnapshot(snap *util.PodSnapshot) map[string]struct{} {
	idx := map[string]struct{}{}
	for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
		if p.DeletionTimestamp != nil {
			continue
		}
		if v, ok := p.Labels[query.LabelInstanceIdx]; ok {
			idx[v] = struct{}{}
		}
	}
	return idx
}

// servingGangIndicesFromSnapshot collects the distinct instance indices whose
// gang is fully serving: every engine pod at that index is ContainersReady
// AND has the serving gate flipped True.
func servingGangIndicesFromSnapshot(snap *util.PodSnapshot) map[string]struct{} {
	total := map[string]int{}
	serving := map[string]int{}
	for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
		v, ok := p.Labels[query.LabelInstanceIdx]
		if !ok {
			continue
		}
		total[v]++
		if podreadiness.IsContainersReady(p) && podreadiness.IsServing(p) {
			serving[v]++
		}
	}
	out := map[string]struct{}{}
	for v, n := range total {
		if n > 0 && serving[v] == n {
			out[v] = struct{}{}
		}
	}
	return out
}
