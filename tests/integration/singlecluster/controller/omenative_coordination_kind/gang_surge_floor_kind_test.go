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

// Gang SurgeThenDrain capacity-floor regression gate.
//
// This is the gang analog of recreate_pod_kind's single-pod serving
// floor: it wraps a multi-pod (Worker.Size=1) SurgeThenDrain roll in the
// 100ms sampler and enforces the three fleet-wipe invariants at gang
// granularity. Post-#618 multi-pod SurgeThenDrain surges a whole
// replacement gang at a FRESH instance index (its own PodGroup), then
// drains the source gang — so the invariants are:
//
//   - capacity floor / surge-before-drain: serving GANGS stay >= replicas
//     throughout. Serving can only hold >= N if the replacement gang is
//     Ready before the source gang drains — the exact ordering a
//     fleet-wipe violates.
//   - distinct surge index: alive gangs (grouped by instance-index) stay
//     <= replicas + maxSurge. #618's distinct-surge-index fix means the
//     replacement gang lands on a new index without colliding with a
//     live source gang; a collision would collapse two gangs into one
//     bucket and let alive under-count while pods vanish.
//   - a fresh surge instance-index appears mid-roll (proves a real surge,
//     not an in-place / recreate that reuses the source index).
//
// Convergence asserts engine reaches v2 with all InstanceStatuses Ready.
//
// servingFn counts pods per invocation via the sampler's snapshot; the
// helpers below reduce a snapshot to gang granularity.
var _ = ginkgo.Describe("Gang SurgeThenDrain capacity floor", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "gang-floor-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("multi-pod SurgeThenDrain holds the serving gang floor, surges a fresh index, converges", func() {
		// Config: engine gang of Worker.Size=1 (leader + 1 worker = 2 pods
		// per Instance), 4 replicas, SurgeThenDrain maxSurge=1 /
		// maxUnavailable=0. Decoder dropped so the sampler tracks the
		// engine gang alone.
		const replicas = 4
		const maxSurgeN = 1
		maxSurge := intstr.FromInt(maxSurgeN)
		maxUnavail := intstr.FromInt(0)
		fix := newCoordFixture(ns.Name, WithISVCName("gang-floor"),
			WithoutDecoder(),
			WithEngineWorkerSize(1),
			WithEngineReplicas(replicas),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategySurgeThenDrain),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
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

		// Wait for v1 convergence: all 4 gangs Ready.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "gang-floor")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(replicas))
			for _, is := range cs.Lifecycle.InstanceStatuses {
				g.Expect(is.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "gang-floor")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision

		// Capture the pre-bump instance-index set so the sampler can detect
		// a fresh surge index appearing mid-roll.
		preIdx := map[int32]bool{}
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "gang-floor")
			g.Expect(err).NotTo(HaveOccurred())
			by := query.BucketPodsByInstanceIdx(snap.ByComponent[v1beta1.EngineComponent])
			g.Expect(by).To(HaveLen(replicas))
			for idx := range by {
				preIdx[idx] = true
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Sampler: capacity floor + distinct-surge-index cap on every 100ms
		// tick. Also records whether a fresh (post-bump) surge index was
		// observed serving-eligible mid-roll. Diagnostic counts land in the
		// message so a violation is debuggable from the test log alone.
		sawFreshIdx := false
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "gang-floor")
			g.Expect(err).NotTo(HaveOccurred())
			pods := snap.ByComponent[v1beta1.EngineComponent]
			by := query.BucketPodsByInstanceIdx(pods)

			servingGangs := fullyServingGangs(by)
			aliveGangs := len(by)
			for idx := range by {
				if !preIdx[idx] {
					sawFreshIdx = true
				}
			}

			g.Expect(servingGangs).To(BeNumerically(">=", replicas),
				"engine serving gangs must stay >= replicas=%d throughout (surge-before-drain): observed servingGangs=%d aliveGangs=%d",
				replicas, servingGangs, aliveGangs)
			g.Expect(aliveGangs).To(BeNumerically("<=", replicas+maxSurgeN),
				"alive engine gangs (grouped by instance-index) must stay <= replicas+maxSurge=%d (distinct-surge-index): observed aliveGangs=%d",
				replicas+maxSurgeN, aliveGangs)
		})
		defer stop()

		// Bump BOTH Leader and Worker images — multi-pod renders from both
		// PodSpecs, so bumping only one leaves the gang hash unchanged and
		// the rollout never triggers.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "gang-floor")
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Convergence: engine advances off v1, all 4 gangs Ready, group Idle.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "gang-floor")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(replicas))
			for _, is := range cs.Lifecycle.InstanceStatuses {
				g.Expect(is.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"every engine gang must be Ready on v2")
			}
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"group must converge to Idle after rollout completes")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		// A fresh surge index must have been observed mid-roll — proves a
		// real per-gang surge (not an in-place / recreate reusing the
		// source index).
		Expect(sawFreshIdx).To(BeTrue(),
			"a fresh surge instance-index must appear during the roll (proves per-gang surge, not in-place/recreate)")
	})
})

// fullyServingGangs returns the number of instance-index buckets whose
// every pod is serving (ContainersReady AND the serving gate True). A gang
// is a capacity unit only when the whole gang serves — a partially-ready
// gang carries no traffic — so the serving floor counts gangs, not pods.
func fullyServingGangs(by map[int32][]*corev1.Pod) int {
	n := 0
	for _, pods := range by {
		if len(pods) == 0 {
			continue
		}
		all := true
		for _, p := range pods {
			if !podreadiness.IsContainersReady(p) || !podreadiness.IsServing(p) {
				all = false
				break
			}
		}
		if all {
			n++
		}
	}
	return n
}
