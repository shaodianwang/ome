package omenative_coordination_kind

import (
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Concurrent gang-surge index-collision regression gate.
//
// Pins the distinct-surge-index contract: when >=2 multi-pod (gang) Instances surge in the
// SAME reconcile pass, each replacement gang MUST claim a DISTINCT
// instance-index. Without it, every sibling gang re-picks the
// same lowest-free index from the start-of-pass snapshot; when that one
// shared surge becomes Ready, the drain-on-ready path releases ALL sharing
// sources at once — a fleet-wipe. gangSurgeUpdate records each
// claim on the source's Operation.SurgeIndex and AllocateSurgeIndex
// excludes in-flight surge slots, so siblings fan out to 3,4,5,...
//
// This is the CI-automated, 100ms-sampled distinct-alive-index guard.
// KIND runs in CI without accelerators, so it catches a regression on
// every push.
//
// Shape: engine multi-node (leader + worker.size=1 => 2 pods/gang),
// minReplicas=maxReplicas=3, SurgeThenDrain, per-Component RollingUpdate
// with MaxSurge=2 so up to two gangs surge concurrently. Decoder rounds
// the RollingUpdate group out to the required >=2 Components; the
// invariant under test is on the engine gangs.
//
// A single engine image bump triggers all three gangs to roll; MaxSurge=2
// forces overlapping surge windows where the collision would occur.
//
// Assertions:
//   - (a) DISTINCT-INDEX: on every 100ms sample, no single engine
//     instance-index is ever occupied by pods from two DISTINCT gangs.
//     A gang is identified by (instance-index, incarnation); the source
//     and its replacement always live at different indices under the
//     gang-surge scheme, so two incarnations sharing one index is the
//     fleet-wipe collision signature.
//   - (b) CONVERGENCE: the fleet reconverges to exactly 3 Ready engine
//     Instances on the new revision.
//   - (c) SERVING FLOOR: engine serving never craters — it stays at or
//     above minReplicas throughout (surge-then-drain keeps the source
//     serving until the replacement is in rotation).
var _ = ginkgo.Describe("concurrent gang surge — distinct index per gang", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "gang-surge-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("3 gang Instances surge on one image bump without ever sharing an instance-index", func() {
		const (
			isvcName    = "gang-surge"
			replicas    = 3
			workerSize  = 1 // leader + 1 worker => 2 pods/gang
			podsPerGang = workerSize + 1
		)
		maxSurge := intstr.FromInt(2)
		maxUnavail := intstr.FromInt(0)

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithEngineReplicas(replicas),
			WithEngineWorkerSize(workerSize),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategySurgeThenDrain),
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

		ginkgo.By("waiting for 3 engine gang Instances to reach Ready at v1")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(replicas)))
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(replicas))
			for _, s := range insts {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"engine instance %d must be Ready at v1", s.Index)
			}
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, isvcName)
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision

		// (a) DISTINCT-INDEX sampler. Bucket every alive engine pod by
		// instance-index, then collect the set of incarnations at each
		// index. A healthy gang surge keeps the source at index i and its
		// replacement at a fresh index k != i, so each index carries pods
		// of exactly ONE incarnation. The fleet-wipe collision is two
		// DISTINCT gangs (two incarnations) landing on the SAME index.
		//
		// Also track the maximum distinct alive index count so a passing
		// run proves the surge path actually fired (>= replicas+1 distinct
		// indices means at least one replacement gang was created at a new
		// index) rather than trivially passing because no surge happened.
		var maxDistinctIdx int
		var maxDistinctMu sync.Mutex
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())

			byIdx := map[int32]map[int64]struct{}{}
			for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
				idx, ok := query.InstanceIdxFromLabels(p)
				if !ok {
					continue
				}
				inc, ok := query.InstanceIncarnationFromLabels(p)
				if !ok {
					continue
				}
				if byIdx[idx] == nil {
					byIdx[idx] = map[int64]struct{}{}
				}
				byIdx[idx][inc] = struct{}{}
			}
			maxDistinctMu.Lock()
			if len(byIdx) > maxDistinctIdx {
				maxDistinctIdx = len(byIdx)
			}
			maxDistinctMu.Unlock()
			for idx, incs := range byIdx {
				g.Expect(incs).To(HaveLen(1),
					"instance-index %d hosts %d distinct gangs (incarnations %v); "+
						"two gangs sharing one index is the shared-surge-index "+
						"fleet-wipe signature", idx, len(incs), incarnationSet(incs))
			}
		})
		defer stop()

		ginkgo.By("bumping engine image v1 -> v2 (all 3 gangs roll; MaxSurge=2 overlaps their surge windows)")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			// Multi-pod reads Leader.PodSpec + Worker.PodSpec; the top-level
			// Engine.PodSpec alone leaves the rendered gang hash unchanged
			// and the rollout never triggers. Bump BOTH.
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// (b) CONVERGENCE: exactly 3 Ready engine Instances on the new rev.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(replicas),
				"fleet must reconverge to exactly %d gang Instances (not collapse)", replicas)
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(replicas)),
				"engine must return to %d serving Instances on v2", replicas)
			for _, s := range insts {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"engine instance %d must be Ready on v2; got %q", s.Index, s.Phase)
			}
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"group must converge to Idle after the rollout completes")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		// Surge-path-fired proof, evaluated after the sampler has stopped so
		// its captured extremum is stable. The serving-floor invariant (c)
		// is asserted continuously in the companion It below.
		//
		// maxDistinctIdx >= replicas+1 means at least one replacement gang
		// was created at a fresh index concurrently with the surviving
		// sources — i.e. the surge path fired and the distinct-index
		// invariant was actually stressed. If it stayed at replicas the
		// test would have passed for the wrong reason (no surge, nothing
		// to collide).
		maxDistinctMu.Lock()
		observedDistinct := maxDistinctIdx
		maxDistinctMu.Unlock()
		Expect(observedDistinct).To(BeNumerically(">=", replicas+1),
			"surge path must fire: expected to observe >= %d distinct engine "+
				"instance-indices (sources + >=1 concurrent replacement), got max=%d — "+
				"test would pass trivially with no surge", replicas+1, observedDistinct)

		// Post-converge: exactly replicas*podsPerGang alive engine pods
		// (one gang per Ready Instance, no orphaned surge gang left behind).
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			alive := 0
			for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
				if p.DeletionTimestamp != nil {
					continue
				}
				alive++
			}
			g.Expect(alive).To(Equal(replicas*podsPerGang),
				"post-converge engine must have exactly %d pods (%d gangs x %d pods); got %d",
				replicas*podsPerGang, replicas, podsPerGang, alive)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("engine serving never craters below minReplicas during the concurrent surge", func() {
		// Companion serving-floor spec. Kept separate from the distinct-
		// index spec so a floor violation and an index collision fail with
		// distinct, self-describing messages instead of one compound run.
		const (
			isvcName   = "gang-surge-floor"
			replicas   = 3
			workerSize = 1
		)
		maxSurge := intstr.FromInt(2)
		maxUnavail := intstr.FromInt(0)

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithEngineReplicas(replicas),
			WithEngineWorkerSize(workerSize),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategySurgeThenDrain),
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

		ginkgo.By("waiting for 3 engine gang Instances to reach Ready at v1")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(replicas)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, isvcName)
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision

		// Serving floor: minReplicas is the hard floor. SurgeThenDrain
		// keeps each source serving until its replacement is PodReady, so
		// serving should never dip below minReplicas. A collapse to 1 (the
		// fleet-wipe symptom) or any dip below minReplicas trips this.
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			serving := snap.Serving(v1beta1.EngineComponent)
			g.Expect(serving).To(BeNumerically(">=", replicas),
				"engine serving must stay >= minReplicas(%d) throughout the surge "+
					"(MaxUnavailable=0); observed serving=%d — a collapse here is the "+
					"fleet-wipe symptom", replicas, serving)
		})
		defer stop()

		ginkgo.By("bumping engine image v1 -> v2")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(replicas)),
				"engine must return to %d serving Instances on v2", replicas)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()
	})
})

// incarnationSet renders the incarnation keys of a set for assertion
// failure messages (map iteration order is unstable but the message is
// diagnostic, not asserted-on).
func incarnationSet(incs map[int64]struct{}) []int64 {
	out := make([]int64, 0, len(incs))
	for inc := range incs {
		out = append(out, inc)
	}
	return out
}
