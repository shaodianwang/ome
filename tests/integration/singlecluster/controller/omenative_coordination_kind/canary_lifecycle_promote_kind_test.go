package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior test — manual canary promotion lifecycle.
//
// Drives a manual two-step canary end-to-end on a real kubelet: the paused
// step serves the declared capacity split with the declared traffic weights,
// a promote whose value is not the canary hash is inert, an explicit
// rollout-rollback=false never arms a rollback, and the matching promote
// carries the rollout through the final step to completion — all pods on the
// canary revision, the done sentinel parked past the last step, the stable
// identity dropped, and the consumed promote annotation removed.
var _ = ginkgo.Describe("behavior — canary manual promote lifecycle", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "canary-lp-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("holds the split at a manual step, ignores non-matching controls, completes on promote", func() {
		// Config: 4-replica engine, no canary yet — converge fully on v1 so
		// the plan added later observes a clean stable revision.
		const isvcName = "canary-lp1"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(4),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var stableHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(4)))
			rev := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent).Status.CurrentRevision
			g.Expect(rev).NotTo(BeEmpty())
			stableHash = query.RevisionFromName(rev).Hash()
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: add a manual canary plan (50% capacity, 25% traffic, bare
		// pause) and bump the image so a canary revision is created.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("50%"), Traffic: 25, Pause: &v1beta1.RolloutPause{}},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			}}}
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the manual step parks Paused serving exactly the declared
		// split — 2 ready canary pods + 2 ready stable pods (capacity), and
		// 25/75 external weights (traffic). Capacity and traffic stay
		// independent: 50% staged capacity must not leak into the weights.
		var canaryHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			canaryHash = c.Status.Canary.CanaryRevisionHash
			g.Expect(canaryHash).NotTo(BeEmpty())
			g.Expect(canaryHash).NotTo(Equal(stableHash))
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Canary.StableRevisionHash).To(Equal(stableHash))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(25)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))

			byRev := map[string]int{}
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp != nil || !podIsReady(&pod) {
					continue
				}
				byRev[pod.Labels[query.LabelRevisionHash]]++
			}
			g.Expect(byRev[canaryHash]).To(Equal(2), "50%% capacity of 4 replicas must stage 2 canary pods")
			g.Expect(byRev[stableHash]).To(Equal(2), "the stable revision must keep the complement")

			// The held step is a settled state for the engine IR: partition 2
			// on spec.pacing, Ready=True/Staged.
			expectIRStagedAt(g, ns.Name, isvcName, v1beta1.EngineComponent, 2)

			traffic := trafficForComponent(c, v1beta1.EngineComponent)
			g.Expect(traffic).To(HaveLen(2), "expected a canary/stable weight pair, got %s", describeTraffic(traffic))
			byTag := map[string]int32{}
			canaryIsLatest := false
			for _, target := range traffic {
				byTag[target.Tag] = target.Percent
				if target.Tag == "canary" {
					canaryIsLatest = target.LatestRevision
				}
			}
			g.Expect(byTag).To(HaveKeyWithValue("canary", int32(25)),
				"declared traffic, not staged capacity, must set the canary weight: %s", describeTraffic(traffic))
			g.Expect(byTag).To(HaveKeyWithValue("stable", int32(75)))
			g.Expect(canaryIsLatest).To(BeTrue())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: promote with a value that is NOT the canary hash (the stable
		// hash here). A promote only matches the recorded canary revision, so
		// this must hold the step, not advance it.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations[constants.RolloutPromoteAnnotation] = stableHash
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)),
				"a promote value that is not the canary hash must not advance the step")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
		}, 10*time.Second, 500*time.Millisecond).Should(Succeed())

		// Action: write an explicit rollout-rollback=false. The rollback verb
		// is a boolean whose false is identical to absence — it must not arm a
		// rollback or disturb the paused step.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Annotations[constants.RolloutRollbackAnnotation] = "false"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.RolledBackRevisionHash).To(BeEmpty(),
				"rollout-rollback=false must never arm a rollback")
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			if ir.Spec.Pacing != nil {
				g.Expect(ir.Spec.Pacing.RollbackToRevision).To(BeNil(),
					"rollout-rollback=false must not point the IR at a rollback target")
			}
			// The IR keeps reporting the held step as Staged for as long as
			// the step is held.
			expectIRStagedAt(g, ns.Name, isvcName, v1beta1.EngineComponent, 2)
		}, 10*time.Second, 500*time.Millisecond).Should(Succeed())

		// Action: promote the real canary hash. This opens the manual gate,
		// the plan advances into the ungated final step, and the rollout
		// completes.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Annotations[constants.RolloutPromoteAnnotation] = canaryHash
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: completion converges every serving pod onto the canary
		// revision and parks the done sentinel — CurrentStep == len(steps),
		// weight 100, phase Stable, the pre-canary stable identity dropped,
		// and the applied promote annotation consumed off the ISVC.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(2)),
				"a finished canary keeps its status with the done sentinel, not a cleared status")
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(100)))
			g.Expect(c.Status.Canary.StableRevisionHash).To(BeEmpty(),
				"completion must drop the pre-canary stable identity")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhaseStable))
			g.Expect(c.Annotations).NotTo(HaveKey(constants.RolloutPromoteAnnotation),
				"the applied promote must be consumed")

			ready := 0
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp != nil {
					continue
				}
				g.Expect(pod.Labels[query.LabelRevisionHash]).To(Equal(canaryHash),
					"pod %s must be on the promoted revision", pod.Name)
				if podIsReady(&pod) {
					ready++
				}
			}
			g.Expect(ready).To(Equal(4))

			traffic := trafficForComponent(c, v1beta1.EngineComponent)
			g.Expect(traffic).To(HaveLen(1), "completed canary must carry a single full-weight target, got %s", describeTraffic(traffic))
			g.Expect(traffic[0].Tag).To(Equal("canary"))
			g.Expect(traffic[0].Percent).To(Equal(int32(100)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
