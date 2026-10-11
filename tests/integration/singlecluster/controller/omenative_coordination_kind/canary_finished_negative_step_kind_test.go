package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior test — a negative step written on a finished canary reopens
// nothing.
//
// The InferenceService status is a subresource nothing validates, so a
// kubectl edit on it can leave the engine canary's recorded step below
// zero after the rollout has finished. A finished unit reads Stable at
// the done sentinel; the controller must read the rewritten record back
// from that, not as step 0: the unit stays at the sentinel with phase
// Stable, all traffic stays on the promoted revision, no new rollout run
// opens, the one completion is not recorded again and no pod is created.
//
// Four single-pod engine Instances roll through a two-step manual canary
// (50% capacity at 25% traffic, then 100%) to completion; then one field
// of status.components.engine.canary, the record the controller reads, is
// written to -1 and nothing else moves.

// finishedCanaryRewriteWindow spans several controller passes after the
// write: a new run or a reopened step 0 shows within one.
const finishedCanaryRewriteWindow = 20 * time.Second

// Cells: canary/Done/record.legacy[negativeStep]
var _ = ginkgo.Describe("behavior — a negative step written on a finished canary reopens nothing", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "canary-neg-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("keeps the finished unit at the done sentinel and opens no run", func() {
		const isvcName = "canary-neg"
		const replicas = 4
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(replicas),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var stableHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(replicas)))
			rev := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent).Status.CurrentRevision
			g.Expect(rev).NotTo(BeEmpty())
			stableHash = query.RevisionFromName(rev).Hash()
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("adding a two-step manual canary and bumping the image")
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

		var canaryHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			unit := finishedCanaryUnit(g, c)
			canaryHash = unit.CanaryRevisionHash
			g.Expect(canaryHash).NotTo(BeEmpty())
			g.Expect(canaryHash).NotTo(Equal(stableHash))
			g.Expect(unit.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).To(Equal(v1beta1.RolloutPhasePaused))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("promoting the held step so the rollout finishes")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations[constants.RolloutPromoteAnnotation] = canaryHash
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		var closedAt time.Time
		var podsAtCompletion map[types.UID]string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			expectFinishedCanaryUnit(g, c, canaryHash, replicas)
			g.Expect(c.Status.Rollout).NotTo(BeNil())
			g.Expect(c.Status.Rollout.ActiveRun).To(BeNil(), "the run must close once the cutover is complete")
			g.Expect(c.Status.Rollout.LastRun).NotTo(BeNil())
			g.Expect(c.Status.Rollout.LastRun.Outcome).To(Equal(v1beta1.RolloutRunCompleted))
			g.Expect(c.Status.Rollout.LastRun.ClosedAt).NotTo(BeNil())
			closedAt = c.Status.Rollout.LastRun.ClosedAt.Time
			podsAtCompletion = finishedCanaryPods(g, ns.Name, isvcName)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("writing a negative step into the finished record")
		writeFinishedCanaryStep(ns.Name, isvcName, -1)

		// Every pass after the write: the step never 0, phase Stable, one
		// full weight target, no run open, the one completion, the same pods.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			unit := finishedCanaryUnit(g, c)
			g.Expect(unit.CurrentStep).NotTo(Equal(int32(0)), "a finished unit must not re-run step 0")
			g.Expect(unit.ObservedTrafficWeight).To(Equal(int32(100)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).To(Equal(v1beta1.RolloutPhaseStable),
				"a finished unit must keep reading Stable")
			expectFinishedCanaryOwnsTraffic(g, c)
			g.Expect(c.Status.Rollout).NotTo(BeNil())
			g.Expect(c.Status.Rollout.ActiveRun).To(BeNil(), "a rewritten finished record must not open a run")
			g.Expect(c.Status.Rollout.LastRun).NotTo(BeNil())
			g.Expect(c.Status.Rollout.LastRun.ClosedAt).NotTo(BeNil())
			g.Expect(c.Status.Rollout.LastRun.ClosedAt.Time).To(BeTemporally("==", closedAt),
				"a finished rollout must not be recorded as completed a second time")
			g.Expect(finishedCanaryPods(g, ns.Name, isvcName)).To(Equal(podsAtCompletion),
				"a rewritten finished record must create or replace no pod")
		}, finishedCanaryRewriteWindow, kindPollInterval).Should(Succeed())

		ginkgo.By("the record reads finished again")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			expectFinishedCanaryUnit(g, c, canaryHash, replicas)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})

// finishedCanaryUnit is the engine unit's own canary record, the one the
// controller reads; the top-level status.canary is a projection of it.
func finishedCanaryUnit(g Gomega, c *v1beta1.InferenceService) *v1beta1.CanaryStatus {
	unit, ok := c.Status.Components[v1beta1.EngineComponent]
	g.ExpectWithOffset(1, ok).To(BeTrue(), "the engine must have a component status")
	g.ExpectWithOffset(1, unit.Canary).NotTo(BeNil(), "the engine unit must carry its canary record")
	return unit.Canary
}

// writeFinishedCanaryStep writes step into the engine unit's canary record
// through the status subresource, the way a hand edit of
// status.components.engine.canary.currentStep does; nothing else moves.
func writeFinishedCanaryStep(namespace, isvcName string, step int32) {
	EventuallyWithOffset(1, func(g Gomega) {
		cur := freshISVC(g, namespace, isvcName)
		unit := cur.Status.Components[v1beta1.EngineComponent]
		g.Expect(unit.Canary).NotTo(BeNil())
		unit.Canary.CurrentStep = step
		cur.Status.Components[v1beta1.EngineComponent] = unit
		g.Expect(k8sClient.Status().Update(suiteCtx, cur)).To(Succeed())
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

// expectFinishedCanaryOwnsTraffic asserts the engine's traffic targets send
// everything to the canary: its target carries 100 and no stable target
// carries weight.
func expectFinishedCanaryOwnsTraffic(g Gomega, c *v1beta1.InferenceService) {
	traffic := trafficForComponent(c, v1beta1.EngineComponent)
	g.ExpectWithOffset(1, traffic).NotTo(BeEmpty(), "the engine must publish traffic targets")
	canary := int32(-1)
	for _, target := range traffic {
		switch target.Tag {
		case "canary":
			canary = target.Percent
		case "stable":
			g.ExpectWithOffset(1, target.Percent).To(BeZero(),
				"the stable revision must not receive traffic again: %s", describeTraffic(traffic))
		}
	}
	g.ExpectWithOffset(1, canary).To(Equal(int32(100)), "the canary must own all traffic: %s", describeTraffic(traffic))
}

// expectFinishedCanaryUnit asserts the engine unit reads finished: the done
// sentinel, all traffic on the canary, the pre-canary identity dropped,
// phase Stable and every live pod Ready on the canary revision.
func expectFinishedCanaryUnit(g Gomega, c *v1beta1.InferenceService, canaryHash string, replicas int) {
	unit := finishedCanaryUnit(g, c)
	g.ExpectWithOffset(1, unit.CurrentStep).To(Equal(int32(2)), "a finished canary keeps the done sentinel")
	g.ExpectWithOffset(1, unit.ObservedTrafficWeight).To(Equal(int32(100)))
	g.ExpectWithOffset(1, unit.StableRevisionHash).To(BeEmpty(), "completion drops the pre-canary stable identity")
	g.ExpectWithOffset(1, c.Status.Components[v1beta1.EngineComponent].RolloutPhase).To(Equal(v1beta1.RolloutPhaseStable))
	traffic := trafficForComponent(c, v1beta1.EngineComponent)
	g.ExpectWithOffset(1, traffic).To(HaveLen(1), "a completed canary carries one full-weight target, got %s", describeTraffic(traffic))
	g.ExpectWithOffset(1, traffic[0].Tag).To(Equal("canary"))
	g.ExpectWithOffset(1, traffic[0].Percent).To(Equal(int32(100)))
	ready := 0
	for _, pod := range listOMENativePods(g, c.Namespace, c.Name) {
		if pod.DeletionTimestamp != nil {
			continue
		}
		g.ExpectWithOffset(1, pod.Labels[query.LabelRevisionHash]).To(Equal(canaryHash), "pod %s must be on the promoted revision", pod.Name)
		if podIsReady(&pod) {
			ready++
		}
	}
	g.ExpectWithOffset(1, ready).To(Equal(replicas))
}

// finishedCanaryPods maps every live pod of the service to the revision
// hash it carries, so two reads can be compared pod for pod.
func finishedCanaryPods(g Gomega, namespace, isvcName string) map[types.UID]string {
	out := map[types.UID]string{}
	for _, pod := range listOMENativePods(g, namespace, isvcName) {
		if pod.DeletionTimestamp != nil {
			continue
		}
		out[pod.UID] = pod.Labels[query.LabelRevisionHash]
	}
	return out
}
