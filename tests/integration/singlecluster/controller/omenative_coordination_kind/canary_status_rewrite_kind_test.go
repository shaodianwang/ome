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

// Behavior test — a hand-written canary status cannot reopen a rollout.
//
// The InferenceService status is a subresource nothing validates, so a
// kubectl edit on it, or a tool, can leave the engine canary's recorded
// step below zero or touch a finished unit again. The controller reads
// such a record back from what the rollout already did, never as step 0:
//
//   - a drain at 100% keeps its last step, its 100% traffic, its drain
//     window and the one old Instance it still holds; the traffic never
//     splits again and the phase never returns to Paused;
//   - a finished unit given a stable identity equal to the canary hash
//     drops it again, opens no run, records no second completion and
//     creates no pod.
//
// Four single-pod engine Instances roll through a two-step manual canary
// (50% capacity at 25% traffic, then 100%) whose drain window is long
// enough to write into on a real kubelet. Each write edits one field of
// the engine unit's own record, the way a hand edit of
// status.components.engine.canary does.

const (
	// canaryRewriteDrainSeconds keeps the last old Instance held long
	// enough for the status write and its observation window to land
	// inside the drain.
	canaryRewriteDrainSeconds = int32(90)
	// canaryRewriteWindow spans several controller passes after each
	// status write: a reopened step 0 shows within one.
	canaryRewriteWindow = 20 * time.Second
)

// Cells: canary/Draining/record.legacy[negativeStep], canary/Done/record.legacy[stableEqualsCanary]
var _ = ginkgo.Describe("behavior — a hand-written canary status cannot reopen a rollout", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "canary-rw-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("keeps a drain at 100% and a finished unit where they are when the record is rewritten", func() {
		const isvcName = "canary-rw"
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

		ginkgo.By("adding a two-step manual canary with a drain window and bumping the image")
		drain := canaryRewriteDrainSeconds
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{
					Steps: []v1beta1.RolloutGroupStep{
						{Capacity: intstr.FromString("50%"), Traffic: 25, Pause: &v1beta1.RolloutPause{}},
						{Capacity: intstr.FromString("100%"), Traffic: 100},
					},
					ScaleDownDelaySeconds: &drain,
				},
			}}}
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		var canaryHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			unit := engineCanaryRecord(g, c)
			canaryHash = unit.CanaryRevisionHash
			g.Expect(canaryHash).NotTo(BeEmpty())
			g.Expect(canaryHash).NotTo(Equal(stableHash))
			g.Expect(unit.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).To(Equal(v1beta1.RolloutPhasePaused))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("promoting the held step so the ladder reaches 100% and drains")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations[constants.RolloutPromoteAnnotation] = canaryHash
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The drain: the last step entered, all traffic on the canary, three
		// Instances on it and the one held Instance still serving the stable
		// revision until the window elapses.
		var heldStablePod types.UID
		var drainDeadline time.Time
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			unit := engineCanaryRecord(g, c)
			g.Expect(unit.CurrentStep).To(Equal(int32(1)), "the ladder must be at its last step")
			g.Expect(unit.ObservedTrafficWeight).To(Equal(int32(100)))
			g.Expect(unit.StepEnteredTime).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).To(Equal(v1beta1.RolloutPhasePromoting))
			expectCanaryOwnsTraffic(g, c)
			byRev := readyPodsByRevision(g, ns.Name, isvcName)
			g.Expect(byRev[canaryHash]).To(HaveLen(replicas-1), "the drain holds exactly one Instance on the stable revision")
			g.Expect(byRev[stableHash]).To(HaveLen(1))
			heldStablePod = byRev[stableHash][0].UID
			drainDeadline = unit.StepEnteredTime.Add(time.Duration(drain) * time.Second)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("writing a negative step into the drain's record")
		writeEngineCanaryRecord(ns.Name, isvcName, func(cs *v1beta1.CanaryStatus) { cs.CurrentStep = -1 })
		Expect(time.Now().Before(drainDeadline)).To(BeTrue(),
			"the write must land while the drain still holds an Instance; it landed after the window of %ds", drain)

		// The drain goes on from its record: never step 0, never a split,
		// never Paused, the held Instance kept while the window is open.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			unit := engineCanaryRecord(g, c)
			g.Expect(unit.CurrentStep).NotTo(Equal(int32(0)), "a negative step must be read back as the drain's step, not as step 0")
			g.Expect(unit.ObservedTrafficWeight).To(Equal(int32(100)), "the drain's traffic must stay on the canary")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).NotTo(Equal(v1beta1.RolloutPhasePaused),
				"a rewritten record must not re-enter the manual step")
			expectCanaryOwnsTraffic(g, c)
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			if ir.Spec.Pacing != nil && ir.Spec.Pacing.Partition != nil {
				g.Expect(*ir.Spec.Pacing.Partition).To(BeNumerically("<=", 1), "the 50%% split must not be staged again")
			}
			if time.Now().Before(drainDeadline.Add(-5 * time.Second)) {
				byRev := readyPodsByRevision(g, ns.Name, isvcName)
				g.Expect(byRev[stableHash]).To(HaveLen(1), "the drain must keep its held Instance while the window is open")
				g.Expect(byRev[stableHash][0].UID).To(Equal(heldStablePod))
			}
		}, canaryRewriteWindow, kindPollInterval).Should(Succeed())

		ginkgo.By("the drain elapses and the rollout completes")
		var closedAt time.Time
		var podsAtCompletion map[types.UID]string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			expectEngineCanaryFinished(g, c, canaryHash, replicas)
			g.Expect(c.Status.Rollout).NotTo(BeNil())
			g.Expect(c.Status.Rollout.ActiveRun).To(BeNil(), "the run must close once the cutover is complete")
			g.Expect(c.Status.Rollout.LastRun).NotTo(BeNil())
			g.Expect(c.Status.Rollout.LastRun.Outcome).To(Equal(v1beta1.RolloutRunCompleted))
			g.Expect(c.Status.Rollout.LastRun.ClosedAt).NotTo(BeNil())
			closedAt = c.Status.Rollout.LastRun.ClosedAt.Time
			podsAtCompletion = livePodRevisions(g, ns.Name, isvcName)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// finishedUnitUntouched is what every pass after a write on the
		// finished unit must keep: the step never 0, phase Stable, one full
		// weight target, no run open, the one completion, the same pods.
		finishedUnitUntouched := func(g Gomega, why string) {
			c := freshISVC(g, ns.Name, isvcName)
			unit := engineCanaryRecord(g, c)
			g.Expect(unit.CurrentStep).NotTo(Equal(int32(0)), "%s: a finished unit must not re-run step 0", why)
			g.Expect(unit.ObservedTrafficWeight).To(Equal(int32(100)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).To(Equal(v1beta1.RolloutPhaseStable),
				"%s: a finished unit must keep reading Stable", why)
			expectCanaryOwnsTraffic(g, c)
			g.Expect(c.Status.Rollout).NotTo(BeNil())
			g.Expect(c.Status.Rollout.ActiveRun).To(BeNil(), "%s: a rewritten finished record must not open a run", why)
			g.Expect(c.Status.Rollout.LastRun).NotTo(BeNil())
			g.Expect(c.Status.Rollout.LastRun.ClosedAt).NotTo(BeNil())
			g.Expect(c.Status.Rollout.LastRun.ClosedAt.Time).To(BeTemporally("==", closedAt),
				"%s: a finished rollout must not be recorded as completed a second time", why)
			g.Expect(livePodRevisions(g, ns.Name, isvcName)).To(Equal(podsAtCompletion),
				"%s: a rewritten finished record must create or replace no pod", why)
		}

		ginkgo.By("writing a stable identity equal to the canary hash into the finished record")
		writeEngineCanaryRecord(ns.Name, isvcName, func(cs *v1beta1.CanaryStatus) { cs.StableRevisionHash = canaryHash })
		Consistently(func(g Gomega) {
			finishedUnitUntouched(g, "stable identity written equal to the canary hash")
		}, canaryRewriteWindow, kindPollInterval).Should(Succeed())

		ginkgo.By("the record reads finished again with the stable identity dropped")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			expectEngineCanaryFinished(g, c, canaryHash, replicas)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})

// engineCanaryRecord is the engine unit's own canary record, the one the
// controller reads; the top-level status.canary is a projection of it.
func engineCanaryRecord(g Gomega, c *v1beta1.InferenceService) *v1beta1.CanaryStatus {
	unit, ok := c.Status.Components[v1beta1.EngineComponent]
	g.ExpectWithOffset(1, ok).To(BeTrue(), "the engine must have a component status")
	g.ExpectWithOffset(1, unit.Canary).NotTo(BeNil(), "the engine unit must carry its canary record")
	return unit.Canary
}

// writeEngineCanaryRecord edits one field of the engine unit's canary
// record through the status subresource, the way a hand edit of
// status.components.engine.canary does; nothing else in the status moves.
func writeEngineCanaryRecord(namespace, isvcName string, edit func(*v1beta1.CanaryStatus)) {
	EventuallyWithOffset(1, func(g Gomega) {
		cur := freshISVC(g, namespace, isvcName)
		unit := cur.Status.Components[v1beta1.EngineComponent]
		g.Expect(unit.Canary).NotTo(BeNil())
		edit(unit.Canary)
		cur.Status.Components[v1beta1.EngineComponent] = unit
		g.Expect(k8sClient.Status().Update(suiteCtx, cur)).To(Succeed())
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

// expectCanaryOwnsTraffic asserts the engine's traffic targets send
// everything to the canary: its target carries 100 and no stable target
// carries weight.
func expectCanaryOwnsTraffic(g Gomega, c *v1beta1.InferenceService) {
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

// expectEngineCanaryFinished asserts the engine unit reads finished: the
// done sentinel, all traffic on the canary, the pre-canary identity
// dropped, phase Stable and every live pod Ready on the canary revision.
func expectEngineCanaryFinished(g Gomega, c *v1beta1.InferenceService, canaryHash string, replicas int) {
	unit := engineCanaryRecord(g, c)
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

// readyPodsByRevision buckets the live, Ready pods of the service by the
// revision hash they carry.
func readyPodsByRevision(g Gomega, namespace, isvcName string) map[string][]corev1.Pod {
	byRev := map[string][]corev1.Pod{}
	for _, pod := range listOMENativePods(g, namespace, isvcName) {
		if pod.DeletionTimestamp != nil || !podIsReady(&pod) {
			continue
		}
		byRev[pod.Labels[query.LabelRevisionHash]] = append(byRev[pod.Labels[query.LabelRevisionHash]], pod)
	}
	return byRev
}

// livePodRevisions maps every live pod of the service to the revision hash
// it carries, so two reads can be compared pod for pod.
func livePodRevisions(g Gomega, namespace, isvcName string) map[types.UID]string {
	out := map[types.UID]string{}
	for _, pod := range listOMENativePods(g, namespace, isvcName) {
		if pod.DeletionTimestamp != nil {
			continue
		}
		out[pod.UID] = pod.Labels[query.LabelRevisionHash]
	}
	return out
}
