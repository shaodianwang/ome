package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior tests — RolloutPolicy-sourced rollout runs.
//
// The rollout run is the mechanism the executors consume: at open the
// controller renders the effective plan (a group's inline progression or its
// referenced RolloutPolicy body), pins it into status.rollout.activeRun, and
// the canary step machine indexes the pinned plan — never the live spec — for
// the run's duration. These specs drive that contract end-to-end on a real
// kubelet: a policy-sourced canary pins with Policy provenance and executes to
// a Completed lastRun; a mid-run policy edit is inert (drift condition, pinned
// digest stable) and lands at the next run; a ref to a missing policy PARKS
// the roll (old revision keeps serving) and recovers watch-driven when the
// policy appears; and the one-shot repin verb swaps a tightened ladder into
// the live run without restarting it.

// rpCanarySteps builds a canary ladder from traffic weights: every
// intermediate step stages 50% capacity and holds on a bare manual pause; the
// final step (traffic must be 100 by validation) is ungated at 100% capacity.
func rpCanarySteps(traffics ...int32) []v1beta1.RolloutGroupStep {
	steps := make([]v1beta1.RolloutGroupStep, 0, len(traffics))
	for i, t := range traffics {
		if i == len(traffics)-1 {
			steps = append(steps, v1beta1.RolloutGroupStep{
				Capacity: intstr.FromString("100%"), Traffic: t,
			})
			continue
		}
		steps = append(steps, v1beta1.RolloutGroupStep{
			Capacity: intstr.FromString("50%"), Traffic: t,
			Pause: &v1beta1.RolloutPause{},
		})
	}
	return steps
}

// mkRunPolicy builds a namespaced RolloutPolicy carrying one canary body.
func mkRunPolicy(namespace, name string, steps []v1beta1.RolloutGroupStep) *v1beta1.RolloutPolicy {
	return &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: v1beta1.RolloutPolicySpec{
			Canary: &v1beta1.GroupCanary{Steps: steps},
		},
	}
}

// policyRefRollout is a one-group spec.rollout whose engine group's only
// progression is a declared-canary policy ref (no inline arm).
func policyRefRollout(policyName string) *v1beta1.RolloutSpec {
	return &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		PolicyRef: &v1beta1.RolloutPolicyRef{
			Name:        policyName,
			Progression: v1beta1.RolloutProgressionCanary,
		},
	}}}
}

// runCondition reads one of the run-model duck conditions (RolloutPlanReady /
// RolloutPlanDrift) off the ISVC status.
func runCondition(c *v1beta1.InferenceService, condType string) *apis.Condition {
	return c.Status.GetCondition(apis.ConditionType(condType))
}

// pinnedRun asserts an active run exists with exactly one pinned group and
// returns it (retryable inside Eventually).
func pinnedRun(g Gomega, c *v1beta1.InferenceService) *v1beta1.RolloutRun {
	g.Expect(c.Status.Rollout).NotTo(BeNil(), "status.rollout must be populated")
	g.Expect(c.Status.Rollout.ActiveRun).NotTo(BeNil(), "a run must be pinned")
	g.Expect(c.Status.Rollout.ActiveRun.Plan.Groups).To(HaveLen(1))
	return c.Status.Rollout.ActiveRun
}

// convergeStableEngine waits for the fixture's engine to fully serve on its
// initial revision and returns that revision's hash — the stable identity the
// canary assertions compare against.
func convergeStableEngine(ns, isvcName string, replicas int32) string {
	var stableHash string
	Eventually(func(g Gomega) {
		c := freshISVC(g, ns, isvcName)
		engCS := c.Status.Components[v1beta1.EngineComponent]
		g.Expect(engCS.Lifecycle).NotTo(BeNil())
		g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(replicas))
		rev := freshInferenceReplica(g, ns, isvcName, v1beta1.EngineComponent).Status.CurrentRevision
		g.Expect(rev).NotTo(BeEmpty(), "the stable revision must be promoted before the canary starts")
		stableHash = query.RevisionFromName(rev).Hash()
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	return stableHash
}

// attachRefAndBump attaches the policy-ref rollout block and bumps the engine
// image in one update — the shape that mints a new revision and opens a run.
func attachRefAndBump(ns, isvcName, policyName, image string) {
	Eventually(func(g Gomega) {
		cur := freshISVC(g, ns, isvcName)
		cur.Spec.Rollout = policyRefRollout(policyName)
		cur.Spec.Engine.PodSpec.Containers[0].Image = image
		g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

// promoteCanary writes the promote verb once the PREVIOUS promote (if any) is
// durably consumed: annotation observed absent AND the durable
// status.canary.promotedThrough record cleared. Re-writing the same hash
// while the record still holds it would be swept as residue of the applied
// promote, not honored as a new one.
func promoteCanary(ns, isvcName, canaryHash string) {
	Eventually(func(g Gomega) {
		c := freshISVC(g, ns, isvcName)
		g.Expect(c.Annotations).NotTo(HaveKey(constants.RolloutPromoteAnnotation),
			"the prior promote must be consumed before a new one is written")
		if c.Status.Canary != nil {
			g.Expect(c.Status.Canary.PromotedThrough).To(BeEmpty(),
				"the durable promote record must clear before a new promote")
		}
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	Eventually(func(g Gomega) {
		cur := freshISVC(g, ns, isvcName)
		if cur.Annotations == nil {
			cur.Annotations = map[string]string{}
		}
		cur.Annotations[constants.RolloutPromoteAnnotation] = canaryHash
		g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

var _ = ginkgo.Describe("behavior — rollout policy runs", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rp-run-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("pins a policy-sourced canary, executes its steps, and records the completed run", func() {
		// Config: 2-replica engine converges on v1 before any rollout block
		// exists, so the run opened later observes a clean stable revision.
		const isvcName = "rp-run1"
		const policyName = "rp-run1-policy"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)
		stableHash := convergeStableEngine(ns.Name, isvcName, 2)

		// Config: the policy carries the whole progression — the group will
		// declare only a canary-kind ref, no inline arm.
		policy := mkRunPolicy(ns.Name, policyName, rpCanarySteps(25, 100))
		Expect(k8sClient.Create(suiteCtx, policy)).To(Succeed())

		// Action: attach the ref and bump the image — the run opens against
		// the freshly minted revision with the policy's rendered body.
		attachRefAndBump(ns.Name, isvcName, policyName, "fake-serving:v2")

		// Invariant: the pinned run carries Policy provenance — source,
		// ref identity, and a portable digest — and the always-current
		// resolution view agrees with the pin (no drift at open).
		var pinnedDigest string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			run := pinnedRun(g, c)
			pg := run.Plan.Groups[0]
			g.Expect(pg.Source).To(Equal(v1beta1.RolloutPlanSourcePolicy))
			g.Expect(pg.PolicyRef).NotTo(BeNil())
			g.Expect(pg.PolicyRef.Name).To(Equal(policyName))
			g.Expect(pg.PortableDigest).To(HavePrefix("rp1:"))
			g.Expect(pg.Group.Canary).NotTo(BeNil(), "the pinned group must carry the rendered canary body")
			g.Expect(pg.Group.Canary.Steps).To(HaveLen(2))
			pinnedDigest = pg.PortableDigest

			g.Expect(c.Status.Rollout.Groups).To(HaveLen(1))
			g.Expect(c.Status.Rollout.Groups[0].Source).To(Equal(v1beta1.RolloutPlanSourcePolicy))
			g.Expect(c.Status.Rollout.Groups[0].ObservedDigest).To(Equal(pg.PortableDigest),
				"the live render must match the pin at open")

			ready := runCondition(c, v1beta1.RolloutPlanReadyCondition)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(ready.Status).To(Equal(corev1.ConditionTrue))
			g.Expect(ready.Reason).To(Equal(v1beta1.RolloutPlanReasonPinned))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the policy's ladder executes — the manual step 0 parks
		// Paused serving the declared 25% weight.
		var canaryHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			canaryHash = c.Status.Canary.CanaryRevisionHash
			g.Expect(canaryHash).NotTo(BeEmpty())
			g.Expect(canaryHash).NotTo(Equal(stableHash))
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(25)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: promote the canary hash through the manual gate.
		promoteCanary(ns.Name, isvcName, canaryHash)

		// Invariant: the run completes — done sentinel, every serving pod on
		// the canary revision, the run closed with a Completed lastRun that
		// keeps the Policy provenance and digest.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(2)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhaseStable))
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp != nil {
					continue
				}
				g.Expect(pod.Labels[query.LabelRevisionHash]).To(Equal(canaryHash),
					"pod %s must be on the promoted revision", pod.Name)
			}

			g.Expect(c.Status.Rollout).NotTo(BeNil())
			g.Expect(c.Status.Rollout.ActiveRun).To(BeNil(), "the completed run must close")
			g.Expect(c.Status.Rollout.LastRun).NotTo(BeNil())
			g.Expect(c.Status.Rollout.LastRun.Outcome).To(Equal(v1beta1.RolloutRunCompleted))
			g.Expect(c.Status.Rollout.LastRun.Groups).To(HaveLen(1))
			g.Expect(c.Status.Rollout.LastRun.Groups[0].Source).To(Equal(v1beta1.RolloutPlanSourcePolicy))
			g.Expect(c.Status.Rollout.LastRun.Groups[0].PortableDigest).To(Equal(pinnedDigest))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("keeps executing the pinned plan through a mid-run policy edit and picks it up next run", func() {
		// Config: converge on v1, then open a run from a 2-step manual
		// canary policy (step 0 at 25% traffic).
		const isvcName = "rp-run2"
		const policyName = "rp-run2-policy"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)
		convergeStableEngine(ns.Name, isvcName, 2)

		policy := mkRunPolicy(ns.Name, policyName, rpCanarySteps(25, 100))
		Expect(k8sClient.Create(suiteCtx, policy)).To(Succeed())
		attachRefAndBump(ns.Name, isvcName, policyName, "fake-serving:v2")

		var pinnedDigest, canaryHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			run := pinnedRun(g, c)
			pinnedDigest = run.Plan.Groups[0].PortableDigest
			g.Expect(c.Status.Canary).NotTo(BeNil())
			canaryHash = c.Status.Canary.CanaryRevisionHash
			g.Expect(canaryHash).NotTo(BeEmpty())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(25)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: edit the policy mid-run — step 0's traffic moves 25 -> 30.
		Eventually(func(g Gomega) {
			cur := &v1beta1.RolloutPolicy{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(policy), cur)).To(Succeed())
			g.Expect(cur.Spec.Canary).NotTo(BeNil())
			cur.Spec.Canary.Steps = rpCanarySteps(30, 100)
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the edit surfaces as drift — live render diverged from
		// the pin — without touching the run.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			drift := runCondition(c, v1beta1.RolloutPlanDriftCondition)
			g.Expect(drift).NotTo(BeNil())
			g.Expect(drift.Status).To(Equal(corev1.ConditionTrue))
			g.Expect(drift.Reason).To(Equal(v1beta1.RolloutPlanDriftReasonPolicyNewerThanRun))
			g.Expect(c.Status.Rollout).NotTo(BeNil())
			g.Expect(c.Status.Rollout.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.Rollout.Groups[0].ObservedDigest).NotTo(Equal(pinnedDigest),
				"the live render must have moved off the pinned digest")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the edit is INERT — the pinned digest and the executing
		// step machine hold the original plan.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			run := pinnedRun(g, c)
			g.Expect(run.Plan.Groups[0].PortableDigest).To(Equal(pinnedDigest),
				"a mid-run policy edit must not replace the pinned plan")
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(25)),
				"the executing ladder must keep the pinned traffic, not the edited one")
		}, 10*time.Second, 500*time.Millisecond).Should(Succeed())

		// Action: promote to completion — run 1 closes on the pinned plan.
		promoteCanary(ns.Name, isvcName, canaryHash)
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(2)))
			g.Expect(c.Status.Rollout).NotTo(BeNil())
			g.Expect(c.Status.Rollout.ActiveRun).To(BeNil())
			g.Expect(c.Status.Rollout.LastRun).NotTo(BeNil())
			g.Expect(c.Status.Rollout.LastRun.Outcome).To(Equal(v1beta1.RolloutRunCompleted))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: mint the next revision (env marker — a third distinct
		// revision without a third image). The next run renders fresh.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Env = append(cur.Spec.Engine.PodSpec.Containers[0].Env,
				corev1.EnvVar{Name: "CANARY_ROUND", Value: "2"})
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the new run pins the EDITED body — digest moved off run
		// 1's pin, agrees with the live render, drift is back InSync, and the
		// executing step 0 serves the edited 30% weight.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			run := pinnedRun(g, c)
			g.Expect(run.Plan.Groups[0].PortableDigest).NotTo(Equal(pinnedDigest),
				"the next run must pick up the mid-run edit")
			g.Expect(c.Status.Rollout.Groups).NotTo(BeEmpty())
			g.Expect(run.Plan.Groups[0].PortableDigest).To(Equal(c.Status.Rollout.Groups[0].ObservedDigest))
			drift := runCondition(c, v1beta1.RolloutPlanDriftCondition)
			g.Expect(drift).NotTo(BeNil())
			g.Expect(drift.Status).To(Equal(corev1.ConditionFalse))
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(30)),
				"the next run must execute the edited ladder")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("parks on a missing policy and recovers watch-driven when it appears", func() {
		// Config: converge on v1, then attach a ref to a policy that does
		// not exist yet and bump the image.
		const isvcName = "rp-run3"
		const policyName = "rp-run3-policy"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)
		stableHash := convergeStableEngine(ns.Name, isvcName, 2)
		attachRefAndBump(ns.Name, isvcName, policyName, "fake-serving:v2")

		// Invariant: the roll PARKS — RolloutPlanReady=False/PolicyNotFound,
		// no run opens, no default progression is silently substituted.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			ready := runCondition(c, v1beta1.RolloutPlanReadyCondition)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(ready.Status).To(Equal(corev1.ConditionFalse))
			g.Expect(ready.Reason).To(Equal(v1beta1.RolloutPlanReasonPolicyNotFound))
			if c.Status.Rollout != nil {
				g.Expect(c.Status.Rollout.ActiveRun).To(BeNil())
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: parked means the OLD revision keeps serving — no run,
		// no step machine, every serving pod stays on the stable revision.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			if c.Status.Rollout != nil {
				g.Expect(c.Status.Rollout.ActiveRun).To(BeNil(),
					"a parked plan must not open a run")
			}
			g.Expect(c.Status.Canary).To(BeNil(),
				"a parked plan must not start the canary step machine")
			ready := 0
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp != nil {
					continue
				}
				g.Expect(pod.Labels[query.LabelRevisionHash]).To(Equal(stableHash),
					"pod %s must stay on the previous revision while parked", pod.Name)
				if podIsReady(&pod) {
					ready++
				}
			}
			g.Expect(ready).To(Equal(2), "the previous revision must keep serving in full")
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

		// Action: create the referenced policy. The policy watch fans out to
		// its consumers, so the parked plan recovers without waiting for the
		// requeue backstop.
		policy := mkRunPolicy(ns.Name, policyName, rpCanarySteps(25, 100))
		Expect(k8sClient.Create(suiteCtx, policy)).To(Succeed())

		// Invariant: the run opens with Policy provenance and the ladder
		// starts executing — paused at step 0 on the declared split.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			run := pinnedRun(g, c)
			g.Expect(run.Plan.Groups[0].Source).To(Equal(v1beta1.RolloutPlanSourcePolicy))
			ready := runCondition(c, v1beta1.RolloutPlanReadyCondition)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(ready.Status).To(Equal(corev1.ConditionTrue))
			g.Expect(ready.Reason).To(Equal(v1beta1.RolloutPlanReasonPinned))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(25)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("repins a tightened ladder into the live run with rollout-repin=now", func() {
		// Config: open a run from a 2-step manual canary (step 0 at 25%
		// traffic) and park it at the manual hold.
		const isvcName = "rp-run4"
		const policyName = "rp-run4-policy"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)
		convergeStableEngine(ns.Name, isvcName, 2)

		policy := mkRunPolicy(ns.Name, policyName, rpCanarySteps(25, 100))
		Expect(k8sClient.Create(suiteCtx, policy)).To(Succeed())
		attachRefAndBump(ns.Name, isvcName, policyName, "fake-serving:v2")

		var runID, pinnedDigest, canaryHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			run := pinnedRun(g, c)
			runID = run.RunID
			pinnedDigest = run.Plan.Groups[0].PortableDigest
			g.Expect(c.Status.Canary).NotTo(BeNil())
			canaryHash = c.Status.Canary.CanaryRevisionHash
			g.Expect(canaryHash).NotTo(BeEmpty())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(25)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: tighten the ladder in the policy — an extra low step in
		// front (10 -> 25 -> 100) — and confirm the edit alone is inert.
		Eventually(func(g Gomega) {
			cur := &v1beta1.RolloutPolicy{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(policy), cur)).To(Succeed())
			g.Expect(cur.Spec.Canary).NotTo(BeNil())
			cur.Spec.Canary.Steps = rpCanarySteps(10, 25, 100)
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			drift := runCondition(c, v1beta1.RolloutPlanDriftCondition)
			g.Expect(drift).NotTo(BeNil())
			g.Expect(drift.Status).To(Equal(corev1.ConditionTrue))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: repin. "now" skips the digest CAS — acceptable here because
		// the spec owns the only writer of this policy.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations[constants.RolloutRepinAnnotation] = "now"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the repin swaps the pinned plan IN PLACE — same run
		// identity, three-step ladder, fresh digest, drift cleared, verb
		// consumed — and the executing step machine now serves the tightened
		// step-0 weight (10 < 25 only holds or lowers exposure, so no
		// pre-step hold arms).
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Annotations).NotTo(HaveKey(constants.RolloutRepinAnnotation),
				"the one-shot repin verb must be consumed")
			run := pinnedRun(g, c)
			g.Expect(run.RunID).To(Equal(runID), "a repin must preserve run identity")
			g.Expect(run.Plan.Groups[0].PortableDigest).NotTo(Equal(pinnedDigest))
			g.Expect(run.Plan.Groups[0].Group.Canary).NotTo(BeNil())
			g.Expect(run.Plan.Groups[0].Group.Canary.Steps).To(HaveLen(3))
			drift := runCondition(c, v1beta1.RolloutPlanDriftCondition)
			g.Expect(drift).NotTo(BeNil())
			g.Expect(drift.Status).To(Equal(corev1.ConditionFalse))
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)),
				"the clamped step index must hold")
			g.Expect(c.Status.Canary.PreStepHold).To(BeFalse(),
				"a step that lowers traffic must not arm the pre-step hold")
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(10)),
				"the tightened ladder's step-0 weight must be programmed")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action + invariant: a promote now advances ONE step of the NEW
		// ladder — to the inserted 25% hold, not to completion as the old
		// two-step plan would have.
		promoteCanary(ns.Name, isvcName, canaryHash)
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(1)),
				"the promote must land on the repinned ladder's next step, not complete the old one")
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(25)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: promote through the second hold — the ungated final step
		// completes the run on the repinned plan.
		promoteCanary(ns.Name, isvcName, canaryHash)
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(3)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhaseStable))
			g.Expect(c.Status.Rollout).NotTo(BeNil())
			g.Expect(c.Status.Rollout.ActiveRun).To(BeNil())
			g.Expect(c.Status.Rollout.LastRun).NotTo(BeNil())
			g.Expect(c.Status.Rollout.LastRun.Outcome).To(Equal(v1beta1.RolloutRunCompleted))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("opens a run on a derived group whose policy ref was inflated away at derive time", func() {
		// Config: the shape a hub places on a member — the policy body INLINED
		// into the group, the spec ref dropped, and the policy identity left
		// only in the derive-time provenance annotation ("<idx>=<name>@<digest>",
		// which has no progression field). Placement is not booted here: this
		// object IS the member controller's input, and the defect is in how it
		// reconstructs the ref from that annotation.
		//
		// This has to round-trip through a real apiserver. The pinned ref's
		// progression is a required CRD enum, so the zero value the annotation
		// leaves behind is rejected by the status subresource — a failure no
		// fake-client test can see.
		const isvcName = "rp-run5"
		const policyName = "vllm-engine-standard"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"),
			WithISVCAnnotation(constants.RolloutPlanSourceAnnotation,
				"0="+policyName+"@rp1:8b734048103c"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)
		convergeStableEngine(ns.Name, isvcName, 2)

		// Action: attach the inlined canary — no policyRef, because the hub
		// replaced it with the composed body — and bump the image to open a run.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary:     &v1beta1.GroupCanary{Steps: rpCanarySteps(25, 100)},
			}}}
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the status write LANDS, carrying the reconstructed policy
		// identity with a progression taken from the composed body.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			run := pinnedRun(g, c)
			pg := run.Plan.Groups[0]
			g.Expect(pg.Source).To(Equal(v1beta1.RolloutPlanSourcePolicy))
			g.Expect(pg.PolicyRef).NotTo(BeNil())
			g.Expect(pg.PolicyRef.Name).To(Equal(policyName))
			g.Expect(pg.PolicyRef.Progression).To(Equal(v1beta1.RolloutProgressionCanary),
				"the composed body must supply the progression the annotation cannot carry")
			g.Expect(pg.Group.Canary).NotTo(BeNil())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the gate actually RUNS. The symptom was never a broken
		// roll — pods converged fine — but an unrecorded one: with no ActiveRun
		// persisted, the canary machinery never engaged and the rollout went
		// ungated.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(25)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the run stays persisted and no status write is rejected.
		// The live failure repeated an UpdateFailed warning every reconcile.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			run := pinnedRun(g, c)
			g.Expect(run.Plan.Groups[0].PolicyRef).NotTo(BeNil())
			g.Expect(run.Plan.Groups[0].PolicyRef.Progression).
				To(Equal(v1beta1.RolloutProgressionCanary))
			g.Expect(eventsWithReason(g, ns.Name, "UpdateFailed")).To(BeEmpty(),
				"the status subresource must accept the pinned plan")
		}, 10*time.Second, 500*time.Millisecond).Should(Succeed())
	})
})
