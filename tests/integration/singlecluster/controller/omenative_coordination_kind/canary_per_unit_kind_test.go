package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior test — two canary units on ONE InferenceService advance independently.
//
// A canary unit is the smallest set of Components that must roll together: the
// router alone, or engine+decoder. Router and engine need ladders that differ by
// more than an order of magnitude — a router pod whose backends are up is Ready
// in seconds, a cold engine takes far longer — so a single shared plan forces one
// of them to be gated wrongly. This is the probe that they are genuinely separate
// runs and not one run wearing two hats.
//
// A real kubelet is required: the claim is about two step machines making
// progress against live pod readiness, which the envtest pod simulator cannot
// reproduce faithfully.
var _ = ginkgo.Describe("behavior — per-unit canary runs", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "canary-unit-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("router and engine units hold their own step, revision and traffic", func() {
		const isvcName = "canary-two-units"

		// No decoder: engine+decoder are ONE unit, so a P/D fixture could not
		// carry an engine-only ladder. The two units this exercises are the
		// router and the engine.
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithRouter(), WithRouterReplicas(2),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Baseline: both Components serving, and their revisions recorded so the
		// per-unit canary hashes can be told apart from each other later.
		var routerStable, engineStable string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			for _, comp := range []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent} {
				cs := c.Status.Components[comp]
				g.Expect(cs.Lifecycle).NotTo(BeNil(), "%s lifecycle", comp)
				g.Expect(cs.Lifecycle.ServingReplicas).To(Equal(int32(2)), "%s serving", comp)
			}
			routerStable = query.RevisionFromName(
				freshInferenceReplica(g, ns.Name, isvcName, v1beta1.RouterComponent).Status.CurrentRevision).Hash()
			engineStable = query.RevisionFromName(
				freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent).Status.CurrentRevision).Hash()
			g.Expect(routerStable).NotTo(BeEmpty())
			g.Expect(engineStable).NotTo(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: two canary groups on one ISVC, one per unit, with DIFFERENT
		// ladders. The router parks on a manual hold at its first step; the
		// engine's ladder goes straight to 100%. If the two runs shared state,
		// the router's hold would stall the engine or the engine's completion
		// would drag the router past its hold.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
				{
					Components: []v1beta1.ComponentType{v1beta1.RouterComponent},
					Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
						{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
						{Capacity: intstr.FromString("100%"), Traffic: 100},
					}},
				},
				{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
						{Capacity: intstr.FromString("100%"), Traffic: 100},
					}},
				},
			}}
			cur.Spec.Router.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: each unit records its OWN run under its entrypoint
		// Component, with its own canary revision. A shared status would show
		// one hash for both.
		var routerCanary, engineCanary string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)

			routerRun := c.Status.Components[v1beta1.RouterComponent].Canary
			g.Expect(routerRun).NotTo(BeNil(), "the router unit must carry its own run")
			routerCanary = routerRun.CanaryRevisionHash
			g.Expect(routerCanary).NotTo(BeEmpty())
			g.Expect(routerRun.StableRevisionHash).To(Equal(routerStable))

			engineRun := c.Status.Components[v1beta1.EngineComponent].Canary
			g.Expect(engineRun).NotTo(BeNil(), "the engine unit must carry its own run")
			engineCanary = engineRun.CanaryRevisionHash
			g.Expect(engineCanary).NotTo(BeEmpty())
			g.Expect(engineRun.StableRevisionHash).To(Equal(engineStable))

			// Revision hashes are per-Component, so two units sharing one hash
			// would mean one run is reporting the other's state.
			g.Expect(routerCanary).NotTo(Equal(engineCanary),
				"each unit must track its own revision, not a shared one")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the engine unit runs its single 100% step to completion
		// while the router unit is still parked on its manual hold. This is the
		// independence claim: one unit finishing must not require, or wait for,
		// the other.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)

			g.Expect(c.Status.Components[v1beta1.RouterComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused), "the router unit must hold at its manual step")
			g.Expect(c.Status.Components[v1beta1.RouterComponent].Canary.CurrentStep).
				To(Equal(int32(0)), "a held router step must not be advanced by the engine unit")

			byRev := map[string]int{}
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp != nil || !podIsReady(&pod) {
					continue
				}
				byRev[pod.Labels[query.LabelRevisionHash]]++
			}
			// Router held at 50% of 2 replicas: one canary pod, one stable.
			g.Expect(byRev[routerCanary]).To(Equal(1), "router 50%% capacity stages one pod")
			g.Expect(byRev[routerStable]).To(Equal(1), "the router's stable pod keeps the complement")
			// Engine cut over fully and its stable revision is gone.
			g.Expect(byRev[engineCanary]).To(Equal(2), "the engine unit must reach full capacity")
			g.Expect(byRev).NotTo(HaveKey(engineStable), "the engine's stable revision must drain")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: traffic is written per Component and never merged. Each
		// unit's weights reflect ITS ladder — the router's 50/50 split and the
		// engine's single 100% target — and the engine's completion does not
		// raise the router's weight.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)

			routerTraffic := trafficForComponent(c, v1beta1.RouterComponent)
			g.Expect(routerTraffic).To(HaveLen(2),
				"the held router step keeps a canary/stable pair: %s", describeTraffic(routerTraffic))
			routerByTag := map[string]int32{}
			for _, t := range routerTraffic {
				routerByTag[t.Tag] = t.Percent
			}
			g.Expect(routerByTag).To(HaveKeyWithValue("canary", int32(50)))
			g.Expect(routerByTag).To(HaveKeyWithValue("stable", int32(50)))

			engineTraffic := trafficForComponent(c, v1beta1.EngineComponent)
			g.Expect(engineTraffic).To(HaveLen(1),
				"a completed engine unit collapses to one target: %s", describeTraffic(engineTraffic))
			g.Expect(engineTraffic[0].Percent).To(Equal(int32(100)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
