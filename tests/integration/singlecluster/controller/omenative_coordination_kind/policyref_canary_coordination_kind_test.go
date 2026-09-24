package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior tests — policyRef-canary groups belong to the canary engine.
//
// A group whose progression comes from a policyRef carries no inline canary
// arm, so any consumer that keys on the inline field reads a declared canary
// as the no-progression default, blueGreen. Two consumers do that on the RAW
// spec: coordination's group resolution, which only sees the raw spec while no
// run is pinned, and the canary-owned drop that gates coordination's traffic
// writer, which reads the raw spec always. Both must key on the DECLARED
// progression instead — a Component with a declared analysis gate must never
// be surged by coordination, and its traffic split must be the canary's
// explicit step weight, not a pod count.

// prcRollout is a two-group Concurrent rollout whose groups declare their
// progression only by ref: router and engine roll as unrelated canaries.
func prcRollout(routerPolicy, enginePolicy string) *v1beta1.RolloutSpec {
	ordering := v1beta1.RolloutGroupOrderingConcurrent
	return &v1beta1.RolloutSpec{
		GroupOrdering: &ordering,
		Groups: []v1beta1.RolloutGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.RouterComponent},
				PolicyRef: &v1beta1.RolloutPolicyRef{
					Name:        routerPolicy,
					Progression: v1beta1.RolloutProgressionCanary,
				},
			},
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				PolicyRef: &v1beta1.RolloutPolicyRef{
					Name:        enginePolicy,
					Progression: v1beta1.RolloutProgressionCanary,
				},
			},
		},
	}
}

// coordGroups is the published coordination group status, nil when the layer
// claims nothing.
func coordGroups(c *v1beta1.InferenceService) []v1beta1.RolloutCoordinationGroupStatus {
	if c.Status.RolloutCoordination == nil {
		return nil
	}
	return c.Status.RolloutCoordination.Groups
}

var _ = ginkgo.Describe("behavior — policyRef-canary coordination ownership", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "prc-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("claims no coordination group for ref-declared canaries while no run is pinned", func() {
		// Config: router + engine converge on v1, then the ref-only rollout
		// block is attached WITHOUT an image bump. Nothing diverges, so no run
		// opens and the coordination layer reads the raw spec — the exact
		// window in which a ref-only canary group has no inline arm to find.
		const isvcName = "prc-nogroup"
		const routerPolicy = "prc-nogroup-router"
		const enginePolicy = "prc-nogroup-engine"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithRouter(),
			WithRouterReplicas(2),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)
		stableHash := convergeStableEngine(ns.Name, isvcName, 2)

		Expect(k8sClient.Create(suiteCtx, mkRunPolicy(ns.Name, routerPolicy, rpCanarySteps(10, 100)))).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, mkRunPolicy(ns.Name, enginePolicy, rpCanarySteps(10, 100)))).To(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = prcRollout(routerPolicy, enginePolicy)
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: coordination claims nothing. Resolving these groups would
		// fold two single-Component groups into one Sequential group over
		// [router, engine] — a policy the spec never declared, ordering
		// Components the operator declared independent.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			for _, sg := range coordGroups(c) {
				g.Expect(sg.Policy).NotTo(Equal(v1beta1.CoordinationPolicySequential),
					"coordination synthesized a Sequential group %q over %v that the spec never declared",
					sg.Name, sg.Components)
				g.Expect(sg.Components).NotTo(ContainElement(v1beta1.EngineComponent),
					"the engine's progression is a declared canary; coordination must not drive it")
				g.Expect(sg.Components).NotTo(ContainElement(v1beta1.RouterComponent),
					"the router's progression is a declared canary; coordination must not drive it")
			}
		}, 20*time.Second, 500*time.Millisecond).Should(Succeed())

		// Invariant: nothing rolled. A synthesized blueGreen group would surge
		// new-revision pods with no analysis gating them; every serving pod
		// must stay on the stable revision.
		Consistently(func(g Gomega) {
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp != nil || pod.Labels[query.LabelRevisionHash] == "" {
					continue
				}
				if pod.Labels[constants.OMEComponentLabel] == string(v1beta1.EngineComponent) {
					g.Expect(pod.Labels[query.LabelRevisionHash]).To(Equal(stableHash),
						"engine pod %s left the stable revision with no canary gating it", pod.Name)
				}
			}
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())
	})

	ginkgo.It("publishes the canary step weight, not a pod-proportional split, for a ref-declared canary", func() {
		// Config: a ladder whose first step deliberately separates capacity
		// from traffic — 50% capacity at 10% traffic. On 4 replicas the
		// canary's declared split is 10/90 while a pod-proportional writer
		// would publish 50/50, so the two producers are distinguishable.
		const isvcName = "prc-weight"
		const routerPolicy = "prc-weight-router"
		const enginePolicy = "prc-weight-engine"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithRouter(),
			WithRouterReplicas(2),
			WithEngineReplicas(4),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)
		stableHash := convergeStableEngine(ns.Name, isvcName, 4)

		Expect(k8sClient.Create(suiteCtx, mkRunPolicy(ns.Name, routerPolicy, rpCanarySteps(10, 100)))).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, mkRunPolicy(ns.Name, enginePolicy, rpCanarySteps(10, 100)))).To(Succeed())

		// Action: attach the refs and bump the engine only — the engine's
		// canary arms and parks on its manual step 0.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = prcRollout(routerPolicy, enginePolicy)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the canary engine owns the roll — the run pins the policy
		// body and the ladder parks at the declared 10% weight.
		// The engine is its own canary unit, so its run state lives under the
		// engine Component; the scalar alias mirrors the router entrypoint.
		var canaryHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Rollout).NotTo(BeNil())
			g.Expect(c.Status.Rollout.ActiveRun).NotTo(BeNil())
			engineRun := c.Status.Components[v1beta1.EngineComponent].Canary
			g.Expect(engineRun).NotTo(BeNil())
			canaryHash = engineRun.CanaryRevisionHash
			g.Expect(canaryHash).NotTo(BeEmpty())
			g.Expect(canaryHash).NotTo(Equal(stableHash))
			g.Expect(engineRun.CurrentStep).To(Equal(int32(0)))
			g.Expect(engineRun.ObservedTrafficWeight).To(Equal(int32(10)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the published split stays the canary's declared 10/90 and
		// is never rewritten to the pod-proportional 50/50. Coordination's
		// traffic writer must treat a ref-declared canary Component as
		// canary-owned, or it clobbers the step weight every reconcile.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engineRun := c.Status.Components[v1beta1.EngineComponent].Canary
			g.Expect(engineRun).NotTo(BeNil())
			g.Expect(engineRun.ObservedTrafficWeight).To(Equal(int32(10)),
				"the canary's step weight was overwritten")
			for _, target := range trafficForComponent(c, v1beta1.EngineComponent) {
				if query.RevisionFromName(target.RevisionName).Hash() == canaryHash {
					g.Expect(target.Percent).To(Equal(int32(10)),
						"engine traffic %s must carry the canary's step weight, not a pod share",
						describeTraffic(trafficForComponent(c, v1beta1.EngineComponent)))
				}
			}
		}, 20*time.Second, 500*time.Millisecond).Should(Succeed())

		// Invariant: coordination still claims no group for either Component
		// while the canary drives the roll.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			for _, sg := range coordGroups(c) {
				g.Expect(sg.Components).NotTo(ContainElement(v1beta1.EngineComponent))
				g.Expect(sg.Policy).NotTo(Equal(v1beta1.CoordinationPolicySequential))
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
