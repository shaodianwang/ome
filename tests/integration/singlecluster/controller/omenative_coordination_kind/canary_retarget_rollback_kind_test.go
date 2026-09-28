package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior test — canary retarget rollback target, and the escape from it.
//
// Pins the persisted stable identity end-to-end on a real kubelet: stable A
// serves, a canary toward B pauses mid-step with B pods alive, a second spec
// bump retargets the canary to C, and a rollback must return the Component to
// A — the revision that was stable when the canary began — never to the
// partially-rolled intermediate B. The identity lives in
// status.canary.stableRevisionHash, so it also survives controller restarts.
//
// The rollback then parks terminal: it rejects C until a DIFFERENT target
// appears. The tail drives ome.io/rollout-resume over that hold and pins the
// whole escape — rejection dropped, IR unpinned from A, ladder back at step 0
// on C with its gate re-running — because clearing the status field alone
// would leave every Instance overridden onto A.
var _ = ginkgo.Describe("behavior — canary retarget rollback", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "canary-rt-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("A -> partial B -> retarget C -> rollback returns to A -> resume re-enters C", func() {
		// Config: 4-replica engine, no canary yet — converge fully on A first
		// so the canary group added later observes a clean stable revision.
		const isvcName = "canary-rt1"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(4),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var stableRevName, stableHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(4)))
			stableRevName = freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent).Status.CurrentRevision
			g.Expect(stableRevName).NotTo(BeEmpty(), "stable revision A must be promoted before the canary starts")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stableHash = query.RevisionFromName(stableRevName).Hash()

		// Action 1: add the canary plan and bump the image to v2 (canary B).
		// Step 0 stages 50% capacity and pauses manually, so B parks partial.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			}}}
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the canary initializes toward B, records A as its stable
		// identity, and pauses at step 0 with B's split serving.
		var bHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			bHash = c.Status.Canary.CanaryRevisionHash
			g.Expect(bHash).NotTo(BeEmpty())
			g.Expect(bHash).NotTo(Equal(stableHash), "canary hash must name B, not A")
			g.Expect(c.Status.Canary.StableRevisionHash).To(Equal(stableHash),
				"the stable identity captured at canary start must be A")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused), "manual step 0 must pause with the split serving")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action 2: bump the spec again mid-pause (env marker — a third
		// distinct revision without a third image), retargeting the canary to C.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Env = append(cur.Spec.Engine.PodSpec.Containers[0].Env,
				corev1.EnvVar{Name: "CANARY_RETARGET", Value: "1"})
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the plan restarts toward C while the stable identity
		// STAYS A — a retarget must not re-infer stable from whatever partial
		// capacity B left behind.
		var cHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			cHash = c.Status.Canary.CanaryRevisionHash
			g.Expect(cHash).NotTo(BeEmpty())
			g.Expect(cHash).NotTo(Equal(bHash), "retarget must record the new canary C")
			g.Expect(cHash).NotTo(Equal(stableHash))
			g.Expect(c.Status.Canary.StableRevisionHash).To(Equal(stableHash),
				"a mid-canary retarget must preserve the original stable identity A")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action 3: request rollback of the retargeted canary.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations[constants.RolloutRollbackAnnotation] = "true"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the IR is pointed at A's ControllerRevision — the
		// persisted stable — not at B (the newest non-rejected revision, which
		// the pre-persistence inference would have chosen).
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Pacing).NotTo(BeNil())
			g.Expect(ir.Spec.Pacing.RollbackToRevision).NotTo(BeNil())
			g.Expect(*ir.Spec.Pacing.RollbackToRevision).To(Equal(stableRevName),
				"rollback must target original stable A, not the partial intermediate B")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the Component converges back onto A — every serving pod
		// carries A's revision hash and the phase parks RolledBack.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhaseRolledBack))
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp != nil {
					continue // rejected-revision pods may still be draining
				}
				g.Expect(pod.Labels[query.LabelRevisionHash]).To(Equal(stableHash),
					"pod %s must be back on stable A", pod.Name)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action 4: resume the parked rollback. The hold rejects C until a
		// DIFFERENT target appears, so without this verb the only way back to C
		// is to mint a cosmetically-new revision of the same content.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Annotations[constants.RolloutResumeAnnotation] = cHash
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the verb is consumed, the rejection is dropped, and the IR's
		// rollback target is unpinned — clearing the phase alone would leave every
		// Instance overridden onto A.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Annotations).NotTo(HaveKey(constants.RolloutResumeAnnotation),
				"a one-shot verb must be consumed")
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.RolledBackRevisionHash).To(BeEmpty(),
				"resume must drop the sticky rejection of C")
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			if ir.Spec.Pacing != nil {
				g.Expect(ir.Spec.Pacing.RollbackToRevision).To(BeNil(),
					"resume must unpin the IR from stable A")
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the ladder actually restarts against C — step 0 stages its
		// 50% split again and its manual gate holds there, rather than the canary
		// completing ungated or re-parking on the old rejection.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary.CanaryRevisionHash).To(Equal(cHash),
				"the resumed ladder must target the revision that was parked")
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)),
				"resume re-enters at step 0, it does not waive the steps already walked")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused), "step 0's manual gate must re-run")
			onC := 0
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp == nil && pod.Labels[query.LabelRevisionHash] == cHash {
					onC++
				}
			}
			g.Expect(onC).To(Equal(2), "step 0 stages half the fleet on C")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
