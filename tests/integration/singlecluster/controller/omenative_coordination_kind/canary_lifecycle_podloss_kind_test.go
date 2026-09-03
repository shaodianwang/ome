package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior test — canary capacity gate under real pod loss.
//
// The capacity gate is a live invariant, not a one-shot check: losing a READY
// canary pod mid-hold must re-close the gate (phase back to Pending) without
// advancing the step, resetting the state machine, or touching the stable
// pods. Once the replacement canary pod serves, the hold resumes where it
// was, and a subsequent promote still carries the rollout to completion.
// Requires a real kubelet: the reopened-gate window exists only while a
// replacement pod is actually starting.
var _ = ginkgo.Describe("behavior — canary capacity gate under pod loss", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "canary-lc-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("re-closes the gate when a canary pod dies, recovers, then completes on promote", func() {
		// Config: 2-replica engine with a startup delay so the recreate window
		// (gate re-closed, replacement not yet Ready) is wide enough to observe.
		const isvcName = "canary-lc1"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithStartupDelay(10),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var stableHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			rev := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent).Status.CurrentRevision
			g.Expect(rev).NotTo(BeEmpty())
			stableHash = query.RevisionFromName(rev).Hash()
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: add a manual canary plan and bump the image.
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

		// Wait for the manual hold: one ready canary pod, one ready stable pod.
		// Capture identities so recovery can be told apart from the originals.
		var canaryHash string
		var lostPodUID ktypes.UID
		var stablePodName string
		var lostPod corev1.Pod
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			canaryHash = c.Status.Canary.CanaryRevisionHash
			g.Expect(canaryHash).NotTo(BeEmpty())
			g.Expect(canaryHash).NotTo(Equal(stableHash))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(50)))

			var canaryPods, stablePods []corev1.Pod
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp != nil || !podIsReady(&pod) {
					continue
				}
				switch pod.Labels[query.LabelRevisionHash] {
				case canaryHash:
					canaryPods = append(canaryPods, pod)
				case stableHash:
					stablePods = append(stablePods, pod)
				}
			}
			g.Expect(canaryPods).To(HaveLen(1))
			g.Expect(stablePods).To(HaveLen(1))
			lostPod = canaryPods[0]
			lostPodUID = canaryPods[0].UID
			stablePodName = stablePods[0].Name
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: kill the ready canary pod out from under the hold.
		zero := int64(0)
		Expect(k8sClient.Delete(suiteCtx, &lostPod,
			&client.DeleteOptions{GracePeriodSeconds: &zero})).To(Succeed())

		// Invariant: the gate re-closes while the replacement starts — phase
		// returns to Pending, the step does not advance, and the state machine
		// keeps its target.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePending),
					"losing the canary pod must re-close the capacity gate")
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Canary.CanaryRevisionHash).To(Equal(canaryHash),
				"pod loss must not retarget or reset the canary")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the partition recreates the canary pod on the canary
		// revision and the hold resumes — a NEW pod (different UID), the same
		// untouched stable pod, still step 0.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused),
					"the hold must resume once the replacement canary pod serves")
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))

			var canaryPods, stablePods []corev1.Pod
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp != nil || !podIsReady(&pod) {
					continue
				}
				switch pod.Labels[query.LabelRevisionHash] {
				case canaryHash:
					canaryPods = append(canaryPods, pod)
				case stableHash:
					stablePods = append(stablePods, pod)
				}
			}
			g.Expect(canaryPods).To(HaveLen(1))
			g.Expect(canaryPods[0].UID).NotTo(Equal(lostPodUID),
				"the serving canary pod must be a fresh replacement")
			g.Expect(stablePods).To(HaveLen(1))
			g.Expect(stablePods[0].Name).To(Equal(stablePodName),
				"recovering canary capacity must not disturb the stable pod")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: promote the canary hash — the recovered hold must still be
		// promotable straight through to completion.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations[constants.RolloutPromoteAnnotation] = canaryHash
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

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
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
