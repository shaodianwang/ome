package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Timed-step duration and freeze window. The freeze must outlast the step
// timer so a broken freeze is observable as an advance; the drain window must
// comfortably contain the completion-hold probe below.
const (
	lfStepDuration = 45 * time.Second
	lfFreezeWindow = 60 * time.Second
	lfDrainSeconds = int32(30)
)

// Behavior test — global pause freezes a timed canary; the drain window
// delays completion.
//
// A timed canary step advances by clock, so it is the sharpest probe of the
// global-pause contract on a real kubelet: with ome.io/rollout-paused=true the
// step machine must hold its step and traffic even after the step timer
// expires, and clearing the pause must resume from the SAME clocks — the
// already-elapsed timer advances immediately rather than restarting. The
// final step's scaleDownDelaySeconds then holds completion for the drain
// window before the done sentinel lands.
var _ = ginkgo.Describe("behavior — canary global pause freeze and drain", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "canary-lf-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("freezes a timed step past its duration, resumes with clocks intact, drains before completing", func() {
		// Config: 2-replica engine, converge on v1 first.
		const isvcName = "canary-lf1"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: add a timed canary plan (step 0 holds for a fixed duration)
		// with a completion drain window, and bump the image.
		drain := lfDrainSeconds
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{
					ScaleDownDelaySeconds: &drain,
					Steps: []v1beta1.RolloutGroupStep{
						{Capacity: intstr.FromString("50%"), Traffic: 20,
							Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: lfStepDuration}}},
						{Capacity: intstr.FromString("100%"), Traffic: 100},
					},
				},
			}}}
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Wait for the timed hold: split serving, weight applied, step 0.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(20)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: freeze the rollout globally, well before the step timer
		// expires (the timer starts when the split first serves; only a few
		// polls have elapsed since).
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations[constants.PausedRolloutAnnotation] = "true"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the freeze outlasts the step timer. If the pause did not
		// freeze the step machine, the timer would advance the step (and the
		// short plan would complete) inside this window.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)),
				"a globally paused canary must not advance past its step even after the step timer expires")
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(20)),
				"a globally paused canary must not move traffic")
			g.Expect(c.Status.Canary.RolledBackRevisionHash).To(BeEmpty())
		}, lfFreezeWindow, time.Second).Should(Succeed())

		// Action: clear the global pause.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			delete(cur.Annotations, constants.PausedRolloutAnnotation)
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: resume keeps the step clocks — the timer expired during
		// the freeze, so the step advances promptly instead of waiting out a
		// fresh duration (the assertion timeout is far below the step timer).
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(BeNumerically(">=", 1),
				"clearing the pause must resume the elapsed timed step without restarting its timer")
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the final step shifts to full weight and enters the drain
		// (Promoting) before completion.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePromoting))
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(100)))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: scaleDownDelaySeconds holds completion — the done
		// sentinel must not land while the drain window is still open. The
		// probe is far shorter than the window, so it always fits inside it.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(1)),
				"the drain window must delay completion of the final step")
		}, 10*time.Second, 500*time.Millisecond).Should(Succeed())

		// Invariant: after the drain the canary completes — done sentinel,
		// phase Stable.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(2)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhaseStable))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
