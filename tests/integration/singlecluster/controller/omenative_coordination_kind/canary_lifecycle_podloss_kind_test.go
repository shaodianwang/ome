package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior test — canary capacity gate under real pod loss, and its
// counterpart under a real crash.
//
// The capacity gate is a live invariant, not a one-shot check: losing a READY
// canary pod mid-hold records the capacity wait and keeps the step from
// moving, while the held step keeps its phase and its programmed split.
// Once the replacement canary pod serves, the wait clears and a subsequent
// promote still carries the rollout to completion.
//
// A canary pod that served and then died is the other side of that line: the
// kubelet brings the container back, so the counts recover on their own, and
// the ladder must still park where it stood with the step's traffic back on
// the stable revision, until the operator's rollback lands.
// Under a timed soak the same crash must not slip the ladder forward: the
// first exit is a single restart that keeps the step, the soak measures from
// that restart rather than from the moment the split first served, and the
// second exit parks the ladder at the step that was soaking, never the next.
// Requires a real kubelet: the reopened-gate window exists only while a
// replacement pod is actually starting, and the crash is a container that
// really exits under a running kubelet.

// The timed soak outlasts the canary runner's life so its second exit lands
// inside the soak its first restart began, while the soak measured from the
// split first serving elapses between the two exits: a ladder that measured
// from step entry would advance between them.
const (
	plSoakDuration = 90 * time.Second
	plRunnerLife   = "after-1m"
)

// The short-lived runner: every start boots for a soak behind the fixture's
// readiness probe, serves ten seconds, and exits. The kubelet starts a
// restarted runner at once and a rebuilt pod is created at once, so a soak
// measured from either is complete the moment the runner serves again,
// while a soak measured from the moment it serves still has a full step to
// go when the runner exits again.
const (
	plShortSoak       = 20 * time.Second
	plShortRunnerLife = "after-10s"
	plShortRunnerBoot = 20
)

// shortLivedRunnerLadder is the four-step timed ladder of the short-lived
// runner's canary: a tenth, then more, each soaked for a step, then the rest.
func shortLivedRunnerLadder() []v1beta1.RolloutGroupStep {
	soak := &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: plShortSoak}}
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("10%"), Traffic: 10, Pause: soak},
		{Capacity: intstr.FromString("30%"), Traffic: 30, Pause: soak},
		{Capacity: intstr.FromString("60%"), Traffic: 60, Pause: soak},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

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

		// Action: delete the ready canary pod out from under the hold, with
		// its grace period, so the pod is on its way out for a while with its
		// runner stopped by the kubelet: the shape a hand deletion has.
		Expect(k8sClient.Delete(suiteCtx, &lostPod)).To(Succeed())

		// Invariant: losing the canary pod does not move the step machine. The
		// hold keeps its phase and its programmed split while the replacement
		// starts; only the capacity wait is recorded, and the target stays.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CapacityWaitSince).NotTo(BeNil(),
				"losing the canary pod must record the capacity wait")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused),
					"a capacity dip must not move a held step back to Pending")
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(50)))
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
			g.Expect(c.Status.Canary.CapacityWaitSince).To(BeNil(),
				"the recorded capacity wait must clear once the replacement serves")
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

	ginkgo.It("parks the ladder when the canary pod keeps dying after it served, with the step's traffic back on stable, and the rollback lands", func() {
		// Config: 2-replica engine under restart policy None, so the crashed
		// container is restarted in place by the kubelet and the pod stays.
		const isvcName = "canary-lc2"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		restartNone := v1beta1.InstanceRestartPolicyNone
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{RestartPolicy: &restartNone}
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

		// Action: add a manual canary plan and push a revision whose
		// container serves for a minute and then exits.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			}}}
			engine := &cur.Spec.Engine.PodSpec.Containers[0]
			engine.Image = "fake-serving:v2"
			engine.Env = append(engine.Env, corev1.EnvVar{Name: "FAIL_AT", Value: "after-1m"})
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The split serves at the manual hold: one ready canary pod under half
		// the traffic.
		var canaryHash, canaryPodName string
		pushedAt := time.Now()
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			canaryHash = c.Status.Canary.CanaryRevisionHash
			g.Expect(canaryHash).NotTo(BeEmpty())
			g.Expect(canaryHash).NotTo(Equal(stableHash))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(50)))
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp == nil && podIsReady(&pod) && pod.Labels[query.LabelRevisionHash] == canaryHash {
					canaryPodName = pod.Name
				}
			}
			g.Expect(canaryPodName).NotTo(BeEmpty(), "the canary pod must be serving at the hold")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the container exits after each minute it runs. The first
		// exit is a single restart the kubelet answers in place and the step
		// keeps its state; the second parks the ladder at step 0 with a
		// reason, no traffic names the canary revision, the stable revision
		// owns it all, and the event names the pod. The pod itself stays and
		// carries the restarts.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhaseFailed), "a canary pod that keeps dying after serving must park the ladder")
			g.Expect(c.Status.Canary.Failed).NotTo(BeNil(), "the park must say why the ladder stopped")
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)), "the park must hold the step where the canary broke")
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(0)))
			traffic := c.Status.Components[v1beta1.EngineComponent].Traffic
			g.Expect(traffic).To(HaveLen(1), "the step's split must be back on the stable revision alone: %+v", traffic)
			g.Expect(query.RevisionFromName(traffic[0].RevisionName).Hash()).To(Equal(stableHash))
			g.Expect(traffic[0].Percent).To(Equal(int32(100)))
			g.Expect(eventsRecorded(ns.Name, isvcName, "CanaryPodCrashed", pushedAt)).To(ContainElement(ContainSubstring(canaryPodName)),
				"the park must name the pod that died")
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, ktypes.NamespacedName{Namespace: ns.Name, Name: canaryPodName}, pod)).To(Succeed())
			g.Expect(pod.DeletionTimestamp).To(BeNil(), "under restart policy None the crashed pod stays")
			restarted := false
			for _, cs := range pod.Status.ContainerStatuses {
				restarted = restarted || cs.RestartCount > 0
			}
			g.Expect(restarted).To(BeTrue(), "the kubelet must have restarted the crashed container in place")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: the operator rolls back.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations[constants.RolloutRollbackAnnotation] = "true"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the revert drains the broken revision and holds at
		// RolledBack with the rejected revision recorded; every pod serves the
		// stable revision again.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhaseRolledBack))
			g.Expect(c.Status.Canary.RolledBackRevisionHash).To(Equal(canaryHash))
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp != nil {
					continue
				}
				g.Expect(pod.Labels[query.LabelRevisionHash]).To(Equal(stableHash),
					"pod %s must be back on the stable revision", pod.Name)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("holds a timed soak across a single restart and parks at the soaking step when the canary pod dies again", func() {
		// Config: 2-replica engine under restart policy None, so the crashed
		// container is restarted in place by the kubelet and the pod stays.
		const isvcName = "canary-lc3"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		restartNone := v1beta1.InstanceRestartPolicyNone
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{RestartPolicy: &restartNone}
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

		// Action: add a timed canary plan and push a revision whose container
		// serves for a minute and then exits.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: plSoakDuration}}},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			}}}
			engine := &cur.Spec.Engine.PodSpec.Containers[0]
			engine.Image = "fake-serving:v2"
			engine.Env = append(engine.Env, corev1.EnvVar{Name: "FAIL_AT", Value: plRunnerLife})
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The split serves and soaks: one ready canary pod under half the traffic.
		var canaryHash, canaryPodName string
		pushedAt := time.Now()
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			canaryHash = c.Status.Canary.CanaryRevisionHash
			g.Expect(canaryHash).NotTo(BeEmpty())
			g.Expect(canaryHash).NotTo(Equal(stableHash))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(50)))
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp == nil && podIsReady(&pod) && pod.Labels[query.LabelRevisionHash] == canaryHash {
					canaryPodName = pod.Name
				}
			}
			g.Expect(canaryPodName).NotTo(BeEmpty(), "the canary pod must be serving in the soak")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the first exit is a single restart the kubelet answers in
		// place. The ladder keeps soaking step 0 with its split programmed.
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, ktypes.NamespacedName{Namespace: ns.Name, Name: canaryPodName}, pod)).To(Succeed())
			restarted := false
			for _, cs := range pod.Status.ContainerStatuses {
				restarted = restarted || cs.RestartCount > 0
			}
			g.Expect(restarted).To(BeTrue(), "the canary runner must have exited once and been restarted in place")
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)), "a single restart keeps the step")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused), "a single restart is a dip the soak waits out, not a park")
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(50)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the soak measured from the split first serving elapses
		// while the restarted runner is up, and the second exit lands inside
		// the soak the restart began: the ladder parks at step 0 with a
		// reason, never having staged step 1; no traffic names the canary
		// revision, the stable revision owns it all, and the event names the
		// pod, which stays and carries its restarts.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)), "the ladder must never leave the step that was soaking")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhaseFailed), "a canary pod that dies again inside its soak must park the ladder")
			g.Expect(c.Status.Canary.Failed).NotTo(BeNil(), "the park must say why the ladder stopped")
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(0)))
			traffic := c.Status.Components[v1beta1.EngineComponent].Traffic
			g.Expect(traffic).To(HaveLen(1), "the step's split must be back on the stable revision alone: %+v", traffic)
			g.Expect(query.RevisionFromName(traffic[0].RevisionName).Hash()).To(Equal(stableHash))
			g.Expect(traffic[0].Percent).To(Equal(int32(100)))
			g.Expect(eventsRecorded(ns.Name, isvcName, "CanaryPodCrashed", pushedAt)).To(ContainElement(ContainSubstring(canaryPodName)),
				"the park must name the pod that died")
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, ktypes.NamespacedName{Namespace: ns.Name, Name: canaryPodName}, pod)).To(Succeed())
			g.Expect(pod.DeletionTimestamp).To(BeNil(), "under restart policy None the crashed pod stays")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("keeps a four-step timed ladder at step 0 and parks there when the pushed runner exits ten seconds after it serves", func() {
		// Config: 8-replica engine under restart policy None, every engine pod
		// booting for a soak behind the readiness probe; the kubelet restarts
		// the crashed container in place and the pod stays.
		const isvcName = "canary-lc4"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(8),
			WithEngineStartupDelay(plShortRunnerBoot),
			WithEngineImage("fake-serving:v1"))
		restartNone := v1beta1.InstanceRestartPolicyNone
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{RestartPolicy: &restartNone}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var stableHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(8)))
			rev := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent).Status.CurrentRevision
			g.Expect(rev).NotTo(BeEmpty())
			stableHash = query.RevisionFromName(rev).Hash()
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: add the timed ladder and push a revision whose runner serves
		// ten seconds after its boot, and exits, every time it starts.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary:     &v1beta1.GroupCanary{Steps: shortLivedRunnerLadder()},
			}}}
			engine := &cur.Spec.Engine.PodSpec.Containers[0]
			engine.Image = "fake-serving:v2"
			engine.Env = append(engine.Env, corev1.EnvVar{Name: "FAIL_AT", Value: plShortRunnerLife})
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The split serves and soaks: one ready canary pod under a tenth of
		// the traffic.
		var canaryHash, canaryPodName string
		pushedAt := time.Now()
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			canaryHash = c.Status.Canary.CanaryRevisionHash
			g.Expect(canaryHash).NotTo(BeEmpty())
			g.Expect(canaryHash).NotTo(Equal(stableHash))
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(10)))
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp == nil && podIsReady(&pod) && pod.Labels[query.LabelRevisionHash] == canaryHash {
					canaryPodName = pod.Name
				}
			}
			g.Expect(canaryPodName).NotTo(BeEmpty(), "the canary pod must be serving in the soak")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the first exit is a single restart the kubelet answers in
		// place; the ladder keeps soaking step 0 with its split programmed.
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, ktypes.NamespacedName{Namespace: ns.Name, Name: canaryPodName}, pod)).To(Succeed())
			restarted := false
			for _, cs := range pod.Status.ContainerStatuses {
				restarted = restarted || cs.RestartCount > 0
			}
			g.Expect(restarted).To(BeTrue(), "the canary runner must have exited once and been restarted in place")
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)), "a single restart keeps the step")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused), "a single restart is a dip the soak waits out, not a park")
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(10)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the restarted runner serves again a soak after its start
		// and exits ten seconds later, inside the soak measured from the
		// moment it served: the ladder never stages step 1, it parks at step 0
		// with a reason, no traffic names the canary revision, the stable
		// revision owns it all, the event names the pod, and the pod stays.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)), "the ladder must never leave the step that was soaking")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhaseFailed), "a canary pod that dies again inside its soak must park the ladder")
			g.Expect(c.Status.Canary.Failed).NotTo(BeNil(), "the park must say why the ladder stopped")
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(0)))
			traffic := c.Status.Components[v1beta1.EngineComponent].Traffic
			g.Expect(traffic).To(HaveLen(1), "the step's split must be back on the stable revision alone: %+v", traffic)
			g.Expect(query.RevisionFromName(traffic[0].RevisionName).Hash()).To(Equal(stableHash))
			g.Expect(traffic[0].Percent).To(Equal(int32(100)))
			g.Expect(eventsRecorded(ns.Name, isvcName, "CanaryPodCrashed", pushedAt)).To(ContainElement(ContainSubstring(canaryPodName)),
				"the park must name the pod that died")
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, ktypes.NamespacedName{Namespace: ns.Name, Name: canaryPodName}, pod)).To(Succeed())
			g.Expect(pod.DeletionTimestamp).To(BeNil(), "under restart policy None the crashed pod stays")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("keeps a four-step timed ladder at step 0 and parks there when the pushed runner exits ten seconds after it serves and the Instance is rebuilt for each death", func() {
		// Config: the short-lived runner's shape under restart policy
		// RecreateInstanceOnPodRestart: a death rebuilds the Instance, so the
		// pod that died is gone and its replacement carries no restart.
		const isvcName = "canary-lc5"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(8),
			WithEngineStartupDelay(plShortRunnerBoot),
			WithEngineImage("fake-serving:v1"))
		recreate := v1beta1.InstanceRestartPolicyRecreateInstance
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{RestartPolicy: &recreate}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var stableHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(8)))
			rev := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent).Status.CurrentRevision
			g.Expect(rev).NotTo(BeEmpty())
			stableHash = query.RevisionFromName(rev).Hash()
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary:     &v1beta1.GroupCanary{Steps: shortLivedRunnerLadder()},
			}}}
			engine := &cur.Spec.Engine.PodSpec.Containers[0]
			engine.Image = "fake-serving:v2"
			engine.Env = append(engine.Env, corev1.EnvVar{Name: "FAIL_AT", Value: plShortRunnerLife})
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The split serves and soaks: one ready canary pod under a tenth of
		// the traffic. Its identity is what the rebuild changes.
		var canaryHash, canaryPodName string
		var firstUID ktypes.UID
		pushedAt := time.Now()
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			canaryHash = c.Status.Canary.CanaryRevisionHash
			g.Expect(canaryHash).NotTo(BeEmpty())
			g.Expect(canaryHash).NotTo(Equal(stableHash))
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(10)))
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp == nil && podIsReady(&pod) && pod.Labels[query.LabelRevisionHash] == canaryHash {
					canaryPodName, firstUID = pod.Name, pod.UID
				}
			}
			g.Expect(canaryPodName).NotTo(BeEmpty(), "the canary pod must be serving in the soak")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the first exit is answered by a rebuild, a fresh canary
		// pod with no restart; the ladder keeps soaking step 0 with its split
		// programmed while the replacement boots and after it serves.
		Eventually(func(g Gomega) {
			rebuilt := false
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.DeletionTimestamp == nil && pod.Labels[query.LabelRevisionHash] == canaryHash && pod.UID != firstUID {
					rebuilt = true
				}
			}
			g.Expect(rebuilt).To(BeTrue(), "the first death must be answered by a rebuilt canary pod")
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)), "a single death keeps the step")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused), "a single death is a dip the soak waits out, not a park")
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(10)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the replacement serves a soak after the death and exits
		// ten seconds later, inside the soak measured from the moment it
		// served: the ladder never stages step 1, it parks at step 0 with a
		// reason, no traffic names the canary revision, the stable revision
		// owns it all, and the event names the pod that died.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)), "the ladder must never leave the step that was soaking")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhaseFailed), "a canary Instance that dies again inside its soak must park the ladder")
			g.Expect(c.Status.Canary.Failed).NotTo(BeNil(), "the park must say why the ladder stopped")
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(0)))
			traffic := c.Status.Components[v1beta1.EngineComponent].Traffic
			g.Expect(traffic).To(HaveLen(1), "the step's split must be back on the stable revision alone: %+v", traffic)
			g.Expect(query.RevisionFromName(traffic[0].RevisionName).Hash()).To(Equal(stableHash))
			g.Expect(traffic[0].Percent).To(Equal(int32(100)))
			g.Expect(eventsRecorded(ns.Name, isvcName, "CanaryPodCrashed", pushedAt)).To(ContainElement(ContainSubstring(canaryPodName)),
				"the park must name the pod that died")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
