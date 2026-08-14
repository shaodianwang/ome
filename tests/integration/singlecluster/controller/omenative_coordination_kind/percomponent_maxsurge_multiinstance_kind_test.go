package omenative_coordination_kind

import (
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// PerComponent maxSurge=1 ceiling MUST be enforced
// per-Component (across Instances), NOT per-Instance.
//
// With a multi-Instance
// engine under SurgeThenDrain + PerComponent MaxSurge=1, the
// per-Component surge budget could be violated by both Instances
// surging in parallel — pod count would temporarily exceed
// minReplicas+MaxSurge during the rollout window.
//
// The sibling spec (percomponent_surge_kind_test.go) covers
// RollingUpdate policy + 2-Instance engine; this spec covers two
// additional scenarios the original did not exercise:
//
//   - BlueGreen policy.
//   - 3-Instance engine, which forces the gate to deny on TWO
//     successive within-wake-up dispatch attempts (Instance 1 and 2)
//     after granting Instance 0. The original 2-Instance setup only
//     stressed the single-deny case.
//
// Each spec asserts via 100ms-tick sampler that alive(engine) NEVER
// exceeds minReplicas+MaxSurge, AND that the surge path was actually
// exercised (max observed alive >= the expected mid-rollout peak),
// so a regression in dispatch routing can't masquerade as a fix.
var _ = ginkgo.Describe("behavior — PerComponent MaxSurge multi-Instance ceiling", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "surge-e1mi-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("E-1bg alive engine pods never exceed minReplicas+MaxSurge under BlueGreen", func() {
		runPerComponentMaxSurgeMultiInstance(ns.Name, "surge-e1bg", v1beta1.CoordinationPolicyBlueGreen, 2)
	})

	ginkgo.It("E-1mi-ru-3 RollingUpdate + 3 engine Instances stays under MaxSurge=1 ceiling", func() {
		runPerComponentMaxSurgeMultiInstance(ns.Name, "surge-e1ru3", v1beta1.CoordinationPolicyRollingUpdate, 3)
	})

	ginkgo.It("E-1mi-bg-3 BlueGreen + 3 engine Instances stays under MaxSurge=1 ceiling", func() {
		runPerComponentMaxSurgeMultiInstance(ns.Name, "surge-e1bg3", v1beta1.CoordinationPolicyBlueGreen, 3)
	})
})

// runPerComponentMaxSurgeMultiInstance is the shared body for the
// PerComponent MaxSurge multi-Instance specs. Stands up the ISVC with
// `replicas` engine Instances + 1 decoder under `policy` + PerComponent
// MaxSurge=1, bumps the engine image, and asserts the per-Component
// ceiling (replicas+MaxSurge) is never exceeded.
func runPerComponentMaxSurgeMultiInstance(namespace, isvcName string, policy v1beta1.CoordinationPolicy, replicas int) {
	maxSurge := intstr.FromInt(1)
	maxUnavail := intstr.FromInt(0)
	ceiling := int32(replicas + 1)
	fix := newCoordFixture(namespace, WithISVCName(isvcName),
		WithEngineReplicas(replicas),
		WithEngineUpdateStrategy(v1beta1.UpdateStrategySurgeThenDrain),
		WithDecoderUpdateStrategy(v1beta1.UpdateStrategySurgeThenDrain),
		WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     policy,
			Pacing: &v1beta1.CoordinationPacing{
				Type:           v1beta1.CoordinationPacingPerComponent,
				MaxSurge:       &maxSurge,
				MaxUnavailable: &maxUnavail,
			},
		}),
		WithEngineImage("fake-serving:v1"))
	applyFixture(Default, fix)
	deferFixtureCleanup(fix)

	ginkgo.By("waiting for steady state at v1")
	Eventually(func(g Gomega) {
		c := freshISVC(g, namespace, isvcName)
		g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
		g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))
		engCS := c.Status.Components[v1beta1.EngineComponent]
		g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(replicas)))
		decCS := c.Status.Components[v1beta1.DecoderComponent]
		g.Expect(decCS.Lifecycle.ServingReplicas).To(Equal(int32(1)))
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

	// CurrentRevision is promoted on a later reconcile than the one the wait
	// above gates on, so poll for it: a bare read races the promotion and
	// captures the empty pre-promotion value.
	var v1EngineRev string
	Eventually(func(g Gomega) {
		preBump := freshISVC(g, namespace, isvcName)
		v1EngineRev = preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
		g.Expect(v1EngineRev).NotTo(BeEmpty(),
			"engine CurrentRevision must be promoted before the bump")
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

	// 100ms-tick sampler asserts the per-Component MaxSurge invariant
	// throughout the rollout. replicas+MaxSurge(1) is the hard ceiling;
	// any two Instances surging in parallel produces ceiling+1 and
	// trips the sampler. Track the maximum alive value observed so a
	// passing run still verifies the surge path was exercised
	// (max == ceiling) rather than trivially passing because no
	// surge fired (max stayed at replicas).
	var maxAlive int32
	var maxAliveMu sync.Mutex
	stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, namespace, isvcName)
		g.Expect(err).NotTo(HaveOccurred())
		alive := int32(snap.Alive(v1beta1.EngineComponent))
		maxAliveMu.Lock()
		if alive > maxAlive {
			maxAlive = alive
		}
		maxAliveMu.Unlock()
		g.Expect(alive).To(BeNumerically("<=", ceiling),
			"engine alive count must stay <= minReplicas(%d)+MaxSurge(1)=%d across the rollout; got alive=%d",
			replicas, ceiling, alive)
	})
	defer stop()

	ginkgo.By("bumping engine image v1 -> v2")
	Eventually(func(g Gomega) {
		cur := freshISVC(g, namespace, isvcName)
		cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
		g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

	ginkgo.By("waiting for the rollout to converge to v2 with steady-state replicas")
	Eventually(func(g Gomega) {
		c := freshISVC(g, namespace, isvcName)
		engCS := c.Status.Components[v1beta1.EngineComponent]
		g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
			"engine CurrentRevision must advance off v1")
		g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(replicas)))
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	stop()

	// Sanity: the rollout must have actually exercised the surge
	// gate. alive==ceiling means the gate let exactly one Instance
	// surge (correct); alive>ceiling means it let multiple surge
	// in parallel (the bug); alive<=replicas means no surge fired
	// at all (test would pass for the wrong reason). Fail loudly so
	// a regression in dispatch routing doesn't masquerade as a fix.
	maxAliveMu.Lock()
	observed := maxAlive
	maxAliveMu.Unlock()
	Expect(observed).To(BeNumerically(">=", ceiling),
		"surge path must fire (max alive engine pods >= %d expected); got max=%d — "+
			"test passed trivially because no surge happened", ceiling, observed)
}
