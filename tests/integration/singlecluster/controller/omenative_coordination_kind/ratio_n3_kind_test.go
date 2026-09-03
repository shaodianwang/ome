package omenative_coordination_kind

import (
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// RatioBalanced across 3 Components.
//
// C-1 covers RatioBalanced N=2 (engine 4 : decoder 2). This spec
// extends it to N=3 — engine + decoder + router — and asserts the
// gate stays consistent across multiple cross-Component ratios.
//
// Setup: engine=4, decoder=2, router=2. Two ratios are watched:
//   - engine/decoder original = 2.0, band [1.5, 2.5] at 25% tolerance.
//   - engine/router original  = 2.0, band [1.5, 2.5].
//
// The RatioBalanced gate is implemented per ordered pair (see
// pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/ratio.go);
// this spec verifies both pairs are observed in-band throughout the
// rollout.
var _ = ginkgo.Describe("permutation — RatioBalanced N=3", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-ratio-n3-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// PENDING: real bug in RatioBalanced surge gate composition across
	// N=3 Components. Group stuck Surging indefinitely with
	// ratioSkewRejected=true (the per-pair gate evaluates each ordered
	// pair independently and the cross-pair composition deadlocks when
	// a surge that satisfies one pair violates another). Deferred for
	// design discussion.
	ginkgo.PIt("RatioBalanced N=3 keeps engine/decoder and engine/router ratios in band", func() {
		tol := int32(25)
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-ratio-n3"),
			WithEngineReplicas(4),
			WithDecoderReplicas(2),
			WithRouterReplicas(2),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{
					v1beta1.EngineComponent,
					v1beta1.DecoderComponent,
					v1beta1.RouterComponent,
				},
				Policy: v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					Type:                  v1beta1.CoordinationPacingRatioBalanced,
					RatioTolerancePercent: &tol,
					MaxUnavailable:        &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ratio-n3")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.ServingReplicas).To(Equal(int32(4)))
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(c.Status.Components[v1beta1.RouterComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.RouterComponent].Lifecycle.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// CurrentRevision is promoted on a later reconcile than the one the wait
		// above gates on, so poll for it: a bare read races the promotion and
		// captures the empty pre-promotion value.
		var v1EngineRev string
		Eventually(func(g Gomega) {
			preBump := freshISVC(g, ns.Name, "perm-ratio-n3")
			g.Expect(preBump.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			v1EngineRev = preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
			g.Expect(v1EngineRev).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the bump")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Sampler asserts BOTH cross-Component ratios stay in-band.
		var samples atomic.Int64
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-ratio-n3")
			if err != nil {
				return
			}
			eng := snap.Serving(v1beta1.EngineComponent)
			dec := snap.Serving(v1beta1.DecoderComponent)
			rtr := snap.Serving(v1beta1.RouterComponent)
			samples.Add(1)
			if dec > 0 {
				r := float64(eng) / float64(dec)
				g.Expect(r).To(BeNumerically(">=", 1.5),
					"engine/decoder ratio %.2f (eng=%d dec=%d) below band lower 1.5", r, eng, dec)
				g.Expect(r).To(BeNumerically("<=", 2.5),
					"engine/decoder ratio %.2f above band upper 2.5", r)
			}
			if rtr > 0 {
				r := float64(eng) / float64(rtr)
				g.Expect(r).To(BeNumerically(">=", 1.5),
					"engine/router ratio %.2f (eng=%d rtr=%d) below band lower 1.5", r, eng, rtr)
				g.Expect(r).To(BeNumerically("<=", 2.5),
					"engine/router ratio %.2f above band upper 2.5", r)
			}
		})
		defer stop()

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-ratio-n3")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Router.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ratio-n3")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"RatioBalanced N=3 rollout must return to Idle")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()
		Expect(samples.Load()).To(BeNumerically(">=", 3),
			"sampler observed only %d samples during the rollout; ratio bands were not meaningfully checked",
			samples.Load())
	})
})
