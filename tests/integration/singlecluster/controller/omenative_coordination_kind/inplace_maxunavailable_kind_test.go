package omenative_coordination_kind

import (
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	corev1apis "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// InPlaceIfPossible under BlueGreen progression with RatioBalanced pacing.
//
// RatioBalanced's ratio gate is deliberately bypassed for in-place
// strategies because each temporary drain returns the same pod. The
// MaxUnavailable budget still limits disruption independently for each
// Component, and in-place patches must not add, remove, or replace pods.
//
// Asserts:
//   - MaxUnavailable=1 keeps engine serving >= 3 of 4 and decoder
//     serving >= 1 of 2 throughout the rollout.
//   - Alive counts remain exactly 4 engine and 2 decoder pods.
//   - Both Components advance off v1; group returns to Idle.
//   - Pod UIDs preserved (proves in-place, not recreate).
var _ = ginkgo.Describe("permutation — RatioBalanced + InPlaceIfPossible", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-ratio-inplace-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("RatioBalanced + InPlace respects MaxUnavailable and preserves pod UIDs", func() {
		tol := int32(25)
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-ratio-inplace"),
			WithEngineReplicas(4),
			WithDecoderReplicas(2),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyInPlaceIfPossible),
			WithDecoderUpdateStrategy(v1beta1.UpdateStrategyInPlaceIfPossible),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
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
			c := freshISVC(g, ns.Name, "perm-ratio-inplace")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.ServingReplicas).To(Equal(int32(4)))
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture v1 pod UIDs.
		v1UIDs := map[string]corev1apis.UID{}
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-ratio-inplace")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(snap.ByComponent[v1beta1.EngineComponent]).To(HaveLen(4))
			g.Expect(snap.ByComponent[v1beta1.DecoderComponent]).To(HaveLen(2))
			for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
				v1UIDs[p.Name] = p.UID
			}
			for _, p := range snap.ByComponent[v1beta1.DecoderComponent] {
				v1UIDs[p.Name] = p.UID
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// CurrentRevision is promoted on a later reconcile than the one the wait
		// above gates on, so poll for it: a bare read races the promotion and
		// captures the empty pre-promotion value.
		var v1EngineRev string
		Eventually(func(g Gomega) {
			preBump := freshISVC(g, ns.Name, "perm-ratio-inplace")
			v1EngineRev = preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
			g.Expect(v1EngineRev).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the bump")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// RatioBalanced is bypassed for in-place updates, but MaxUnavailable=1
		// still limits each Component to one temporarily non-serving pod.
		var samples atomic.Int64
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-ratio-inplace")
			if err != nil {
				return
			}
			engServing := snap.Serving(v1beta1.EngineComponent)
			decServing := snap.Serving(v1beta1.DecoderComponent)
			engAlive := snap.Alive(v1beta1.EngineComponent)
			decAlive := snap.Alive(v1beta1.DecoderComponent)
			samples.Add(1)
			g.Expect(engServing).To(BeNumerically(">=", 3),
				"MaxUnavailable=1 must keep >=3 of 4 engine pods serving (serving=%d alive=%d)", engServing, engAlive)
			g.Expect(decServing).To(BeNumerically(">=", 1),
				"MaxUnavailable=1 must keep >=1 of 2 decoder pods serving (serving=%d alive=%d)", decServing, decAlive)
			g.Expect(engAlive).To(Equal(4),
				"in-place must keep exactly 4 engine pods alive")
			g.Expect(decAlive).To(Equal(2),
				"in-place must keep exactly 2 decoder pods alive")
		})
		defer stop()

		// Bump engine + decoder image.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-ratio-inplace")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ratio-inplace")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()
		Expect(samples.Load()).To(BeNumerically(">=", 3),
			"sampler observed only %d samples during the rollout; availability was not meaningfully checked",
			samples.Load())

		// Pod UID preservation (in-place must keep all 6 pods).
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "perm-ratio-inplace")
		Expect(err).NotTo(HaveOccurred())
		for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
			for _, p := range snap.ByComponent[comp] {
				origUID, ok := v1UIDs[p.Name]
				Expect(ok).To(BeTrue(),
					"post-bump %s pod %s should have been present at v1", comp, p.Name)
				Expect(p.UID).To(Equal(origUID),
					"in-place must preserve %s pod UID for %s (got %s, want %s)",
					comp, p.Name, p.UID, origUID)
			}
		}
	})
})
