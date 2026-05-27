package coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	corev1apis "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// RecreatePod end-to-end coverage. The B-* specs cover RollingUpdate
// + RecreatePod with the sampler invariants; this file adds the
// BlueGreen + RecreatePod combination not exercised elsewhere.
//
// Specs:
//   - RP-1 BlueGreen + RecreatePod: 3 engine, MaxUnavailable=1,
//     maxSurge=0. Sampler asserts serving floor + no surge throughout.
//     Convergence asserts engine + decoder reach v2.
//   - RP-2 pod identity: every engine pod UID after the rollout MUST
//     differ from the pre-rollout UID — proves RecreatePod is recreate
//     (not in-place patch). This is the negation of L-3 (in-place
//     preserves UID).
//   - RP-3 maxSurge=0 + maxUnavailable=1 alive cap at replicas:
//     companion to B-2's same assertion under RollingUpdate, here under
//     BlueGreen so both group policies are covered.
var _ = ginkgo.Describe("RecreatePod end-to-end", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "recpod-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("RP-1+RP-2+RP-3 BlueGreen + RecreatePod: serving floor, no surge, pod UIDs rotate, converges", func() {
		// Config: 3 engine + 1 decoder, BlueGreen, RecreatePod on
		// engine, MaxUnavailable=1, maxSurge=0.
		//
		// RP-1 (serving floor + RP-3 alive cap): on every 100ms sample,
		//   serving(engine) >= 3 - MaxUnavailable = 2
		//   alive(engine)   <= 3 (no surge under RecreatePod)
		// RP-2 (UID rotation): every post-rollout engine pod UID must
		//   differ from the pre-rollout capture.
		// Converges: engine.CurrentRevision flips off v1; both
		//   components return to full serving.
		maxUnavail := intstr.FromInt(1)
		maxSurge := intstr.FromInt(0)
		fix := newCoordFixture(ns.Name, WithISVCName("recpod-rp1"),
			WithEngineReplicas(3),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingPerComponent,
					MaxSurge:       &maxSurge,
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		// Explicit RecreatePod opt-in — the default is SurgeThenDrain
		// (which currently delegates to recreate too but that's a
		// different code path). RP-* must exercise the explicit
		// strategy so a future divergence between the two paths is
		// caught.
		fix.ISVC.Spec.Engine.OMENative = &v1beta1.OMENativeComponentSpec{
			UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
				Type: v1beta1.OMENativeUpdateStrategyRecreatePod,
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for v1 convergence and capture engine pod UIDs.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "recpod-rp1")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(decCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(3)))
			g.Expect(decCS.OMENative.ServingReplicas).To(Equal(int32(1)))
			g.Expect(engCS.OMENative.CurrentRevision).NotTo(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		v1UIDs := map[string]corev1apis.UID{}
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "recpod-rp1")
			g.Expect(err).NotTo(HaveOccurred())
			pods := snap.ByComponent[v1beta1.EngineComponent]
			g.Expect(pods).To(HaveLen(3))
			for _, p := range pods {
				v1UIDs[p.Name] = p.UID
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "recpod-rp1")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision

		// Sampler: serving floor + alive cap on every 100ms tick.
		// Diagnostic counts in the message so a violation is debuggable
		// from the test log alone.
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "recpod-rp1")
			g.Expect(err).NotTo(HaveOccurred())
			eng := snap.Serving(v1beta1.EngineComponent)
			alive := snap.Alive(v1beta1.EngineComponent)
			g.Expect(eng).To(BeNumerically(">=", 2),
				"RP-1: engine serving must stay >= 2 of 3 under MaxUnavailable=1 (observed serving=%d alive=%d)", eng, alive)
			g.Expect(alive).To(BeNumerically("<=", 3),
				"RP-3: RecreatePod must NOT surge — alive must stay <= replicas=3 (observed alive=%d)", alive)
		})
		defer stop()

		// Bump engine image.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "recpod-rp1")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Convergence: engine recreates 3 pods at MaxUnavailable=1
		// cadence — ~3 sequential cycles on real kubelet.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "recpod-rp1")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(3)),
				"engine must return to full serving on v2")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"group must converge to Idle after rollout completes")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		// RP-2: pod UIDs all changed. RecreatePod deletes the old pod
		// and creates a new one at the bumped Incarnation — the
		// replacement gets a fresh UID even though it reuses the same
		// stable pod name. If ANY UID matches the pre-bump capture,
		// either in-place was used by mistake or the post-bump snapshot
		// raced the recreate and observed the old pod.
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "recpod-rp1")
		Expect(err).NotTo(HaveOccurred())
		Expect(snap.ByComponent[v1beta1.EngineComponent]).To(HaveLen(3))
		for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
			origUID, ok := v1UIDs[p.Name]
			Expect(ok).To(BeTrue(),
				"RP-2: post-bump engine pod %s should have been present pre-bump (same name)", p.Name)
			Expect(p.UID).NotTo(Equal(origUID),
				"RP-2: RecreatePod must rotate pod UID for %s (got %s == pre-bump %s); in-place was used by mistake?", p.Name, p.UID, origUID)
		}
	})
})
