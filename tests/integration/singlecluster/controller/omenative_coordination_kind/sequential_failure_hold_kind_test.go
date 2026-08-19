package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Sequential earlier-Component failure holds the later Component.
//
// The ordered-group contract: a failed earlier Component prevents later
// Components from starting. Both images are bumped in one generation with
// Order=[decoder, engine], but the decoder's new image does not exist —
// real kubelet hits ErrImagePull, the decoder's surge pod never reaches
// ContainersReady, the InstanceReadyTimeout escalates the group to
// Failed. Throughout that window the engine must stay entirely on v1: no
// v2 engine pod may ever exist, and the v1 engine pod must keep its UID.
//
// The recovery half: publishing a corrected decoder revision (a spec
// roll-forward, no group-level latch to clear) must resume the chain —
// decoder converges on the fixed image first, then the engine's held
// rollout is released and both Components land on v2 with the group back
// at Idle. No auto-rollback: the operator's corrective edit is the only
// input.
var _ = ginkgo.Describe("behavior — Sequential earlier-Component failure hold", ginkgo.Ordered, func() {
	const (
		name      = "sequenti-fh"
		goodImage = "fake-serving:v1"
		// fixedImage is the corrective decoder revision; also the engine's
		// bump target, so recovery converges the whole chain on one image.
		fixedImage = "fake-serving:v2"
		badImage   = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
	)

	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "sequenti-fh-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("holds the engine on v1 while the decoder is Failed; a corrected revision resumes the chain", func() {
		maxUnavail := intstr.FromInt(1)
		shortTimeout := metav1.Duration{Duration: 45 * time.Second}
		fix := newCoordFixture(ns.Name, WithISVCName(name),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
				Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage(goodImage))
		// Short per-Instance Ready timeout so the decoder's bad-image surge
		// escalates to Failed inside the suite budget; the hold invariant is
		// independent of the value.
		fix.ISVC.Spec.Decoder.Lifecycle = &v1beta1.LifecycleSpec{
			InstanceReadyTimeout: &shortTimeout,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, name)
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// CurrentRevision is promoted on a later reconcile than the one the
		// wait above gates on, so poll for it.
		var v1EngineRev, v1DecoderRev string
		Eventually(func(g Gomega) {
			preBump := freshISVC(g, ns.Name, name)
			v1EngineRev = preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
			v1DecoderRev = preBump.Status.Components[v1beta1.DecoderComponent].Lifecycle.CurrentRevision
			g.Expect(v1EngineRev).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the bump")
			g.Expect(v1DecoderRev).NotTo(BeEmpty(),
				"decoder CurrentRevision must be promoted before the bump")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture the serving v1 engine pods: the hold invariant is that
		// these exact pods (by UID) survive the whole failure window.
		engineUIDs := map[string]types.UID{}
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, name)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(snap.Serving(v1beta1.EngineComponent)).To(BeNumerically(">=", 1))
			for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
				engineUIDs[pod.Name] = pod.UID
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Continuous hold invariant, sampled from before the bump until just
		// before the corrective edit: the engine never leaves v1. No engine
		// pod may ever carry the engine's bump target, and every captured v1
		// engine pod must keep existing with its original UID, undeleted.
		stop := util.StartSampler(suiteCtx, kindPollInterval, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, name)
			g.Expect(err).NotTo(HaveOccurred())
			live := map[string]*corev1.Pod{}
			for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
				live[pod.Name] = pod
				for _, container := range pod.Spec.Containers {
					g.Expect(container.Image).NotTo(Equal(fixedImage),
						"engine pod %s runs the bump target while the decoder (earlier in Order) is wedged", pod.Name)
				}
			}
			for podName, uid := range engineUIDs {
				pod, ok := live[podName]
				g.Expect(ok).To(BeTrue(),
					"v1 engine pod %s must survive the earlier Component's failure window", podName)
				g.Expect(pod.UID).To(Equal(uid),
					"v1 engine pod %s must keep its UID (a new UID means the engine started rolling)", podName)
				g.Expect(pod.DeletionTimestamp).To(BeNil(),
					"v1 engine pod %s must not be terminating while the decoder is wedged", podName)
			}
		})
		defer stop()

		// Bump BOTH Components in one generation: decoder (first in Order)
		// to a non-existent tag, engine to the real v2.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, name)
			cur.Spec.Decoder.PodSpec.Containers[0].Image = badImage
			cur.Spec.Engine.PodSpec.Containers[0].Image = fixedImage
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The decoder's failure must surface as the group-level Failed
		// rollup. Budget = InstanceReadyTimeout (45s) + kubelet pull-retry
		// backoff + reconcile + status flush headroom.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, name)
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseFailed),
					"group must roll up Failed while the first Component in Order is wedged")
		}).WithTimeout(3 * time.Minute).WithPolling(kindPollInterval).Should(Succeed())

		// Soak in the Failed state: the engine hold (enforced continuously
		// by the sampler) must persist — Failed is not a release.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, name)
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision).
				To(Equal(v1EngineRev),
					"engine CurrentRevision must not advance while the decoder is Failed")
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		// Corrective edit: roll the decoder forward to the fixed image. The
		// chain must resume — decoder first, then the released engine.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, name)
			cur.Spec.Decoder.PodSpec.Containers[0].Image = fixedImage
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, name)
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(decCS.Lifecycle.CurrentRevision).NotTo(Equal(v1DecoderRev),
				"decoder CurrentRevision must advance once the corrected revision lands")
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance after the decoder recovers")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"group must return to Idle once the whole chain converges")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Both Components' serving pods run the fixed image.
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, name)
			g.Expect(err).NotTo(HaveOccurred())
			for _, comp := range []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent} {
				g.Expect(snap.Serving(comp)).To(BeNumerically(">=", 1))
				for _, pod := range snap.ByComponent[comp] {
					if pod.DeletionTimestamp != nil {
						continue
					}
					g.Expect(pod.Spec.Containers[0].Image).To(Equal(fixedImage),
						"%s pod %s must run the fixed image after recovery", comp, pod.Name)
				}
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
