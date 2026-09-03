package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Sequential later-Component-only bump releases without a handoff wait.
//
// Order=[decoder, engine], but only the ENGINE (later in Order) is
// bumped. The Sequential active-selector must route straight to the one
// Component actually in flight: the engine rolls to v2 promptly, and the
// untouched decoder is never disturbed — its v1 pod keeps its UID and
// its CurrentRevision never moves.
//
// This pins the gate's bump-moment behavior end-to-end. At the instant
// of the spec write, every Component's status is one generation behind,
// so the gate transiently observes the whole group as in-flight and
// routes to the FIRST Order entry (the decoder). That routing must be
// self-clearing noise: the decoder has no real update to run, status
// catches up, and the engine is released. A regression that latches the
// first-in-Order routing would wedge every partial-bump rollout behind
// a Component that has nothing to do.
var _ = ginkgo.Describe("behavior — Sequential later-Component-only bump", ginkgo.Ordered, func() {
	const name = "sequenti-lb"

	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "sequenti-lb-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("rolls the engine without waiting on the idle decoder ahead of it in Order", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName(name),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
				Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, name)
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// CurrentRevision is promoted on a later reconcile than the one the
		// wait above gates on, so poll for it.
		var v1EngineRev, v1DecoderRev string
		Eventually(func(g Gomega) {
			preBump := freshISVC(g, ns.Name, name)
			g.Expect(preBump.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			v1EngineRev = preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
			g.Expect(preBump.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(BeNil())
			v1DecoderRev = preBump.Status.Components[v1beta1.DecoderComponent].Lifecycle.CurrentRevision
			g.Expect(v1EngineRev).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the bump")
			g.Expect(v1DecoderRev).NotTo(BeEmpty(),
				"decoder CurrentRevision must be promoted before the bump")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture the serving decoder pods: untouched means these exact
		// pods (by UID) survive the engine's whole rollout.
		decoderUIDs := map[string]types.UID{}
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, name)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(snap.Serving(v1beta1.DecoderComponent)).To(BeNumerically(">=", 1))
			for _, pod := range snap.ByComponent[v1beta1.DecoderComponent] {
				decoderUIDs[pod.Name] = pod.UID
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Continuous invariant across the engine rollout: the decoder is
		// never touched — no decoder pod ever runs v2, every captured
		// decoder pod keeps existing with its original UID, undeleted.
		stop := util.StartSampler(suiteCtx, kindPollInterval, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, name)
			g.Expect(err).NotTo(HaveOccurred())
			live := map[string]*corev1.Pod{}
			for _, pod := range snap.ByComponent[v1beta1.DecoderComponent] {
				live[pod.Name] = pod
				for _, container := range pod.Spec.Containers {
					g.Expect(container.Image).To(Equal("fake-serving:v1"),
						"decoder pod %s must stay on v1 during an engine-only bump", pod.Name)
				}
			}
			for podName, uid := range decoderUIDs {
				pod, ok := live[podName]
				g.Expect(ok).To(BeTrue(),
					"decoder pod %s must survive the engine-only rollout", podName)
				g.Expect(pod.UID).To(Equal(uid),
					"decoder pod %s must keep its UID (a new UID means the decoder was rolled)", podName)
				g.Expect(pod.DeletionTimestamp).To(BeNil(),
					"decoder pod %s must not be terminating during an engine-only bump", podName)
			}
		})
		defer stop()

		// Bump ONLY the engine — the later entry in Order.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, name)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The engine must converge inside the normal rollout budget — a
		// wedge behind the idle decoder would blow this timeout.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, name)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance without a decoder handoff")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		// The decoder's authoritative revision never moved.
		c := freshISVC(Default, ns.Name, name)
		Expect(c.Status.Components[v1beta1.DecoderComponent].Lifecycle.CurrentRevision).
			To(Equal(v1DecoderRev),
				"decoder CurrentRevision must be untouched by an engine-only bump")
	})
})
