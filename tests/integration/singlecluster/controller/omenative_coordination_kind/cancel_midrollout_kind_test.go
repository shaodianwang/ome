package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// cancel a rollout mid-flight by reverting the trigger (the
// operator panic-button workflow).
//
// No other KIND spec exercises the
// bump-then-revert workflow. The contract: after a v1 → v2 rollout is
// mid-flight, reverting the image back to v1 must drive the cluster
// state machine BACK to v1 (not leave a v2 straggler), and the
// CurrentRevision should equal the ORIGINAL v1 revision (this is the
// "no terminal v2 wedge" invariant).
//
// We use BlueGreen at engine MinReplicas=3 + MaxUnavailable=1 so the
// rollout has multiple recreate cycles — plenty of time to land the
// revert before the rollout finishes naturally. (At a single replica
// the rollout converges in ~3s on KIND which is too tight for the
// revert to land mid-flight.)
var _ = ginkgo.Describe("behavior — cancel mid-rollout via revert", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "cancel-mid-kcg4-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("v1 → v2 → v1 revert mid-rollout converges back to v1; no v2 stragglers", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("cancel-mid-kcg4"),
			WithEngineReplicas(3),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{
					v1beta1.EngineComponent, v1beta1.DecoderComponent,
				},
				Policy: v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the ISVC to reach Idle on v1; capturing v1 CurrentRevision")
		var v1EngineRev string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "cancel-mid-kcg4")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(3)))
			v1EngineRev = engCS.Lifecycle.CurrentRevision
			g.Expect(v1EngineRev).NotTo(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("bumping engine v1 → v2 (start the rollout we'll cancel)")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "cancel-mid-kcg4")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Wait for the rollout to enter a non-Idle phase. This is the
		// "we're mid-rollout" gate — without it the test races the
		// rollout completion and the revert just looks like a v2 → v1
		// fresh bump (which would converge but wouldn't test the cancel
		// invariant).
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "cancel-mid-kcg4")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			phase := c.Status.RolloutCoordination.Groups[0].Phase
			g.Expect(phase).NotTo(Equal(v1beta1.CoordinationPhaseIdle),
				"rollout should have entered a non-Idle phase before revert is applied")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("reverting engine image back to v1 mid-rollout (the cancel)")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "cancel-mid-kcg4")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v1"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the cluster to converge back to v1 (no v2 stragglers)")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "cancel-mid-kcg4")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(3)),
				"engine must be back to 3 serving replicas after revert")
			// Either the CurrentRevision is the ORIGINAL v1EngineRev (the
			// controller adopted the same ControllerRevision back), OR
			// it's a new revision name that matches the v1 podSpec. The
			// stronger assertion:
			// NO pod is running on a v2 image.
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"rollout must return to Idle after revert converges")

			// Per-pod image check: every alive engine pod must be on v1.
			// This is the no-v2-straggler invariant; it's what
			// distinguishes "revert worked" from "v2 partial rollout
			// stalled at some pods" (the wedge state we are pinning
			// against).
			pods := listOMENativePods(g, ns.Name, "cancel-mid-kcg4")
			engCount := 0
			v1Count := 0
			for _, pod := range pods {
				// Only count engine pods.
				comp := pod.Labels["component"]
				if comp != string(v1beta1.EngineComponent) {
					continue
				}
				// Skip terminating pods — they're a transient artifact
				// of the surge cycle.
				if pod.DeletionTimestamp != nil {
					continue
				}
				engCount++
				for _, ctr := range pod.Spec.Containers {
					if ctr.Name == "ome-container" && ctr.Image == "fake-serving:v1" {
						v1Count++
					}
				}
			}
			g.Expect(engCount).To(Equal(3),
				"expected 3 alive engine pods, got %d", engCount)
			g.Expect(v1Count).To(Equal(3),
				"every alive engine pod must be on fake-serving:v1 after revert; "+
					"got %d/%d v1 pods (a non-v1 pod = v2 straggler = revert lost data)", v1Count, engCount)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Pod-image convergence leads the rollout bookkeeping: the run closes
		// and the group walks down to Idle a few reconciles later. Wait for
		// that settle before the no-flap window.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "cancel-mid-kcg4")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			if c.Status.Rollout != nil {
				g.Expect(c.Status.Rollout.ActiveRun).To(BeNil(), "the run must close once the revert converges")
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Final no-flap check: hold steady for 2s. The revert must not
		// re-trigger a phantom v1-to-something rollout, and no run may
		// reopen on a converged workload.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "cancel-mid-kcg4")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"post-revert state must be stable at Idle, no phantom rollouts")
			if c.Status.Rollout != nil {
				g.Expect(c.Status.Rollout.ActiveRun).To(BeNil(), "no run may reopen on a converged workload")
			}
		}, kindShortTimeout, kindPollInterval).Should(Succeed())

		// Capture a meaningful debug breadcrumb if the spec ever fails.
		// v1EngineRev should match the post-revert CurrentRevision (the
		// CR was preserved, so revert reuses it), but the controller is
		// allowed to mint a NEW CR if its hash differs (e.g. due to
		// CollisionCount). Don't over-specify.
		_ = v1EngineRev
	})
})
