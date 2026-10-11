package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// bad-image failure: the attempt fails, the Instance keeps serving v1.
//
// Operators rely on the "no auto-rollback" principle: a failed
// rollout freezes; v1 keeps serving; the operator decides what to do
// next. Real kubelet handles the failure naturally — non-existent
// image tags hit ErrImagePull / ImagePullBackOff, the replacement never
// reaches ContainersReady, and the controller's InstanceReadyTimeout
// ends the attempt. The attempt is counted on the revision's retry
// ladder, which holds the revision, and stays on the Instance's record.
// The Instance itself reads Ready: its v1 pod never stopped serving, so
// the published phase follows that pod, not the replacement that
// failed. The coordination group reads the roll as in flight and held
// rather than Failed.
//
// InstanceReadyTimeout is set short (45s) so the test fits the suite
// budget; the invariant (the failed attempt on the record, the revision
// held, v1 serving preserved) is independent of the value.
var _ = ginkgo.Describe("behavior — bad-image failure", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "badimg-x1-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("bad-image bump times out → the attempt is held on the record; v1 capacity preserved", func() {
		// 2-replica engine + 1-replica decoder, BlueGreen,
		// MaxUnavailable=1. With MaxUnavailable=1 the rollout takes
		// one v1 pod offline per cycle; if the v2 image never reaches
		// Ready, the controller times out on the first instance and
		// stops disrupting v1.
		maxUnavail := intstr.FromInt(1)
		shortTimeout := metav1.Duration{Duration: 45 * time.Second}
		fix := newCoordFixture(ns.Name, WithISVCName("badimg-x1"),
			WithEngineReplicas(2),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		// Shorten the per-Instance Ready timeout so we don't sit on
		// the default 30 minutes.
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			InstanceReadyTimeout: &shortTimeout,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "badimg-x1")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Sample v1 serving capacity during the failure window. The
		// invariant: serving NEVER drops to 0 — the failed v2 pod
		// occupies its instance slot, but the OTHER v1 pod keeps
		// serving.
		stop := util.StartSampler(suiteCtx, 200*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "badimg-x1")
			g.Expect(err).NotTo(HaveOccurred())
			eng := snap.Serving(v1beta1.EngineComponent)
			g.Expect(eng).To(BeNumerically(">=", 1),
				"at least 1 v1 engine pod must keep serving while v2 fails; observed serving=%d", eng)
		})
		defer stop()

		// Bump engine to a non-existent tag. kubelet will
		// ErrImagePull / ImagePullBackOff; the pod never reaches
		// ContainersReady; the InstanceReadyTimeout fires.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "badimg-x1")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Wait for the attempt to end. Budget = InstanceReadyTimeout (45s) +
		// kubelet pull-retry backoff (~1 min worst case) + reconcile
		// + status flush headroom. 3 minutes is comfortable. The Instance
		// the roll reached keeps publishing Ready: its v1 pod serves, the
		// replacement's failure sits on its record with the revision the
		// attempt was for, the ladder holds that revision and the hold
		// says so. The other Instance is never started.
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, "badimg-x1", v1beta1.EngineComponent)
			badRev := ir.Status.UpdateRevision
			g.Expect(badRev).NotTo(BeEmpty())
			g.Expect(badRev).NotTo(Equal(ir.Status.CurrentRevision), "the roll must still be in flight")
			block := findRetryBlock(ir.Status.RetryBlocks, badRev)
			g.Expect(block).NotTo(BeNil(), "the failed attempt must count on the bad revision's retry ladder")
			g.Expect(block.State).To(Equal(v1beta1.RetryBlockHeld), "with no retry policy the first failure holds the revision")
			g.Expect(block.Reason).NotTo(BeEmpty())
			g.Expect(ir.Status.RolloutHold).NotTo(BeNil(), "status.rolloutHold must say why the roll stopped")
			g.Expect(ir.Status.RolloutHold.Gate).To(Equal(v1beta1.RolloutHoldGateHeld))
			g.Expect(ir.Status.RolloutHold.Target).To(Equal(badRev))
			rows := decodedInstanceStatuses(g, ir)
			g.Expect(rows).To(HaveLen(2))
			attempted := 0
			for _, row := range rows {
				g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"instance %d serves on v1 and reads Ready whether or not its replacement failed", row.Index)
				g.Expect(row.Operation).To(BeNil(), "instance %d: no attempt stays open at a held revision", row.Index)
				g.Expect(row.RunningRevision).To(Equal(ir.Status.CurrentRevision), "instance %d must still run v1", row.Index)
				if row.TargetRevision == badRev {
					attempted++
					g.Expect(row.LastFailure).NotTo(BeNil(), "instance %d: the failed attempt must stay on the record", row.Index)
					g.Expect(row.LastFailure.Reason).NotTo(BeEmpty())
				}
			}
			g.Expect(attempted).To(Equal(1), "exactly one Instance carries the failed attempt; the other was never started")
			c := freshISVC(g, ns.Name, "badimg-x1")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseSurging),
					"the group reads a roll in flight and held, not Failed: the Instance it reached still serves")
		}).WithTimeout(3 * time.Minute).WithPolling(kindPollInterval).Should(Succeed())
		stop()
	})
})
