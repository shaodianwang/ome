package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A new revision whose container crashes on every start is bounded.
//
// CrashLoopBackOff is not attributed to the revision (the same crash can
// be a dead device), yet every attempt the disposition ends without a
// relocation directive is a failed attempt at the revision and counts on
// its one retry ladder, lifecycle.updateRetry.maxAttempts. The bound
// Holds the revision the way a bad image is held: one RetryHeld warning,
// a rolloutHold that names the crash loop, no further attempt while
// Held, and the existing releases reopening it.
//
// The shape: two single-pod engine Instances rolling SurgeThenDrain under
// maxSurge=1 toward a revision that crashes at start (fake-serving's
// FAIL_AT=startup), beside a decoder rolling to a sound new image at the
// same time. The decoder is the peer: it must converge while the engine's
// attempts are retried and after they are Held, and the engine sources
// must keep serving throughout.

// crashLoopBoundAttempts is the bound the spec configures and asserts
// against; the lifecycle JSON below states the same number.
const crashLoopBoundAttempts = int32(2)

// A seconds-scale ladder and a short stuck-pod grace so the crash loop
// is classified within the suite budget. No relocation budget: every
// disposed attempt is terminal and counts on the ladder.
const crashLoopBoundLifecycleJSON = `{"updateRetry":{"maxAttempts":2,"initialDelay":"5s","maxDelay":"20s","multiplier":2.0},"stuckPodGracePeriod":"10s"}`

// Cells: instance/Update/Surge/pod.waiting[CrashLoopBackOff]/before
var _ = ginkgo.Describe("behavior - a new revision that crash-loops is bounded (KIND)", ginkgo.Ordered, func() {
	const (
		isvcName   = "crashloop-bound"
		goodImage  = "fake-serving:v1"
		fixedImage = "fake-serving:v2"
		// postHeldWindow covers the whole fast ladder (5s + 10s) with
		// slack, long enough that an attempt reopened past the bound
		// would surface as an Updating row or a new crash pod.
		postHeldWindow = 25 * time.Second
	)

	var (
		ns *corev1.Namespace

		engineRev  string
		decoderRev string
		badRev     string
		// sourceUIDs is the engine v1 pod name -> UID set captured at
		// Ready; the sources must survive the whole episode untouched.
		sourceUIDs map[string]types.UID
		// crashUIDsSeen accumulates every crash-revision pod UID any poll
		// observed. Distinct UIDs bound the attempts that materialized.
		crashUIDsSeen map[types.UID]struct{}
	)

	ginkgo.BeforeAll(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "crashloop-bound-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		})
		configureLifecycleConfig(crashLoopBoundLifecycleJSON)

		maxSurge, maxUnavailable := intstr.FromInt(1), intstr.FromInt(0)
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithEngineReplicas(2),
			WithEngineImage(goodImage))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type:          v1beta1.UpdateStrategySurgeThenDrain,
				RollingUpdate: &v1beta1.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		sourceUIDs = map[string]types.UID{}
		crashUIDsSeen = map[types.UID]struct{}{}
	})

	// enginePods returns the non-deleting engine pods.
	enginePods := func(g Gomega) []*corev1.Pod {
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
		g.Expect(err).NotTo(HaveOccurred())
		var live []*corev1.Pod
		for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
			if pod.DeletionTimestamp == nil {
				live = append(live, pod)
			}
		}
		return live
	}

	// observe enforces the standing invariants on every poll that threads
	// it: the engine sources keep their UIDs and stay ready and serving,
	// at most one crash-revision pod is alive at a time (maxSurge=1), and
	// every crash-revision pod UID is recorded for the wave accounting.
	observe := func(g Gomega) {
		pods := enginePods(g)
		alive := 0
		for _, pod := range pods {
			if podCrashesAtStart(pod) {
				crashUIDsSeen[pod.UID] = struct{}{}
				alive++
			}
		}
		g.Expect(alive).To(BeNumerically("<=", 1),
			"crash-revision pods alive at once must never exceed maxSurge; observed %d", alive)
		for podName, uid := range sourceUIDs {
			var live *corev1.Pod
			for _, pod := range pods {
				if pod.Name == podName {
					live = pod
					break
				}
			}
			g.Expect(live).NotTo(BeNil(), "engine source pod %s must survive the crashing rollout", podName)
			g.Expect(live.UID).To(Equal(uid), "engine source pod %s must keep its UID", podName)
			g.Expect(podreadiness.IsServing(live)).To(BeTrue(),
				"engine source pod %s must stay in rotation while the crashing replacement is retried", podName)
		}
	}

	ginkgo.It("converges both Components on v1", func() {
		Eventually(func(g Gomega) {
			for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
				ir := freshInferenceReplica(g, ns.Name, isvcName, component)
				g.Expect(ir.Status.CurrentRevision).NotTo(BeEmpty())
				g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision))
				for _, row := range decodedInstanceStatuses(g, ir) {
					g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				}
			}
			engineRev = freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent).Status.CurrentRevision
			decoderRev = freshInferenceReplica(g, ns.Name, isvcName, v1beta1.DecoderComponent).Status.CurrentRevision
			pods := enginePods(g)
			g.Expect(pods).To(HaveLen(2))
			sourceUIDs = make(map[string]types.UID, len(pods))
			for _, pod := range pods {
				g.Expect(podreadiness.IsServing(pod)).To(BeTrue())
				sourceUIDs[pod.Name] = pod.UID
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("publishes a crashing engine revision beside a sound decoder revision", func() {
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			engine := &cur.Spec.Engine.PodSpec.Containers[0]
			engine.Env = append(engine.Env, corev1.EnvVar{Name: "FAIL_AT", Value: "startup"})
			cur.Spec.Decoder.PodSpec.Containers[0].Image = fixedImage
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty())
			g.Expect(ir.Status.UpdateRevision).NotTo(Equal(engineRev))
			badRev = ir.Status.UpdateRevision
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("converges the decoder peer while the engine attempts are retried", func() {
		Eventually(func(g Gomega) {
			observe(g)
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.DecoderComponent)
			g.Expect(ir.Status.UpdateRevision).NotTo(Equal(decoderRev))
			g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision),
				"the decoder must converge on its own new revision while the engine retries a crash loop")
			for _, row := range decodedInstanceStatuses(g, ir) {
				g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(row.RunningRevision).To(Equal(ir.Status.UpdateRevision))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("holds the crashing revision at the bound with the crash loop named", func() {
		Eventually(func(g Gomega) {
			observe(g)
			b := findRetryBlock(retryBlocks(g, ns.Name, isvcName, v1beta1.EngineComponent), badRev)
			g.Expect(b).NotTo(BeNil(), "a RetryBlock must be recorded for the crashing revision %s", badRev)
			g.Expect(b.State).To(Equal(v1beta1.RetryBlockHeld))
			g.Expect(b.AttemptsStarted).To(Equal(crashLoopBoundAttempts))
			g.Expect(b.NextRetryAt).To(BeNil(), "Held has no time bound")
			g.Expect(b.Reason).To(Equal("CrashLoopBackOff"))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			observe(g)
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.RolloutHold).NotTo(BeNil(), "status.rolloutHold must surface the Held denial")
			g.Expect(ir.Status.RolloutHold.Gate).To(Equal(v1beta1.RolloutHoldGateHeld))
			g.Expect(ir.Status.RolloutHold.Target).To(Equal(badRev))
			g.Expect(ir.Status.RolloutHold.Reason).To(ContainSubstring("CrashLoopBackOff"))
			for _, row := range decodedInstanceStatuses(g, ir) {
				g.Expect(row.Operation).To(BeNil(), "a held revision leaves no attempt open on instance %d", row.Index)
				g.Expect(row.RunningRevision).To(Equal(engineRev), "instance %d must still run the sound revision", row.Index)
			}
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			held := 0
			for _, e := range events.Items {
				if e.Reason == string(workloadtypes.EventReasonRetryHeld) && e.InvolvedObject.Name == isvcName {
					held++
				}
			}
			g.Expect(held).To(Equal(1), "exactly one RetryHeld warning must announce the hold")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("opens no further attempt while Held", func() {
		heldUIDs := map[types.UID]struct{}{}
		for uid := range crashUIDsSeen {
			heldUIDs[uid] = struct{}{}
		}
		Consistently(func(g Gomega) {
			observe(g)
			for uid := range crashUIDsSeen {
				_, known := heldUIDs[uid]
				g.Expect(known).To(BeTrue(), "no crash-revision pod may be created after Held (new UID %s)", uid)
			}
			for _, row := range instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent) {
				g.Expect(row.Operation).To(BeNil(), "no attempt may reopen on instance %d while the revision is Held", row.Index)
			}
		}, postHeldWindow, kindPollInterval).Should(Succeed())
		Expect(len(crashUIDsSeen)).To(BeNumerically("<=", int(crashLoopBoundAttempts)),
			"distinct crash-revision pods (%d) must not exceed the bound (%d)", len(crashUIDsSeen), crashLoopBoundAttempts)
	})

	ginkgo.It("reopens the revision on the release annotation", func() {
		// The block the release removes; a fresh attempt at the same
		// revision counts from zero and records a later failure time.
		held := findRetryBlock(retryBlocks(Default, ns.Name, isvcName, v1beta1.EngineComponent), badRev)
		Expect(held).NotTo(BeNil())
		Expect(held.LastFailureAt).NotTo(BeNil())
		heldAt := held.LastFailureAt.Time

		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			if ir.Annotations == nil {
				ir.Annotations = map[string]string{}
			}
			ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = badRev
			g.Expect(k8sClient.Update(suiteCtx, ir)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			observe(g)
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			b := findRetryBlock(ir.Status.RetryBlocks, badRev)
			reopened := b == nil || b.State != v1beta1.RetryBlockHeld ||
				(b.LastFailureAt != nil && b.LastFailureAt.Time.After(heldAt))
			for _, row := range decodedInstanceStatuses(g, ir) {
				if row.Operation != nil && row.Operation.TargetRevision == badRev {
					reopened = true
				}
			}
			g.Expect(reopened).To(BeTrue(),
				"the release annotation must remove the Held block and admit a fresh attempt, got %+v", b)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("converges the engine on a corrected revision", func() {
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			engine := &cur.Spec.Engine.PodSpec.Containers[0]
			kept := engine.Env[:0]
			for _, env := range engine.Env {
				if env.Name != "FAIL_AT" {
					kept = append(kept, env)
				}
			}
			engine.Env = kept
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty())
			g.Expect(ir.Status.UpdateRevision).NotTo(Equal(badRev), "the corrected spec must produce a new revision")
			g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision), "the engine must converge on the corrected revision")
			rows := decodedInstanceStatuses(g, ir)
			g.Expect(rows).To(HaveLen(2))
			for _, row := range rows {
				g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(row.RunningRevision).To(Equal(ir.Status.UpdateRevision))
			}
			g.Expect(ir.Status.RolloutHold).To(BeNil(), "status.rolloutHold must clear once the rollout converges")
			for _, pod := range enginePods(g) {
				g.Expect(podCrashesAtStart(pod)).To(BeFalse(), "no crash-revision pod may remain: %s", pod.Name)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
