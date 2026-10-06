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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A gang whose new revision crashes on every start is bounded like a
// single pod.
//
// A gang's recreate is a disposable attempt: once its leader has parked
// in CrashLoopBackOff past the stuck-pod grace the attempt is disposed,
// counted on the revision's one retry ladder, and the row is Failed with
// the attempt parked on the ladder's wait while its pods live. The ladder
// re-drives it at its own pace and the bound Holds the revision with one
// RetryHeld warning and a rolloutHold naming the crash loop. A gang whose
// attempt stayed in flight instead would be rebuilt on the crashing
// revision every stuck-pod grace, with nothing counting the attempts and
// nothing in status saying why.
//
// The shape: two leader+worker engine gangs under RecreatePod with
// maxUnavailable=1 toward a revision that crashes at start (fake-serving's
// FAIL_AT=startup). The first gang's attempts hold the revision, so the
// roll never reaches the second gang: it keeps serving v1, untouched, for
// the whole episode.

// Cells: instance/Update/Drain/pod.waiting[CrashLoopBackOff]/before, instance/Failed/timer.retryAt/before
var _ = ginkgo.Describe("behavior - a gang whose new revision crash-loops is bounded (KIND)", ginkgo.Ordered, func() {
	const (
		isvcName       = "crashloop-gang-bound"
		goodImage      = "fake-serving:v1"
		untouchedIndex = "1"
		engineReplicas = 2
		podsPerGang    = 2
		// postHeldWindow covers the whole fast ladder (5s + 10s) with
		// slack, long enough that an attempt reopened past the bound
		// would surface as an Updating row or a new crash pod.
		postHeldWindow = 25 * time.Second
	)

	var (
		ns *corev1.Namespace

		engineRev string
		badRev    string
		// untouchedUIDs is the untouched gang's pod name -> UID set
		// captured at Ready; those pods must survive the whole episode.
		untouchedUIDs map[string]types.UID
		// crashUIDsSeen accumulates every crash-revision pod UID any poll
		// observed. Distinct UIDs bound the attempts that materialized.
		crashUIDsSeen map[types.UID]struct{}
	)

	ginkgo.BeforeAll(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "crashloop-gang-bound-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		})
		configureLifecycleConfig(crashLoopBoundLifecycleJSON)

		maxUnavailable := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithEngineReplicas(engineReplicas),
			WithEngineWorkerSize(podsPerGang-1),
			WithEngineImage(goodImage))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type:          v1beta1.UpdateStrategyRecreatePod,
				RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: &maxUnavailable},
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		untouchedUIDs = map[string]types.UID{}
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
	// it: the untouched gang keeps its pods, in rotation; at most one gang
	// carries the crashing revision at a time (maxUnavailable=1); and every
	// crash-revision pod UID is recorded for the attempt accounting.
	observe := func(g Gomega) {
		pods := enginePods(g)
		alive := 0
		for _, pod := range pods {
			if podCrashesAtStart(pod) {
				crashUIDsSeen[pod.UID] = struct{}{}
				alive++
			}
		}
		g.Expect(alive).To(BeNumerically("<=", podsPerGang),
			"crash-revision pods alive at once must never exceed one gang; observed %d", alive)
		for podName, uid := range untouchedUIDs {
			var live *corev1.Pod
			for _, pod := range pods {
				if pod.Name == podName {
					live = pod
					break
				}
			}
			g.Expect(live).NotTo(BeNil(), "untouched gang pod %s must survive the crashing rollout", podName)
			g.Expect(live.UID).To(Equal(uid), "untouched gang pod %s must keep its UID", podName)
			g.Expect(podreadiness.IsServing(live)).To(BeTrue(),
				"untouched gang pod %s must stay in rotation while the crashing gang is retried", podName)
		}
	}

	ginkgo.It("converges both gangs on v1", func() {
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.CurrentRevision).NotTo(BeEmpty())
			g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision))
			rows := decodedInstanceStatuses(g, ir)
			g.Expect(rows).To(HaveLen(engineReplicas))
			for _, row := range rows {
				g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			engineRev = ir.Status.CurrentRevision
			pods := enginePods(g)
			g.Expect(pods).To(HaveLen(engineReplicas * podsPerGang))
			untouchedUIDs = map[string]types.UID{}
			for _, pod := range pods {
				g.Expect(podreadiness.IsServing(pod)).To(BeTrue())
				if pod.Labels[query.LabelInstanceIdx] == untouchedIndex {
					untouchedUIDs[pod.Name] = pod.UID
				}
			}
			g.Expect(untouchedUIDs).To(HaveLen(podsPerGang))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("publishes a crashing engine revision", func() {
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			// A gang renders the leader and the worker templates; both move
			// so the revision does, and both members crash at start.
			for _, podSpec := range []*v1beta1.PodSpec{&cur.Spec.Engine.Leader.PodSpec, &cur.Spec.Engine.Worker.PodSpec} {
				container := &podSpec.Containers[0]
				container.Env = append(container.Env, corev1.EnvVar{Name: "FAIL_AT", Value: "startup"})
			}
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty())
			g.Expect(ir.Status.UpdateRevision).NotTo(Equal(engineRev))
			badRev = ir.Status.UpdateRevision
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
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
				expectNoAttemptInFlight(g, row, badRev)
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
				expectNoAttemptInFlight(g, row, badRev)
			}
		}, postHeldWindow, kindPollInterval).Should(Succeed())
		Expect(len(crashUIDsSeen)).To(BeNumerically("<=", int(crashLoopBoundAttempts)*podsPerGang),
			"distinct crash-revision pods (%d) must not exceed the bound (%d gangs of %d)", len(crashUIDsSeen), crashLoopBoundAttempts, podsPerGang)
	})

	ginkgo.It("converges both gangs on a corrected revision", func() {
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			for _, podSpec := range []*v1beta1.PodSpec{&cur.Spec.Engine.Leader.PodSpec, &cur.Spec.Engine.Worker.PodSpec} {
				container := &podSpec.Containers[0]
				kept := container.Env[:0]
				for _, env := range container.Env {
					if env.Name != "FAIL_AT" {
						kept = append(kept, env)
					}
				}
				container.Env = kept
			}
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty())
			g.Expect(ir.Status.UpdateRevision).NotTo(Equal(badRev), "the corrected spec must produce a new revision")
			g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision), "the engine must converge on the corrected revision")
			rows := decodedInstanceStatuses(g, ir)
			g.Expect(rows).To(HaveLen(engineReplicas))
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

// podCrashesAtStart reports whether the pod runs a revision that crashes
// at start: fake-serving's FAIL_AT=startup on any of its containers.
func podCrashesAtStart(pod *corev1.Pod) bool {
	for _, container := range pod.Spec.Containers {
		for _, env := range container.Env {
			if env.Name == "FAIL_AT" && env.Value == "startup" {
				return true
			}
		}
	}
	return false
}
