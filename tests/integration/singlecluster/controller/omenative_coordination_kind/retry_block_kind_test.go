package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/tests/integration/util"
)

// RetryBlock: same-target gang-surge failure containment.
//
// The failure mode this bounds: a failed same-target gang surge is
// abandoned, the source resets to Ready, and the unchanged target
// re-triggers a fresh surge every pass — an unbounded delete/recreate
// loop burning accelerator capacity. The RetryBlock persists
// per-target-revision retry authority on the IR so the loop is bounded:
// <= maxAttempts authorized waves, then Held until a NEW revision is
// published.
//
// Coverage here is GANG surges (leader+worker): the RetryBlock writers are
// wired on the abandonFailedGangSurge path, which clears the failed
// attempt's Operation in the same transition (the writer-ordering
// invariant). Single-pod same-target failures record blocks through the
// deadline disposition — pinned by disposition_kind_test.go.
//
// One Ordered container, one fixture: the three Its walk the same rollout
// through Backoff -> Held -> release-by-new-revision, which is both the
// operator story and ~3 minutes cheaper than three isolated fixtures.
var _ = ginkgo.Describe("behavior - RetryBlock same-target containment", ginkgo.Ordered, func() {
	const (
		name      = "retry-block"
		goodImage = "fake-serving:v1"
		// fixedImage is the corrective revision released in It 3. v2 (not a
		// revert to v1) so the release is a genuinely NEW revision driving a
		// real gang surge out of Held — the wedge regression under test.
		fixedImage = "fake-serving:v2"
		badImage   = "ghcr.io/ome-projects/this-image-does-not-exist:p0a"
		// gangSize: leader + 1 worker per Instance.
		gangSize = 2
	)

	// Observation windows, sized against fastRetryInitialDelay (5s):
	// noChurnWindow covers ~2 backoff windows (5s + 10s would be the full
	// ladder; 15s spans the first window twice over with slack), and
	// postHeldWindow covers the entire remaining ladder (5s + 10s) plus
	// slack — long enough that a rogue post-Held retry wave would appear.
	const (
		noChurnWindow  = 15 * time.Second
		postHeldWindow = 25 * time.Second
	)

	var (
		ns *corev1.Namespace

		v1Rev  string
		badRev string
		// sourceUIDs is the v1 gang's pod name -> UID set captured at Ready.
		// The no-churn invariant: these never change through Its 1-2.
		sourceUIDs map[string]types.UID
		// badUIDsSeen accumulates every bad-revision pod UID any poll
		// observed across Its 1-2. Distinct UIDs bound the number of
		// authorized create waves: <= maxAttempts * gangSize.
		badUIDsSeen map[types.UID]struct{}
	)

	ginkgo.BeforeAll(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "retry-block-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		})

		// Seconds-scale retry policy; restored (removed) after the container.
		configureFastUpdateRetry()

		fix := newCoordFixture(ns.Name, WithISVCName(name),
			WithoutDecoder(),
			WithEngineWorkerSize(1),
			WithEngineImage(goodImage))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		sourceUIDs = map[string]types.UID{}
		badUIDsSeen = map[types.UID]struct{}{}
	})

	// enginePodsWithImage returns non-deleting engine pods whose ome
	// container runs image.
	enginePodsWithImage := func(g Gomega, image string) []*corev1.Pod {
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, name)
		g.Expect(err).NotTo(HaveOccurred())
		var matched []*corev1.Pod
		for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
			if pod.DeletionTimestamp != nil {
				continue
			}
			for _, container := range pod.Spec.Containers {
				if container.Image == image {
					matched = append(matched, pod)
					break
				}
			}
		}
		return matched
	}

	// observe enforces the standing no-churn invariants on every poll that
	// calls it (Its 1-2 thread it through their Eventually/Consistently
	// bodies, so the invariants hold continuously, not just at checkpoints):
	//
	//   - bad-revision pods alive at any instant <= gangSize (no pile-up:
	//     one candidate gang at a time, never candidate + leftovers);
	//   - every bad-revision pod UID is recorded into badUIDsSeen (wave
	//     accounting for the <= maxAttempts bound);
	//   - the source gang's pods still exist with their original UIDs, are
	//     not deleting, and their containers are Ready — the source is
	//     never torn down or recreated while the target revision fails.
	observe := func(g Gomega) {
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, name)
		g.Expect(err).NotTo(HaveOccurred())
		pods := snap.ByComponent[v1beta1.EngineComponent]

		activeBad := 0
		for _, pod := range pods {
			bad := false
			for _, container := range pod.Spec.Containers {
				if container.Image == badImage {
					bad = true
					break
				}
			}
			if !bad {
				continue
			}
			badUIDsSeen[pod.UID] = struct{}{}
			if pod.DeletionTimestamp == nil {
				activeBad++
			}
		}
		g.Expect(activeBad).To(BeNumerically("<=", gangSize),
			"bad-revision pods alive at once must never exceed one gang (=%d); observed %d", gangSize, activeBad)

		for podName, uid := range sourceUIDs {
			var live *corev1.Pod
			for _, pod := range pods {
				if pod.Name == podName {
					live = pod
					break
				}
			}
			g.Expect(live).NotTo(BeNil(),
				"source gang pod %s must survive the failed rollout (no delete/recreate churn)", podName)
			g.Expect(live.UID).To(Equal(uid),
				"source gang pod %s must keep its UID (a new UID means a delete/recreate cycle)", podName)
			g.Expect(live.DeletionTimestamp).To(BeNil(),
				"source gang pod %s must not be terminating", podName)
			g.Expect(podreadiness.IsContainersReady(live)).To(BeTrue(),
				"source gang pod %s must stay Ready while the target revision fails", podName)
		}
	}

	ginkgo.It("contains a failing same-target gang surge in Backoff with zero source churn", func() {
		// v1 gang converges Ready.
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// CurrentRevision is promoted on a later reconcile than the one that
		// flips the Instance to Ready, so poll for it: a bare read here races
		// the promotion and observes the empty pre-promotion value.
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			v1Rev = ir.Status.CurrentRevision
			g.Expect(v1Rev).NotTo(BeEmpty(),
				"IR CurrentRevision must be promoted once the gang is Ready")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture the source gang UID set at Ready. Rebuilt per poll so a
		// failed early attempt can't leave stale entries behind.
		Eventually(func(g Gomega) {
			pods := enginePodsWithImage(g, goodImage)
			g.Expect(pods).To(HaveLen(gangSize))
			sourceUIDs = make(map[string]types.UID, len(pods))
			for _, pod := range pods {
				sourceUIDs[pod.Name] = pod.UID
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Roll BOTH halves of the gang to an unpullable image (bumping only
		// one leaves the gang hash unchanged and no rollout triggers).
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, name)
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = badImage
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = badImage
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The bad target revision becomes the IR's UpdateRevision — the
		// RetryBlock is keyed on this name.
		Eventually(func(g Gomega) {
			cur := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(cur.Status.UpdateRevision).NotTo(BeEmpty())
			g.Expect(cur.Status.UpdateRevision).NotTo(Equal(v1Rev))
			badRev = cur.Status.UpdateRevision
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// First failed wave (kubelet ImagePullBackOff -> stuck-pod
		// escalation -> abandon) lands a Backoff block. The Backoff window
		// (fastRetryInitialDelay=5s) is wide against the poll interval, so
		// the state is reliably observable before the retry flips it.
		Eventually(func(g Gomega) {
			observe(g)
			b := findRetryBlock(retryBlocks(g, ns.Name, name, v1beta1.EngineComponent), badRev)
			g.Expect(b).NotTo(BeNil(), "a RetryBlock must be recorded for the failed target revision %s", badRev)
			g.Expect(b.State).To(Equal(v1beta1.RetryBlockBackoff))
			g.Expect(b.AttemptsStarted).To(BeNumerically(">=", 1))
			g.Expect(b.NextRetryAt).NotTo(BeNil(), "Backoff must persist its resume time")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// ~2 backoff windows + slack of continuous observation: the source
		// gang never churns and bad pods never pile past one gang.
		Consistently(observe).WithTimeout(noChurnWindow).WithPolling(200 * time.Millisecond).Should(Succeed())
	})

	ginkgo.It("exhausts attempts into Held with a RetryHeld event and no further churn", func() {
		// The second failed wave exhausts the fast policy (maxAttempts=2).
		Eventually(func(g Gomega) {
			observe(g)
			b := findRetryBlock(retryBlocks(g, ns.Name, name, v1beta1.EngineComponent), badRev)
			g.Expect(b).NotTo(BeNil())
			g.Expect(b.State).To(Equal(v1beta1.RetryBlockHeld))
			g.Expect(b.AttemptsStarted).To(Equal(fastRetryMaxAttempts))
			g.Expect(b.NextRetryAt).To(BeNil(), "Held has no time bound")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The operator-facing Warning lands on the parent ISVC at the
		// transition into Held.
		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "RetryHeld", "InferenceService", name)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Record the bad-revision UID set at Held (any stragglers are
		// mid-teardown); from here it may only shrink — a NEW bad-revision
		// UID would prove a retry wave fired past exhaustion.
		heldBadUIDs := map[types.UID]struct{}{}
		for uid := range badUIDsSeen {
			heldBadUIDs[uid] = struct{}{}
		}
		Consistently(func(g Gomega) {
			observe(g)
			for uid := range badUIDsSeen {
				_, known := heldBadUIDs[uid]
				g.Expect(known).To(BeTrue(),
					"no new bad-revision pod may be created after the block reaches Held (new UID %s)", uid)
			}
		}).WithTimeout(postHeldWindow).WithPolling(200 * time.Millisecond).Should(Succeed())

		// Wave accounting across the whole observation: every authorized
		// attempt creates at most one gang, so the distinct bad-revision
		// pod UIDs seen are bounded by maxAttempts * gangSize.
		Expect(len(badUIDsSeen)).To(BeNumerically("<=", int(fastRetryMaxAttempts)*gangSize),
			"distinct bad-revision pod UIDs (%d) must not exceed maxAttempts(%d) x gangSize(%d) — more means unauthorized retry waves",
			len(badUIDsSeen), fastRetryMaxAttempts, gangSize)
	})

	ginkgo.It("releases the hold when a corrected revision is published", func() {
		// Publish the corrective revision — a NEW RetrySubject, so the Held
		// block on badRev must not gate it (the Failed-wedge regression).
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, name)
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = fixedImage
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = fixedImage
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The rollout proceeds normally and converges Ready on the
		// corrected revision, which carries NO RetryBlock. The Held block
		// for badRev may remain as bounded history — it must simply not
		// gate the new revision, which convergence itself proves.
		Eventually(func(g Gomega) {
			cur := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(cur.Status.UpdateRevision).NotTo(BeEmpty())
			g.Expect(cur.Status.UpdateRevision).NotTo(Equal(badRev))
			g.Expect(cur.Status.UpdateRevision).NotTo(Equal(v1Rev))
			g.Expect(cur.Status.CurrentRevision).To(Equal(cur.Status.UpdateRevision),
				"rollout must converge on the corrected revision")
			g.Expect(cur.Status.InstanceStatuses).To(HaveLen(1))
			g.Expect(cur.Status.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cur.Status.InstanceStatuses[0].RunningRevision).To(Equal(cur.Status.UpdateRevision))
			g.Expect(findRetryBlock(cur.Status.RetryBlocks, cur.Status.UpdateRevision)).To(BeNil(),
				"the corrected revision must carry no RetryBlock")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
