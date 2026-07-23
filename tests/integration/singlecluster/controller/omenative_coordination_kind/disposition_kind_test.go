package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Deadline disposition — workload-caused failure holds the revision,
// never relocates, and the churn loop is bounded.
//
// A single-pod recreate-class engine created with an unpullable image
// parks in ImagePullBackOff: a failure that travels with the revision,
// so relocating the pod would reproduce it identically on any node.
// The disposition must therefore:
//
//   - record a RetryBlock against the target revision (Backoff →
//     Held per the operator updateRetry policy) and clear the failed
//     attempt's Operation — Phase=Failed with no Operation, a fresh
//     start the create/update gates then deny for the blocked revision;
//   - NEVER touch the relocation machinery, even with
//     MigrationPolicy.Mode=Auto and a configured relocation budget: no
//     AutoRecover ledger entry, no Migrate operation, no surge index;
//   - stop the churn: once the block reaches Held, no new pod is ever
//     created for the bad revision (distinct bad-pod UIDs are bounded
//     by the authorized attempt count);
//   - release immediately on a corrected revision: a NEW revision is a
//     different retry subject, so publishing a fixed image rolls out
//     without waiting on anything.
var _ = ginkgo.Describe("OMENative deadline disposition holds workload-caused failures (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "disposition-hold-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("bad image → RetryBlock + zero relocation; churn stops at Held; a fixed image releases", func() {
		const (
			isvcName   = "disp-hold"
			badImage   = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
			fixedImage = "fake-serving:v1"
		)
		// Post-Held observation window: covers the full fast backoff
		// ladder (5s + 10s) plus slack, long enough that a rogue retry
		// wave past exhaustion would surface as a new pod UID.
		const postHeldWindow = 20 * time.Second

		configureFastDisposition()

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithEngineImage(badImage))
		// Mode=Auto + a configured budget on purpose: the workload-caused
		// classification must win over relocation, not merely coincide
		// with it being disabled.
		fix.ISVC.Spec.Engine.Lifecycle.MigrationPolicy = &v1beta1.MigrationPolicy{
			Mode: v1beta1.MigrationPolicyModeAuto,
		}
		fix.ISVC.Spec.Engine.Lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 45 * time.Second}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// badUIDsSeen accumulates every bad-revision pod UID any poll
		// observed. Distinct UIDs bound the authorized create waves.
		badUIDsSeen := map[types.UID]struct{}{}

		// observe enforces the standing no-relocation invariants on
		// every poll that threads it:
		//   - the audit ledger never records an AutoRecover directive;
		//   - no Instance ever carries a Migrate operation and no surge
		//     index appears (the index space stays exactly {0});
		//   - at most one bad-revision pod is alive at a time (waves
		//     replace, they never pile up), and every bad-pod UID is
		//     recorded for the wave accounting.
		observe := func(g Gomega) {
			g.Expect(autoRecoverEntries(loadLedger(g, ns.Name, isvcName), "engine")).To(BeEmpty(),
				"a workload-caused failure must NEVER record a relocation directive")

			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(len(insts)).To(BeNumerically("<=", 1),
				"no surge index may appear for a held revision; got %+v", insts)
			for _, s := range insts {
				if s.Operation != nil {
					g.Expect(s.Operation.Type).NotTo(Equal(v1beta1.InstanceOperationMigrate),
						"a workload-caused failure must never dispatch a Migrate operation")
				}
			}

			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			alive := 0
			for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
				bad := false
				for _, c := range pod.Spec.Containers {
					if c.Image == badImage {
						bad = true
						break
					}
				}
				if !bad {
					continue
				}
				badUIDsSeen[pod.UID] = struct{}{}
				if pod.DeletionTimestamp == nil {
					alive++
				}
			}
			g.Expect(alive).To(BeNumerically("<=", 1),
				"bad-revision pods alive at once must never exceed one; observed %d", alive)
		}

		ginkgo.By("waiting for the RetryBlock to form for the bad target revision")
		var badRev string
		Eventually(func(g Gomega) {
			observe(g)
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty())
			badRev = ir.Status.UpdateRevision
			b := findRetryBlock(ir.Status.RetryBlocks, badRev)
			g.Expect(b).NotTo(BeNil(),
				"a RetryBlock must be recorded for the failed target revision %s", badRev)
			g.Expect(b.AttemptsStarted).To(BeNumerically(">=", 1))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the attempts to exhaust into Held")
		Eventually(func(g Gomega) {
			observe(g)
			b := findRetryBlock(retryBlocks(g, ns.Name, isvcName, v1beta1.EngineComponent), badRev)
			g.Expect(b).NotTo(BeNil())
			g.Expect(b.State).To(Equal(v1beta1.RetryBlockHeld))
			g.Expect(b.AttemptsStarted).To(Equal(fastRetryMaxAttempts))
			g.Expect(b.NextRetryAt).To(BeNil(), "Held has no time bound")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("holding: churn stopped, no relocation machinery, no new bad-revision pods")
		// Freeze the UID set at Held; from here it may only shrink — a
		// NEW bad-revision UID proves an unauthorized wave fired.
		heldBadUIDs := map[types.UID]struct{}{}
		for uid := range badUIDsSeen {
			heldBadUIDs[uid] = struct{}{}
		}
		Consistently(func(g Gomega) {
			observe(g)
			for uid := range badUIDsSeen {
				_, known := heldBadUIDs[uid]
				g.Expect(known).To(BeTrue(),
					"no new bad-revision pod may be created after Held (new UID %s)", uid)
			}
		}, postHeldWindow, 200*time.Millisecond).Should(Succeed())

		// Wave accounting: each authorized attempt creates at most one
		// pod, so the distinct bad-revision UIDs seen are bounded by the
		// policy's attempt budget.
		Expect(len(badUIDsSeen)).To(BeNumerically("<=", int(fastRetryMaxAttempts)),
			"distinct bad-revision pod UIDs (%d) must not exceed maxAttempts (%d) — more means unauthorized retry waves",
			len(badUIDsSeen), fastRetryMaxAttempts)

		ginkgo.By("publishing the corrected revision")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = fixedImage
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the corrected revision to release immediately and converge Ready")
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty())
			g.Expect(ir.Status.UpdateRevision).NotTo(Equal(badRev),
				"the fixed image must produce a NEW target revision")
			g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision),
				"rollout must converge on the corrected revision")
			g.Expect(ir.Status.InstanceStatuses).To(HaveLen(1))
			g.Expect(ir.Status.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(ir.Status.InstanceStatuses[0].RunningRevision).To(Equal(ir.Status.UpdateRevision))
			g.Expect(findRetryBlock(ir.Status.RetryBlocks, ir.Status.UpdateRevision)).To(BeNil(),
				"the corrected revision must carry no RetryBlock")
			g.Expect(autoRecoverEntries(loadLedger(g, ns.Name, isvcName), "engine")).To(BeEmpty(),
				"the release path must not involve relocation either")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
