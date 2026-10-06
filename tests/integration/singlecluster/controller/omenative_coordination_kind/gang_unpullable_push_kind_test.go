package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A gang pushed onto an image that cannot be pulled is bounded the way a
// single pod is.
//
// Under RecreatePod the first gang is drained and its replacement parks
// in ImagePullBackOff. Each attempt ends within the stuck-pod grace, is
// counted on the revision's retry ladder and leaves the gang's row at
// Failed with the pull failure as its reason and the disposed attempt
// parked on the ladder's wait while its pods live; at the bound the revision
// is Held, the status says so, and no further attempt is opened. The gang
// the roll never reached keeps serving throughout, and a corrected push
// lands on both gangs.
//
// The shape: two leader-plus-worker engine gangs under RecreatePod with
// maxUnavailable=1 and no decoder, pushed onto an image the cluster cannot
// pull.
//
// Cells: instance/Update/Drain/pod.waiting[ImagePullBackOff]/before

// gangUnpullablePushAttempts is the bound the spec configures and asserts
// against; the lifecycle JSON below states the same number.
const gangUnpullablePushAttempts = int32(2)

// A seconds-scale ladder and a short stuck-pod grace so the pull failure
// is classified within the suite budget.
const gangUnpullablePushLifecycleJSON = `{"updateRetry":{"maxAttempts":2,"initialDelay":"5s","maxDelay":"20s","multiplier":2.0},"stuckPodGracePeriod":"10s"}`

var _ = ginkgo.Describe("behavior - a gang pushed onto an unpullable image is held within the ladder (KIND)", ginkgo.Ordered, func() {
	const (
		isvcName        = "gang-unpullable"
		goodImage       = "fake-serving:v1"
		fixedImage      = "fake-serving:v2"
		unpullableImage = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
		// healthyIndex is the gang the roll reaches second; under
		// maxUnavailable=1 it is never started while the first gang's
		// attempts are retried.
		healthyIndex = "1"
		// postHeldWindow covers the whole fast ladder (5s + 10s) with
		// slack, long enough that an attempt reopened past the bound
		// would surface as an Updating row or a new pod.
		postHeldWindow = 25 * time.Second
	)

	var (
		ns *corev1.Namespace

		v1Rev  string
		badRev string
		// healthyUIDs is the pod name -> UID set of the healthy gang at
		// Ready; it must survive the whole episode untouched.
		healthyUIDs map[string]types.UID
		// badUIDsSeen accumulates every unpullable-image pod UID any poll
		// observed. Distinct UIDs bound the attempts that materialized.
		badUIDsSeen map[types.UID]struct{}
	)

	ginkgo.BeforeAll(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "gang-unpullable-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		})
		configureLifecycleConfig(gangUnpullablePushLifecycleJSON)

		maxSurge, maxUnavailable := intstr.FromInt(0), intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineWorkerSize(1),
			WithEngineImage(goodImage),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type:          v1beta1.UpdateStrategyRecreatePod,
				RollingUpdate: &v1beta1.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		healthyUIDs = map[string]types.UID{}
		badUIDsSeen = map[types.UID]struct{}{}
	})

	onUnpullableImage := func(pod *corev1.Pod) bool {
		return pod.Spec.Containers[0].Image == unpullableImage
	}

	// enginePods returns the non-deleting engine pods.
	enginePods := func(g Gomega) []*corev1.Pod {
		var live []*corev1.Pod
		for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
			if pod.DeletionTimestamp == nil {
				live = append(live, &pod)
			}
		}
		return live
	}

	// observe enforces the standing invariants on every poll that threads
	// it: the healthy gang keeps its pods and stays in rotation, and every
	// unpullable-image pod UID is recorded for the attempt accounting.
	observe := func(g Gomega) {
		pods := enginePods(g)
		for _, pod := range pods {
			if onUnpullableImage(pod) {
				badUIDsSeen[pod.UID] = struct{}{}
			}
		}
		for podName, uid := range healthyUIDs {
			var live *corev1.Pod
			for _, pod := range pods {
				if pod.Name == podName {
					live = pod
					break
				}
			}
			g.Expect(live).NotTo(BeNil(), "healthy gang pod %s must survive the unpullable push", podName)
			g.Expect(live.UID).To(Equal(uid), "healthy gang pod %s must keep its UID", podName)
			g.Expect(podreadiness.IsServing(live)).To(BeTrue(),
				"healthy gang pod %s must stay in rotation while the other gang's attempts are retried", podName)
		}
	}

	ginkgo.It("converges both gangs on v1", func() {
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.CurrentRevision).NotTo(BeEmpty())
			g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision))
			rows := decodedInstanceStatuses(g, ir)
			g.Expect(rows).To(HaveLen(2))
			for _, row := range rows {
				g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			v1Rev = ir.Status.CurrentRevision
			pods := enginePods(g)
			g.Expect(pods).To(HaveLen(4), "two gangs of a leader and a worker")
			healthyUIDs = map[string]types.UID{}
			for _, pod := range pods {
				g.Expect(podreadiness.IsServing(pod)).To(BeTrue())
				if pod.Labels[query.LabelInstanceIdx] == healthyIndex {
					healthyUIDs[pod.Name] = pod.UID
				}
			}
			g.Expect(healthyUIDs).To(HaveLen(2))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("pushes an image the cluster cannot pull", func() {
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			// A gang renders the leader and the worker templates; both move
			// so the revision does.
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = unpullableImage
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = unpullableImage
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty())
			g.Expect(ir.Status.UpdateRevision).NotTo(Equal(v1Rev))
			badRev = ir.Status.UpdateRevision
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("holds the revision at the bound with the pull failure named", func() {
		Eventually(func(g Gomega) {
			observe(g)
			b := findRetryBlock(retryBlocks(g, ns.Name, isvcName, v1beta1.EngineComponent), badRev)
			g.Expect(b).NotTo(BeNil(), "a RetryBlock must be recorded for the unpullable revision %s", badRev)
			g.Expect(b.State).To(Equal(v1beta1.RetryBlockHeld))
			g.Expect(b.AttemptsStarted).To(Equal(gangUnpullablePushAttempts))
			g.Expect(b.NextRetryAt).To(BeNil(), "Held has no time bound")
			g.Expect(b.Reason).To(BeElementOf("ImagePullBackOff", "ErrImagePull"))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			observe(g)
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.RolloutHold).NotTo(BeNil(), "status.rolloutHold must surface the Held denial")
			g.Expect(ir.Status.RolloutHold.Gate).To(Equal(v1beta1.RolloutHoldGateHeld))
			g.Expect(ir.Status.RolloutHold.Target).To(Equal(badRev))
			rows := decodedInstanceStatuses(g, ir)
			g.Expect(rows).To(HaveLen(2))
			for _, row := range rows {
				expectNoAttemptInFlight(g, row, badRev)
				g.Expect(row.RunningRevision).To(Equal(v1Rev), "instance %d must still record the sound revision", row.Index)
				if row.Index == 0 {
					g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceFailed), "the gang the push reached must read Failed")
					g.Expect(row.LastFailure).NotTo(BeNil(), "the Failed row must carry the pull failure")
					g.Expect(row.LastFailure.Reason).To(BeElementOf("ImagePullBackOff", "ErrImagePull"))
				} else {
					g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady), "the gang the push never reached must stay Ready")
				}
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("opens no further attempt while Held", func() {
		heldUIDs := map[types.UID]struct{}{}
		for uid := range badUIDsSeen {
			heldUIDs[uid] = struct{}{}
		}
		Consistently(func(g Gomega) {
			observe(g)
			for uid := range badUIDsSeen {
				_, known := heldUIDs[uid]
				g.Expect(known).To(BeTrue(), "no unpullable-image pod may be created after Held (new UID %s)", uid)
			}
			for _, row := range instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent) {
				expectNoAttemptInFlight(g, row, badRev)
			}
		}, postHeldWindow, kindPollInterval).Should(Succeed())
		// Each attempt materializes one gang of two pods.
		Expect(len(badUIDsSeen)).To(BeNumerically("<=", 2*int(gangUnpullablePushAttempts)),
			"distinct unpullable-image pods (%d) must not exceed one gang per attempt of the bound (%d)", len(badUIDsSeen), 2*gangUnpullablePushAttempts)
	})

	ginkgo.It("converges both gangs on a corrected revision", func() {
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = fixedImage
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = fixedImage
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
			pods := enginePods(g)
			g.Expect(pods).To(HaveLen(4), "two gangs of a leader and a worker on the corrected revision")
			for _, pod := range pods {
				g.Expect(onUnpullableImage(pod)).To(BeFalse(), "no unpullable-image pod may remain: %s", pod.Name)
				g.Expect(podreadiness.IsServing(pod)).To(BeTrue(), "every member of both gangs must serve: %s", pod.Name)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
