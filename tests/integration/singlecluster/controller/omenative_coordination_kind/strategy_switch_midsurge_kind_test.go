package omenative_coordination_kind

import (
	"strconv"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// spec.engine.lifecycle.updateStrategy.type edited while a SurgeThenDrain roll
// is already past its point of no return.
//
// UpdateStrategy is not part of the revision payload, so the edit retargets
// nothing: the same roll continues under a different mechanism. Once the surge
// has drained its source, the cycle must run to completion — the source is
// deleted and ActiveOrdinal advances onto the replacement's slot. A mode that
// reuses one pod name has no concept of that second slot: it patches the
// drained source back to life and promotes, leaving the Instance permanently at
// two pods on one slot, with the source held out of rotation by a surge drain
// entry only its deletion would have released.
//
// The window the edit lands in is the drain itself: the row records
// Step=SurgeDrain once the replacement clears the promote bar, and holds
// there while the source is unrouted and its EndpointSlice convergence is
// polled. The spec gates the edit on that step with the source still alive,
// and re-checks the source afterwards, so a surge that finished early fails
// loudly instead of passing vacuously. The source then leaves on an ordinary
// graceful delete — its own terminationGracePeriodSeconds, never a force.
var _ = ginkgo.Describe("behavior — update strategy edited mid-surge", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "strat-switch-kss1-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("finishes the surge cycle when the edit lands after the source has drained", func() {
		const isvcName = "strat-switch-kss1"
		sourcePod := isvcName + "-engine-0-default-0"
		replacementPod := isvcName + "-engine-0-default-1"

		fix := newCoordFixture(ns.Name,
			WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))

		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type: v1beta1.UpdateStrategySurgeThenDrain,
			},
		}

		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the engine Ready at ordinal 0")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].ActiveOrdinal).To(Equal(int32(0)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("bumping the image to open a SurgeThenDrain roll")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the drain: replacement serving, source unrouted but alive")
		var replacementUID string
		Eventually(func(g Gomega) {
			// The row holds on this step while the source's EndpointSlice
			// convergence is polled. Gating on the step itself, rather than on
			// pod shape alone, is what makes the edit below land inside the
			// drain instead of racing it.
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Operation).NotTo(BeNil())
			g.Expect(insts[0].Operation.Step).To(Equal("SurgeDrain"))

			replacement := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: replacementPod,
			}, replacement)).To(Succeed())
			g.Expect(podIsReady(replacement)).To(BeTrue())
			replacementUID = string(replacement.UID)

			// The source has left rotation and is still alive: the surge is
			// past its point of no return.
			source := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: sourcePod,
			}, source)).To(Succeed())
			g.Expect(source.DeletionTimestamp).To(BeNil())
			g.Expect(podIsReady(source)).To(BeFalse())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("editing the strategy to InPlaceIfPossible inside the drain")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.Lifecycle.UpdateStrategy.Type = v1beta1.UpdateStrategyInPlaceIfPossible
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Proves the edit landed while the source was still up. Without this
		// the spec would pass vacuously if the surge had already finished on
		// its own.
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: ns.Name, Name: sourcePod,
		}, &corev1.Pod{})).To(Succeed(),
			"source was already deleted before the edit landed; the drain window was missed")

		ginkgo.By("expecting the surge cycle to finish rather than hand over mid-flight")
		Eventually(func(g Gomega) {
			source := &corev1.Pod{}
			err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: sourcePod,
			}, source)
			if err == nil && source.DeletionTimestamp != nil {
				// Whenever the going pod is observable it must be leaving on
				// its own termination grace: a zero here would mean the drain
				// force-deleted it.
				g.Expect(source.DeletionGracePeriodSeconds).NotTo(HaveValue(BeZero()),
					"the drained source must be deleted gracefully, never forced")
			}
			g.Expect(err).To(HaveOccurred(),
				"drained source survived: it is held out of rotation by a surge drain entry "+
					"that only its deletion releases, and the Instance is left at two pods")

			replacement := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: replacementPod,
			}, replacement)).To(Succeed())
			g.Expect(string(replacement.UID)).To(Equal(replacementUID))
			g.Expect(podIsReady(replacement)).To(BeTrue())

			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].ActiveOrdinal).To(Equal(int32(1)),
				"the surge promote must advance ActiveOrdinal onto the replacement's slot")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// The edit lands while a RecreatePod roll is rebuilding an Instance at a
	// revision whose image cannot be pulled. The surge the edit admits beside
	// the rebuild wedges on the same image, both attempts are disposed with
	// their pods left alive, and the Component stands at replicas plus
	// maxSurge with nothing Terminating. A corrected push must stay under
	// that ceiling: a replacement left alive holds a surge slot until a start
	// on its own Instance replaces it, so the Component never carries more
	// than replicas plus maxSurge live pods, however the wreckage was made.
	ginkgo.It("keeps live pods within replicas plus maxSurge when the edit lands during a rebuild at a broken revision and a fix is pushed", func() {
		const (
			isvcName    = "strat-switch-kss2"
			brokenImage = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
			replicas    = 2
			maxSurge    = 1
		)
		one := intstr.FromInt(1)
		readyTimeout := metav1.Duration{Duration: 45 * time.Second}

		fix := newCoordFixture(ns.Name,
			WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(replicas),
			WithEngineImage("fake-serving:v1"))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type:          v1beta1.UpdateStrategyRecreatePod,
				RollingUpdate: &v1beta1.RollingUpdate{MaxSurge: &one, MaxUnavailable: &one},
			},
			// Bounds the wedged attempts so both are disposed well inside the
			// spec's wait, instead of the default readiness window.
			InstanceReadyTimeout: &readyTimeout,
		}

		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for both engine Instances Ready on the starting revision")
		var startRevision string
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(replicas))
			for _, inst := range insts {
				g.Expect(inst.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(inst.RunningRevision).NotTo(BeEmpty())
			}
			startRevision = insts[0].RunningRevision
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The ceiling, sampled for the rest of the spec: pods no deletion
		// has been requested for, since a Terminating pod is leaving and the
		// budget already waits for its grace.
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			live := 0
			for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
				if pod.DeletionTimestamp == nil {
					live++
				}
			}
			g.Expect(live).To(BeNumerically("<=", replicas+maxSurge),
				"engine carries %d live pods; the ceiling is replicas(%d) plus maxSurge(%d)", live, replicas, maxSurge)
		})
		defer stop()

		ginkgo.By("pushing a revision whose image cannot be pulled; the recreate rebuilds one Instance at it")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = brokenImage
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the rebuild to be materializing: the Instance on its Drain step with its rebuilt pod waiting on the image")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			rebuilding := int32(-1)
			for _, inst := range insts {
				if inst.Phase == v1beta1.OMENativeInstanceUpdating && inst.Operation != nil && inst.Operation.Step == "Drain" {
					rebuilding = inst.Index
				}
			}
			g.Expect(rebuilding).NotTo(Equal(int32(-1)), "no Instance is on a recreate's Drain step yet")
			rebuilt := false
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if pod.Labels[query.LabelInstanceIdx] == strconv.Itoa(int(rebuilding)) && pod.Spec.Containers[0].Image == brokenImage {
					rebuilt = true
				}
			}
			g.Expect(rebuilt).To(BeTrue(), "the rebuilt pod at the broken revision has not been created yet")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("editing the strategy to SurgeThenDrain while the rebuild is materializing")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.Lifecycle.UpdateStrategy.Type = v1beta1.UpdateStrategySurgeThenDrain
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for both attempts at the broken revision to be disposed")
		// The rebuilt Instance has no serving pod left: it reads Failed with
		// its attempt parked. The surged Instance keeps serving its source:
		// it reads Ready with the failed attempt on its record. Neither has
		// an attempt in flight.
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			brokenRev := ir.Status.UpdateRevision
			g.Expect(brokenRev).NotTo(BeEmpty())
			g.Expect(brokenRev).NotTo(Equal(startRevision))
			insts := decodedInstanceStatuses(g, ir)
			g.Expect(insts).To(HaveLen(replicas))
			for _, inst := range insts {
				expectNoAttemptInFlight(g, inst, brokenRev)
				g.Expect(inst.LastFailure).NotTo(BeNil(),
					"Instance %d must record its failed attempt at the broken revision", inst.Index)
				switch inst.Phase {
				case v1beta1.OMENativeInstanceFailed:
				case v1beta1.OMENativeInstanceReady:
					g.Expect(inst.Operation).To(BeNil(), "Instance %d: a Ready row carries no parked attempt", inst.Index)
					g.Expect(inst.RunningRevision).To(Equal(startRevision),
						"Instance %d reads Ready only on its serving source", inst.Index)
					g.Expect(inst.TargetRevision).To(Equal(brokenRev),
						"Instance %d: the revision the failed attempt was for stays on the row", inst.Index)
				default:
					g.Expect(inst.Phase).To(BeElementOf(v1beta1.OMENativeInstanceFailed, v1beta1.OMENativeInstanceReady),
						"Instance %d must be disposed at the broken revision", inst.Index)
				}
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("pushing the corrected image")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the fix to land on every Instance with nothing of the broken revision left")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(replicas))
			for _, inst := range insts {
				g.Expect(inst.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(inst.RunningRevision).NotTo(BeEmpty())
				g.Expect(inst.RunningRevision).NotTo(Equal(startRevision))
				g.Expect(inst.RunningRevision).To(Equal(insts[0].RunningRevision))
			}
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				g.Expect(pod.Spec.Containers[0].Image).NotTo(Equal(brokenImage),
					"pod %s still runs the broken revision", pod.Name)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		// Ends the sampler before teardown so the ceiling is judged on the
		// roll alone, and so a sampler failure surfaces here.
		stop()
	})
})
