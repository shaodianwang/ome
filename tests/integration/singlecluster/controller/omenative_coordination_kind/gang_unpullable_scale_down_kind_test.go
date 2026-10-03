package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A scale-down lands on a gang whose push is stuck: the first gang has
// been drained and its replacement sits in ImagePullBackOff with the
// attempt still open, inside a stuck-pod grace long enough that nothing
// else ends it. The scale retires that gang — the one that serves nothing
// — and the attempt ends with it: the ladder records the pull failure, the
// revision is Held at its bound of one attempt, the gang the roll never
// reached keeps the very pods it serves through, no further pod on the
// unpullable revision is created, and the status says why the push has
// not landed. A corrected push then lands on the one gang left.
//
// The shape: two leader-plus-worker engine gangs under RecreatePod with
// maxUnavailable=1 and no decoder, pushed onto an image the cluster cannot
// pull, scaled to one while the first attempt is open.
//
// Cells: instance/Update/Drain/spec.replicasDown/before

// gangUnpullableScaleDownLifecycleJSON bounds the ladder at one attempt
// and keeps the stuck-pod grace well past the spec, so only the scale can
// end the attempt it finds open.
const gangUnpullableScaleDownLifecycleJSON = `{"updateRetry":{"maxAttempts":1,"initialDelay":"5s","maxDelay":"20s","multiplier":2.0},"stuckPodGracePeriod":"10m"}`

var _ = ginkgo.Describe("behavior - a scale-down on a gang stuck on an unpullable image ends the attempt on the ladder (KIND)", ginkgo.Ordered, func() {
	const (
		isvcName        = "gang-unpullable-scale"
		goodImage       = "fake-serving:v1"
		fixedImage      = "fake-serving:v2"
		unpullableImage = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
		// stuckIndex is the gang the roll reaches first and the scale
		// retires; healthyIndex is the gang the roll never reaches.
		stuckIndex   = "0"
		healthyIndex = "1"
		// afterScaleWindow is how long the survivor is watched after the
		// scale has settled: several times the ladder's delays, so an
		// attempt reopened on it would surface as an Updating row or a
		// pod on the unpullable image.
		afterScaleWindow = 30 * time.Second
	)

	var (
		ns *corev1.Namespace

		v1Rev  string
		badRev string
		// healthyUIDs is the pod name -> UID set of the healthy gang at
		// Ready; it must survive the whole episode untouched.
		healthyUIDs map[string]types.UID
	)

	ginkgo.BeforeAll(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "gang-unpullable-scale-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		})
		configureLifecycleConfig(gangUnpullableScaleDownLifecycleJSON)

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

	// pullFailureShown reports whether a container of the pod is parked
	// in a pull failure, the evidence the wave records the attempt by.
	pullFailureShown := func(pod *corev1.Pod) bool {
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Waiting != nil && (cs.State.Waiting.Reason == "ImagePullBackOff" || cs.State.Waiting.Reason == "ErrImagePull") {
				return true
			}
		}
		return false
	}

	// healthyUntouched enforces the standing invariant on every poll that
	// threads it: the healthy gang keeps its pods and stays in rotation.
	healthyUntouched := func(g Gomega) {
		pods := enginePods(g)
		for podName, uid := range healthyUIDs {
			var live *corev1.Pod
			for _, pod := range pods {
				if pod.Name == podName {
					live = pod
					break
				}
			}
			g.Expect(live).NotTo(BeNil(), "healthy gang pod %s must survive the scale on the stuck gang", podName)
			g.Expect(live.UID).To(Equal(uid), "healthy gang pod %s must keep its UID", podName)
			g.Expect(podreadiness.IsServing(live)).To(BeTrue(), "healthy gang pod %s must stay in rotation", podName)
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

	ginkgo.It("pushes an image the cluster cannot pull and finds the first attempt parked", func() {
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

		// The attempt is open: the stuck gang's row is Updating toward the
		// pushed revision, its replacement pods show the pull failure, and
		// no retry block exists yet — a first attempt opens without one.
		Eventually(func(g Gomega) {
			healthyUntouched(g)
			var stuck *v1beta1.OMENativeInstanceStatus
			for _, row := range instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent) {
				if row.Index == 0 {
					r := row
					stuck = &r
				}
			}
			g.Expect(stuck).NotTo(BeNil())
			g.Expect(stuck.Phase).To(Equal(v1beta1.OMENativeInstanceUpdating), "the roll must hold the stuck gang open")
			g.Expect(stuck.Operation).NotTo(BeNil())
			g.Expect(stuck.Operation.TargetRevision).To(Equal(badRev))
			parked := 0
			for _, pod := range enginePods(g) {
				if pod.Labels[query.LabelInstanceIdx] == stuckIndex && onUnpullableImage(pod) && pullFailureShown(pod) {
					parked++
				}
			}
			g.Expect(parked).To(BeNumerically(">=", 1), "the replacement must show the pull failure the wave records the attempt by")
			g.Expect(findRetryBlock(retryBlocks(g, ns.Name, isvcName, v1beta1.EngineComponent), badRev)).To(BeNil(),
				"the first attempt opens with no retry block; the scale is what ends it")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("scales to one and the attempt ends on the ladder with the stuck gang", func() {
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			one := 1
			cur.Spec.Engine.MinReplicas = &one
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			healthyUntouched(g)
			b := findRetryBlock(retryBlocks(g, ns.Name, isvcName, v1beta1.EngineComponent), badRev)
			g.Expect(b).NotTo(BeNil(), "the wave must record the attempt it ended on the ladder for %s", badRev)
			g.Expect(b.State).To(Equal(v1beta1.RetryBlockHeld), "one attempt is the bound; the attempt the scale ended fills it")
			g.Expect(b.AttemptsStarted).To(Equal(int32(1)))
			g.Expect(b.NextRetryAt).To(BeNil(), "Held has no time bound")
			g.Expect(b.Reason).To(BeElementOf("ImagePullBackOff", "ErrImagePull"))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			healthyUntouched(g)
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			rows := decodedInstanceStatuses(g, ir)
			g.Expect(rows).To(HaveLen(1), "the scale leaves the one gang that serves")
			g.Expect(rows[0].Index).To(Equal(int32(1)), "the gang the roll never reached is the survivor")
			g.Expect(rows[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(rows[0].Operation).To(BeNil(), "a held revision leaves no attempt open on the survivor")
			g.Expect(rows[0].RunningRevision).To(Equal(v1Rev), "the survivor must still record the sound revision")
			g.Expect(ir.Status.RolloutHold).NotTo(BeNil(), "status.rolloutHold must surface the Held denial")
			g.Expect(ir.Status.RolloutHold.Gate).To(Equal(v1beta1.RolloutHoldGateHeld))
			g.Expect(ir.Status.RolloutHold.Target).To(Equal(badRev))
			pods := enginePods(g)
			g.Expect(pods).To(HaveLen(2), "one gang of a leader and a worker")
			for _, pod := range pods {
				g.Expect(onUnpullableImage(pod)).To(BeFalse(), "no unpullable-image pod may remain: %s", pod.Name)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("opens no further attempt on the survivor while Held", func() {
		Consistently(func(g Gomega) {
			healthyUntouched(g)
			for _, pod := range enginePods(g) {
				g.Expect(onUnpullableImage(pod)).To(BeFalse(), "no pod may be created on the unpullable image after the scale: %s", pod.Name)
			}
			for _, row := range instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent) {
				g.Expect(row.Operation).To(BeNil(), "no attempt may reopen on instance %d while the revision is Held", row.Index)
			}
		}, afterScaleWindow, kindPollInterval).Should(Succeed())
	})

	ginkgo.It("converges the survivor on a corrected revision", func() {
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
			g.Expect(rows).To(HaveLen(1))
			g.Expect(rows[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(rows[0].RunningRevision).To(Equal(ir.Status.UpdateRevision))
			g.Expect(ir.Status.RolloutHold).To(BeNil(), "status.rolloutHold must clear once the rollout converges")
			pods := enginePods(g)
			g.Expect(pods).To(HaveLen(2), "one gang of a leader and a worker on the corrected revision")
			for _, pod := range pods {
				g.Expect(onUnpullableImage(pod)).To(BeFalse(), "no unpullable-image pod may remain: %s", pod.Name)
				g.Expect(podreadiness.IsServing(pod)).To(BeTrue(), "every member of the gang must serve: %s", pod.Name)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})

// The prefill-decode shape of the same episode under rolling coordination:
// a single-pod engine and a single-pod decoder, two Instances each, roll in
// parallel onto the unpullable image and each parks its first Instance's
// replacement with the attempt open. Both are scaled to one. On each
// Component the scale retires the stuck Instance and the attempt ends on
// that Component's own ladder: Held at its bound, the Instance the roll
// never reached keeps the very pod it serves through, and the status
// reports the hold and its target.
//
// Cells: instance/Update/Drain/spec.replicasDown/before
var _ = ginkgo.Describe("behavior - a scale-down on a prefill-decode pair stuck on an unpullable image ends each attempt on its ladder (KIND)", ginkgo.Ordered, func() {
	const (
		isvcName        = "pd-unpullable-scale"
		goodImage       = "fake-serving:v1"
		unpullableImage = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
		stuckIndex      = "0"
		healthyIndex    = "1"
		// afterScaleWindow is how long the survivors are watched after
		// the scale has settled: several times the ladder's delays, so an
		// attempt reopened on one would surface as an Updating row or a
		// pod on the unpullable image.
		afterScaleWindow = 30 * time.Second
	)
	components := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}

	var (
		ns *corev1.Namespace

		v1Rev  map[v1beta1.ComponentType]string
		badRev map[v1beta1.ComponentType]string
		// healthyUIDs is, per Component, the pod name -> UID of the
		// Instance the roll never reaches; it must survive untouched.
		healthyUIDs map[v1beta1.ComponentType]map[string]types.UID
	)

	ginkgo.BeforeAll(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "pd-unpullable-scale-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		})
		configureLifecycleConfig(gangUnpullableScaleDownLifecycleJSON)

		maxSurge, maxUnavailable := intstr.FromInt(0), intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithEngineReplicas(2),
			WithEngineImage(goodImage),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithDecoderReplicas(2),
			WithDecoderImage(goodImage),
			WithDecoderUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithRollingUpdateGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		lifecycle := func() *v1beta1.LifecycleSpec {
			return &v1beta1.LifecycleSpec{
				UpdateStrategy: &v1beta1.UpdateStrategy{
					Type:          v1beta1.UpdateStrategyRecreatePod,
					RollingUpdate: &v1beta1.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
				},
			}
		}
		fix.ISVC.Spec.Engine.Lifecycle = lifecycle()
		fix.ISVC.Spec.Decoder.Lifecycle = lifecycle()
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		v1Rev = map[v1beta1.ComponentType]string{}
		badRev = map[v1beta1.ComponentType]string{}
		healthyUIDs = map[v1beta1.ComponentType]map[string]types.UID{}
	})

	onUnpullableImage := func(pod *corev1.Pod) bool {
		return pod.Spec.Containers[0].Image == unpullableImage
	}

	// componentPods returns the non-deleting pods of one Component.
	componentPods := func(g Gomega, component v1beta1.ComponentType) []*corev1.Pod {
		var live []*corev1.Pod
		for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
			if pod.DeletionTimestamp == nil && pod.Labels[constants.OMEComponentLabel] == string(component) {
				live = append(live, &pod)
			}
		}
		return live
	}

	pullFailureShown := func(pod *corev1.Pod) bool {
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Waiting != nil && (cs.State.Waiting.Reason == "ImagePullBackOff" || cs.State.Waiting.Reason == "ErrImagePull") {
				return true
			}
		}
		return false
	}

	// healthyUntouched enforces the standing invariant on every poll that
	// threads it: on each Component the healthy Instance keeps its pod
	// and stays in rotation.
	healthyUntouched := func(g Gomega) {
		for _, component := range components {
			pods := componentPods(g, component)
			for podName, uid := range healthyUIDs[component] {
				var live *corev1.Pod
				for _, pod := range pods {
					if pod.Name == podName {
						live = pod
						break
					}
				}
				g.Expect(live).NotTo(BeNil(), "%s pod %s must survive the scale on the stuck Instance", component, podName)
				g.Expect(live.UID).To(Equal(uid), "%s pod %s must keep its UID", component, podName)
				g.Expect(podreadiness.IsServing(live)).To(BeTrue(), "%s pod %s must stay in rotation", component, podName)
			}
		}
	}

	ginkgo.It("converges both Components on v1", func() {
		Eventually(func(g Gomega) {
			for _, component := range components {
				ir := freshInferenceReplica(g, ns.Name, isvcName, component)
				g.Expect(ir.Status.CurrentRevision).NotTo(BeEmpty())
				g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision))
				rows := decodedInstanceStatuses(g, ir)
				g.Expect(rows).To(HaveLen(2))
				for _, row := range rows {
					g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				}
				v1Rev[component] = ir.Status.CurrentRevision
				pods := componentPods(g, component)
				g.Expect(pods).To(HaveLen(2), "%s: two single-pod Instances", component)
				healthyUIDs[component] = map[string]types.UID{}
				for _, pod := range pods {
					g.Expect(podreadiness.IsServing(pod)).To(BeTrue())
					if pod.Labels[query.LabelInstanceIdx] == healthyIndex {
						healthyUIDs[component][pod.Name] = pod.UID
					}
				}
				g.Expect(healthyUIDs[component]).To(HaveLen(1))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("pushes an image the cluster cannot pull and finds each first attempt parked", func() {
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = unpullableImage
			cur.Spec.Decoder.PodSpec.Containers[0].Image = unpullableImage
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			for _, component := range components {
				ir := freshInferenceReplica(g, ns.Name, isvcName, component)
				g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty())
				g.Expect(ir.Status.UpdateRevision).NotTo(Equal(v1Rev[component]))
				badRev[component] = ir.Status.UpdateRevision
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Rolling coordination rolls the Components in parallel, so each
		// holds its first Instance open at once: the row is Updating toward
		// the pushed revision, its replacement shows the pull failure, and
		// no retry block exists yet.
		Eventually(func(g Gomega) {
			healthyUntouched(g)
			for _, component := range components {
				var stuck *v1beta1.OMENativeInstanceStatus
				for _, row := range instanceStatuses(g, ns.Name, isvcName, component) {
					if row.Index == 0 {
						r := row
						stuck = &r
					}
				}
				g.Expect(stuck).NotTo(BeNil(), "%s: instance 0 must be present", component)
				g.Expect(stuck.Phase).To(Equal(v1beta1.OMENativeInstanceUpdating), "%s: the roll must hold the stuck Instance open", component)
				g.Expect(stuck.Operation).NotTo(BeNil())
				g.Expect(stuck.Operation.TargetRevision).To(Equal(badRev[component]))
				parked := 0
				for _, pod := range componentPods(g, component) {
					if pod.Labels[query.LabelInstanceIdx] == stuckIndex && onUnpullableImage(pod) && pullFailureShown(pod) {
						parked++
					}
				}
				g.Expect(parked).To(Equal(1), "%s: the replacement must show the pull failure the wave records the attempt by", component)
				g.Expect(findRetryBlock(retryBlocks(g, ns.Name, isvcName, component), badRev[component])).To(BeNil(),
					"%s: the first attempt opens with no retry block; the scale is what ends it", component)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("scales both to one and each attempt ends on its own ladder", func() {
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			one := 1
			cur.Spec.Engine.MinReplicas = &one
			cur.Spec.Decoder.MinReplicas = &one
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			healthyUntouched(g)
			for _, component := range components {
				b := findRetryBlock(retryBlocks(g, ns.Name, isvcName, component), badRev[component])
				g.Expect(b).NotTo(BeNil(), "%s: the wave must record the attempt it ended on the ladder for %s", component, badRev[component])
				g.Expect(b.State).To(Equal(v1beta1.RetryBlockHeld), "%s: one attempt is the bound; the attempt the scale ended fills it", component)
				g.Expect(b.AttemptsStarted).To(Equal(int32(1)))
				g.Expect(b.Reason).To(BeElementOf("ImagePullBackOff", "ErrImagePull"))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			healthyUntouched(g)
			for _, component := range components {
				ir := freshInferenceReplica(g, ns.Name, isvcName, component)
				rows := decodedInstanceStatuses(g, ir)
				g.Expect(rows).To(HaveLen(1), "%s: the scale leaves the one Instance that serves", component)
				g.Expect(rows[0].Index).To(Equal(int32(1)), "%s: the Instance the roll never reached is the survivor", component)
				g.Expect(rows[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(rows[0].Operation).To(BeNil(), "%s: a held revision leaves no attempt open on the survivor", component)
				g.Expect(rows[0].RunningRevision).To(Equal(v1Rev[component]), "%s: the survivor must still record the sound revision", component)
				g.Expect(ir.Status.RolloutHold).NotTo(BeNil(), "%s: status.rolloutHold must surface the Held denial", component)
				g.Expect(ir.Status.RolloutHold.Gate).To(Equal(v1beta1.RolloutHoldGateHeld))
				g.Expect(ir.Status.RolloutHold.Target).To(Equal(badRev[component]))
				pods := componentPods(g, component)
				g.Expect(pods).To(HaveLen(1), "%s: one Instance left", component)
				g.Expect(onUnpullableImage(pods[0])).To(BeFalse(), "%s: no unpullable-image pod may remain: %s", component, pods[0].Name)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("opens no further attempt on either survivor while Held", func() {
		Consistently(func(g Gomega) {
			healthyUntouched(g)
			for _, component := range components {
				for _, pod := range componentPods(g, component) {
					g.Expect(onUnpullableImage(pod)).To(BeFalse(), "%s: no pod may be created on the unpullable image after the scale: %s", component, pod.Name)
				}
				for _, row := range instanceStatuses(g, ns.Name, isvcName, component) {
					g.Expect(row.Operation).To(BeNil(), "%s: no attempt may reopen on instance %d while the revision is Held", component, row.Index)
				}
			}
		}, afterScaleWindow, kindPollInterval).Should(Succeed())
	})
})
