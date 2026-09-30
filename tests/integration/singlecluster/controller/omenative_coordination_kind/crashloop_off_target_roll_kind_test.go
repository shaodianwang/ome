package omenative_coordination_kind

import (
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A fix pushed as a new revision must land on the Instance that is broken.
//
// An Instance whose revision is not the roll target belongs to the
// update pass: its wedge is unavailability the roll must cross anyway, so
// the roll replaces the pod set at the target and no repair opens on the
// revision the Component does not want. The shape here is three engine
// Instances under SurgeThenDrain with maxSurge=1 / maxUnavailable=0 and one
// Instance wedged before the bump. maxUnavailable=0 denies the crash-loop
// repair on every pass, so the wedge stands until the bump; the roll then
// surges the fixed replacement beside the wedged pod and drains it away.
//
// The wedge is a live pod pointed at an image the cluster cannot pull: a
// running pod's image is its only mutable field, and the fake serving image
// has no per-pod crash switch. ImagePullBackOff is a terminal kubelet
// waiting reason of the same class as a crash at start, and the crash-loop
// repair reads both the same way.
//
// A seconds-scale stuck-pod grace so the wedge is classified within the
// suite budget. Nothing else about the shape depends on its value.
const offTargetRollLifecycleJSON = `{"stuckPodGracePeriod":"10s"}`

// Cells: instance/Ready/pod.waiting[ImagePullBackOff]/boundary@T-ready-surge, instance/Ready/spec.revision/boundary@T-ready-crashloop-restart
var _ = ginkgo.Describe("behavior — a wedged Instance is rolled to the fixed revision, never repaired on the broken one", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "crashroll-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("the wedged Instance ends serving on the fixed revision with no repair recorded for it", func() {
		const isvcName = "crashroll"
		const wedgedIndex = "0"
		configureLifecycleConfig(offTargetRollLifecycleJSON)

		maxSurge := intstr.FromInt(1)
		maxUnavail := intstr.FromInt(0)
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineImage("fake-serving:v1"))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type: v1beta1.UpdateStrategySurgeThenDrain,
				RollingUpdate: &v1beta1.RollingUpdate{
					MaxSurge:       &maxSurge,
					MaxUnavailable: &maxUnavail,
				},
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var v1Hash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(3)))
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the bump")
			v1Hash = query.RevisionHashFromControllerRevisionName(engCS.Lifecycle.CurrentRevision)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("wedging instance " + wedgedIndex + " on the running revision")
		var wedgedPod string
		Eventually(func(g Gomega) {
			pod := singleEnginePodOfInstance(g, ns.Name, isvcName, wedgedIndex)
			pod.Spec.Containers[0].Image = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
			g.Expect(k8sClient.Update(suiteCtx, pod)).To(Succeed())
			wedgedPod = pod.Name
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: wedgedPod}, pod)).To(Succeed())
			g.Expect(podreadiness.IsContainersReady(pod)).To(BeFalse(),
				"the wedged pod must leave ContainersReady before the bump")
			g.Expect(terminalWaitingReasons(pod)).NotTo(BeEmpty(),
				"kubelet must report a terminal waiting reason on the wedged pod")
		}).WithTimeout(3 * time.Minute).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("bumping the engine image while the wedge stands")
		bumpedAt := time.Now()
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Every Instance, the wedged one included, must end serving on the
		// fixed revision, and nothing may still carry v1: the wedged pod is
		// drained away by its own Instance's surge, not rebuilt on v1.
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(servingEnginePodsOffRevision(snap, v1Hash)).To(Equal(3),
				"all three Instances must be serving on the fixed revision")
			g.Expect(snap.OnRevision(v1beta1.EngineComponent, v1Hash)).To(Equal(0),
				"no pod may still carry the broken revision")
			g.Expect(servingEnginePodOfInstance(snap, wedgedIndex, v1Hash)).To(BeTrue(),
				"the wedged Instance must be the one serving on the fixed revision, not a stray index")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("confirming no repair was recorded for the wedged Instance after the bump")
		Expect(restartsRecordedForInstance(ns.Name, isvcName, wedgedIndex, bumpedAt)).To(BeEmpty(),
			"an Instance off the roll target is the update pass's; a repair on the broken revision is the defect")
	})
})

// singleEnginePodOfInstance returns the one engine pod of an Instance
// index. Fails g (retryable inside Eventually) until exactly one exists.
func singleEnginePodOfInstance(g Gomega, namespace, isvcName, index string) *corev1.Pod {
	pods := &corev1.PodList{}
	g.Expect(k8sClient.List(suiteCtx, pods,
		client.InNamespace(namespace),
		client.MatchingLabels{
			util.LabelManagedByKey:                util.ManagedByOMENativeValue,
			constants.InferenceServicePodLabelKey: isvcName,
			constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
			query.LabelInstanceIdx:                index,
		})).To(Succeed())
	g.Expect(pods.Items).To(HaveLen(1), "instance %s must have exactly one engine pod", index)
	return &pods.Items[0]
}

// servingEnginePodsOffRevision counts the engine pods in rotation
// (ContainersReady plus the ome.io/serving gate) that carry a revision
// hash other than hash.
func servingEnginePodsOffRevision(snap *util.PodSnapshot, hash string) int {
	n := 0
	for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
		if pod.Labels[query.LabelRevisionHash] == hash {
			continue
		}
		if podreadiness.IsContainersReady(pod) && podreadiness.IsServing(pod) {
			n++
		}
	}
	return n
}

// servingEnginePodOfInstance reports whether the Instance at index has a
// serving engine pod off the named revision.
func servingEnginePodOfInstance(snap *util.PodSnapshot, index, hash string) bool {
	for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
		if pod.Labels[query.LabelInstanceIdx] != index || pod.Labels[query.LabelRevisionHash] == hash {
			continue
		}
		if podreadiness.IsContainersReady(pod) && podreadiness.IsServing(pod) {
			return true
		}
	}
	return false
}

// terminalWaitingReasons lists the terminal kubelet waiting reasons across
// a pod's containers — the evidence the crash-loop repair trigger reads.
func terminalWaitingReasons(pod *corev1.Pod) []string {
	var out []string
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for _, cs := range statuses {
			if cs.State.Waiting != nil && workloadtypes.IsTerminalWaitingReason(cs.State.Waiting.Reason) {
				out = append(out, cs.State.Waiting.Reason)
			}
		}
	}
	return out
}

// restartsRecordedForInstance returns the RestartTriggered event messages
// naming the engine Instance at index whose latest occurrence is at or
// after since.
func restartsRecordedForInstance(namespace, isvcName, index string, since time.Time) []string {
	events := &corev1.EventList{}
	Expect(k8sClient.List(suiteCtx, events, client.InNamespace(namespace))).To(Succeed())
	key := workloadtypes.InstanceKey(workloadtypes.ComponentEngine, mustAtoi(index))
	var out []string
	for _, ev := range events.Items {
		if ev.InvolvedObject.Name != isvcName || ev.Reason != string(workloadtypes.EventReasonRestartTriggered) {
			continue
		}
		when := ev.LastTimestamp.Time
		if when.IsZero() {
			when = ev.EventTime.Time
		}
		if when.IsZero() {
			when = ev.CreationTimestamp.Time
		}
		if when.Before(since) || !strings.Contains(ev.Message, key) {
			continue
		}
		out = append(out, ev.Message)
	}
	return out
}

func mustAtoi(s string) int32 {
	var n int32
	for _, r := range s {
		n = n*10 + int32(r-'0')
	}
	return n
}
