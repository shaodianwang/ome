package omenative_coordination_kind

import (
	"strings"
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

// A crash loop at steady state is repaired under a rollout group.
//
// A Component listed in spec.rollout.groups has no run open once it has
// converged, and the group's gate stack holds every consult until a run
// pins a plan. A repair of an Instance whose pod set serves nothing
// removes no serving capacity, so it is admitted by neither the
// per-Component unavailability budget nor that gate: the Instance is
// rebuilt on its running revision while its sibling never leaves
// rotation. The shape is a blueGreen engine with two Instances under
// maxUnavailable=0 — the lifecycle under which no serving pod may ever be
// taken offline — and one Instance wedged after both are Ready.
//
// The wedge is a live pod pointed at an image the cluster cannot pull: a
// running pod's image is its only mutable field, and the fake serving image
// has no per-pod crash switch. ImagePullBackOff is a terminal kubelet
// waiting reason of the same class as a crash at start, and the crash-loop
// repair reads both the same way.
//
// A seconds-scale stuck-pod grace so the wedge is classified within the
// suite budget. Nothing else about the shape depends on its value.
const groupedRepairLifecycleJSON = `{"stuckPodGracePeriod":"10s"}`

// Cells: instance/Ready/pod.waiting[ImagePullBackOff]/before
var _ = ginkgo.Describe("behavior — a grouped Component repairs a crash loop at steady state", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "grouprepair-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("the wedged Instance is rebuilt under a zero budget and its sibling stays in rotation", func() {
		const isvcName = "grouprepair"
		const wedgedIndex = "0"
		const siblingIndex = "1"
		configureLifecycleConfig(groupedRepairLifecycleJSON)

		maxUnavail := intstr.FromInt(0)
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithBlueGreenGroup(v1beta1.EngineComponent),
			WithEngineImage("fake-serving:v1"))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type:          v1beta1.UpdateStrategyRecreatePod,
				RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: &maxUnavail},
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the wedge")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		var wedgedIncarnation int64
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(2))
			for _, inst := range insts {
				g.Expect(inst.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				if inst.Index == mustAtoi(wedgedIndex) {
					wedgedIncarnation = inst.Incarnation
				}
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		sibling := singleEnginePodOfInstance(Default, ns.Name, isvcName, siblingIndex)
		siblingUID := sibling.UID
		Expect(podreadiness.IsServing(sibling)).To(BeTrue(), "the sibling must be in rotation before the wedge")

		// The invariant a repair that removes no serving capacity keeps:
		// the sibling keeps serving under the same pod, nothing surges
		// above two pods, and the Component never loses both Instances.
		stop := util.StartSampler(suiteCtx, 500*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(snap.Alive(v1beta1.EngineComponent)).To(BeNumerically("<=", 2),
				"a repair rebuilds in place and never surges")
			g.Expect(snap.Serving(v1beta1.EngineComponent)).To(BeNumerically(">=", 1),
				"the budget keeps at least one Instance in rotation")
			g.Expect(servingSiblingPod(snap, siblingIndex, siblingUID)).To(BeTrue(),
				"the sibling must stay in rotation under the same pod")
		})
		defer stop()

		ginkgo.By("wedging instance " + wedgedIndex + " on the running revision")
		wedgedAt := time.Now()
		var wedgedPodUID types.UID
		Eventually(func(g Gomega) {
			pod := singleEnginePodOfInstance(g, ns.Name, isvcName, wedgedIndex)
			pod.Spec.Containers[0].Image = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
			g.Expect(k8sClient.Update(suiteCtx, pod)).To(Succeed())
			wedgedPodUID = pod.UID
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the repair to rebuild it Ready on its running revision")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(2))
			for _, inst := range insts {
				g.Expect(inst.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				if inst.Index == mustAtoi(wedgedIndex) {
					g.Expect(inst.Incarnation).To(BeNumerically(">", wedgedIncarnation),
						"the wedged Instance must be rebuilt at a new incarnation")
				}
			}
			// A rebuild keeps the pod's name (index, runner, ordinal) and
			// changes its UID and incarnation label.
			pod := singleEnginePodOfInstance(g, ns.Name, isvcName, wedgedIndex)
			g.Expect(pod.UID).NotTo(Equal(wedgedPodUID), "the wedged pod must be replaced, not patched")
			g.Expect(pod.Spec.Containers[0].Image).To(Equal("fake-serving:v1"),
				"the rebuild renders the running revision's template")
			g.Expect(podreadiness.IsContainersReady(pod) && podreadiness.IsServing(pod)).To(BeTrue(),
				"the rebuilt Instance must be back in rotation")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(stop()).To(BeNumerically(">", 0), "the invariant must have been sampled during the repair")

		Expect(restartsRecordedForInstance(ns.Name, isvcName, wedgedIndex, wedgedAt)).NotTo(BeEmpty(),
			"the repair must be recorded on the wedged Instance")
		Expect(eventsRecorded(ns.Name, isvcName, string(workloadtypes.EventReasonRepairHeld), wedgedAt)).To(BeEmpty(),
			"a repair that removes no serving capacity is owed no hold")

		sibling = singleEnginePodOfInstance(Default, ns.Name, isvcName, siblingIndex)
		Expect(sibling.UID).To(Equal(siblingUID), "the sibling must keep its pod")
		Expect(podreadiness.IsServing(sibling)).To(BeTrue(), "the sibling must still be in rotation")
	})
})

// servingSiblingPod reports whether the engine pod of the Instance at index
// is the pod with uid and is in rotation.
func servingSiblingPod(snap *util.PodSnapshot, index string, uid types.UID) bool {
	for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
		if pod.Labels[query.LabelInstanceIdx] != index {
			continue
		}
		return pod.UID == uid && podreadiness.IsContainersReady(pod) && podreadiness.IsServing(pod)
	}
	return false
}

// eventsRecorded returns the messages of the events with reason on the
// ISVC whose latest occurrence is at or after since.
func eventsRecorded(namespace, isvcName, reason string, since time.Time) []string {
	events := &corev1.EventList{}
	Expect(k8sClient.List(suiteCtx, events, client.InNamespace(namespace))).To(Succeed())
	var out []string
	for _, ev := range events.Items {
		if ev.InvolvedObject.Name != isvcName || ev.Reason != reason {
			continue
		}
		when := ev.LastTimestamp.Time
		if when.IsZero() {
			when = ev.EventTime.Time
		}
		if when.IsZero() {
			when = ev.CreationTimestamp.Time
		}
		if when.Before(since) {
			continue
		}
		out = append(out, strings.TrimSpace(ev.Message))
	}
	return out
}
