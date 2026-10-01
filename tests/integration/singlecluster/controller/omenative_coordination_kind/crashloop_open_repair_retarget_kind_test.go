package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A fix pushed while a repair is open lands on the rebuild.
//
// An Instance wedged on the running revision has its repair opened: its pod
// is deleted and the rebuild waits for the name to clear. The operator
// pushes the fix in that window. The rebuild renders the pushed revision
// rather than the broken one, so the Instance comes back once, on the fix,
// and no pod of the broken revision is created once the engine replica
// carries the new target. The shape is two engine Instances under
// SurgeThenDrain with maxSurge=1 / maxUnavailable=0; the sibling is rolled
// by the ordinary surge.
//
// The window is held open by a finalizer on the wedged pod: the repair's
// delete leaves it Terminating, and the rebuild cannot create a pod under
// the same name until it is gone. The spec releases the finalizer once the
// engine replica carries the new target, so the create step runs after the
// push by construction rather than by timing.
//
// The wedge is a live pod pointed at an image the cluster cannot pull: a
// running pod's image is its only mutable field, and the fake serving image
// has no per-pod crash switch. ImagePullBackOff is a terminal kubelet
// waiting reason of the same class as a crash at start, and the crash-loop
// repair reads both the same way.
//
// A seconds-scale stuck-pod grace so the repair opens within the suite
// budget. Nothing else about the shape depends on its value.
const openRepairRetargetLifecycleJSON = `{"stuckPodGracePeriod":"10s"}`

// holdTerminationFinalizer keeps the wedged pod Terminating until the spec
// releases it.
const holdTerminationFinalizer = "test.ome.io/hold-termination"

// Cells: instance/Restart/Drain/spec.revision/before, instance/Ready/spec.revision/after@T-ready-crashloop-restart
var _ = ginkgo.Describe("behavior — a fix pushed while a repair is open lands on the rebuild", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "openrepair-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("the wedged Instance comes back once on the fixed revision and no pod of the broken revision is created after the push", func() {
		const isvcName = "openrepair"
		const wedgedIndex = "0"
		configureLifecycleConfig(openRepairRetargetLifecycleJSON)

		maxSurge := intstr.FromInt(1)
		maxUnavail := intstr.FromInt(0)
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
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

		var v1Hash, v1Target string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the wedge")
			v1Hash = query.RevisionHashFromControllerRevisionName(engCS.Lifecycle.CurrentRevision)
			v1Target = engineTargetRevision(g, ns.Name, isvcName)
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

		// The hold is released on every exit path: a pod kept Terminating
		// would block the namespace's deletion.
		var wedgedPod string
		releaseHold := func() {
			if wedgedPod == "" {
				return
			}
			Eventually(func(g Gomega) {
				pod := &corev1.Pod{}
				err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: wedgedPod}, pod)
				if apierrors.IsNotFound(err) {
					return
				}
				g.Expect(err).NotTo(HaveOccurred())
				if !controllerutil.RemoveFinalizer(pod, holdTerminationFinalizer) {
					return
				}
				g.Expect(k8sClient.Update(suiteCtx, pod)).To(Succeed())
			}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}
		ginkgo.DeferCleanup(releaseHold)

		ginkgo.By("wedging instance " + wedgedIndex + " on the running revision, with its termination held")
		wedgedAt := time.Now()
		Eventually(func(g Gomega) {
			pod := singleEnginePodOfInstance(g, ns.Name, isvcName, wedgedIndex)
			pod.Spec.Containers[0].Image = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
			controllerutil.AddFinalizer(pod, holdTerminationFinalizer)
			g.Expect(k8sClient.Update(suiteCtx, pod)).To(Succeed())
			wedgedPod = pod.Name
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the repair to open and delete the wedged pod")
		Eventually(func(g Gomega) {
			g.Expect(restartsRecordedForInstance(ns.Name, isvcName, wedgedIndex, wedgedAt)).NotTo(BeEmpty(),
				"the crash-loop repair must open on the wedged Instance")
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: wedgedPod}, pod)).To(Succeed())
			g.Expect(pod.DeletionTimestamp).NotTo(BeNil(), "the repair must have deleted the wedged pod")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("pushing the fix while the rebuild waits for the old pod to clear")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		pushSeenAt := awaitEngineTargetMoved(ns.Name, isvcName, v1Target)
		v2Target := engineTargetRevision(Default, ns.Name, isvcName)

		// Every engine pod that exists now was created before the push; a
		// pod on the broken revision under any other UID is one created
		// after it.
		knownUIDs := enginePodUIDs(ns.Name, isvcName)
		stop := util.StartSampler(suiteCtx, 500*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
				if pod.Labels[query.LabelRevisionHash] != v1Hash {
					continue
				}
				_, known := knownUIDs[pod.UID]
				g.Expect(known).To(BeTrue(),
					"pod %s was created on the broken revision after the fix was pushed", pod.Name)
			}
		})
		defer stop()

		ginkgo.By("releasing the held pod so the rebuild's create step runs after the push")
		releaseHold()

		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(2))
			for _, inst := range insts {
				g.Expect(inst.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(inst.RunningRevision).To(Equal(v2Target),
					"every Instance must record the fixed revision, the rebuilt one included")
				if inst.Index == mustAtoi(wedgedIndex) {
					g.Expect(inst.Incarnation).To(Equal(wedgedIncarnation+1),
						"the wedged Instance is rebuilt exactly once")
				}
			}
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(servingEnginePodsOffRevision(snap, v1Hash)).To(Equal(2),
				"both Instances must be serving on the fixed revision")
			g.Expect(snap.OnRevision(v1beta1.EngineComponent, v1Hash)).To(Equal(0),
				"no pod may still carry the broken revision")
			g.Expect(servingEnginePodOfInstance(snap, wedgedIndex, v1Hash)).To(BeTrue(),
				"the wedged Instance must be the one serving on the fixed revision")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(stop()).To(BeNumerically(">", 0), "the invariant must have been sampled while the Instance was rebuilt")

		Expect(restartsRecordedForInstance(ns.Name, isvcName, wedgedIndex, pushSeenAt)).To(BeEmpty(),
			"the repair that was open is the only one; nothing re-opens on the rebuilt Instance after the push")
	})
})

// enginePodUIDs returns the UIDs of every engine pod of the ISVC,
// Terminating ones included.
func enginePodUIDs(namespace, isvcName string) map[types.UID]struct{} {
	snap, err := util.SnapshotPods(suiteCtx, k8sClient, namespace, isvcName)
	Expect(err).NotTo(HaveOccurred())
	out := map[types.UID]struct{}{}
	for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
		out[pod.UID] = struct{}{}
	}
	return out
}
