package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A scale-up that lands while a repair is open adds exactly one Instance.
//
// Four engine Instances serve on one revision. One is wedged on the running
// revision and its repair opens: the row reads Restarting and the wedged
// pod is deleted. The operator scales from four to five in that window,
// which is where a scale lands in the field when the operator acts on a
// row that reads Restarting. The repair keeps its index and rebuilds it
// once the old pod's name clears; the create pass adds index 4 and no
// other; the healthy Instances keep their pods; the Component ends with
// five Instances Ready on the running revision, and the repair is the only
// restart recorded.
//
// The window is held open by a finalizer on the wedged pod, so the scale
// lands between the old pod and the rebuilt one by construction rather than
// by timing. The wedge is a live pod pointed at an image the cluster cannot
// pull: a running pod's image is its only mutable field, and
// ImagePullBackOff is a terminal kubelet waiting reason of the same class
// as a crash at start.
//
// A seconds-scale stuck-pod grace so the repair opens within the suite
// budget. Nothing else about the shape depends on its value.
const scaleBesideRepairLifecycleJSON = `{"stuckPodGracePeriod":"10s"}`

// Cells: instance/Restart/Drain/spec.replicasUp/before
var _ = ginkgo.Describe("behavior — a scale-up beside an open repair adds exactly one Instance", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "scalerepair-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("index 4 is the only new Instance and the wedged Instance is rebuilt once under its own index", func() {
		const isvcName = "scalerepair"
		const wedgedIndex = "0"
		configureLifecycleConfig(scaleBesideRepairLifecycleJSON)

		none := v1beta1.InstanceRestartPolicyNone
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(4),
			WithEngineImage("fake-serving:v1"))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{RestartPolicy: &none}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var v1Hash, v1Target string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(4)))
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the wedge")
			v1Hash = query.RevisionHashFromControllerRevisionName(engCS.Lifecycle.CurrentRevision)
			v1Target = engineTargetRevision(g, ns.Name, isvcName)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		var wedgedIncarnation int64
		healthyPods := map[string]types.UID{}
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(4))
			for _, inst := range insts {
				g.Expect(inst.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				if inst.Index == mustAtoi(wedgedIndex) {
					wedgedIncarnation = inst.Incarnation
				}
			}
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
				if pod.Labels[query.LabelInstanceIdx] != wedgedIndex {
					healthyPods[pod.Name] = pod.UID
				}
			}
			g.Expect(healthyPods).To(HaveLen(3))
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

		ginkgo.By("scaling the engine from four to five while the rebuild waits for the old pod to clear")
		scaledAt := time.Now()
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			five := 5
			cur.Spec.Engine.MinReplicas = &five
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// A second new Instance beside the repair is what the scale must not
		// produce: no pod carries an index above 4 and the engine replica
		// never holds more than five rows, at any sample.
		stop := util.StartSampler(suiteCtx, 500*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
				idx, ok := query.InstanceIdxFromLabels(pod)
				g.Expect(ok).To(BeTrue(), "pod %s carries no instance index", pod.Name)
				g.Expect(idx).To(BeNumerically("<=", 4),
					"pod %s carries index %d: a second new Instance was created beside the repair", pod.Name, idx)
			}
			g.Expect(len(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent))).To(BeNumerically("<=", 5),
				"more rows than the five Instances asked for")
		})
		defer stop()

		ginkgo.By("releasing the held pod so the rebuild's create step runs after the scale")
		releaseHold()

		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(5))
			indices := map[int32]bool{}
			for _, inst := range insts {
				indices[inst.Index] = true
				g.Expect(inst.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(inst.RunningRevision).To(Equal(v1Target),
					"every Instance must record the running revision, the rebuilt and the new one included")
				if inst.Index == mustAtoi(wedgedIndex) {
					g.Expect(inst.Incarnation).To(Equal(wedgedIncarnation+1),
						"the wedged Instance is rebuilt exactly once under its own index")
				}
			}
			g.Expect(indices).To(Equal(map[int32]bool{0: true, 1: true, 2: true, 3: true, 4: true}),
				"index 4 is the only new Instance")
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(snap.Serving(v1beta1.EngineComponent)).To(Equal(5), "five Instances must be serving")
			g.Expect(snap.OnRevision(v1beta1.EngineComponent, v1Hash)).To(Equal(5),
				"every pod must carry the running revision")
			for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
				if uid, healthy := healthyPods[pod.Name]; healthy {
					g.Expect(pod.UID).To(Equal(uid), "healthy Instance pod %s must be the pod it had before the scale", pod.Name)
				}
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(stop()).To(BeNumerically(">", 0), "the invariant must have been sampled while the Instances came up")

		Expect(restartsRecordedForInstance(ns.Name, isvcName, "4", scaledAt)).To(BeEmpty(),
			"the new Instance is created, not repaired")
	})
})
