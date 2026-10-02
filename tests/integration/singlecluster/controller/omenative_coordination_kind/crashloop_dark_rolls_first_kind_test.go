package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A fix pushed over a wedged Instance lands on that Instance first.
//
// Four engine Instances under SurgeThenDrain with maxSurge=1 /
// maxUnavailable=0, the highest index wedged on the running revision
// before the bump: its pod set serves nothing, so replacing it costs no
// capacity, and the roll offers its one surge slot to it before any
// healthy peer. The first pod on the fixed revision is the wedged
// Instance's while the three healthy Instances still serve the running
// revision; every Instance ends serving on the fix, nothing still carries
// the running revision, and no repair is recorded for the wedged Instance.
//
// The wedge and the stuck-pod grace are those of the off-target roll spec
// (offTargetRollLifecycleJSON): a pull that cannot succeed parks the pod in
// a terminal waiting reason, and the long grace keeps the repair from
// rebuilding the pod on the running revision before the bump lands.

// Cells: instance/Ready/pod.waiting[ImagePullBackOff]/boundary@T-ready-surge, instance/Ready/spec.revision/boundary@T-ready-crashloop-restart
var _ = ginkgo.Describe("behavior — a wedged Instance is the first to receive the fix", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "darkfirst-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("the wedged Instance lands the fixed revision before any healthy peer is touched", func() {
		const isvcName = "darkfirst"
		const replicas = 4
		const wedgedIndex = "3"
		healthyIndices := []string{"0", "1", "2"}
		configureLifecycleConfig(offTargetRollLifecycleJSON)

		maxSurge := intstr.FromInt(1)
		maxUnavail := intstr.FromInt(0)
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(replicas),
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
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(replicas)))
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

		// The first pod on the fixed revision is the wedged Instance's, and
		// at that moment every healthy Instance still serves the running
		// revision: the one surge slot went to the Instance that served
		// nothing. Read in one snapshot so the two facts are of one instant.
		ginkgo.By("the wedged Instance receives the first pod on the fixed revision")
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			fixed := enginePodsOffRevision(snap, v1Hash)
			g.Expect(fixed).NotTo(BeEmpty(), "the roll must open on the fixed revision")
			for _, pod := range fixed {
				g.Expect(pod.Labels[query.LabelInstanceIdx]).To(Equal(wedgedIndex),
					"the first pod on the fixed revision must belong to the wedged Instance, got %s", pod.Name)
			}
			for _, idx := range healthyIndices {
				g.Expect(servingEnginePodOfInstanceOn(snap, idx, v1Hash)).To(BeTrue(),
					"healthy instance %s must still serve the running revision while the wedged Instance is replaced", idx)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("every Instance ends serving on the fixed revision")
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(servingEnginePodsOffRevision(snap, v1Hash)).To(Equal(replicas),
				"all four Instances must be serving on the fixed revision")
			g.Expect(snap.OnRevision(v1beta1.EngineComponent, v1Hash)).To(Equal(0),
				"no pod may still carry the running revision")
			g.Expect(servingEnginePodOfInstance(snap, wedgedIndex, v1Hash)).To(BeTrue(),
				"the wedged Instance must be serving on the fixed revision under its own index")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("confirming no repair was recorded for the wedged Instance after the bump")
		Expect(restartsRecordedForInstance(ns.Name, isvcName, wedgedIndex, bumpedAt)).To(BeEmpty(),
			"an Instance off the roll target is the update pass's; a repair on the running revision is the defect")
	})
})

// A gang serves through its leader: with the leader wedged the gang serves
// nothing whatever its worker reports, and a fix pushed over it lands on
// that gang first.
//
// Two leader+worker engine gangs under RecreatePod with maxUnavailable=1,
// grouped with a decoder under RollingUpdate with the same per-Component
// budget, the leader of the higher index wedged on the running revision
// before the bump: the first pods on the fixed revision are the wedged
// gang's while the healthy gang still serves the running revision, both
// gangs end serving on the fix at their own index, nothing still carries
// the running revision, and no repair is recorded for the wedged gang
// from the wedge on.
//
// The engine's restart policy is None so that the wedge is the roll's
// alone to answer. The kubelet applies the wedge image by restarting the
// leader's container in place, and under the gang default,
// RecreateInstanceOnPodRestart, that restart is a runner restart after
// Ready and opens a repair of the gang; whether the repair then rebuilds
// the gang on the running revision or on the fix turns on whether the bump
// has reached the engine replica by the time its create step runs, which
// is a race against the manager's load, not the order the roll takes. The
// crash-loop repair stays off through the stuck-pod grace of the
// off-target roll spec.

// Cells: instance/Ready/pod.waiting[ImagePullBackOff]/boundary@T-ready-recreate, instance/Ready/spec.revision/boundary@T-ready-crashloop-restart
var _ = ginkgo.Describe("behavior — a gang with a wedged leader is the first to receive the fix", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "darkgang-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("the wedged gang lands the fixed revision before the healthy gang is touched", func() {
		const isvcName = "darkgang"
		const engineReplicas = 2
		const podsPerGang = 2
		const wedgedIndex = "1"
		const healthyIndex = "0"
		configureLifecycleConfig(offTargetRollLifecycleJSON)

		maxUnavail := intstr.FromInt(1)
		restartNone := v1beta1.InstanceRestartPolicyNone
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithEngineReplicas(engineReplicas),
			WithEngineWorkerSize(podsPerGang-1),
			WithEngineImage("fake-serving:v1"),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingPerComponent,
					MaxUnavailable: &maxUnavail,
				},
			}))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			RestartPolicy: &restartNone,
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type:          v1beta1.UpdateStrategyRecreatePod,
				RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: &maxUnavail},
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var v1Hash, v1Target string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(engineReplicas)))
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the bump")
			v1Hash = query.RevisionHashFromControllerRevisionName(engCS.Lifecycle.CurrentRevision)
			v1Target = engineTargetRevision(g, ns.Name, isvcName)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("wedging the leader of gang " + wedgedIndex + " on the running revision")
		wedgedAt := time.Now()
		var wedgedPod string
		Eventually(func(g Gomega) {
			pod := engineLeaderPodOfInstance(g, ns.Name, isvcName, wedgedIndex)
			pod.Spec.Containers[0].Image = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
			g.Expect(k8sClient.Update(suiteCtx, pod)).To(Succeed())
			wedgedPod = pod.Name
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: wedgedPod}, pod)).To(Succeed())
			g.Expect(podreadiness.IsContainersReady(pod)).To(BeFalse(),
				"the wedged leader must leave ContainersReady before the bump")
			g.Expect(terminalWaitingReasons(pod)).NotTo(BeEmpty(),
				"kubelet must report a terminal waiting reason on the wedged leader")
		}).WithTimeout(3 * time.Minute).WithPolling(kindPollInterval).Should(Succeed())
		Expect(restartsRecordedForInstance(ns.Name, isvcName, wedgedIndex, wedgedAt)).To(BeEmpty(),
			"the wedge must stand unrepaired when the bump lands: under RestartPolicy None the leader's in-place container restart opens no repair")

		ginkgo.By("bumping the engine image while the wedge stands")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			// A gang renders the leader and the worker templates; both move
			// so the revision does.
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		ginkgo.By("waiting for the bump to reach the engine replica")
		awaitEngineTargetMoved(ns.Name, isvcName, v1Target)

		// The first pods on the fixed revision belong to the wedged gang, and
		// at that moment the healthy gang still serves the running revision:
		// the one slot the budget offers went to the gang that served
		// nothing. Read in one snapshot so the two facts are of one instant.
		ginkgo.By("the wedged gang receives the first pods on the fixed revision")
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			fixed := enginePodsOffRevision(snap, v1Hash)
			g.Expect(fixed).NotTo(BeEmpty(), "the roll must open on the fixed revision")
			for _, pod := range fixed {
				g.Expect(pod.Labels[query.LabelInstanceIdx]).To(Equal(wedgedIndex),
					"the first pods on the fixed revision must belong to the wedged gang, got %s", pod.Name)
			}
			g.Expect(servingEnginePodOfInstanceOn(snap, healthyIndex, v1Hash)).To(BeTrue(),
				"the healthy gang must still serve the running revision while the wedged gang is replaced")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("both gangs end serving on the fixed revision")
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(servingEnginePodsOffRevision(snap, v1Hash)).To(Equal(engineReplicas*podsPerGang),
				"every member of both gangs must be serving on the fixed revision")
			g.Expect(snap.OnRevision(v1beta1.EngineComponent, v1Hash)).To(Equal(0),
				"no pod may still carry the running revision")
			g.Expect(servingEnginePodOfInstance(snap, wedgedIndex, v1Hash)).To(BeTrue(),
				"the wedged gang must be serving on the fixed revision under its own index")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("confirming no repair was recorded for the wedged gang from the wedge on")
		Expect(restartsRecordedForInstance(ns.Name, isvcName, wedgedIndex, wedgedAt)).To(BeEmpty(),
			"the wedged gang is the roll's: a repair, on the running revision or on the fix, is the defect")
	})
})

// enginePodsOffRevision lists the engine pods, Terminating or not, whose
// revision hash is not hash.
func enginePodsOffRevision(snap *util.PodSnapshot, hash string) []*corev1.Pod {
	var out []*corev1.Pod
	for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
		if pod.Labels[query.LabelRevisionHash] != hash {
			out = append(out, pod)
		}
	}
	return out
}

// servingEnginePodOfInstanceOn reports whether the Instance at index has a
// serving engine pod on the named revision.
func servingEnginePodOfInstanceOn(snap *util.PodSnapshot, index, hash string) bool {
	for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
		if pod.Labels[query.LabelInstanceIdx] != index || pod.Labels[query.LabelRevisionHash] != hash {
			continue
		}
		if podreadiness.IsContainersReady(pod) && podreadiness.IsServing(pod) {
			return true
		}
	}
	return false
}
