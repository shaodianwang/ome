package omenative_coordination_kind

import (
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

// A gang whose leader is wedged on the old revision is rolled to the fix
// under a coordination group too.
//
// A gang serves through its leader: with the leader wedged the Instance is
// already out of the coordination gate's serving count, whatever its
// workers report. The roll's drain-first start on that gang takes nothing
// further offline, so it must open without the gate's consult; consulting
// it would count the gang's own outage a second time and, under an
// unavailability budget of one, deny the only start that can end the
// outage on every pass, holding the Component with the healthy gang rolled
// and the wedged one on the broken revision.
//
// The shape: two leader+worker engine gangs under RecreatePod with
// maxUnavailable=1, grouped with a decoder under RollingUpdate with the
// same per-Component budget. The healthy gang rolls first, so the wedged
// one is reached with the roll's own budget free and the gate as the only
// layer left to answer to. The wedge is a live leader pointed at an image
// the cluster cannot pull, the same terminal waiting class as a crash at
// start, because a running pod's image is its only mutable field.
//
// Cells: instance/Ready/pod.waiting[ImagePullBackOff]/boundary@T-ready-recreate, instance/Ready/spec.revision/boundary@T-ready-crashloop-restart
var _ = ginkgo.Describe("behavior — a gang with a wedged leader is rolled under a coordination group", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "crashgang-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("the wedged gang ends serving on the fixed revision with no repair recorded for it", func() {
		const isvcName = "crashgang"
		const wedgedIndex = "1"
		const engineReplicas = 2
		const podsPerGang = 2
		configureLifecycleConfig(offTargetRollLifecycleJSON)

		maxUnavail := intstr.FromInt(1)
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
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type:          v1beta1.UpdateStrategyRecreatePod,
				RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: &maxUnavail},
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var v1Hash, v1EngineRev, v1Target string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(engineReplicas)))
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the bump")
			v1EngineRev = engCS.Lifecycle.CurrentRevision
			v1Hash = query.RevisionHashFromControllerRevisionName(v1EngineRev)
			v1Target = engineTargetRevision(g, ns.Name, isvcName)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("wedging the leader of gang " + wedgedIndex + " on the running revision")
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
		bumpSeenAt := awaitEngineTargetMoved(ns.Name, isvcName, v1Target)

		// Both gangs, the wedged one included, end serving on the fixed
		// revision at their own index, and nothing still carries v1: the
		// wedged gang is rebuilt by its own Instance's roll, not repaired on
		// the broken revision and not left behind the gate.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(engineReplicas)),
				"both gangs must be serving again")
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(servingEnginePodsOffRevision(snap, v1Hash)).To(Equal(engineReplicas*podsPerGang),
				"every member of both gangs must be serving on the fixed revision")
			g.Expect(snap.OnRevision(v1beta1.EngineComponent, v1Hash)).To(Equal(0),
				"no pod may still carry the broken revision")
			g.Expect(servingEnginePodOfInstance(snap, wedgedIndex, v1Hash)).To(BeTrue(),
				"the wedged gang must be the one serving on the fixed revision at its own index")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("confirming no repair was recorded for the wedged gang after the bump")
		Expect(restartsRecordedForInstance(ns.Name, isvcName, wedgedIndex, bumpSeenAt)).To(BeEmpty(),
			"an Instance off the roll target is the update pass's; a repair on the broken revision is the defect")
	})
})

// A gang whose worker is wedged beside a serving leader is rolled to the
// fix under a coordination group too, and first.
//
// The coordination gate counts a gang as serving only when every member
// is, so a gang with its worker parked is already inside the gate's
// unavailability while its leader still answers traffic. Under an
// unavailability budget of one the gate denies the healthy gang's start
// for as long as the broken gang is out of the count, and the broken
// gang's own start must not be put to the gate for the outage it already
// is: consulting it would deny the only start that can end the outage,
// and with the worker parked for good nothing would ever move.
//
// The shape: two leader+worker engine gangs under RecreatePod with
// maxUnavailable=1 and restart policy None, grouped with a decoder under
// RollingUpdate with the same per-Component budget. The worker of the
// higher index is pointed at an image the cluster cannot pull while its
// leader keeps serving, which the engine's ServingReplicas records as one
// gang short before the bump. The policy is None because the wedge is an
// image edit on a live pod: the kubelet kills the running container to
// start the new image, and under RecreateInstanceOnPodRestart that kill
// reads as a runner restart and rebuilds the gang on the template image
// before the bump, which undoes the wedge. Under None the worker stays
// parked for good, the shape the story is about. The first pods on the
// fixed revision belong to the broken gang while the healthy gang still
// serves the running revision; both gangs end serving on the fix at their
// own index and no repair is recorded for the broken gang.
//
// Cells: instance/Ready/pod.waiting[ImagePullBackOff]/boundary@T-ready-recreate, instance/Ready/spec.revision/boundary@T-ready-crashloop-restart
var _ = ginkgo.Describe("behavior — a gang with a wedged worker is rolled first under a coordination group", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "crashworker-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("the gang with the wedged worker lands the fixed revision before the healthy gang is touched", func() {
		const isvcName = "crashworker"
		const engineReplicas = 2
		const podsPerGang = 2
		const brokenIndex = "1"
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
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(engineReplicas)))
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the bump")
			v1Hash = query.RevisionHashFromControllerRevisionName(engCS.Lifecycle.CurrentRevision)
			v1Target = engineTargetRevision(g, ns.Name, isvcName)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("wedging the worker of gang " + brokenIndex + " on the running revision")
		var wedgedPod string
		Eventually(func(g Gomega) {
			pod := engineRunnerPodOfInstance(g, ns.Name, isvcName, brokenIndex, workloadtypes.RunnerWorker)
			pod.Spec.Containers[0].Image = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
			g.Expect(k8sClient.Update(suiteCtx, pod)).To(Succeed())
			wedgedPod = pod.Name
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The precondition is the self-count: the leader still serves, yet
		// the Component counts the gang as unavailable, which is what the
		// coordination gate reads.
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: wedgedPod}, pod)).To(Succeed())
			g.Expect(podreadiness.IsContainersReady(pod)).To(BeFalse(),
				"the wedged worker must leave ContainersReady before the bump")
			g.Expect(terminalWaitingReasons(pod)).NotTo(BeEmpty(),
				"kubelet must report a terminal waiting reason on the wedged worker")
			leader := engineRunnerPodOfInstance(g, ns.Name, isvcName, brokenIndex, workloadtypes.RunnerLeader)
			g.Expect(podreadiness.ReadyAndServing(leader)).To(BeTrue(),
				"the leader of the broken gang must keep serving beside its wedged worker")
			engCS := freshISVC(g, ns.Name, isvcName).Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(engineReplicas-1)),
				"a gang with a wedged worker must leave the Component's serving count")
		}).WithTimeout(3 * time.Minute).WithPolling(kindPollInterval).Should(Succeed())

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
		bumpSeenAt := awaitEngineTargetMoved(ns.Name, isvcName, v1Target)

		// The first pods on the fixed revision belong to the broken gang, and
		// at that moment the healthy gang still serves the running revision:
		// the gate held the healthy gang's start while the broken gang was
		// out of the serving count, and the broken gang's own start was not
		// put to the gate. Read in one snapshot so the two facts are of one
		// instant.
		ginkgo.By("the broken gang receives the first pods on the fixed revision")
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			fixed := enginePodsOffRevision(snap, v1Hash)
			g.Expect(fixed).NotTo(BeEmpty(), "the roll must open on the fixed revision")
			for _, pod := range fixed {
				g.Expect(pod.Labels[query.LabelInstanceIdx]).To(Equal(brokenIndex),
					"the first pods on the fixed revision must belong to the broken gang, got %s", pod.Name)
			}
			g.Expect(servingEnginePodOfInstanceOn(snap, healthyIndex, v1Hash)).To(BeTrue(),
				"the healthy gang must still serve the running revision while the broken gang is replaced")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("both gangs end serving on the fixed revision")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(engineReplicas)),
				"both gangs must be serving again")
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(servingEnginePodsOffRevision(snap, v1Hash)).To(Equal(engineReplicas*podsPerGang),
				"every member of both gangs must be serving on the fixed revision")
			g.Expect(snap.OnRevision(v1beta1.EngineComponent, v1Hash)).To(Equal(0),
				"no pod may still carry the running revision")
			g.Expect(servingEnginePodOfInstance(snap, brokenIndex, v1Hash)).To(BeTrue(),
				"the broken gang must be serving on the fixed revision under its own index")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("confirming no repair was recorded for the broken gang after the bump")
		Expect(restartsRecordedForInstance(ns.Name, isvcName, brokenIndex, bumpSeenAt)).To(BeEmpty(),
			"an Instance off the roll target is the update pass's; a repair on the running revision is the defect")
	})
})

// engineLeaderPodOfInstance returns the leader pod of the engine gang at
// index. Fails g (retryable inside Eventually) until exactly one exists.
func engineLeaderPodOfInstance(g Gomega, namespace, isvcName, index string) *corev1.Pod {
	return engineRunnerPodOfInstance(g, namespace, isvcName, index, workloadtypes.RunnerLeader)
}

// engineRunnerPodOfInstance returns the pod of the named runner in the
// engine gang at index. Fails g (retryable inside Eventually) until
// exactly one exists.
func engineRunnerPodOfInstance(g Gomega, namespace, isvcName, index, runner string) *corev1.Pod {
	pods := &corev1.PodList{}
	g.Expect(k8sClient.List(suiteCtx, pods,
		client.InNamespace(namespace),
		client.MatchingLabels{
			util.LabelManagedByKey:                util.ManagedByOMENativeValue,
			constants.InferenceServicePodLabelKey: isvcName,
			constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
			query.LabelInstanceIdx:                index,
			query.LabelRunner:                     runner,
		})).To(Succeed())
	g.Expect(pods.Items).To(HaveLen(1), "gang %s must have exactly one %s pod", index, runner)
	return &pods.Items[0]
}
