package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A stuck-pod grace shorter than the steady-state window: the wedge below
// is one the crash-loop repair would act on within the freeze, so the
// freeze is all that withholds it.
const liftedFreezeLifecycleJSON = `{"stuckPodGracePeriod":"10s"}`

// Pause depths. ome.io/rollout-paused: "true" suspends fleet changes
// (Update / Create / Migration) while the RestartPolicy keeps repairing
// existing Instances at the revision they already run; "freeze"
// additionally suspends repair. These specs pin both depths on a gang,
// including the property that repair under pause can never advance a
// pending rollout.
var _ = ginkgo.Describe("behavior — pause keeps gang repair running", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "pause-repair-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	setPauseAnnotation := func(name, value string) {
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, name)
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			if value == "" {
				delete(cur.Annotations, constants.PausedRolloutAnnotation)
			} else {
				cur.Annotations[constants.PausedRolloutAnnotation] = value
			}
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	}

	// livePodHashes returns the revision-hash label counts of live pods.
	livePodHashes := func(g Gomega, isvcName string) map[string]int {
		out := map[string]int{}
		for _, p := range listOMENativePods(g, ns.Name, isvcName) {
			if p.DeletionTimestamp == nil {
				out[p.Labels[query.LabelRevisionHash]]++
			}
		}
		return out
	}

	ginkgo.It("repairs a Ready gang while paused, at the running revision, never the pending one", func() {
		const name = "pause-repair"
		fix := newCoordFixture(ns.Name, WithISVCName(name),
			WithoutDecoder(),
			WithEngineWorkerSize(1),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var readyIncarnation int64
		var v1Hash string
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			readyIncarnation = insts[0].Incarnation
			hashes := livePodHashes(g, name)
			g.Expect(hashes).To(HaveLen(1), "a converged gang runs exactly one revision")
			for h, n := range hashes {
				g.Expect(n).To(Equal(2))
				v1Hash = h
			}
			g.Expect(v1Hash).NotTo(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("pausing, then minting a pending v2 revision under the pause")
		setPauseAnnotation(name, "true")
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Paused).To(BeTrue())
			g.Expect(ir.Spec.PauseMode).To(BeEmpty(), "plain pause must not project the freeze depth")
			g.Expect(ir.Status.ObservedGeneration).To(Equal(ir.Generation))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, name)
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("losing a gang member while paused")
		_, worker := gangPods(Default, ns.Name, name)
		deleteGangPod(ns.Name, worker)

		ginkgo.By("the RestartPolicy repairs the whole gang under the pause")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Incarnation).To(BeNumerically(">", readyIncarnation),
				"a paused Ready gang that loses a member must still restart as a unit")
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(livePodHashes(g, name)).To(Equal(map[string]int{v1Hash: 2}),
				"repair must recreate at the running revision, not the pending v2 target")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the pending rollout stays parked while paused")
		Consistently(func(g Gomega) {
			g.Expect(livePodHashes(g, name)).To(Equal(map[string]int{v1Hash: 2}),
				"no pod may reach the v2 revision while paused")
			ir := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Paused).To(BeTrue())
		}, kindSteadyStateWindow, kindPollInterval).Should(Succeed())

		ginkgo.By("unpausing resumes the rollout to v2")
		setPauseAnnotation(name, "")
		Eventually(func(g Gomega) {
			hashes := livePodHashes(g, name)
			g.Expect(hashes).To(HaveLen(1))
			g.Expect(hashes).NotTo(HaveKey(v1Hash), "unpause must let the fleet converge to the v2 revision")
			for _, n := range hashes {
				g.Expect(n).To(Equal(2))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("freeze suspends repair; downgrading to plain pause resumes it without unpausing", func() {
		const name = "pause-freeze"
		fix := newCoordFixture(ns.Name, WithISVCName(name),
			WithoutDecoder(),
			WithEngineWorkerSize(1),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var readyIncarnation int64
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			readyIncarnation = insts[0].Incarnation
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		setPauseAnnotation(name, constants.PausedRolloutFreezeValue)
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Paused).To(BeTrue())
			g.Expect(ir.Spec.PauseMode).To(Equal(v1beta1.PauseModeFreeze))
			g.Expect(ir.Status.ObservedGeneration).To(Equal(ir.Generation))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("losing a gang member while frozen")
		_, worker := gangPods(Default, ns.Name, name)
		deleteGangPod(ns.Name, worker)
		Eventually(func(g Gomega) {
			g.Expect(gangPodGone(g, ns.Name, worker)).To(BeTrue())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("freeze holds the degraded gang: no repair, no recreate churn")
		Consistently(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Incarnation).To(Equal(readyIncarnation),
				"a frozen pause must not restart the gang")
			live := 0
			for _, p := range listOMENativePods(g, ns.Name, name) {
				if p.DeletionTimestamp == nil {
					live++
				}
			}
			g.Expect(live).To(Equal(1), "the survivor stays; the lost member stays lost while frozen")
		}, kindSteadyStateWindow, kindPollInterval).Should(Succeed())

		ginkgo.By("downgrading freeze to plain pause resumes repair while still paused")
		setPauseAnnotation(name, "true")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Incarnation).To(BeNumerically(">", readyIncarnation),
				"plain pause must repair the gang the freeze was holding back")
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			ir := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Paused).To(BeTrue(), "repair must not require unpausing")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// A freeze that lands before a single-pod Instance wedges holds the
	// repair for as long as it stands, and lifting it is what opens the
	// repair: the pod's own kubelet evidence dates the wedge, so the first
	// pass after the lift records the repair's event and rebuilds the
	// Instance at the revision it was running, while its peer is left alone.
	// The wedge is a live pod pointed at an image the cluster cannot pull,
	// a terminal kubelet waiting reason of the same class as a crash at
	// start, which the crash-loop repair reads the same way.
	ginkgo.It("a freeze lifted over a wedged single-pod Instance opens its repair at once", func() {
		const name = "freeze-lift"
		const wedgedIndex = "1"
		const peerIndex = "0"
		configureLifecycleConfig(liftedFreezeLifecycleJSON)
		fix := newCoordFixture(ns.Name, WithISVCName(name),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		incarnationBefore := map[int32]int64{}
		var v1Hash string
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(2))
			for _, inst := range insts {
				g.Expect(inst.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				incarnationBefore[inst.Index] = inst.Incarnation
			}
			hashes := livePodHashes(g, name)
			g.Expect(hashes).To(HaveLen(1), "a converged fleet runs exactly one revision")
			for h := range hashes {
				v1Hash = h
			}
			g.Expect(v1Hash).NotTo(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("freezing the service before anything breaks")
		frozenAt := time.Now().Truncate(time.Second)
		setPauseAnnotation(name, constants.PausedRolloutFreezeValue)
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Paused).To(BeTrue())
			g.Expect(ir.Spec.PauseMode).To(Equal(v1beta1.PauseModeFreeze))
			g.Expect(ir.Status.ObservedGeneration).To(Equal(ir.Generation))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("wedging instance " + wedgedIndex + " under the freeze")
		var wedgedPod string
		var wedgedUID types.UID
		Eventually(func(g Gomega) {
			pod := singleEnginePodOfInstance(g, ns.Name, name, wedgedIndex)
			pod.Spec.Containers[0].Image = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
			g.Expect(k8sClient.Update(suiteCtx, pod)).To(Succeed())
			wedgedPod, wedgedUID = pod.Name, pod.UID
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: wedgedPod}, pod)).To(Succeed())
			g.Expect(terminalWaitingReasons(pod)).NotTo(BeEmpty(),
				"kubelet must report a terminal waiting reason on the wedged pod")
		}).WithTimeout(3 * time.Minute).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the freeze holds the wedged Instance as it stands")
		Consistently(func(g Gomega) {
			g.Expect(restartsRecordedForInstance(ns.Name, name, wedgedIndex, frozenAt)).To(BeEmpty(),
				"a freeze starts no repair")
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: wedgedPod}, pod)).To(Succeed())
			g.Expect(pod.UID).To(Equal(wedgedUID), "the wedged pod must stand while frozen")
			for _, inst := range instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent) {
				g.Expect(inst.Incarnation).To(Equal(incarnationBefore[inst.Index]),
					"no Instance may be rebuilt while frozen")
			}
		}, kindSteadyStateWindow, kindPollInterval).Should(Succeed())

		ginkgo.By("lifting the freeze with the Instance still wedged")
		liftedAt := time.Now().Truncate(time.Second)
		setPauseAnnotation(name, "")

		ginkgo.By("the lift opens the repair, which rebuilds the Instance at its running revision")
		Eventually(func(g Gomega) {
			g.Expect(restartsRecordedForInstance(ns.Name, name, wedgedIndex, liftedAt)).NotTo(BeEmpty(),
				"the lift must release the repair the freeze held, recorded by its event")
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(2))
			for _, inst := range insts {
				g.Expect(inst.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				if inst.Index == mustAtoi(wedgedIndex) {
					g.Expect(inst.Incarnation).To(BeNumerically(">", incarnationBefore[inst.Index]),
						"the wedged Instance must be rebuilt")
				} else {
					g.Expect(inst.Incarnation).To(Equal(incarnationBefore[inst.Index]),
						"the peer must be left as it was")
				}
			}
			pod := singleEnginePodOfInstance(g, ns.Name, name, wedgedIndex)
			g.Expect(pod.UID).NotTo(Equal(wedgedUID), "the repair replaces the wedged pod")
			g.Expect(pod.Labels[query.LabelRevisionHash]).To(Equal(v1Hash), "the repair rebuilds the running revision")
			g.Expect(podreadiness.IsContainersReady(pod) && podreadiness.IsServing(pod)).To(BeTrue(),
				"the rebuilt Instance must serve")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(restartsRecordedForInstance(ns.Name, name, peerIndex, frozenAt)).To(BeEmpty(),
			"only the wedged Instance is repaired")
	})
})
