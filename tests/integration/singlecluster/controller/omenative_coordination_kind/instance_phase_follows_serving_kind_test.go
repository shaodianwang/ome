package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/tests/integration/util"
)

// An Instance whose pod stops serving publishes Restarting, not Ready.
//
// One engine Instance under restart policy None runs an image whose
// container passes readiness and exits 20 seconds later. The kubelet
// restarts the container in place with a growing back-off between runs, so
// the one pod alternates between serving and down. To the engine the
// Instance stays Ready the whole time: under this policy no repair opens
// inside the stuck-pod grace, the pod keeps its identity and no operation
// is written. The phase the status publishes must follow the pod:
// Restarting with no serving pod while the container is down, Ready again
// once it serves. A dashboard that counts Ready Instances as capacity is
// then never shown a Ready Instance that serves nothing for longer than one
// status write.
//
// The stuck-pod grace is raised to 300 seconds so the crash-loop repair it
// arms stays out of the observation. The shared invariants are enforced
// throughout: a Ready row's pod serves.
//
// Cells: instance/Ready/pod.containerRestart/before
const (
	phaseTruthISVC          = "phase-follows-serving"
	phaseTruthLifecycleJSON = `{"stuckPodGracePeriod":"300s"}`
	// phaseTruthObserveWindow spans the container's return, its next exit
	// and the back-off after it, so the phase is seen to follow the pod
	// both ways more than once.
	phaseTruthObserveWindow = 90 * time.Second
)

var _ = ginkgo.Describe("behavior — an Instance whose pod stops serving publishes Restarting (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "phase-truth-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("publishes Restarting while the pod is down and Ready once it serves, on the same pod with no operation", func() {
		configureLifecycleConfig(phaseTruthLifecycleJSON)

		none := v1beta1.InstanceRestartPolicyNone
		fix := newCoordFixture(ns.Name, WithISVCName(phaseTruthISVC),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		engine := &fix.ISVC.Spec.Engine.PodSpec.Containers[0]
		engine.Env = append(engine.Env, corev1.EnvVar{Name: "FAIL_AT", Value: "after-20s"})
		engine.ReadinessProbe = fakeServingReadinessProbe()
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{RestartPolicy: &none}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the Instance to be promoted on its one serving pod")
		var podUID types.UID
		var runningRev string
		var incarnation int64
		Eventually(func(g Gomega) {
			rows := instanceStatuses(g, ns.Name, phaseTruthISVC, v1beta1.EngineComponent)
			g.Expect(rows).To(HaveLen(1))
			g.Expect(rows[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(rows[0].Operation).To(BeNil())
			g.Expect(rows[0].RunningRevision).NotTo(BeEmpty())
			pods := listOMENativePods(g, ns.Name, phaseTruthISVC)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(podIsReady(&pods[0])).To(BeTrue())
			g.Expect(podreadiness.IsServing(&pods[0])).To(BeTrue())
			podUID, runningRev, incarnation = pods[0].UID, rows[0].RunningRevision, rows[0].Incarnation
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The shared invariants, enforced: a Ready row's pod serves.
		invariants := util.NewInvariantSampler(k8sClient,
			util.InvariantTarget{Namespace: ns.Name, Name: phaseTruthISVC},
			util.InvariantSamplerOptions{Enforce: true})
		invariants.Start(suiteCtx)
		defer invariants.Stop()

		// samePod is the one pod of the service, which must keep its
		// identity: under restart policy None the kubelet restarts the
		// container in place and OME rebuilds nothing.
		samePod := func(g Gomega) *corev1.Pod {
			pods := listOMENativePods(g, ns.Name, phaseTruthISVC)
			g.Expect(pods).To(HaveLen(1), "the Instance keeps its one pod")
			g.Expect(pods[0].UID).To(Equal(podUID), "the pod was replaced; nothing rebuilds under restart policy None")
			return &pods[0]
		}
		// sameRow is the one Instance row, settled on its revision with no
		// operation, in one of the two phases the status may publish.
		sameRow := func(g Gomega) v1beta1.OMENativeInstanceStatus {
			rows := instanceStatuses(g, ns.Name, phaseTruthISVC, v1beta1.EngineComponent)
			g.Expect(rows).To(HaveLen(1))
			row := rows[0]
			g.Expect(row.Operation).To(BeNil(), "no operation may open under restart policy None: %+v", row.Operation)
			g.Expect(row.RunningRevision).To(Equal(runningRev), "the row keeps its revision")
			g.Expect(row.Incarnation).To(Equal(incarnation), "the row keeps its incarnation")
			g.Expect(row.Phase).To(BeElementOf(v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceRestarting),
				"the status may publish Ready or Restarting for a settled row, got %s", row.Phase)
			return row
		}

		ginkgo.By("expecting the published phase to read Restarting once the container exits")
		Eventually(func(g Gomega) {
			row := sameRow(g)
			g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceRestarting))
			g.Expect(row.ServingPodCount).To(BeZero(), "a Restarting row publishes its pods as not serving")
			g.Expect(podIsReady(samePod(g))).To(BeFalse(), "the pod must be down while the row reads Restarting")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting Ready again once the kubelet restarts the container and it serves")
		Eventually(func(g Gomega) {
			row := sameRow(g)
			g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := samePod(g)
			g.Expect(podIsReady(pod)).To(BeTrue())
			g.Expect(podreadiness.IsServing(pod)).To(BeTrue())
			restarts := int32(0)
			for _, cs := range pod.Status.ContainerStatuses {
				restarts += cs.RestartCount
			}
			g.Expect(restarts).To(BeNumerically(">", 0), "the container must have been restarted in place")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("watching the phase follow the pod across further exits and returns")
		seen := map[v1beta1.OMENativeInstancePhase]bool{}
		Consistently(func(g Gomega) {
			row := sameRow(g)
			samePod(g)
			seen[row.Phase] = true
		}).WithTimeout(phaseTruthObserveWindow).WithPolling(kindPollInterval).Should(Succeed())
		Expect(seen).To(HaveKey(v1beta1.OMENativeInstanceRestarting), "the container exits again inside the window and the row must say so")
		Expect(seen).To(HaveKey(v1beta1.OMENativeInstanceReady), "the container returns inside the window and the row must say so")

		invariants.Stop()
		invariants.Report(Default)
	})
})
