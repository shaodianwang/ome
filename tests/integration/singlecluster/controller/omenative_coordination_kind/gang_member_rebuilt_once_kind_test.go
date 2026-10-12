package omenative_coordination_kind

import (
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A gang that loses a member is rebuilt once, and the rebuild is not a roll.
//
// Two engine gangs, a leader and one worker each, serve one revision under
// RecreateInstanceOnPodRestart, with SurgeThenDrain paced at one surge and
// one Instance out of rotation. The worker of gang 0 is deleted by hand.
// OME rebuilds the whole gang once: both members come back as new pods at
// the next incarnation, the gang returns to Ready with no attempt open, and
// gang 1 is never touched.
//
// The rebuilt pods are created after the time gang 0 last entered Ready,
// which its row keeps until the rebuilt gang is promoted. Their first
// container start is a start, not a restart: OME must not read the new gang
// as crashed, so no second rebuild opens for it, its row records no failure,
// and the Component publishes no rollout hold while it comes up. Nothing is
// rolling, and a repair that read its own new pods as crashed would report a
// spent surge budget on their account. The engine's containers start 20
// seconds late, so the window between the rebuilt pods' first start and the
// gang serving is wide enough to sample.
//
// The shared invariants are enforced throughout: gang 1 keeps the Component
// at its floor, every row tells the truth about its pods, and every traffic
// share is backed.
//
// Cells: instance/Ready/pod.deleted[source], instance/Restart/Drain/pod.containersReady
const (
	rebuiltOnceISVC     = "gang-rebuilt-once"
	rebuiltOnceGangs    = 2
	rebuiltOnceGangSize = 2
	// rebuiltOnceStartupDelaySec keeps a rebuilt member running but not
	// Ready long enough to sample the status while the gang comes up.
	rebuiltOnceStartupDelaySec = 20
	// rebuiltOnceSteadyWindow is how long the rebuilt gang is watched after
	// it serves: long enough for several passes to open a second repair.
	rebuiltOnceSteadyWindow = 30 * time.Second
)

var _ = ginkgo.Describe("behavior — a gang that loses a member is rebuilt once and publishes no rollout hold (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "gang-rebuilt-once-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("rebuilds gang 0 once at the next incarnation, leaves gang 1 alone and publishes no hold", func() {
		restartPolicy := v1beta1.InstanceRestartPolicyRecreateInstance
		fix := newCoordFixture(ns.Name, WithISVCName(rebuiltOnceISVC),
			WithoutDecoder(),
			WithEngineReplicas(rebuiltOnceGangs),
			WithEngineWorkerSize(rebuiltOnceGangSize-1),
			WithEngineImage("fake-serving:v1"),
			WithEngineStartupDelay(rebuiltOnceStartupDelaySec))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type: v1beta1.UpdateStrategySurgeThenDrain,
				RollingUpdate: &v1beta1.RollingUpdate{
					MaxSurge:       ptr.To(intstr.FromInt(1)),
					MaxUnavailable: ptr.To(intstr.FromInt(1)),
				},
			},
			RestartPolicy: &restartPolicy,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// repairsOf counts the RestartTriggered warnings that name gang idx,
		// summing the count the event correlator folds identical ones into.
		repairsOf := func(g Gomega, idx int32) int32 {
			key := workloadtypes.InstanceKey(workloadtypes.ComponentEngine, idx)
			var n int32
			for _, e := range eventsWithReason(g, ns.Name, string(workloadtypes.EventReasonRestartTriggered)) {
				if strings.Contains(e.Message, key) {
					n += max(int32(1), e.Count)
				}
			}
			return n
		}

		ginkgo.By("waiting for both gangs to serve")
		original := map[int32]map[types.UID]string{}
		var incarnation int64
		var worker corev1.Pod
		Eventually(func(g Gomega) {
			rows := instanceStatuses(g, ns.Name, rebuiltOnceISVC, v1beta1.EngineComponent)
			g.Expect(rows).To(HaveLen(rebuiltOnceGangs))
			for _, row := range rows {
				g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady), "instance %d", row.Index)
				g.Expect(row.Operation).To(BeNil(), "instance %d", row.Index)
			}
			ir := freshInferenceReplica(g, ns.Name, rebuiltOnceISVC, v1beta1.EngineComponent)
			g.Expect(ir.Status.RolloutHold).To(BeNil(), "a steady Component publishes no rollout hold")
			for idx := int32(0); idx < rebuiltOnceGangs; idx++ {
				members := liveEnginePodsOfInstance(g, ns.Name, rebuiltOnceISVC, idx)
				g.Expect(members).To(HaveLen(rebuiltOnceGangSize), "gang %d must be whole", idx)
				original[idx] = map[types.UID]string{}
				for _, pod := range members {
					g.Expect(podIsReady(&pod)).To(BeTrue(), "member %s must be Ready", pod.Name)
					g.Expect(podreadiness.IsServing(&pod)).To(BeTrue(), "member %s must be in rotation", pod.Name)
					original[idx][pod.UID] = pod.Name
					if idx == 0 {
						inc, ok := query.InstanceIncarnationFromLabels(&pod)
						g.Expect(ok).To(BeTrue(), "member %s must carry its incarnation", pod.Name)
						incarnation = inc
						if pod.Labels[query.LabelRunner] == "worker" {
							worker = pod
						}
					}
				}
			}
			g.Expect(worker.Name).NotTo(BeEmpty(), "gang 0 must have a worker to lose")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The shared invariants, enforced: the floor, status truth and
		// traffic backing hold through the repair.
		invariants := util.NewInvariantSampler(k8sClient,
			util.InvariantTarget{Namespace: ns.Name, Name: rebuiltOnceISVC},
			util.InvariantSamplerOptions{Enforce: true})
		invariants.Start(suiteCtx)
		defer invariants.Stop()

		// From the loss to the end: one repair at most, no hold, gang 1
		// untouched, no failure recorded for a member the repair created.
		stop := util.StartSampler(suiteCtx, kindPollInterval, func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, rebuiltOnceISVC, v1beta1.EngineComponent)
			g.Expect(ir.Status.RolloutHold).To(BeNil(),
				"a repair is not a roll, yet the Component published a rollout hold while gang 0 was rebuilt: %+v", ir.Status.RolloutHold)
			g.Expect(repairsOf(g, 0)).To(BeNumerically("<=", 1), "gang 0 was repaired more than once")
			g.Expect(repairsOf(g, 1)).To(BeZero(), "gang 1 lost nothing and must not be repaired")
			for _, pod := range liveEnginePodsOfInstance(g, ns.Name, rebuiltOnceISVC, 1) {
				_, old := original[1][pod.UID]
				g.Expect(old).To(BeTrue(), "gang 1 member %s is not an original pod: the repair touched the healthy gang", pod.Name)
			}
			rows := instanceStatuses(g, ns.Name, rebuiltOnceISVC, v1beta1.EngineComponent)
			g.Expect(rows).To(HaveLen(rebuiltOnceGangs), "no extra Instance may appear")
			for _, row := range rows {
				g.Expect(row.Phase).NotTo(Equal(v1beta1.OMENativeInstanceFailed), "instance %d must not park Failed", row.Index)
				if row.Index == 0 {
					g.Expect(row.LastFailure).To(BeNil(), "a lost member is a loss, not a crash: instance 0 must record no failure, got %+v", row.LastFailure)
				}
			}
		})
		defer stop()

		ginkgo.By("deleting the worker of gang 0")
		grace := int64(0)
		Expect(k8sClient.Delete(suiteCtx, &worker, &client.DeleteOptions{GracePeriodSeconds: &grace})).To(Succeed())

		ginkgo.By("expecting the whole gang rebuilt once at the next incarnation")
		rebuilt := map[types.UID]string{}
		Eventually(func(g Gomega) {
			g.Expect(repairsOf(g, 0)).To(Equal(int32(1)), "exactly one RestartTriggered must name gang 0")
			rows := instanceStatuses(g, ns.Name, rebuiltOnceISVC, v1beta1.EngineComponent)
			g.Expect(rows).To(HaveLen(rebuiltOnceGangs))
			for _, row := range rows {
				if row.Index != 0 {
					continue
				}
				g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady), "gang 0 must return to Ready")
				g.Expect(row.Operation).To(BeNil(), "the rebuild must end with no attempt left open")
			}
			members := liveEnginePodsOfInstance(g, ns.Name, rebuiltOnceISVC, 0)
			g.Expect(members).To(HaveLen(rebuiltOnceGangSize), "the gang must be rebuilt whole")
			runners := map[string]int{}
			fresh := map[types.UID]string{}
			for _, pod := range members {
				_, old := original[0][pod.UID]
				g.Expect(old).To(BeFalse(), "member %s is an original pod; a lost member rebuilds the whole gang", pod.Name)
				inc, ok := query.InstanceIncarnationFromLabels(&pod)
				g.Expect(ok).To(BeTrue(), "member %s must carry its incarnation", pod.Name)
				g.Expect(inc).To(Equal(incarnation+1), "member %s must carry the next incarnation: one rebuild", pod.Name)
				g.Expect(podIsReady(&pod)).To(BeTrue(), "member %s must be Ready", pod.Name)
				g.Expect(podreadiness.IsServing(&pod)).To(BeTrue(), "member %s must be in rotation", pod.Name)
				expectBoundBySuiteScheduler(g, &pod)
				runners[pod.Labels[query.LabelRunner]]++
				fresh[pod.UID] = pod.Name
			}
			g.Expect(runners).To(Equal(map[string]int{"leader": 1, "worker": rebuiltOnceGangSize - 1}),
				"the rebuilt gang must have one leader and its worker")
			rebuilt = fresh
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("watching the rebuilt gang for a steady window: the same pods, no second repair, no hold")
		Consistently(func(g Gomega) {
			g.Expect(repairsOf(g, 0)).To(Equal(int32(1)), "a second repair opened on the rebuilt gang")
			rows := instanceStatuses(g, ns.Name, rebuiltOnceISVC, v1beta1.EngineComponent)
			g.Expect(rows).To(HaveLen(rebuiltOnceGangs))
			for _, row := range rows {
				g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady), "instance %d", row.Index)
				g.Expect(row.Operation).To(BeNil(), "instance %d opened an attempt after the rebuild: %+v", row.Index, row.Operation)
			}
			members := liveEnginePodsOfInstance(g, ns.Name, rebuiltOnceISVC, 0)
			g.Expect(members).To(HaveLen(rebuiltOnceGangSize))
			for _, pod := range members {
				_, kept := rebuilt[pod.UID]
				g.Expect(kept).To(BeTrue(), "member %s replaced a rebuilt pod: the gang was rebuilt a second time", pod.Name)
				g.Expect(podIsReady(&pod)).To(BeTrue(), "member %s must stay Ready", pod.Name)
				g.Expect(podreadiness.IsServing(&pod)).To(BeTrue(), "member %s must stay in rotation", pod.Name)
			}
			ir := freshInferenceReplica(g, ns.Name, rebuiltOnceISVC, v1beta1.EngineComponent)
			g.Expect(ir.Status.RolloutHold).To(BeNil(), "no rollout hold may stand after the repair: %+v", ir.Status.RolloutHold)
		}).WithTimeout(rebuiltOnceSteadyWindow).WithPolling(kindPollInterval).Should(Succeed())

		samples := stop()
		Expect(samples).To(BeNumerically(">", 0), "the repair must have been sampled")
		invariants.Stop()
		invariants.Report(Default)
	})
})
