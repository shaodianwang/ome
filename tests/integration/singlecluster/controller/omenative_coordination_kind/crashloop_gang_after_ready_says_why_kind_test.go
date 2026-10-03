package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A gang whose pushed runner stops itself after serving, on every set,
// under RecreateInstanceOnPodRestart. The leader of each new set exits
// twenty seconds after it is Ready, inside the crash window; the policy
// rebuilds the first set at once, and from the rebuilt set's death on the
// running revision's retry ladder counts the crash, so the status says
// why (a RetryBlock on the pushed revision and a rolloutHold naming it)
// before any further crash-revision set is built, the ladder holds after
// its attempts with a RetryHeld warning, and no crash-revision pod is
// created after the hold. A corrected revision then lands on every gang.
//
// The ladder is the chart's shape at a seconds scale: three attempts.
// The runner counts its exit from the moment it starts serving, so the
// gang's containers carry the readiness probe and the leader's death is
// a Ready-to-NotReady flap the kubelet reports, as on a cluster.

// Cells: instance/Ready/pod.containerRestart/before, instance/Ready/gate.deny[Held]/before
var _ = ginkgo.Describe("behavior - a gang whose runner dies after Ready on every set says why and holds (KIND)", func() {
	for _, tc := range []struct {
		name     string
		strategy v1beta1.UpdateStrategyType
		// slug keeps the service name short: every Service and pod name
		// derived from it, with a revision hash and the headless suffix,
		// must stay a DNS label.
		slug string
	}{
		{name: "SurgeThenDrain maxSurge 1", strategy: v1beta1.UpdateStrategySurgeThenDrain, slug: "surge"},
		{name: "RecreatePod maxUnavailable 1", strategy: v1beta1.UpdateStrategyRecreatePod, slug: "recreate"},
	} {
		tc := tc
		ginkgo.Describe(tc.name, ginkgo.Ordered, func() {
			const (
				goodImage      = "fake-serving:v1"
				engineReplicas = 4
				podsPerGang    = 2
				exitAfterReady = "20"
				ladderAttempts = int32(3)
				// postHeldWindow outlives the exit delay and a rebuild, so a
				// set rebuilt past the hold would surface as a new pod UID.
				postHeldWindow = 45 * time.Second
			)
			// The crash window is the stuck-pod grace with no minReadySeconds;
			// the runner's exit must fall inside it.
			const lifecycleJSON = `{"updateRetry":{"maxAttempts":3,"initialDelay":"5s","maxDelay":"20s","multiplier":2.0},"stuckPodGracePeriod":"60s"}`
			isvcName := "dies-after-ready-" + tc.slug

			var (
				ns        *corev1.Namespace
				engineRev string
				badRev    string
				// seenByIndex records every crash-revision pod UID per
				// Instance; diedByIndex the ones whose runner restarted.
				seenByIndex = map[string]map[types.UID]struct{}{}
				diedByIndex = map[string]map[types.UID]struct{}{}
				// setsAtLoop is, per Instance, how many crash-revision pods it
				// had when its second set died; a pod beyond that count before
				// the status says why is a rebuild the ladder did not pace.
				setsAtLoop = map[string]int{}
				saidWhy    bool
				uidsAtHeld = -1
			)

			ginkgo.BeforeAll(func() {
				var err error
				ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "dies-after-ready-")
				Expect(err).NotTo(HaveOccurred())
				ginkgo.DeferCleanup(func() {
					Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
				})
				configureLifecycleConfig(lifecycleJSON)

				one := intstr.FromInt(1)
				fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
					WithEngineReplicas(engineReplicas),
					WithEngineWorkerSize(podsPerGang-1),
					WithEngineImage(goodImage),
					WithEngineUpdateStrategy(tc.strategy))
				rolling := &v1beta1.RollingUpdate{MaxUnavailable: &one}
				if tc.strategy == v1beta1.UpdateStrategySurgeThenDrain {
					zero := intstr.FromInt(0)
					rolling = &v1beta1.RollingUpdate{MaxSurge: &one, MaxUnavailable: &zero}
				}
				fix.ISVC.Spec.Engine.Lifecycle.UpdateStrategy.RollingUpdate = rolling
				fix.ISVC.Spec.Engine.Lifecycle.RestartPolicy = ptr.To(v1beta1.InstanceRestartPolicyRecreateInstance)
				// The runner's readiness is probed, so a leader that stops
				// itself leaves Ready and returns to it the way a served
				// runner does on a cluster.
				for _, ps := range []*v1beta1.PodSpec{&fix.ISVC.Spec.Engine.PodSpec, &fix.ISVC.Spec.Engine.Leader.PodSpec, &fix.ISVC.Spec.Engine.Worker.PodSpec} {
					for i := range ps.Containers {
						ps.Containers[i].ReadinessProbe = fakeServingReadinessProbe()
					}
				}
				applyFixture(Default, fix)
				deferFixtureCleanup(fix)
			})

			enginePods := func(g Gomega) []*corev1.Pod {
				snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
				g.Expect(err).NotTo(HaveOccurred())
				var live []*corev1.Pod
				for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
					if pod.DeletionTimestamp == nil {
						live = append(live, pod)
					}
				}
				return live
			}

			crashPods := func(g Gomega) int {
				n := 0
				for idx := range seenByIndex {
					n += len(seenByIndex[idx])
				}
				_ = g
				return n
			}

			// observe records the crash-revision pods and their deaths and
			// enforces the standing invariants on every poll that threads it.
			observe := func(g Gomega) {
				for _, pod := range enginePods(g) {
					if !podStopsAfterReady(pod) {
						continue
					}
					idx := pod.Labels[query.LabelInstanceIdx]
					if seenByIndex[idx] == nil {
						seenByIndex[idx] = map[types.UID]struct{}{}
						diedByIndex[idx] = map[types.UID]struct{}{}
					}
					seenByIndex[idx][pod.UID] = struct{}{}
					for _, cs := range pod.Status.ContainerStatuses {
						if cs.RestartCount > 0 {
							diedByIndex[idx][pod.UID] = struct{}{}
						}
					}
					if len(diedByIndex[idx]) >= 2 {
						if _, noted := setsAtLoop[idx]; !noted {
							setsAtLoop[idx] = len(seenByIndex[idx])
						}
					}
				}
				b := findRetryBlock(retryBlocks(g, ns.Name, isvcName, v1beta1.EngineComponent), badRev)
				if b != nil {
					saidWhy = true
					g.Expect(b.AttemptsStarted).To(BeNumerically("<=", ladderAttempts))
				}
				for idx, at := range setsAtLoop {
					if !saidWhy {
						g.Expect(len(seenByIndex[idx])).To(BeNumerically("<=", at),
							"Instance %s got a further crash-revision pod before the status said why", idx)
					}
				}
				if uidsAtHeld >= 0 {
					g.Expect(crashPods(g)).To(Equal(uidsAtHeld), "no crash-revision pod may be created after the ladder held")
				}
			}

			ginkgo.It("converges every gang on v1", func() {
				Eventually(func(g Gomega) {
					ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
					g.Expect(ir.Status.CurrentRevision).NotTo(BeEmpty())
					g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision))
					rows := decodedInstanceStatuses(g, ir)
					g.Expect(rows).To(HaveLen(engineReplicas))
					for _, row := range rows {
						g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
					}
					engineRev = ir.Status.CurrentRevision
					g.Expect(enginePods(g)).To(HaveLen(engineReplicas * podsPerGang))
				}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
			})

			ginkgo.It("publishes a revision whose leader stops itself after serving", func() {
				Eventually(func(g Gomega) {
					cur := freshISVC(g, ns.Name, isvcName)
					container := &cur.Spec.Engine.Leader.PodSpec.Containers[0]
					container.Env = append(container.Env, corev1.EnvVar{Name: "EXIT_AFTER_READY_SEC", Value: exitAfterReady})
					g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
				}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
				Eventually(func(g Gomega) {
					ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
					g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty())
					g.Expect(ir.Status.UpdateRevision).NotTo(Equal(engineRev))
					badRev = ir.Status.UpdateRevision
				}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
			})

			ginkgo.It("says why before a third set and holds the revision after its attempts", func() {
				Eventually(func(g Gomega) {
					observe(g)
					g.Expect(saidWhy).To(BeTrue(), "a RetryBlock must be recorded for %s once a rebuilt set died", badRev)
					ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
					g.Expect(ir.Status.RolloutHold).NotTo(BeNil(), "status.rolloutHold must say why the roll stopped")
					g.Expect(ir.Status.RolloutHold.Target).To(Equal(badRev))
					g.Expect(ir.Status.RolloutHold.Gate).To(BeElementOf(v1beta1.RolloutHoldGateRetryBlock, v1beta1.RolloutHoldGateHeld))
				}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

				Eventually(func(g Gomega) {
					observe(g)
					b := findRetryBlock(retryBlocks(g, ns.Name, isvcName, v1beta1.EngineComponent), badRev)
					g.Expect(b).NotTo(BeNil())
					g.Expect(b.State).To(Equal(v1beta1.RetryBlockHeld))
					g.Expect(b.AttemptsStarted).To(Equal(ladderAttempts))
					events := &corev1.EventList{}
					g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
					held, failed := 0, 0
					for _, e := range events.Items {
						if e.InvolvedObject.Name != isvcName {
							continue
						}
						switch e.Reason {
						case string(workloadtypes.EventReasonRetryHeld):
							held++
						case string(workloadtypes.EventReasonInstanceFailed):
							failed++
						}
					}
					g.Expect(held).To(Equal(1), "exactly one RetryHeld warning must announce the hold")
					g.Expect(failed).To(BeNumerically(">=", 1), "an InstanceFailed warning must name the parked gang")
				}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
				uidsAtHeld = crashPods(Default)

				Consistently(func(g Gomega) {
					observe(g)
				}, postHeldWindow, kindPollInterval).Should(Succeed())
			})

			ginkgo.It("converges every gang on a corrected revision", func() {
				Eventually(func(g Gomega) {
					cur := freshISVC(g, ns.Name, isvcName)
					container := &cur.Spec.Engine.Leader.PodSpec.Containers[0]
					kept := container.Env[:0]
					for _, env := range container.Env {
						if env.Name != "EXIT_AFTER_READY_SEC" {
							kept = append(kept, env)
						}
					}
					container.Env = kept
					g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
				}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
				Eventually(func(g Gomega) {
					ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
					g.Expect(ir.Status.UpdateRevision).NotTo(Equal(badRev))
					g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision))
					for _, row := range decodedInstanceStatuses(g, ir) {
						g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
						g.Expect(row.RunningRevision).To(Equal(ir.Status.UpdateRevision))
					}
					g.Expect(ir.Status.RolloutHold).To(BeNil())
					for _, pod := range enginePods(g) {
						g.Expect(podStopsAfterReady(pod)).To(BeFalse(), "no crash-revision pod may remain: %s", pod.Name)
					}
				}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
			})
		})
	}
})

// podStopsAfterReady reports whether the pod runs a revision whose runner
// stops itself after serving: fake-serving's EXIT_AFTER_READY_SEC on any
// of its containers.
func podStopsAfterReady(pod *corev1.Pod) bool {
	for _, container := range pod.Spec.Containers {
		for _, env := range container.Env {
			if env.Name == "EXIT_AFTER_READY_SEC" && env.Value != "" {
				return true
			}
		}
	}
	return false
}
