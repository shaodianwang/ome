package omenative_coordination_kind

import (
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A rolling push holds while a promoted set of the new revision is
// unproven.
//
// The pushed revision is Ready and then dies ten seconds later
// (fake-serving's FAIL_AT=after-10s). The roll promotes the first
// Instance on it and admits the next start while that pod still looks
// healthy; then the pod restarts. From that restart on the promoted set
// is unproven until it has held Ready for the window, the Component's
// minReadySeconds floored by the stuck-pod grace, and a set that dies
// again inside the window stays unproven. So no further Instance leaves
// the running revision: at most the budget plus that one start is off
// it, the running revision keeps serving on the rest, and the
// Component's status names the Budget hold with the Instance behind it.
// Budget plus one is the ceiling the contract allows, not a peak the roll
// must reach: a drain-first strategy admits the next start the moment
// the first Instance is promoted, ten seconds before its pod dies, so
// those stories see both; under SurgeThenDrain the next surge opens only
// once the first source has drained, which the crash can precede, and
// the roll may stop at one off. A corrected revision then lands
// everywhere without operator action.
//
// Four single-pod engine Instances: under SurgeThenDrain maxSurge 1 and
// under RecreatePod maxUnavailable 1 with restart policy None, so the
// roll's hold is told from a repair's; and under RecreatePod with
// RecreateInstanceOnPodRestart, where the restart rebuilds the Instance
// and the rebuilt pod, created inside the window after the failure the
// row recorded, is the set's comeback. The pushed revision also starts
// slowly, so the roll cannot outrun the first crash the way a revision
// that binds in milliseconds would.

const (
	// rollHoldReplicas is the fleet; rollHoldOffAllowed is its budget of
	// one plus the one start admitted while the promoted pod still looked
	// healthy.
	rollHoldReplicas   = 4
	rollHoldOffAllowed = 2
	// rollHoldStartupDelaySec outlasts the ten seconds the pushed revision
	// serves, so its first crash is visible before the next start is
	// Ready.
	rollHoldStartupDelaySec = 15
	// rollHoldObserveWindow is how long the bound is watched after the
	// first restart: long enough for the comeback, the second crash, and
	// for an unbounded roll to reach a third Instance, and short of the
	// stuck-pod grace, past which the crash loop is the repair's story.
	rollHoldObserveWindow = 90 * time.Second
)

// The grace floors the proven window and, measured from each pod's
// creation, keeps the stuck-pod escalation out of the observation.
const rollHoldLifecycleJSON = `{"stuckPodGracePeriod":"300s"}`

// Cells: instance/Ready/pod.containerRestart/before, instance/Ready/gate.deny[Budget]/before
var _ = ginkgo.Describe("behavior - a roll holds while a promoted set is unproven (KIND)", func() {
	describeRollHoldsWhileUnproven("under SurgeThenDrain maxSurge 1", "unproven-surge",
		v1beta1.UpdateStrategySurgeThenDrain, intstr.FromInt(1), intstr.FromInt(0), v1beta1.InstanceRestartPolicyNone, false)
	describeRollHoldsWhileUnproven("under RecreatePod maxUnavailable 1", "unproven-recreate",
		v1beta1.UpdateStrategyRecreatePod, intstr.FromInt(0), intstr.FromInt(1), v1beta1.InstanceRestartPolicyNone, true)
	describeRollHoldsWhileUnproven("under RecreatePod maxUnavailable 1 and RecreateInstanceOnPodRestart", "unproven-rebuild",
		v1beta1.UpdateStrategyRecreatePod, intstr.FromInt(0), intstr.FromInt(1), v1beta1.InstanceRestartPolicyRecreateInstance, true)
})

// holdNamesInstance reports whether a hold's not-serving clause names idx:
// the indices between "Instance"/"Instances" and " on revision".
func holdNamesInstance(reason string, idx int32) bool {
	start := strings.LastIndex(reason, "; Instance")
	end := strings.LastIndex(reason, " on revision ")
	if start < 0 || end < 0 || end <= start {
		return false
	}
	clause := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(reason[start+len("; "):end], "Instances"), "Instance"))
	for _, field := range strings.Split(clause, ",") {
		if strings.TrimSpace(field) == strconv.Itoa(int(idx)) {
			return true
		}
	}
	return false
}

// describeRollHoldsWhileUnproven registers the story for one strategy and
// restart policy. drainFirst says the strategy admits the next start the
// moment the first Instance is promoted, so the roll reaches the ceiling
// before the first pod dies.
func describeRollHoldsWhileUnproven(name, isvcName string, strategy v1beta1.UpdateStrategyType, maxSurge, maxUnavailable intstr.IntOrString, policy v1beta1.InstanceRestartPolicy, drainFirst bool) {
	ginkgo.Describe(name, ginkgo.Ordered, func() {
		var (
			ns *corev1.Namespace

			runningRev string
			crashRev   string
			// peakOff is the most Instances any poll saw off the running
			// revision; everOff is every Instance any poll saw carrying a
			// pod off it, which bounds the starts the roll ever opened.
			peakOff int
			everOff map[int32]struct{}
		)

		ginkgo.BeforeAll(func() {
			var err error
			ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, isvcName+"-")
			Expect(err).NotTo(HaveOccurred())
			ginkgo.DeferCleanup(func() {
				Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
			})
			configureLifecycleConfig(rollHoldLifecycleJSON)

			restartPolicy := policy
			fix := newCoordFixture(ns.Name, WithISVCName(isvcName), WithoutDecoder(),
				WithEngineReplicas(rollHoldReplicas))
			fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
				UpdateStrategy: &v1beta1.UpdateStrategy{
					Type:          strategy,
					RollingUpdate: &v1beta1.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
				},
				RestartPolicy: &restartPolicy,
			}
			applyFixture(Default, fix)
			deferFixtureCleanup(fix)
			everOff = map[int32]struct{}{}
		})

		// enginePods returns the non-deleting engine pods.
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

		// podsByInstance buckets live pods by Instance index.
		podsByInstance := func(pods []*corev1.Pod) map[int32][]*corev1.Pod {
			byIndex := map[int32][]*corev1.Pod{}
			for _, pod := range pods {
				if idx, ok := query.InstanceIdxFromLabels(pod); ok {
					byIndex[idx] = append(byIndex[idx], pod)
				}
			}
			return byIndex
		}

		// tally reads the fleet by Instance: off is how many Instances carry
		// a live pod on any revision but the running one, crashing how many
		// carry one on the crashing revision, serving how many serve the
		// running revision on every live pod. Every Instance seen off is
		// recorded in everOff.
		tally := func(pods []*corev1.Pod) (off, crashing, serving int) {
			running, crash := query.RevisionFromName(runningRev), query.RevisionFromName(crashRev)
			for idx, set := range podsByInstance(pods) {
				offRunning, onCrash, servesRunning := false, false, true
				for _, pod := range set {
					rev := query.RevisionFromPod(pod)
					if !rev.Same(running) {
						offRunning = true
						onCrash = onCrash || (crashRev != "" && rev.Same(crash))
					} else if !podIsReady(pod) || !podreadiness.IsServing(pod) {
						servesRunning = false
					}
				}
				switch {
				case offRunning:
					off++
					everOff[idx] = struct{}{}
					if onCrash {
						crashing++
					}
				case servesRunning:
					serving++
				}
			}
			return off, crashing, serving
		}

		// observe enforces the bound on every poll that threads it: at most
		// the allowance is off the running revision or carries the crashing
		// revision, no Instance beyond the allowance has ever left the
		// running revision, and the running revision keeps serving on the
		// rest.
		observe := func(g Gomega) {
			off, crashing, serving := tally(enginePods(g))
			peakOff = max(peakOff, off)
			g.Expect(off).To(BeNumerically("<=", rollHoldOffAllowed),
				"%d engine Instances are off the running revision, budget %d plus the one start admitted while the promoted pod still looked healthy: the roll kept replacing Instances with a revision that dies after Ready",
				off, rollHoldOffAllowed-1)
			g.Expect(crashing).To(BeNumerically("<=", rollHoldOffAllowed),
				"%d engine Instances carry the crashing revision at once, budget %d plus one", crashing, rollHoldOffAllowed-1)
			g.Expect(len(everOff)).To(BeNumerically("<=", rollHoldOffAllowed),
				"the roll started a further Instance while the promoted set was unproven: %d Instances have left the running revision", len(everOff))
			g.Expect(serving).To(BeNumerically(">=", rollHoldReplicas-rollHoldOffAllowed),
				"the running revision must keep serving on %d Instances, serves on %d", rollHoldReplicas-rollHoldOffAllowed, serving)
		}

		ginkgo.It("settles the fleet on the running revision", func() {
			Eventually(func(g Gomega) {
				ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
				g.Expect(ir.Status.CurrentRevision).NotTo(BeEmpty())
				g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision))
				rows := decodedInstanceStatuses(g, ir)
				g.Expect(rows).To(HaveLen(rollHoldReplicas))
				for _, row := range rows {
					g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				}
				runningRev = ir.Status.CurrentRevision
				pods := enginePods(g)
				g.Expect(pods).To(HaveLen(rollHoldReplicas))
				for _, pod := range pods {
					g.Expect(podreadiness.IsServing(pod)).To(BeTrue())
				}
			}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		})

		ginkgo.It("pushes a revision that is Ready and then dies", func() {
			Eventually(func(g Gomega) {
				cur := freshISVC(g, ns.Name, isvcName)
				engine := &cur.Spec.Engine.PodSpec.Containers[0]
				engine.Env = append(engine.Env,
					corev1.EnvVar{Name: "FAIL_AT", Value: "after-10s"},
					corev1.EnvVar{Name: "STARTUP_DELAY_SEC", Value: strconv.Itoa(rollHoldStartupDelaySec)})
				engine.ReadinessProbe = fakeServingReadinessProbe()
				g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

			Eventually(func(g Gomega) {
				ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
				g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty())
				g.Expect(ir.Status.UpdateRevision).NotTo(Equal(runningRev))
				crashRev = ir.Status.UpdateRevision
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		})

		ginkgo.It("promotes the first Instance onto it, whose runner then restarts", func() {
			// The restart that makes the promoted set unproven: a restart on
			// the pod's record, or, where the policy rebuilds the Instance,
			// the failure the rebuild answers on the row's record.
			Eventually(func(g Gomega) {
				observe(g)
				ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
				byIndex := podsByInstance(enginePods(g))
				restarted := false
				for _, row := range decodedInstanceStatuses(g, ir) {
					if row.RunningRevision != crashRev {
						continue
					}
					if row.LastFailure != nil {
						restarted = true
					}
					for _, pod := range byIndex[row.Index] {
						for _, cs := range pod.Status.ContainerStatuses {
							if cs.RestartCount > 0 {
								restarted = true
							}
						}
					}
				}
				g.Expect(restarted).To(BeTrue(), "a pod of an Instance promoted onto %s must have restarted after it served", crashRev)
			}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		})

		ginkgo.It("names the Budget hold with the unproven Instance behind it", func() {
			Eventually(func(g Gomega) {
				observe(g)
				ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
				g.Expect(ir.Status.RolloutHold).NotTo(BeNil(), "status.rolloutHold must say why the roll stopped")
				g.Expect(ir.Status.RolloutHold.Gate).To(Equal(v1beta1.RolloutHoldGateBudget))
				g.Expect(ir.Status.RolloutHold.Target).To(Equal(crashRev))
				g.Expect(ir.Status.RolloutHold.Reason).To(ContainSubstring("not serving"))
				named := false
				for _, row := range decodedInstanceStatuses(g, ir) {
					if row.RunningRevision == crashRev && holdNamesInstance(ir.Status.RolloutHold.Reason, row.Index) {
						named = true
					}
				}
				g.Expect(named).To(BeTrue(), "the hold must name an Instance promoted onto %s: %q", crashRev, ir.Status.RolloutHold.Reason)
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		})

		ginkgo.It("starts no further Instance while the promoted set is unproven", func() {
			Consistently(observe, rollHoldObserveWindow, kindPollInterval).Should(Succeed())
			if drainFirst {
				Expect(peakOff).To(Equal(rollHoldOffAllowed),
					"a drain-first roll admits the next start the moment the first Instance is promoted and must stop there; peak %d off the running revision", peakOff)
			}
		})

		ginkgo.It("converges on a corrected revision", func() {
			Eventually(func(g Gomega) {
				cur := freshISVC(g, ns.Name, isvcName)
				engine := &cur.Spec.Engine.PodSpec.Containers[0]
				kept := engine.Env[:0]
				for _, env := range engine.Env {
					if env.Name != "FAIL_AT" {
						kept = append(kept, env)
					}
				}
				engine.Env = kept
				g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

			Eventually(func(g Gomega) {
				ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
				g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty())
				g.Expect(ir.Status.UpdateRevision).NotTo(Equal(crashRev), "the corrected spec must produce a new revision")
				g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision), "the engine must converge on the corrected revision")
				rows := decodedInstanceStatuses(g, ir)
				g.Expect(rows).To(HaveLen(rollHoldReplicas))
				for _, row := range rows {
					g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
					g.Expect(row.RunningRevision).To(Equal(ir.Status.UpdateRevision))
				}
				g.Expect(ir.Status.RolloutHold).To(BeNil(), "status.rolloutHold must clear once the rollout converges")
			}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		})
	})
}
