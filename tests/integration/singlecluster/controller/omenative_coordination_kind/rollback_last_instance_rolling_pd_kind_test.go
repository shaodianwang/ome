package omenative_coordination_kind

import (
	"fmt"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A rollback issued at the last Instance of a rolling-coordinated
// prefill-decode roll. The engine and the decoder roll together under one
// RollingUpdate group; when every Instance but one carries the new
// revision the starting revision is re-applied on both. The run that
// targeted the new revision must close as superseded, whether or not the
// last Instance promoted in the same breath: the service's spec is the
// authority on what the members are meant to run, and a member whose
// projection still trails that spec is not evidence the roll completed.
// The group then returns to the starting revision under a fresh run, no
// pod of the withdrawn revision is created after the rollback, and no
// RetryBlock of the withdrawn revision survives.

// Cells: instance/Ready/spec.revision/before, instance/Update/InPlace/spec.revision/before, instance/Update/Drain/spec.revision/before
var _ = ginkgo.Describe("behavior - a rollback at the last Instance of a rolling PD roll supersedes the run (KIND)", func() {
	for _, tc := range []struct {
		name     string
		strategy v1beta1.UpdateStrategyType
		slug     string
	}{
		{name: "RecreatePod maxUnavailable 1", strategy: v1beta1.UpdateStrategyRecreatePod, slug: "recreate"},
		{name: "InPlaceIfPossible maxUnavailable 1", strategy: v1beta1.UpdateStrategyInPlaceIfPossible, slug: "inplace"},
	} {
		tc := tc
		ginkgo.Describe(tc.name, ginkgo.Ordered, func() {
			const (
				replicas   = 4
				startImage = "fake-serving:v1"
				pushImage  = "fake-serving:v2"
				// afterRollbackQuiet is how long no pod of the withdrawn
				// revision may appear once the group settled back.
				afterRollbackQuiet = 20 * time.Second
			)
			components := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
			isvcName := "rollback-last-" + tc.slug

			var (
				ns        *corev1.Namespace
				startRev  = map[v1beta1.ComponentType]string{}
				pushedRev = map[v1beta1.ComponentType]string{}
				pushedAt  time.Time
				// withdrawnUIDs holds every pod UID seen on the pushed
				// revision up to and including the rollback moment.
				withdrawnUIDs = map[types.UID]struct{}{}
				// supersededSeen latches the pushed run's close as superseded:
				// the service's lastRun names the pushed revision with that
				// outcome until the rollback's own run closes over it, and the
				// RolloutRunClosed event says the same when the recorder kept
				// it.
				supersededSeen bool
			)

			pushedRunSuperseded := func(g Gomega) bool {
				cur := freshISVC(g, ns.Name, isvcName)
				if cur.Status.Rollout != nil && cur.Status.Rollout.LastRun != nil && cur.Status.Rollout.LastRun.Outcome == v1beta1.RolloutRunSuperseded {
					for _, target := range cur.Status.Rollout.LastRun.TargetRevisions {
						if target.Component == v1beta1.EngineComponent && target.Revision == query.RevisionFromName(pushedRev[v1beta1.EngineComponent]).Hash() {
							return true
						}
					}
				}
				for _, msg := range eventsRecorded(ns.Name, isvcName, rolloutrun.EventRunClosed, pushedAt) {
					if strings.Contains(msg, "superseded") {
						return true
					}
				}
				return false
			}

			ginkgo.BeforeAll(func() {
				var err error
				ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rollback-last-")
				Expect(err).NotTo(HaveOccurred())
				ginkgo.DeferCleanup(func() {
					Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
				})
				one := intstr.FromInt(1)
				fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
					WithEngineReplicas(replicas), WithDecoderReplicas(replicas),
					WithEngineImage(startImage), WithDecoderImage(startImage),
					WithEngineUpdateStrategy(tc.strategy), WithDecoderUpdateStrategy(tc.strategy),
					WithRollingUpdateGroup(components...))
				fix.ISVC.Spec.Engine.Lifecycle.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{MaxUnavailable: &one}
				fix.ISVC.Spec.Decoder.Lifecycle.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{MaxUnavailable: &one}
				applyFixture(Default, fix)
				deferFixtureCleanup(fix)
			})

			// podsOf returns the non-deleting pods of comp.
			podsOf := func(g Gomega, comp v1beta1.ComponentType) []*corev1.Pod {
				snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
				g.Expect(err).NotTo(HaveOccurred())
				var live []*corev1.Pod
				for _, pod := range snap.ByComponent[comp] {
					if pod.DeletionTimestamp == nil {
						live = append(live, pod)
					}
				}
				return live
			}

			// instancesOn counts the Instances of comp whose live pods all
			// carry rev.
			instancesOn := func(g Gomega, comp v1beta1.ComponentType, rev string) int {
				onRev := map[string]bool{}
				for _, pod := range podsOf(g, comp) {
					idx := pod.Labels[query.LabelInstanceIdx]
					if query.RevisionFromPod(pod).Same(query.RevisionFromName(rev)) {
						if _, seen := onRev[idx]; !seen {
							onRev[idx] = true
						}
					} else {
						onRev[idx] = false
					}
				}
				n := 0
				for _, all := range onRev {
					if all {
						n++
					}
				}
				return n
			}

			setImages := func(g Gomega, image string) {
				cur := freshISVC(g, ns.Name, isvcName)
				cur.Spec.Engine.PodSpec.Containers[0].Image = image
				cur.Spec.Decoder.PodSpec.Containers[0].Image = image
				g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
			}

			ginkgo.It("converges both Components on the starting revision", func() {
				Eventually(func(g Gomega) {
					for _, comp := range components {
						ir := freshInferenceReplica(g, ns.Name, isvcName, comp)
						g.Expect(ir.Status.CurrentRevision).NotTo(BeEmpty())
						g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision))
						rows := decodedInstanceStatuses(g, ir)
						g.Expect(rows).To(HaveLen(replicas))
						for _, row := range rows {
							g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
						}
						startRev[comp] = ir.Status.CurrentRevision
					}
				}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
			})

			ginkgo.It("rolls back at the last Instance of the pushed roll", func() {
				pushedAt = time.Now()
				Eventually(func(g Gomega) { setImages(g, pushImage) }).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
				Eventually(func(g Gomega) {
					for _, comp := range components {
						ir := freshInferenceReplica(g, ns.Name, isvcName, comp)
						g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty())
						g.Expect(ir.Status.UpdateRevision).NotTo(Equal(startRev[comp]))
						pushedRev[comp] = ir.Status.UpdateRevision
					}
				}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

				// The rollback lands when every Instance but one carries the
				// pushed revision; a roll that completes first leaves the
				// story unrun and says so.
				Eventually(func(g Gomega) {
					onNew := 0
					for _, comp := range components {
						onNew += instancesOn(g, comp, pushedRev[comp])
						for _, pod := range podsOf(g, comp) {
							if query.RevisionFromPod(pod).Same(query.RevisionFromName(pushedRev[comp])) {
								withdrawnUIDs[pod.UID] = struct{}{}
							}
						}
					}
					g.Expect(onNew).To(BeNumerically("<", 2*replicas), "the roll completed before the rollback could land at its last Instance")
					g.Expect(onNew).To(Equal(2*replicas-1), "waiting for the roll to reach its last Instance (%d of %d on the pushed revision)", onNew, 2*replicas)
					setImages(g, startImage)
				}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
			})

			ginkgo.It("returns to the starting revision under a fresh run with the pushed run superseded", func() {
				// The pushed run closes as superseded before the rollback's run
				// can complete and take lastRun over; the first poll that sees
				// it latches the fact.
				Eventually(func(g Gomega) {
					if !supersededSeen {
						supersededSeen = pushedRunSuperseded(g)
					}
					cur := freshISVC(g, ns.Name, isvcName)
					last := ""
					if cur.Status.Rollout != nil && cur.Status.Rollout.LastRun != nil {
						last = string(cur.Status.Rollout.LastRun.Outcome)
					}
					g.Expect(supersededSeen).To(BeTrue(), "the run that targeted the pushed revision must close as superseded (lastRun outcome %q; closed runs since the push: %v)",
						last, eventsRecorded(ns.Name, isvcName, rolloutrun.EventRunClosed, pushedAt))
				}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

				Eventually(func(g Gomega) {
					for _, comp := range components {
						ir := freshInferenceReplica(g, ns.Name, isvcName, comp)
						g.Expect(ir.Status.UpdateRevision).To(Equal(startRev[comp]), "%s must target the starting revision again", comp)
						g.Expect(ir.Status.CurrentRevision).To(Equal(startRev[comp]), "%s must converge on the starting revision", comp)
						for _, row := range decodedInstanceStatuses(g, ir) {
							g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady), "%s instance %d", comp, row.Index)
							g.Expect(row.Operation).To(BeNil(), "%s instance %d must settle with no operation open", comp, row.Index)
						}
						for _, pod := range podsOf(g, comp) {
							g.Expect(query.RevisionFromPod(pod).Same(query.RevisionFromName(startRev[comp]))).To(BeTrue(),
								"pod %s must carry the starting revision", pod.Name)
						}
						g.Expect(findRetryBlock(retryBlocks(g, ns.Name, isvcName, comp), pushedRev[comp])).To(BeNil(),
							"no RetryBlock of the withdrawn revision may survive on %s", comp)
					}
					cur := freshISVC(g, ns.Name, isvcName)
					g.Expect(cur.Status.Rollout).NotTo(BeNil())
					g.Expect(cur.Status.Rollout.ActiveRun).To(BeNil(), "the rollback's run must close once the group settled")
				}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

				Consistently(func(g Gomega) {
					for _, comp := range components {
						for _, pod := range podsOf(g, comp) {
							if query.RevisionFromPod(pod).Same(query.RevisionFromName(pushedRev[comp])) {
								_, known := withdrawnUIDs[pod.UID]
								g.Expect(known).To(BeTrue(), fmt.Sprintf("pod %s of the withdrawn revision was created after the rollback", pod.Name))
							}
						}
					}
				}, afterRollbackQuiet, kindPollInterval).Should(Succeed())
			})
		})
	}
})
