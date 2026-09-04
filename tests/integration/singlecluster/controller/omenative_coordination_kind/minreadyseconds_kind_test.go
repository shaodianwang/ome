package omenative_coordination_kind

import (
	"sort"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// lifecycle.minReadySeconds under SurgeThenDrain.
//
// A 2-Instance single-pod engine with maxSurge=1 / maxUnavailable=0 and a
// 20s window rolls to a new image. The window gates the old pod leaving
// rotation, so for every Instance the old pod's ome.io/serving transition
// to False must not precede the surge pod's PodReady transition plus the
// window; the delete request, which additionally follows the drain and the
// settle, is checked as a weaker second bound. Because the surge slot stays
// in flight for the whole wait, the second Instance's surge pod may only be
// created after the first Instance's old pod was deleted (promotion follows
// that deletion).
var _ = ginkgo.Describe("behavior — MinReadySeconds paces SurgeThenDrain", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "minready-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("MinReadySeconds: old pods stay in rotation through the window and Instances surge one at a time", func() {
		const isvcName = "minready"
		const window = int32(20)
		maxSurge := intstr.FromInt(1)
		maxUnavail := intstr.FromInt(0)
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			MinReadySeconds: ptr.To(window),
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type: v1beta1.UpdateStrategySurgeThenDrain,
				RollingUpdate: &v1beta1.RollingUpdate{
					MaxSurge:       &maxSurge,
					MaxUnavailable: &maxUnavail,
				},
				// A short post-drain settle keeps the spec's wall-clock
				// dominated by the window under test, not the connection
				// grace that the settle exists for.
				InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
					GracePeriodSeconds: ptr.To(int32(5)),
				},
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var v1Rev string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			v1Rev = engCS.Lifecycle.CurrentRevision
			g.Expect(v1Rev).NotTo(BeEmpty(), "engine CurrentRevision must be promoted before the bump")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		v1Hash := query.RevisionHashFromControllerRevisionName(v1Rev)

		// Per-pod timeline, keyed by pod name and grouped by Instance index.
		// Old-revision pods record the moment they left rotation (the
		// ome.io/serving condition's transition to False, stamped by the
		// controller when it drains the pod), the moment the delete was
		// requested (deletionTimestamp minus the grace it already includes)
		// and the last sample that saw them without a deletionTimestamp,
		// which bounds the delete request from below should the pod vanish
		// between two samples; new-revision pods record creation and the
		// first observed PodReady transition.
		type podFact struct {
			index             int32
			created           time.Time
			readyAt           time.Time
			drainedAt         time.Time
			deleteRequestedAt time.Time
			lastSeenLive      time.Time
		}
		deleteBound := func(f *podFact) time.Time {
			if !f.deleteRequestedAt.IsZero() {
				return f.deleteRequestedAt
			}
			return f.lastSeenLive
		}
		var mu sync.Mutex
		oldPods := map[string]*podFact{}
		newPods := map[string]*podFact{}
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			engine := snap.ByComponent[v1beta1.EngineComponent]
			g.Expect(len(engine)).To(BeNumerically("<=", 3),
				"maxSurge=1 on 2 Instances allows at most one surge pod alive; got %d", len(engine))
			sampledAt := time.Now()
			mu.Lock()
			defer mu.Unlock()
			for _, pod := range engine {
				idx, ok := query.InstanceIdxFromLabels(pod)
				if !ok {
					continue
				}
				if pod.Labels[query.LabelRevisionHash] == v1Hash {
					f, seen := oldPods[pod.Name]
					if !seen {
						f = &podFact{index: idx}
						oldPods[pod.Name] = f
					}
					if f.drainedAt.IsZero() {
						for _, cond := range pod.Status.Conditions {
							if cond.Type == query.ServingConditionType && cond.Status == corev1.ConditionFalse {
								f.drainedAt = cond.LastTransitionTime.Time
							}
						}
					}
					switch {
					case pod.DeletionTimestamp == nil:
						f.lastSeenLive = sampledAt
					case f.deleteRequestedAt.IsZero():
						grace := time.Duration(ptr.Deref(pod.DeletionGracePeriodSeconds, 0)) * time.Second
						f.deleteRequestedAt = pod.DeletionTimestamp.Add(-grace)
					}
					continue
				}
				f, seen := newPods[pod.Name]
				if !seen {
					f = &podFact{index: idx, created: pod.CreationTimestamp.Time}
					newPods[pod.Name] = f
				}
				if f.readyAt.IsZero() {
					for _, cond := range pod.Status.Conditions {
						if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
							f.readyAt = cond.LastTransitionTime.Time
						}
					}
				}
			}
		})
		defer stop()

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1Rev), "engine CurrentRevision must advance off v1")
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(snap.OnRevision(v1beta1.EngineComponent, v1Hash)).To(BeZero(), "every v1 pod must be gone")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		mu.Lock()
		defer mu.Unlock()
		Expect(oldPods).To(HaveLen(2), "two v1 pods must have been observed")
		Expect(newPods).To(HaveLen(2), "two v2 surge pods must have been observed")
		readyByIndex := map[int32]time.Time{}
		for name, f := range newPods {
			Expect(f.readyAt).NotTo(BeZero(), "surge pod %s never observed PodReady", name)
			readyByIndex[f.index] = f.readyAt
		}
		for name, f := range oldPods {
			readyAt, ok := readyByIndex[f.index]
			Expect(ok).To(BeTrue(), "no surge pod observed for Instance %d", f.index)
			earliest := readyAt.Add(time.Duration(window) * time.Second)
			// The gated step: the controller drains the old pod (serving=False)
			// only once the surge pod has been PodReady for the window. Both
			// timestamps are condition lastTransitionTimes, so the comparison
			// carries no sampling slack.
			Expect(f.drainedAt).NotTo(BeZero(), "old pod %s was never observed leaving rotation (ome.io/serving=False)", name)
			Expect(f.drainedAt.Before(earliest)).To(BeFalse(),
				"Instance %d: old pod %s left rotation at %s, before its surge pod's PodReady %s + %ds",
				f.index, name, f.drainedAt.Format(time.RFC3339), readyAt.Format(time.RFC3339), window)
			deletedAt := deleteBound(f)
			Expect(deletedAt).NotTo(BeZero(), "old pod %s was never observed", name)
			Expect(deletedAt.Before(earliest)).To(BeFalse(),
				"Instance %d: old pod %s delete requested at %s, before its surge pod's PodReady %s + %ds",
				f.index, name, deletedAt.Format(time.RFC3339), readyAt.Format(time.RFC3339), window)
		}

		// One surge at a time: the later surge pod is created only after the
		// earlier Instance's old pod was deleted, which is what its promotion
		// (and so the release of the single surge slot) waits on.
		surges := make([]*podFact, 0, len(newPods))
		for _, f := range newPods {
			surges = append(surges, f)
		}
		sort.Slice(surges, func(i, j int) bool { return surges[i].created.Before(surges[j].created) })
		first, second := surges[0], surges[1]
		Expect(second.index).NotTo(Equal(first.index))
		var firstOldDeleted time.Time
		for _, f := range oldPods {
			if f.index == first.index {
				firstOldDeleted = deleteBound(f)
			}
		}
		Expect(second.created.Before(firstOldDeleted)).To(BeFalse(),
			"Instance %d surged at %s before Instance %d's old pod was deleted at %s",
			second.index, second.created.Format(time.RFC3339), first.index, firstOldDeleted.Format(time.RFC3339))
	})
})
