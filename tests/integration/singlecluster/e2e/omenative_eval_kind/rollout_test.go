package omenative_eval_kind

import (
	"context"
	"sync"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	workloadquery "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// p19 pins both sides of the rolling budget: one surge is allowed and no
// serving Instance may be made unavailable during the revision change.
var _ = ginkgo.Describe("rollout — SurgeThenDrain holds the surge budget", func() {
	const (
		isvcName = "p19"
		replicas = 2
		ceiling  = replicas + 1
	)

	ginkgo.It("peaks within replicas+1, preserves capacity, and converges", func() {
		prepareEvalCase(isvcName)
		isvc := p19InferenceService(evalNamespace())
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("waiting for the initial fleet to serve")
		Eventually(func(g Gomega) {
			cur := freshEvalISVC(g, isvcName)
			cs := cur.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.ServingReplicas).To(Equal(int32(replicas)))
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, evalNamespace(), isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(snap.Serving(v1beta1.EngineComponent)).To(Equal(replicas))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		var v1Rev string
		Eventually(func(g Gomega) {
			cur := freshEvalISVC(g, isvcName)
			v1Rev = cur.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
			g.Expect(v1Rev).NotTo(BeEmpty())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		var sampleMu sync.Mutex
		peak := replicas
		minServing := replicas
		samples := 0
		samplerCtx, cancelSampler := context.WithCancel(suiteCtx)
		stopSampler := util.StartSampler(samplerCtx, kindPollInterval, func(g Gomega) {
			snap, err := util.SnapshotPods(samplerCtx, k8sClient, evalNamespace(), isvcName)
			if err != nil && samplerCtx.Err() != nil {
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			alive := 0
			for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
				if p.DeletionTimestamp == nil {
					alive++
				}
			}
			serving := snap.Serving(v1beta1.EngineComponent)
			sampleMu.Lock()
			samples++
			peak = max(peak, alive)
			minServing = min(minServing, serving)
			sampleMu.Unlock()
			g.Expect(alive).To(BeNumerically("<=", ceiling),
				"engine pods alive at once must stay within the surge budget (%d); observed %d", ceiling, alive)
			g.Expect(serving).To(BeNumerically(">=", replicas),
				"maxUnavailable=0 requires at least %d serving engine Instances; observed %d", replicas, serving)
		})
		stop := func() {
			cancelSampler()
			stopSampler()
		}
		defer stop()

		ginkgo.By("bumping the revision")
		Eventually(func(g Gomega) {
			cur := freshEvalISVC(g, isvcName)
			if cur.Spec.Engine.Annotations == nil {
				cur.Spec.Engine.Annotations = map[string]string{}
			}
			cur.Spec.Engine.Annotations["perm.test/rev"] = "v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for pod, IR status, and ISVC status to converge")
		Eventually(func(g Gomega) {
			cur := freshEvalISVC(g, isvcName)
			cs := cur.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			lifecycle := cs.Lifecycle
			g.Expect(lifecycle.CurrentRevision).NotTo(Equal(v1Rev),
				"the annotation bump must produce a new revision")
			g.Expect(lifecycle.CurrentRevision).To(Equal(lifecycle.UpdateRevision))
			g.Expect(lifecycle.Replicas).To(Equal(int32(replicas)))
			g.Expect(lifecycle.ReadyReplicas).To(Equal(int32(replicas)))
			g.Expect(lifecycle.ServingReplicas).To(Equal(int32(replicas)))
			g.Expect(lifecycle.UpdatedReplicas).To(Equal(int32(replicas)))
			g.Expect(lifecycle.UpdatedReadyReplicas).To(Equal(int32(replicas)))

			snap, err := util.SnapshotPods(suiteCtx, k8sClient, evalNamespace(), isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			enginePods := snap.ByComponent[v1beta1.EngineComponent]
			g.Expect(enginePods).To(HaveLen(replicas), "no old or surge pods may remain")
			g.Expect(snap.Serving(v1beta1.EngineComponent)).To(Equal(replicas))
			currentHash := workloadquery.RevisionFromName(lifecycle.CurrentRevision).Hash()
			g.Expect(currentHash).NotTo(BeEmpty())
			podsByIndex := make(map[string]int, replicas)
			for _, pod := range enginePods {
				g.Expect(pod.DeletionTimestamp).To(BeNil())
				g.Expect(pod.Labels[workloadquery.LabelRevisionHash]).To(Equal(currentHash))
				index, found := pod.Labels[workloadquery.LabelInstanceIdx]
				g.Expect(found).To(BeTrue())
				podsByIndex[index]++
			}
			g.Expect(podsByIndex).To(Equal(map[string]int{"0": 1, "1": 1}))

			irs := listEvalIRs(g, isvcName)
			g.Expect(irs).To(HaveLen(1))
			if len(irs) != 1 {
				return
			}
			ir := irs[0]
			g.Expect(ir.Status.CurrentRevision).To(Equal(lifecycle.CurrentRevision))
			g.Expect(ir.Status.UpdateRevision).To(Equal(lifecycle.UpdateRevision))
			g.Expect(ir.Status.Replicas).To(Equal(int32(replicas)))
			g.Expect(ir.Status.ReadyReplicas).To(Equal(int32(replicas)))
			g.Expect(ir.Status.ServingReplicas).To(Equal(int32(replicas)))
			g.Expect(ir.Status.UpdatedReplicas).To(Equal(int32(replicas)))
			g.Expect(ir.Status.UpdatedReadyReplicas).To(Equal(int32(replicas)))
			rows, err := util.DecodedInstanceStatuses(&ir)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(rows).To(HaveLen(replicas))
			instanceIndexes := make([]int32, 0, replicas)
			for _, status := range rows {
				instanceIndexes = append(instanceIndexes, status.Index)
				g.Expect(status.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(status.RunningRevision).To(Equal(lifecycle.CurrentRevision))
				g.Expect(status.PodCount).To(Equal(int32(1)))
				g.Expect(status.ServingPodCount).To(Equal(int32(1)))
				g.Expect(status.AvailablePodCount).To(Equal(int32(1)))
				g.Expect(status.Admitted).To(BeTrue())
				g.Expect(status.ReadyPodCount).To(BeZero())
				g.Expect(status.ScheduledPodCount).To(BeZero())
				g.Expect(status.NodesOccupied).To(BeNil())
			}
			g.Expect(instanceIndexes).To(ConsistOf(int32(0), int32(1)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		sampleMu.Lock()
		observedPeak := peak
		observedMinServing := minServing
		observedSamples := samples
		sampleMu.Unlock()
		Expect(observedSamples).To(BeNumerically(">", 0))
		Expect(observedPeak).To(BeNumerically(">", replicas),
			"expected the fleet to surge above %d at some point; peak was %d", replicas, observedPeak)
		Expect(observedMinServing).To(BeNumerically(">=", replicas),
			"maxUnavailable=0 requires a serving floor of %d; minimum was %d", replicas, observedMinServing)
	})
})

// freshEvalISVC re-reads the ISVC under test from the cluster.
func freshEvalISVC(g Gomega, name string) *v1beta1.InferenceService {
	out := &v1beta1.InferenceService{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx,
		client.ObjectKey{Namespace: evalNamespace(), Name: name}, out)).To(Succeed())
	return out
}

func p19InferenceService(namespace string) *v1beta1.InferenceService {
	two := 2
	maxSurge := intstr.FromInt32(1)
	maxUnavailable := intstr.FromInt32(0)
	return v1beta1testing.MakeInferenceService("p19", namespace).
		Model(fixtureModel).
		Runtime(fixtureRuntime).
		AutoSync(true).
		Engine(&v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: &two,
				MaxReplicas: two,
				Annotations: map[string]string{
					constants.DeploymentMode: string(constants.OMENative),
				},
				Lifecycle: &v1beta1.LifecycleSpec{
					UpdateStrategy: &v1beta1.UpdateStrategy{
						Type: v1beta1.UpdateStrategySurgeThenDrain,
						RollingUpdate: &v1beta1.RollingUpdate{
							MaxSurge:       &maxSurge,
							MaxUnavailable: &maxUnavailable,
						},
					},
				},
			},
		}).
		Obj()
}
