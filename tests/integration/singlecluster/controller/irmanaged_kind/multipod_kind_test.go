package irmanaged_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// OMENative multi-pod (Leader + Worker) gang on the IR-managed default path —
// the non-LWS multi-node mechanism. The projector emits leader + worker Runners
// and the inferencereplica controller materializes the gang directly, so a real
// kubelet runs leader + N workers with no LeaderWorkerSet operator.
var _ = ginkgo.Describe("OMENative multi-pod gang", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "ir-mp-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		_ = util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)
	})

	ginkgo.It("a leader + 2 workers gang all reach Ready with ordinals 0,1,2", func() {
		const workerSize = 2 // 1 leader (ordinal 0) + 2 workers (ordinals 1,2)
		cbm := kindfix.StandardClusterBaseModel("ir-mp-cbm", kindfix.StandardModelStorageURI)
		csr := kindfix.StandardClusterServingRuntime("ir-mp-csr")
		kindfix.ApplyModelRuntime(suiteCtx, Default, k8sClient, cbm, csr, kindfix.ApplyModelRuntimeOpts{
			CachedReader: kindutil.Suite().Manager.GetClient(),
		})

		container := corev1.Container{Name: constants.MainContainerName, Image: "fake-serving:v1"}
		size := workerSize
		engine := &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
			},
			PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{container}},
			Leader: &v1beta1.LeaderSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{container}},
			},
			Worker: &v1beta1.WorkerSpec{
				Size:    &size,
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{container}},
			},
		}
		isvc := v1beta1testing.MakeInferenceService("ir-mp-isvc", ns.Name).
			Model("ir-mp-cbm").
			Runtime("ir-mp-csr").
			DeploymentMode(constants.OMENative).
			Engine(engine).
			Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			kindfix.CleanupModelRuntime(suiteCtx, k8sClient, cbm, csr, isvc)
		})

		ginkgo.By("the gang materializes 1 leader + 2 worker pods, all containers Ready")
		Eventually(func(g Gomega) {
			pods := listGangPods(g, ns.Name, "ir-mp-isvc")
			g.Expect(pods).To(HaveLen(workerSize + 1))
			byRunner := map[string]int{}
			for i := range pods {
				g.Expect(omeContainerReady(&pods[i])).To(BeTrue(), "pod %s ome-container not Ready", pods[i].Name)
				byRunner[pods[i].Labels[query.LabelRunner]]++
			}
			g.Expect(byRunner[string(v1beta1.RunnerNameLeader)]).To(Equal(1), "expected 1 leader pod, runners=%v", byRunner)
			g.Expect(byRunner[string(v1beta1.RunnerNameWorker)]).To(Equal(workerSize), "expected %d worker pods, runners=%v", workerSize, byRunner)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the IR-managed path emits a PodGroup for the gang (minMember = leader + workers)")
		Eventually(func(g Gomega) {
			pgs := &schedulingv1alpha1.PodGroupList{}
			g.Expect(k8sClient.List(suiteCtx, pgs, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(pgs.Items).NotTo(BeEmpty(), "no PodGroup created — multi-node gang has no gang reference")
			found := false
			for i := range pgs.Items {
				if pgs.Items[i].Spec.MinMember == int32(workerSize+1) {
					found = true
				}
			}
			g.Expect(found).To(BeTrue(), "expected a PodGroup with minMember=%d (1 leader + %d workers)", workerSize+1, workerSize)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("a multi-node gang rolls to a new revision atomically per Instance", func() {
		const workerSize, instances = 1, 2 // 2 Instances x (1 leader + 1 worker) = 4 pods
		podsPerInstance := workerSize + 1
		cbm := kindfix.StandardClusterBaseModel("ir-gang-cbm", kindfix.StandardModelStorageURI)
		csr := kindfix.StandardClusterServingRuntime("ir-gang-csr")
		kindfix.ApplyModelRuntime(suiteCtx, Default, k8sClient, cbm, csr, kindfix.ApplyModelRuntimeOpts{
			CachedReader: kindutil.Suite().Manager.GetClient(),
		})

		isvc := v1beta1testing.MakeInferenceService("ir-gang-isvc", ns.Name).
			Model("ir-gang-cbm").
			Runtime("ir-gang-csr").
			DeploymentMode(constants.OMENative).
			Engine(gangEngineSpec("fake-serving:v1", workerSize, instances)).
			Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			kindfix.CleanupModelRuntime(suiteCtx, k8sClient, cbm, csr, isvc)
		})

		ginkgo.By("initial gang Ready: all pods on a single revision")
		var baseRev string
		Eventually(func(g Gomega) {
			pods := listGangPods(g, ns.Name, "ir-gang-isvc")
			g.Expect(pods).To(HaveLen(instances * podsPerInstance))
			revs := map[string]bool{}
			for i := range pods {
				g.Expect(omeContainerReady(&pods[i])).To(BeTrue())
				revs[pods[i].Labels[query.LabelRevisionHash]] = true
			}
			g.Expect(revs).To(HaveLen(1), "gang not on a single revision yet: %v", revs)
			for r := range revs {
				baseRev = r
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("bumping the engine image to trigger a gang roll")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), fresh)).To(Succeed())
			fresh.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			fresh.Spec.Engine.Leader.PodSpec.Containers[0].Image = "fake-serving:v2"
			fresh.Spec.Engine.Worker.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("each Instance flips revision as a unit (no sustained mixed-revision gang)")
		worst := sampleGangRoll(ns.Name, "ir-gang-isvc", baseRev, instances, podsPerInstance, kindLongTimeout)
		Expect(worst).To(BeNumerically("<", gangAtomicityGrace),
			"an Instance held leader+worker on mixed revisions for %s (>= %s grace) — the gang rolled piecemeal, not atomically", worst, gangAtomicityGrace)
	})
})

// listGangPods returns the OMENative-managed pods of an ISVC in a namespace.
func listGangPods(g Gomega, namespace, isvcName string) []corev1.Pod {
	pods := &corev1.PodList{}
	g.Expect(k8sClient.List(suiteCtx, pods,
		client.InNamespace(namespace),
		client.MatchingLabels{
			util.LabelManagedByKey:                util.ManagedByOMENativeValue,
			constants.InferenceServicePodLabelKey: isvcName,
		},
	)).To(Succeed())
	return pods.Items
}

// omeContainerReady reports whether the pod's ome-container is Ready.
func omeContainerReady(p *corev1.Pod) bool {
	for i := range p.Status.ContainerStatuses {
		if p.Status.ContainerStatuses[i].Name == constants.MainContainerName {
			return p.Status.ContainerStatuses[i].Ready
		}
	}
	return false
}

// gangAtomicityGrace is how long an Instance may hold alive pods on more than one
// revision during a roll before it counts as a piecemeal (non-atomic) gang flip.
// Generous for KIND pod-recreate latency; an atomic recreate keeps it near zero.
const gangAtomicityGrace = 12 * time.Second

// gangEngineSpec builds a multi-node engine: each Instance is 1 leader +
// workerSize workers, minReplicas Instances. Leader and worker run the same
// container so the fake-serving image satisfies both runners.
func gangEngineSpec(image string, workerSize, minReplicas int) *v1beta1.EngineSpec {
	c := corev1.Container{Name: constants.MainContainerName, Image: image}
	size := workerSize
	mr := minReplicas
	return &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
			MinReplicas: &mr,
		},
		PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{c}},
		Leader:  &v1beta1.LeaderSpec{PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{c}}},
		Worker:  &v1beta1.WorkerSpec{Size: &size, PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{c}}},
	}
}

// sampleGangRoll polls the gang until it converges to a single new revision and
// returns the longest any one Instance simultaneously held alive pods on more
// than one revision — the gang-atomicity span. An atomic recreate keeps this
// near the pod-recreate transient; a piecemeal roll (leader flips while workers
// lag) holds an Instance mixed for the whole lag. Fails the spec if the gang
// never converges within budget.
func sampleGangRoll(namespace, isvcName, baseRev string, instances, podsPerInstance int, budget time.Duration) time.Duration {
	deadline := time.Now().Add(budget)
	mixedSince := map[string]time.Time{}
	var worst time.Duration
	for {
		pods := &corev1.PodList{}
		Expect(k8sClient.List(suiteCtx, pods, client.InNamespace(namespace), client.MatchingLabels{
			util.LabelManagedByKey:                util.ManagedByOMENativeValue,
			constants.InferenceServicePodLabelKey: isvcName,
		})).To(Succeed())

		revsByInst := map[string]map[string]bool{}
		readyNew := 0
		for i := range pods.Items {
			p := &pods.Items[i]
			if p.DeletionTimestamp != nil {
				continue // count only alive pods
			}
			inst := p.Labels[query.LabelInstanceIdx]
			rev := p.Labels[query.LabelRevisionHash]
			if revsByInst[inst] == nil {
				revsByInst[inst] = map[string]bool{}
			}
			revsByInst[inst][rev] = true
			if rev != baseRev && rev != "" && omeContainerReady(p) {
				readyNew++
			}
		}

		now := time.Now()
		converged := len(revsByInst) == instances && readyNew == instances*podsPerInstance
		for inst, revs := range revsByInst {
			if len(revs) > 1 {
				if _, ok := mixedSince[inst]; !ok {
					mixedSince[inst] = now
				}
				if span := now.Sub(mixedSince[inst]); span > worst {
					worst = span
				}
			} else {
				delete(mixedSince, inst)
			}
			if len(revs) != 1 || revs[baseRev] {
				converged = false
			}
		}
		if converged {
			return worst
		}
		Expect(now.Before(deadline)).To(BeTrue(),
			"gang did not converge to a new revision within %s (worst mixed span %s)", budget, worst)
		time.Sleep(500 * time.Millisecond)
	}
}
