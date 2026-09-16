package multicluster

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Real-Kueue smoke: prove the member-side admission chain end to end on a
// SINGLE member (member1), with no control-plane fan-out yet. This is the real
// replacement for the sim-path admitDerivedOn fake — instead of writing
// InstanceStatus.Admitted=true directly, we let the member's running managers
// produce it:
//
//	derived-shaped ISVC (OMENative engine, queue-name label, CPU request)
//	  → ISVC reconciler projects an InferenceReplica
//	  → IR reconciler creates engine pod(s) carrying the queue-name label
//	  → Kueue pod webhook GATES the pod (scheduling gate)
//	  → Kueue scheduler UNGATES it once a quota admits it
//	  → IR controller flips InstanceStatus.Admitted = true
//
// Skipped unless OME_MC_REALKUEUE=1 (members are bare otherwise).
var _ = ginkgo.Describe("Real-Kueue member admission (smoke)", ginkgo.Ordered, func() {
	const queueCPURequest = "500m"

	ginkgo.BeforeEach(func() {
		skipUnlessRealKueue()
	})

	// engineQueuedISVC builds a derived-shaped ISVC for the member: lean path
	// (named ClusterServingRuntime, no model), OMENative engine, the Kueue
	// queue-name label stamped on the engine pod metadata (mirroring
	// placement.DeriveISVC → stampQueue), and a small CPU request so Kueue's
	// quota math is non-trivial.
	engineQueuedISVC := func(ns, name, runtime, queue string) *v1beta1.InferenceService {
		engine := &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				// Per-Component OMENative annotation (highest-priority mode
				// selector in DetermineEngineDeploymentMode) so the engine
				// dispatches through the InferenceReplica path and the IR
				// controller creates pods — RawDeployment would emit a
				// Deployment (and Kueue's deployment webhook would intercept it).
				Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
				// This is the load-bearing stamp: it lands on the engine pod
				// metadata and is what Kueue's pod integration keys on to gate.
				Labels: map[string]string{constants.KueueQueueLabelKey: queue},
			},
			PodSpec: v1beta1.PodSpec{
				Containers: []corev1.Container{{
					Name:  "ome-container",
					Image: "example/model-server:test",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse(queueCPURequest),
						},
					},
				}},
			},
		}
		return v1beta1testing.MakeInferenceService(name, ns).
			Runtime(runtime).
			DeploymentMode(constants.OMENative).
			Engine(engine).
			Obj()
	}

	// leanRuntime builds a ClusterServingRuntime that the lean (no-model) path
	// resolves directly: it carries an engine runner so the merged engine spec
	// is valid, and supports the safetensors/Llama shape so a future model-bearing
	// variant would also auto-select it.
	leanRuntime := func(name string) *v1beta1.ClusterServingRuntime {
		return v1beta1testing.MakeClusterServingRuntime(name).
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  "ome-container",
				Image: "example/model-server:test",
			}).Obj()
	}

	enginePods := func(ns, isvcName string) []corev1.Pod {
		pods := &corev1.PodList{}
		gomega.Expect(member1Cluster.client.List(member1Cluster.ctx, pods,
			client.InNamespace(ns),
			client.MatchingLabels{
				util.LabelManagedByKey:          util.ManagedByOMENativeValue,
				constants.InferenceServiceLabel: isvcName,
			})).To(gomega.Succeed())
		return pods.Items
	}

	engineAdmitted := func(ns, isvcName string) bool {
		// The authoritative per-instance status now lives on the component's
		// InferenceReplica (<isvcName>-engine), not the ISVC's LifecycleStatus.
		rows, err := util.GetInstanceStatuses(member1Cluster.ctx, member1Cluster.client,
			types.NamespacedName{Namespace: ns, Name: isvcName + "-" + string(v1beta1.EngineComponent)})
		if err != nil {
			return false
		}
		for _, is := range rows {
			if is.Admitted {
				return true
			}
		}
		return false
	}

	ginkgo.It("admits a queue-labeled engine: pods gated then ungated, InstanceStatus.Admitted flips true", func() {
		ns := uniqueName("rk-admit")
		queue := uniqueName("rk-lq")
		csrName := uniqueName("rk-rt")
		isvcName := uniqueName("rk-isvc")

		createNamespace(member1Cluster, ns)

		ginkgo.By("granting an admissible quota (1 CPU admits the 500m engine pod)")
		gomega.Expect(framework.ApplyKueueQuota(member1Cluster.ctx, member1Cluster.client, ns, queue, "1")).
			To(gomega.Succeed())

		ginkgo.By("creating the lean runtime + derived-shaped OMENative ISVC")
		csr := leanRuntime(csrName)
		gomega.Expect(member1Cluster.client.Create(member1Cluster.ctx, csr)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = member1Cluster.client.Delete(member1Cluster.ctx, csr) })

		isvc := engineQueuedISVC(ns, isvcName, csrName, queue)
		gomega.Expect(member1Cluster.client.Create(member1Cluster.ctx, isvc)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = member1Cluster.client.Delete(member1Cluster.ctx, isvc) })

		ginkgo.By("expecting the member to create at least one engine pod carrying the queue-name label")
		gomega.Eventually(func(g gomega.Gomega) {
			pods := enginePods(ns, isvcName)
			g.Expect(pods).NotTo(gomega.BeEmpty(), "expected engine pod(s)")
			for _, p := range pods {
				g.Expect(p.Labels).To(gomega.HaveKeyWithValue(constants.KueueQueueLabelKey, queue),
					"engine pod must carry the queue-name label")
			}
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		// The pod is created with the Kueue scheduling gate and the scheduler
		// ungates it against quota="1". We do NOT assert on the transient gated
		// state here: with a generous quota the scheduler ungates near-instantly
		// in envtest (sub-poll-interval), so the gate is racy to observe. The
		// negative spec (quota="0") is the authoritative proof that the gate
		// holds; here we assert the admitted END STATE.
		ginkgo.By("observing the engine pod UNGATED once the scheduler admits it against quota")
		gomega.Eventually(func(g gomega.Gomega) {
			pods := enginePods(ns, isvcName)
			g.Expect(pods).NotTo(gomega.BeEmpty())
			for _, p := range pods {
				g.Expect(p.Spec.SchedulingGates).To(gomega.BeEmpty(),
					"engine pod %s should be ungated once admitted", p.Name)
			}
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		ginkgo.By("asserting the derived ISVC engine InstanceStatus.Admitted flips true (driven by real Kueue, not the fake)")
		gomega.Eventually(func() bool {
			return engineAdmitted(ns, isvcName)
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.BeTrue())
	})

	ginkgo.It("blocks a queue-labeled engine under zero quota: pods stay gated, Admitted stays false", func() {
		ns := uniqueName("rk-block")
		queue := uniqueName("rk-lq")
		csrName := uniqueName("rk-rt")
		isvcName := uniqueName("rk-isvc")

		createNamespace(member1Cluster, ns)

		ginkgo.By("granting a zero quota (blocks any non-zero CPU request)")
		gomega.Expect(framework.ApplyKueueQuota(member1Cluster.ctx, member1Cluster.client, ns, queue, "0")).
			To(gomega.Succeed())

		csr := leanRuntime(csrName)
		gomega.Expect(member1Cluster.client.Create(member1Cluster.ctx, csr)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = member1Cluster.client.Delete(member1Cluster.ctx, csr) })

		isvc := engineQueuedISVC(ns, isvcName, csrName, queue)
		gomega.Expect(member1Cluster.client.Create(member1Cluster.ctx, isvc)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = member1Cluster.client.Delete(member1Cluster.ctx, isvc) })

		ginkgo.By("waiting for the gated engine pod to appear")
		gomega.Eventually(func(g gomega.Gomega) {
			pods := enginePods(ns, isvcName)
			g.Expect(pods).NotTo(gomega.BeEmpty())
			gated := false
			for _, p := range pods {
				if len(p.Spec.SchedulingGates) > 0 {
					gated = true
				}
			}
			g.Expect(gated).To(gomega.BeTrue())
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		ginkgo.By("asserting the pod stays gated and Admitted never flips true under zero quota")
		gomega.Consistently(func(g gomega.Gomega) {
			pods := enginePods(ns, isvcName)
			g.Expect(pods).NotTo(gomega.BeEmpty())
			for _, p := range pods {
				g.Expect(p.Spec.SchedulingGates).NotTo(gomega.BeEmpty(),
					"engine pod %s must stay gated under zero quota", p.Name)
			}
			g.Expect(engineAdmitted(ns, isvcName)).To(gomega.BeFalse(),
				"InstanceStatus.Admitted must stay false while gated")
		}, "5s", eventuallyPoll).Should(gomega.Succeed())
	})
})
