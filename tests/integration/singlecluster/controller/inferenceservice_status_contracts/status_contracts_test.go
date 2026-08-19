package inferenceservicestatuscontracts

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// The IR controller does not run in this suite, so specs write
// IR.Status by hand and the assertions isolate the ISVC controller's
// projection layer: IR.Status → ISVC.Status.Components[engine].Lifecycle
// plus the derived EngineReady top-level condition.
var _ = ginkgo.Describe("InferenceService OMENative status contracts", ginkgo.Ordered, func() {
	var (
		ns      *corev1.Namespace
		isvc    *v1beta1.InferenceService
		irKey   types.NamespacedName
		isvcKey types.NamespacedName
	)

	// setIRStatus rewrites the engine IR's status subresource with the
	// given mutation applied to a fresh read, retrying on conflict.
	setIRStatus := func(mutate func(*v1beta1.InferenceReplica)) {
		Eventually(func(g Gomega) {
			ir := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, irKey, ir)).To(Succeed())
			mutate(ir)
			g.Expect(k8sClient.Status().Update(suiteCtx, ir)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	}

	ginkgo.BeforeAll(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "status-c-")
		Expect(err).NotTo(HaveOccurred())

		cbm := v1beta1testing.MakeClusterBaseModel("status-c-proj-model").
			StorageURI("oci://n/test/b/models/o/status-c-proj").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("status-c-proj-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  constants.MainContainerName,
				Image: "example.com/runtime:test",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		minReplicas := 2
		isvc = v1beta1testing.MakeInferenceService("status-c-proj", ns.Name).
			Model(cbm.Name).
			Runtime(csr.Name).
			Engine(&v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: &minReplicas,
				Autoscaler:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			}}).Obj()
		omeNativeMode := constants.OMENative
		isvc.Spec.DeploymentMode = &omeNativeMode
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		isvcKey = client.ObjectKeyFromObject(isvc)
		irKey = types.NamespacedName{Namespace: ns.Name, Name: isvc.Name + "-engine"}

		ginkgo.By("waiting for the projector to create the engine InferenceReplica")
		Eventually(func(g Gomega) {
			ir := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, irKey, ir)).To(Succeed())
			g.Expect(ir.Spec.Replicas).NotTo(BeNil())
			g.Expect(*ir.Spec.Replicas).To(Equal(int32(2)))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.AfterAll(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("projects IR.Status 1:1 onto the ISVC Lifecycle subtree, conditions included", func() {
		now := metav1.Now()
		setIRStatus(func(ir *v1beta1.InferenceReplica) {
			ir.Status.ObservedGeneration = ir.Generation
			ir.Status.Replicas = 2
			ir.Status.ReadyReplicas = 1
			ir.Status.ServingReplicas = 1
			ir.Status.AvailableReplicas = 1
			ir.Status.UpdatedReplicas = 1
			ir.Status.UpdatedReadyReplicas = 1
			ir.Status.CurrentRevision = ir.Name + "-aaaa1111"
			ir.Status.UpdateRevision = ir.Name + "-bbbb2222"
			ir.Status.LabelSelector = "app=status-c-proj,component=engine"
			ir.Status.Conditions = []metav1.Condition{{
				Type:               "Ready",
				Status:             metav1.ConditionUnknown,
				Reason:             "RolloutInProgress",
				Message:            "Rolling out",
				LastTransitionTime: now,
				ObservedGeneration: ir.Generation,
			}}
		})

		ir := &v1beta1.InferenceReplica{}
		Expect(k8sClient.Get(suiteCtx, irKey, ir)).To(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, isvcKey, fresh)).To(Succeed())
			lc := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle
			g.Expect(lc).NotTo(BeNil(), "the IR-managed engine must carry a Lifecycle subtree")
			g.Expect(lc.ObservedGeneration).To(Equal(ir.Generation),
				"Lifecycle.ObservedGeneration must mirror the IR's observed generation")
			g.Expect(lc.Replicas).To(Equal(int32(2)))
			g.Expect(lc.ReadyReplicas).To(Equal(int32(1)))
			g.Expect(lc.ServingReplicas).To(Equal(int32(1)))
			g.Expect(lc.AvailableReplicas).To(Equal(int32(1)))
			g.Expect(lc.UpdatedReplicas).To(Equal(int32(1)))
			g.Expect(lc.UpdatedReadyReplicas).To(Equal(int32(1)))
			g.Expect(lc.CurrentRevision).To(Equal(ir.Name + "-aaaa1111"))
			g.Expect(lc.UpdateRevision).To(Equal(ir.Name + "-bbbb2222"))
			g.Expect(lc.LabelSelector).To(Equal("app=status-c-proj,component=engine"))
			g.Expect(lc.Conditions).To(HaveLen(1),
				"the IR's own conditions must ride along on the Lifecycle subtree")
			g.Expect(lc.Conditions[0].Type).To(Equal("Ready"))
			g.Expect(lc.Conditions[0].Status).To(Equal(metav1.ConditionUnknown))
			g.Expect(lc.Conditions[0].Reason).To(Equal("RolloutInProgress"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("derives EngineReady from the projected counters: partial serving below the strict floor is False", func() {
		// State carried over: 1/2 serving with no lifecycle budget → the
		// strict availability floor (2) is unmet.
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, isvcKey, fresh)).To(Succeed())
			cond := fresh.Status.GetCondition(v1beta1.EngineReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(corev1.ConditionFalse))
			g.Expect(cond.Reason).To(Equal("InsufficientAvailable"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("flips EngineReady True/Ready when every Instance is Ready", func() {
		setIRStatus(func(ir *v1beta1.InferenceReplica) {
			ir.Status.ObservedGeneration = ir.Generation
			ir.Status.Replicas = 2
			ir.Status.ReadyReplicas = 2
			ir.Status.ServingReplicas = 2
			ir.Status.AvailableReplicas = 2
			ir.Status.UpdatedReplicas = 2
			ir.Status.UpdatedReadyReplicas = 2
			ir.Status.CurrentRevision = ir.Name + "-bbbb2222"
			ir.Status.UpdateRevision = ir.Name + "-bbbb2222"
		})

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, isvcKey, fresh)).To(Succeed())
			cond := fresh.Status.GetCondition(v1beta1.EngineReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(corev1.ConditionTrue))
			g.Expect(cond.Reason).To(Equal("Ready"))
			lc := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle
			g.Expect(lc).NotTo(BeNil())
			g.Expect(lc.ReadyReplicas).To(Equal(int32(2)))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("performs zero ISVC writes when a reconcile observes an unchanged IR rollup", func() {
		// Let the projection converge fully before capturing the baseline.
		var baselineRV string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, isvcKey, fresh)).To(Succeed())
			cond := fresh.Status.GetCondition(v1beta1.EngineReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(corev1.ConditionTrue))
			baselineRV = fresh.ResourceVersion
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Nudge the IR with a foreign annotation: the Owns() watch
		// re-enqueues the parent ISVC, but the recomputed rollup equals the
		// live status, so no ISVC write may land.
		Eventually(func(g Gomega) {
			ir := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, irKey, ir)).To(Succeed())
			if ir.Annotations == nil {
				ir.Annotations = map[string]string{}
			}
			ir.Annotations["status-c.example.com/nudge"] = "steady-state"
			g.Expect(k8sClient.Update(suiteCtx, ir)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Consistently(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, isvcKey, fresh)).To(Succeed())
			g.Expect(fresh.ResourceVersion).To(Equal(baselineRV),
				"a no-op rollup must not churn the ISVC (each status write re-triggers the reconciler)")
		}).WithTimeout(15 * util.PollInterval).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("reads NoReplicas-first: a zeroed Component is EngineReady=False/NoReplicas, never 0/0 Ready", func() {
		setIRStatus(func(ir *v1beta1.InferenceReplica) {
			ir.Status.ObservedGeneration = ir.Generation
			ir.Status.Replicas = 0
			ir.Status.ReadyReplicas = 0
			ir.Status.ServingReplicas = 0
			ir.Status.AvailableReplicas = 0
			ir.Status.UpdatedReplicas = 0
			ir.Status.UpdatedReadyReplicas = 0
		})

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, isvcKey, fresh)).To(Succeed())
			cond := fresh.Status.GetCondition(v1beta1.EngineReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(corev1.ConditionFalse),
				"zero desired Instances must not satisfy the ReadyReplicas >= Replicas branch via 0 >= 0")
			g.Expect(cond.Reason).To(Equal("NoReplicas"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
