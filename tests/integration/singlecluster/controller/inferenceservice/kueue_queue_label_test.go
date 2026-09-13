package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// A Component is placed under Kueue admission control only when its pods hold
// accelerator capacity. These specs drive the whole path against a real
// apiserver — the manager's --accelerator-resources list → dispatch site →
// projector → the InferenceReplica and its pod templates — which is what proves
// the deploy-time flag actually reaches the decision. The queue-name label is
// written the way the multi-cluster placer writes it, onto
// spec.<component>.labels.
//
// The engine holds TPU rather than the one vendor OME recognized on its own
// before the flag existed, so the spec fails if the decision is ever taken on a
// built-in default instead of on the configured list.
var _ = ginkgo.Describe("InferenceService Kueue queue-name label", ginkgo.Ordered, func() {
	const queue = "team-a"
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-queue-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("gates the accelerator engine and leaves the cpu-only router ungated", func() {
		ginkgo.By("creating a runtime whose engine holds TPU chips and whose router does not")
		cbm := v1beta1testing.MakeClusterBaseModel("queue-llama").
			StorageURI("oci://n/example/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("queue-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  constants.MainContainerName,
				Image: "example.com/runtime:engine",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						constants.GoogleTPUResourceType: resource.MustParse("4"),
					},
				},
			}).
			RouterRunner(corev1.Container{
				Name:  constants.MainContainerName,
				Image: "example.com/runtime:router",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating an OMENative ISVC with the queue-name label on both components")
		queueLabels := func() map[string]string {
			return map[string]string{constants.KueueQueueLabelKey: queue}
		}
		isvc := v1beta1testing.MakeInferenceService("queue-isvc", ns.Name).
			Model("queue-llama").
			Runtime("queue-runtime").
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Labels: queueLabels()},
			}).
			Router(&v1beta1.RouterSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Labels: queueLabels()},
			}).Obj()
		// spec.deploymentMode, not the top-level annotation: only the
		// per-Component annotation and this field are in the resolution chain,
		// and both components must land on OMENative to be projected as IRs.
		isvc.Spec.DeploymentMode = ptr.To(constants.OMENative)
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting the engine InferenceReplica and its pod template to keep the label")
		Eventually(func(g Gomega) {
			ir := getInferenceReplica(g, ns.Name, "queue-isvc-engine")
			g.Expect(ir.Labels).To(HaveKeyWithValue(constants.KueueQueueLabelKey, queue),
				"a TPU engine must stay under Kueue admission control")
			g.Expect(ir.Spec.Runners).NotTo(BeEmpty())
			for _, r := range ir.Spec.Runners {
				g.Expect(r.Template.ObjectMeta.Labels).To(
					HaveKeyWithValue(constants.KueueQueueLabelKey, queue),
					"engine runner %q template must carry the queue-name label", r.Name)
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting the router InferenceReplica and its pod template to have dropped it")
		Eventually(func(g Gomega) {
			ir := getInferenceReplica(g, ns.Name, "queue-isvc-router")
			g.Expect(ir.Labels).NotTo(HaveKey(constants.KueueQueueLabelKey),
				"a cpu-only router charges no accelerator budget, so it must not claim a queue")
			g.Expect(ir.Spec.Runners).NotTo(BeEmpty())
			for _, r := range ir.Spec.Runners {
				g.Expect(r.Template.ObjectMeta.Labels).NotTo(
					HaveKey(constants.KueueQueueLabelKey),
					"router runner %q template must not carry the queue-name label", r.Name)
				g.Expect(r.Template.ObjectMeta.Labels).To(
					HaveKeyWithValue(constants.InferenceServicePodLabelKey, "queue-isvc"),
					"router runner %q template must keep every other label", r.Name)
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// getInferenceReplica reads the per-Component InferenceReplica, failing the
// enclosing Eventually while the ISVC reconciler has yet to project it.
func getInferenceReplica(g Gomega, namespace, name string) *v1beta1.InferenceReplica {
	ir := &v1beta1.InferenceReplica{}
	g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: namespace, Name: name}, ir)).To(Succeed())
	return ir
}
