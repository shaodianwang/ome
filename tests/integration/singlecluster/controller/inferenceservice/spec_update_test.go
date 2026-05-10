package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	v1beta1testing "github.com/sgl-project/ome/pkg/utils/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// ISVC update flow: the controller is supposed to PROPAGATE changes
// to the live child resources, not just create them once. These specs
// mutate an existing ISVC and assert the corresponding Deployment +
// HPA pick up the new values.
//
// The reconciliation path for updates is reconcilers/deployment/
// deployment_reconciler.go:209 (client.Update) for Deployments and
// reconcilers/hpa/hpa_reconciler.go:177 for HPAs.
var _ = ginkgo.Describe("InferenceService update flow", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-update-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("changing Engine.MinReplicas/MaxReplicas → HPA bounds update in place", func() {
		ginkgo.By("creating BaseModel + Runtime baseline")
		cbm := v1beta1testing.MakeClusterBaseModel("update-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("update-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:v1"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating ISVC with Engine.MinReplicas=2 + MaxReplicas=5")
		minR1, maxR1 := 2, 5
		isvc := v1beta1testing.MakeInferenceService("update-isvc", ns.Name).
			Model("update-llama").
			Runtime("update-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minR1,
					MaxReplicas: maxR1,
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("waiting for the initial HPA with min=2 max=5")
		Eventually(func(g Gomega) {
			hpas := &autoscalingv2.HorizontalPodAutoscalerList{}
			g.Expect(k8sClient.List(suiteCtx, hpas, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(hpas.Items).To(HaveLen(1))
			g.Expect(*hpas.Items[0].Spec.MinReplicas).To(BeNumerically("==", int32(minR1)))
			g.Expect(hpas.Items[0].Spec.MaxReplicas).To(BeNumerically("==", int32(maxR1)))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("mutating ISVC to MinReplicas=3 + MaxReplicas=8")
		minR2, maxR2 := 3, 8
		gotISVC := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "update-isvc", Namespace: ns.Name}, gotISVC)).To(Succeed())
		gotISVC.Spec.Engine.MinReplicas = &minR2
		gotISVC.Spec.Engine.MaxReplicas = maxR2
		Expect(k8sClient.Update(suiteCtx, gotISVC)).To(Succeed())

		ginkgo.By("expecting the EXISTING HPA's bounds to update in place (no new HPA)")
		Eventually(func(g Gomega) {
			hpas := &autoscalingv2.HorizontalPodAutoscalerList{}
			g.Expect(k8sClient.List(suiteCtx, hpas, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(hpas.Items).To(HaveLen(1),
				"update flow must mutate the existing HPA, not create a new one")
			g.Expect(*hpas.Items[0].Spec.MinReplicas).To(BeNumerically("==", int32(minR2)),
				"HPA.Spec.MinReplicas must reflect the updated Engine.MinReplicas")
			g.Expect(hpas.Items[0].Spec.MaxReplicas).To(BeNumerically("==", int32(maxR2)),
				"HPA.Spec.MaxReplicas must reflect the updated Engine.MaxReplicas")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("changing the runtime image → existing Deployment's container image updates in place", func() {
		ginkgo.By("creating BaseModel + Runtime baseline (initial image v1)")
		cbm := v1beta1testing.MakeClusterBaseModel("img-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("img-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:v1"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("img-isvc", ns.Name).
			Model("img-llama").
			Runtime("img-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("waiting for the initial Deployment with image=v1")
		var depName, depNamespace string
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(1))
			containers := deps.Items[0].Spec.Template.Spec.Containers
			g.Expect(containers).NotTo(BeEmpty())
			g.Expect(containers[0].Image).To(Equal("ghcr.io/test/runtime:v1"))
			depName = deps.Items[0].Name
			depNamespace = deps.Items[0].Namespace
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("rolling the runtime to image v2 (engineConfig.runner.image swap)")
		// Updating the CSR triggers an ISVC reconcile via the
		// runtime watch (controller registers a watch for CSR with
		// no-op enqueue → cache update → ISVC's next reconcile picks
		// up the new image during MergeRuntimeSpecs).
		Eventually(func(g Gomega) {
			gotCSR := &v1beta1.ClusterServingRuntime{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: "img-runtime"}, gotCSR)).To(Succeed())
			gotCSR.Spec.EngineConfig.Runner.Image = "ghcr.io/test/runtime:v2"
			err := k8sClient.Update(suiteCtx, gotCSR)
			g.Expect(err).To(Or(BeNil(), Satisfy(apierrors.IsConflict)))
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Force an ISVC requeue by touching an annotation. In a real
		// cluster the runtime watch handles this; in envtest we
		// remove the lag by nudging the ISVC directly.
		ginkgo.By("triggering an ISVC reconcile by updating an inert annotation")
		Eventually(func(g Gomega) {
			gotISVC := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "img-isvc", Namespace: ns.Name}, gotISVC)).To(Succeed())
			if gotISVC.Annotations == nil {
				gotISVC.Annotations = map[string]string{}
			}
			gotISVC.Annotations["ome.io/test-trigger"] = "v2"
			err := k8sClient.Update(suiteCtx, gotISVC)
			g.Expect(err).To(Or(BeNil(), Satisfy(apierrors.IsConflict)))
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting the SAME Deployment to flip to image=v2")
		Eventually(func(g Gomega) {
			dep := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: depName, Namespace: depNamespace}, dep)).To(Succeed())
			containers := dep.Spec.Template.Spec.Containers
			g.Expect(containers).NotTo(BeEmpty())
			g.Expect(containers[0].Image).To(Equal("ghcr.io/test/runtime:v2"),
				"Deployment must update in place when the runtime's runner image changes")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
