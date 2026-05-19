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

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Spec-update edge cases beyond what spec_update_test.go covers
// (replica + image bumps). This file focuses on shape-changing
// updates: removing components, adding components, switching
// autoscaler classes mid-flight, and the observedGeneration
// propagation operators rely on for GitOps reconvergence checks.
var _ = ginkgo.Describe("InferenceService spec update edge cases", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-suedge-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// makePDRuntime registers a CSR that supports both engine and
	// decoder configs so a single ISVC can drive 2 Deployments (the
	// PDDisaggregated mode trigger). Reused by the field-removal and
	// component-addition specs.
	makePDRuntime := func(prefix string) {
		csr := v1beta1testing.MakeClusterServingRuntime(prefix+"-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/engine:test"}).
			DecoderRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/decoder:test"}).
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })
	}

	makeCBM := func(prefix string) {
		cbm := v1beta1testing.MakeClusterBaseModel(prefix + "-llama").
			StorageURI("oci://n/test/b/models/o/" + prefix).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })
	}

	ginkgo.It("removing Decoder from a PDDisaggregated ISVC → decoder Deployment is GC'd by cleanupRemovedComponents", func() {
		// pddisaggregated_test.go covers the create-time shape
		// (Engine + Decoder → 2 Deployments). This spec pins the
		// *shrink* path: removing the Decoder field from an existing
		// ISVC must take down the decoder Deployment via the explicit
		// cleanupRemovedComponents path in controller.go:476 — envtest
		// has no Kubernetes GC so the only force in play is the
		// controller's own deletion code.
		prefix := "sue-dec"
		makeCBM(prefix)
		makePDRuntime(prefix)

		minR, maxR := 1, 1
		isvc := v1beta1testing.MakeInferenceService(prefix+"-isvc", ns.Name).
			Model(prefix + "-llama").
			Runtime(prefix + "-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &minR, MaxReplicas: maxR},
			}).
			Decoder(&v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &minR, MaxReplicas: maxR},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("baseline: 2 Deployments (engine + decoder) exist")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			names := make([]string, 0, len(deps.Items))
			for _, d := range deps.Items {
				names = append(names, d.Name)
			}
			g.Expect(names).To(ContainElements(isvc.Name+"-engine", isvc.Name+"-decoder"))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("remove the Decoder spec from the ISVC")
		Eventually(func(g Gomega) {
			latest := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name, Namespace: ns.Name}, latest)).To(Succeed())
			latest.Spec.Decoder = nil
			g.Expect(k8sClient.Update(suiteCtx, latest)).To(Succeed())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("decoder Deployment is deleted; engine Deployment retained")
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-decoder", Namespace: ns.Name}, &appsv1.Deployment{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"decoder Deployment must be GC'd by cleanupRemovedComponents after Decoder field is removed (got err=%v)", err)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Engine must survive — the cleanup must scope only to
		// components the spec dropped.
		Consistently(func(g Gomega) {
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-engine", Namespace: ns.Name}, &appsv1.Deployment{})).To(Succeed(),
				"engine Deployment must NOT be touched when only the Decoder is removed")
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())
	})

	ginkgo.It("adding Decoder to an Engine-only ISVC → second Deployment appears (incremental component growth)", func() {
		// Inverse of the removal spec: an operator scaling up from
		// a single-component ISVC to PDDisaggregated. The controller
		// must pick up the new Decoder spec on the next reconcile and
		// emit the corresponding Deployment alongside the existing
		// engine. Pins the symmetric add-path that GitOps consumers
		// rely on when graduating an ISVC.
		prefix := "sue-add"
		makeCBM(prefix)
		makePDRuntime(prefix)

		minR, maxR := 1, 1
		isvc := v1beta1testing.MakeInferenceService(prefix+"-isvc", ns.Name).
			Model(prefix + "-llama").
			Runtime(prefix + "-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &minR, MaxReplicas: maxR},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("baseline: only the engine Deployment exists")
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-engine", Namespace: ns.Name}, &appsv1.Deployment{})).To(Succeed())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Consistently(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-decoder", Namespace: ns.Name}, &appsv1.Deployment{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "no decoder Deployment yet (spec doesn't have one)")
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())

		ginkgo.By("add a Decoder spec to the ISVC")
		Eventually(func(g Gomega) {
			latest := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name, Namespace: ns.Name}, latest)).To(Succeed())
			latest.Spec.Decoder = &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &minR, MaxReplicas: maxR},
			}
			g.Expect(k8sClient.Update(suiteCtx, latest)).To(Succeed())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("decoder Deployment now exists alongside engine")
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-decoder", Namespace: ns.Name}, &appsv1.Deployment{})).To(Succeed())
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-engine", Namespace: ns.Name}, &appsv1.Deployment{})).To(Succeed())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("autoscalerClass HPA (default) → external mid-flight → existing HPA is deleted", func() {
		// Default-HPA produces an HPA on create (per spec_update_test
		// already). The mid-flight flip to external runs through
		// hpa_reconciler.go:179 (client.Delete on the existing HPA
		// when shouldCreateHPA returns false). Pins the cleanup half
		// of the autoscaler lifecycle for the HPA→external direction —
		// the existing autoscaler_class_test covers only the
		// create-time "no HPA" case for external, and the
		// autoscaler_extensions_test covers the KEDA→external case.
		prefix := "sue-asflip"
		makeCBM(prefix)

		csr := v1beta1testing.MakeClusterServingRuntime(prefix+"-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		minR, maxR := 2, 5
		isvc := v1beta1testing.MakeInferenceService(prefix+"-isvc", ns.Name).
			Model(prefix + "-llama").
			Runtime(prefix + "-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &minR, MaxReplicas: maxR},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("baseline: HPA exists (default autoscalerClass=hpa)")
		Eventually(func(g Gomega) {
			hpas := &autoscalingv2.HorizontalPodAutoscalerList{}
			g.Expect(k8sClient.List(suiteCtx, hpas, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(hpas.Items).NotTo(BeEmpty(),
				"default autoscalerClass=hpa with MinReplicas<MaxReplicas must produce an HPA")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("flip annotation to external")
		Eventually(func(g Gomega) {
			latest := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name, Namespace: ns.Name}, latest)).To(Succeed())
			if latest.Annotations == nil {
				latest.Annotations = map[string]string{}
			}
			latest.Annotations[constants.AutoscalerClass] = string(constants.AutoscalerClassExternal)
			g.Expect(k8sClient.Update(suiteCtx, latest)).To(Succeed())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("HPA is deleted")
		Eventually(func(g Gomega) {
			hpas := &autoscalingv2.HorizontalPodAutoscalerList{}
			g.Expect(k8sClient.List(suiteCtx, hpas, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(hpas.Items).To(BeEmpty(),
				"HPA must be deleted once autoscalerClass flips to external (got %d)", len(hpas.Items))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("switching the explicit Runtime name on an ISVC → Deployment template rebuilt with the new runtime's image", func() {
		// spec_update_test covers updating the IMAGE within a single
		// runtime. This spec exercises a different update path:
		// changing spec.runtime to point at a DIFFERENT CSR. The
		// controller must re-resolve the runtime on the next
		// reconcile and rewrite the engine Deployment's container
		// image to match the new CSR's EngineRunner. Pins runtime-
		// rebinding as a first-class update, which operators use to
		// hot-swap serving stacks (e.g., vllm → sglang) without
		// recreating the ISVC.
		prefix := "sue-rt"
		makeCBM(prefix)

		// Two CSRs, each with a distinct engine container image.
		csrA := v1beta1testing.MakeClusterServingRuntime(prefix+"-rt-a").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime-A:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csrA)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csrA) })

		csrB := v1beta1testing.MakeClusterServingRuntime(prefix+"-rt-b").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime-B:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csrB)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csrB) })

		minR, maxR := 1, 1
		isvc := v1beta1testing.MakeInferenceService(prefix+"-isvc", ns.Name).
			Model(prefix + "-llama").
			Runtime(prefix + "-rt-a").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &minR, MaxReplicas: maxR},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("baseline: Deployment uses runtime A's image")
		Eventually(func(g Gomega) {
			dep := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-engine", Namespace: ns.Name}, dep)).To(Succeed())
			g.Expect(dep.Spec.Template.Spec.Containers).NotTo(BeEmpty())
			g.Expect(dep.Spec.Template.Spec.Containers[0].Image).To(Equal("ghcr.io/test/runtime-A:test"))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("switch spec.runtime to runtime B")
		Eventually(func(g Gomega) {
			latest := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name, Namespace: ns.Name}, latest)).To(Succeed())
			latest.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: prefix + "-rt-b"}
			g.Expect(k8sClient.Update(suiteCtx, latest)).To(Succeed())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Deployment's container image picks up runtime B")
		Eventually(func(g Gomega) {
			dep := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name + "-engine", Namespace: ns.Name}, dep)).To(Succeed())
			g.Expect(dep.Spec.Template.Spec.Containers).NotTo(BeEmpty())
			g.Expect(dep.Spec.Template.Spec.Containers[0].Image).To(Equal("ghcr.io/test/runtime-B:test"),
				"Deployment image must rebind to the new runtime after spec.runtime is changed")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
