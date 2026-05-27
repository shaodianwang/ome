package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Lean path: spec.model omitted, spec.runtime named directly. The
// controller skips model fetch + runtime auto-select + runtime-vs-model
// validation, and reconciles using the runtime spec as-is.
var _ = ginkgo.Describe("InferenceService lean path (runtime only, no model)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-lean-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("ISVC without spec.model reconciles to a Deployment using the named runtime's image", func() {
		csr := v1beta1testing.MakeClusterServingRuntime("lean-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  "ome-container",
				Image: "ghcr.io/test/runtime:lean",
				Ports: []corev1.ContainerPort{{ContainerPort: 8080, Name: "http1"}},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("lean-isvc", ns.Name).
			Runtime("lean-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		// No .Model(...) call — spec.model stays nil.
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting a Service whose selector matches the lean ISVC")
		Eventually(func(g Gomega) {
			services := &corev1.ServiceList{}
			g.Expect(k8sClient.List(suiteCtx, services, client.InNamespace(ns.Name))).To(Succeed())
			matched := false
			for _, s := range services.Items {
				if s.Spec.Selector["ome.io/inferenceservice"] == "lean-isvc" {
					matched = true
					break
				}
			}
			g.Expect(matched).To(BeTrue(), "lean ISVC must produce a Service")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Regression: previously the controller stamped
	// status.modelStatus.{transitionStatus=InProgress,
	// modelRevisionStates.targetModelState=Pending} on every lean ISVC
	// because PropagateModelStatus ran unconditionally. The block never
	// advanced past "InProgress" because no model was being loaded — it
	// just lied to operators inspecting status. Fix: skip the modelStatus
	// writer entirely when spec.model is nil.
	//
	// Repro reference: bug report against image bf0641b applied a
	// runtime-only ISVC and saw `status.modelStatus.transitionStatus =
	// "InProgress"` stuck forever despite Ready=True.
	ginkgo.It("ISVC without spec.model leaves status.modelStatus unset (no 'InProgress' lie)", func() {
		csr := v1beta1testing.MakeClusterServingRuntime("lean-runtime-status").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  "ome-container",
				Image: "ghcr.io/test/runtime:lean",
				Ports: []corev1.ContainerPort{{ContainerPort: 8080, Name: "http1"}},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("lean-status-isvc", ns.Name).
			Runtime("lean-runtime-status").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("waiting for the controller to observe the lean ISVC at least once")
		// Use the produced Service as the synchronization signal — once
		// we see it, we know UpdateComponentStatus has run, which is
		// the precise codepath that used to leak modelStatus values.
		Eventually(func(g Gomega) {
			services := &corev1.ServiceList{}
			g.Expect(k8sClient.List(suiteCtx, services, client.InNamespace(ns.Name))).To(Succeed())
			matched := false
			for _, s := range services.Items {
				if s.Spec.Selector["ome.io/inferenceservice"] == "lean-status-isvc" {
					matched = true
					break
				}
			}
			g.Expect(matched).To(BeTrue())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("asserting modelStatus is NOT populated with misleading 'InProgress' state")
		// Use Consistently: even after the controller runs UpdateComponentStatus
		// repeatedly, modelStatus must remain untouched. With the kubebuilder
		// `default=UpToDate` marker removed and the controller guard in place,
		// the field stays at its zero value.
		Consistently(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "lean-status-isvc", Namespace: ns.Name}, got)).To(Succeed())
			g.Expect(got.Status.ModelStatus.TransitionStatus).NotTo(
				Equal(v1beta1.InProgress),
				"status.modelStatus.transitionStatus must NOT be 'InProgress' for a lean (no-model) ISVC — there is no model load in progress",
			)
			g.Expect(got.Status.ModelStatus.TransitionStatus).NotTo(
				Equal(v1beta1.BlockedByFailedLoad),
				"status.modelStatus.transitionStatus must NOT be 'BlockedByFailedLoad' for a lean ISVC",
			)
			g.Expect(got.Status.ModelStatus.ModelRevisionStates).To(
				BeNil(),
				"status.modelStatus.modelRevisionStates must NOT be populated for a lean ISVC",
			)
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())
	})

	// Regression guard for the with-model path: removing the
	// kubebuilder default + guarding the writer must not break the
	// lifecycle for ISVCs that DO declare a model. The controller still
	// drives Pending → InProgress → UpToDate. We can only observe up to
	// Pending/InProgress in envtest (no real kubelet to mark pods
	// Ready), so we assert that the lifecycle starts moving — that's
	// enough to catch a regression where modelStatus stops being
	// populated entirely for valid model ISVCs.
	ginkgo.It("ISVC with spec.model still populates status.modelStatus lifecycle", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("lean-cbm-regression").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("with-model-rt").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  "ome-container",
				Image: "ghcr.io/test/runtime:with-model",
				Ports: []corev1.ContainerPort{{ContainerPort: 8080, Name: "http1"}},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("with-model-isvc", ns.Name).
			Model("lean-cbm-regression").
			Runtime("with-model-rt").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("waiting for the model-loading lifecycle to be populated")
		Eventually(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "with-model-isvc", Namespace: ns.Name}, got)).To(Succeed())
			// Before any pods come up, transitionStatus = InProgress
			// and modelRevisionStates.targetModelState = Pending.
			// This is the correct behavior for a model-bearing ISVC
			// and we must NOT regress to leaving it unset.
			g.Expect(got.Status.ModelStatus.TransitionStatus).To(
				Equal(v1beta1.InProgress),
				"with-model ISVC must still report 'InProgress' while pods are coming up",
			)
			g.Expect(got.Status.ModelStatus.ModelRevisionStates).NotTo(BeNil(),
				"with-model ISVC must populate modelRevisionStates")
			g.Expect(got.Status.ModelStatus.ModelRevisionStates.TargetModelState).To(
				Equal(v1beta1.Pending),
				"with-model ISVC targetModelState should be 'Pending' before any pods are observed",
			)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

})
