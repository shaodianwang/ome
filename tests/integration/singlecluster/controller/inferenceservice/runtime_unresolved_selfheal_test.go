package inferenceservice

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// An unresolvable runtime is a permanent-config condition, not a transient
// error: the controller parks the ISVC with RuntimeReady=False (reason
// RuntimeNotFound), leaves existing children untouched, and the
// ServingRuntime watch fan-out re-triggers the reconcile as soon as a
// usable runtime appears — no error-requeue loop involved.
var _ = ginkgo.Describe("Runtime-unresolved parking and self-heal", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "model-ru-heal-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	getISVC := func(namespace, name string) *v1beta1.InferenceService {
		out := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: namespace, Name: name}, out)).To(Succeed())
		return out
	}

	hasRuntimeReadyCondition := func(isvc *v1beta1.InferenceService, wantStatus corev1.ConditionStatus, wantReason string) bool {
		for _, c := range isvc.Status.Conditions {
			if c.Type != v1beta1.RuntimeReady {
				continue
			}
			if c.Status != wantStatus {
				return false
			}
			if wantReason != "" && c.Reason != wantReason {
				return false
			}
			return true
		}
		return false
	}

	expectEngineImageEventually := func(namespace, wantImage, because string) {
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(namespace))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(1))
			containers := deps.Items[0].Spec.Template.Spec.Containers
			g.Expect(containers).NotTo(BeEmpty())
			g.Expect(containers[0].Image).To(Equal(wantImage), because)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	}

	ginkgo.It("auto-select ISVC with no compatible runtime parks with RuntimeReady=False, then self-heals when one is created", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("model-ru-heal-auto-model").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("model-ru-heal-auto-fmt").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("creating an auto-select ISVC while NO runtime supports the model's format")
		isvc := v1beta1testing.MakeInferenceService("model-ru-heal-auto-isvc", ns.Name).
			Model("model-ru-heal-auto-model").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting RuntimeReady=False / RuntimeNotFound and no Deployment")
		Eventually(func() bool {
			return hasRuntimeReadyCondition(getISVC(ns.Name, isvc.Name), corev1.ConditionFalse, "RuntimeNotFound")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue(),
			"unresolvable auto-selection must park the ISVC with an advisory condition")
		deps := &appsv1.DeploymentList{}
		Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
		Expect(deps.Items).To(BeEmpty())

		ginkgo.By("creating a compatible runtime AFTER the ISVC — watch fan-out must wake the parked ISVC")
		csr := v1beta1testing.MakeClusterServingRuntime("model-ru-heal-auto-csr").
			SupportsModelFormat("model-ru-heal-auto-fmt", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:healed"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		expectEngineImageEventually(ns.Name, "ghcr.io/test/runtime:healed",
			"the late-created runtime must be auto-selected without any manual poke")
		Eventually(func() bool {
			return hasRuntimeReadyCondition(getISVC(ns.Name, isvc.Name), corev1.ConditionTrue, "RuntimeResolved")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue(),
			"RuntimeReady must flip to True once selection succeeds")
	})

	ginkgo.It("runtime-only ISVC naming a missing runtime parks, then self-heals when the runtime is created", func() {
		ginkgo.By("creating a model-less ISVC that names a runtime which does not exist yet")
		isvc := v1beta1testing.MakeInferenceService("model-ru-heal-named-isvc", ns.Name).
			Runtime("model-ru-heal-named-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting RuntimeReady=False / RuntimeNotFound and no Deployment")
		Eventually(func() bool {
			return hasRuntimeReadyCondition(getISVC(ns.Name, isvc.Name), corev1.ConditionFalse, "RuntimeNotFound")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue(),
			"a named-but-missing runtime must park the ISVC, not error-loop")
		deps := &appsv1.DeploymentList{}
		Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
		Expect(deps.Items).To(BeEmpty())

		ginkgo.By("creating the named runtime — name-index fan-out must wake the ISVC")
		csr := v1beta1testing.MakeClusterServingRuntime("model-ru-heal-named-csr").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:named-healed"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		expectEngineImageEventually(ns.Name, "ghcr.io/test/runtime:named-healed",
			"creating the named runtime must un-park the ISVC without any manual poke")
		Eventually(func() bool {
			return hasRuntimeReadyCondition(getISVC(ns.Name, isvc.Name), corev1.ConditionTrue, "RuntimeResolved")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue())
	})

	ginkgo.It("disabling the selected runtime parks the ISVC WITHOUT tearing down the Deployment; re-enable resumes", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("model-ru-heal-flip-model").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("model-ru-heal-flip-fmt").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("model-ru-heal-flip-csr").
			SupportsModelFormat("model-ru-heal-flip-fmt", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:flip"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("model-ru-heal-flip-isvc", ns.Name).
			Model("model-ru-heal-flip-model").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		expectEngineImageEventually(ns.Name, "ghcr.io/test/runtime:flip", "baseline deployment")

		ginkgo.By("disabling the only compatible runtime")
		Eventually(func(g Gomega) {
			live := &v1beta1.ClusterServingRuntime{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: csr.Name}, live)).To(Succeed())
			live.Spec.Disabled = ptr.To(true)
			g.Expect(k8sClient.Update(suiteCtx, live)).To(Succeed())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// A bound auto-select ISVC has no name reference to fan out on, so
		// force reconciles via annotation bumps until one observes the
		// disabled runtime and parks the ISVC.
		ginkgo.By("poking the ISVC until a reconcile observes the disabled runtime")
		poke := 0
		Eventually(func(g Gomega) {
			poke++
			live := getISVC(ns.Name, isvc.Name)
			if live.Annotations == nil {
				live.Annotations = map[string]string{}
			}
			live.Annotations["test.example.com/poke"] = fmt.Sprintf("%d", poke)
			g.Expect(k8sClient.Update(suiteCtx, live)).To(Succeed())
			g.Expect(hasRuntimeReadyCondition(getISVC(ns.Name, isvc.Name), corev1.ConditionFalse, "RuntimeNotFound")).To(BeTrue())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed(),
			"selection failure on an already-serving ISVC must park it with RuntimeReady=False")

		ginkgo.By("the existing Deployment must survive the parked state (non-destructive degrade)")
		Consistently(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(1))
			g.Expect(deps.Items[0].Spec.Template.Spec.Containers[0].Image).To(Equal("ghcr.io/test/runtime:flip"))
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())

		ginkgo.By("re-enabling the runtime — the unresolved-index fan-out must wake the parked ISVC on its own")
		Eventually(func(g Gomega) {
			live := &v1beta1.ClusterServingRuntime{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: csr.Name}, live)).To(Succeed())
			live.Spec.Disabled = ptr.To(false)
			g.Expect(k8sClient.Update(suiteCtx, live)).To(Succeed())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func() bool {
			return hasRuntimeReadyCondition(getISVC(ns.Name, isvc.Name), corev1.ConditionTrue, "RuntimeResolved")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue(),
			"re-enabling the runtime must resume the parked ISVC without any manual poke")
	})
})
