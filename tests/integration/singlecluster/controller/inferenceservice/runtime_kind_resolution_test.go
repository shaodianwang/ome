package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// spec.runtime.kind contracts on the live (autoSync) path: a declared
// ClusterServingRuntime kind wins a namespaced/cluster name collision, a
// bare-name reference to a namespaced-only runtime keeps resolving even
// though the API server defaults the kind to ClusterServingRuntime, and a
// missing named runtime on a model-backed ISVC parks (advisory condition,
// no error requeue) instead of hard-erroring every reconcile.
var _ = ginkgo.Describe("Explicit runtime kind resolution", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "model-ru-kind-")
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
			return c.Status == wantStatus && (wantReason == "" || c.Reason == wantReason)
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

	makeNamespacedRuntime := func(name, format, image string) *v1beta1.ServingRuntime {
		sr := v1beta1testing.MakeServingRuntime(name, ns.Name).
			SupportsModelFormat(format, true, 10).Obj()
		sr.Spec.EngineConfig = &v1beta1.EngineSpec{
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Name: "ome-container", Image: image,
			}},
		}
		return sr
	}

	ginkgo.It("declared ClusterServingRuntime kind resolves the cluster runtime despite a same-named namespaced ServingRuntime", func() {
		const runtimeName = "model-ru-kind-collide-rt"

		ginkgo.By("creating same-named runtimes in both scopes with different engine images")
		sr := makeNamespacedRuntime(runtimeName, "safetensors", "ghcr.io/test/runtime:namespace-scoped")
		Expect(k8sClient.Create(suiteCtx, sr)).To(Succeed())
		csr := v1beta1testing.MakeClusterServingRuntime(runtimeName).
			SupportsModelFormat("safetensors", true, 10).
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:cluster-scoped"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating an ISVC that declares kind ClusterServingRuntime")
		isvc := v1beta1testing.MakeInferenceService("model-ru-kind-collide-isvc", ns.Name).
			Runtime(runtimeName).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		expectEngineImageEventually(ns.Name, "ghcr.io/test/runtime:cluster-scoped",
			"the declared ClusterServingRuntime kind must win the name collision")
	})

	ginkgo.It("declared ClusterServingRuntime kind still resolves a namespaced-only runtime (API-defaulted kind compatibility)", func() {
		const runtimeName = "model-ru-kind-nsonly-rt"

		ginkgo.By("creating ONLY a namespaced runtime under the referenced name")
		sr := makeNamespacedRuntime(runtimeName, "safetensors", "ghcr.io/test/runtime:namespace-only")
		Expect(k8sClient.Create(suiteCtx, sr)).To(Succeed())

		ginkgo.By("creating an ISVC whose ref carries the defaulted ClusterServingRuntime kind")
		isvc := v1beta1testing.MakeInferenceService("model-ru-kind-nsonly-isvc", ns.Name).
			Runtime(runtimeName).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		expectEngineImageEventually(ns.Name, "ghcr.io/test/runtime:namespace-only",
			"a bare-name reference to a namespaced runtime must keep resolving despite the defaulted kind")
	})

	ginkgo.It("model-backed ISVC naming a missing runtime parks without error-looping, then self-heals", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("model-ru-kind-park-model").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("model-ru-kind-park-fmt").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("creating a model-backed ISVC that names a runtime which does not exist")
		isvc := v1beta1testing.MakeInferenceService("model-ru-kind-park-isvc", ns.Name).
			Model("model-ru-kind-park-model").
			Runtime("model-ru-kind-park-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting RuntimeReady=False / RuntimeNotFound and no Deployment")
		Eventually(func() bool {
			return hasRuntimeReadyCondition(getISVC(ns.Name, isvc.Name), corev1.ConditionFalse, "RuntimeNotFound")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue(),
			"a named-but-missing runtime must park a model-backed ISVC, not hard-error")
		deps := &appsv1.DeploymentList{}
		Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
		Expect(deps.Items).To(BeEmpty())

		ginkgo.By("expecting NO RuntimeValidationError warning events — the hard-error requeue loop must not run")
		Consistently(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			for _, ev := range events.Items {
				if ev.InvolvedObject.Name != isvc.Name {
					continue
				}
				g.Expect(ev.Reason).NotTo(Equal("RuntimeValidationError"),
					"missing named runtime must take the parking path, not the validation hard-error path")
			}
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())

		ginkgo.By("creating the named runtime — name-index fan-out must wake the parked ISVC")
		csr := v1beta1testing.MakeClusterServingRuntime("model-ru-kind-park-csr").
			SupportsModelFormat("model-ru-kind-park-fmt", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:park-healed"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		expectEngineImageEventually(ns.Name, "ghcr.io/test/runtime:park-healed",
			"creating the named runtime must un-park the model-backed ISVC without any manual poke")
		Eventually(func() bool {
			return hasRuntimeReadyCondition(getISVC(ns.Name, isvc.Name), corev1.ConditionTrue, "RuntimeResolved")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue())
	})
})
