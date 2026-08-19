package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Component-declared disruption budgets in RawDeployment mode. The
// budget resolver prefers the per-Component spec fields over the
// ConfigMap fallback and never merges the two sources, and the Raw path
// carries the declared value onto the child PodDisruptionBudget
// verbatim (no OMENative-style normalization). These specs pin that
// contract end-to-end: create from spec, in-place update, precedence
// over the configured fallback, and deletion when the budget is removed.
var _ = ginkgo.Describe("InferenceService RawDeployment component disruption budgets", ginkgo.Serial, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "raw-depl-pdb-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	createModelAndRuntime := func(prefix string) {
		ginkgo.GinkgoHelper()
		cbm := v1beta1testing.MakeClusterBaseModel(prefix + "-cbm").
			StorageURI("oci://n/example/b/models/o/" + prefix).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime(prefix+"-csr").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  constants.MainContainerName,
				Image: "example.com/engine:v1",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })
	}

	setPDBConfig := func(value *string) {
		ginkgo.GinkgoHelper()
		configKey := client.ObjectKey{
			Namespace: constants.OMENamespace,
			Name:      constants.InferenceServiceConfigMapName,
		}
		Eventually(func(g Gomega) {
			fresh := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(suiteCtx, configKey, fresh)).To(Succeed())
			if fresh.Data == nil {
				fresh.Data = map[string]string{}
			}
			if value == nil {
				delete(fresh.Data, controllerconfig.PodDisruptionBudgetConfigName)
			} else {
				fresh.Data[controllerconfig.PodDisruptionBudgetConfigName] = *value
			}
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	}

	updateISVCEngineBudget := func(key types.NamespacedName, minAvailable, maxUnavailable *intstr.IntOrString) {
		ginkgo.GinkgoHelper()
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, key, fresh)).To(Succeed())
			fresh.Spec.Engine.MinAvailable = minAvailable
			fresh.Spec.Engine.MaxUnavailable = maxUnavailable
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	}

	rawSelector := func(componentName string) map[string]string {
		return map[string]string{
			constants.RawDeploymentAppLabel: constants.GetRawServiceLabel(componentName),
		}
	}

	ginkgo.It("creates, updates, and deletes the engine budget from the component spec without any configured fallback", func() {
		createModelAndRuntime("raw-depl-pdb-spec")

		minReplicas := 3
		maxUnavailable := intstr.FromInt(1)
		isvc := v1beta1testing.MakeInferenceService("raw-depl-pdb-spec-isvc", ns.Name).
			Model("raw-depl-pdb-spec-cbm").
			Runtime("raw-depl-pdb-spec-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas:    &minReplicas,
					MaxUnavailable: &maxUnavailable,
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		Expect(isvc.UID).NotTo(BeEmpty())

		pdbKey := types.NamespacedName{Namespace: ns.Name, Name: constants.EngineServiceName(isvc.Name)}
		isvcKey := types.NamespacedName{Namespace: ns.Name, Name: isvc.Name}

		ginkgo.By("the budget is created verbatim from the component spec with the raw selector")
		var pdbUID types.UID
		Eventually(func(g Gomega) {
			budget := &policyv1.PodDisruptionBudget{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, budget)).To(Succeed())
			controller := metav1.GetControllerOf(budget)
			g.Expect(controller).NotTo(BeNil())
			g.Expect(controller.UID).To(Equal(isvc.UID))
			g.Expect(budget.Spec.MinAvailable).To(BeNil())
			g.Expect(budget.Spec.MaxUnavailable).NotTo(BeNil())
			g.Expect(*budget.Spec.MaxUnavailable).To(Equal(intstr.FromInt(1)))
			g.Expect(budget.Spec.Selector).NotTo(BeNil())
			g.Expect(budget.Spec.Selector.MatchLabels).To(Equal(rawSelector(pdbKey.Name)))
			pdbUID = budget.UID
			g.Expect(pdbUID).NotTo(BeEmpty())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("switching the budget form to a percentage minAvailable updates the budget in place")
		minAvailable := intstr.FromString("50%")
		updateISVCEngineBudget(isvcKey, &minAvailable, nil)
		Eventually(func(g Gomega) {
			budget := &policyv1.PodDisruptionBudget{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, budget)).To(Succeed())
			g.Expect(budget.UID).To(Equal(pdbUID))
			g.Expect(budget.Spec.MaxUnavailable).To(BeNil())
			g.Expect(budget.Spec.MinAvailable).NotTo(BeNil())
			g.Expect(*budget.Spec.MinAvailable).To(Equal(intstr.FromString("50%")))
			g.Expect(budget.Spec.Selector.MatchLabels).To(Equal(rawSelector(pdbKey.Name)))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("removing the budget from the spec deletes the owned PodDisruptionBudget")
		updateISVCEngineBudget(isvcKey, nil, nil)
		Eventually(func() bool {
			err := k8sClient.Get(suiteCtx, pdbKey, &policyv1.PodDisruptionBudget{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue())
	})

	ginkgo.It("prefers the component-declared budget over the configured RawDeployment fallback", func() {
		configKey := client.ObjectKey{
			Namespace: constants.OMENamespace,
			Name:      constants.InferenceServiceConfigMapName,
		}
		configMap := &corev1.ConfigMap{}
		Expect(k8sClient.Get(suiteCtx, configKey, configMap)).To(Succeed())
		priorPDBConfig, priorPDBConfigPresent := configMap.Data[controllerconfig.PodDisruptionBudgetConfigName]
		ginkgo.DeferCleanup(func() {
			if priorPDBConfigPresent {
				setPDBConfig(&priorPDBConfig)
				return
			}
			setPDBConfig(nil)
		})
		fallbackConfig := `{"rawDeployment":{"maxUnavailable":3}}`
		setPDBConfig(&fallbackConfig)

		createModelAndRuntime("raw-depl-pdb-prec")

		minReplicas := 4
		maxUnavailable := intstr.FromInt(1)
		isvc := v1beta1testing.MakeInferenceService("raw-depl-pdb-prec-isvc", ns.Name).
			Model("raw-depl-pdb-prec-cbm").
			Runtime("raw-depl-pdb-prec-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas:    &minReplicas,
					MaxUnavailable: &maxUnavailable,
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		pdbKey := types.NamespacedName{Namespace: ns.Name, Name: constants.EngineServiceName(isvc.Name)}
		Eventually(func(g Gomega) {
			budget := &policyv1.PodDisruptionBudget{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, budget)).To(Succeed())
			g.Expect(budget.Spec.MaxUnavailable).NotTo(BeNil())
			g.Expect(*budget.Spec.MaxUnavailable).To(Equal(intstr.FromInt(1)),
				"the component-declared budget must win over the configured fallback")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
