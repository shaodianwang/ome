package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("InferenceService PodDisruptionBudget lifecycle", ginkgo.Ordered, ginkgo.Serial, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-pdb-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("creates, updates, transitions, and deletes the engine budget", func() {
		configKey := client.ObjectKey{
			Namespace: constants.OMENamespace,
			Name:      constants.InferenceServiceConfigMapName,
		}
		configMap := &corev1.ConfigMap{}
		Expect(k8sClient.Get(suiteCtx, configKey, configMap)).To(Succeed())
		priorPDBConfig, priorPDBConfigPresent := configMap.Data[controllerconfig.PodDisruptionBudgetConfigName]

		setPDBConfig := func(value *string) {
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

		ginkgo.DeferCleanup(func() {
			if priorPDBConfigPresent {
				setPDBConfig(&priorPDBConfig)
				return
			}
			setPDBConfig(nil)
		})

		initialConfig := `{"rawDeployment":{"maxUnavailable":1},"omeNative":{"maxUnavailable":1}}`
		setPDBConfig(&initialConfig)

		cbm := v1beta1testing.MakeClusterBaseModel("pdb-lifecycle-model").
			StorageURI("oci://n/test/b/models/o/pdb-lifecycle").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("pdb-lifecycle-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  constants.MainContainerName,
				Image: "example.com/runtime:test",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		minReplicas := 3
		isvc := v1beta1testing.MakeInferenceService("pdb-lifecycle-isvc", ns.Name).
			Model(cbm.Name).
			Runtime(csr.Name).
			Engine(&v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: &minReplicas,
				Autoscaler:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			}}).Obj()
		omeNativeMode := constants.OMENative
		isvc.Spec.DeploymentMode = &omeNativeMode
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		Expect(isvc.UID).NotTo(BeEmpty())

		pdbKey := types.NamespacedName{Namespace: ns.Name, Name: isvc.Name + "-engine"}
		omeNativeSelector := map[string]string{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		}
		var pdbUID types.UID

		ginkgo.By("creating the OMENative budget from the configured fallback")
		Eventually(func(g Gomega) {
			budget := &policyv1.PodDisruptionBudget{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, budget)).To(Succeed())
			controller := metav1.GetControllerOf(budget)
			g.Expect(controller).NotTo(BeNil())
			g.Expect(controller.UID).To(Equal(isvc.UID))
			g.Expect(budget.Spec.MinAvailable).NotTo(BeNil())
			g.Expect(*budget.Spec.MinAvailable).To(Equal(intstr.FromInt(2)))
			g.Expect(budget.Spec.MaxUnavailable).To(BeNil())
			g.Expect(budget.Spec.Selector).NotTo(BeNil())
			g.Expect(budget.Spec.Selector.MatchLabels).To(Equal(omeNativeSelector))
			pdbUID = budget.UID
			g.Expect(pdbUID).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		updateISVC := func(mutate func(*v1beta1.InferenceService)) {
			Eventually(func(g Gomega) {
				fresh := &v1beta1.InferenceService{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), fresh)).To(Succeed())
				mutate(fresh)
				g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
			}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		}

		ginkgo.By("updating the OMENative fallback and nudging the InferenceService")
		updatedConfig := `{"rawDeployment":{"maxUnavailable":1},"omeNative":{"maxUnavailable":2}}`
		setPDBConfig(&updatedConfig)
		updateISVC(func(fresh *v1beta1.InferenceService) {
			if fresh.Annotations == nil {
				fresh.Annotations = map[string]string{}
			}
			fresh.Annotations["example.com/pdb-config-revision"] = "two"
		})
		Eventually(func(g Gomega) {
			budget := &policyv1.PodDisruptionBudget{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, budget)).To(Succeed())
			g.Expect(budget.UID).To(Equal(pdbUID))
			g.Expect(budget.Spec.MinAvailable).NotTo(BeNil())
			g.Expect(*budget.Spec.MinAvailable).To(Equal(intstr.FromInt(1)))
			g.Expect(budget.Spec.MaxUnavailable).To(BeNil())
			g.Expect(budget.Spec.Selector.MatchLabels).To(Equal(omeNativeSelector))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("switching the same component to RawDeployment")
		updateISVC(func(fresh *v1beta1.InferenceService) {
			rawMode := constants.RawDeployment
			fresh.Spec.DeploymentMode = &rawMode
		})
		Eventually(func(g Gomega) {
			deployment := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, deployment)).To(Succeed())
			budget := &policyv1.PodDisruptionBudget{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, budget)).To(Succeed())
			g.Expect(budget.UID).To(Equal(pdbUID))
			g.Expect(budget.Spec.Selector.MatchLabels).To(Equal(omeNativeSelector))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("marking the target RawDeployment fully available")
		Eventually(func(g Gomega) {
			deployment := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, deployment)).To(Succeed())
			g.Expect(deployment.Spec.Replicas).NotTo(BeNil())
			deployment.Status.ObservedGeneration = deployment.Generation
			deployment.Status.Replicas = *deployment.Spec.Replicas
			deployment.Status.UpdatedReplicas = *deployment.Spec.Replicas
			deployment.Status.ReadyReplicas = *deployment.Spec.Replicas
			deployment.Status.AvailableReplicas = *deployment.Spec.Replicas
			g.Expect(k8sClient.Status().Update(suiteCtx, deployment)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			budget := &policyv1.PodDisruptionBudget{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, budget)).To(Succeed())
			g.Expect(budget.UID).To(Equal(pdbUID))
			g.Expect(budget.Spec.MinAvailable).To(BeNil())
			g.Expect(budget.Spec.MaxUnavailable).NotTo(BeNil())
			g.Expect(*budget.Spec.MaxUnavailable).To(Equal(intstr.FromInt(1)))
			g.Expect(budget.Spec.Selector.MatchLabels).To(Equal(map[string]string{
				constants.RawDeploymentAppLabel: constants.GetRawServiceLabel(pdbKey.Name),
			}))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("switching back to OMENative without an OMENative policy")
		rawOnlyConfig := `{"rawDeployment":{"maxUnavailable":1}}`
		setPDBConfig(&rawOnlyConfig)
		priorIR := &v1beta1.InferenceReplica{}
		Expect(k8sClient.Get(suiteCtx, pdbKey, priorIR)).To(Succeed())
		priorIRGeneration := priorIR.Generation
		updateISVC(func(fresh *v1beta1.InferenceService) {
			mode := constants.OMENative
			fresh.Spec.DeploymentMode = &mode
			targetReplicas := 4
			fresh.Spec.Engine.MinReplicas = &targetReplicas
		})
		Eventually(func(g Gomega) {
			ir := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, ir)).To(Succeed())
			g.Expect(ir.Generation).To(BeNumerically(">", priorIRGeneration))
			g.Expect(ir.Spec.Replicas).NotTo(BeNil())
			g.Expect(ir.Status.ReadyReplicas).To(BeNumerically("<", *ir.Spec.Replicas))
			budget := &policyv1.PodDisruptionBudget{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, budget)).To(Succeed())
			g.Expect(budget.UID).To(Equal(pdbUID))
			g.Expect(budget.Spec.Selector.MatchLabels).To(Equal(map[string]string{
				constants.RawDeploymentAppLabel: constants.GetRawServiceLabel(pdbKey.Name),
			}))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("marking the target InferenceReplica fully ready")
		Eventually(func(g Gomega) {
			ir := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, ir)).To(Succeed())
			g.Expect(ir.Spec.Replicas).NotTo(BeNil())
			ir.Status.ObservedGeneration = ir.Generation
			ir.Status.ReadyReplicas = *ir.Spec.Replicas
			ir.Status.AvailableReplicas = *ir.Spec.Replicas
			ir.Status.UpdatedReadyReplicas = *ir.Spec.Replicas
			ir.Status.CurrentRevision = ir.Name + "-ready"
			ir.Status.UpdateRevision = ir.Status.CurrentRevision
			g.Expect(k8sClient.Status().Update(suiteCtx, ir)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		Eventually(func() bool {
			err := k8sClient.Get(suiteCtx, pdbKey, &policyv1.PodDisruptionBudget{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue())

		ginkgo.By("re-enabling the OMENative policy")
		setPDBConfig(&updatedConfig)
		updateISVC(func(fresh *v1beta1.InferenceService) {
			if fresh.Annotations == nil {
				fresh.Annotations = map[string]string{}
			}
			fresh.Annotations["example.com/pdb-config-revision"] = "re-enabled"
		})
		Eventually(func(g Gomega) {
			budget := &policyv1.PodDisruptionBudget{}
			g.Expect(k8sClient.Get(suiteCtx, pdbKey, budget)).To(Succeed())
			g.Expect(budget.UID).NotTo(BeEmpty())
			g.Expect(budget.UID).NotTo(Equal(pdbUID))
			g.Expect(budget.Spec.MinAvailable).NotTo(BeNil())
			g.Expect(*budget.Spec.MinAvailable).To(Equal(intstr.FromInt(2)))
			g.Expect(budget.Spec.MaxUnavailable).To(BeNil())
			g.Expect(budget.Spec.Selector.MatchLabels).To(Equal(omeNativeSelector))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("removing the configured policy from the active mode")
		setPDBConfig(nil)
		updateISVC(func(fresh *v1beta1.InferenceService) {
			if fresh.Annotations == nil {
				fresh.Annotations = map[string]string{}
			}
			fresh.Annotations["example.com/pdb-config-revision"] = "absent"
		})
		Eventually(func() bool {
			err := k8sClient.Get(suiteCtx, pdbKey, &policyv1.PodDisruptionBudget{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue())
	})
})
