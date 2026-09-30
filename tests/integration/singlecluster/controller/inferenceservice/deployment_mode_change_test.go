package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	knapis "knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A Component's deployment mode selects exactly one backend. Changing the
// mode of a live InferenceService removes the objects of the backend the
// Component leaves in the pass that projects the selected one, and a switch
// to VirtualDeployment, which selects no backend, removes every owned object.
var _ = ginkgo.Describe("InferenceService deployment mode change", ginkgo.Ordered, func() {
	const (
		isvcName    = "mode-change-isvc"
		modelName   = "mode-change-llama"
		runtimeName = "mode-change-runtime"
	)
	var (
		ns          *corev1.Namespace
		isvcKey     types.NamespacedName
		engineKey   types.NamespacedName
		externalKey types.NamespacedName
	)

	notFound := func(g Gomega, obj client.Object, key types.NamespacedName, what string) {
		err := k8sClient.Get(suiteCtx, key, obj)
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "%s %s must be gone, got err=%v", what, key.Name, err)
	}

	ginkgo.BeforeAll(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-mode-change-")
		Expect(err).NotTo(HaveOccurred())
		isvcKey = types.NamespacedName{Namespace: ns.Name, Name: isvcName}
		engineKey = types.NamespacedName{Namespace: ns.Name, Name: isvcName + "-engine"}
		externalKey = isvcKey

		cbm := v1beta1testing.MakeClusterBaseModel(modelName).
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime(runtimeName).
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  constants.MainContainerName,
				Image: "ghcr.io/test/runtime:test",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		minReplicas := 1
		isvc := v1beta1testing.MakeInferenceService(isvcName, ns.Name).
			Model(modelName).
			Runtime(runtimeName).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: &minReplicas,
				Autoscaler:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			}}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	ginkgo.AfterAll(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("projects a RawDeployment engine as a Deployment with its Services", func() {
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(suiteCtx, engineKey, &appsv1.Deployment{})).To(Succeed())
			g.Expect(k8sClient.Get(suiteCtx, engineKey, &corev1.Service{})).To(Succeed())
			g.Expect(k8sClient.Get(suiteCtx, externalKey, &corev1.Service{})).To(Succeed())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("replaces the Deployment with an InferenceReplica when the engine moves to OMENative", func() {
		updateInferenceService(isvcKey, func(isvc *v1beta1.InferenceService) {
			mode := constants.OMENative
			isvc.Spec.DeploymentMode = &mode
		})

		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(suiteCtx, engineKey, &v1beta1.InferenceReplica{})).To(Succeed())
			notFound(g, &appsv1.Deployment{}, engineKey, "Deployment")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("recording the removal as an event on the InferenceService")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			var found bool
			for _, ev := range events.Items {
				if ev.InvolvedObject.Kind == "InferenceService" && ev.InvolvedObject.Name == isvcName &&
					ev.Reason == "DeploymentModeChanged" {
					found = true
					g.Expect(ev.Message).To(ContainSubstring("Deployment"))
				}
			}
			g.Expect(found).To(BeTrue(), "expected a DeploymentModeChanged event on %s", isvcName)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("removes the InferenceReplica and its status footprint when the engine moves back to RawDeployment", func() {
		updateInferenceService(isvcKey, func(isvc *v1beta1.InferenceService) {
			mode := constants.RawDeployment
			isvc.Spec.DeploymentMode = &mode
		})

		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(suiteCtx, engineKey, &appsv1.Deployment{})).To(Succeed())
			notFound(g, &v1beta1.InferenceReplica{}, engineKey, "InferenceReplica")

			isvc := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, isvcKey, isvc)).To(Succeed())
			g.Expect(isvc.Status.Components[v1beta1.EngineComponent].Lifecycle).To(BeNil(),
				"a RawDeployment engine carries no OMENative lifecycle status")
			// Readiness follows the selected backend: nothing in envtest marks the
			// fresh Deployment Available, so the engine, and the service, stay
			// not ready whatever the OMENative backend reported before.
			cond := isvc.Status.GetCondition(v1beta1.EngineReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(corev1.ConditionFalse))
			ready := isvc.Status.GetCondition(knapis.ConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(ready.Status).NotTo(Equal(corev1.ConditionTrue))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("removes every owned object when the InferenceService becomes a VirtualDeployment", func() {
		updateInferenceService(isvcKey, func(isvc *v1beta1.InferenceService) {
			if isvc.Annotations == nil {
				isvc.Annotations = map[string]string{}
			}
			isvc.Annotations[constants.DeploymentMode] = string(constants.VirtualDeployment)
		})

		Eventually(func(g Gomega) {
			notFound(g, &appsv1.Deployment{}, engineKey, "Deployment")
			notFound(g, &corev1.Service{}, engineKey, "Service")
			notFound(g, &corev1.Service{}, externalKey, "external Service")

			isvc := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, isvcKey, isvc)).To(Succeed())
			ready := isvc.Status.GetCondition(knapis.ConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(ready.Status).To(Equal(corev1.ConditionTrue))
			g.Expect(ready.Reason).To(Equal("VirtualDeployment"))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
