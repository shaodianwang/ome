package inferenceservice

import (
	"strconv"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// An InferenceReplica write the API server rejects fails every reconcile until
// what the rejection depends on changes. The rejection is recorded on the
// InferenceService, where its owner looks, and the reconcile keeps retrying.
// A ValidatingAdmissionPolicy stands in for the admission webhook here: both
// reject through the API server.
var _ = ginkgo.Describe("InferenceService with a rejected InferenceReplica write", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "ir-rejected-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("records the rejection on the InferenceService and writes once it is allowed", func() {
		const (
			prefix      = "ir-rejected"
			denyMessage = "InferenceReplica updates are frozen in this namespace"
		)
		modelName := prefix + "-model"
		runtimeName := prefix + "-runtime"
		isvcName := prefix + "-isvc"

		model := v1beta1testing.MakeClusterBaseModel(modelName).
			StorageURI("oci://n/example/b/models/o/" + prefix).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, model)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, model) })

		runtime := v1beta1testing.MakeClusterServingRuntime(runtimeName).
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineConfig(&v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: corev1.Container{
						Name:  constants.MainContainerName,
						Image: "example.com/runtime:v1",
						Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
					},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, runtime)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, runtime) })

		isvc := v1beta1testing.MakeInferenceService(isvcName, ns.Name).
			Model(modelName).
			Runtime(runtimeName).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: ptr.To(1),
					MaxReplicas: 2,
				},
			}).Obj()
		omeNativeMode := constants.OMENative
		isvc.Spec.DeploymentMode = &omeNativeMode
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		isvcKey := types.NamespacedName{Namespace: ns.Name, Name: isvcName}
		irKey := types.NamespacedName{Namespace: ns.Name, Name: isvcName + "-engine"}

		// projectedGeneration returns the InferenceService generation the
		// InferenceReplica was last written for, and the InferenceService's
		// current generation.
		projectedGeneration := func(g Gomega) (projected, current string) {
			cur := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, isvcKey, cur)).To(Succeed())
			ir := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, irKey, ir)).To(Succeed())
			return ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey], strconv.FormatInt(cur.Generation, 10)
		}

		ginkgo.By("writing the InferenceReplica for the InferenceService's current generation")
		var settled string
		Eventually(func(g Gomega) {
			projected, current := projectedGeneration(g)
			g.Expect(projected).To(Equal(current))
			settled = projected
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("denying every update of an InferenceReplica in the namespace")
		policy := &admissionregistrationv1.ValidatingAdmissionPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: ns.Name + "-deny-ir-updates"},
			Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
				FailurePolicy: ptr.To(admissionregistrationv1.Fail),
				MatchConstraints: &admissionregistrationv1.MatchResources{
					ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{
						RuleWithOperations: admissionregistrationv1.RuleWithOperations{
							Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Update},
							Rule: admissionregistrationv1.Rule{
								APIGroups:   []string{v1beta1.SchemeGroupVersion.Group},
								APIVersions: []string{v1beta1.SchemeGroupVersion.Version},
								Resources:   []string{"inferencereplicas"},
							},
						},
					}},
				},
				Validations: []admissionregistrationv1.Validation{{Expression: "false", Message: denyMessage}},
			},
		}
		binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
			ObjectMeta: metav1.ObjectMeta{Name: policy.Name},
			Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
				PolicyName:        policy.Name,
				ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
				MatchResources: &admissionregistrationv1.MatchResources{
					NamespaceSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{corev1.LabelMetadataName: ns.Name},
					},
				},
			},
		}
		Expect(k8sClient.Create(suiteCtx, policy)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = client.IgnoreNotFound(k8sClient.Delete(suiteCtx, policy)) })
		Expect(k8sClient.Create(suiteCtx, binding)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = client.IgnoreNotFound(k8sClient.Delete(suiteCtx, binding)) })

		// The API server loads a policy asynchronously. A dry-run update
		// the policy denies shows it is in force.
		Eventually(func(g Gomega) {
			ir := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, irKey, ir)).To(Succeed())
			err := k8sClient.Patch(suiteCtx, ir,
				client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"labels":{"probe":"denied"}}}`)),
				client.DryRunAll)
			g.Expect(err).To(MatchError(ContainSubstring(denyMessage)))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("changing the InferenceService so its InferenceReplica has to be patched")
		updateInferenceService(isvcKey, func(cur *v1beta1.InferenceService) {
			cur.Spec.Engine.MinReplicas = ptr.To(2)
		})

		ginkgo.By("recording the rejection on the InferenceService as a Warning")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			var rejected []corev1.Event
			for _, ev := range events.Items {
				if ev.InvolvedObject.Kind == "InferenceService" && ev.InvolvedObject.Name == isvcName &&
					ev.Reason == "ComponentReconcileError" {
					rejected = append(rejected, ev)
				}
			}
			g.Expect(rejected).NotTo(BeEmpty())
			for _, ev := range rejected {
				g.Expect(ev.Type).To(Equal(corev1.EventTypeWarning))
				g.Expect(ev.Message).To(ContainSubstring(irKey.String()))
				g.Expect(ev.Message).To(ContainSubstring(denyMessage))
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		projected, current := projectedGeneration(Default)
		Expect(projected).To(Equal(settled), "the rejected patch must not have been written")
		Expect(current).NotTo(Equal(settled))

		ginkgo.By("patching the InferenceReplica once the policy no longer denies it")
		Expect(k8sClient.Delete(suiteCtx, binding)).To(Succeed())
		Eventually(func(g Gomega) {
			projected, current := projectedGeneration(g)
			g.Expect(projected).To(Equal(current))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
