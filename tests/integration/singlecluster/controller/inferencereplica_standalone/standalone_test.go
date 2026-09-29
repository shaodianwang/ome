package inferencereplicastandalone

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferencereplica"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

const (
	timeout  = 60 * time.Second
	interval = 250 * time.Millisecond

	// The runner images a refs replica renders: the runtime's engine runner
	// carries the first, and an edit of that runner to the second is what a
	// re-render follows.
	runtimeImage        = "example.com/serving:v1"
	updatedRuntimeImage = "example.com/serving:v2"
)

// bareIR is a standalone replica: rendered runners, no parent, no owner.
func bareIR(namespace, name string, component v1beta1.ComponentType) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: component,
			Runners: []v1beta1.Runner{{
				Name: v1beta1.RunnerNameDefault,
				Size: 1,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "server", Image: "example.com/serving:1.0"}},
				}},
			}},
		},
	}
}

// refsCatalog creates what a refs replica renders against and deletes it
// when the spec ends: a ClusterBaseModel and a ClusterServingRuntime whose
// engine runner is the whole pod template. The runtime's supported format
// names the model's format and framework, which is what selecting a runtime
// for the model matches on. Both are cluster-scoped, so they are named after
// the spec's namespace.
func refsCatalog(ns, image string) (*v1beta1.ClusterBaseModel, *v1beta1.ClusterServingRuntime) {
	model := v1beta1testing.MakeClusterBaseModel(ns + "-model").
		StorageURI("pvc://models/" + ns).ModelFormat("safetensors").ModelFramework("Transformers").Obj()
	runtime := v1beta1testing.MakeClusterServingRuntime(ns+"-runtime").
		SupportsModelFormat("safetensors", true, 10).
		EngineRunner(corev1.Container{Name: "ome-container", Image: image}).Obj()
	Expect(k8sClient.Create(suiteCtx, model)).To(Succeed())
	Expect(k8sClient.Create(suiteCtx, runtime)).To(Succeed())
	ginkgo.DeferCleanup(func() {
		Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, runtime))).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, model))).To(Succeed())
	})
	return model, runtime
}

// refsIR is a standalone replica that renders from the named ClusterBaseModel
// and, when runtime is set, the named ClusterServingRuntime; without one the
// controller selects a runtime for the model.
func refsIR(namespace, name string, component v1beta1.ComponentType, model, runtime string) *v1beta1.InferenceReplica {
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: component,
			ModelRef:  &v1beta1.ModelRef{Name: model, Kind: ptr.To("ClusterBaseModel")},
		},
	}
	if runtime != "" {
		ir.Spec.RuntimeRef = &v1beta1.ServingRuntimeRef{Name: runtime, Kind: ptr.To("ClusterServingRuntime")}
	}
	return ir
}

// replicaPods lists the pods labeled with the replica's name.
func replicaPods(g Gomega, namespace, name string) []corev1.Pod {
	pods := &corev1.PodList{}
	g.Expect(k8sClient.List(suiteCtx, pods, client.InNamespace(namespace),
		client.MatchingLabels{constants.InferenceServicePodLabelKey: name})).To(Succeed())
	return pods.Items
}

// firstContainerImages is the image of each pod's first container.
func firstContainerImages(pods []corev1.Pod) []string {
	images := make([]string, 0, len(pods))
	for _, pod := range pods {
		if len(pod.Spec.Containers) > 0 {
			images = append(images, pod.Spec.Containers[0].Image)
		}
	}
	return images
}

// envValue returns the container's value for the named env var.
func envValue(c corev1.Container, name string) (string, bool) {
	for _, env := range c.Env {
		if env.Name == name {
			return env.Value, true
		}
	}
	return "", false
}

var _ = ginkgo.Describe("standalone InferenceReplica", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "ir-standalone-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		if ns != nil {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		}
	})

	ginkgo.It("derives its pod, revision, Service and selector from its own name", func() {
		ir := bareIR(ns.Name, "pool-a", v1beta1.EngineComponent)
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: "pool-a-engine-0-default-0"}, pod)).To(Succeed())
			owner := metav1.GetControllerOf(pod)
			g.Expect(owner).NotTo(BeNil())
			g.Expect(owner.Kind).To(Equal("InferenceReplica"))
			g.Expect(owner.Name).To(Equal("pool-a"))
			g.Expect(pod.Labels[constants.InferenceServicePodLabelKey]).To(Equal("pool-a"))
			g.Expect(pod.Labels[constants.OMEComponentLabel]).To(Equal("engine"))
		}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())

		Eventually(func(g Gomega) {
			svc := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: "pool-a-engine-headless"}, svc)).To(Succeed())
			owner := metav1.GetControllerOf(svc)
			g.Expect(owner).NotTo(BeNil())
			g.Expect(owner.Kind).To(Equal("InferenceReplica"))
			g.Expect(owner.Name).To(Equal("pool-a"))
			g.Expect(svc.Spec.Selector[constants.InferenceServicePodLabelKey]).To(Equal("pool-a"))
		}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())

		Eventually(func(g Gomega) {
			revisions := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, revisions, client.InNamespace(ns.Name),
				client.MatchingLabels{constants.InferenceServicePodLabelKey: "pool-a"})).To(Succeed())
			g.Expect(revisions.Items).To(HaveLen(1))
			g.Expect(revisions.Items[0].Name).To(HavePrefix("pool-a-engine-"))
		}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
			g.Expect(fresh.Finalizers).To(ContainElement(inferencereplica.TeardownFinalizer))
			g.Expect(fresh.Status.Replicas).To(Equal(int32(1)))
			g.Expect(fresh.Status.UpdateRevision).To(HavePrefix("pool-a-engine-"))
			g.Expect(fresh.Status.LabelSelector).To(ContainSubstring(constants.InferenceServicePodLabelKey + "=pool-a"))
		}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())
	})

	ginkgo.It("reports on itself when it has no parent", func() {
		ir := bareIR(ns.Name, "pool-b", v1beta1.EngineComponent)
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
			g.Expect(apimeta.IsStatusConditionTrue(fresh.Status.Conditions,
				string(workloadtypes.ConditionInstanceReadyTimeoutUnconfigured))).To(BeTrue())
		}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())

		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			var onReplica int
			for _, e := range events.Items {
				if e.InvolvedObject.Kind == "InferenceReplica" && e.InvolvedObject.Name == "pool-b" &&
					e.Type == corev1.EventTypeWarning &&
					e.Reason == string(workloadtypes.EventReasonInstanceReadyTimeoutUnconfigured) {
					onReplica++
				}
			}
			g.Expect(onReplica).To(BeNumerically(">=", 1))
		}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())
	})

	ginkgo.It("runs a router replica that has nothing to route to", func() {
		ir := bareIR(ns.Name, "pool-r", v1beta1.RouterComponent)
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: "pool-r-router-0-default-0"}, pod)).To(Succeed())
		}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())
	})

	ginkgo.It("tears down through its finalizer", func() {
		ir := bareIR(ns.Name, "pool-d", v1beta1.EngineComponent)
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: "pool-d-engine-0-default-0"}, pod)).To(Succeed())
		}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())

		Expect(k8sClient.Delete(suiteCtx, ir)).To(Succeed())
		// Pods here are never bound to a node, so the apiserver removes them
		// as soon as the teardown pipeline deletes them; the finalizer then
		// lifts and the replica disappears.
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), &v1beta1.InferenceReplica{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}).WithTimeout(2 * time.Minute).WithPolling(interval).Should(Succeed())

		// envtest runs no garbage collector, so the pod can only be gone
		// because the teardown pipeline deleted it before lifting the finalizer.
		err := k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: "pool-d-engine-0-default-0"}, &corev1.Pod{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "pod survived teardown: %v", err)
	})

	// A replica with modelRef or runtimeRef stores no runners: the controller
	// renders its pod templates from the referenced objects on every pass, so
	// the stored spec stays as written and the pods follow the runtime.
	ginkgo.Context("rendering from refs", func() {
		ginkgo.It("renders its pods from a model and a runtime", func() {
			model, runtime := refsCatalog(ns.Name, runtimeImage)
			ir := refsIR(ns.Name, "pool-m", v1beta1.EngineComponent, model.Name, runtime.Name)
			Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())

			Eventually(func(g Gomega) {
				pods := replicaPods(g, ns.Name, "pool-m")
				g.Expect(pods).To(HaveLen(1))
				pod := pods[0]
				g.Expect(pod.Name).To(HavePrefix("pool-m-engine-"))
				owner := metav1.GetControllerOf(&pod)
				g.Expect(owner).NotTo(BeNil())
				g.Expect(owner.Kind).To(Equal("InferenceReplica"))
				g.Expect(owner.Name).To(Equal("pool-m"))
				g.Expect(pod.Spec.Containers).NotTo(BeEmpty())
				g.Expect(pod.Spec.Containers[0].Image).To(Equal(runtimeImage))
				path, ok := envValue(pod.Spec.Containers[0], constants.ModelPathEnvVarKey)
				g.Expect(ok).To(BeTrue(), "the rendered container carries no %s", constants.ModelPathEnvVarKey)
				g.Expect(path).To(Equal(constants.ModelDefaultMountPath), "a PVC model mounts at the default model path")
			}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())

			Eventually(func(g Gomega) {
				fresh := &v1beta1.InferenceReplica{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
				g.Expect(fresh.Spec.Runners).To(BeEmpty())
				g.Expect(fresh.Spec.ModelRef).NotTo(BeNil())
				g.Expect(fresh.Spec.RuntimeRef).NotTo(BeNil())
				g.Expect(fresh.Status.UpdateRevision).To(HavePrefix("pool-m-engine-"))
			}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())
		})

		ginkgo.It("selects the runtime for its model", func() {
			model, _ := refsCatalog(ns.Name, runtimeImage)
			ir := refsIR(ns.Name, "pool-s", v1beta1.EngineComponent, model.Name, "")
			Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())

			Eventually(func(g Gomega) {
				pods := replicaPods(g, ns.Name, "pool-s")
				g.Expect(pods).To(HaveLen(1))
				g.Expect(firstContainerImages(pods)).To(ConsistOf(runtimeImage))
				fresh := &v1beta1.InferenceReplica{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
				g.Expect(fresh.Spec.RuntimeRef).To(BeNil())
				g.Expect(fresh.Status.UpdateRevision).To(HavePrefix("pool-s-engine-"))
			}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())
		})

		ginkgo.It("reports a runtime that has no piece for its component", func() {
			model, runtime := refsCatalog(ns.Name, runtimeImage)
			ir := refsIR(ns.Name, "pool-p", v1beta1.DecoderComponent, model.Name, runtime.Name)
			Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())

			Eventually(func(g Gomega) {
				fresh := &v1beta1.InferenceReplica{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
				cond := apimeta.FindStatusCondition(fresh.Status.Conditions, inferencereplica.InferenceReplicaConditionReady)
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(cond.Reason).To(Equal(inferencereplica.ReasonRuntimePieceMissing))
				g.Expect(cond.Message).To(ContainSubstring(runtime.Name))
				g.Expect(cond.ObservedGeneration).To(Equal(fresh.Generation))
				g.Expect(fresh.Status.UpdateRevision).To(BeEmpty())
			}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())

			// A blocked render skips the workload pass: nothing is created.
			revisions := &appsv1.ControllerRevisionList{}
			Expect(k8sClient.List(suiteCtx, revisions, client.InNamespace(ns.Name),
				client.MatchingLabels{constants.InferenceServicePodLabelKey: "pool-p"})).To(Succeed())
			Expect(revisions.Items).To(BeEmpty())
			Expect(replicaPods(Default, ns.Name, "pool-p")).To(BeEmpty())
		})

		ginkgo.It("re-renders when the runtime image changes", func() {
			model, runtime := refsCatalog(ns.Name, runtimeImage)
			ir := refsIR(ns.Name, "pool-u", v1beta1.EngineComponent, model.Name, runtime.Name)
			Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())

			first := &v1beta1.InferenceReplica{}
			Eventually(func(g Gomega) {
				g.Expect(firstContainerImages(replicaPods(g, ns.Name, "pool-u"))).To(ConsistOf(runtimeImage))
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), first)).To(Succeed())
				g.Expect(first.Status.UpdateRevision).To(HavePrefix("pool-u-engine-"))
			}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())

			live := &v1beta1.ClusterServingRuntime{}
			Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(runtime), live)).To(Succeed())
			live.Spec.EngineConfig.Runner.Container.Image = updatedRuntimeImage
			Expect(k8sClient.Update(suiteCtx, live)).To(Succeed())

			// The runtime watch re-renders the replica: a new revision and a
			// pod on the new image, with the replica's own spec untouched.
			Eventually(func(g Gomega) {
				fresh := &v1beta1.InferenceReplica{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
				g.Expect(fresh.Generation).To(Equal(first.Generation))
				g.Expect(fresh.Status.UpdateRevision).To(HavePrefix("pool-u-engine-"))
				g.Expect(fresh.Status.UpdateRevision).NotTo(Equal(first.Status.UpdateRevision))
				revision := &appsv1.ControllerRevision{}
				g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: fresh.Status.UpdateRevision}, revision)).To(Succeed())
				g.Expect(firstContainerImages(replicaPods(g, ns.Name, "pool-u"))).To(ContainElement(updatedRuntimeImage))
			}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())
		})
	})
})
