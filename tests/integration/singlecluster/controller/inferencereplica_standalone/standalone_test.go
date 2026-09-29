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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferencereplica"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/tests/integration/util"
)

const (
	timeout  = 60 * time.Second
	interval = 250 * time.Millisecond
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
})
