package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	knapis "knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A service over replicas the user created (spec.replicaRefs): the router
// and engine case. No InferenceReplica controller runs in this suite, so the
// specs stamp the replicas' status themselves and assert what the service
// derives from it. The seeded ingress config disables ingress creation, so
// the route is the external Service named after the service.
var _ = ginkgo.Describe("InferenceService over referenced replicas", ginkgo.Ordered, func() {
	const engine, router, name = "pool-a", "router-a", "svc"
	var ns *corev1.Namespace
	var replicas []*v1beta1.InferenceReplica

	replica := func(replicaName string, c v1beta1.ComponentType, port int32) *v1beta1.InferenceReplica {
		return &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Name: replicaName, Namespace: ns.Name},
			Spec: v1beta1.InferenceReplicaSpec{
				Component: c,
				Runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "server", Image: "example.com/serving:1.0", Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: port}}}},
				}}}},
			},
		}
	}

	ginkgo.BeforeAll(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-refs-")
		Expect(err).NotTo(HaveOccurred())
		replicas = []*v1beta1.InferenceReplica{replica(engine, v1beta1.EngineComponent, 8080), replica(router, v1beta1.RouterComponent, 8000)}
		for _, ir := range replicas {
			Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())
		}
		isvc := v1beta1testing.MakeInferenceService(name, ns.Name).Obj()
		isvc.Spec.ReplicaRefs = &v1beta1.ReplicaRefs{Engine: []string{engine}, Router: []string{router}}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})
	ginkgo.AfterAll(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("creates a stable Service per referenced role that selects the replica's pods", func() {
		for _, tc := range []struct {
			svc, prefix string
			c           v1beta1.ComponentType
			port        int32
		}{{name + "-engine", engine, v1beta1.EngineComponent, 8080}, {name + "-router", router, v1beta1.RouterComponent, 8000}} {
			Eventually(func(g Gomega) {
				svc := &corev1.Service{}
				g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: tc.svc}, svc)).To(Succeed())
				g.Expect(svc.Spec.Selector).To(Equal(map[string]string{
					constants.InferenceServicePodLabelKey: tc.prefix,
					constants.OMEComponentLabel:           string(tc.c),
					query.LabelManagedBy:                  query.ManagedByOMENative,
				}))
				g.Expect(svc.Spec.Ports).To(HaveLen(1))
				g.Expect(svc.Spec.Ports[0].Port).To(Equal(tc.port))
				owner := metav1.GetControllerOf(svc)
				g.Expect(owner).NotTo(BeNil())
				g.Expect(owner.Kind).To(Equal("InferenceService"))
				g.Expect(owner.Name).To(Equal(name))
			}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		}
		// The service projects no replica of its own.
		err := k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: name + "-engine"}, &v1beta1.InferenceReplica{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	ginkgo.It("routes to the router's Service", func() {
		Eventually(func(g Gomega) {
			external := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: name}, external)).To(Succeed())
			g.Expect(external.Spec.Selector[constants.InferenceServicePodLabelKey]).To(Equal(router))
			g.Expect(external.Spec.Selector[constants.OMEComponentLabel]).To(Equal("router"))
			g.Expect(external.Spec.Ports[0].Port).To(Equal(int32(8000)))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("mirrors the replicas' lifecycle counters and readiness", func() {
		for _, ir := range replicas {
			Eventually(func(g Gomega) {
				fresh := &v1beta1.InferenceReplica{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
				fresh.Status = v1beta1.InferenceReplicaStatus{ObservedGeneration: fresh.Generation, Replicas: 1, ReadyReplicas: 1, ServingReplicas: 1, AvailableReplicas: 1}
				g.Expect(k8sClient.Status().Update(suiteCtx, fresh)).To(Succeed())
			}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
		}
		Eventually(func(g Gomega) {
			isvc := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: name}, isvc)).To(Succeed())
			for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent} {
				cs := isvc.Status.Components[c]
				g.Expect(cs.Lifecycle).NotTo(BeNil(), string(c))
				g.Expect(cs.Lifecycle.ReadyReplicas).To(Equal(int32(1)), string(c))
				g.Expect(cs.Lifecycle.ObservedGeneration).To(Equal(isvc.Generation), string(c))
				g.Expect(cs.ScaleTargetRef).NotTo(BeNil(), string(c))
			}
			g.Expect(isvc.Status.Components[v1beta1.EngineComponent].ScaleTargetRef.Name).To(Equal(engine))
			g.Expect(isvc.Status.IsConditionReady(v1beta1.EngineReady)).To(BeTrue())
			g.Expect(isvc.Status.IsConditionReady(v1beta1.RouterReady)).To(BeTrue())
			g.Expect(isvc.Status.IsConditionReady(knapis.ConditionReady)).To(BeTrue())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("leaves the replicas as they were when the service is deleted", func() {
		isvc := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: name}, isvc)).To(Succeed())
		Expect(k8sClient.Delete(suiteCtx, isvc)).To(Succeed())
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: name}, &v1beta1.InferenceService{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		// envtest runs no garbage collector: the Services stay behind with the
		// deleted service as their controller owner, and the replicas carry
		// neither an owner reference nor any spec write.
		for _, ir := range replicas {
			fresh := &v1beta1.InferenceReplica{}
			Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
			Expect(fresh.OwnerReferences).To(BeEmpty())
			Expect(equality.Semantic.DeepEqual(fresh.Spec, ir.Spec)).To(BeTrue(), "spec of %s changed", ir.Name)
		}
		svc := &corev1.Service{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: name + "-engine"}, svc)).To(Succeed())
		Expect(metav1.GetControllerOf(svc).UID).To(Equal(isvc.UID))
	})
})
