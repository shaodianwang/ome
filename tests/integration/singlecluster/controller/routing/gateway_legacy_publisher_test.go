package routing

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

var _ = ginkgo.Describe("Gateway API legacy endpoint publisher", func() {
	ginkgo.BeforeEach(func() {
		ometesting.SetDefaultEventually()
	})

	ginkgo.It("retains subordinate resources until a finalizer-held route is gone", func() {
		namespace := uniqueName("gateway-legacy-publisher")
		gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(gomega.Succeed())

		publisher := placementendpoint.NewGatewayAPIPublisher(
			k8sClient,
			placementendpoint.Config{
				GlobalGateway:  namespace + "/shared-gateway",
				RouteNamespace: namespace,
				BackendPort:    443,
			},
			placementendpoint.WithGatewayAPIReader(k8sClient),
		)
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: namespace, UID: types.UID("test-owner")},
		}
		target := placementendpoint.Target{
			GlobalHost: fmt.Sprintf("%s.routing.example.com", namespace),
			Homes: []placementendpoint.Home{
				{Cluster: "cluster-a", BackendHost: "a.routing.example.com", Weight: 3},
				{Cluster: "cluster-b", BackendHost: "b.routing.example.com", Weight: 1},
			},
		}
		gomega.Expect(publisher.Publish(suiteCtx, isvc, target)).To(gomega.Succeed())

		routeKey := types.NamespacedName{Namespace: namespace, Name: "svc-global"}
		route := &gatewayapiv1.HTTPRoute{}
		gomega.Expect(k8sClient.Get(suiteCtx, routeKey, route)).To(gomega.Succeed())
		route.Finalizers = []string{"example.com/hold"}
		route.Spec.Rules[0].BackendRefs[0].Weight = nil
		gomega.Expect(k8sClient.Update(suiteCtx, route)).To(gomega.Succeed())

		err := publisher.Unpublish(suiteCtx, isvc)
		gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("deletion is pending")))
		gomega.Expect(k8sClient.Get(suiteCtx, routeKey, route)).To(gomega.Succeed())
		gomega.Expect(route.DeletionTimestamp).NotTo(gomega.BeNil())
		for _, rule := range route.Spec.Rules {
			for _, backend := range rule.BackendRefs {
				gomega.Expect(backend.Weight).NotTo(gomega.BeNil())
				gomega.Expect(*backend.Weight).To(gomega.BeZero())
			}
		}
		serviceKeys := []types.NamespacedName{
			{Namespace: namespace, Name: "svc-global-cluster-a"},
			{Namespace: namespace, Name: "svc-global-cluster-b"},
		}
		for _, serviceKey := range serviceKeys {
			gomega.Expect(k8sClient.Get(suiteCtx, serviceKey, &corev1.Service{})).To(gomega.Succeed())
		}

		route.Finalizers = nil
		gomega.Expect(k8sClient.Update(suiteCtx, route)).To(gomega.Succeed())
		gomega.Eventually(func() error {
			return publisher.Unpublish(suiteCtx, isvc)
		}).Should(gomega.Succeed())
		gomega.Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(suiteCtx, routeKey, &gatewayapiv1.HTTPRoute{}))
		}).Should(gomega.BeTrue())
		for _, serviceKey := range serviceKeys {
			serviceKey := serviceKey
			gomega.Eventually(func() bool {
				return apierrors.IsNotFound(k8sClient.Get(suiteCtx, serviceKey, &corev1.Service{}))
			}).Should(gomega.BeTrue())
		}
	})

	ginkgo.It("completes an ownerless TrafficMap handoff through manager events", func() {
		namespace := uniqueName("gateway-handoff")
		gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(gomega.Succeed())

		name := uniqueName("svc")
		key := types.NamespacedName{Namespace: namespace, Name: name}
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{
				Name:       name,
				Namespace:  namespace,
				Finalizers: []string{placementendpoint.EndpointFinalizer},
			},
			Spec: v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Runner: &v1beta1.RunnerSpec{
						Container: corev1.Container{Name: "ome-container", Image: "example/model-server:test"},
					},
				},
			},
		}
		publisher := placementendpoint.NewGatewayAPIPublisher(
			k8sClient,
			placementendpoint.Config{
				GlobalGateway: namespace + "/shared-gateway",
				BackendPort:   443,
			},
			placementendpoint.WithGatewayAPIReader(k8sClient),
		)
		gomega.Expect(publisher.Publish(suiteCtx, isvc, placementendpoint.Target{
			GlobalHost: fmt.Sprintf("%s.routing.example.com", namespace),
			Homes: []placementendpoint.Home{{
				Cluster: "cluster-a", BackendHost: "a.routing.example.com", Weight: 1,
			}},
		})).To(gomega.Succeed())

		routeKey := types.NamespacedName{Namespace: namespace, Name: name + "-global"}
		serviceKey := types.NamespacedName{Namespace: namespace, Name: name + "-global-cluster-a"}
		route := &gatewayapiv1.HTTPRoute{}
		gomega.Expect(k8sClient.Get(suiteCtx, routeKey, route)).To(gomega.Succeed())
		route.Finalizers = []string{"example.com/hold"}
		gomega.Expect(k8sClient.Update(suiteCtx, route)).To(gomega.Succeed())

		trafficMap := &v1beta1.TrafficMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:       name,
				Namespace:  namespace,
				Finalizers: []string{placementendpoint.TrafficMapPublisherFinalizer},
			},
			Spec: v1beta1.TrafficMapSpec{
				Service: name,
				Mode:    v1beta1.PlacementModeSingle,
			},
		}
		gomega.Expect(k8sClient.Create(suiteCtx, trafficMap)).To(gomega.Succeed())

		createdRead := handoffReads.arm(key)
		gomega.Expect(k8sClient.Create(suiteCtx, isvc)).To(gomega.Succeed())
		gomega.Eventually(createdRead).Should(gomega.Receive(),
			"the legacy controller must observe the blocking TrafficMap")

		deletedRead := handoffReads.arm(key)
		gomega.Expect(k8sClient.Delete(suiteCtx, isvc)).To(gomega.Succeed())
		gomega.Eventually(deletedRead).Should(gomega.Receive(),
			"the source deletion must reconcile while the handoff is blocked")
		currentISVC := &v1beta1.InferenceService{}
		gomega.Expect(k8sClient.Get(suiteCtx, key, currentISVC)).To(gomega.Succeed())
		gomega.Expect(currentISVC.DeletionTimestamp).NotTo(gomega.BeNil())
		gomega.Expect(currentISVC.Finalizers).To(gomega.ContainElement(placementendpoint.EndpointFinalizer))
		gomega.Expect(k8sClient.Get(suiteCtx, routeKey, route)).To(gomega.Succeed())
		gomega.Expect(route.DeletionTimestamp).To(gomega.BeNil())

		ginkgo.By("releasing the ownerless TrafficMap handoff")
		gomega.Expect(k8sClient.Get(suiteCtx, key, trafficMap)).To(gomega.Succeed())
		trafficMap.Finalizers = nil
		gomega.Expect(k8sClient.Update(suiteCtx, trafficMap)).To(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(suiteCtx, routeKey, route)).To(gomega.Succeed())
			g.Expect(route.DeletionTimestamp).NotTo(gomega.BeNil())
			for _, rule := range route.Spec.Rules {
				for _, backend := range rule.BackendRefs {
					g.Expect(backend.Weight).NotTo(gomega.BeNil())
					g.Expect(*backend.Weight).To(gomega.BeZero())
				}
			}
			g.Expect(k8sClient.Get(suiteCtx, serviceKey, &corev1.Service{})).To(gomega.Succeed())
			g.Expect(k8sClient.Get(suiteCtx, key, currentISVC)).To(gomega.Succeed())
			g.Expect(currentISVC.Finalizers).To(gomega.ContainElement(placementendpoint.EndpointFinalizer))
		}).Should(gomega.Succeed())

		ginkgo.By("letting the legacy cleanup retry after the route disappears")
		route.Finalizers = nil
		gomega.Expect(k8sClient.Update(suiteCtx, route)).To(gomega.Succeed())
		gomega.Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(suiteCtx, routeKey, &gatewayapiv1.HTTPRoute{}))
		}).Should(gomega.BeTrue())
		gomega.Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(suiteCtx, key, &v1beta1.InferenceService{}))
		}).Should(gomega.BeTrue())
		gomega.Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(suiteCtx, serviceKey, &corev1.Service{}))
		}).Should(gomega.BeTrue())
	})
})
