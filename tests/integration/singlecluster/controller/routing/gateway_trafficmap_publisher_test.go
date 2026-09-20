package routing

import (
	"fmt"
	"slices"

	"github.com/go-logr/logr"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

var _ = ginkgo.Describe("Gateway API TrafficMap publisher", func() {
	ginkgo.BeforeEach(func() {
		ometesting.SetDefaultEventually()
	})

	ginkgo.It("publishes, converges, and finalizes an owned TrafficMap", func() {
		namespace := uniqueName("gateway-publisher")
		gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(gomega.Succeed())

		name := uniqueName("svc")
		isvc := createISVC(namespace, name, &v1beta1.PlacementSpec{
			Mode:         v1beta1.PlacementModeSplit,
			Requirements: "accelerator=test",
		})
		placeISVC(isvc,
			candidate("cluster-a", "a.routing.example.com", 2, 2),
			candidate("cluster-b", "b.routing.example.com", 1, 1),
		)

		key := types.NamespacedName{Namespace: namespace, Name: name}
		gomega.Eventually(func(g gomega.Gomega) {
			trafficMap := getTrafficMap(g, namespace, name)
			entries := entriesByCluster(g, trafficMap)
			g.Expect(entries).To(gomega.HaveLen(2))
			g.Expect(entries).To(gomega.HaveKey("cluster-a"))
			g.Expect(entries).To(gomega.HaveKey("cluster-b"))
			g.Expect(entries["cluster-a"].Weight).To(gomega.Equal(int32(2)))
			g.Expect(entries["cluster-b"].Weight).To(gomega.Equal(int32(1)))
			routable := apimeta.FindStatusCondition(trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable)
			g.Expect(routable).NotTo(gomega.BeNil())
			g.Expect(routable.Status).To(gomega.Equal(metav1.ConditionTrue))
			g.Expect(trafficMapStatusFieldOwners(trafficMap)).To(gomega.HaveKey("ome-trafficmap-routing"))
		}).Should(gomega.Succeed())

		publisher := placementendpoint.NewGatewayAPITrafficMapPublisher(
			k8sClient,
			k8sClient,
			placementendpoint.Config{
				GlobalHostTemplate: "{{.Name}}.{{.Namespace}}.routing.example.com",
				GlobalGateway:      namespace + "/shared-gateway",
				RouteNamespace:     namespace,
				BackendPort:        443,
			},
		)
		reconciler := &placementendpoint.TrafficMapPublisherReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Log:       logr.Discard(),
			Publisher: publisher,
			Active:    true,
		}

		routeKey := types.NamespacedName{Namespace: namespace, Name: name + "-global"}
		ginkgo.By("persisting the finalizer before claims or Gateway API effects")
		result, err := reconciler.Reconcile(suiteCtx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result.Requeue).To(gomega.BeTrue())
		trafficMap := getTrafficMap(gomega.Default, namespace, name)
		gomega.Expect(trafficMap.Finalizers).To(gomega.ContainElement(
			placementendpoint.TrafficMapPublisherFinalizer,
		))
		gomega.Expect(trafficMap.Status.Publisher).To(gomega.BeNil())
		gomega.Expect(apierrors.IsNotFound(
			k8sClient.Get(suiteCtx, routeKey, &gatewayapiv1.HTTPRoute{}),
		)).To(gomega.BeTrue())
		gomega.Expect(gatewayServicesForSource(namespace, key)).To(gomega.BeEmpty())

		ginkgo.By("retaining the preclaim when strict ownership rejects a foreign route")
		foreignRoute := &gatewayapiv1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{Namespace: routeKey.Namespace, Name: routeKey.Name},
			Spec: gatewayapiv1.HTTPRouteSpec{
				Hostnames: []gatewayapiv1.Hostname{"foreign.routing.example.com"},
			},
		}
		gomega.Expect(k8sClient.Create(suiteCtx, foreignRoute)).To(gomega.Succeed())
		_, err = reconciler.Reconcile(suiteCtx, ctrl.Request{NamespacedName: key})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		trafficMap = getTrafficMap(gomega.Default, namespace, name)
		gomega.Expect(trafficMap.Finalizers).To(gomega.ContainElement(
			placementendpoint.TrafficMapPublisherFinalizer,
		))
		gomega.Expect(trafficMap.Status.Publisher).NotTo(gomega.BeNil())
		gomega.Expect(trafficMap.Status.Publisher.ClaimedTargets).To(gomega.Equal([]string{
			fmt.Sprintf("v1:httproute:%s/%s", routeKey.Namespace, routeKey.Name),
		}))
		gomega.Expect(trafficMap.Status.Publisher.ObservedOptionsDigest).To(gomega.BeEmpty())
		gomega.Expect(trafficMap.Status.Programmed).To(gomega.BeFalse())
		gomega.Expect(trafficMap.Status.GatewayRef).To(gomega.BeNil())
		failed := apimeta.FindStatusCondition(trafficMap.Status.Conditions, v1beta1.TrafficMapProgrammed)
		gomega.Expect(failed).NotTo(gomega.BeNil())
		gomega.Expect(failed.Status).To(gomega.Equal(metav1.ConditionFalse))
		gomega.Expect(failed.Reason).To(gomega.Equal("ClaimRejected"))
		gomega.Expect(gatewayServicesForSource(namespace, key)).To(gomega.BeEmpty())
		unchangedForeign := getGatewayHTTPRoute(routeKey)
		gomega.Expect(unchangedForeign.Spec).To(gomega.Equal(foreignRoute.Spec))
		gomega.Expect(k8sClient.Delete(suiteCtx, unchangedForeign)).To(gomega.Succeed())
		gomega.Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(suiteCtx, routeKey, &gatewayapiv1.HTTPRoute{}))
		}).Should(gomega.BeTrue())

		ginkgo.By("applying the durable claim after the collision is removed")
		trafficMap = reconcileGatewayTrafficMap(reconciler, key)
		gomega.Expect(trafficMap.Status.Publisher.PublisherName).To(gomega.Equal(
			placementendpoint.GatewayAPITrafficMapPublisherName,
		))
		gomega.Expect(trafficMap.Status.Publisher.ObservedOptionsDigest).NotTo(gomega.BeEmpty())
		gomega.Expect(trafficMap.Status.GatewayRef).NotTo(gomega.BeNil())
		gomega.Expect(trafficMap.Status.GatewayRef.Group).To(gomega.Equal(gatewayapiv1.GroupVersion.Group))
		gomega.Expect(trafficMap.Status.GatewayRef.Kind).To(gomega.Equal("HTTPRoute"))
		gomega.Expect(trafficMap.Status.GatewayRef.Namespace).To(gomega.Equal(namespace))
		gomega.Expect(trafficMap.Status.Publisher.ClaimedTargets).To(gomega.Equal([]string{
			fmt.Sprintf("v1:httproute:%s/%s", namespace, trafficMap.Status.GatewayRef.Name),
		}))

		programmed := apimeta.FindStatusCondition(trafficMap.Status.Conditions, v1beta1.TrafficMapProgrammed)
		gomega.Expect(programmed).NotTo(gomega.BeNil())
		gomega.Expect(programmed.Status).To(gomega.Equal(metav1.ConditionTrue))
		assertTrafficMapStatusFieldOwners(trafficMap)

		gomega.Expect(trafficMap.Status.GatewayRef.Namespace).To(gomega.Equal(routeKey.Namespace))
		gomega.Expect(trafficMap.Status.GatewayRef.Name).To(gomega.Equal(routeKey.Name))
		route := getGatewayHTTPRoute(routeKey)
		gomega.Expect(route.Labels).To(gomega.HaveKeyWithValue(
			placementendpoint.ManagedByLabel, placementendpoint.ManagedByValue,
		))
		gomega.Expect(route.Labels).To(gomega.HaveKeyWithValue(
			placementendpoint.PlacementEndpointISVCLabel, name,
		))
		gomega.Expect(route.Labels).To(gomega.HaveKeyWithValue(
			placementendpoint.PlacementEndpointISVCNamespaceLabel, namespace,
		))
		gomega.Expect(route.Spec.Hostnames).To(gomega.Equal([]gatewayapiv1.Hostname{
			gatewayapiv1.Hostname(fmt.Sprintf("%s.%s.routing.example.com", name, namespace)),
		}))
		services := gatewayServicesForSource(namespace, key)
		gomega.Expect(services).To(gomega.HaveLen(2))
		serviceByCluster := indexGatewayServicesByCluster(services)
		gomega.Expect(serviceByCluster).To(gomega.HaveKey("cluster-a"))
		gomega.Expect(serviceByCluster).To(gomega.HaveKey("cluster-b"))
		for _, service := range services {
			gomega.Expect(service.Labels).To(gomega.HaveKeyWithValue(
				placementendpoint.ManagedByLabel, placementendpoint.ManagedByValue,
			))
			gomega.Expect(service.Labels).To(gomega.HaveKeyWithValue(
				placementendpoint.PlacementEndpointISVCLabel, name,
			))
			gomega.Expect(service.Labels).To(gomega.HaveKeyWithValue(
				placementendpoint.PlacementEndpointISVCNamespaceLabel, namespace,
			))
		}
		gomega.Expect(serviceByCluster["cluster-a"].Spec.ExternalName).To(gomega.Equal("a.routing.example.com"))
		gomega.Expect(serviceByCluster["cluster-b"].Spec.ExternalName).To(gomega.Equal("b.routing.example.com"))
		assertGatewayBackendWeights(route, map[gatewayapiv1.ObjectName]int32{
			gatewayapiv1.ObjectName(serviceByCluster["cluster-a"].Name): 2,
			gatewayapiv1.ObjectName(serviceByCluster["cluster-b"].Name): 1,
		})

		ginkgo.By("authoritatively changing a weight and pruning a removed home")
		previousGeneration := trafficMap.Generation
		removedServiceKey := client.ObjectKeyFromObject(serviceByCluster["cluster-b"])
		placeISVC(isvc, candidate("cluster-a", "a.routing.example.com", 1, 1))
		waitForTrafficMapEntries(namespace, name, previousGeneration, map[string]int32{"cluster-a": 1})
		trafficMap = reconcileGatewayTrafficMap(reconciler, key)
		route = getGatewayHTTPRoute(routeKey)
		services = gatewayServicesForSource(namespace, key)
		gomega.Expect(services).To(gomega.HaveLen(1))
		gomega.Expect(services[0].Labels[placementendpoint.PlacementClusterLabel]).To(gomega.Equal("cluster-a"))
		assertGatewayBackendWeights(route, map[gatewayapiv1.ObjectName]int32{
			gatewayapiv1.ObjectName(services[0].Name): 1,
		})
		gomega.Eventually(func() bool {
			err := k8sClient.Get(suiteCtx, removedServiceKey, &corev1.Service{})
			return apierrors.IsNotFound(err)
		}).Should(gomega.BeTrue())

		ginkgo.By("preserving all-zero weights as an authoritative no-traffic state")
		previousGeneration = trafficMap.Generation
		placeISVC(isvc,
			candidate("cluster-a", "a.routing.example.com", 2, 0),
			candidate("cluster-b", "b.routing.example.com", 1, 0),
		)
		waitForTrafficMapEntries(namespace, name, previousGeneration, map[string]int32{
			"cluster-a": 0,
			"cluster-b": 0,
		})
		reconcileGatewayTrafficMap(reconciler, key)
		route = getGatewayHTTPRoute(routeKey)
		services = gatewayServicesForSource(namespace, key)
		gomega.Expect(services).To(gomega.HaveLen(2))
		serviceByCluster = indexGatewayServicesByCluster(services)
		gomega.Expect(serviceByCluster).To(gomega.HaveKey("cluster-a"))
		gomega.Expect(serviceByCluster).To(gomega.HaveKey("cluster-b"))
		assertGatewayBackendWeights(route, map[gatewayapiv1.ObjectName]int32{
			gatewayapiv1.ObjectName(serviceByCluster["cluster-a"].Name): 0,
			gatewayapiv1.ObjectName(serviceByCluster["cluster-b"].Name): 0,
		})

		ginkgo.By("retaining an empty route while pruning every backend resource")
		trafficMap = getTrafficMap(gomega.Default, namespace, name)
		previousGeneration = trafficMap.Generation
		updateISVCStatus(isvc, func(current *v1beta1.InferenceService) {
			current.Status.Placement.Phase = v1beta1.PlacementPhaseRacing
		})
		waitForTrafficMapEntries(namespace, name, previousGeneration, map[string]int32{})
		reconcileGatewayTrafficMap(reconciler, key)
		route = getGatewayHTTPRoute(routeKey)
		gomega.Expect(route.Spec.Rules).To(gomega.HaveLen(1))
		gomega.Expect(route.Spec.Rules[0].BackendRefs).To(gomega.BeEmpty())
		gomega.Expect(gatewayServicesForSource(namespace, key)).To(gomega.BeEmpty())

		ginkgo.By("restoring resources so deletion exercises journal-based cleanup")
		trafficMap = getTrafficMap(gomega.Default, namespace, name)
		previousGeneration = trafficMap.Generation
		placeISVC(isvc,
			candidate("cluster-a", "a.routing.example.com", 1, 1),
			candidate("cluster-b", "b.routing.example.com", 1, 1),
		)
		waitForTrafficMapEntries(namespace, name, previousGeneration, map[string]int32{
			"cluster-a": 1,
			"cluster-b": 1,
		})
		reconcileGatewayTrafficMap(reconciler, key)
		ownedServiceKeys := gatewayServiceKeys(gatewayServicesForSource(namespace, key))
		gomega.Expect(ownedServiceKeys).To(gomega.HaveLen(2))

		const holdFinalizer = "example.com/gateway-publisher-test-hold"
		gomega.Eventually(func(g gomega.Gomega) {
			current := getTrafficMap(g, namespace, name)
			if !slices.Contains(current.Finalizers, holdFinalizer) {
				current.Finalizers = append(current.Finalizers, holdFinalizer)
			}
			g.Expect(k8sClient.Update(suiteCtx, current)).To(gomega.Succeed())
		}).Should(gomega.Succeed())

		ginkgo.By("deleting the map and finalizing only the journaled resources")
		disabled := false
		gomega.Eventually(func(g gomega.Gomega) {
			current := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, key, current)).To(gomega.Succeed())
			current.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &disabled}
			g.Expect(k8sClient.Update(suiteCtx, current)).To(gomega.Succeed())
		}).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(getTrafficMap(g, namespace, name).DeletionTimestamp.IsZero()).To(gomega.BeFalse())
		}).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			_, err := reconciler.Reconcile(suiteCtx, ctrl.Request{NamespacedName: key})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			current := getTrafficMap(g, namespace, name)
			g.Expect(current.Finalizers).To(gomega.ContainElement(holdFinalizer))
			g.Expect(current.Finalizers).NotTo(gomega.ContainElement(
				placementendpoint.TrafficMapPublisherFinalizer,
			))
			g.Expect(current.Status.Publisher).To(gomega.BeNil())
			g.Expect(current.Status.GatewayRef).To(gomega.BeNil())
			g.Expect(current.Status.Programmed).To(gomega.BeFalse())
			g.Expect(current.Status.ObservedTrafficMapGeneration).To(gomega.BeZero())
			g.Expect(apimeta.FindStatusCondition(
				current.Status.Conditions, v1beta1.TrafficMapRoutable,
			)).NotTo(gomega.BeNil())
		}).Should(gomega.Succeed())

		gomega.Eventually(func() bool {
			err := k8sClient.Get(suiteCtx, routeKey, &gatewayapiv1.HTTPRoute{})
			return apierrors.IsNotFound(err)
		}).Should(gomega.BeTrue())
		for _, serviceKey := range ownedServiceKeys {
			serviceKey := serviceKey
			gomega.Eventually(func() bool {
				err := k8sClient.Get(suiteCtx, serviceKey, &corev1.Service{})
				return apierrors.IsNotFound(err)
			}).Should(gomega.BeTrue())
		}
		gomega.Expect(gatewayServicesForSource(namespace, key)).To(gomega.BeEmpty())

		ginkgo.By("releasing the test hold after cleanup has completed")
		gomega.Eventually(func(g gomega.Gomega) {
			current := getTrafficMap(g, namespace, name)
			current.Finalizers = slices.DeleteFunc(current.Finalizers, func(value string) bool {
				return value == holdFinalizer
			})
			g.Expect(k8sClient.Update(suiteCtx, current)).To(gomega.Succeed())
		}).Should(gomega.Succeed())
		gomega.Eventually(func() bool {
			err := k8sClient.Get(suiteCtx, key, &v1beta1.TrafficMap{})
			return apierrors.IsNotFound(err)
		}).Should(gomega.BeTrue())
	})
})

func reconcileGatewayTrafficMap(
	reconciler *placementendpoint.TrafficMapPublisherReconciler,
	key types.NamespacedName,
) *v1beta1.TrafficMap {
	var programmed *v1beta1.TrafficMap
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		_, err := reconciler.Reconcile(suiteCtx, ctrl.Request{NamespacedName: key})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		current := &v1beta1.TrafficMap{}
		g.Expect(k8sClient.Get(suiteCtx, key, current)).To(gomega.Succeed())
		g.Expect(current.Finalizers).To(gomega.ContainElement(
			placementendpoint.TrafficMapPublisherFinalizer,
		))
		g.Expect(current.Status.Programmed).To(gomega.BeTrue())
		g.Expect(current.Status.ObservedTrafficMapGeneration).To(gomega.Equal(current.Generation))
		g.Expect(current.Status.Publisher).NotTo(gomega.BeNil())
		g.Expect(current.Status.Publisher.ClaimedTargets).To(gomega.HaveLen(1))
		g.Expect(current.Status.GatewayRef).NotTo(gomega.BeNil())
		programmed = current
	}).Should(gomega.Succeed())
	return programmed
}

func waitForTrafficMapEntries(
	namespace string,
	name string,
	previousGeneration int64,
	want map[string]int32,
) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		trafficMap := getTrafficMap(g, namespace, name)
		g.Expect(trafficMap.Generation).To(gomega.BeNumerically(">", previousGeneration))
		entries := entriesByCluster(g, trafficMap)
		g.Expect(entries).To(gomega.HaveLen(len(want)))
		for cluster, weight := range want {
			entry, found := entries[cluster]
			g.Expect(found).To(gomega.BeTrue())
			g.Expect(entry.Weight).To(gomega.Equal(weight))
		}
	}).Should(gomega.Succeed())
}

func getGatewayHTTPRoute(key types.NamespacedName) *gatewayapiv1.HTTPRoute {
	route := &gatewayapiv1.HTTPRoute{}
	gomega.ExpectWithOffset(1, k8sClient.Get(suiteCtx, key, route)).To(gomega.Succeed())
	return route
}

func gatewayServicesForSource(routeNamespace string, sourceKey types.NamespacedName) []*corev1.Service {
	list := &corev1.ServiceList{}
	gomega.ExpectWithOffset(1, k8sClient.List(
		suiteCtx,
		list,
		client.InNamespace(routeNamespace),
		client.MatchingLabels{
			placementendpoint.ManagedByLabel:                      placementendpoint.ManagedByValue,
			placementendpoint.PlacementEndpointISVCLabel:          sourceKey.Name,
			placementendpoint.PlacementEndpointISVCNamespaceLabel: sourceKey.Namespace,
		},
	)).To(gomega.Succeed())
	services := make([]*corev1.Service, 0, len(list.Items))
	for i := range list.Items {
		services = append(services, &list.Items[i])
	}
	return services
}

func indexGatewayServicesByCluster(services []*corev1.Service) map[string]*corev1.Service {
	indexed := make(map[string]*corev1.Service, len(services))
	for _, service := range services {
		cluster := service.Labels[placementendpoint.PlacementClusterLabel]
		gomega.ExpectWithOffset(1, cluster).NotTo(gomega.BeEmpty())
		gomega.ExpectWithOffset(1, indexed).NotTo(gomega.HaveKey(cluster))
		indexed[cluster] = service
	}
	return indexed
}

func gatewayServiceKeys(services []*corev1.Service) []types.NamespacedName {
	keys := make([]types.NamespacedName, 0, len(services))
	for _, service := range services {
		keys = append(keys, client.ObjectKeyFromObject(service))
	}
	return keys
}

func assertGatewayBackendWeights(
	route *gatewayapiv1.HTTPRoute,
	want map[gatewayapiv1.ObjectName]int32,
) {
	gomega.ExpectWithOffset(1, route.Spec.Rules).To(gomega.HaveLen(1))
	refs := route.Spec.Rules[0].BackendRefs
	gomega.ExpectWithOffset(1, refs).To(gomega.HaveLen(len(want)))
	got := make(map[gatewayapiv1.ObjectName]int32, len(refs))
	for _, ref := range refs {
		gomega.ExpectWithOffset(1, ref.Weight).NotTo(gomega.BeNil())
		got[ref.Name] = *ref.Weight
	}
	gomega.ExpectWithOffset(1, got).To(gomega.Equal(want))
}

func assertTrafficMapStatusFieldOwners(trafficMap *v1beta1.TrafficMap) {
	owners := trafficMapStatusFieldOwners(trafficMap)
	gomega.ExpectWithOffset(1, owners).To(gomega.HaveKey("ome-trafficmap-routing"))
	gomega.ExpectWithOffset(1, owners).To(gomega.HaveKey("ome-trafficmap-publisher"))
	gomega.ExpectWithOffset(1, owners["ome-trafficmap-routing"]).To(gomega.ContainSubstring("Routable"))
	gomega.ExpectWithOffset(1, owners["ome-trafficmap-routing"]).NotTo(gomega.ContainSubstring("Programmed"))
	gomega.ExpectWithOffset(1, owners["ome-trafficmap-publisher"]).To(gomega.ContainSubstring("Programmed"))
	gomega.ExpectWithOffset(1, owners["ome-trafficmap-publisher"]).NotTo(gomega.ContainSubstring("Routable"))
}

func trafficMapStatusFieldOwners(trafficMap *v1beta1.TrafficMap) map[string]string {
	owners := map[string]string{}
	for _, fields := range trafficMap.ManagedFields {
		if fields.Operation == metav1.ManagedFieldsOperationApply && fields.Subresource == "status" {
			owners[fields.Manager] += string(fields.FieldsV1.Raw)
		}
	}
	return owners
}
