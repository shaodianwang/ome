package routing

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

type publicationActivity struct {
	client.Client
	reads   atomic.Int64
	updates atomic.Int64
}

func (c *publicationActivity) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1beta1.TrafficMap); ok {
		c.reads.Add(1)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *publicationActivity) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	switch obj.(type) {
	case *corev1.Service, *gatewayapiv1.HTTPRoute:
		c.updates.Add(1)
	}
	return c.Client.Update(ctx, obj, opts...)
}

var _ = ginkgo.Describe("TrafficMap publication convergence", func() {
	ginkgo.BeforeEach(func() {
		ometesting.SetDefaultEventually()
	})

	ginkgo.It("quiesces with API defaulting and repairs external drift on resync", func() {
		namespace := uniqueName("publication-convergence")
		gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(gomega.Succeed())
		isvc := createISVC(namespace, "service", &v1beta1.PlacementSpec{
			Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeAll, ClusterAffinity: routingTestAffinity(),
		})
		placeISVC(isvc, candidate("member-a", "a.example.com", 1, 1), candidate("member-b", "b.example.com", 1, 1))
		key := client.ObjectKeyFromObject(isvc)
		gomega.Eventually(func(g gomega.Gomega) {
			tm := getTrafficMap(g, namespace, isvc.Name)
			g.Expect(tm.Status.SourceUID).To(gomega.Equal(isvc.UID))
			g.Expect(tm.Spec.Entries).To(gomega.HaveLen(2))
		}).Should(gomega.Succeed())

		watchClient, err := client.NewWithWatch(suiteCfg, client.Options{Scheme: k8sClient.Scheme()})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		changes, err := watchClient.Watch(suiteCtx, &v1beta1.TrafficMapList{}, client.InNamespace(namespace), client.MatchingFields{"metadata.name": isvc.Name})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var inconsistent atomic.Bool
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			for change := range changes.ResultChan() {
				if tm, ok := change.Object.(*v1beta1.TrafficMap); ok {
					condition := apimeta.FindStatusCondition(tm.Status.Conditions, v1beta1.TrafficMapPublished)
					if condition != nil && tm.Status.Published != (condition.Status == metav1.ConditionTrue) {
						inconsistent.Store(true)
					}
				}
			}
		}()
		ginkgo.DeferCleanup(func() { changes.Stop(); <-watchDone })

		mgr, err := ctrl.NewManager(suiteCfg, ctrl.Options{
			Scheme:  k8sClient.Scheme(),
			Cache:   cache.Options{DefaultNamespaces: map[string]cache.Config{namespace: {}}},
			Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
			LeaderElection: true, LeaderElectionNamespace: namespace, LeaderElectionID: "publication-test",
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		activity := &publicationActivity{Client: mgr.GetClient()}
		reconciler := &placementendpoint.TrafficMapPublisherReconciler{
			Client: activity, APIReader: mgr.GetAPIReader(), Log: logr.Discard(), Active: true,
			LeaderElectionEnabled: true, RequeueAfter: 5 * time.Second,
			Publisher: placementendpoint.NewGatewayAPITrafficMapPublisher(activity, mgr.GetAPIReader(), placementendpoint.Config{
				GlobalHostTemplate: "{{.Name}}.{{.Namespace}}.example.com", GlobalGateway: namespace + "/gateway", BackendPort: 8080,
			}),
		}
		gomega.Expect(reconciler.SetupWithManager(mgr)).To(gomega.Succeed())
		ctx, cancel := context.WithCancel(suiteCtx)
		stopped := make(chan error, 1)
		go func() { stopped <- mgr.Start(ctx) }()
		ginkgo.DeferCleanup(func() { cancel(); gomega.Eventually(stopped).Should(gomega.Receive(gomega.Succeed())) })
		gomega.Eventually(func(g gomega.Gomega) {
			tm := getTrafficMap(g, namespace, isvc.Name)
			g.Expect(tm.Status.Published).To(gomega.BeTrue())
			g.Expect(tm.Status.GatewayRef).NotTo(gomega.BeNil())
		}).Should(gomega.Succeed())

		// Quiet windows are kept well under RequeueAfter: they assert the publisher is idle between resyncs.
		ginkgo.By("observing a quiet interval with unchanged routing and publication status")
		var stableVersion string
		gomega.Eventually(func(g gomega.Gomega) {
			tm := getTrafficMap(g, namespace, isvc.Name)
			stableVersion = tm.ResourceVersion
			g.Consistently(func() string { return getTrafficMap(gomega.Default, namespace, isvc.Name).ResourceVersion }, time.Second, 50*time.Millisecond).Should(gomega.Equal(stableVersion))
		}).Should(gomega.Succeed())
		reads, updates := activity.reads.Load(), activity.updates.Load()
		gomega.Consistently(func() int64 { return activity.reads.Load() }, time.Second, 50*time.Millisecond).Should(gomega.Equal(reads))
		gomega.Expect(updates).To(gomega.BeZero(), "API defaults must not cause no-op PUTs")

		ginkgo.By("reapplying the plan without updating API-defaulted Services or HTTPRoutes")
		gomega.Eventually(func() int64 { return activity.reads.Load() }).Should(gomega.BeNumerically(">", reads))
		gomega.Consistently(func() int64 { return activity.updates.Load() }, time.Second, 50*time.Millisecond).Should(gomega.BeZero())

		ginkgo.By("repairing a backend changed outside the publisher")
		services := gatewayServicesForSource(namespace, key)
		gomega.Expect(services).To(gomega.HaveLen(2))
		backend := services[0].DeepCopy()
		wantedHost := backend.Spec.ExternalName
		backend.Spec.ExternalName = "drift.example.com"
		gomega.Expect(k8sClient.Update(suiteCtx, backend)).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			live := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(backend), live)).To(gomega.Succeed())
			g.Expect(live.Spec.ExternalName).To(gomega.Equal(wantedHost))
		}).Should(gomega.Succeed())
		gomega.Expect(activity.updates.Load()).To(gomega.Equal(int64(1)))
		liveSource := &v1beta1.InferenceService{}
		gomega.Expect(k8sClient.Get(suiteCtx, key, liveSource)).To(gomega.Succeed())
		disabled := false
		liveSource.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &disabled}
		gomega.Expect(k8sClient.Update(suiteCtx, liveSource)).To(gomega.Succeed())
		gomega.Eventually(func() bool {
			tm := &v1beta1.TrafficMap{}
			return client.IgnoreNotFound(k8sClient.Get(suiteCtx, key, tm)) == nil && tm.Name == ""
		}).Should(gomega.BeTrue())
		changes.Stop()
		<-watchDone
		gomega.Expect(inconsistent.Load()).To(gomega.BeFalse(), "publication boolean and condition must change atomically")
	})
})
