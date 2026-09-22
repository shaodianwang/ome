package routing

import (
	"fmt"
	"sync/atomic"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// These specs drive the routing controller end-to-end through envtest: they set
// an InferenceService's placement status directly (nothing else writes it in
// this suite) and assert the TrafficMap the controller generates — its entries,
// capacity-planned health-gated weights, provenance, owner reference, and the
// emptied-but-retained table the ISVC keeps when it leaves Placed.
var _ = ginkgo.Describe("TrafficMap routing controller", func() {
	var isvcNS string

	ginkgo.BeforeEach(func() {
		ometesting.SetDefaultEventually()
		isvcNS = uniqueName("routing")
		gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: isvcNS},
		})).To(gomega.Succeed())
	})

	ginkgo.It("generates an owner-ref'd TrafficMap for a Placed single-home ISVC", func() {
		name := uniqueName("svc")
		isvc := createISVC(isvcNS, name, &v1beta1.PlacementSpec{
			Mode:         v1beta1.PlacementModeSingle,
			Requirements: "accelerator=test",
		})
		placeISVC(isvc, candidate("cluster-a", "cluster-a.example.com", 2, 2))

		var tm *v1beta1.TrafficMap
		gomega.Eventually(func(g gomega.Gomega) {
			tm = getTrafficMap(g, isvcNS, name)
			g.Expect(tm.Spec.Service).To(gomega.Equal(name))
			g.Expect(tm.Spec.Entries).To(gomega.HaveLen(1))
			e := tm.Spec.Entries[0]
			g.Expect(e.Cluster).To(gomega.Equal("cluster-a"))
			// A lone serving home takes all traffic — reduced to the unit weight 1.
			g.Expect(e.Weight).To(gomega.Equal(int32(1)))
			g.Expect(e.Healthy).To(gomega.BeTrue())
			g.Expect(e.Endpoint).NotTo(gomega.BeNil())
			g.Expect(e.Endpoint.Host).To(gomega.Equal("cluster-a.example.com"))
			g.Expect(e.Capacity).NotTo(gomega.BeNil())
			g.Expect(e.Capacity.Allocated).To(gomega.Equal(int32(2)))
			g.Expect(e.Capacity.Ready).To(gomega.Equal(int32(2)))
		}).Should(gomega.Succeed())

		ginkgo.By("asserting the TrafficMap is controller-owned by the ISVC")
		gomega.Expect(metav1.IsControlledBy(tm, isvc)).To(gomega.BeTrue(),
			"the TrafficMap must be owner-ref'd to its ISVC so it is garbage-collected with it")
		gomega.Expect(tm.Spec.ObservedISVCGeneration).To(gomega.Equal(isvc.Generation))
	})

	ginkgo.It("weights homes by admitted replicas times the per-cluster capacity factor", func() {
		name := uniqueName("svc")
		// cluster-a's replicas each serve twice a baseline replica, so with equal
		// admitted counts a's share is double b's: a 2:1 split.
		isvc := createISVC(isvcNS, name, &v1beta1.PlacementSpec{
			Mode:         v1beta1.PlacementModeSplit,
			Requirements: "accelerator=test",
			CapacityFactors: map[string]resource.Quantity{
				"cluster-a": resource.MustParse("2"),
			},
		})
		placeISVC(isvc,
			candidate("cluster-a", "cluster-a.example.com", 2, 2),
			candidate("cluster-b", "cluster-b.example.com", 2, 2))

		gomega.Eventually(func(g gomega.Gomega) {
			tm := getTrafficMap(g, isvcNS, name)
			g.Expect(tm.Spec.Mode).To(gomega.Equal(v1beta1.PlacementModeSplit))
			byCluster := entriesByCluster(g, tm)
			g.Expect(byCluster).To(gomega.HaveLen(2))

			a := byCluster["cluster-a"]
			g.Expect(a.Weight).To(gomega.Equal(int32(2)))
			g.Expect(a.Healthy).To(gomega.BeTrue())
			g.Expect(a.Capacity).NotTo(gomega.BeNil())
			g.Expect(a.Capacity.Factor).NotTo(gomega.BeNil())
			g.Expect(a.Capacity.Factor.String()).To(gomega.Equal("2"),
				"the applied capacity factor must be recorded as weight provenance")

			b := byCluster["cluster-b"]
			g.Expect(b.Weight).To(gomega.Equal(int32(1)))
			g.Expect(b.Healthy).To(gomega.BeTrue())
			// An absent factor is the identity 1.0, defaulted by the CRD schema.
			g.Expect(b.Capacity).NotTo(gomega.BeNil())
		}).Should(gomega.Succeed())
	})

	ginkgo.It("health-gates an unready home to weight 0 and re-weights the survivor", func() {
		name := uniqueName("svc")
		isvc := createISVC(isvcNS, name, &v1beta1.PlacementSpec{
			Mode:         v1beta1.PlacementModeSplit,
			Requirements: "accelerator=test",
			CapacityFactors: map[string]resource.Quantity{
				"cluster-a": resource.MustParse("2"),
			},
		})
		placeISVC(isvc,
			candidate("cluster-a", "cluster-a.example.com", 2, 2),
			candidate("cluster-b", "cluster-b.example.com", 2, 2))

		ginkgo.By("waiting for the initial 2:1 split")
		gomega.Eventually(func(g gomega.Gomega) {
			byCluster := entriesByCluster(g, getTrafficMap(g, isvcNS, name))
			g.Expect(byCluster["cluster-a"].Weight).To(gomega.Equal(int32(2)))
			g.Expect(byCluster["cluster-b"].Weight).To(gomega.Equal(int32(1)))
		}).Should(gomega.Succeed())

		ginkgo.By("dropping cluster-b's ready replicas to zero")
		placeISVC(isvc,
			candidate("cluster-a", "cluster-a.example.com", 2, 2),
			candidate("cluster-b", "cluster-b.example.com", 2, 0))

		// b is gated to 0 so a is the only serving home; a's weight reduces to the
		// unit 1 now that it is alone.
		gomega.Eventually(func(g gomega.Gomega) {
			byCluster := entriesByCluster(g, getTrafficMap(g, isvcNS, name))
			g.Expect(byCluster["cluster-a"].Weight).To(gomega.Equal(int32(1)))
			g.Expect(byCluster["cluster-a"].Healthy).To(gomega.BeTrue())
			g.Expect(byCluster["cluster-b"].Weight).To(gomega.Equal(int32(0)))
			g.Expect(byCluster["cluster-b"].Healthy).To(gomega.BeFalse())
		}).Should(gomega.Succeed())
	})

	ginkgo.It("publishes all-zero weights when every home is unready", func() {
		name := uniqueName("svc")
		isvc := createISVC(isvcNS, name, &v1beta1.PlacementSpec{
			Mode:         v1beta1.PlacementModeSplit,
			Requirements: "accelerator=test",
		})
		placeISVC(isvc,
			candidate("cluster-a", "cluster-a.example.com", 2, 0),
			candidate("cluster-b", "cluster-b.example.com", 2, 0))

		gomega.Eventually(func(g gomega.Gomega) {
			tm := getTrafficMap(g, isvcNS, name)
			g.Expect(tm.Spec.Entries).To(gomega.HaveLen(2))
			for _, entry := range tm.Spec.Entries {
				g.Expect(entry.Weight).To(gomega.BeZero())
				g.Expect(entry.Healthy).To(gomega.BeFalse())
			}
			cond := apimeta.FindStatusCondition(tm.Status.Conditions, v1beta1.TrafficMapRoutable)
			g.Expect(cond).NotTo(gomega.BeNil())
			g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionFalse))
			g.Expect(cond.Reason).To(gomega.Equal(v1beta1.TrafficMapReasonAllHomesUnready))
		}).Should(gomega.Succeed())
	})

	ginkgo.It("keeps an emptied TrafficMap that explains itself when the ISVC leaves Placed", func() {
		name := uniqueName("svc")
		isvc := createISVC(isvcNS, name, &v1beta1.PlacementSpec{
			Mode:         v1beta1.PlacementModeSingle,
			Requirements: "accelerator=test",
		})
		placeISVC(isvc, candidate("cluster-a", "cluster-a.example.com", 1, 1))

		ginkgo.By("waiting for the TrafficMap to be generated with a routable home")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(getTrafficMap(g, isvcNS, name).Spec.Entries).To(gomega.HaveLen(1))
		}).Should(gomega.Succeed())

		ginkgo.By("demoting the ISVC out of Placed")
		updateISVCStatus(isvc, func(s *v1beta1.InferenceService) {
			s.Status.Placement.Phase = v1beta1.PlacementPhaseAdmitting
		})

		// A status demotion does not make the source ineligible: the map is
		// emptied and Routable carries the reason, so a consumer distinguishes
		// "known and unroutable" from "unknown".
		gomega.Eventually(func(g gomega.Gomega) {
			tm := getTrafficMap(g, isvcNS, name)
			g.Expect(tm.Spec.Entries).To(gomega.BeEmpty(),
				"an unplaced ISVC keeps its TrafficMap but with no routing entries")
			cond := apimeta.FindStatusCondition(tm.Status.Conditions, v1beta1.TrafficMapRoutable)
			g.Expect(cond).NotTo(gomega.BeNil(), "the Routable condition must explain the empty table")
			g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionFalse))
			g.Expect(cond.Reason).To(gomega.Equal(v1beta1.TrafficMapReasonNotPlaced))
		}).Should(gomega.Succeed())
	})
})

// createISVC creates a minimal InferenceService (single Engine runner) with an
// optional placement spec, and returns the created object (with its generation
// and UID populated).
func createISVC(ns, name string, pl *v1beta1.PlacementSpec) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: corev1.Container{Name: "ome-container", Image: "example/model-server:test"},
				},
			},
			Placement: pl,
		},
	}
	gomega.ExpectWithOffset(1, k8sClient.Create(suiteCtx, isvc)).To(gomega.Succeed())
	return isvc
}

// placeISVC drives the ISVC to Placed with the given candidates — the input the
// routing controller projects into a TrafficMap. The controller reads the mode
// from the spec, so status carries only phase and candidates.
func placeISVC(isvc *v1beta1.InferenceService, candidates ...v1beta1.CandidatePlacement) {
	updateISVCStatusWithOffset(1, isvc, func(s *v1beta1.InferenceService) {
		s.Status.Placement = &v1beta1.PlacementStatus{
			Phase:      v1beta1.PlacementPhasePlaced,
			Candidates: candidates,
		}
	})
}

// candidate builds an Admitted candidate with an addressable endpoint and the
// given admitted/ready replica counts — a serving home the controller will route to.
func candidate(cluster, host string, admitted, ready int32) v1beta1.CandidatePlacement {
	return v1beta1.CandidatePlacement{
		Cluster:          cluster,
		Phase:            v1beta1.CandidatePhaseAdmitted,
		Endpoint:         &apis.URL{Scheme: "http", Host: host},
		AdmittedReplicas: admitted,
		ReadyReplicas:    ready,
	}
}

// updateISVCStatus applies mutate to the ISVC's status, retrying on conflict.
func updateISVCStatus(isvc *v1beta1.InferenceService, mutate func(*v1beta1.InferenceService)) {
	updateISVCStatusWithOffset(1, isvc, mutate)
}

func updateISVCStatusWithOffset(offset int, isvc *v1beta1.InferenceService, mutate func(*v1beta1.InferenceService)) {
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}
	gomega.EventuallyWithOffset(offset+1, func(g gomega.Gomega) {
		cur := &v1beta1.InferenceService{}
		g.Expect(k8sClient.Get(suiteCtx, key, cur)).To(gomega.Succeed())
		mutate(cur)
		g.Expect(k8sClient.Status().Update(suiteCtx, cur)).To(gomega.Succeed())
	}).Should(gomega.Succeed())
}

// getTrafficMap fetches the ISVC's TrafficMap (named after it), failing the
// enclosing assertion via g when absent.
func getTrafficMap(g gomega.Gomega, ns, name string) *v1beta1.TrafficMap {
	tm := &v1beta1.TrafficMap{}
	g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns, Name: name}, tm)).To(gomega.Succeed())
	return tm
}

// entriesByCluster indexes a TrafficMap's entries by cluster for order-independent assertions.
func entriesByCluster(g gomega.Gomega, tm *v1beta1.TrafficMap) map[string]v1beta1.TrafficMapEntry {
	out := make(map[string]v1beta1.TrafficMapEntry, len(tm.Spec.Entries))
	for _, e := range tm.Spec.Entries {
		out[e.Cluster] = e
	}
	return out
}

// nameCounter backs uniqueName so every spec gets a distinct, DNS-safe name. The
// controller watches all ISVCs cluster-wide, so specs that reused a name would
// observe each other's objects.
var nameCounter atomic.Int64

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, nameCounter.Add(1))
}
